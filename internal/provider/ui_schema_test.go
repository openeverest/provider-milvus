package provider

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

// uiFields returns every component of the generated UI schema that writes a
// path.
func uiFields(t *testing.T) []map[string]any {
	t.Helper()
	spec := shippedProviderSpec(t)
	require.NotNil(t, spec.UISchema)
	var schema map[string]any
	require.NoError(t, json.Unmarshal(spec.UISchema.Raw, &schema))

	var fields []map[string]any
	var walk func(any)
	walk = func(node any) {
		switch n := node.(type) {
		case map[string]any:
			if _, ok := n["path"].(string); ok {
				if _, ok := n["uiType"]; ok {
					fields = append(fields, n)
				}
			}
			for _, v := range n {
				walk(v)
			}
		case []any:
			for _, v := range n {
				walk(v)
			}
		}
	}
	walk(schema)
	require.NotEmpty(t, fields)
	return fields
}

var (
	// resourceField matches the paths of CPU and memory quantities, of the
	// Milvus components and of the bundled dependencies.
	resourceField = regexp.MustCompile(`\.resources\.(?:limits|requests)\.(cpu|memory)$`)
	// componentLimit matches the Milvus component limits Validate has
	// minimums for.
	componentLimit = regexp.MustCompile(`^spec\.components\.\w+\.resources\.limits\.(cpu|memory)$`)
)

// Resource fields are quantity fields: the form holds a number in the field's
// unit and writes a quantity string, so fractional CPU ("0.5") and memory
// ("0.25Gi") reach the API as strings it accepts. CPU is in cores (no unit),
// memory and volume sizes in Gi. The smallest value a field offers must be a
// quantity the provider accepts.
func TestUIResourceFieldsAreQuantities(t *testing.T) {
	floors := map[string]resource.Quantity{"cpu": minComponentCPU, "memory": minComponentMemory}

	var checked int
	for _, field := range uiFields(t) {
		path := field["path"].(string)
		kind := ""
		if m := resourceField.FindStringSubmatch(path); m != nil {
			kind = m[1]
		} else if strings.HasSuffix(path, ".size") {
			kind = "size"
		} else {
			continue
		}
		checked++

		require.Equal(t, "quantity", field["uiType"], path)
		params, _ := field["fieldParams"].(map[string]any)
		validation, _ := field["validation"].(map[string]any)
		assert.NotContains(t, params, "badge", path)
		assert.NotContains(t, params, "badgeToApi", path)

		unit, _ := params["unit"].(string)
		if kind == "cpu" {
			assert.Empty(t, unit, "%s is in cores", path)
			assert.Equal(t, "CPU", params["unitLabel"], path)
		} else {
			assert.Equal(t, "Gi", unit, path)
		}

		for _, v := range []any{params["defaultValue"], validation["min"]} {
			n, ok := v.(float64)
			require.True(t, ok, "%s needs a numeric defaultValue and min", path)
			_, err := resource.ParseQuantity(strconv.FormatFloat(n, 'f', -1, 64) + unit)
			assert.NoError(t, err, "%s writes %v%s", path, n, unit)
		}

		uiMin := resource.MustParse(strconv.FormatFloat(validation["min"].(float64), 'f', -1, 64) + unit)
		floor, hasFloor := floors[kind]
		if kind == "size" {
			floor, hasFloor = minStorageSize, true
		} else if !componentLimit.MatchString(path) {
			hasFloor = false
		}
		if hasFloor {
			assert.GreaterOrEqual(t, uiMin.Cmp(floor), 0, "%s min %s is below the provider's %s", path, uiMin.String(), floor.String())
		}
	}
	require.Positive(t, checked)
}

var celPath = regexp.MustCompile(`(?:original\.)?(spec(?:\.\w+)+)`)

// setPath stores value at a dotted path, creating the maps on the way.
func setPath(obj map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	for _, p := range parts[:len(parts)-1] {
		next, ok := obj[p].(map[string]any)
		if !ok {
			next = map[string]any{}
			obj[p] = next
		}
		obj = next
	}
	if value != nil {
		obj[parts[len(parts)-1]] = value
	}
}

// Every volume size refuses to shrink in the edit dialog. An edit dialog
// evaluates the CEL rules of every section, and a rule that errors keeps Save
// disabled, so each must hold whether the value reaches CEL as a number or as
// a string, and for an instance that never set the size.
func TestUIEditRulesRefuseShrinkingVolumes(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Variable("spec", cel.DynType),
		cel.Variable("original", cel.DynType),
	)
	require.NoError(t, err)

	rules := map[string]string{}
	for _, field := range uiFields(t) {
		path := field["path"].(string)
		if !strings.HasSuffix(path, ".size") {
			continue
		}
		validation, _ := field["validation"].(map[string]any)
		modes, _ := validation["modes"].(map[string]any)
		edit, _ := modes["edit"].(map[string]any)
		exprs, _ := edit["celExpressions"].([]any)
		require.Len(t, exprs, 1, "%s refuses to shrink", path)
		rules[path] = exprs[0].(map[string]any)["celExpr"].(string)
	}
	require.NotEmpty(t, rules)

	eval := func(expr string, form, original any) (any, error) {
		ast, iss := env.Compile(expr)
		require.NoError(t, iss.Err(), expr)
		prg, err := env.Program(ast)
		require.NoError(t, err)
		vars := map[string]any{"original": map[string]any{}}
		for _, m := range celPath.FindAllStringSubmatch(expr, -1) {
			setPath(vars, m[1], form)
			setPath(vars["original"].(map[string]any), m[1], original)
		}
		out, _, err := prg.Eval(vars)
		if err != nil {
			return nil, err
		}
		return out.Value(), nil
	}

	for path, expr := range rules {
		assert.Contains(t, expr, "original."+path, path)
		for _, tc := range []struct {
			form, original any
			want           bool
		}{
			{"10", 10, true},
			{10, 10, true},
			{int64(11), int64(10), true},
			{10.5, 10, true},
			{"9", 10, false},
			{9, 10, false},
			{9, nil, true},
			{nil, nil, true},
		} {
			got, err := eval(expr, tc.form, tc.original)
			if assert.NoError(t, err, "%s with %#v vs %#v", path, tc.form, tc.original) {
				assert.Equal(t, tc.want, got, "%s with %#v vs %#v", path, tc.form, tc.original)
			}
		}
	}
}
