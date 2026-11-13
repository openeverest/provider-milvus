package provider

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller/validationtest"

	"github.com/openeverest/provider-milvus/internal/common"
)

const chartValuesPath = "../../charts/provider-milvus/values.yaml"

// chartPresets returns the ClusterInstancePresets the chart installs by
// default, keyed by name, as Instances built from their spec.
func chartPresets(t *testing.T) map[string]*corev1alpha1.Instance {
	t.Helper()
	data, err := os.ReadFile(chartValuesPath)
	require.NoError(t, err)
	var values struct {
		Provider struct {
			Name      string `json:"name"`
			ShortName string `json:"shortName"`
		} `json:"provider"`
		Presets []struct {
			Name    string          `json:"name"`
			Enabled bool            `json:"enabled"`
			Spec    json.RawMessage `json:"spec"`
		} `json:"presets"`
	}
	require.NoError(t, yaml.Unmarshal(data, &values))
	require.Equal(t, common.ProviderName, values.Provider.Name)
	require.NotEmpty(t, values.Provider.ShortName)

	presets := map[string]*corev1alpha1.Instance{}
	for _, p := range values.Presets {
		require.NotEmpty(t, p.Name)
		if !p.Enabled {
			continue
		}
		var spec corev1alpha1.ClusterInstancePresetSpec
		// Strict, like the API server: a field the Instance does not have
		// would be pruned from the preset without notice.
		require.NoError(t, yaml.UnmarshalStrict(p.Spec, &spec), "preset %s", p.Name)
		assert.Empty(t, spec.ProviderRef.Name, "preset %s: the chart sets providerRef", p.Name)
		spec.ProviderRef.Name = values.Provider.Name

		name := values.Provider.ShortName + "-" + p.Name
		presets[name] = &corev1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Name: "from-" + name, Namespace: "default"},
			Spec:       spec.InstanceSpec,
		}
	}
	return presets
}

// Every preset the chart ships must create a valid Instance on the
// provider's default version, and every topology must have one.
func TestChartPresetsAreValidInstances(t *testing.T) {
	v := validationtest.New(t, New(), shippedProviderSpecPath)
	presets := chartPresets(t)
	spec := shippedProviderSpec(t)

	topologies := map[string]bool{}
	for name, in := range presets {
		t.Run(name, func(t *testing.T) {
			res := v.Run(in.DeepCopy(), nil)
			assert.Empty(t, res.Errors, "preset %s", name)
			assert.Empty(t, res.Warnings, "preset %s", name)
		})
		assert.Equal(t, spec.DefaultVersion, in.Spec.Version, "preset %s pins the default version bundle", name)
		require.NotNil(t, in.Spec.Topology, "preset %s names its topology", name)
		topologies[in.Spec.Topology.Type] = true

		// A cluster-scoped preset cannot name objects of one namespace.
		storage := storageDependencyParam(in, instanceTopology(in))
		assert.True(t, storage == nil || (storage.BackupStorageName == "" && storage.CredentialsSecret == ""),
			"preset %s names a namespaced object", name)
		assert.Nil(t, tlsParameters(in), "preset %s sets TLS, which may name a Secret", name)
	}
	for topology := range spec.Topologies {
		assert.True(t, topologies[topology], "no preset for the %s topology", topology)
	}
}
