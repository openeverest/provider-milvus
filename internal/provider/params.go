package provider

import (
	"encoding/json"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
)

// Field paths of the Instance that the rules report errors on.
var (
	componentsPath         = field.NewPath("spec", "components")
	parametersPath         = field.NewPath("spec", "parameters")
	topologyTypePath       = field.NewPath("spec", "topology", "type")
	topologyParametersPath = field.NewPath("spec", "topology", "parameters")
	dependenciesPath       = topologyParametersPath.Child("dependencies")
)

// instanceTopology is the topology the instance runs: standalone unless it
// asks for another.
func instanceTopology(in *corev1alpha1.Instance) string {
	if in.Spec.Topology == nil {
		return "standalone"
	}
	return normalizeTopologyName(in.Spec.Topology.Type)
}

// decodeRaw unmarshals raw parameters into target. Unset parameters leave
// target untouched.
func decodeRaw(raw *runtime.RawExtension, target any) error {
	if raw == nil || raw.Raw == nil {
		return nil
	}
	return json.Unmarshal(raw.Raw, target)
}

// tryDecodeTopologyParameters decodes the instance's topology parameters,
// reporting false when they are unset or malformed.
func tryDecodeTopologyParameters(in *corev1alpha1.Instance, target any) bool {
	raw := in.GetTopologyParameters()
	return raw != nil && raw.Raw != nil && json.Unmarshal(raw.Raw, target) == nil
}

// invalid reports a rule the value at path breaks, without repeating the value.
func invalid(path *field.Path, detail string) *field.Error {
	return field.Invalid(path, field.OmitValueType{}, detail)
}

// appendErr adds err to errs on path, unless err already carries its own.
func appendErr(errs field.ErrorList, path *field.Path, err error) field.ErrorList {
	if err == nil {
		return errs
	}
	if fieldErr, ok := errors.AsType[*field.Error](err); ok {
		return append(errs, fieldErr)
	}
	return append(errs, invalid(path, err.Error()))
}

// fieldErrorf formats a rule's detail onto the field it is about.
func fieldErrorf(path *field.Path, format string, args ...any) error {
	return invalid(path, fmt.Sprintf(format, args...))
}
