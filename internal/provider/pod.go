package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func componentParameters(in *corev1alpha1.Instance, name string) components.MilvusParameters {
	var params components.MilvusParameters
	if err := decodeRaw(in.Spec.Components[name].Parameters, &params); err != nil {
		return components.MilvusParameters{}
	}
	return params
}

// applyPodCustomization hands the pod-level settings to the operator, which
// renders them onto the component's pod template and Milvus container.
func applyPodCustomization(component *milvusapi.Component, pod *components.PodCustomization) {
	if pod == nil {
		return
	}
	component.PodAnnotations = pod.Annotations
	component.Env = withEnvDefaults(pod.Env)
	component.VolumeMounts = pod.VolumeMounts
	component.SecurityContext = pod.SecurityContext
	for _, raw := range pod.Volumes {
		var volume corev1.Volume
		component.Volumes = append(component.Volumes, withDefaults(raw, &volume, func() { defaultVolume(&volume) }))
	}
	for _, raw := range pod.InitContainers {
		var container corev1.Container
		component.InitContainers = append(component.InitContainers, withDefaults(raw, &container, func() {
			container.Env = withEnvDefaults(container.Env)
		}))
	}
}

// withEnvDefaults sets what the API server would default. The operator diffs
// the pod template it renders against the live Deployment, so a missing
// server default reads as a change on every reconcile and query nodes roll
// forever; the same holds for defaultVolume.
func withEnvDefaults(env []corev1.EnvVar) []corev1.EnvVar {
	for i := range env {
		if ref := env[i].ValueFrom; ref != nil && ref.FieldRef != nil && ref.FieldRef.APIVersion == "" {
			ref.FieldRef.APIVersion = "v1"
		}
	}
	return env
}

func defaultVolume(volume *corev1.Volume) {
	if volume.HostPath != nil && volume.HostPath.Type == nil {
		volume.HostPath.Type = ptr.To(corev1.HostPathUnset)
	}
	if volume.DownwardAPI != nil {
		if volume.DownwardAPI.DefaultMode == nil {
			volume.DownwardAPI.DefaultMode = ptr.To(corev1.DownwardAPIVolumeSourceDefaultMode)
		}
		defaultDownwardAPIItems(volume.DownwardAPI.Items)
	}
	if volume.Projected != nil {
		if volume.Projected.DefaultMode == nil {
			volume.Projected.DefaultMode = ptr.To(corev1.ProjectedVolumeSourceDefaultMode)
		}
		for _, source := range volume.Projected.Sources {
			if source.DownwardAPI != nil {
				defaultDownwardAPIItems(source.DownwardAPI.Items)
			}
		}
	}
}

func defaultDownwardAPIItems(items []corev1.DownwardAPIVolumeFile) {
	for i := range items {
		if ref := items[i].FieldRef; ref != nil && ref.APIVersion == "" {
			ref.APIVersion = "v1"
		}
	}
}

// withDefaults decodes a validated free-form object into target, applies the
// defaults and re-encodes it; an undecodable object is passed through as-is.
func withDefaults(raw apiextensionsv1.JSON, target any, applyDefaults func()) milvusapi.Values {
	if err := decodeKubernetesObject(raw, target); err != nil {
		var values milvusapi.Values
		_ = json.Unmarshal(raw.Raw, &values)
		return values
	}
	applyDefaults()
	defaulted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(target)
	if err != nil {
		var values milvusapi.Values
		_ = json.Unmarshal(raw.Raw, &values)
		return values
	}
	return defaulted
}

// validateComponentParameters rejects malformed component parameters, which
// would otherwise be dropped silently and deploy pods without the requested
// settings.
func validateComponentParameters(in *corev1alpha1.Instance) field.ErrorList {
	var errs field.ErrorList
	for _, name := range slices.Sorted(maps.Keys(in.Spec.Components)) {
		component := in.Spec.Components[name]
		path := componentsPath.Key(name).Child("parameters")
		var params components.MilvusParameters
		if err := decodeRaw(component.Parameters, &params); err != nil {
			errs = append(errs, invalid(path, fmt.Sprintf("component %q has invalid parameters: %v", name, err)))
			continue
		}
		errs = appendErr(errs, path.Child("pod"), validatePodCustomization(name, params.Pod))
		errs = appendErr(errs, path.Child("groups"), validateDeploymentGroups(name, component.Replicas, params.Groups))
		errs = appendErr(errs, path.Child("autoscaling"), validateAutoscaling(name, component, params))
		errs = appendErr(errs, path.Child("probes"), validateProbes(name, params.Probes))
	}
	return errs
}

// validatePodCustomization checks the free-form Kubernetes objects strictly so
// a typo fails here instead of being pruned by the operator.
func validatePodCustomization(name string, pod *components.PodCustomization) error {
	if pod == nil {
		return nil
	}
	for i, env := range pod.Env {
		if env.Name == "" {
			return fmt.Errorf("component %q pod.env[%d].name is required", name, i)
		}
	}
	for i, raw := range pod.Volumes {
		var volume corev1.Volume
		if err := decodeKubernetesObject(raw, &volume); err != nil {
			return fmt.Errorf("component %q pod.volumes[%d] is invalid: %w", name, i, err)
		}
		if volume.Name == "" {
			return fmt.Errorf("component %q pod.volumes[%d].name is required", name, i)
		}
	}
	if pod.SecurityContext != nil {
		var securityContext corev1.SecurityContext
		if err := decodeKubernetesObject(pod.SecurityContext, &securityContext); err != nil {
			return fmt.Errorf("component %q pod.securityContext is invalid: %w", name, err)
		}
	}
	for i, raw := range pod.InitContainers {
		var container corev1.Container
		if err := decodeKubernetesObject(raw, &container); err != nil {
			return fmt.Errorf("component %q pod.initContainers[%d] is invalid: %w", name, i, err)
		}
		if container.Name == "" || container.Image == "" {
			return fmt.Errorf("component %q pod.initContainers[%d] requires name and image", name, i)
		}
	}
	return nil
}

// decodeKubernetesObject decodes a free-form object (a map or an
// apiextensionsv1.JSON) into target, rejecting fields target does not have.
func decodeKubernetesObject(raw any, target any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
