package provider

import (
	"fmt"
	"maps"
	"slices"

	"golang.org/x/mod/semver"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// validateNoDowngrade rejects moving any component to an older Milvus
// release than the Milvus CR runs. Milvus migrates its metadata forward on
// upgrade and an older binary cannot be trusted to read it back.
func validateNoDowngrade(existing *milvusapi.Milvus, desired milvusapi.MilvusSpec) field.ErrorList {
	if existing.Spec.Mode == "" {
		return nil
	}
	return downgrades(componentVersions(existing.Spec.Com), componentVersions(desired.Com))
}

// validateNoDowngradeTransition is validateNoDowngrade against the previous
// Instance, for admission, both resolved through the Provider's catalog.
func validateNoDowngradeTransition(in, old *corev1alpha1.Instance, providerSpec *corev1alpha1.ProviderSpec) field.ErrorList {
	return downgrades(requestedVersions(old, providerSpec), requestedVersions(in, providerSpec))
}

// downgrades reports every component whose requested version is older than
// its current one.
func downgrades(current, requested map[string]string) field.ErrorList {
	var errs field.ErrorList
	for _, name := range slices.Sorted(maps.Keys(requested)) {
		from, to := current[name], requested[name]
		if !semver.IsValid("v"+from) || !semver.IsValid("v"+to) {
			continue
		}
		if semver.Compare("v"+to, "v"+from) < 0 {
			errs = append(errs, field.Forbidden(componentsPath.Key(name).Child("version"),
				fmt.Sprintf("component %q cannot be downgraded from %s to %s; choose %s or newer", name, from, to, from)))
		}
	}
	return errs
}

// requestedVersions maps each Milvus component of the instance's topology to
// the version the operator would run it at, as BuildMilvusSpec resolves it:
// its own version, else its version in the instance's bundle, else the
// bundle's name.
func requestedVersions(in *corev1alpha1.Instance, providerSpec *corev1alpha1.ProviderSpec) map[string]string {
	bundleName := controller.EffectiveVersionBundleName(providerSpec, in)
	var bundle *corev1alpha1.VersionBundle
	if bundleName != "" {
		bundle, _ = controller.ResolveVersionBundle(providerSpec, bundleName)
	}
	versions := map[string]string{}
	for _, name := range allowedComponentsForTopology(instanceTopology(in)) {
		version := in.Spec.Components[name].Version
		if version == "" && bundle != nil {
			version = bundle.Components[name]
		}
		if version == "" {
			version = bundleName
		}
		if version != "" {
			versions[name] = operatorVersion(version)
		}
	}
	return versions
}

// componentVersions maps each deployed component to the Milvus version the
// operator runs it at; a component without its own version inherits the
// instance-wide one.
func componentVersions(com milvusapi.MilvusComponents) map[string]string {
	versions := map[string]string{}
	for name, spec := range milvusComponentSpecs(com) {
		versions[name] = spec.Version
		if spec.Version == "" {
			versions[name] = com.Version
		}
	}
	return versions
}

// lowestComponentVersion is the oldest valid Milvus version any component
// runs, or empty when none is known.
func lowestComponentVersion(com milvusapi.MilvusComponents) string {
	lowest := ""
	for _, version := range componentVersions(com) {
		if !semver.IsValid("v" + version) {
			continue
		}
		if lowest == "" || semver.Compare("v"+version, "v"+lowest) < 0 {
			lowest = version
		}
	}
	return lowest
}

// applyImageUpdateMode picks how the operator rolls out new images. Its
// dependency-ordered rolling upgrade moves each component only once the ones
// it depends on run the instance-wide image, so it can finish only when every
// component shares that image; components on different images are updated
// all at once instead, or the upgrade would stall forever.
func applyImageUpdateMode(com *milvusapi.MilvusComponents) {
	shared, ok := sharedComponentImage(*com)
	if !ok {
		com.ImageUpdateMode = milvusapi.ImageUpdateModeAll
		return
	}
	com.Image, com.Version = shared.image, shared.version
	com.ImageUpdateMode = milvusapi.ImageUpdateModeRollingUpgrade
}

// sharedComponentImage returns the image and version every component runs,
// or false when they differ.
func sharedComponentImage(com milvusapi.MilvusComponents) (milvusImage, bool) {
	shared := milvusImage{image: com.Image, version: com.Version}
	first := true
	for _, spec := range milvusComponentSpecs(com) {
		image := milvusImage{image: spec.Image, version: spec.Version}
		if first {
			shared, first = image, false
			continue
		}
		if image != shared {
			return milvusImage{}, false
		}
	}
	return shared, true
}

// milvusComponentSpecs returns the spec of every component present in com,
// keyed by Instance component name.
func milvusComponentSpecs(com milvusapi.MilvusComponents) map[string]milvusapi.ComponentSpec {
	specs := map[string]milvusapi.ComponentSpec{}
	if com.Standalone != nil {
		specs[common.ComponentStandalone] = com.Standalone.ComponentSpec
	}
	if com.Proxy != nil {
		specs[common.ComponentProxy] = com.Proxy.ComponentSpec
	}
	if com.MixCoord != nil {
		specs[common.ComponentMixCoord] = com.MixCoord.ComponentSpec
	}
	if com.DataNode != nil {
		specs[common.ComponentDataNode] = com.DataNode.ComponentSpec
	}
	if com.QueryNode != nil {
		specs[common.ComponentQueryNode] = com.QueryNode.ComponentSpec
	}
	if com.StreamingNode != nil {
		specs[common.ComponentStreaming] = com.StreamingNode.ComponentSpec
	}
	return specs
}
