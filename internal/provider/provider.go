package provider

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/yaml"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for the provider-milvus provider.
type Provider struct {
	controller.BaseProvider
}

// New creates a new Provider instance.
func New() *Provider {
	return &Provider{
		BaseProvider: controller.BaseProvider{
			ProviderName: common.ProviderName,
			SchemeFuncs: []func(*runtime.Scheme) error{
				milvusapi.AddToScheme,
			},
			WatchConfigs: []controller.WatchConfig{
				controller.WatchOwned(&milvusapi.Milvus{}),
			},
		},
	}
}

func normalizeTopologyName(t string) string {
	switch strings.ToLower(t) {
	case "", "standalone":
		return "standalone"
	case "cluster":
		return "cluster"
	default:
		return strings.ToLower(t)
	}
}

func makeComponentReplica(replicas int32) *int32 {
	return ptr.To(replicas)
}

func componentReplicasOrDefault(components map[string]corev1alpha1.ComponentSpec, name string, defaultReplicas int32) *int32 {
	component := components[name]
	if component.Replicas != nil {
		return component.Replicas
	}
	return makeComponentReplica(defaultReplicas)
}

func componentResourcesOrNil(components map[string]corev1alpha1.ComponentSpec, name string) *corev1.ResourceRequirements {
	component := components[name]
	if component.Resources == nil || (len(component.Resources.Limits) == 0 && len(component.Resources.Requests) == 0) {
		return nil
	}
	return &corev1.ResourceRequirements{
		Limits:   component.Resources.Limits.DeepCopy(),
		Requests: component.Resources.Requests.DeepCopy(),
	}
}

func makeMilvusComponentSpec(components map[string]corev1alpha1.ComponentSpec, name, image, version string) milvusapi.ComponentSpec {
	return milvusapi.ComponentSpec{
		Image:     image,
		Version:   version,
		Resources: componentResourcesOrNil(components, name),
	}
}

// milvusEngineConfig collects the `configuration` YAML from every component's
// parameters and deep-merges it into a single Values map. Milvus uses one shared
// engine config (spec.config), so configuration provided on any component is
// merged; components are processed in sorted name order for deterministic output.
func milvusEngineConfig(c *controller.Context) (milvusapi.Values, error) {
	instanceComponents := c.Instance().Spec.Components
	names := make([]string, 0, len(instanceComponents))
	for name := range instanceComponents {
		names = append(names, name)
	}
	sort.Strings(names)

	merged := milvusapi.Values{}
	for _, name := range names {
		var params components.MilvusParameters
		if !c.TryDecodeComponentParameters(instanceComponents[name], &params) || params.Configuration == "" {
			continue
		}
		parsed := map[string]any{}
		if err := yaml.Unmarshal([]byte(params.Configuration), &parsed); err != nil {
			return nil, fmt.Errorf("component %q has invalid configuration: %w", name, err)
		}
		deepMergeValues(merged, parsed)
	}
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

// deepMergeValues recursively merges src into dst. Nested maps are merged;
// any non-map value in src overrides the corresponding key in dst.
func deepMergeValues(dst, src map[string]any) {
	for key, srcVal := range src {
		srcMap, srcIsMap := srcVal.(map[string]any)
		dstMap, dstIsMap := dst[key].(map[string]any)
		if srcIsMap && dstIsMap {
			deepMergeValues(dstMap, srcMap)
			continue
		}
		dst[key] = srcVal
	}
}

func resolveMilvusImage(c *controller.Context, componentName, version string) string {
	providerSpec, err := c.ProviderSpec()
	if err != nil || providerSpec == nil {
		return ""
	}
	if version != "" {
		if image := controller.GetImageForVersion(providerSpec, componentName, version); image != "" {
			return image
		}
	}
	return controller.GetDefaultImageForComponent(providerSpec, componentName)
}

func BuildMilvusSpec(c *controller.Context) (milvusapi.MilvusSpec, error) {
	instance := c.Instance()
	topologyType := "standalone"
	if instance.Spec.Topology != nil && instance.Spec.Topology.Type != "" {
		topologyType = normalizeTopologyName(instance.Spec.Topology.Type)
	}

	providerSpec, err := c.ProviderSpec()
	if err != nil {
		return milvusapi.MilvusSpec{}, err
	}

	resolvedVersion := instance.Spec.Version
	if resolvedVersion == "" {
		if bundle := controller.GetDefaultVersionBundle(providerSpec); bundle != nil {
			resolvedVersion = bundle.Name
		}
	}
	if resolvedVersion == "" {
		resolvedVersion = "2.6.11"
	}

	baseImage := "milvusdb/milvus:v2.6.11"
	if image := resolveMilvusImage(c, common.ComponentStandalone, resolvedVersion); image != "" {
		baseImage = image
	}

	mode := milvusapi.MilvusModeStandalone
	if topologyType == "cluster" {
		mode = milvusapi.MilvusModeCluster
	}

	spec := milvusapi.MilvusSpec{
		Mode: mode,
		Com: milvusapi.MilvusComponents{
			ComponentSpec: milvusapi.ComponentSpec{
				Image:   baseImage,
				Version: resolvedVersion,
			},
		},
	}

	engineConfig, err := milvusEngineConfig(c)
	if err != nil {
		return milvusapi.MilvusSpec{}, err
	}
	spec.Conf = engineConfig

	if topologyType == "standalone" {
		spec.Com.Standalone = &milvusapi.MilvusStandalone{
			ServiceComponent: milvusapi.ServiceComponent{
				Component: milvusapi.Component{
					ComponentSpec: makeMilvusComponentSpec(instance.Spec.Components, common.ComponentStandalone, baseImage, resolvedVersion),
					Replicas:      componentReplicasOrDefault(instance.Spec.Components, common.ComponentStandalone, 1),
				},
				Port: 19530,
			},
		}
		applyServiceExposure(&spec.Com.Standalone.ServiceComponent, instance.Spec.Components[common.ComponentStandalone].Service)
		spec.Dep = buildDependencies(c, topologyType)
		return spec, nil
	}

	spec.Com.Proxy = &milvusapi.MilvusProxy{
		ServiceComponent: milvusapi.ServiceComponent{
			Component: milvusapi.Component{
				ComponentSpec: makeMilvusComponentSpec(instance.Spec.Components, common.ComponentProxy, baseImage, resolvedVersion),
				Replicas:      componentReplicasOrDefault(instance.Spec.Components, common.ComponentProxy, 1),
			},
			Port: 19530,
		},
	}
	applyServiceExposure(&spec.Com.Proxy.ServiceComponent, instance.Spec.Components[common.ComponentProxy].Service)

	spec.Com.MixCoord = &milvusapi.MilvusMixCoord{Component: milvusapi.Component{
		ComponentSpec: makeMilvusComponentSpec(instance.Spec.Components, common.ComponentMixCoord, baseImage, resolvedVersion),
		Replicas:      componentReplicasOrDefault(instance.Spec.Components, common.ComponentMixCoord, 1),
	}}

	for _, name := range []string{common.ComponentDataNode, common.ComponentQueryNode, common.ComponentStreaming} {
		replicas := componentReplicasOrDefault(instance.Spec.Components, name, 1)
		componentSpec := makeMilvusComponentSpec(instance.Spec.Components, name, baseImage, resolvedVersion)
		switch name {
		case common.ComponentDataNode:
			spec.Com.DataNode = &milvusapi.MilvusDataNode{Component: milvusapi.Component{ComponentSpec: componentSpec, Replicas: replicas}}
		case common.ComponentQueryNode:
			spec.Com.QueryNode = &milvusapi.MilvusQueryNode{Component: milvusapi.Component{ComponentSpec: componentSpec, Replicas: replicas}}
		case common.ComponentStreaming:
			spec.Com.StreamingNode = &milvusapi.MilvusStreamingNode{Component: milvusapi.Component{ComponentSpec: componentSpec, Replicas: replicas}}
		}
	}
	spec.Dep = buildDependencies(c, topologyType)

	return spec, nil
}

// Validate checks if the Instance spec is valid.
func (p *Provider) Validate(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Validating instance", "name", c.Name())

	return validateInstance(c)
}

// Sync ensures all required resources exist and are configured correctly.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Syncing instance", "name", c.Name())

	_, password, err := ensureCredentials(c)
	if err != nil {
		return err
	}

	spec, err := BuildMilvusSpec(c)
	if err != nil {
		return err
	}
	applyAuthConfig(&spec, password)

	cr := &milvusapi.Milvus{
		ObjectMeta: c.ObjectMeta(c.Name()),
		Spec:       spec,
	}
	preserveOperatorMetadata(c, cr)
	return c.Apply(cr)
}

// preserveOperatorMetadata carries the milvus-operator's own labels and
// annotations (the milvus.io/* namespace) forward across the provider's
// full-object apply. The operator classifies the CR via the
// milvus.io/operator-version label and gates a one-time dependency-value
// migration on milvus.io/dependency-values-* annotations. Dropping them each
// sync demotes the CR back to "legacy", retriggering that migration and putting
// the provider and operator into a reconcile battle.
func preserveOperatorMetadata(c *controller.Context, cr *milvusapi.Milvus) {
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil {
		// Not created yet (create path) or transient read error: nothing to carry.
		return
	}
	cr.Labels = mergeOperatorMetadata(cr.Labels, existing.Labels)
	cr.Annotations = mergeOperatorMetadata(cr.Annotations, existing.Annotations)
}

// mergeOperatorMetadata copies milvus.io/-prefixed keys from src into dst,
// without overwriting keys the provider already set.
func mergeOperatorMetadata(dst, src map[string]string) map[string]string {
	const operatorPrefix = "milvus.io/"
	for k, v := range src {
		if !strings.HasPrefix(k, operatorPrefix) {
			continue
		}
		if dst == nil {
			dst = map[string]string{}
		}
		if _, ok := dst[k]; !ok {
			dst[k] = v
		}
	}
	return dst
}

// Status computes the current status of the database instance.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	l := log.FromContext(c.Context())
	l.Info("Computing status", "name", c.Name())

	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err != nil {
		return controller.Provisioning("waiting for Milvus CR to be created"), nil
	}

	switch cr.Status.Status {
	case milvusapi.StatusHealthy:
		host, port, ready, message := resolveEndpoint(c, cr)
		if !ready {
			return controller.Provisioning(message), nil
		}

		username, password, err := ensureCredentials(c)
		if err != nil {
			return controller.Status{}, err
		}

		return controller.ReadyWithConnectionDetails(controller.ConnectionDetails{
			Type:     "milvus",
			Provider: common.ProviderName,
			Host:     host,
			Port:     port,
			Username: username,
			Password: password,
			URI:      fmt.Sprintf("http://%s:%s", host, port),
			AdditionalProperties: map[string]string{
				"token": fmt.Sprintf("%s:%s", username, password),
			},
		}), nil
	case milvusapi.StatusPending, milvusapi.StatusDeleting:
		return controller.Provisioning("Milvus is being initialized or updated"), nil
	case milvusapi.StatusStopped:
		return controller.Pending("Milvus is stopped"), nil
	case milvusapi.StatusUnhealthy:
		return controller.Provisioning("Milvus is unhealthy"), nil
	default:
		return controller.Provisioning("Milvus is initializing"), nil
	}
}

// Cleanup handles deletion of provider-managed resources.
func (p *Provider) Cleanup(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up instance", "name", c.Name())
	return nil
}
