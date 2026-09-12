package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/cluster"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// Minimum per-component sizing. Values mirror the topology UI schema
// (definition/topologies/*/topology.yaml) so API and UI reject the same specs.
var (
	minComponentCPU    = resource.MustParse("100m")
	minComponentMemory = resource.MustParse("512Mi")
	minStorageSize     = resource.MustParse("1Gi")
)

// clusterCoordinators are the coordinator components required to have at least
// one replica in cluster mode.
var clusterCoordinators = []string{
	common.ComponentRootCoord,
	common.ComponentIndexCoord,
	common.ComponentDataCoord,
	common.ComponentQueryCoord,
}

// allowedComponentsForTopology returns the component names valid for the given
// normalized topology type.
func allowedComponentsForTopology(topologyType string) []string {
	if topologyType == "cluster" {
		return []string{
			common.ComponentProxy,
			common.ComponentRootCoord,
			common.ComponentIndexCoord,
			common.ComponentDataCoord,
			common.ComponentQueryCoord,
			common.ComponentIndexNode,
			common.ComponentDataNode,
			common.ComponentQueryNode,
		}
	}
	return []string{common.ComponentStandalone}
}

// validateInstance runs all spec validations for the instance.
func validateInstance(c *controller.Context) error {
	instance := c.Instance()

	topologyType := "standalone"
	if instance.Spec.Topology != nil && instance.Spec.Topology.Type != "" {
		topologyType = normalizeTopologyName(instance.Spec.Topology.Type)
	}
	if topologyType != "standalone" && topologyType != "cluster" {
		requested := ""
		if instance.Spec.Topology != nil {
			requested = instance.Spec.Topology.Type
		}
		return fmt.Errorf("unsupported topology %q; expected standalone or cluster", requested)
	}

	if err := validateComponentsForTopology(instance.Spec.Components, topologyType); err != nil {
		return err
	}

	if err := validateTopologyRules(instance.Spec.Components, topologyType); err != nil {
		return err
	}

	if err := validateStorageNotDecreased(c, topologyType); err != nil {
		return err
	}

	if err := validateDependencies(c, topologyType); err != nil {
		return err
	}

	if _, err := milvusEngineConfig(c); err != nil {
		return err
	}

	return nil
}

// validateComponentsForTopology rejects components that do not belong to the
// topology and validates replicas, resource sizing, and request/limit balance
// for every allowed component that is present.
func validateComponentsForTopology(components map[string]corev1alpha1.ComponentSpec, topologyType string) error {
	allowed := allowedComponentsForTopology(topologyType)
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}

	for name := range components {
		if _, ok := allowedSet[name]; !ok {
			return fmt.Errorf("component %q is not valid for %s topology", name, topologyType)
		}
	}

	for _, name := range allowed {
		component, ok := components[name]
		if !ok {
			continue
		}
		if component.Replicas != nil && *component.Replicas < 1 {
			return fmt.Errorf("component %q replicas must be >= 1", name)
		}
		if err := validateComponentResources(name, component); err != nil {
			return err
		}
		if err := validateComponentStorage(name, component); err != nil {
			return err
		}
	}

	return nil
}

// validateComponentResources enforces minimum CPU/RAM and request/limit balance
// when resources are specified. Unset resources fall back to operator defaults.
func validateComponentResources(name string, component corev1alpha1.ComponentSpec) error {
	if component.Resources == nil {
		return nil
	}

	limits := component.Resources.Limits
	if cpu, ok := limits[corev1.ResourceCPU]; ok && cpu.Cmp(minComponentCPU) < 0 {
		return fmt.Errorf("component %q resources.limits.cpu must be >= %s", name, minComponentCPU.String())
	}
	if mem, ok := limits[corev1.ResourceMemory]; ok && mem.Cmp(minComponentMemory) < 0 {
		return fmt.Errorf("component %q resources.limits.memory must be >= %s", name, minComponentMemory.String())
	}

	requests := component.Resources.Requests
	if err := validateRequestNotAboveLimit(name, corev1.ResourceCPU, requests, limits); err != nil {
		return err
	}
	if err := validateRequestNotAboveLimit(name, corev1.ResourceMemory, requests, limits); err != nil {
		return err
	}

	return nil
}

// validateRequestNotAboveLimit ensures a resource request does not exceed its
// limit when both are set.
func validateRequestNotAboveLimit(name string, resourceName corev1.ResourceName, requests, limits corev1.ResourceList) error {
	request, hasRequest := requests[resourceName]
	limit, hasLimit := limits[resourceName]
	if hasRequest && hasLimit && request.Cmp(limit) > 0 {
		return fmt.Errorf("component %q resources.requests.%s (%s) must not exceed resources.limits.%s (%s)",
			name, resourceName, request.String(), resourceName, limit.String())
	}
	return nil
}

// validateComponentStorage enforces the minimum storage size when storage is
// specified for a component.
func validateComponentStorage(name string, component corev1alpha1.ComponentSpec) error {
	if component.Storage == nil || component.Storage.Size.IsZero() {
		return nil
	}
	if component.Storage.Size.Cmp(minStorageSize) < 0 {
		return fmt.Errorf("component %q storage.size must be >= %s", name, minStorageSize.String())
	}
	return nil
}

// validateTopologyRules enforces topology-specific composition rules, such as
// requiring at least one replica for every coordinator in cluster mode.
func validateTopologyRules(components map[string]corev1alpha1.ComponentSpec, topologyType string) error {
	if topologyType != "cluster" {
		return nil
	}
	for _, name := range clusterCoordinators {
		component, ok := components[name]
		if !ok {
			continue
		}
		if component.Replicas != nil && *component.Replicas < 1 {
			return fmt.Errorf("cluster mode requires at least 1 replica for coordinator %q", name)
		}
	}
	return nil
}

// validateStorageNotDecreased guards against shrinking persistent storage on
// edit. It compares the requested size against the size already applied to the
// existing Milvus CR; storage may only grow or stay the same.
func validateStorageNotDecreased(c *controller.Context, topologyType string) error {
	requested := requestedStorageSize(c.Instance().Spec.Components, topologyType)
	if requested == "" {
		return nil
	}
	requestedQty, err := resource.ParseQuantity(requested)
	if err != nil {
		return fmt.Errorf("invalid storage size %q: %w", requested, err)
	}

	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil {
		// No existing CR yet (create path) or transient read error: nothing to compare.
		return nil
	}
	currentSize := currentStorageSize(existing)
	if currentSize == "" {
		return nil
	}
	currentQty, err := resource.ParseQuantity(currentSize)
	if err != nil {
		return nil
	}

	if requestedQty.Cmp(currentQty) < 0 {
		return fmt.Errorf("storage size cannot be decreased from %s to %s", currentQty.String(), requestedQty.String())
	}
	return nil
}

// requestedStorageSize returns the storage size the spec would apply, matching
// the component precedence used by BuildMilvusSpec.
func requestedStorageSize(components map[string]corev1alpha1.ComponentSpec, topologyType string) string {
	if topologyType == "cluster" {
		return storageSizeFromComponents(components, common.ComponentDataNode, common.ComponentQueryNode)
	}
	return storageSizeFromComponent(components, common.ComponentStandalone)
}

// currentStorageSize extracts the persistence size applied to an existing
// Milvus CR, mirroring the layout written by buildStorage.
func currentStorageSize(m *milvusapi.Milvus) string {
	if m.Spec.Dep == nil || m.Spec.Dep.Storage.InCluster == nil {
		return ""
	}
	persistence, ok := m.Spec.Dep.Storage.InCluster.Values["persistence"].(map[string]any)
	if !ok {
		return ""
	}
	size, ok := persistence["size"].(string)
	if !ok {
		return ""
	}
	return size
}

// validateDependencies validates the bundled/external dependency configuration
// carried in the topology parameters.
func validateDependencies(c *controller.Context, topologyType string) error {
	var etcd *dependencies.Etcd
	var pulsar *dependencies.Pulsar
	var storage *dependencies.Storage

	if topologyType == "cluster" {
		var params cluster.ClusterTopologyParameters
		if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
			etcd = params.Dependencies.Etcd
			pulsar = params.Dependencies.Pulsar
			storage = params.Dependencies.Storage
		}
	} else {
		var params standalone.StandaloneTopologyParameters
		if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
			etcd = params.Dependencies.Etcd
			storage = params.Dependencies.Storage
		}
	}

	if err := validateEtcdDependency(etcd); err != nil {
		return err
	}
	if err := validatePulsarDependency(pulsar); err != nil {
		return err
	}
	return validateStorageDependency(storage)
}

func validateEtcdDependency(etcd *dependencies.Etcd) error {
	if etcd == nil {
		return nil
	}
	if etcd.External {
		if len(etcd.Endpoints) == 0 {
			return fmt.Errorf("etcd.endpoints is required when etcd.external is true")
		}
		return nil
	}
	if err := validateDependencyReplicas("etcd", etcd.Replicas); err != nil {
		return err
	}
	return validateDependencyResources("etcd", etcd.Resources)
}

func validatePulsarDependency(pulsar *dependencies.Pulsar) error {
	if pulsar == nil {
		return nil
	}
	if pulsar.External {
		if pulsar.Endpoint == "" {
			return fmt.Errorf("pulsar.endpoint is required when pulsar.external is true")
		}
		return nil
	}
	components := map[string]*dependencies.PulsarComponent{
		"pulsar.broker":     pulsar.Broker,
		"pulsar.bookkeeper": pulsar.BookKeeper,
		"pulsar.zookeeper":  pulsar.ZooKeeper,
		"pulsar.proxy":      pulsar.Proxy,
	}
	for _, name := range []string{"pulsar.broker", "pulsar.bookkeeper", "pulsar.zookeeper", "pulsar.proxy"} {
		component := components[name]
		if component == nil {
			continue
		}
		if err := validateDependencyReplicas(name, component.Replicas); err != nil {
			return err
		}
		if err := validateDependencyResources(name, component.Resources); err != nil {
			return err
		}
	}
	return nil
}

func validateStorageDependency(storage *dependencies.Storage) error {
	if storage == nil {
		return nil
	}
	if storage.External {
		if storage.Endpoint == "" {
			return fmt.Errorf("storage.endpoint is required when storage.external is true")
		}
		return nil
	}
	if err := validateDependencyReplicas("storage", storage.Replicas); err != nil {
		return err
	}
	return validateDependencyResources("storage", storage.Resources)
}

func validateDependencyReplicas(name string, replicas *int32) error {
	if replicas != nil && *replicas < 1 {
		return fmt.Errorf("%s.replicas must be >= 1", name)
	}
	return nil
}

// validateDependencyResources parses the CPU/memory quantity strings and
// ensures requests do not exceed limits.
func validateDependencyResources(name string, resources *dependencies.Resources) error {
	if resources == nil {
		return nil
	}
	requests, err := parseResourceList(name, "requests", resources.Requests)
	if err != nil {
		return err
	}
	limits, err := parseResourceList(name, "limits", resources.Limits)
	if err != nil {
		return err
	}
	if err := validateRequestNotAboveLimit(name, corev1.ResourceCPU, requests, limits); err != nil {
		return err
	}
	return validateRequestNotAboveLimit(name, corev1.ResourceMemory, requests, limits)
}

// parseResourceList converts a dependency ResourceList into a corev1.ResourceList,
// validating that every provided quantity string is well-formed.
func parseResourceList(name, kind string, list *dependencies.ResourceList) (corev1.ResourceList, error) {
	result := corev1.ResourceList{}
	if list == nil {
		return result, nil
	}
	if list.CPU != "" {
		cpu, err := resource.ParseQuantity(list.CPU)
		if err != nil {
			return nil, fmt.Errorf("%s.resources.%s.cpu %q is invalid: %w", name, kind, list.CPU, err)
		}
		result[corev1.ResourceCPU] = cpu
	}
	if list.Memory != "" {
		mem, err := resource.ParseQuantity(list.Memory)
		if err != nil {
			return nil, fmt.Errorf("%s.resources.%s.memory %q is invalid: %w", name, kind, list.Memory, err)
		}
		result[corev1.ResourceMemory] = mem
	}
	return result, nil
}
