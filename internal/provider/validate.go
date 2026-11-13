package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

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
	common.ComponentMixCoord,
}

// allowedComponentsForTopology returns the component names valid for the given
// normalized topology type.
func allowedComponentsForTopology(topologyType string) []string {
	if topologyType == "cluster" {
		return []string{
			common.ComponentProxy,
			common.ComponentMixCoord,
			common.ComponentDataNode,
			common.ComponentQueryNode,
			common.ComponentStreaming,
		}
	}
	return []string{common.ComponentStandalone}
}

// Validate checks the rules about the Instance alone, and those about changing
// it, against the previous Instance. The runtime runs it at admission and
// before every Sync, once the Instance matches the topologies, components,
// versions and parameter schemas the Provider declares.
func (p *Provider) Validate(in controller.ValidationInput) controller.ValidationResult {
	errs := validateSpec(in.Instance)
	errs = append(errs, validateTransition(in.Instance, in.Old, &in.Provider.Spec)...)
	return controller.ValidationResult{Errors: errs, Warnings: monitoringWarnings(in.Instance)}
}

// ValidateReferences checks the rules that read other objects: the storage
// location, the TLS certificate Secret and, once it runs,
// the Milvus CR and the collections it has loaded. Admission reports what it
// finds as warnings; before Sync it blocks the Instance until fixed.
func (p *Provider) ValidateReferences(c *controller.Context) (field.ErrorList, error) {
	errs, err := validateStorageLocationReferences(c)
	if err != nil || len(errs) > 0 {
		// The running instance is compared with a spec built from the location.
		return errs, err
	}
	for _, check := range []func(*controller.Context) (field.ErrorList, error){
		validateTLSReferences,
		p.validateRunningInstance,
	} {
		checkErrs, err := check(c)
		if err != nil {
			return nil, err
		}
		errs = append(errs, checkErrs...)
	}
	return errs, nil
}

// validateStorageLocationReferences checks that the storage location the
// object storage names exists with access keys.
func validateStorageLocationReferences(c *controller.Context) (field.ErrorList, error) {
	storage := storageDependencyParam(c.Instance(), instanceTopology(c.Instance()))
	if storage == nil || storage.BackupStorageName == "" {
		return nil, nil
	}
	path := dependenciesPath.Child("storage", "backupStorageName")
	if _, err := resolveStorageLocation(c, storage); err != nil {
		return field.ErrorList{invalid(path, err.Error())}, nil
	}
	if _, _, err := storageLocationKeys(c, storage.BackupStorageName); err != nil {
		return field.ErrorList{invalid(path, err.Error())}, nil
	}
	return nil, nil
}

// validateRunningInstance checks the changes the running Milvus cannot take,
// against its Milvus CR: validateTransition at reconcile time, when the
// previous Instance is unknown.
func (p *Provider) validateRunningInstance(c *controller.Context) (field.ErrorList, error) {
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get Milvus %q: %w", c.Name(), err)
	}
	in := c.Instance()
	topologyType := instanceTopology(in)
	var errs field.ErrorList
	errs = appendErr(errs, topologyTypePath, validateTopologyUnchanged(existing, topologyType))
	errs = appendErr(errs, dependenciesPath, validateStorageNotDecreased(existing, in, topologyType))
	if topologyType == "cluster" {
		errs = appendErr(errs, dependenciesPath, validateMessageStreamUnchanged(existing, in))
		errs = appendErr(errs, dependenciesPath.Child("pulsar"), validatePulsarInUse(existing, in))
	}
	if len(errs) == 0 {
		desired, err := BuildMilvusSpec(c)
		if err != nil {
			return nil, err
		}
		errs = append(errs, validateDataLocationUnchanged(existing, desired, c.Name())...)
		errs = appendErr(errs, dependenciesPath, validateStorageClassUnchanged(existing, desired))
		errs = append(errs, validateNoDowngrade(existing, desired)...)
	}
	return appendErr(errs, componentsPath, p.validateReplicaHostsScaleIn(c)), nil
}

// validateTransition checks, against the previous Instance, the changes a
// created instance cannot take, so admission refuses them.
// validateRunningInstance checks the same against the Milvus CR.
func validateTransition(in, old *corev1alpha1.Instance, providerSpec *corev1alpha1.ProviderSpec) field.ErrorList {
	if old == nil || !specApplied(old) {
		return nil
	}
	topologyType := instanceTopology(in)
	if previous := instanceTopology(old); previous != topologyType {
		return field.ErrorList{field.Forbidden(topologyTypePath,
			fmt.Sprintf("topology cannot be changed from %s to %s", previous, topologyType))}
	}

	var errs field.ErrorList
	if previous, requested := requestedStorageSize(old, topologyType), requestedStorageSize(in, topologyType); previous != "" && requested != "" {
		previousQty, prevErr := resource.ParseQuantity(previous)
		requestedQty, reqErr := resource.ParseQuantity(requested)
		if prevErr == nil && reqErr == nil && requestedQty.Cmp(previousQty) < 0 {
			errs = append(errs, field.Forbidden(dependenciesPath.Child("storage", "persistence", "size"),
				fmt.Sprintf("storage size cannot be decreased from %s to %s", previousQty.String(), requestedQty.String())))
		}
	}
	if previous, requested := dependencyStorageClassParam(old, topologyType), dependencyStorageClassParam(in, topologyType); previous != requested &&
		hasBundledVolumes(old, topologyType) {
		errs = append(errs, field.Forbidden(dependenciesPath.Child("storageClassName"),
			fmt.Sprintf("dependencies storageClassName cannot be changed from %q to %q", previous, requested)))
	}
	errs = append(errs, dataLocationTransition(old, in, topologyType)...)
	if topologyType == "cluster" {
		previous, requested := clusterDependencyParams(old).MessageStreamType, clusterDependencyParams(in).MessageStreamType
		if previous != "" && requested != "" && previous != requested {
			errs = append(errs, field.Forbidden(dependenciesPath.Child("messageStreamType"),
				fmt.Sprintf("message stream cannot be changed from %s to %s; set messageStreamType to %s", previous, requested, previous)))
		}
	}
	return append(errs, validateNoDowngradeTransition(in, old, providerSpec)...)
}

// specApplied reports whether the provider has reconciled the Instance's spec:
// it has created the instance and has not held the spec back. A spec written
// while admission was unavailable, or held by a validation or reference
// error, never reached Milvus, so reverting it is no transition; the running
// instance is checked at reconcile time instead.
func specApplied(in *corev1alpha1.Instance) bool {
	if in.Status.Phase == "" || in.Status.Phase == corev1alpha1.InstancePhasePending ||
		in.Status.ObservedGeneration < in.Generation {
		return false
	}
	ready := meta.FindStatusCondition(in.Status.Conditions, corev1alpha1.ConditionReady)
	return ready == nil ||
		(ready.Reason != corev1alpha1.ReasonValidationFailed && ready.Reason != corev1alpha1.ReasonInvalidReference)
}

// hasBundledVolumes reports whether the instance runs a bundled dependency
// with volumes, which its dependencies storageClassName applies to.
func hasBundledVolumes(in *corev1alpha1.Instance, topologyType string) bool {
	if etcd := etcdDependencyParam(in, topologyType); etcd == nil || !etcd.External {
		return true
	}
	if objectStoreMode(storageDependencyParam(in, topologyType)) == "bundled" {
		return true
	}
	if topologyType != "cluster" {
		return false
	}
	deps := clusterDependencyParams(in)
	return deps.MessageStreamType == dependencies.MessageStreamPulsar && (deps.Pulsar == nil || !deps.Pulsar.External)
}

// dependencyStorageClassParam is the StorageClass the instance asks for its
// bundled dependency volumes.
func dependencyStorageClassParam(in *corev1alpha1.Instance, topologyType string) string {
	if topologyType == "cluster" {
		return clusterDependencyParams(in).StorageClassName
	}
	var params standalone.StandaloneTopologyParameters
	if tryDecodeTopologyParameters(in, &params) && params.Dependencies != nil {
		return params.Dependencies.StorageClassName
	}
	return ""
}

// dataLocationTransition rejects moving the objects or the metadata of a
// created instance: Milvus would start empty and orphan what it wrote before.
// Moving between a storage location and the same external store is left to
// validateDataLocationUnchanged, which sees where both resolve.
func dataLocationTransition(old, in *corev1alpha1.Instance, topologyType string) field.ErrorList {
	var errs field.ErrorList
	before, after := storageDependencyParam(old, topologyType), storageDependencyParam(in, topologyType)
	beforeMode, afterMode := objectStoreMode(before), objectStoreMode(after)
	path := dependenciesPath.Child("storage")
	switch {
	case (beforeMode == "bundled") != (afterMode == "bundled"):
		errs = append(errs, field.Forbidden(path, "the object storage cannot move between the bundled MinIO and an external store"))
	case beforeMode == afterMode && beforeMode == "external" &&
		(before.Endpoint != after.Endpoint || before.Bucket != after.Bucket ||
			defaulted(before.RootPath, defaultMinioRootPath) != defaulted(after.RootPath, defaultMinioRootPath)):
		errs = append(errs, field.Forbidden(path, "the external object storage endpoint, bucket and rootPath cannot change"))
	case beforeMode == afterMode && beforeMode == "location":
		locationRoot := defaultStorageLocationRootPath(old.Namespace, old.Name)
		if before.BackupStorageName != after.BackupStorageName ||
			defaulted(before.RootPath, locationRoot) != defaulted(after.RootPath, locationRoot) {
			errs = append(errs, field.Forbidden(path, "the storage location and its rootPath cannot change"))
		}
	}
	beforeEtcd := externalEtcdRootPath(etcdDependencyParam(old, topologyType), old.Name)
	afterEtcd := externalEtcdRootPath(etcdDependencyParam(in, topologyType), old.Name)
	if beforeEtcd != afterEtcd {
		errs = append(errs, field.Forbidden(dependenciesPath.Child("etcd", "rootPath"),
			fmt.Sprintf("etcd rootPath cannot be changed from %q to %q", beforeEtcd, afterEtcd)))
	}
	return errs
}

// objectStoreMode says where the objects live: the bundled MinIO, a storage
// location or an external store.
func objectStoreMode(storage *dependencies.Storage) string {
	switch {
	case storage == nil:
		return "bundled"
	case storage.BackupStorageName != "":
		return "location"
	case storage.External:
		return "external"
	default:
		return "bundled"
	}
}

// externalEtcdRootPath is the prefix the instance's metadata is kept under in
// etcd, as externalEtcdConfig renders it: the instance name unless an external
// etcd sets one.
func externalEtcdRootPath(etcd *dependencies.Etcd, instanceName string) string {
	if etcd == nil || !etcd.External {
		return instanceName
	}
	return defaulted(etcd.RootPath, instanceName)
}

// defaulted is value, or def when value is empty.
func defaulted(value, def string) string {
	if value == "" {
		return def
	}
	return value
}

// validateSpec checks the rules about the Instance alone, each error on the
// field it is about.
func validateSpec(in *corev1alpha1.Instance) field.ErrorList {
	topologyType := instanceTopology(in)
	if topologyType != "standalone" && topologyType != "cluster" {
		return field.ErrorList{invalid(topologyTypePath,
			fmt.Sprintf("unsupported topology %q; expected standalone or cluster", in.Spec.Topology.Type))}
	}

	var errs field.ErrorList
	errs = appendErr(errs, componentsPath, validateComponentsForTopology(in.Spec.Components, topologyType))
	errs = appendErr(errs, componentsPath, validateTopologyRules(in.Spec.Components, topologyType))
	errs = appendErr(errs, dependenciesPath, validateDependencies(in, topologyType))
	errs = appendErr(errs, componentsPath, validateServiceExposure(in.Spec.Components, topologyType))
	if _, err := milvusEngineConfig(in); err != nil {
		errs = appendErr(errs, componentsPath, err)
	}
	errs = append(errs, validateComponentParameters(in)...)
	errs = appendErr(errs, parametersPath.Child("tls"), validateTLS(in))
	if topologyType == "cluster" {
		errs = appendErr(errs, componentsPath.Key(common.ComponentMixCoord).Child("replicas"), validateMixCoordStandby(in))
	}
	return errs
}

// validateMixCoordStandby rejects several MixCoord replicas when the user's
// configuration disables active-standby: they would all try to be active.
func validateMixCoordStandby(in *corev1alpha1.Instance) error {
	replicas := in.Spec.Components[common.ComponentMixCoord].Replicas
	if replicas == nil || *replicas <= 1 {
		return nil
	}
	userConfig, err := milvusEngineConfig(in)
	if err != nil {
		return err
	}
	for _, section := range coordinatorConfigSections {
		settings, _ := userConfig[section].(map[string]any)
		if enabled, set := settings["enableActiveStandby"].(bool); set && !enabled {
			return fmt.Errorf("mixCoord replicas > 1 requires active-standby; remove %s.enableActiveStandby: false from the configuration", section)
		}
	}
	return nil
}

// validateTopologyUnchanged rejects switching an existing instance between
// standalone and cluster: the operator cannot migrate a running deployment.
func validateTopologyUnchanged(existing *milvusapi.Milvus, topologyType string) error {
	if current := string(existing.Spec.Mode); current != "" && current != topologyType {
		return field.Forbidden(topologyTypePath, fmt.Sprintf("topology cannot be changed from %s to %s", current, topologyType))
	}
	return nil
}

// validateServiceExposure rejects unsupported service types on the component
// that owns the client-facing service (standalone or proxy).
func validateServiceExposure(components map[string]corev1alpha1.ComponentSpec, topologyType string) error {
	name := exposedComponentName(topologyType)
	component := components[name]
	if component.Service == nil {
		return nil
	}
	switch component.Service.ServiceType {
	case "",
		corev1.ServiceTypeClusterIP,
		corev1.ServiceTypeLoadBalancer,
		corev1.ServiceTypeNodePort:
		return nil
	default:
		return fieldErrorf(componentsPath.Key(name).Child("service", "serviceType"),
			"component %q service.serviceType must be one of ClusterIP, LoadBalancer or NodePort", name)
	}
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
			return fieldErrorf(componentsPath.Key(name), "component %q is not valid for %s topology", name, topologyType)
		}
	}

	for _, name := range allowed {
		component, ok := components[name]
		if !ok {
			continue
		}
		if component.Replicas != nil && *component.Replicas < 1 {
			return fieldErrorf(componentsPath.Key(name).Child("replicas"), "component %q replicas must be >= 1", name)
		}
		// Milvus components are stateless; their data lives in the object storage.
		if component.Storage != nil {
			return fieldErrorf(componentsPath.Key(name).Child("storage"), "component %q does not take storage; size the object storage with spec.topology.parameters.dependencies.storage.persistence.size", name)
		}
		if err := validateComponentResources(name, component); err != nil {
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

	path := componentsPath.Key(name).Child("resources")
	limits := component.Resources.Limits
	if cpu, ok := limits[corev1.ResourceCPU]; ok && cpu.Cmp(minComponentCPU) < 0 {
		return fieldErrorf(path.Child("limits", "cpu"), "component %q resources.limits.cpu must be >= %s", name, minComponentCPU.String())
	}
	if mem, ok := limits[corev1.ResourceMemory]; ok && mem.Cmp(minComponentMemory) < 0 {
		return fieldErrorf(path.Child("limits", "memory"), "component %q resources.limits.memory must be >= %s", name, minComponentMemory.String())
	}

	requests := component.Resources.Requests
	if err := validateRequestNotAboveLimit(name, corev1.ResourceCPU, requests, limits); err != nil {
		return invalid(path.Child("requests", "cpu"), err.Error())
	}
	if err := validateRequestNotAboveLimit(name, corev1.ResourceMemory, requests, limits); err != nil {
		return invalid(path.Child("requests", "memory"), err.Error())
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

// validateTopologyRules enforces topology-specific composition rules, such as
// requiring at least one replica for every coordinator in cluster mode.
func validateTopologyRules(components map[string]corev1alpha1.ComponentSpec, topologyType string) error {
	if topologyType != "cluster" {
		// Standalone embeds the coordinators; a second pod would compete with it.
		if replicas := components[common.ComponentStandalone].Replicas; replicas != nil && *replicas > 1 {
			return fieldErrorf(componentsPath.Key(common.ComponentStandalone).Child("replicas"),
				"standalone runs a single replica; use the cluster topology to scale out")
		}
		return nil
	}
	for _, name := range clusterCoordinators {
		component, ok := components[name]
		if !ok {
			continue
		}
		if component.Replicas != nil && *component.Replicas < 1 {
			return fieldErrorf(componentsPath.Key(name).Child("replicas"), "cluster mode requires at least 1 replica for coordinator %q", name)
		}
	}
	return nil
}

// validateStorageNotDecreased guards against shrinking the bundled object
// storage: it compares the requested size against the size applied to the
// Milvus CR, as storage may only grow or stay the same.
func validateStorageNotDecreased(existing *milvusapi.Milvus, in *corev1alpha1.Instance, topologyType string) error {
	requested, err := resource.ParseQuantity(requestedStorageSize(in, topologyType))
	if err != nil {
		return nil
	}
	current, err := resource.ParseQuantity(currentStorageSize(existing))
	if err != nil {
		return nil
	}
	if requested.Cmp(current) < 0 {
		return field.Forbidden(dependenciesPath.Child("storage", "persistence", "size"),
			fmt.Sprintf("storage size cannot be decreased from %s to %s", current.String(), requested.String()))
	}
	return nil
}

// requestedStorageSize returns the object-storage PVC size the spec would apply,
// mirroring buildStorage: the explicit storage dependency persistence size, or
// the default. External storage has no bundled PVC.
func requestedStorageSize(in *corev1alpha1.Instance, topologyType string) string {
	storageParam := storageDependencyParam(in, topologyType)
	if storageParam != nil && (storageParam.External || storageParam.BackupStorageName != "") {
		return ""
	}
	if storageParam != nil && storageParam.Persistence != nil && storageParam.Persistence.Size != "" {
		return storageParam.Persistence.Size
	}
	return defaultStoragePersistence
}

// storageDependencyParam decodes the storage dependency parameter for the
// topology, returning nil when unset.
func storageDependencyParam(in *corev1alpha1.Instance, topologyType string) *dependencies.Storage {
	if topologyType == "cluster" {
		var params cluster.ClusterTopologyParameters
		if tryDecodeTopologyParameters(in, &params) && params.Dependencies != nil {
			return params.Dependencies.Storage
		}
		return nil
	}
	var params standalone.StandaloneTopologyParameters
	if tryDecodeTopologyParameters(in, &params) && params.Dependencies != nil {
		return params.Dependencies.Storage
	}
	return nil
}

// etcdDependencyParam decodes the etcd dependency parameter for the topology,
// returning nil when unset.
func etcdDependencyParam(in *corev1alpha1.Instance, topologyType string) *dependencies.Etcd {
	if topologyType == "cluster" {
		return clusterDependencyParams(in).Etcd
	}
	var params standalone.StandaloneTopologyParameters
	if tryDecodeTopologyParameters(in, &params) && params.Dependencies != nil {
		return params.Dependencies.Etcd
	}
	return nil
}

// validateDataLocationUnchanged rejects moving an existing instance's metadata
// or objects (prefix, object store or bucket): Milvus would start empty and
// orphan what it wrote before.
func validateDataLocationUnchanged(existing *milvusapi.Milvus, desired milvusapi.MilvusSpec, instanceName string) field.ErrorList {
	if existing.Spec.Mode == "" {
		return nil
	}
	var errs field.ErrorList
	if current, requested := objectStore(existing.Spec), objectStore(desired); current != requested {
		errs = append(errs, field.Forbidden(dependenciesPath.Child("storage"),
			fmt.Sprintf("object storage cannot be changed from %s to %s", current, requested)))
	}
	for _, prefix := range []struct {
		section, defaultValue string
		path                  *field.Path
	}{
		{section: "etcd", defaultValue: instanceName, path: dependenciesPath.Child("etcd", "rootPath")},
		{section: "minio", defaultValue: defaultMinioRootPath, path: dependenciesPath.Child("storage", "rootPath")},
	} {
		current := rootPathOrDefault(existing.Spec.Conf, prefix.section, prefix.defaultValue)
		requested := rootPathOrDefault(desired.Conf, prefix.section, prefix.defaultValue)
		if current != requested {
			errs = append(errs, field.Forbidden(prefix.path,
				fmt.Sprintf("%s rootPath cannot be changed from %q to %q", prefix.section, current, requested)))
		}
	}
	return errs
}

// defaultMinioRootPath is the operator's object prefix when none is set.
const defaultMinioRootPath = "files"

// validateStorageClassUnchanged rejects moving the bundled dependency volumes
// to another StorageClass: a StatefulSet cannot change its volume claims.
func validateStorageClassUnchanged(existing *milvusapi.Milvus, desired milvusapi.MilvusSpec) error {
	if existing.Spec.Dep == nil {
		return nil
	}
	if current, requested := dependencyStorageClass(existing.Spec.Dep), dependencyStorageClass(desired.Dep); current != requested {
		return field.Forbidden(dependenciesPath.Child("storageClassName"),
			fmt.Sprintf("dependencies storageClassName cannot be changed from %q to %q", current, requested))
	}
	return nil
}

// dependencyStorageClass is the StorageClass the bundled etcd and MinIO
// volumes are rendered with, mirroring buildEtcd and buildStorage.
func dependencyStorageClass(dep *milvusapi.MilvusDependencies) string {
	if dep == nil {
		return ""
	}
	for _, inCluster := range []*milvusapi.InClusterConfig{dep.Etcd.InCluster, dep.Storage.InCluster} {
		if inCluster == nil {
			continue
		}
		persistence, _ := inCluster.Values["persistence"].(map[string]any)
		if storageClass, _ := persistence["storageClass"].(string); storageClass != "" {
			return storageClass
		}
	}
	return ""
}

// objectStore names where an instance keeps its objects: the bundled MinIO or
// an external endpoint and bucket.
func objectStore(spec milvusapi.MilvusSpec) string {
	if spec.Dep == nil || !spec.Dep.Storage.External {
		return "the bundled MinIO"
	}
	minio, _ := spec.Conf["minio"].(map[string]any)
	bucket, _ := minio["bucketName"].(string)
	return fmt.Sprintf("%s/%s", spec.Dep.Storage.Endpoint, bucket)
}

func rootPathOrDefault(config milvusapi.Values, section, defaultValue string) string {
	settings, _ := config[section].(map[string]any)
	if rootPath, _ := settings["rootPath"].(string); rootPath != "" {
		return rootPath
	}
	return defaultValue
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
func validateDependencies(in *corev1alpha1.Instance, topologyType string) error {
	var etcd *dependencies.Etcd
	var pulsar *dependencies.Pulsar
	var storage *dependencies.Storage

	if topologyType == "cluster" {
		var params cluster.ClusterTopologyParameters
		if err := decodeTopologyParametersIfPresent(in, &params); err != nil {
			return err
		}
		if params.Dependencies != nil {
			etcd = params.Dependencies.Etcd
			storage = params.Dependencies.Storage
			switch params.Dependencies.MessageStreamType {
			case dependencies.MessageStreamPulsar:
				pulsar = params.Dependencies.Pulsar
			case "", dependencies.MessageStreamWoodpecker:
				// Unset keeps the running stream; validatePulsarInUse checks the
				// Pulsar settings of an instance that runs Pulsar without naming it.
			default:
				return fieldErrorf(dependenciesPath.Child("messageStreamType"), "messageStreamType must be %s or %s",
					dependencies.MessageStreamWoodpecker, dependencies.MessageStreamPulsar)
			}
		}
	} else {
		var params standalone.StandaloneTopologyParameters
		if err := decodeTopologyParametersIfPresent(in, &params); err != nil {
			return err
		}
		if params.Dependencies != nil {
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

// validateMessageStreamUnchanged rejects switching an existing instance's
// write-ahead log, which would lose it.
func validateMessageStreamUnchanged(existing *milvusapi.Milvus, in *corev1alpha1.Instance) error {
	requested := clusterDependencyParams(in).MessageStreamType
	if requested == "" {
		return nil
	}
	if current := appliedMessageStreamType(existing); current != "" && current != requested {
		return field.Forbidden(dependenciesPath.Child("messageStreamType"),
			fmt.Sprintf("message stream cannot be changed from %s to %s; set messageStreamType to %s", current, requested, current))
	}
	return nil
}

// validatePulsarInUse checks the Pulsar settings of an instance created
// before the stream was selectable, which runs Pulsar without naming it.
func validatePulsarInUse(existing *milvusapi.Milvus, in *corev1alpha1.Instance) error {
	deps := clusterDependencyParams(in)
	if deps.MessageStreamType != "" || appliedMessageStreamType(existing) != dependencies.MessageStreamPulsar {
		return nil
	}
	return validatePulsarDependency(deps.Pulsar)
}

// decodeTopologyParametersIfPresent decodes the instance's topology parameters
// when they are set, surfacing malformed input as an error instead of silently
// falling back to defaults (which would deploy a spec that ignores the request).
func decodeTopologyParametersIfPresent(in *corev1alpha1.Instance, target any) error {
	if err := decodeRaw(in.GetTopologyParameters(), target); err != nil {
		return invalid(topologyParametersPath, fmt.Sprintf("invalid topology parameters: %v", err))
	}
	return nil
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
	if err := validateDependencyResources("etcd", etcd.Resources); err != nil {
		return err
	}
	return validatePersistence("etcd.persistence", etcd.Persistence)
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
	if err := validatePulsarComponent("pulsar.broker", pulsar.Broker); err != nil {
		return err
	}
	if err := validatePulsarComponent("pulsar.proxy", pulsar.Proxy); err != nil {
		return err
	}
	if bk := pulsar.BookKeeper; bk != nil {
		if err := validateDependencyReplicas("pulsar.bookkeeper", bk.Replicas); err != nil {
			return err
		}
		if err := validateDependencyResources("pulsar.bookkeeper", bk.Resources); err != nil {
			return err
		}
		if err := validatePersistence("pulsar.bookkeeper.journal", bk.Journal); err != nil {
			return err
		}
		if err := validatePersistence("pulsar.bookkeeper.ledgers", bk.Ledgers); err != nil {
			return err
		}
	}
	if zk := pulsar.ZooKeeper; zk != nil {
		if err := validateDependencyReplicas("pulsar.zookeeper", zk.Replicas); err != nil {
			return err
		}
		if err := validateDependencyResources("pulsar.zookeeper", zk.Resources); err != nil {
			return err
		}
		if err := validatePersistence("pulsar.zookeeper.data", zk.Data); err != nil {
			return err
		}
	}
	return nil
}

// validatePulsarComponent validates a stateless Pulsar sub-component (broker or
// proxy).
func validatePulsarComponent(name string, component *dependencies.PulsarComponent) error {
	if component == nil {
		return nil
	}
	if err := validateDependencyReplicas(name, component.Replicas); err != nil {
		return err
	}
	return validateDependencyResources(name, component.Resources)
}

func validateStorageDependency(storage *dependencies.Storage) error {
	if storage == nil {
		return nil
	}
	if storage.BackupStorageName != "" {
		if storage.External || storage.Endpoint != "" || storage.Type != "" || storage.CredentialsSecret != "" ||
			storage.UseIAM || storage.Region != "" || storage.Bucket != "" || storage.UseSSL {
			return fmt.Errorf("storage.backupStorageName takes the endpoint, bucket, region and credentials from the storage location; " +
				"remove storage.external, endpoint, type, credentialsSecret, useIAM, region, bucket and useSSL")
		}
		return nil
	}
	if storage.External {
		if storage.Endpoint == "" {
			return fmt.Errorf("storage.endpoint is required when storage.external is true")
		}
		if storage.CredentialsSecret == "" && !storage.UseIAM {
			return fmt.Errorf("storage.credentialsSecret is required when storage.external is true, unless storage.useIAM is set")
		}
		switch storage.Type {
		case "", "MinIO", "S3", "Azure":
			return nil
		default:
			return fmt.Errorf("storage.type must be one of MinIO, S3 or Azure")
		}
	}
	if err := validateDependencyReplicas("storage", storage.Replicas); err != nil {
		return err
	}
	if err := validateDependencyResources("storage", storage.Resources); err != nil {
		return err
	}
	return validatePersistence("storage.persistence", storage.Persistence)
}

// validatePersistence enforces the minimum PVC size when a dependency
// persistence size is specified.
func validatePersistence(name string, persistence *dependencies.Persistence) error {
	if persistence == nil || persistence.Size == "" {
		return nil
	}
	size, err := resource.ParseQuantity(persistence.Size)
	if err != nil {
		return fmt.Errorf("%s.size %q is invalid: %w", name, persistence.Size, err)
	}
	if size.Cmp(minStorageSize) < 0 {
		return fmt.Errorf("%s.size must be >= %s", name, minStorageSize.String())
	}
	return nil
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
		cpu, err := resource.ParseQuantity(string(list.CPU))
		if err != nil {
			return nil, fmt.Errorf("%s.resources.%s.cpu %q is invalid: %w", name, kind, list.CPU, err)
		}
		result[corev1.ResourceCPU] = cpu
	}
	if list.Memory != "" {
		mem, err := resource.ParseQuantity(string(list.Memory))
		if err != nil {
			return nil, fmt.Errorf("%s.resources.%s.memory %q is invalid: %w", name, kind, list.Memory, err)
		}
		result[corev1.ResourceMemory] = mem
	}
	return result, nil
}
