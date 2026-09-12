package provider

import (
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"k8s.io/utils/ptr"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/cluster"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// Per-topology defaults keep bundled dependencies small so they don't
// over-provision on modest clusters. Users override any field via topology
// parameters; unset fields fall back to these values.
var (
	defaultStandaloneEtcdReplicas int32 = 1
	defaultClusterEtcdReplicas    int32 = 3

	defaultEtcdResources = &dependencies.Resources{
		Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"},
	}
	defaultStorageResources = &dependencies.Resources{
		Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"},
	}
	defaultStorageReplicas int32 = 1

	defaultPulsarBroker = dependencies.PulsarComponent{
		Replicas:  ptr.To(int32(1)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "200m", Memory: "512Mi"}},
	}
	defaultPulsarBookKeeper = dependencies.PulsarComponent{
		Replicas:  ptr.To(int32(2)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "200m", Memory: "512Mi"}},
	}
	defaultPulsarZooKeeper = dependencies.PulsarComponent{
		Replicas:  ptr.To(int32(1)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"}},
	}
	defaultPulsarProxy = dependencies.PulsarComponent{
		Replicas:  ptr.To(int32(1)),
		Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "100m", Memory: "256Mi"}},
	}
)

// buildDependencies renders the Milvus dependency spec (etcd, Pulsar, MinIO)
// from the topology parameters, applying per-topology defaults and honouring
// external-dependency overrides. persistenceSize, when set, is the object
// storage PVC size derived from the data-bearing component.
func buildDependencies(c *controller.Context, topologyType, persistenceSize string) *milvusapi.MilvusDependencies {
	var etcdParam *dependencies.Etcd
	var pulsarParam *dependencies.Pulsar
	var storageParam *dependencies.Storage

	if topologyType == "cluster" {
		var params cluster.ClusterTopologyParameters
		if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
			etcdParam = params.Dependencies.Etcd
			pulsarParam = params.Dependencies.Pulsar
			storageParam = params.Dependencies.Storage
		}
	} else {
		var params standalone.StandaloneTopologyParameters
		if c.TryDecodeTopologyParameters(&params) && params.Dependencies != nil {
			etcdParam = params.Dependencies.Etcd
			storageParam = params.Dependencies.Storage
		}
	}

	dep := &milvusapi.MilvusDependencies{
		Etcd:    buildEtcd(etcdParam, topologyType),
		Storage: buildStorage(storageParam, persistenceSize),
	}
	if topologyType == "cluster" {
		dep.Pulsar = buildPulsar(pulsarParam)
	}
	return dep
}

// buildEtcd renders the etcd dependency, switching to external endpoints when
// requested, otherwise sizing the bundled cluster.
func buildEtcd(param *dependencies.Etcd, topologyType string) milvusapi.MilvusEtcd {
	if param != nil && param.External {
		return milvusapi.MilvusEtcd{External: true, Endpoints: param.Endpoints}
	}

	defaultReplicas := defaultStandaloneEtcdReplicas
	if topologyType == "cluster" {
		defaultReplicas = defaultClusterEtcdReplicas
	}
	replicas := defaultReplicas
	resources := defaultEtcdResources
	if param != nil {
		if param.Replicas != nil {
			replicas = *param.Replicas
		}
		resources = mergeResources(param.Resources, defaultEtcdResources)
	}

	values := milvusapi.Values{"replicaCount": int(replicas)}
	if res := resourcesToValues(resources); res != nil {
		values["resources"] = res
	}
	return milvusapi.MilvusEtcd{InCluster: &milvusapi.InClusterConfig{Values: values}}
}

// buildStorage renders the MinIO object-storage dependency. External storage
// bypasses sizing; bundled storage carries the PVC size plus resource and
// replica tuning (a replica count above one enables MinIO distributed mode).
func buildStorage(param *dependencies.Storage, persistenceSize string) milvusapi.MilvusStorage {
	if param != nil && param.External {
		return milvusapi.MilvusStorage{External: true, Endpoint: param.Endpoint}
	}

	replicas := defaultStorageReplicas
	resources := defaultStorageResources
	if param != nil {
		if param.Replicas != nil {
			replicas = *param.Replicas
		}
		resources = mergeResources(param.Resources, defaultStorageResources)
	}

	values := milvusapi.Values{
		"mode":     minioMode(replicas),
		"replicas": int(replicas),
	}
	if res := resourcesToValues(resources); res != nil {
		values["resources"] = res
	}
	if persistenceSize != "" {
		values["persistence"] = map[string]any{"size": persistenceSize}
	}
	return milvusapi.MilvusStorage{InCluster: &milvusapi.InClusterConfig{Values: values}}
}

// buildPulsar renders the Pulsar message-stream dependency, sizing each
// sub-component (broker, bookkeeper, zookeeper, proxy) or switching to an
// external endpoint.
func buildPulsar(param *dependencies.Pulsar) milvusapi.MilvusPulsar {
	if param != nil && param.External {
		return milvusapi.MilvusPulsar{External: true, Endpoint: param.Endpoint}
	}

	broker := defaultPulsarBroker
	bookkeeper := defaultPulsarBookKeeper
	zookeeper := defaultPulsarZooKeeper
	proxy := defaultPulsarProxy
	if param != nil {
		broker = mergePulsarComponent(param.Broker, defaultPulsarBroker)
		bookkeeper = mergePulsarComponent(param.BookKeeper, defaultPulsarBookKeeper)
		zookeeper = mergePulsarComponent(param.ZooKeeper, defaultPulsarZooKeeper)
		proxy = mergePulsarComponent(param.Proxy, defaultPulsarProxy)
	}

	values := milvusapi.Values{
		"broker":     pulsarComponentValues(broker),
		"bookkeeper": pulsarComponentValues(bookkeeper),
		"zookeeper":  pulsarComponentValues(zookeeper),
		"proxy":      pulsarComponentValues(proxy),
	}
	return milvusapi.MilvusPulsar{InCluster: &milvusapi.InClusterConfig{Values: values}}
}

// minioMode maps a replica count to the MinIO deployment mode.
func minioMode(replicas int32) string {
	if replicas > 1 {
		return "distributed"
	}
	return "standalone"
}

// mergePulsarComponent overlays user-provided replica/resource settings on top
// of a default sub-component.
func mergePulsarComponent(param *dependencies.PulsarComponent, def dependencies.PulsarComponent) dependencies.PulsarComponent {
	if param == nil {
		return def
	}
	merged := def
	if param.Replicas != nil {
		merged.Replicas = param.Replicas
	}
	merged.Resources = mergeResources(param.Resources, def.Resources)
	return merged
}

// pulsarComponentValues renders a Pulsar sub-component into Helm values.
func pulsarComponentValues(component dependencies.PulsarComponent) map[string]any {
	values := map[string]any{}
	if component.Replicas != nil {
		values["replicaCount"] = int(*component.Replicas)
	}
	if res := resourcesToValues(component.Resources); res != nil {
		values["resources"] = res
	}
	return values
}

// mergeResources returns param when it specifies a resource list, falling back
// to def for each of requests and limits independently.
func mergeResources(param, def *dependencies.Resources) *dependencies.Resources {
	if param == nil {
		return def
	}
	merged := &dependencies.Resources{Requests: param.Requests, Limits: param.Limits}
	if def != nil {
		if merged.Requests == nil {
			merged.Requests = def.Requests
		}
		if merged.Limits == nil {
			merged.Limits = def.Limits
		}
	}
	return merged
}

// resourcesToValues renders resource requests/limits into a Helm values map,
// returning nil when nothing is set.
func resourcesToValues(resources *dependencies.Resources) map[string]any {
	if resources == nil {
		return nil
	}
	values := map[string]any{}
	if req := resourceListToValues(resources.Requests); req != nil {
		values["requests"] = req
	}
	if lim := resourceListToValues(resources.Limits); lim != nil {
		values["limits"] = lim
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

// resourceListToValues renders a CPU/memory pair into a Helm values map,
// returning nil when both are empty.
func resourceListToValues(list *dependencies.ResourceList) map[string]any {
	if list == nil {
		return nil
	}
	values := map[string]any{}
	if list.CPU != "" {
		values["cpu"] = list.CPU
	}
	if list.Memory != "" {
		values["memory"] = list.Memory
	}
	if len(values) == 0 {
		return nil
	}
	return values
}
