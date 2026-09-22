package provider

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/cluster"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func topologyParams(t *testing.T, v any) *runtime.RawExtension {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: raw}
}

func TestBuildDependenciesStandaloneDefaults(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	require.NotNil(t, spec.Dep)

	require.NotNil(t, spec.Dep.Etcd.InCluster)
	assert.Equal(t, 1, spec.Dep.Etcd.InCluster.Values["replicaCount"])

	require.NotNil(t, spec.Dep.Storage.InCluster)
	assert.Equal(t, "standalone", spec.Dep.Storage.InCluster.Values["mode"])
	assert.Equal(t, map[string]any{"size": "10Gi"}, spec.Dep.Storage.InCluster.Values["persistence"])
	assert.Equal(t, map[string]any{"repository": "pgsty/silo", "tag": "RELEASE.2026-09-03T13-18-01Z"}, spec.Dep.Storage.InCluster.Values["image"])
	assert.Equal(t, map[string]any{"repository": "pgsty/mc", "tag": "RELEASE.2026-09-13T00-00-00Z"}, spec.Dep.Storage.InCluster.Values["mcImage"])

	// Standalone uses embedded rocksmq: no Pulsar dependency is configured.
	assert.Nil(t, spec.Dep.Pulsar.InCluster)
}

func TestBuildDependenciesClusterDefaults(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	require.NotNil(t, spec.Dep)

	require.NotNil(t, spec.Dep.Etcd.InCluster)
	assert.Equal(t, 3, spec.Dep.Etcd.InCluster.Values["replicaCount"])

	require.NotNil(t, spec.Dep.Pulsar.InCluster)
	broker, ok := spec.Dep.Pulsar.InCluster.Values["broker"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 1, broker["replicaCount"])
	bookkeeper, ok := spec.Dep.Pulsar.InCluster.Values["bookkeeper"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 2, bookkeeper["replicaCount"])

	require.NotNil(t, spec.Dep.Storage.InCluster)
	assert.Equal(t, map[string]any{"size": "10Gi"}, spec.Dep.Storage.InCluster.Values["persistence"])
}

func TestBuildDependenciesExternal(t *testing.T) {
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{
			Etcd:    &dependencies.Etcd{External: true, Endpoints: []string{"etcd-a:2379", "etcd-b:2379"}},
			Pulsar:  &dependencies.Pulsar{External: true, Endpoint: "pulsar://broker:6650"},
			Storage: &dependencies.Storage{External: true, Endpoint: "s3.amazonaws.com"},
		},
	}
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentDataNode: {Storage: storage(t, "50Gi")},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	require.NotNil(t, spec.Dep)

	assert.True(t, spec.Dep.Etcd.External)
	assert.Equal(t, []string{"etcd-a:2379", "etcd-b:2379"}, spec.Dep.Etcd.Endpoints)
	assert.Nil(t, spec.Dep.Etcd.InCluster)

	assert.True(t, spec.Dep.Pulsar.External)
	assert.Equal(t, "pulsar://broker:6650", spec.Dep.Pulsar.Endpoint)
	assert.Nil(t, spec.Dep.Pulsar.InCluster)

	assert.True(t, spec.Dep.Storage.External)
	assert.Equal(t, "s3.amazonaws.com", spec.Dep.Storage.Endpoint)
	assert.Nil(t, spec.Dep.Storage.InCluster)
}

func TestBuildDependenciesUserOverrides(t *testing.T) {
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{
			Etcd: &dependencies.Etcd{
				Replicas:  ptr.To(int32(5)),
				Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "250m", Memory: "1Gi"}},
			},
			Storage: &dependencies.Storage{Replicas: ptr.To(int32(4))},
			Pulsar: &dependencies.Pulsar{
				Broker: &dependencies.PulsarComponent{Replicas: ptr.To(int32(3))},
			},
		},
	}
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentDataNode: {Storage: storage(t, "50Gi")},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	assert.Equal(t, 5, spec.Dep.Etcd.InCluster.Values["replicaCount"])
	etcdRes, ok := spec.Dep.Etcd.InCluster.Values["resources"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{"cpu": "250m", "memory": "1Gi"}, etcdRes["requests"])

	// Replicas > 1 switches MinIO to distributed mode.
	assert.Equal(t, "distributed", spec.Dep.Storage.InCluster.Values["mode"])
	assert.Equal(t, 4, spec.Dep.Storage.InCluster.Values["replicas"])

	broker := spec.Dep.Pulsar.InCluster.Values["broker"].(map[string]any)
	assert.Equal(t, 3, broker["replicaCount"])
	// Unset broker resources fall back to the default request.
	brokerRes := broker["resources"].(map[string]any)
	assert.Equal(t, map[string]any{"cpu": "200m", "memory": "512Mi"}, brokerRes["requests"])
}

func TestBuildDependenciesPersistenceDefaults(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	// etcd data PVC falls back to the modest provider default.
	assert.Equal(t, map[string]any{"size": "10Gi"}, spec.Dep.Etcd.InCluster.Values["persistence"])

	// MinIO falls back to the modest provider default when unset.
	assert.Equal(t, map[string]any{"size": "10Gi"}, spec.Dep.Storage.InCluster.Values["persistence"])

	bookkeeper := spec.Dep.Pulsar.InCluster.Values["bookkeeper"].(map[string]any)
	assert.Equal(t, map[string]any{
		"journal": map[string]any{"size": "5Gi"},
		"ledgers": map[string]any{"size": "10Gi"},
	}, bookkeeper["volumes"])

	zookeeper := spec.Dep.Pulsar.InCluster.Values["zookeeper"].(map[string]any)
	assert.Equal(t, map[string]any{"data": map[string]any{"size": "5Gi"}}, zookeeper["volumes"])
}

func TestBuildDependenciesPersistenceOverrides(t *testing.T) {
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{
			Etcd:    &dependencies.Etcd{Persistence: &dependencies.Persistence{Size: "20Gi"}},
			Storage: &dependencies.Storage{Persistence: &dependencies.Persistence{Size: "100Gi"}},
			Pulsar: &dependencies.Pulsar{
				BookKeeper: &dependencies.PulsarBookKeeper{
					Journal: &dependencies.Persistence{Size: "8Gi"},
					Ledgers: &dependencies.Persistence{Size: "16Gi"},
				},
				ZooKeeper: &dependencies.PulsarZooKeeper{
					Data: &dependencies.Persistence{Size: "3Gi"},
				},
			},
		},
	}
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentDataNode: {Storage: storage(t, "50Gi")},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{"size": "20Gi"}, spec.Dep.Etcd.InCluster.Values["persistence"])
	// Explicit MinIO persistence overrides the component-derived size.
	assert.Equal(t, map[string]any{"size": "100Gi"}, spec.Dep.Storage.InCluster.Values["persistence"])

	bookkeeper := spec.Dep.Pulsar.InCluster.Values["bookkeeper"].(map[string]any)
	assert.Equal(t, map[string]any{
		"journal": map[string]any{"size": "8Gi"},
		"ledgers": map[string]any{"size": "16Gi"},
	}, bookkeeper["volumes"])

	zookeeper := spec.Dep.Pulsar.InCluster.Values["zookeeper"].(map[string]any)
	assert.Equal(t, map[string]any{"data": map[string]any{"size": "3Gi"}}, zookeeper["volumes"])
}

func TestBuildDependenciesStorageDefaultPersistence(t *testing.T) {
	// Cluster with no data-bearing component storage falls back to the
	// predictable MinIO default rather than the chart's oversized value.
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"size": "10Gi"}, spec.Dep.Storage.InCluster.Values["persistence"])
}

func TestBuildDependenciesNumericResourceQuantities(t *testing.T) {
	// The UI writes a unit-less CPU field as a bare JSON number (cpu: 0.1).
	// The dependency block must still decode and win over defaults, instead of
	// json.Unmarshal failing and the whole block silently reverting to defaults.
	raw := []byte(`{"dependencies":{"etcd":{"replicas":1,"resources":{"requests":{"cpu":0.1,"memory":"256Mi"}}}}}`)
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: &runtime.RawExtension{Raw: raw}},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentDataNode: {Storage: storage(t, "50Gi")},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	// 1, not the cluster default of 3.
	assert.Equal(t, 1, spec.Dep.Etcd.InCluster.Values["replicaCount"])
	etcdRes, ok := spec.Dep.Etcd.InCluster.Values["resources"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{"cpu": "0.1", "memory": "256Mi"}, etcdRes["requests"])
}

func TestBuildDependenciesDeletionPolicy(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	// Bundled dependencies are torn down (pods + PVCs) with the Instance, so
	// deleting an Instance leaves no orphaned StatefulSets or volumes.
	for name, inCluster := range map[string]*milvusapi.InClusterConfig{
		"etcd":    spec.Dep.Etcd.InCluster,
		"storage": spec.Dep.Storage.InCluster,
		"pulsar":  spec.Dep.Pulsar.InCluster,
	} {
		require.NotNil(t, inCluster, name)
		assert.Equal(t, "Delete", inCluster.DeletionPolicy, name)
		assert.True(t, inCluster.PVCDeletion, name)
	}
}

func TestValidateDependencies(t *testing.T) {
	tests := []struct {
		name    string
		spec    corev1alpha1.InstanceSpec
		wantErr string
	}{
		{
			name: "external etcd without endpoints",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{Etcd: &dependencies.Etcd{External: true}},
				})},
			},
			wantErr: "etcd.endpoints is required",
		},
		{
			name: "external pulsar without endpoint",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{Pulsar: &dependencies.Pulsar{External: true}},
				})},
			},
			wantErr: "pulsar.endpoint is required",
		},
		{
			name: "external storage without endpoint",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, standalone.StandaloneTopologyParameters{
					Dependencies: &standalone.StandaloneDependencies{Storage: &dependencies.Storage{External: true}},
				})},
			},
			wantErr: "storage.endpoint is required",
		},
		{
			name: "etcd replicas below one",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{Etcd: &dependencies.Etcd{Replicas: ptr.To(int32(0))}},
				})},
			},
			wantErr: "etcd.replicas must be >= 1",
		},
		{
			name: "pulsar broker request exceeds limit",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{Pulsar: &dependencies.Pulsar{
						Broker: &dependencies.PulsarComponent{Resources: &dependencies.Resources{
							Requests: &dependencies.ResourceList{CPU: "2"},
							Limits:   &dependencies.ResourceList{CPU: "1"},
						}},
					}},
				})},
			},
			wantErr: "pulsar.broker\" resources.requests.cpu",
		},
		{
			name: "invalid resource quantity",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, standalone.StandaloneTopologyParameters{
					Dependencies: &standalone.StandaloneDependencies{Etcd: &dependencies.Etcd{
						Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "abc"}},
					}},
				})},
			},
			wantErr: "is invalid",
		},
		{
			name: "valid external etcd",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{Etcd: &dependencies.Etcd{External: true, Endpoints: []string{"etcd:2379"}}},
				})},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, tt.spec)
			err := validateInstance(c)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
