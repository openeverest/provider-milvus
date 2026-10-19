package provider

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

var pulsarCluster = cluster.ClusterTopologyParameters{
	Dependencies: &cluster.ClusterDependencies{MessageStreamType: dependencies.MessageStreamPulsar},
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
	// The provider pins the pullable pgsty/silo image (operator default is gated).
	assert.Equal(t, map[string]any{"repository": "pgsty/silo", "tag": "RELEASE.2026-09-03T13-18-01Z"}, spec.Dep.Storage.InCluster.Values["image"])

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

	// New clusters keep the WAL in object storage: no Pulsar is deployed.
	assert.Equal(t, dependencies.MessageStreamWoodpecker, spec.Dep.MsgStreamType)
	assert.Nil(t, spec.Dep.Pulsar.InCluster)

	require.NotNil(t, spec.Dep.Storage.InCluster)
	assert.Equal(t, map[string]any{"size": "10Gi"}, spec.Dep.Storage.InCluster.Values["persistence"])
}

func TestBuildDependenciesClusterPulsar(t *testing.T) {
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{MessageStreamType: dependencies.MessageStreamPulsar},
	}
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	assert.Equal(t, dependencies.MessageStreamPulsar, spec.Dep.MsgStreamType)
	require.NotNil(t, spec.Dep.Pulsar.InCluster)
	broker, ok := spec.Dep.Pulsar.InCluster.Values["broker"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 1, broker["replicaCount"])
	bookkeeper, ok := spec.Dep.Pulsar.InCluster.Values["bookkeeper"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 2, bookkeeper["replicaCount"])
}

func TestBuildDependenciesWoodpeckerIgnoresPulsarParams(t *testing.T) {
	// The UI always sends Pulsar defaults; they must not deploy Pulsar.
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{
			MessageStreamType: dependencies.MessageStreamWoodpecker,
			Pulsar:            &dependencies.Pulsar{Broker: &dependencies.PulsarComponent{Replicas: ptr.To(int32(3))}},
		},
	}
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	assert.Equal(t, dependencies.MessageStreamWoodpecker, spec.Dep.MsgStreamType)
	assert.Nil(t, spec.Dep.Pulsar.InCluster)
}

func TestBuildDependenciesExistingInstanceKeepsMessageStream(t *testing.T) {
	tests := map[string]milvusapi.MilvusDependencies{
		"stream recorded on the CR": {MsgStreamType: dependencies.MessageStreamPulsar},
		"pre-selection CR with bundled Pulsar": {
			Pulsar: milvusapi.MilvusPulsar{InCluster: &milvusapi.InClusterConfig{}},
		},
	}
	for name, existingDeps := range tests {
		t.Run(name, func(t *testing.T) {
			existing := &milvusapi.Milvus{
				ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
				Spec:       milvusapi.MilvusSpec{Mode: milvusapi.MilvusModeCluster, Dep: &existingDeps},
			}
			c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			}, existing)
			spec, err := BuildMilvusSpec(c)
			require.NoError(t, err)
			assert.Equal(t, dependencies.MessageStreamPulsar, spec.Dep.MsgStreamType)
			assert.NotNil(t, spec.Dep.Pulsar.InCluster)
		})
	}
}

func TestValidateMessageStreamUnchanged(t *testing.T) {
	existing := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec: milvusapi.MilvusSpec{
			Mode: milvusapi.MilvusModeCluster,
			Dep:  &milvusapi.MilvusDependencies{MsgStreamType: dependencies.MessageStreamPulsar},
		},
	}
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{MessageStreamType: dependencies.MessageStreamWoodpecker},
	}
	c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
	}, existing)
	err := validateInstance(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "message stream cannot be changed from pulsar to woodpecker")
}

func TestBuildDependenciesExternal(t *testing.T) {
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{
			Etcd:              &dependencies.Etcd{External: true, Endpoints: []string{"etcd-a:2379", "etcd-b:2379"}},
			MessageStreamType: dependencies.MessageStreamPulsar,
			Pulsar:            &dependencies.Pulsar{External: true, Endpoint: "pulsar://broker:6650"},
			Storage: &dependencies.Storage{
				External: true, Endpoint: "s3.amazonaws.com", Type: "S3",
				CredentialsSecret: "s3-creds", Bucket: "vectors", UseSSL: true,
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
	require.NotNil(t, spec.Dep)

	assert.True(t, spec.Dep.Etcd.External)
	assert.Equal(t, []string{"etcd-a:2379", "etcd-b:2379"}, spec.Dep.Etcd.Endpoints)
	assert.Nil(t, spec.Dep.Etcd.InCluster)

	assert.True(t, spec.Dep.Pulsar.External)
	assert.Equal(t, "pulsar://broker:6650", spec.Dep.Pulsar.Endpoint)
	assert.Nil(t, spec.Dep.Pulsar.InCluster)

	assert.True(t, spec.Dep.Storage.External)
	assert.Equal(t, "s3.amazonaws.com", spec.Dep.Storage.Endpoint)
	assert.Equal(t, "S3", spec.Dep.Storage.Type)
	assert.Equal(t, "s3-creds", spec.Dep.Storage.SecretRef)
	assert.Nil(t, spec.Dep.Storage.InCluster)
	assert.Equal(t, map[string]any{"bucketName": "vectors", "useSSL": true}, spec.Conf["minio"])
}

func TestBuildDependenciesExternalS3WithIAM(t *testing.T) {
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{
			Etcd: &dependencies.Etcd{External: true, Endpoints: []string{"etcd:2379"}, RootPath: "vectors-prod"},
			Storage: &dependencies.Storage{
				External: true, Endpoint: "s3.us-east-1.amazonaws.com:443", Type: "S3", Bucket: "vectors",
				UseSSL: true, UseIAM: true, ServiceAccountName: "milvus-s3",
				Region: "us-east-1", CloudProvider: "aws", RootPath: "prod",
			},
		},
	}
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
	})
	require.NoError(t, validateInstance(c))
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	assert.Empty(t, spec.Dep.Storage.SecretRef)
	assert.Equal(t, "milvus-s3", spec.Com.ServiceAccountName)
	assert.Equal(t, map[string]any{
		"bucketName": "vectors", "rootPath": "prod", "region": "us-east-1",
		"cloudProvider": "aws", "useSSL": true, "useIAM": true,
	}, spec.Conf["minio"])
	assert.Equal(t, "vectors-prod", spec.Conf["etcd"].(map[string]any)["rootPath"])
	// Live clusters showed Woodpecker logs keyed by the channel prefix under a
	// fixed etcd prefix, outside etcd.rootPath.
	assert.Equal(t, map[string]any{"chanNamePrefix": map[string]any{"cluster": "vectors-prod"}}, spec.Conf["msgChannel"])
}

func TestValidateDataLocationUnchanged(t *testing.T) {
	external := func(endpoint, storageRootPath string) *runtime.RawExtension {
		return topologyParams(t, standalone.StandaloneTopologyParameters{
			Dependencies: &standalone.StandaloneDependencies{Storage: &dependencies.Storage{
				External: true, Endpoint: endpoint, CredentialsSecret: "creds", RootPath: storageRootPath,
			}},
		})
	}
	onExternal := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec: milvusapi.MilvusSpec{
			Mode: milvusapi.MilvusModeStandalone,
			Dep:  &milvusapi.MilvusDependencies{Storage: milvusapi.MilvusStorage{External: true, Endpoint: "minio:9000"}},
		},
	}
	onBundled := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       milvusapi.MilvusSpec{Mode: milvusapi.MilvusModeStandalone},
	}

	tests := []struct {
		name     string
		existing *milvusapi.Milvus
		params   *runtime.RawExtension
		wantErr  string
	}{
		{name: "default prefix kept", existing: onExternal, params: external("minio:9000", "")},
		{
			name: "prefix moved", existing: onExternal, params: external("minio:9000", "tenant-a"),
			wantErr: `minio rootPath cannot be changed from "files" to "tenant-a"`,
		},
		{
			name: "endpoint moved", existing: onExternal, params: external("other:9000", ""),
			wantErr: "object storage cannot be changed from minio:9000/ to other:9000/",
		},
		{
			name: "bundled to external", existing: onBundled, params: external("minio:9000", ""),
			wantErr: "object storage cannot be changed from the bundled MinIO to minio:9000/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: tt.params},
			}, tt.existing.DeepCopyObject().(*milvusapi.Milvus))
			err := validateInstance(c)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestBuildDependenciesUserOverrides(t *testing.T) {
	params := cluster.ClusterTopologyParameters{
		Dependencies: &cluster.ClusterDependencies{
			Etcd: &dependencies.Etcd{
				Replicas:  ptr.To(int32(5)),
				Resources: &dependencies.Resources{Requests: &dependencies.ResourceList{CPU: "250m", Memory: "1Gi"}},
			},
			Storage: &dependencies.Storage{Replicas: ptr.To(int32(4))},
			MessageStreamType: dependencies.MessageStreamPulsar,
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
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, pulsarCluster)},
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
			MessageStreamType: dependencies.MessageStreamPulsar,
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

func TestBuildDependenciesStorageClass(t *testing.T) {
	t.Run("standalone etcd and MinIO", func(t *testing.T) {
		params := standalone.StandaloneTopologyParameters{
			Dependencies: &standalone.StandaloneDependencies{StorageClassName: "fast-delete"},
		}
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, params)},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"size": "10Gi", "storageClass": "fast-delete"}, spec.Dep.Etcd.InCluster.Values["persistence"])
		assert.Equal(t, map[string]any{"size": "10Gi", "storageClass": "fast-delete"}, spec.Dep.Storage.InCluster.Values["persistence"])
	})

	t.Run("cluster Pulsar volumes", func(t *testing.T) {
		params := cluster.ClusterTopologyParameters{
			Dependencies: &cluster.ClusterDependencies{
				MessageStreamType: dependencies.MessageStreamPulsar,
				StorageClassName:  "fast-delete",
			},
		}
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, params)},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"size": "10Gi", "storageClass": "fast-delete"}, spec.Dep.Etcd.InCluster.Values["persistence"])
		assert.Equal(t, map[string]any{"size": "10Gi", "storageClass": "fast-delete"}, spec.Dep.Storage.InCluster.Values["persistence"])
		bookkeeper := spec.Dep.Pulsar.InCluster.Values["bookkeeper"].(map[string]any)
		assert.Equal(t, map[string]any{
			"journal": map[string]any{"size": "5Gi", "storageClassName": "fast-delete"},
			"ledgers": map[string]any{"size": "10Gi", "storageClassName": "fast-delete"},
		}, bookkeeper["volumes"])
		zookeeper := spec.Dep.Pulsar.InCluster.Values["zookeeper"].(map[string]any)
		assert.Equal(t, map[string]any{"data": map[string]any{"size": "5Gi", "storageClassName": "fast-delete"}}, zookeeper["volumes"])
	})
}

func TestValidateStorageClassUnchanged(t *testing.T) {
	existingWith := func(storageClass string) *milvusapi.Milvus {
		persistence := map[string]any{"size": "10Gi"}
		if storageClass != "" {
			persistence["storageClass"] = storageClass
		}
		return &milvusapi.Milvus{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
			Spec: milvusapi.MilvusSpec{
				Mode: milvusapi.MilvusModeStandalone,
				Dep: &milvusapi.MilvusDependencies{
					Etcd:    milvusapi.MilvusEtcd{InCluster: &milvusapi.InClusterConfig{Values: milvusapi.Values{"persistence": persistence}}},
					Storage: milvusapi.MilvusStorage{InCluster: &milvusapi.InClusterConfig{Values: milvusapi.Values{"persistence": persistence}}},
				},
			},
		}
	}
	requesting := func(storageClass string) *runtime.RawExtension {
		return topologyParams(t, standalone.StandaloneTopologyParameters{
			Dependencies: &standalone.StandaloneDependencies{StorageClassName: storageClass},
		})
	}
	tests := []struct {
		name      string
		existing  string
		requested string
		wantErr   string
	}{
		{name: "default kept"},
		{name: "class kept", existing: "fast-delete", requested: "fast-delete"},
		{name: "class set later", requested: "fast-delete", wantErr: `dependencies storageClassName cannot be changed from "" to "fast-delete"`},
		{name: "class switched", existing: "fast-delete", requested: "slow-retain", wantErr: `dependencies storageClassName cannot be changed from "fast-delete" to "slow-retain"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: requesting(tt.requested)},
			}, existingWith(tt.existing))
			err := validateInstance(c)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
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
		Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, pulsarCluster)},
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
					Dependencies: &cluster.ClusterDependencies{
						MessageStreamType: dependencies.MessageStreamPulsar,
						Pulsar:            &dependencies.Pulsar{External: true},
					},
				})},
			},
			wantErr: "pulsar.endpoint is required",
		},
		{
			name: "unknown message stream",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{MessageStreamType: "kafka"},
				})},
			},
			wantErr: "messageStreamType must be woodpecker or pulsar",
		},
		{
			name: "pulsar settings are not validated for woodpecker",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{Pulsar: &dependencies.Pulsar{External: true}},
				})},
			},
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
			name: "external storage without credentials",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, standalone.StandaloneTopologyParameters{
					Dependencies: &standalone.StandaloneDependencies{Storage: &dependencies.Storage{External: true, Endpoint: "minio:9000"}},
				})},
			},
			wantErr: "storage.credentialsSecret is required",
		},
		{
			name: "external storage with IAM needs no credentials",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, standalone.StandaloneTopologyParameters{
					Dependencies: &standalone.StandaloneDependencies{Storage: &dependencies.Storage{External: true, Endpoint: "s3:443", UseIAM: true}},
				})},
			},
		},
		{
			name: "external storage with unknown type",
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, standalone.StandaloneTopologyParameters{
					Dependencies: &standalone.StandaloneDependencies{Storage: &dependencies.Storage{
						External: true, Endpoint: "minio:9000", CredentialsSecret: "creds", Type: "GCS",
					}},
				})},
			},
			wantErr: "storage.type must be one of",
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
					Dependencies: &cluster.ClusterDependencies{MessageStreamType: dependencies.MessageStreamPulsar, Pulsar: &dependencies.Pulsar{
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
