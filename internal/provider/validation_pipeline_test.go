package provider

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller/validationtest"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/cluster"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

var _ controller.ReferenceValidator = (*Provider)(nil)

// skippedFixtures are not whole valid Instances: chainsaw applies them onto an
// Instance an earlier step created, or expects admission to refuse them.
var skippedFixtures = map[string]bool{
	"../../test/integration/scaling/20-scale-out.yaml":      true,
	"../../test/integration/scaling/30-autoscaling-on.yaml": true,
	"../../test/integration/core/14-invalid-instance.yaml":  true,
}

// shippedInstances reads every Instance the examples and test fixtures
// create.
func shippedInstances(t *testing.T) map[string]*corev1alpha1.Instance {
	t.Helper()
	instances := map[string]*corev1alpha1.Instance{}
	for _, root := range []string{"../../examples", "../../test"} {
		require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || filepath.Ext(path) != ".yaml" ||
				strings.HasSuffix(path, "-assert.yaml") || filepath.Base(path) == "chainsaw-test.yaml" {
				return err
			}
			data, err := os.ReadFile(filepath.Clean(path))
			if err != nil {
				return err
			}
			reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
			for i := 0; ; i++ {
				doc, err := reader.Read()
				if err != nil {
					break
				}
				var meta metav1.TypeMeta
				if yaml.Unmarshal(doc, &meta) != nil || meta.Kind != "Instance" {
					continue
				}
				in := &corev1alpha1.Instance{}
				require.NoError(t, yaml.UnmarshalStrict(doc, in), path)
				if skippedFixtures[filepath.ToSlash(path)] {
					continue
				}
				instances[fmt.Sprintf("%s#%d", path, i)] = in
			}
			return nil
		}))
	}
	require.NotEmpty(t, instances)
	return instances
}

func TestShippedInstancesPassValidation(t *testing.T) {
	v := validationtest.New(t, New(), shippedProviderSpecPath)
	for path, in := range shippedInstances(t) {
		t.Run(path, func(t *testing.T) {
			res := v.Run(in, nil)
			assert.Empty(t, res.Errors)
		})
	}
}

func TestValidateReportsFieldPaths(t *testing.T) {
	v := validationtest.New(t, New(), shippedProviderSpecPath)
	tests := map[string]struct {
		spec corev1alpha1.InstanceSpec
		want string
	}{
		"memory below the minimum": {
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {Type: "milvus", Resources: resources(t, map[corev1.ResourceName]string{"memory": "256Mi"}, nil)},
				},
			},
			want: "spec.components[standalone].resources.limits.memory",
		},
		"unknown message stream": {
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{
					Dependencies: &cluster.ClusterDependencies{MessageStreamType: "kafka"},
				})},
				Components: clusterComponents(),
			},
			want: "spec.topology.parameters.dependencies.messageStreamType",
		},
		"configuration that is not YAML": {
			spec: corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {Type: "milvus", Parameters: configParams(t, "a: [b")},
				},
			},
			want: "spec.components",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			res := v.Run(newInstance(tt.spec), nil)
			require.Len(t, res.Errors, 1)
			assert.Equal(t, tt.want, res.Errors[0].Field)
		})
	}
}

func TestValidateTransition(t *testing.T) {
	v := validationtest.New(t, New(), shippedProviderSpecPath)
	clusterWith := func(deps cluster.ClusterDependencies) corev1alpha1.InstanceSpec {
		return corev1alpha1.InstanceSpec{
			Topology:   &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, cluster.ClusterTopologyParameters{Dependencies: &deps})},
			Components: clusterComponents(),
		}
	}
	standaloneWith := func(deps standalone.StandaloneDependencies) corev1alpha1.InstanceSpec {
		return corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: topologyParams(t, standalone.StandaloneTopologyParameters{Dependencies: &deps})},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {Type: "milvus"},
			},
		}
	}
	sized := func(size string) *dependencies.Storage {
		return &dependencies.Storage{Persistence: &dependencies.Persistence{Size: size}}
	}
	external := func(bucket string) *dependencies.Storage {
		return &dependencies.Storage{External: true, Endpoint: "s3.example.com:443", Bucket: bucket, CredentialsSecret: "s3"}
	}

	externalEtcd := func(rootPath string) *dependencies.Etcd {
		return &dependencies.Etcd{External: true, Endpoints: []string{"etcd:2379"}, RootPath: rootPath}
	}
	versionOnly := func(version string) corev1alpha1.InstanceSpec {
		return corev1alpha1.InstanceSpec{Version: version}
	}

	tests := map[string]struct {
		old, in   corev1alpha1.InstanceSpec
		oldPhase  corev1alpha1.InstancePhase
		oldStatus func(*corev1alpha1.Instance)
		wantField string
	}{
		"topology": {
			old: standaloneWith(standalone.StandaloneDependencies{}), in: clusterWith(cluster.ClusterDependencies{}),
			wantField: "spec.topology.type",
		},
		"storage shrinks": {
			old: standaloneWith(standalone.StandaloneDependencies{Storage: sized("20Gi")}), in: standaloneWith(standalone.StandaloneDependencies{Storage: sized("10Gi")}),
			wantField: "spec.topology.parameters.dependencies.storage.persistence.size",
		},
		"storage grows": {
			old: standaloneWith(standalone.StandaloneDependencies{Storage: sized("20Gi")}), in: standaloneWith(standalone.StandaloneDependencies{Storage: sized("30Gi")}),
		},
		"storage class": {
			old: standaloneWith(standalone.StandaloneDependencies{StorageClassName: "a"}), in: standaloneWith(standalone.StandaloneDependencies{StorageClassName: "b"}),
			wantField: "spec.topology.parameters.dependencies.storageClassName",
		},
		"bundled to external storage": {
			old: clusterWith(cluster.ClusterDependencies{}), in: clusterWith(cluster.ClusterDependencies{Storage: external("milvus")}),
			wantField: "spec.topology.parameters.dependencies.storage",
		},
		"external bucket": {
			old: clusterWith(cluster.ClusterDependencies{Storage: external("a")}), in: clusterWith(cluster.ClusterDependencies{Storage: external("b")}),
			wantField: "spec.topology.parameters.dependencies.storage",
		},
		"message stream": {
			old:       clusterWith(cluster.ClusterDependencies{MessageStreamType: dependencies.MessageStreamWoodpecker}),
			in:        clusterWith(cluster.ClusterDependencies{MessageStreamType: dependencies.MessageStreamPulsar}),
			wantField: "spec.topology.parameters.dependencies.messageStreamType",
		},
		"message stream named for the first time": {
			old: clusterWith(cluster.ClusterDependencies{}),
			in:  clusterWith(cluster.ClusterDependencies{MessageStreamType: dependencies.MessageStreamPulsar}),
		},
		"downgrade": {
			old:       withVersion(clusterWith(cluster.ClusterDependencies{}), "2.6.15"),
			in:        withVersion(clusterWith(cluster.ClusterDependencies{}), "2.6.11"),
			wantField: "spec.components[dataNode].version",
		},
		"upgrade": {
			old: withVersion(clusterWith(cluster.ClusterDependencies{}), "2.6.11"),
			in:  withVersion(clusterWith(cluster.ClusterDependencies{}), "2.6.15"),
		},
		"downgrade of an instance that names no components": {
			old: versionOnly("2.6.15"), in: versionOnly("2.6.11"),
			wantField: "spec.components[standalone].version",
		},
		"reverting an edit a reference error held back": {
			old: standaloneWith(standalone.StandaloneDependencies{}), in: clusterWith(cluster.ClusterDependencies{}),
			oldStatus: func(old *corev1alpha1.Instance) {
				old.Status.Conditions = []metav1.Condition{{
					Type: corev1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: corev1alpha1.ReasonInvalidReference,
				}}
			},
		},
		"reverting an edit not reconciled yet": {
			old: withVersion(clusterWith(cluster.ClusterDependencies{}), "2.6.15"),
			in:  withVersion(clusterWith(cluster.ClusterDependencies{}), "2.6.11"),
			oldStatus: func(old *corev1alpha1.Instance) {
				old.Generation, old.Status.ObservedGeneration = 3, 2
			},
		},
		"default external rootPath set explicitly": {
			old: clusterWith(cluster.ClusterDependencies{Storage: external("milvus")}),
			in: clusterWith(cluster.ClusterDependencies{Storage: func() *dependencies.Storage {
				s := external("milvus")
				s.RootPath = defaultMinioRootPath
				return s
			}()}),
		},
		"default external etcd rootPath set explicitly": {
			old: clusterWith(cluster.ClusterDependencies{Etcd: externalEtcd("")}),
			in:  clusterWith(cluster.ClusterDependencies{Etcd: externalEtcd("test-milvus")}),
		},
		"external etcd rootPath": {
			old:       clusterWith(cluster.ClusterDependencies{Etcd: externalEtcd("a")}),
			in:        clusterWith(cluster.ClusterDependencies{Etcd: externalEtcd("b")}),
			wantField: "spec.topology.parameters.dependencies.etcd.rootPath",
		},
		"storage class without bundled volumes": {
			old: clusterWith(cluster.ClusterDependencies{Etcd: externalEtcd(""), Storage: external("milvus"), StorageClassName: "a"}),
			in:  clusterWith(cluster.ClusterDependencies{Etcd: externalEtcd(""), Storage: external("milvus"), StorageClassName: "b"}),
		},
		"anything goes before the instance is created": {
			old: standaloneWith(standalone.StandaloneDependencies{}), in: clusterWith(cluster.ClusterDependencies{}),
			oldPhase: corev1alpha1.InstancePhasePending,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			old := newInstance(tt.old)
			old.Status.Phase = corev1alpha1.InstancePhaseReady
			if tt.oldPhase != "" {
				old.Status.Phase = tt.oldPhase
			}
			if tt.oldStatus != nil {
				tt.oldStatus(old)
			}
			res := v.Run(newInstance(tt.in), old)
			if tt.wantField == "" {
				assert.Empty(t, res.Errors)
				return
			}
			require.NotEmpty(t, res.Errors)
			assert.Equal(t, tt.wantField, res.Errors[0].Field)
		})
	}
}

func TestValidatePulsarInUse(t *testing.T) {
	invalidPulsar := cluster.ClusterTopologyParameters{Dependencies: &cluster.ClusterDependencies{
		Pulsar: &dependencies.Pulsar{External: true},
	}}
	for name, tt := range map[string]struct {
		applied milvusapi.MilvusDependencies
		wantErr string
	}{
		"legacy instance running pulsar": {
			applied: milvusapi.MilvusDependencies{Pulsar: milvusapi.MilvusPulsar{InCluster: &milvusapi.InClusterConfig{}}},
			wantErr: "pulsar.endpoint is required",
		},
		"instance running woodpecker": {
			applied: milvusapi.MilvusDependencies{MsgStreamType: dependencies.MessageStreamWoodpecker},
		},
	} {
		t.Run(name, func(t *testing.T) {
			existing := &milvusapi.Milvus{Spec: milvusapi.MilvusSpec{Mode: milvusapi.MilvusModeCluster, Dep: &tt.applied}}
			in := newInstance(corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster", Parameters: topologyParams(t, invalidPulsar)},
			})
			err := validatePulsarInUse(existing, in)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func newInstance(spec corev1alpha1.InstanceSpec) *corev1alpha1.Instance {
	return &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       withProviderRef(spec),
	}
}

func withVersion(spec corev1alpha1.InstanceSpec, version string) corev1alpha1.InstanceSpec {
	spec.Version = version
	return spec
}

func clusterComponents() map[string]corev1alpha1.ComponentSpec {
	components := map[string]corev1alpha1.ComponentSpec{}
	for _, name := range allowedComponentsForTopology("cluster") {
		components[name] = corev1alpha1.ComponentSpec{Type: "milvus", Replicas: ptr.To(int32(1))}
	}
	return components
}
