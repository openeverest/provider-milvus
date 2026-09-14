package provider

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func configParams(t *testing.T, cfg string) *runtime.RawExtension {
	t.Helper()
	raw, err := json.Marshal(components.MilvusParameters{Configuration: cfg})
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: raw}
}

func newTestContext(t *testing.T, spec corev1alpha1.InstanceSpec) *controller.Context {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, milvusapi.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       spec,
	}
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, provider).Build()
	return controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)
}

func TestMilvusEngineConfig(t *testing.T) {
	tests := []struct {
		name       string
		components map[string]corev1alpha1.ComponentSpec
		want       milvusapi.Values
		wantErr    string
	}{
		{
			name:       "no components",
			components: map[string]corev1alpha1.ComponentSpec{},
			want:       nil,
		},
		{
			name: "component without configuration",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {},
			},
			want: nil,
		},
		{
			name: "single component configuration",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Parameters: configParams(t, "log:\n  level: debug\n"),
				},
			},
			want: milvusapi.Values{
				"log": map[string]any{"level": "debug"},
			},
		},
		{
			name: "deep merge across components",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy: {
					Parameters: configParams(t, "proxy:\n  maxNameLength: 255\n"),
				},
				common.ComponentQueryNode: {
					Parameters: configParams(t, "queryNode:\n  gracefulTime: 5000\n"),
				},
			},
			want: milvusapi.Values{
				"proxy":     map[string]any{"maxNameLength": float64(255)},
				"queryNode": map[string]any{"gracefulTime": float64(5000)},
			},
		},
		{
			name: "nested keys under same section are merged",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentDataCoord: {
					Parameters: configParams(t, "dataCoord:\n  segment:\n    maxSize: 1024\n"),
				},
				common.ComponentDataNode: {
					Parameters: configParams(t, "dataCoord:\n  enableCompaction: true\n"),
				},
			},
			want: milvusapi.Values{
				"dataCoord": map[string]any{
					"segment":          map[string]any{"maxSize": float64(1024)},
					"enableCompaction": true,
				},
			},
		},
		{
			name: "invalid yaml returns error",
			components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Parameters: configParams(t, "log: [unclosed"),
				},
			},
			wantErr: "invalid configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{Components: tt.components})
			got, err := milvusEngineConfig(c)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, map[string]any(tt.want), map[string]any(got))
		})
	}
}

func TestBuildMilvusSpecConfiguration(t *testing.T) {
	t.Run("standalone config lands in spec.config", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Parameters: configParams(t, "log:\n  level: info\n"),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"level": "info"}, spec.Conf["log"])
	})

	t.Run("cluster merges config from multiple components", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy: {
					Parameters: configParams(t, "proxy:\n  maxNameLength: 255\n"),
				},
				common.ComponentQueryNode: {
					Parameters: configParams(t, "queryNode:\n  gracefulTime: 5000\n"),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"maxNameLength": float64(255)}, spec.Conf["proxy"])
		assert.Equal(t, map[string]any{"gracefulTime": float64(5000)}, spec.Conf["queryNode"])
	})

	t.Run("no configuration leaves spec.config empty", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Nil(t, spec.Conf)
	})
}

func TestBuildMilvusSpec_TopologyAndResources(t *testing.T) {
	t.Run("standalone topology configuration", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Version:  "2.5.0",
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Replicas: ptr.To[int32](2),
					Storage: &corev1alpha1.Storage{
						Size: resource.MustParse("10Gi"),
					},
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, milvusapi.MilvusModeStandalone, spec.Mode)
		assert.Equal(t, "2.5.0", spec.Com.Version)
		require.NotNil(t, spec.Com.Standalone)
		assert.Equal(t, ptr.To[int32](2), spec.Com.Standalone.Replicas)
		require.NotNil(t, spec.Dep)
		require.NotNil(t, spec.Dep.Storage.InCluster)
		require.NotNil(t, spec.Dep.Storage.InCluster.Values)
		persistence, ok := spec.Dep.Storage.InCluster.Values["persistence"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "10Gi", persistence["size"])
	})

	t.Run("cluster topology configuration", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy: {
					Replicas: ptr.To[int32](3),
				},
				common.ComponentDataNode: {
					Replicas: ptr.To[int32](5),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, milvusapi.MilvusModeCluster, spec.Mode)
		require.NotNil(t, spec.Com.Proxy)
		assert.Equal(t, ptr.To[int32](3), spec.Com.Proxy.Replicas)
		require.NotNil(t, spec.Com.DataNode)
		assert.Equal(t, ptr.To[int32](5), spec.Com.DataNode.Replicas)
	})

	t.Run("default version fallback", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		// Defaults to 2.6.11 if not set
		assert.Equal(t, "2.6.11", spec.Com.Version)
	})

	t.Run("storage size is propagated in cluster topology", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentDataNode: {
					Storage: &corev1alpha1.Storage{
						Size: resource.MustParse("50Gi"),
					},
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		persistence, ok := spec.Dep.Storage.InCluster.Values["persistence"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "50Gi", persistence["size"])
	})

	t.Run("invalid configuration returns error", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Parameters: configParams(t, "invalid: yaml: ["),
				},
			},
		})
		_, err := BuildMilvusSpec(c)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid configuration")
	})
}

func newMilvusCR(status milvusapi.MilvusHealthStatus, endpoint string) *milvusapi.Milvus {
	return &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Status: milvusapi.MilvusStatus{
			Status:   status,
			Endpoint: endpoint,
		},
	}
}

func TestProvider_Status(t *testing.T) {
	t.Run("missing Milvus CR returns Provisioning", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})
		p := &Provider{}
		status, err := p.Status(c)
		require.NoError(t, err)

		assert.Equal(t, corev1alpha1.InstancePhaseProvisioning, status.Phase)
		assert.Contains(t, status.Message, "waiting for Milvus CR")
	})

	t.Run("healthy status returns ReadyWithConnectionDetails", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})

		cr := newMilvusCR(milvusapi.StatusHealthy, "my-endpoint.db.svc.cluster.local:19530")
		require.NoError(t, c.Client().Create(context.Background(), cr))

		p := &Provider{}
		status, err := p.Status(c)
		require.NoError(t, err)

		assert.Equal(t, corev1alpha1.InstancePhaseReady, status.Phase)
		require.NotNil(t, status.ConnectionDetails)
		assert.Equal(t, "my-endpoint.db.svc.cluster.local", status.ConnectionDetails.Host)
		assert.Equal(t, "19530", status.ConnectionDetails.Port)
	})

	t.Run("healthy status without port defaults to 19530", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})

		cr := newMilvusCR(milvusapi.StatusHealthy, "my-endpoint.db.svc.cluster.local")
		require.NoError(t, c.Client().Create(context.Background(), cr))

		p := &Provider{}
		status, err := p.Status(c)
		require.NoError(t, err)

		assert.Equal(t, corev1alpha1.InstancePhaseReady, status.Phase)
		require.NotNil(t, status.ConnectionDetails)
		assert.Equal(t, "my-endpoint.db.svc.cluster.local", status.ConnectionDetails.Host)
		assert.Equal(t, "19530", status.ConnectionDetails.Port)
	})

	t.Run("stopped status returns Pending", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})

		cr := newMilvusCR(milvusapi.StatusStopped, "")
		require.NoError(t, c.Client().Create(context.Background(), cr))

		p := &Provider{}
		status, err := p.Status(c)
		require.NoError(t, err)

		assert.Equal(t, corev1alpha1.InstancePhasePending, status.Phase)
	})
}
