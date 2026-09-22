package provider

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	require.NoError(t, corev1.AddToScheme(scheme))
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
				common.ComponentMixCoord: {
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

func TestBuildMilvusSpecClusterComponents(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	// Milvus 2.6 cluster uses a single MixCoord plus a StreamingNode; the
	// pre-2.6 coordinators and IndexNode are no longer generated.
	require.NotNil(t, spec.Com.Proxy)
	require.NotNil(t, spec.Com.MixCoord)
	require.NotNil(t, spec.Com.DataNode)
	require.NotNil(t, spec.Com.QueryNode)
	require.NotNil(t, spec.Com.StreamingNode)
}

func TestBuildMilvusSpecComponentResources(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentDataNode: {
				Resources: resources(t,
					map[corev1.ResourceName]string{"cpu": "2", "memory": "4Gi"},
					map[corev1.ResourceName]string{"cpu": "1", "memory": "2Gi"}),
			},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	require.NotNil(t, spec.Com.DataNode)
	res := spec.Com.DataNode.Resources
	require.NotNil(t, res)
	// Both limits and requests flow through, not just limits.
	assert.Equal(t, "2", res.Limits.Cpu().String())
	assert.Equal(t, "4Gi", res.Limits.Memory().String())
	assert.Equal(t, "1", res.Requests.Cpu().String())
	assert.Equal(t, "2Gi", res.Requests.Memory().String())
}

func TestSyncSeedsAuthAndStatusSurfacesCredentials(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentStandalone: {},
		},
	})
	p := New()

	require.NoError(t, p.Sync(c))

	cr := &milvusapi.Milvus{}
	require.NoError(t, c.Get(cr, c.Name()))
	security := cr.Spec.Conf["common"].(map[string]any)["security"].(map[string]any)
	assert.Equal(t, true, security["authorizationEnabled"])
	password, ok := security["defaultRootPassword"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, password)

	cr.Status.Status = milvusapi.StatusHealthy
	require.NoError(t, c.Apply(cr))

	status, err := p.Status(c)
	require.NoError(t, err)

	cd := status.ConnectionDetails
	assert.Equal(t, rootUsername, cd.Username)
	assert.Equal(t, password, cd.Password, "connection password must match the seeded root password")
	assert.Equal(t, "test-milvus-milvus.db.svc.cluster.local", cd.Host)
	assert.Equal(t, "19530", cd.Port)
	assert.Equal(t, rootUsername+":"+password, cd.AdditionalProperties["token"])
}
