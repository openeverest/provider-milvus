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
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

func TestBuildMilvusSpecTopology(t *testing.T) {
	t.Run("standalone maps mode, version, replicas and storage", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Version:  "2.5.0",
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {
					Replicas: ptr.To[int32](2),
					Storage:  storage(t, "20Gi"),
				},
			},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)

		assert.Equal(t, milvusapi.MilvusModeStandalone, spec.Mode)
		assert.Equal(t, "2.5.0", spec.Com.Version)
		require.NotNil(t, spec.Com.Standalone)
		assert.Equal(t, ptr.To[int32](2), spec.Com.Standalone.Replicas)
		// MinIO derives its size from the standalone component's storage.
		require.NotNil(t, spec.Dep)
		assert.Equal(t, map[string]any{"size": "20Gi"}, spec.Dep.Storage.InCluster.Values["persistence"])
	})

	t.Run("cluster maps component replicas", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentProxy:    {Replicas: ptr.To[int32](3)},
				common.ComponentDataNode: {Replicas: ptr.To[int32](5)},
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

	t.Run("unset version falls back to the default", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		})
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, "2.6.11", spec.Com.Version)
	})

	t.Run("unset version resolves the default bundle and image from the Provider", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		})
		provider := &corev1alpha1.Provider{}
		require.NoError(t, c.Client().Get(context.Background(), client.ObjectKey{Name: common.ProviderName}, provider))
		provider.Spec = corev1alpha1.ProviderSpec{
			ComponentTypes: map[string]corev1alpha1.ComponentType{
				"milvus": {Versions: []corev1alpha1.ComponentVersion{{Version: "2.6.0", Image: "example.com/milvus:v2.6.0"}}},
			},
			Components: map[string]corev1alpha1.Component{
				common.ComponentStandalone: {Type: "milvus"},
			},
			Versions: []corev1alpha1.VersionBundle{{Name: "2.6.0", Default: true}},
		}
		require.NoError(t, c.Client().Update(context.Background(), provider))

		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, "2.6.0", spec.Com.Version)
		assert.Equal(t, "example.com/milvus:v2.6.0", spec.Com.Image)
	})

	t.Run("invalid configuration returns an error", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {Parameters: configParams(t, "invalid: yaml: [")},
			},
		})
		_, err := BuildMilvusSpec(c)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid configuration")
	})

	t.Run("missing Provider CR returns an error", func(t *testing.T) {
		scheme := runtime.NewScheme()
		require.NoError(t, corev1alpha1.AddToScheme(scheme))
		require.NoError(t, milvusapi.AddToScheme(scheme))

		instance := &corev1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		c := controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)

		_, err := BuildMilvusSpec(c)
		require.Error(t, err)
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

func TestProviderStatus(t *testing.T) {
	t.Run("missing Milvus CR returns Provisioning", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})

		status, err := New().Status(c)
		require.NoError(t, err)
		assert.Equal(t, corev1alpha1.InstancePhaseProvisioning, status.Phase)
		assert.Contains(t, status.Message, "waiting for Milvus CR")
	})

	notReadyTests := []struct {
		name        string
		status      milvusapi.MilvusHealthStatus
		wantPhase   corev1alpha1.InstancePhase
		wantMessage string
	}{
		{name: "stopped", status: milvusapi.StatusStopped, wantPhase: corev1alpha1.InstancePhasePending, wantMessage: "Milvus is stopped"},
		{name: "unhealthy", status: milvusapi.StatusUnhealthy, wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is unhealthy"},
		{name: "pending", status: milvusapi.StatusPending, wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is being initialized or updated"},
		{name: "deleting", status: milvusapi.StatusDeleting, wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is being initialized or updated"},
		{name: "unknown", status: "Unknown", wantPhase: corev1alpha1.InstancePhaseProvisioning, wantMessage: "Milvus is initializing"},
	}
	for _, tt := range notReadyTests {
		t.Run(tt.name+" status is not ready", func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{})
			require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(tt.status, "")))

			status, err := New().Status(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhase, status.Phase)
			assert.Contains(t, status.Message, tt.wantMessage)
		})
	}

	endpointTests := []struct {
		name     string
		endpoint string
		wantHost string
		wantPort string
	}{
		{name: "host and port", endpoint: "my-endpoint.db.svc.cluster.local:19530", wantHost: "my-endpoint.db.svc.cluster.local", wantPort: "19530"},
		{name: "host without port defaults to 19530", endpoint: "my-endpoint.db.svc.cluster.local", wantHost: "my-endpoint.db.svc.cluster.local", wantPort: "19530"},
	}
	for _, tt := range endpointTests {
		t.Run("healthy status parses endpoint with "+tt.name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{})
			require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(milvusapi.StatusHealthy, tt.endpoint)))

			status, err := New().Status(c)
			require.NoError(t, err)
			assert.Equal(t, corev1alpha1.InstancePhaseReady, status.Phase)
			cd := status.ConnectionDetails
			assert.Equal(t, tt.wantHost, cd.Host)
			assert.Equal(t, tt.wantPort, cd.Port)
			assert.Equal(t, "http://"+tt.wantHost+":"+tt.wantPort, cd.URI)
		})
	}

	t.Run("healthy status without a load balancer address is not ready", func(t *testing.T) {
		c := newTestContext(t, corev1alpha1.InstanceSpec{})
		cr := newMilvusCR(milvusapi.StatusHealthy, "")
		cr.Spec.Com.Standalone = &milvusapi.MilvusStandalone{
			ServiceComponent: milvusapi.ServiceComponent{ServiceType: corev1.ServiceTypeLoadBalancer},
		}
		require.NoError(t, c.Client().Create(context.Background(), cr))

		status, err := New().Status(c)
		require.NoError(t, err)
		assert.Equal(t, corev1alpha1.InstancePhaseProvisioning, status.Phase)
		assert.Contains(t, status.Message, "waiting for load balancer address")
	})
}
