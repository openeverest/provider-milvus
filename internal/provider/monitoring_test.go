package provider

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller/validationtest"

	"github.com/openeverest/provider-milvus/definition/monitoring"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func monitoringInstanceSpec(t *testing.T, prometheus *monitoring.Prometheus) corev1alpha1.InstanceSpec {
	t.Helper()
	raw, err := json.Marshal(topologyMonitoring{Monitoring: &monitoring.Monitoring{Prometheus: prometheus}}) //nolint:staticcheck // Instances still carry it
	require.NoError(t, err)
	return corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: &runtime.RawExtension{Raw: raw}},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentStandalone: {Type: "milvus"},
		},
	}
}

// milvusWithStatus is the Milvus CR of test-milvus in mode, reporting status.
func milvusWithStatus(mode milvusapi.MilvusMode, status milvusapi.MilvusHealthStatus) *milvusapi.Milvus {
	cr := &milvusapi.Milvus{
		ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
		Spec:       milvusapi.MilvusSpec{Mode: mode},
		Status:     milvusapi.MilvusStatus{Status: status},
	}
	if mode == milvusapi.MilvusModeStandalone {
		cr.Spec.Com.Standalone = &milvusapi.MilvusStandalone{}
		return cr
	}
	cr.Spec.Com.Proxy = &milvusapi.MilvusProxy{}
	cr.Spec.Com.MixCoord = &milvusapi.MilvusMixCoord{}
	cr.Spec.Com.DataNode = &milvusapi.MilvusDataNode{}
	cr.Spec.Com.QueryNode = &milvusapi.MilvusQueryNode{}
	cr.Spec.Com.StreamingNode = &milvusapi.MilvusStreamingNode{}
	return cr
}

func TestMonitoringSources(t *testing.T) {
	endpoint := func(component string) corev1alpha1.MetricsEndpoint {
		return corev1alpha1.MetricsEndpoint{
			Component: component,
			PodSelector: map[string]string{
				controller.InstanceLabel:  "test-milvus",
				controller.ComponentLabel: component,
			},
			Port:   intstr.FromString("metrics"),
			Path:   "/metrics",
			Scheme: "http",
		}
	}
	tests := map[string]struct {
		cr   *milvusapi.Milvus
		want []corev1alpha1.MetricsEndpoint
	}{
		"no Milvus CR yet":          {},
		"creating":                  {cr: milvusWithStatus(milvusapi.MilvusModeStandalone, milvusapi.StatusPending)},
		"stopped":                   {cr: milvusWithStatus(milvusapi.MilvusModeStandalone, milvusapi.StatusStopped)},
		"deleting":                  {cr: milvusWithStatus(milvusapi.MilvusModeStandalone, milvusapi.StatusDeleting)},
		"standalone ready":          {cr: milvusWithStatus(milvusapi.MilvusModeStandalone, milvusapi.StatusHealthy), want: []corev1alpha1.MetricsEndpoint{endpoint("standalone")}},
		"updating, so not healthy":  {cr: milvusWithStatus(milvusapi.MilvusModeStandalone, milvusapi.StatusUnhealthy), want: []corev1alpha1.MetricsEndpoint{endpoint("standalone")}},
		"cluster ready, every role": {cr: milvusWithStatus(milvusapi.MilvusModeCluster, milvusapi.StatusHealthy), want: []corev1alpha1.MetricsEndpoint{endpoint("dataNode"), endpoint("mixCoord"), endpoint("proxy"), endpoint("queryNode"), endpoint("streamingNode")}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var objs []client.Object
			if tt.cr != nil {
				objs = append(objs, tt.cr)
			}
			c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{}, objs...)
			sources, err := New().MonitoringSources(c)
			require.NoError(t, err)
			require.NotNil(t, sources, "an empty, not nil, result clears the published sources")
			assert.Equal(t, tt.want, sources.Metrics)
		})
	}

	t.Run("read error keeps the published sources", func(t *testing.T) {
		instance := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"}}
		cl := fakeClientBuilder(t).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errors.New("apiserver unavailable")
			},
		}).Build()
		sources, err := New().MonitoringSources(controller.NewContext(context.Background(), cl, instance, common.ProviderName))
		require.Error(t, err)
		assert.Nil(t, sources)
	})
}

func TestShippedProviderDeclaresNativeMetrics(t *testing.T) {
	spec := shippedProviderSpec(t)
	assert.Equal(t, "milvus", spec.ComponentTypes["milvus"].Kind)
	require.NotNil(t, spec.Monitoring)
	assert.Equal(t, corev1alpha1.MetricsSourceNative, spec.Monitoring.ComponentTypes["milvus"].Metrics)
	assert.Empty(t, spec.Monitoring.Integrations, "Milvus has no operator-native monitoring integration")
	for name, component := range spec.Components {
		assert.Equal(t, "milvus", component.Type, "component %s publishes metrics under the milvus type", name)
	}
}

func TestDeleteLegacyPodMonitor(t *testing.T) {
	legacy := func() *unstructured.Unstructured {
		pm := &unstructured.Unstructured{}
		pm.SetGroupVersionKind(podMonitorGVK)
		pm.SetNamespace("db")
		pm.SetName("test-milvus-metrics")
		return pm
	}
	withPodMonitorCRD := func() meta.RESTMapper {
		mapper := meta.NewDefaultRESTMapper(nil)
		mapper.Add(podMonitorGVK, meta.RESTScopeNamespace)
		return mapper
	}
	instance := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"}}

	t.Run("deleted", func(t *testing.T) {
		cl := fakeClientBuilder(t).WithRESTMapper(withPodMonitorCRD()).WithObjects(legacy()).Build()
		require.NoError(t, deleteLegacyPodMonitor(controller.NewContext(context.Background(), cl, instance, common.ProviderName)))
		err := cl.Get(context.Background(), client.ObjectKeyFromObject(legacy()), legacy())
		assert.True(t, apierrors.IsNotFound(err), "got %v", err)
	})

	t.Run("already gone", func(t *testing.T) {
		cl := fakeClientBuilder(t).WithRESTMapper(withPodMonitorCRD()).Build()
		assert.NoError(t, deleteLegacyPodMonitor(controller.NewContext(context.Background(), cl, instance, common.ProviderName)))
	})

	t.Run("no Prometheus Operator CRD", func(t *testing.T) {
		cl := fakeClientBuilder(t).WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return &meta.NoKindMatchError{GroupKind: podMonitorGVK.GroupKind()}
			},
		}).Build()
		assert.NoError(t, deleteLegacyPodMonitor(controller.NewContext(context.Background(), cl, instance, common.ProviderName)))
	})

	t.Run("API error", func(t *testing.T) {
		cl := fakeClientBuilder(t).WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return errors.New("forbidden")
			},
		}).Build()
		assert.ErrorContains(t, deleteLegacyPodMonitor(controller.NewContext(context.Background(), cl, instance, common.ProviderName)), "forbidden")
	})
}

// Instances written by earlier releases carry the Prometheus parameter, and
// the runtime rejects parameters the schema does not declare, so it stays
// accepted, whatever it holds, with a warning while it is turned on.
//nolint:staticcheck // the deprecated parameter is what this tests
func TestDeprecatedPrometheusParameter(t *testing.T) {
	v := validationtest.New(t, New(), shippedProviderSpecPath)
	tests := map[string]struct {
		prometheus  *monitoring.Prometheus
		wantWarning bool
	}{
		"enabled": {
			prometheus:  &monitoring.Prometheus{Enabled: true, Interval: "30s", PodMonitorLabels: map[string]string{"release": "kube-prometheus-stack"}},
			wantWarning: true,
		},
		"enabled with settings the PodMonitor CRD would refuse": {
			prometheus:  &monitoring.Prometheus{Enabled: true, Interval: "soon", PodMonitorLabels: map[string]string{"bad key": "not valid!"}},
			wantWarning: true,
		},
		"disabled, as the old UI wrote it": {prometheus: &monitoring.Prometheus{}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			res := v.Run(newInstance(monitoringInstanceSpec(t, tt.prometheus)), nil)
			assert.Empty(t, res.Errors)
			if !tt.wantWarning {
				assert.Empty(t, res.Warnings)
				return
			}
			require.Len(t, res.Warnings, 1)
			assert.Contains(t, res.Warnings[0], "spec.topology.parameters.monitoring.prometheus is deprecated and has no effect")
		})
	}
}
