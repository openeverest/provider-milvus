package provider

import (
	"context"
	"encoding/json"
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/monitoring"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func monitoringInstanceSpec(t *testing.T, prometheus *monitoring.Prometheus) corev1alpha1.InstanceSpec {
	t.Helper()
	raw, err := json.Marshal(topologyMonitoring{Monitoring: &monitoring.Monitoring{Prometheus: prometheus}})
	require.NoError(t, err)
	return corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: &runtime.RawExtension{Raw: raw}},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentStandalone: {},
		},
	}
}

func TestSyncPodMonitorFollowsPrometheusToggle(t *testing.T) {
	c := newTestContext(t, monitoringInstanceSpec(t, &monitoring.Prometheus{
		Enabled:  true,
		Interval: "15s",
		PodMonitorLabels: map[string]string{
			"release":                    "kube-prometheus-stack",
			"app.kubernetes.io/instance": "spoofed",
		},
	}))

	require.NoError(t, New().Sync(c))

	cr := &milvusapi.Milvus{}
	require.NoError(t, c.Get(cr, c.Name()))
	assert.True(t, cr.Spec.Com.DisableMetric, "the operator must not create a second PodMonitor")

	podMonitor := &monitoringv1.PodMonitor{}
	require.NoError(t, c.Get(podMonitor, "test-milvus-metrics"))
	assert.Equal(t, "kube-prometheus-stack", podMonitor.Labels["release"])
	assert.Equal(t, "test-milvus", podMonitor.Labels["app.kubernetes.io/instance"], "user labels must not override managed ones")
	assert.Equal(t, map[string]string{
		controller.ProviderLabel: common.ProviderName,
		controller.InstanceLabel: "test-milvus",
	}, podMonitor.Spec.Selector.MatchLabels)
	require.Len(t, podMonitor.Spec.PodMetricsEndpoints, 1)
	endpoint := podMonitor.Spec.PodMetricsEndpoints[0]
	assert.Equal(t, ptr.To("metrics"), endpoint.Port)
	assert.Equal(t, "/metrics", endpoint.Path)
	assert.Equal(t, monitoringv1.Duration("15s"), endpoint.Interval)

	disabled := monitoringInstanceSpec(t, &monitoring.Prometheus{Enabled: false})
	c.Instance().Spec.Topology = disabled.Topology
	require.NoError(t, New().Sync(c))

	err := c.Get(&monitoringv1.PodMonitor{}, "test-milvus-metrics")
	assert.True(t, apierrors.IsNotFound(err), "disabling Prometheus must remove the PodMonitor, got %v", err)
}

func TestSyncRemovesOnlyTheOperatorPodMonitor(t *testing.T) {
	tests := []struct {
		name        string
		owner       *metav1.OwnerReference
		wantDeleted bool
	}{
		{
			name: "created by the Milvus operator",
			owner: &metav1.OwnerReference{
				APIVersion: milvusapi.GroupVersion, Kind: "Milvus", Name: "test-milvus", UID: "milvus-uid",
				Controller: ptr.To(true),
			},
			wantDeleted: true,
		},
		{name: "created by the user", wantDeleted: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, monitoringInstanceSpec(t, nil))
			existing := &monitoringv1.PodMonitor{ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"}}
			if tt.owner != nil {
				existing.OwnerReferences = []metav1.OwnerReference{*tt.owner}
			}
			require.NoError(t, c.Client().Create(c.Context(), existing))

			require.NoError(t, New().Sync(c))

			err := c.Get(&monitoringv1.PodMonitor{}, "test-milvus")
			if tt.wantDeleted {
				assert.True(t, apierrors.IsNotFound(err), "got %v", err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateMonitoring(t *testing.T) {
	tests := []struct {
		name       string
		prometheus *monitoring.Prometheus
		withCRD    bool
		wantErr    string
	}{
		{name: "disabled with an invalid interval is ignored", prometheus: &monitoring.Prometheus{Interval: "soon"}},
		{name: "enabled", prometheus: &monitoring.Prometheus{Enabled: true, Interval: "1m30s"}, withCRD: true},
		{
			name:       "enabled without the Prometheus Operator",
			prometheus: &monitoring.Prometheus{Enabled: true},
			wantErr:    "requires the Prometheus Operator",
		},
		{
			name:       "invalid interval",
			prometheus: &monitoring.Prometheus{Enabled: true, Interval: "30"},
			withCRD:    true,
			wantErr:    "monitoring.prometheus.interval",
		},
		{
			name:       "invalid label key",
			prometheus: &monitoring.Prometheus{Enabled: true, PodMonitorLabels: map[string]string{"bad key": "x"}},
			withCRD:    true,
			wantErr:    "podMonitorLabels key",
		},
		{
			name:       "invalid label value",
			prometheus: &monitoring.Prometheus{Enabled: true, PodMonitorLabels: map[string]string{"release": "not valid!"}},
			withCRD:    true,
			wantErr:    "podMonitorLabels[\"release\"]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := meta.NewDefaultRESTMapper(nil)
			if tt.withCRD {
				mapper.Add(monitoringv1.SchemeGroupVersion.WithKind(monitoringv1.PodMonitorsKind), meta.RESTScopeNamespace)
			}
			instance := &corev1alpha1.Instance{
				ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
				Spec:       monitoringInstanceSpec(t, tt.prometheus),
			}
			c := controller.NewContext(context.Background(), fake.NewClientBuilder().WithRESTMapper(mapper).Build(), instance, common.ProviderName)

			err := validateMonitoring(c)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
