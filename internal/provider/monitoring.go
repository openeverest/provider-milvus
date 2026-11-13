package provider

import (
	"fmt"
	"maps"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/monitoring"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// The operator exposes these on every Milvus container, whether or not it
// manages a PodMonitor itself.
const (
	milvusMetricsPortName = "metrics"
	milvusMetricsPath     = "/metrics"
)

var _ controller.MonitoringSourcesProvider = (*Provider)(nil)

// MonitoringSources implements controller.MonitoringSourcesProvider. Milvus
// serves OpenMetrics itself on every component (metrics: Native in
// definition/monitoring.yaml), so the endpoints need no exporter and are
// published whether or not a destination asks for metrics. They are
// published once Milvus has come up, and stay published while later changes
// roll its pods; a stopped or deleted instance publishes none. Monitoring
// extensions, or the user's own scraper, read them from
// Instance.status.monitoring.sources.metrics.
func (p *Provider) MonitoringSources(c *controller.Context) (*corev1alpha1.MonitoringSources, error) {
	sources := &corev1alpha1.MonitoringSources{}
	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return sources, nil
		}
		return nil, fmt.Errorf("get Milvus %q: %w", c.Name(), err)
	}
	// The operator reports Pending only until Milvus first comes up, and
	// Healthy or Unhealthy from then on, mid-rollout too.
	if cr.Status.Status != milvusapi.StatusHealthy && cr.Status.Status != milvusapi.StatusUnhealthy {
		return sources, nil
	}
	components := milvusComponentSpecs(cr.Spec.Com)
	for _, name := range slices.Sorted(maps.Keys(components)) {
		sources.Metrics = append(sources.Metrics, corev1alpha1.MetricsEndpoint{
			Component: name,
			PodSelector: map[string]string{
				controller.InstanceLabel:  c.Name(),
				controller.ComponentLabel: name,
			},
			Port:   intstr.FromString(milvusMetricsPortName),
			Path:   milvusMetricsPath,
			Scheme: "http",
		})
	}
	return sources, nil
}

// podMonitorGVK is the Prometheus Operator PodMonitor, deleted only as
// metadata so the provider needs neither its types nor its CRD.
var podMonitorGVK = schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "PodMonitor"}

// deleteLegacyPodMonitor removes the PodMonitor that releases before the
// monitoring contract created for the monitoring.prometheus parameter; a
// monitoring extension now creates the scrape objects from the published
// sources. Clusters without the Prometheus Operator CRDs are skipped.
// TODO: drop once no supported release created PodMonitors.
func deleteLegacyPodMonitor(c *controller.Context) error {
	pm := &metav1.PartialObjectMetadata{ObjectMeta: c.ObjectMeta(legacyPodMonitorName(c.Name()))}
	pm.SetGroupVersionKind(podMonitorGVK)
	if err := c.Delete(pm); err != nil && !meta.IsNoMatchError(err) {
		return fmt.Errorf("delete legacy PodMonitor: %w", err)
	}
	return nil
}

// legacyPodMonitorName is the name of the PodMonitor earlier releases created.
func legacyPodMonitorName(instanceName string) string {
	return instanceName + "-metrics"
}

// topologyMonitoring reads only the monitoring block, which both topologies
// share.
type topologyMonitoring struct {
	Monitoring *monitoring.Monitoring `json:"monitoring,omitempty"`
}

// legacyPrometheusPath is the deprecated parameter earlier releases scraped
// metrics with.
var legacyPrometheusPath = topologyParametersPath.Child("monitoring", "prometheus")

// monitoringWarnings tells the user that the deprecated Prometheus parameter
// no longer does anything. It stays in the schema so Instances that set it
// remain valid.
func monitoringWarnings(in *corev1alpha1.Instance) []string {
	var params topologyMonitoring
	if !tryDecodeTopologyParameters(in, &params) || params.Monitoring == nil ||
		params.Monitoring.Prometheus == nil || !params.Monitoring.Prometheus.Enabled { //nolint:staticcheck // warns about the deprecated parameter
		return nil
	}
	return []string{legacyPrometheusPath.String() + " is deprecated and has no effect: the provider no longer " +
		"creates a PodMonitor. Its metrics endpoints are listed in status.monitoring.sources.metrics; send them " +
		"to a monitoring destination in spec.monitoring.destinations, or point your own scraper at them."}
}
