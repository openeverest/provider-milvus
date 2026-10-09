package provider

import (
	"fmt"
	"maps"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/prometheus/common/model"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/monitoring"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// The operator exposes these on every Milvus container, whether or not it
// manages a PodMonitor itself.
const (
	milvusMetricsPortName = "metrics"
	milvusMetricsPath     = "/metrics"
)

// topologyMonitoring reads only the monitoring block, which both topologies
// share, so a malformed dependency block cannot hide it.
type topologyMonitoring struct {
	Monitoring *monitoring.Monitoring `json:"monitoring,omitempty"`
}

func prometheusParams(c *controller.Context) *monitoring.Prometheus {
	var params topologyMonitoring
	if !c.TryDecodeTopologyParameters(&params) || params.Monitoring == nil {
		return nil
	}
	return params.Monitoring.Prometheus
}

func prometheusEnabled(params *monitoring.Prometheus) bool {
	return params != nil && params.Enabled
}

func podMonitorName(instanceName string) string {
	return instanceName + "-metrics"
}

// validateMonitoring rejects settings the PodMonitor CRD would refuse and an
// enabled integration on a cluster without the Prometheus Operator.
func validateMonitoring(c *controller.Context) error {
	var params topologyMonitoring
	if err := decodeTopologyParametersIfPresent(c, &params); err != nil {
		return err
	}
	if params.Monitoring == nil || !prometheusEnabled(params.Monitoring.Prometheus) {
		return nil
	}
	prometheus := params.Monitoring.Prometheus
	if prometheus.Interval != "" {
		if _, err := model.ParseDuration(prometheus.Interval); err != nil {
			return fmt.Errorf("monitoring.prometheus.interval %q is not a valid duration such as 30s or 1m", prometheus.Interval)
		}
	}
	for key, value := range prometheus.PodMonitorLabels {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return fmt.Errorf("monitoring.prometheus.podMonitorLabels key %q: %s", key, errs[0])
		}
		if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
			return fmt.Errorf("monitoring.prometheus.podMonitorLabels[%q] value %q: %s", key, value, errs[0])
		}
	}
	podMonitorKind := monitoringv1.SchemeGroupVersion.WithKind(monitoringv1.PodMonitorsKind)
	if _, err := c.Client().RESTMapper().RESTMapping(podMonitorKind.GroupKind(), podMonitorKind.Version); err != nil {
		if meta.IsNoMatchError(err) {
			return fmt.Errorf("prometheus monitoring requires the Prometheus Operator: the PodMonitor CRD (monitoring.coreos.com/v1) is not installed")
		}
		return fmt.Errorf("look up the PodMonitor CRD: %w", err)
	}
	return nil
}

// syncPodMonitor creates the PodMonitor that scrapes every Milvus component,
// or removes it once the Prometheus integration is turned off.
func syncPodMonitor(c *controller.Context) error {
	if err := deleteOperatorPodMonitor(c); err != nil {
		return err
	}

	name := podMonitorName(c.Name())
	params := prometheusParams(c)
	if !prometheusEnabled(params) {
		err := c.Delete(&monitoringv1.PodMonitor{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace()}})
		if err != nil && !meta.IsNoMatchError(err) {
			return fmt.Errorf("delete PodMonitor %q: %w", name, err)
		}
		return nil
	}

	if err := c.Apply(milvusPodMonitor(c.ObjectMeta(name), c.Name(), params)); err != nil {
		if meta.IsNoMatchError(err) {
			return fmt.Errorf("prometheus monitoring requires the Prometheus Operator PodMonitor CRD (monitoring.coreos.com/v1): %w", err)
		}
		return fmt.Errorf("apply PodMonitor %q: %w", name, err)
	}
	return nil
}

// deleteOperatorPodMonitor removes the PodMonitor the Milvus operator created
// before the provider turned its metrics off, so a disabled instance is not
// scraped and an enabled one is not scraped twice.
func deleteOperatorPodMonitor(c *controller.Context) error {
	legacy := &monitoringv1.PodMonitor{}
	if err := c.Get(legacy, c.Name()); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("get PodMonitor %q: %w", c.Name(), err)
	}
	owner := metav1.GetControllerOf(legacy)
	if owner == nil || owner.APIVersion != milvusapi.GroupVersion || owner.Kind != "Milvus" {
		return nil
	}
	// The UID precondition keeps a stale cache from deleting a recreated object.
	err := c.Client().Delete(c.Context(), legacy, client.Preconditions{UID: &legacy.UID})
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return fmt.Errorf("delete operator PodMonitor %q: %w", c.Name(), err)
	}
	return nil
}

// milvusPodMonitor selects the pods the provider labels for every component,
// which leaves out the bundled etcd, MinIO and Pulsar. User labels never
// override the managed ones, which identify the owning Instance.
func milvusPodMonitor(objectMeta metav1.ObjectMeta, instanceName string, params *monitoring.Prometheus) *monitoringv1.PodMonitor {
	labels := maps.Clone(params.PodMonitorLabels)
	if labels == nil {
		labels = map[string]string{}
	}
	maps.Copy(labels, objectMeta.Labels)
	objectMeta.Labels = labels

	return &monitoringv1.PodMonitor{
		ObjectMeta: objectMeta,
		Spec: monitoringv1.PodMonitorSpec{
			Selector: metav1.LabelSelector{MatchLabels: map[string]string{
				controller.ProviderLabel: common.ProviderName,
				controller.InstanceLabel: instanceName,
			}},
			PodTargetLabels: []string{controller.ComponentLabel},
			PodMetricsEndpoints: []monitoringv1.PodMetricsEndpoint{{
				Port:     new(milvusMetricsPortName),
				Path:     milvusMetricsPath,
				Interval: monitoringv1.Duration(params.Interval),
			}},
		},
	}
}
