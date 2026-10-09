// Package monitoring contains the shared parameter types that control how the
// instance's metrics are collected. They are embedded into the per-topology
// parameter structs and converted to OpenAPI schema during generation.
//
// +k8s:openapi-gen=true
package monitoring

// Monitoring configures metrics collection for the instance.
type Monitoring struct {
	// Prometheus configures scraping by the Prometheus Operator.
	Prometheus *Prometheus `json:"prometheus,omitempty"`
}

// Prometheus creates a PodMonitor that scrapes the metrics endpoint of every
// Milvus component pod. Requires the Prometheus Operator CRDs in the cluster.
type Prometheus struct {
	// Enabled creates the PodMonitor; disabling it removes the PodMonitor.
	Enabled bool `json:"enabled,omitempty"`
	// Interval between scrapes, e.g. 30s. Defaults to the Prometheus global
	// scrape interval.
	Interval string `json:"interval,omitempty"`
	// PodMonitorLabels are added to the PodMonitor so that the Prometheus
	// podMonitorSelector picks it up (e.g. release: kube-prometheus-stack).
	PodMonitorLabels map[string]string `json:"podMonitorLabels,omitempty"`
}
