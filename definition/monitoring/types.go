// Package monitoring contains the shared parameter types of the deprecated
// Prometheus integration. They are embedded into the per-topology parameter
// structs and converted to OpenAPI schema during generation.
//
// +k8s:openapi-gen=true
package monitoring

// Monitoring is deprecated and has no effect.
type Monitoring struct {
	// Deprecated: Prometheus has no effect. The provider publishes its
	// metrics endpoints in status.monitoring.sources.metrics; send them to a
	// monitoring destination listed in spec.monitoring.destinations.
	Prometheus *Prometheus `json:"prometheus,omitempty"`
}

// Prometheus is deprecated and has no effect. It is accepted so Instances
// written by earlier releases stay valid.
type Prometheus struct {
	// Deprecated: Enabled has no effect.
	Enabled bool `json:"enabled,omitempty"`
	// Deprecated: Interval has no effect.
	Interval string `json:"interval,omitempty"`
	// Deprecated: PodMonitorLabels has no effect.
	PodMonitorLabels map[string]string `json:"podMonitorLabels,omitempty"`
}
