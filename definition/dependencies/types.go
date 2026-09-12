// Package dependencies contains shared parameter types describing the bundled
// dependencies (etcd, Pulsar, MinIO) that the Milvus operator deploys.
//
// These types are embedded into the per-topology parameter structs and are
// converted to OpenAPI schema during generation, so users can size and tune
// operator-managed dependencies or point Milvus at external equivalents.
//
// +k8s:openapi-gen=true
package dependencies

// ResourceList maps CPU and memory quantities as Kubernetes resource strings
// (e.g. "500m", "2Gi"). Empty fields are omitted, letting the chart default apply.
type ResourceList struct {
	// CPU quantity, e.g. "500m" or "1".
	CPU string `json:"cpu,omitempty"`
	// Memory quantity, e.g. "512Mi" or "2Gi".
	Memory string `json:"memory,omitempty"`
}

// Resources holds resource requests and limits for a dependency component.
type Resources struct {
	// Requests is the minimum resources guaranteed to the component.
	Requests *ResourceList `json:"requests,omitempty"`
	// Limits is the maximum resources the component may use.
	Limits *ResourceList `json:"limits,omitempty"`
}

// Etcd configures the etcd metadata store dependency.
type Etcd struct {
	// External disables the bundled etcd and points Milvus at the provided
	// Endpoints instead. Endpoints must be set when External is true.
	External bool `json:"external,omitempty"`
	// Endpoints lists external etcd endpoints (host:port). Used only when
	// External is true.
	Endpoints []string `json:"endpoints,omitempty"`
	// Replicas sets the bundled etcd cluster size. Ignored when External.
	Replicas *int32 `json:"replicas,omitempty"`
	// Resources sizes the bundled etcd pods. Ignored when External.
	Resources *Resources `json:"resources,omitempty"`
}

// Storage configures the MinIO object storage dependency.
type Storage struct {
	// External disables the bundled MinIO and points Milvus at the provided
	// Endpoint instead. Endpoint must be set when External is true.
	External bool `json:"external,omitempty"`
	// Endpoint is the external object storage endpoint (host:port). Used only
	// when External is true.
	Endpoint string `json:"endpoint,omitempty"`
	// Replicas sets the bundled MinIO server count. A value > 1 switches MinIO
	// to distributed mode. Ignored when External.
	Replicas *int32 `json:"replicas,omitempty"`
	// Resources sizes the bundled MinIO pods. Ignored when External.
	Resources *Resources `json:"resources,omitempty"`
}

// PulsarComponent sizes a single Pulsar sub-component (broker, bookkeeper,
// zookeeper or proxy).
type PulsarComponent struct {
	// Replicas sets the sub-component replica count.
	Replicas *int32 `json:"replicas,omitempty"`
	// Resources sizes the sub-component pods.
	Resources *Resources `json:"resources,omitempty"`
}

// Pulsar configures the Pulsar message-stream dependency (cluster mode only).
type Pulsar struct {
	// External disables the bundled Pulsar and points Milvus at the provided
	// Endpoint instead. Endpoint must be set when External is true.
	External bool `json:"external,omitempty"`
	// Endpoint is the external Pulsar endpoint (host:port). Used only when
	// External is true.
	Endpoint string `json:"endpoint,omitempty"`
	// Broker sizes the Pulsar broker sub-component. Ignored when External.
	Broker *PulsarComponent `json:"broker,omitempty"`
	// BookKeeper sizes the Pulsar bookkeeper (bookie) sub-component. Ignored
	// when External.
	BookKeeper *PulsarComponent `json:"bookkeeper,omitempty"`
	// ZooKeeper sizes the Pulsar zookeeper sub-component. Ignored when External.
	ZooKeeper *PulsarComponent `json:"zookeeper,omitempty"`
	// Proxy sizes the Pulsar proxy sub-component. Ignored when External.
	Proxy *PulsarComponent `json:"proxy,omitempty"`
}
