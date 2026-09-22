// Package dependencies contains shared parameter types describing the bundled
// dependencies (etcd, Pulsar, MinIO) that the Milvus operator deploys.
//
// These types are embedded into the per-topology parameter structs and are
// converted to OpenAPI schema during generation, so users can size and tune
// operator-managed dependencies or point Milvus at external equivalents.
//
// +k8s:openapi-gen=true
package dependencies

import (
	"bytes"
	"encoding/json"
)

// Quantity is a Kubernetes resource quantity such as "500m", "1", or "512Mi".
// It decodes from either a JSON string or a bare number, so numeric UI inputs
// (e.g. cpu: 0.1) do not fail the whole topology-parameters block.
type Quantity string

// UnmarshalJSON accepts both a quoted string and a bare JSON number.
func (q *Quantity) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*q = Quantity(s)
		return nil
	}
	*q = Quantity(data)
	return nil
}

// ResourceList maps CPU and memory quantities as Kubernetes resource strings
// (e.g. "500m", "2Gi"). Empty fields are omitted, letting the chart default apply.
type ResourceList struct {
	// CPU quantity, e.g. "500m" or "1".
	CPU Quantity `json:"cpu,omitempty"`
	// Memory quantity, e.g. "512Mi" or "2Gi".
	Memory Quantity `json:"memory,omitempty"`
}

// Resources holds resource requests and limits for a dependency component.
type Resources struct {
	// Requests is the minimum resources guaranteed to the component.
	Requests *ResourceList `json:"requests,omitempty"`
	// Limits is the maximum resources the component may use.
	Limits *ResourceList `json:"limits,omitempty"`
}

// Persistence sizes a single persistent volume of a bundled dependency.
type Persistence struct {
	// Size is the PVC size, e.g. "10Gi". Empty falls back to the per-topology
	// default so bundled dependencies never inherit the chart's oversized values.
	Size string `json:"size,omitempty"`
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
	// Persistence sizes the etcd data PVC. Ignored when External.
	Persistence *Persistence `json:"persistence,omitempty"`
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
	// Persistence sizes the MinIO data PVC. When unset it derives from the
	// data-bearing Milvus component's storage size. Ignored when External.
	Persistence *Persistence `json:"persistence,omitempty"`
}

// PulsarComponent sizes a stateless Pulsar sub-component (broker or proxy).
type PulsarComponent struct {
	// Replicas sets the sub-component replica count.
	Replicas *int32 `json:"replicas,omitempty"`
	// Resources sizes the sub-component pods.
	Resources *Resources `json:"resources,omitempty"`
}

// PulsarBookKeeper sizes the Pulsar bookkeeper (bookie) sub-component, whose
// pods carry two persistent volumes: a write-ahead journal and the ledger store.
type PulsarBookKeeper struct {
	// Replicas sets the bookie replica count.
	Replicas *int32 `json:"replicas,omitempty"`
	// Resources sizes the bookie pods.
	Resources *Resources `json:"resources,omitempty"`
	// Journal sizes the bookie write-ahead journal PVC.
	Journal *Persistence `json:"journal,omitempty"`
	// Ledgers sizes the bookie ledger storage PVC.
	Ledgers *Persistence `json:"ledgers,omitempty"`
}

// PulsarZooKeeper sizes the Pulsar zookeeper sub-component, whose pods carry a
// single data persistent volume.
type PulsarZooKeeper struct {
	// Replicas sets the zookeeper replica count.
	Replicas *int32 `json:"replicas,omitempty"`
	// Resources sizes the zookeeper pods.
	Resources *Resources `json:"resources,omitempty"`
	// Data sizes the zookeeper data PVC.
	Data *Persistence `json:"data,omitempty"`
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
	BookKeeper *PulsarBookKeeper `json:"bookkeeper,omitempty"`
	// ZooKeeper sizes the Pulsar zookeeper sub-component. Ignored when External.
	ZooKeeper *PulsarZooKeeper `json:"zookeeper,omitempty"`
	// Proxy sizes the Pulsar proxy sub-component. Ignored when External.
	Proxy *PulsarComponent `json:"proxy,omitempty"`
}
