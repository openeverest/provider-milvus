package cluster

import (
	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/monitoring"
)

// ClusterTopologyParameters holds optional configuration for the
// cluster topology.
type ClusterTopologyParameters struct {
	// Dependencies configures the operator-managed dependencies (etcd, Pulsar,
	// MinIO) for the cluster deployment.
	Dependencies *ClusterDependencies `json:"dependencies,omitempty"`
	// Monitoring configures metrics collection.
	Monitoring *monitoring.Monitoring `json:"monitoring,omitempty"`
}

// ClusterDependencies groups the dependency configuration available in cluster
// mode.
type ClusterDependencies struct {
	// Etcd configures the metadata store.
	Etcd *dependencies.Etcd `json:"etcd,omitempty"`
	// MessageStreamType selects the write-ahead log: "woodpecker" keeps it in
	// the object storage (no extra dependency), "pulsar" deploys or connects to
	// Pulsar. New instances default to woodpecker; it cannot be changed later.
	MessageStreamType string `json:"messageStreamType,omitempty"`
	// Pulsar configures the Pulsar message stream. Used only when
	// MessageStreamType is pulsar.
	Pulsar *dependencies.Pulsar `json:"pulsar,omitempty"`
	// Storage configures the MinIO object storage.
	Storage *dependencies.Storage `json:"storage,omitempty"`
	// StorageClassName is the StorageClass of every bundled dependency volume
	// (etcd, MinIO and Pulsar). Empty uses the cluster default. Cannot be
	// changed after creation.
	StorageClassName string `json:"storageClassName,omitempty"`
}
