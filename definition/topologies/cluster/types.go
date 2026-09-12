package cluster

import "github.com/openeverest/provider-milvus/definition/dependencies"

// ClusterTopologyParameters holds optional configuration for the
// cluster topology.
type ClusterTopologyParameters struct {
	// Dependencies configures the operator-managed dependencies (etcd, Pulsar,
	// MinIO) for the cluster deployment.
	Dependencies *ClusterDependencies `json:"dependencies,omitempty"`
}

// ClusterDependencies groups the dependency configuration available in cluster
// mode. Cluster Milvus uses Pulsar as its message stream.
type ClusterDependencies struct {
	// Etcd configures the metadata store.
	Etcd *dependencies.Etcd `json:"etcd,omitempty"`
	// Pulsar configures the message stream.
	Pulsar *dependencies.Pulsar `json:"pulsar,omitempty"`
	// Storage configures the MinIO object storage.
	Storage *dependencies.Storage `json:"storage,omitempty"`
}
