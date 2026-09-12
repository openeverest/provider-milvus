package standalone

import "github.com/openeverest/provider-milvus/definition/dependencies"

// StandaloneTopologyParameters holds optional configuration for the
// standalone topology.
type StandaloneTopologyParameters struct {
	// Dependencies configures the operator-managed dependencies (etcd, MinIO)
	// for the standalone deployment. Standalone Milvus uses an embedded
	// message stream (rocksmq), so Pulsar is not configurable here.
	Dependencies *StandaloneDependencies `json:"dependencies,omitempty"`
}

// StandaloneDependencies groups the dependency configuration available in
// standalone mode.
type StandaloneDependencies struct {
	// Etcd configures the metadata store.
	Etcd *dependencies.Etcd `json:"etcd,omitempty"`
	// Storage configures the MinIO object storage.
	Storage *dependencies.Storage `json:"storage,omitempty"`
}
