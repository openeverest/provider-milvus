package milvusapi

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const GroupVersion = "milvus.io/v1beta1"

var SchemeGroupVersion = schema.GroupVersion{Group: "milvus.io", Version: "v1beta1"}

// MilvusMode is the deployment mode for a Milvus instance.
type MilvusMode string

const (
	MilvusModeCluster    MilvusMode = "cluster"
	MilvusModeStandalone MilvusMode = "standalone"
)

// MilvusHealthStatus describes the observed health of the Milvus control plane.
type MilvusHealthStatus string

const (
	StatusPending   MilvusHealthStatus = "Pending"
	StatusHealthy   MilvusHealthStatus = "Healthy"
	StatusUnhealthy MilvusHealthStatus = "Unhealthy"
	StatusStopped   MilvusHealthStatus = "Stopped"
	StatusDeleting  MilvusHealthStatus = "Deleting"
)

// ComponentSpec holds shared image/version information for a Milvus component.
type ComponentSpec struct {
	Image     string                       `json:"image,omitempty"`
	Version   string                       `json:"version,omitempty"`
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// Component is a generic Milvus component with replicas and image metadata.
type Component struct {
	ComponentSpec `json:",inline"`
	Replicas      *int32 `json:"replicas,omitempty"`
}

// ServiceComponent adds a port number to a component.
type ServiceComponent struct {
	Component `json:",inline"`
	Port      int32 `json:"port,omitempty"`
}

// MilvusStandalone defines the standalone deployment component.
type MilvusStandalone struct {
	ServiceComponent `json:",inline"`
}

// MilvusProxy defines the proxy component in cluster mode.
type MilvusProxy struct {
	ServiceComponent `json:",inline"`
}

// MilvusRootCoord defines the root coordinator component.
type MilvusRootCoord struct {
	Component `json:",inline"`
}

// MilvusIndexCoord defines the index coordinator component.
type MilvusIndexCoord struct {
	Component `json:",inline"`
}

// MilvusDataCoord defines the data coordinator component.
type MilvusDataCoord struct {
	Component `json:",inline"`
}

// MilvusQueryCoord defines the query coordinator component.
type MilvusQueryCoord struct {
	Component `json:",inline"`
}

// MilvusIndexNode defines the index node component.
type MilvusIndexNode struct {
	Component `json:",inline"`
}

// MilvusDataNode defines the data node component.
type MilvusDataNode struct {
	Component `json:",inline"`
}

// MilvusQueryNode defines the query node component.
type MilvusQueryNode struct {
	Component `json:",inline"`
}

// MilvusComponents contains the concrete Milvus deployment components.
type MilvusComponents struct {
	ComponentSpec    `json:",inline"`
	Standalone       *MilvusStandalone `json:"standalone,omitempty"`
	Proxy            *MilvusProxy      `json:"proxy,omitempty"`
	RootCoord        *MilvusRootCoord  `json:"rootCoord,omitempty"`
	IndexCoord       *MilvusIndexCoord `json:"indexCoord,omitempty"`
	DataCoord        *MilvusDataCoord  `json:"dataCoord,omitempty"`
	QueryCoord       *MilvusQueryCoord `json:"queryCoord,omitempty"`
	IndexNode        *MilvusIndexNode  `json:"indexNode,omitempty"`
	DataNode         *MilvusDataNode   `json:"dataNode,omitempty"`
	QueryNode        *MilvusQueryNode  `json:"queryNode,omitempty"`
	EnableManualMode bool              `json:"enableManualMode,omitempty"`
}

// MilvusSpec is the desired state of a Milvus deployment.
type MilvusSpec struct {
	Mode MilvusMode          `json:"mode,omitempty"`
	Com  MilvusComponents    `json:"components,omitempty"`
	Dep  *MilvusDependencies `json:"dependencies,omitempty"`
	// Conf is the Milvus engine configuration, deep-merged into the generated
	// Milvus config file by the operator (maps to the CRD's spec.config).
	Conf Values `json:"config,omitempty"`
}

type Values map[string]any

type InClusterConfig struct {
	Values         Values `json:"values,omitempty"`
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
	PVCDeletion    bool   `json:"pvcDeletion,omitempty"`
}

// MilvusEtcd configures the etcd metadata store dependency.
type MilvusEtcd struct {
	Endpoints []string         `json:"endpoints,omitempty"`
	External  bool             `json:"external,omitempty"`
	InCluster *InClusterConfig `json:"inCluster,omitempty"`
}

// MilvusPulsar configures the Pulsar message-stream dependency (cluster mode).
type MilvusPulsar struct {
	InCluster *InClusterConfig `json:"inCluster,omitempty"`
	External  bool             `json:"external,omitempty"`
	Endpoint  string           `json:"endpoint,omitempty"`
}

type MilvusStorage struct {
	Type      string           `json:"type,omitempty"`
	SecretRef string           `json:"secretRef,omitempty"`
	Endpoint  string           `json:"endpoint,omitempty"`
	InCluster *InClusterConfig `json:"inCluster,omitempty"`
	External  bool             `json:"external,omitempty"`
}

type MilvusDependencies struct {
	Etcd          MilvusEtcd    `json:"etcd,omitempty"`
	MsgStreamType string        `json:"msgStreamType,omitempty"`
	Pulsar        MilvusPulsar  `json:"pulsar,omitempty"`
	Storage       MilvusStorage `json:"storage,omitempty"`
}

// MilvusStatus is the observed state of a Milvus deployment.
type MilvusStatus struct {
	Status   MilvusHealthStatus `json:"status,omitempty"`
	Endpoint string             `json:"endpoint,omitempty"`
}

// Milvus is a minimal CRD model used by the provider to render the Milvus control-plane resource.
type Milvus struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MilvusSpec   `json:"spec,omitempty"`
	Status            MilvusStatus `json:"status,omitempty"`
}

// MilvusList is the list type for a Milvus resource.
type MilvusList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Milvus `json:"items"`
}

func (m *Milvus) DeepCopyObject() runtime.Object {
	if m == nil {
		return nil
	}
	out := *m
	return &out
}

func (m *MilvusList) DeepCopyObject() runtime.Object {
	if m == nil {
		return nil
	}
	out := *m
	if m.Items != nil {
		out.Items = make([]Milvus, len(m.Items))
		copy(out.Items, m.Items)
	}
	return &out
}

func AddToScheme(scheme *runtime.Scheme) error {
	if scheme == nil {
		return nil
	}
	scheme.AddKnownTypes(SchemeGroupVersion, &Milvus{}, &MilvusList{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
