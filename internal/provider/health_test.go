package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

const testEndpoint = "my-endpoint.db.svc.cluster.local:19530"

func milvusDeployment(name, component string, replicas, ready int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "db", Labels: map[string]string{
			milvusInstanceLabel: "test-milvus", milvusNameLabel: "milvus", milvusComponentLabel: component,
		}},
		Spec:   appsv1.DeploymentSpec{Replicas: ptr.To(replicas)},
		Status: appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
}

func releaseStatefulSet(name, release string, replicas, ready int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "db", Annotations: map[string]string{
			helmReleaseAnnotation: release,
		}},
		Spec:   appsv1.StatefulSetSpec{Replicas: ptr.To(replicas)},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: ready},
	}
}

func TestStatusRequiresHealthyDependencies(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})
	cr := newMilvusCR(milvusapi.StatusHealthy, testEndpoint)
	// The operator stays Healthy while a dependency is down.
	cr.Status.Conditions = []milvusapi.MilvusCondition{
		{Type: "MilvusReady", Status: corev1.ConditionTrue},
		{Type: "EtcdReady", Status: corev1.ConditionTrue},
		{Type: "StorageReady", Status: corev1.ConditionFalse, Reason: "StorageNotReady", Message: "dial tcp: connection refused"},
	}
	require.NoError(t, c.Client().Create(context.Background(), cr))

	status, err := New().Status(c)
	require.NoError(t, err)
	assert.Equal(t, corev1alpha1.InstancePhaseProvisioning, status.Phase)
	assert.Equal(t, "a Milvus dependency is not ready: StorageReady: dial tcp: connection refused", status.Message)
}

func TestStatusReportsReducedRedundancy(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})
	require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(milvusapi.StatusHealthy, testEndpoint)))
	for _, obj := range []client.Object{
		milvusDeployment("test-milvus-milvus-querynode-0", "querynode", 2, 1),
		// The idle blue/green query node deployment.
		milvusDeployment("test-milvus-milvus-querynode-1", "querynode", 0, 0),
		milvusDeployment("test-milvus-milvus-proxy", "proxy", 2, 2),
		releaseStatefulSet("test-milvus-etcd", "test-milvus-etcd", 3, 2),
		releaseStatefulSet("test-milvus-pulsar-bookie", "test-milvus-pulsar", 2, 1),
		releaseStatefulSet("other-etcd", "other-etcd", 3, 0),
		releaseStatefulSet("unrelated", "unrelated", 1, 0),
	} {
		require.NoError(t, c.Client().Create(context.Background(), obj))
	}

	status, err := New().Status(c)
	require.NoError(t, err)
	assert.Equal(t, corev1alpha1.InstancePhaseReady, status.Phase)
	assert.Equal(t, "serving with reduced redundancy: etcd 2/3 ready, pulsar-bookie 1/2 ready, queryNode 1/2 ready", status.Message)
	assert.Equal(t, "http://"+testEndpoint, status.ConnectionDetails.URI)
}

func TestStatusReadyWithoutMessageWhenFullyReady(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})
	require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(milvusapi.StatusHealthy, testEndpoint)))
	require.NoError(t, c.Client().Create(context.Background(), milvusDeployment("test-milvus-milvus-standalone", "standalone", 1, 1)))
	require.NoError(t, c.Client().Create(context.Background(), releaseStatefulSet("test-milvus-etcd", "test-milvus-etcd", 1, 1)))

	status, err := New().Status(c)
	require.NoError(t, err)
	assert.Equal(t, corev1alpha1.InstancePhaseReady, status.Phase)
	assert.Empty(t, status.Message)
}

func TestEnqueueDependencyInstance(t *testing.T) {
	tests := []struct {
		release string
		want    []reconcile.Request
	}{
		{release: "docs-etcd", want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "db", Name: "docs"}}}},
		{release: "docs-etcd-minio", want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "db", Name: "docs-etcd"}}}},
		{release: "docs-pulsar", want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "db", Name: "docs"}}}},
		{release: "provider-milvus"},
		{release: ""},
	}
	for _, tt := range tests {
		t.Run(tt.release, func(t *testing.T) {
			obj := releaseStatefulSet("x", tt.release, 1, 1)
			assert.Equal(t, tt.want, enqueueDependencyInstance(context.Background(), obj))
		})
	}
}
