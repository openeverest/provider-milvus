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
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

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
	assert.False(t, status.Converged)
	assert.Equal(t, "a Milvus dependency is not ready: StorageReady: dial tcp: connection refused", status.Message)
	assert.Equal(t, controller.AvailabilityUnknown, status.Available)
	assert.Equal(t, reasonDependencyNotReady, status.AvailableReason)
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
	// Clients still connect, but Milvus does not run every pod it asks for.
	assert.False(t, status.Converged)
	assert.Equal(t, controller.Available, status.Available)
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
	assert.True(t, status.Converged)
	assert.Equal(t, controller.Available, status.Available)
	assert.Empty(t, status.Message)
}

// Seen live: with etcd gone the operator still reported Healthy for about
// 45s while the standalone pod was already unready.
func TestStatusWorkloadWithoutReadyPodsIsNotReady(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})
	require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(milvusapi.StatusHealthy, testEndpoint)))
	require.NoError(t, c.Client().Create(context.Background(), milvusDeployment("test-milvus-milvus-standalone", "standalone", 1, 0)))
	require.NoError(t, c.Client().Create(context.Background(), releaseStatefulSet("test-milvus-etcd", "test-milvus-etcd", 3, 2)))

	status, err := New().Status(c)
	require.NoError(t, err)
	assert.False(t, status.Converged)
	assert.Equal(t, controller.Unavailable, status.Available)
	assert.Equal(t, reasonComponentsNotReady, status.AvailableReason)
	assert.Equal(t, "Milvus is unavailable: etcd 2/3 ready, standalone 0/1 ready", status.Message)
	assert.True(t, status.ConnectionDetails.IsEmpty())
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

// The runtime reports the seeding itself; clients get the connection details
// only once it has finished.
func TestStatusWhileSeeding(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})
	require.NoError(t, c.Client().Create(context.Background(), newMilvusCR(milvusapi.StatusHealthy, testEndpoint)))
	c.SetDataSourceStatus(controller.DataSourceStatus{State: controller.DataSourceStateRestoring, Message: "restoring backup b1"})

	status, err := New().Status(c)
	require.NoError(t, err)
	assert.False(t, status.Converged)
	assert.Equal(t, "restoring backup b1", status.Message)
	assert.Empty(t, status.Operations)
	assert.True(t, status.ConnectionDetails.IsEmpty())
}

// Standalone Milvus is recreated on a change: while its only pod is replaced,
// clients cannot connect even though the operator still says Healthy.
func TestStatusRolloutWithoutReadyPodsIsUnavailable(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})
	cr := newMilvusCR(milvusapi.StatusHealthy, testEndpoint)
	cr.Spec.Com.Image, cr.Status.CurrentImage = "milvusdb/milvus:v2.6.27", "milvusdb/milvus:v2.6.25"
	cr.Status.Conditions = []milvusapi.MilvusCondition{{
		Type: milvusapi.ConditionMilvusUpdated, Status: corev1.ConditionFalse, Message: "Milvus is performing rolling upgrade",
	}}
	require.NoError(t, c.Client().Create(context.Background(), cr))
	require.NoError(t, c.Client().Create(context.Background(), milvusDeployment("test-milvus-milvus-standalone", "standalone", 1, 0)))

	status, err := New().Status(c)
	require.NoError(t, err)
	assert.False(t, status.Converged)
	assert.Equal(t, controller.Unavailable, status.Available)
	assert.Equal(t, reasonComponentsNotReady, status.AvailableReason)
	assert.True(t, status.ConnectionDetails.IsEmpty())
	require.Len(t, status.Operations, 1)
	assert.Equal(t, corev1alpha1.ConditionUpgrading, status.Operations[0].Type)
}
