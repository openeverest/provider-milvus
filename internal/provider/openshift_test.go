package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
)

func openShiftContext(t *testing.T, instance *corev1alpha1.Instance, objs ...client.Object) *controller.Context {
	t.Helper()
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(sccKind.WithVersion("v1"), meta.RESTScopeRoot)
	k8s := fakeClientBuilder(t).WithRESTMapper(mapper).WithObjects(append([]client.Object{instance}, objs...)...).Build()
	return controller.NewContext(context.Background(), k8s, instance, common.ProviderName)
}

func TestBackupJobsRunAs65532(t *testing.T) {
	c := newScheduleContext(t, scheduledInstance(daily(nil)), backupFixtures("2.6.15")...)
	_, err := New().SyncBackup(c, newBackup("daily-1", nil))
	require.NoError(t, err)
	require.NoError(t, syncBackupSchedules(c))

	for name, pod := range map[string]corev1.PodSpec{
		"job":     getJob(t, c, "daily-1-mb").Spec.Template.Spec,
		"cronjob": getCronJob(t, c, "test-milvus-daily").Spec.JobTemplate.Spec.Template.Spec,
	} {
		// The image has no USER, so runAsNonRoot can only be verified with a numeric one.
		assert.Equal(t, ptr.To(int64(65532)), pod.SecurityContext.RunAsUser, name)
		assert.Equal(t, ptr.To(int64(65532)), pod.SecurityContext.RunAsGroup, name)
	}
}

func TestBackupJobsLeaveIDsToOpenShift(t *testing.T) {
	c := openShiftContext(t, scheduledInstance(daily(nil)), backupFixtures("2.6.15")...)
	_, err := New().SyncBackup(c, newBackup("daily-1", nil))
	require.NoError(t, err)
	require.NoError(t, syncBackupSchedules(c))

	for name, pod := range map[string]corev1.PodSpec{
		"job":     getJob(t, c, "daily-1-mb").Spec.Template.Spec,
		"cronjob": getCronJob(t, c, "test-milvus-daily").Spec.JobTemplate.Spec.Template.Spec,
	} {
		assert.Nil(t, pod.SecurityContext.RunAsUser, "%s: restricted-v2 rejects UIDs outside the namespace range", name)
		assert.Nil(t, pod.SecurityContext.RunAsGroup, name)
		assert.Equal(t, ptr.To(true), pod.SecurityContext.RunAsNonRoot, name)
		assert.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, pod.SecurityContext.SeccompProfile.Type, name)
	}
}
