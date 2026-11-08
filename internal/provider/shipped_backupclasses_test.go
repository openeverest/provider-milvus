package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	s3class "github.com/openeverest/openeverest/v2/backupstorageclasses/s3"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/backupclasses"
)

func TestShippedBackupClasses(t *testing.T) {
	t.Parallel()

	p := New()
	classes := p.ShippedBackupClasses()
	require.Len(t, classes, 1)
	class := classes[0]
	assert.Equal(t, backupClassName, class.Name)
	assert.Equal(t, p.Name(), class.Spec.ExecutorProvider())

	entry := class.Spec.FindBackupStorageClass(s3class.ClassName)
	require.NotNil(t, entry)
	assert.Equal(t, &backupv1alpha1.BackupStorageTLSSupport{
		CustomCA:           backupv1alpha1.TLSOptionNone,
		InsecureSkipVerify: backupv1alpha1.TLSOptionNone,
	}, entry.TLS)
	assert.Equal(t, controller.TLSSupport{}, backupclasses.TLSSupport(backupClassName, s3class.ClassName))
}
