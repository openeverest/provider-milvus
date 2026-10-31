package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/definition/dependencies"
	"github.com/openeverest/provider-milvus/definition/topologies/cluster"
	"github.com/openeverest/provider-milvus/definition/topologies/standalone"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func storageLocation(endpointURL string, insecureSkipVerify bool) *backupv1alpha1.BackupStorage {
	bs := &backupv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-prod", Namespace: "db"},
		Spec: backupv1alpha1.BackupStorageSpec{
			BackupStorageClassRef: commonv1alpha1.ObjectRef{Name: "s3"},
			Location:              backupv1alpha1.BackupStorageLocation{ObjectStore: &backupv1alpha1.ObjectStoreLocation{Bucket: "vectors"}},
			Parameters:            &runtime.RawExtension{Raw: []byte(`{"region":"eu-west-1","endpointURL":"` + endpointURL + `"}`)},
			Authentication: commonv1alpha1.Authentication{
				Mode:                 "AccessKey",
				CredentialsSecretRef: &commonv1alpha1.SecretRef{Name: "s3-prod-keys"},
			},
		},
	}
	if insecureSkipVerify {
		bs.Spec.TLS = &commonv1alpha1.TLSConfig{InsecureSkipVerify: true}
	}
	return bs
}

func storageLocationCredentials(accessKey string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-prod-keys", Namespace: "db"},
		Data: map[string][]byte{
			"AWS_ACCESS_KEY_ID":     []byte(accessKey),
			"AWS_SECRET_ACCESS_KEY": []byte("secret"),
		},
	}
}

func onStorageLocation(t *testing.T, storage *dependencies.Storage) *runtime.RawExtension {
	return topologyParams(t, standalone.StandaloneTopologyParameters{
		Dependencies: &standalone.StandaloneDependencies{Storage: storage},
	})
}

func TestStorageLocation(t *testing.T) {
	t.Run("S3 location becomes external storage with mirrored keys", func(t *testing.T) {
		c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{
				Type:       "cluster",
				Parameters: topologyParams(t, cluster.ClusterTopologyParameters{Dependencies: &cluster.ClusterDependencies{Storage: &dependencies.Storage{BackupStorageName: "s3-prod"}}}),
			},
		}, storageLocation("https://s3.eu-west-1.amazonaws.com", false), storageLocationCredentials("AKIA"))
		require.NoError(t, validateInstance(c))
		require.NoError(t, New().Sync(c))

		cr := &milvusapi.Milvus{}
		require.NoError(t, c.Get(cr, c.Name()))
		assert.Equal(t, milvusapi.MilvusStorage{
			External: true, Endpoint: "s3.eu-west-1.amazonaws.com:443", Type: "S3", SecretRef: "test-milvus-object-storage",
		}, cr.Spec.Dep.Storage)
		assert.Equal(t, map[string]any{
			"bucketName": "vectors", "region": "eu-west-1", "useSSL": true, "rootPath": "milvus/db/test-milvus",
		}, cr.Spec.Conf["minio"])

		secret := &corev1.Secret{}
		require.NoError(t, c.Get(secret, "test-milvus-object-storage"))
		assert.Equal(t, map[string][]byte{"accesskey": []byte("AKIA"), "secretkey": []byte("secret")}, secret.Data)
		assert.Equal(t, "Instance", secret.OwnerReferences[0].Kind)
	})

	t.Run("plain HTTP MinIO location with a custom prefix", func(t *testing.T) {
		c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{
				Type:       "standalone",
				Parameters: onStorageLocation(t, &dependencies.Storage{BackupStorageName: "s3-prod", RootPath: "tenant-a"}),
			},
		}, storageLocation("http://minio.storage.svc", false), storageLocationCredentials("minio"))
		spec, err := BuildMilvusSpec(c)
		require.NoError(t, err)
		assert.Equal(t, "minio.storage.svc:80", spec.Dep.Storage.Endpoint)
		assert.Equal(t, "MinIO", spec.Dep.Storage.Type)
		assert.Equal(t, "tenant-a", spec.Conf["minio"].(map[string]any)["rootPath"])
		assert.Nil(t, spec.Dep.Storage.InCluster, "no bundled MinIO")
	})

	t.Run("rotated keys reach the mirrored Secret", func(t *testing.T) {
		keys := storageLocationCredentials("old")
		c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{
				Type:       "standalone",
				Parameters: onStorageLocation(t, &dependencies.Storage{BackupStorageName: "s3-prod"}),
			},
		}, storageLocation("https://minio.example.com", false), keys)
		require.NoError(t, New().Sync(c))
		keys.Data["AWS_ACCESS_KEY_ID"] = []byte("new")
		require.NoError(t, c.Client().Update(c.Context(), keys))
		require.NoError(t, New().Sync(c))

		secret := &corev1.Secret{}
		require.NoError(t, c.Get(secret, "test-milvus-object-storage"))
		assert.Equal(t, "new", string(secret.Data["accesskey"]))
	})
}

func TestValidateStorageLocation(t *testing.T) {
	tests := []struct {
		name    string
		storage *dependencies.Storage
		objects []client.Object
		wantErr string
	}{
		{
			name:    "missing location",
			storage: &dependencies.Storage{BackupStorageName: "s3-prod"},
			wantErr: `storage location: failed to get BackupStorage "s3-prod"`,
		},
		{
			name:    "location without keys",
			storage: &dependencies.Storage{BackupStorageName: "s3-prod"},
			objects: []client.Object{storageLocation("https://minio.example.com", false), storageLocationCredentials("")},
			wantErr: "needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
		},
		{
			name:    "TLS verification off",
			storage: &dependencies.Storage{BackupStorageName: "s3-prod"},
			objects: []client.Object{storageLocation("https://minio.example.com", true), storageLocationCredentials("k")},
			wantErr: "tls.insecureSkipVerify is not supported",
		},
		{
			name:    "mixed with raw external fields",
			storage: &dependencies.Storage{BackupStorageName: "s3-prod", Endpoint: "minio:9000"},
			objects: []client.Object{storageLocation("https://minio.example.com", false), storageLocationCredentials("k")},
			wantErr: "storage.backupStorageName takes the endpoint, bucket, region and credentials from the storage location",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContextWithObjects(t, corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone", Parameters: onStorageLocation(t, tt.storage)},
			}, tt.objects...)
			assert.ErrorContains(t, validateInstance(c), tt.wantErr)
		})
	}
}
