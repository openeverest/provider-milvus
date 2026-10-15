package provider

import (
	"fmt"
	"net"
	"strconv"

	corev1 "k8s.io/api/core/v1"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/dependencies"
)

// storageLocationSecretSuffix names the Secret that carries a storage
// location's keys under the names the operator reads.
const storageLocationSecretSuffix = "-object-storage"

func storageLocationSecretName(instance string) string {
	return instance + storageLocationSecretSuffix
}

// defaultStorageLocationRootPath keeps instances sharing a bucket apart from
// each other and from their backups (everest/milvus/...).
func defaultStorageLocationRootPath(namespace, instance string) string {
	return fmt.Sprintf("milvus/%s/%s", namespace, instance)
}

// resolveStorageLocation turns a storage location reference into the external
// storage settings it stands for; other parameters pass through unchanged.
func resolveStorageLocation(c *controller.Context, param *dependencies.Storage) (*dependencies.Storage, error) {
	if param == nil || param.BackupStorageName == "" {
		return param, nil
	}
	bs, err := c.BackupStorage(param.BackupStorageName)
	if err != nil {
		return nil, fmt.Errorf("storage location: %w", err)
	}
	store, err := backupStorageObjectStore(bs, "")
	if err != nil {
		return nil, fmt.Errorf("storage location: %w", err)
	}
	resolved := *param
	resolved.External = true
	resolved.Endpoint = net.JoinHostPort(store.Address, strconv.Itoa(store.Port))
	resolved.Type = "MinIO"
	if store.Provider == "aws" {
		resolved.Type = "S3"
	}
	resolved.Bucket = store.Bucket
	resolved.Region = store.Region
	resolved.UseSSL = store.UseSSL
	resolved.CredentialsSecret = storageLocationSecretName(c.Name())
	if resolved.RootPath == "" {
		resolved.RootPath = defaultStorageLocationRootPath(c.Namespace(), c.Name())
	}
	return &resolved, nil
}

// storageLocationKeys reads the storage location's access keys.
func storageLocationKeys(c *controller.Context, name string) (accessKey, secretKey string, err error) {
	bs, err := c.BackupStorage(name)
	if err != nil {
		return "", "", fmt.Errorf("storage location: %w", err)
	}
	accessKey, secretKey, err = c.BackupStorageCredentials(bs)
	if err != nil {
		return "", "", fmt.Errorf("storage location %q: %w", name, err)
	}
	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("storage location %q: its credentials Secret has no AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY", name)
	}
	return accessKey, secretKey, nil
}

// syncStorageLocationSecret copies the storage location's keys into the
// Secret the operator reads; it only accepts accesskey/secretkey.
func syncStorageLocationSecret(c *controller.Context, param *dependencies.Storage) error {
	if param == nil || param.BackupStorageName == "" {
		return nil
	}
	accessKey, secretKey, err := storageLocationKeys(c, param.BackupStorageName)
	if err != nil {
		return err
	}
	return c.Apply(&corev1.Secret{
		ObjectMeta: c.ObjectMeta(storageLocationSecretName(c.Name())),
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			storageSecretKeyUser:     []byte(accessKey),
			storageSecretKeyPassword: []byte(secretKey),
		},
	})
}
