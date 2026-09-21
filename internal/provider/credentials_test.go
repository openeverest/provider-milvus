package provider

import (
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func TestGeneratePassword(t *testing.T) {
	pw, err := generatePassword()
	require.NoError(t, err)

	assert.Len(t, pw, passwordLength)
	assert.LessOrEqual(t, len(pw), 72, "must fit Milvus defaultRootPassword limit")
	assert.Contains(t, passwordLetters, string(pw[0]), "must start with a letter")

	other, err := generatePassword()
	require.NoError(t, err)
	assert.NotEqual(t, pw, other, "passwords must be random")
}

func TestEnsureCredentialsGeneratesAndPersists(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})

	username, password, err := ensureCredentials(c)
	require.NoError(t, err)
	assert.Equal(t, rootUsername, username)
	assert.NotEmpty(t, password)

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, credentialsSecretName(c.Name())))
	assert.Equal(t, rootUsername, string(secret.Data[secretKeyUsername]))
	assert.Equal(t, password, string(secret.Data[secretKeyPassword]))
}

func TestEnsureCredentialsIsStableAcrossCalls(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{})

	_, first, err := ensureCredentials(c)
	require.NoError(t, err)
	_, second, err := ensureCredentials(c)
	require.NoError(t, err)

	assert.Equal(t, first, second, "existing password must be reused")
}

func TestApplyAuthConfig(t *testing.T) {
	t.Run("seeds auth into empty config", func(t *testing.T) {
		spec := &milvusapi.MilvusSpec{}
		applyAuthConfig(spec, "s3cret")

		security := spec.Conf["common"].(map[string]any)["security"].(map[string]any)
		assert.Equal(t, true, security["authorizationEnabled"])
		assert.Equal(t, "s3cret", security["defaultRootPassword"])
	})

	t.Run("preserves user-provided config", func(t *testing.T) {
		spec := &milvusapi.MilvusSpec{
			Conf: milvusapi.Values{
				"log":    map[string]any{"level": "debug"},
				"common": map[string]any{"gracefulTime": float64(5000)},
			},
		}
		applyAuthConfig(spec, "s3cret")

		assert.Equal(t, map[string]any{"level": "debug"}, spec.Conf["log"])
		common := spec.Conf["common"].(map[string]any)
		assert.Equal(t, float64(5000), common["gracefulTime"])
		security := common["security"].(map[string]any)
		assert.Equal(t, true, security["authorizationEnabled"])
		assert.Equal(t, "s3cret", security["defaultRootPassword"])
	})
}
