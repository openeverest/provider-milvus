package provider

import (
	"crypto/rand"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

const (
	// rootUsername is the built-in Milvus superuser created on first boot.
	rootUsername = "root"
	// credentialsSecretSuffix is appended to the Instance name to form the
	// provider-owned Secret that persists the generated root password.
	credentialsSecretSuffix = "-credentials"
	// secretKeyUsername and secretKeyPassword are the keys under which the
	// credentials are stored in the Secret.
	secretKeyUsername = "username"
	secretKeyPassword = "password"
	// passwordLength is the generated root password length. Milvus caps
	// defaultRootPassword at 72 characters.
	passwordLength = 24
)

// passwordAlphabet excludes ambiguous characters and starts every generated
// password with a letter, satisfying Milvus password rules and avoiding YAML
// numeric-precision issues when the password is embedded into spec.config.
const (
	passwordLetters  = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
	passwordAlphabet = passwordLetters + "23456789"
)

func credentialsSecretName(instanceName string) string {
	return instanceName + credentialsSecretSuffix
}

// generatePassword returns a random password that starts with a letter and
// draws the remaining characters from an unambiguous alphanumeric alphabet.
func generatePassword() (string, error) {
	out := make([]byte, passwordLength)
	if err := fillFromAlphabet(out[:1], passwordLetters); err != nil {
		return "", err
	}
	if err := fillFromAlphabet(out[1:], passwordAlphabet); err != nil {
		return "", err
	}
	return string(out), nil
}

func fillFromAlphabet(dst []byte, alphabet string) error {
	buf := make([]byte, len(dst))
	if _, err := rand.Read(buf); err != nil {
		return fmt.Errorf("generate password: %w", err)
	}
	for i, b := range buf {
		dst[i] = alphabet[int(b)%len(alphabet)]
	}
	return nil
}

// ensureCredentials returns the root credentials for the instance, creating the
// backing Secret with a freshly generated password on first reconcile. The
// password is generated once and reused on every subsequent call so it stays
// stable across reconciles and matches the value seeded into Milvus.
func ensureCredentials(c *controller.Context) (username, password string, err error) {
	secretName := credentialsSecretName(c.Name())

	existing := &corev1.Secret{}
	if getErr := c.Get(existing, secretName); getErr == nil {
		if pw := string(existing.Data[secretKeyPassword]); pw != "" {
			return rootUsername, pw, nil
		}
	}

	password, err = generatePassword()
	if err != nil {
		return "", "", err
	}

	secret := &corev1.Secret{
		ObjectMeta: c.ObjectMeta(secretName),
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			secretKeyUsername: []byte(rootUsername),
			secretKeyPassword: []byte(password),
		},
	}
	if err := c.Apply(secret); err != nil {
		return "", "", fmt.Errorf("apply credentials secret: %w", err)
	}
	return rootUsername, password, nil
}

// applyAuthConfig enables authentication and seeds the initial root password on
// the Milvus spec by deep-merging into spec.config. defaultRootPassword only
// takes effect on first initialization; the operator carries it forward on
// every reconcile without rotating an already-initialized root user.
func applyAuthConfig(spec *milvusapi.MilvusSpec, password string) {
	if spec.Conf == nil {
		spec.Conf = milvusapi.Values{}
	}
	deepMergeValues(spec.Conf, map[string]any{
		"common": map[string]any{
			"security": map[string]any{
				"authorizationEnabled": true,
				"defaultRootPassword":  password,
			},
		},
	})
}
