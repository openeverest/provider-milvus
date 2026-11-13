package provider

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/yaml"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

func tlsInstanceSpec(t *testing.T, topology string, params *definition.TLSParameters) corev1alpha1.InstanceSpec {
	t.Helper()
	spec := corev1alpha1.InstanceSpec{Topology: &corev1alpha1.TopologySpec{Type: topology}}
	if topology == "cluster" {
		spec.Components = map[string]corev1alpha1.ComponentSpec{
			common.ComponentProxy: {}, common.ComponentMixCoord: {}, common.ComponentDataNode: {},
			common.ComponentQueryNode: {}, common.ComponentStreaming: {},
		}
	} else {
		spec.Components = map[string]corev1alpha1.ComponentSpec{common.ComponentStandalone: {}}
	}
	if params != nil {
		raw, err := json.Marshal(definition.Parameters{TLS: params})
		require.NoError(t, err)
		spec.Parameters = &runtime.RawExtension{Raw: raw}
	}
	return spec
}

func requireTLS() *definition.TLSParameters {
	return &definition.TLSParameters{Mode: definition.TLSModeRequire}
}

func tlsSecret(name string, data map[string]string) *corev1.Secret {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "db"}, Data: map[string][]byte{}}
	for k, v := range data {
		secret.Data[k] = []byte(v)
	}
	return secret
}

func parseCert(t *testing.T, data []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(data)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return cert
}

func TestInstanceTLS(t *testing.T) {
	tests := map[string]struct {
		params *definition.TLSParameters
		want   tlsSpec
	}{
		"no parameters":  {params: nil, want: tlsSpec{}},
		"disabled":       {params: &definition.TLSParameters{Mode: definition.TLSModeDisabled, CertificateSecretName: "x"}, want: tlsSpec{}},
		"generated":      {params: requireTLS(), want: tlsSpec{enabled: true, source: tlsSourceGenerated, secretName: "test-milvus-tls"}},
		"user secret":    {params: &definition.TLSParameters{Mode: definition.TLSModeRequire, CertificateSecretName: "mine"}, want: tlsSpec{enabled: true, source: tlsSourceSecret, secretName: "mine"}},
		"issuer default": {params: &definition.TLSParameters{Mode: definition.TLSModeRequire, Issuer: &definition.IssuerReference{Name: "ca"}}, want: tlsSpec{enabled: true, source: tlsSourceIssuer, secretName: "test-milvus-tls-issued", issuer: definition.IssuerReference{Name: "ca", Kind: definition.IssuerKindIssuer}}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := newTestContext(t, tlsInstanceSpec(t, "standalone", tt.params))
			assert.Equal(t, tt.want, instanceTLS(c))
		})
	}
}

func TestValidateTLS(t *testing.T) {
	keyPair := map[string]string{tlsKeyCert: "cert", tlsKeyKey: "key"}
	tests := map[string]struct {
		params  *definition.TLSParameters
		objs    []client.Object
		wantErr string
	}{
		"no parameters": {},
		"generated":     {params: requireTLS()},
		"unknown mode":  {params: &definition.TLSParameters{Mode: "preferTLS"}, wantErr: `tls.mode "preferTLS" is not supported`},
		"secret and issuer": {
			params:  &definition.TLSParameters{Mode: definition.TLSModeRequire, CertificateSecretName: "s", Issuer: &definition.IssuerReference{Name: "i"}},
			wantErr: "either certificateSecretName or issuer",
		},
		"issuer kind": {
			params:  &definition.TLSParameters{Mode: definition.TLSModeRequire, Issuer: &definition.IssuerReference{Name: "i", Kind: "Vault"}},
			wantErr: `tls.issuer.kind "Vault" is not supported`,
		},
		"issuer kind without name": {
			params:  &definition.TLSParameters{Mode: definition.TLSModeRequire, Issuer: &definition.IssuerReference{Kind: definition.IssuerKindClusterIssuer}},
			wantErr: "tls.issuer.name is required",
		},
		"additional names with a secret": {
			params:  &definition.TLSParameters{Mode: definition.TLSModeRequire, CertificateSecretName: "s", AdditionalNames: []string{"a.example.com"}},
			objs:    []client.Object{tlsSecret("s", keyPair)},
			wantErr: "tls.additionalNames applies to issued certificates",
		},
		"invalid additional name": {
			params:  &definition.TLSParameters{Mode: definition.TLSModeRequire, AdditionalNames: []string{"not a name"}},
			wantErr: `"not a name" is neither an IP address nor a DNS name`,
		},
		"valid additional names": {
			params: &definition.TLSParameters{Mode: definition.TLSModeRequire, AdditionalNames: []string{"milvus.example.com", "*.example.com", "203.0.113.7"}},
		},
		"missing secret": {
			params:  &definition.TLSParameters{Mode: definition.TLSModeRequire, CertificateSecretName: "s"},
			wantErr: `certificate secret "s" not found`,
		},
		"secret without key": {
			params:  &definition.TLSParameters{Mode: definition.TLSModeRequire, CertificateSecretName: "s"},
			objs:    []client.Object{tlsSecret("s", map[string]string{tlsKeyCert: "cert"})},
			wantErr: `certificate secret "s" needs tls.crt and tls.key`,
		},
		"secret checked only when TLS is on": {
			params: &definition.TLSParameters{Mode: definition.TLSModeDisabled, CertificateSecretName: "s"},
		},
		"secret with key pair": {
			params: &definition.TLSParameters{Mode: definition.TLSModeRequire, CertificateSecretName: "s"},
			objs:   []client.Object{tlsSecret("s", keyPair)},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := newTestContextWithObjects(t, tlsInstanceSpec(t, "standalone", tt.params), tt.objs...)
			err := validateTLS(c)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestSyncTLSGeneratesCertificate(t *testing.T) {
	params := requireTLS()
	c := newTestContext(t, tlsInstanceSpec(t, "standalone", params))

	state, err := syncTLS(c)
	require.NoError(t, err)
	assert.True(t, state.enabled)
	assert.Equal(t, "test-milvus-tls", state.secretName)
	assert.NotEmpty(t, state.hash)

	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, "test-milvus-tls"))
	assert.Equal(t, corev1.SecretTypeTLS, secret.Type)
	assert.Equal(t, secret.Data[tlsKeyCA], state.ca)
	leaf := parseCert(t, secret.Data[tlsKeyCert])
	ca := parseCert(t, state.ca)
	assert.True(t, ca.IsCA)

	roots := x509.NewCertPool()
	roots.AddCert(ca)
	for _, name := range []string{"test-milvus-milvus", "test-milvus-milvus.db.svc", "test-milvus-milvus.db.svc.cluster.local", "localhost"} {
		_, err := leaf.Verify(x509.VerifyOptions{DNSName: name, Roots: roots})
		assert.NoError(t, err, name)
	}
	assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, leaf.ExtKeyUsage)
	assert.Equal(t, "test-milvus-milvus.db.svc", leaf.Subject.CommonName)

	t.Run("stable across reconciles", func(t *testing.T) {
		again, err := syncTLS(c)
		require.NoError(t, err)
		assert.Equal(t, state.hash, again.hash)
	})

	t.Run("re-issued for new names under the same CA", func(t *testing.T) {
		params.AdditionalNames = []string{"milvus.example.com", "203.0.113.7"}
		c := newTestContextWithObjects(t, tlsInstanceSpec(t, "standalone", params), secret.DeepCopy(), caSecret(t, c))
		renamed, err := syncTLS(c)
		require.NoError(t, err)
		assert.NotEqual(t, state.hash, renamed.hash)
		assert.Equal(t, state.ca, renamed.ca, "clients keep trusting the instance")

		updated := &corev1.Secret{}
		require.NoError(t, c.Get(updated, "test-milvus-tls"))
		leaf := parseCert(t, updated.Data[tlsKeyCert])
		assert.Contains(t, leaf.DNSNames, "milvus.example.com")
		require.Len(t, leaf.IPAddresses, 2)
		assert.Equal(t, "203.0.113.7", leaf.IPAddresses[1].String())
	})

	t.Run("renewed before it expires", func(t *testing.T) {
		defer func() { now = time.Now }()
		now = func() time.Time { return time.Now().Add(generatedCertValidity - 10*24*time.Hour) }
		c := newTestContextWithObjects(t, tlsInstanceSpec(t, "standalone", requireTLS()), secret.DeepCopy(), caSecret(t, c))
		renewed, err := syncTLS(c)
		require.NoError(t, err)
		assert.NotEqual(t, state.hash, renewed.hash)
		assert.Equal(t, state.ca, renewed.ca)
	})
}

func caSecret(t *testing.T, c *controller.Context) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, "test-milvus-tls-ca"))
	return secret.DeepCopy()
}

func TestSyncTLSUserSecret(t *testing.T) {
	params := &definition.TLSParameters{Mode: definition.TLSModeRequire, CertificateSecretName: "corp-tls"}
	user := tlsSecret("corp-tls", map[string]string{tlsKeyCert: "cert", tlsKeyKey: "key", tlsKeyCA: "ca"})
	user.Annotations = map[string]string{tlsInstancesAnnotation: "other"}
	c := newTestContextWithObjects(t, tlsInstanceSpec(t, "standalone", params), user)

	state, err := syncTLS(c)
	require.NoError(t, err)
	assert.Equal(t, tlsState{enabled: true, secretName: "corp-tls", ca: []byte("ca"), hash: state.hash}, state)
	assert.NotEmpty(t, state.hash)

	labeled := &corev1.Secret{}
	require.NoError(t, c.Get(labeled, "corp-tls"))
	assert.Equal(t, "true", labeled.Labels[tlsWatchLabel])
	assert.Equal(t, "other,test-milvus", labeled.Annotations[tlsInstancesAnnotation])
	assert.True(t, tlsSecretChanged().Generic(event.GenericEvent{Object: labeled}))

	requests := enqueueTLSSecretInstances(c.Context(), labeled)
	require.Len(t, requests, 2)
	assert.Equal(t, types.NamespacedName{Namespace: "db", Name: "test-milvus"}, requests[1].NamespacedName)

	t.Run("a public CA leaves ca.crt out", func(t *testing.T) {
		public := tlsSecret("corp-tls", map[string]string{tlsKeyCert: "cert", tlsKeyKey: "key"})
		c := newTestContextWithObjects(t, tlsInstanceSpec(t, "standalone", params), public)
		state, err := syncTLS(c)
		require.NoError(t, err)
		assert.Empty(t, state.ca)
	})
}

func TestSyncTLSIssuer(t *testing.T) {
	params := &definition.TLSParameters{
		Mode:            definition.TLSModeRequire,
		Issuer:          &definition.IssuerReference{Name: "corp-ca", Kind: definition.IssuerKindClusterIssuer},
		AdditionalNames: []string{"203.0.113.7"},
	}
	c := newTestContext(t, tlsInstanceSpec(t, "standalone", params))

	_, err := syncTLS(c)
	require.Error(t, err)
	assert.True(t, controller.IsWaitError(err), "waits for cert-manager: %v", err)

	certificate := &unstructured.Unstructured{}
	certificate.SetGroupVersionKind(certificateGVK)
	require.NoError(t, c.Get(certificate, "test-milvus-tls-issued"))
	spec := certificate.Object["spec"].(map[string]any)
	assert.Equal(t, "test-milvus-tls-issued", spec["secretName"])
	assert.Equal(t, map[string]any{"name": "corp-ca", "kind": "ClusterIssuer", "group": "cert-manager.io"}, spec["issuerRef"])
	assert.Contains(t, spec["dnsNames"], "test-milvus-milvus.db.svc")
	assert.Equal(t, []any{"127.0.0.1", "203.0.113.7"}, spec["ipAddresses"])
	template := spec["secretTemplate"].(map[string]any)
	assert.Equal(t, map[string]any{tlsWatchLabel: "true"}, template["labels"])
	assert.Equal(t, map[string]any{tlsInstancesAnnotation: "test-milvus"}, template["annotations"])
	require.Len(t, certificate.GetOwnerReferences(), 1)

	issued := tlsSecret("test-milvus-tls-issued", map[string]string{tlsKeyCert: "cert", tlsKeyKey: "key", tlsKeyCA: "ca"})
	require.NoError(t, c.Client().Create(c.Context(), issued))
	state, err := syncTLS(c)
	require.NoError(t, err)
	assert.Equal(t, "test-milvus-tls-issued", state.secretName)
	assert.Equal(t, []byte("ca"), state.ca)

	t.Run("dropping the issuer removes the certificate", func(t *testing.T) {
		c := newTestContextWithObjects(t, tlsInstanceSpec(t, "standalone", requireTLS()), certificate.DeepCopy(), issued.DeepCopy())
		state, err := syncTLS(c)
		require.NoError(t, err)
		assert.Equal(t, "test-milvus-tls", state.secretName)
		assert.True(t, apierrors.IsNotFound(c.Get(&corev1.Secret{}, "test-milvus-tls-issued")))
		gone := &unstructured.Unstructured{}
		gone.SetGroupVersionKind(certificateGVK)
		assert.True(t, apierrors.IsNotFound(c.Get(gone, "test-milvus-tls-issued")))
	})
}

func TestApplyTLS(t *testing.T) {
	state := tlsState{enabled: true, secretName: "test-milvus-tls", ca: []byte("ca"), hash: "abc"}

	t.Run("standalone", func(t *testing.T) {
		spec := milvusapi.MilvusSpec{Mode: milvusapi.MilvusModeStandalone, Com: milvusapi.MilvusComponents{
			Standalone: &milvusapi.MilvusStandalone{ServiceComponent: milvusapi.ServiceComponent{Component: milvusapi.Component{
				ComponentSpec: milvusapi.ComponentSpec{PodAnnotations: map[string]string{"user": "kept"}},
			}}},
		}}
		applyTLS(&spec, state)

		conf, err := yaml.Marshal(spec.Conf)
		require.NoError(t, err)
		assert.YAMLEq(t, `
common: {security: {tlsMode: 1}}
proxy: {http: {port: 8443}}
tls:
  serverPemPath: /milvus/tls/tls.crt
  serverKeyPath: /milvus/tls/tls.key
  caPemPath: /milvus/tls/ca.crt
`, string(conf))
		sc := spec.Com.Standalone.ServiceComponent
		assert.Equal(t, int32(8443), sc.ServiceRestfulPort)
		assert.Equal(t, []milvusapi.Values{{
			"name":   "milvus-tls",
			"secret": map[string]any{"secretName": "test-milvus-tls", "defaultMode": int64(420)},
		}}, sc.Volumes)
		assert.Equal(t, []corev1.VolumeMount{{Name: "milvus-tls", MountPath: "/milvus/tls", ReadOnly: true}}, sc.VolumeMounts)
		assert.Equal(t, map[string]string{"user": "kept", tlsHashAnnotation: "abc"}, sc.PodAnnotations)
	})

	t.Run("cluster serves clients from the proxy only", func(t *testing.T) {
		spec := milvusapi.MilvusSpec{Mode: milvusapi.MilvusModeCluster, Com: milvusapi.MilvusComponents{
			Proxy:     &milvusapi.MilvusProxy{},
			QueryNode: &milvusapi.MilvusQueryNode{},
		}}
		applyTLS(&spec, tlsState{enabled: true, secretName: "s", hash: "h"})
		assert.Equal(t, int32(8443), spec.Com.Proxy.ServiceRestfulPort)
		assert.Len(t, spec.Com.Proxy.Volumes, 1)
		assert.Empty(t, spec.Com.QueryNode.Volumes)
		assert.NotContains(t, spec.Conf["tls"], "caPemPath", "no CA to point at")
	})

	t.Run("disabled changes nothing", func(t *testing.T) {
		spec := milvusapi.MilvusSpec{Mode: milvusapi.MilvusModeStandalone, Com: milvusapi.MilvusComponents{Standalone: &milvusapi.MilvusStandalone{}}}
		applyTLS(&spec, tlsState{})
		assert.Nil(t, spec.Conf)
		assert.Zero(t, spec.Com.Standalone.ServiceRestfulPort)
	})
}

func TestSyncAndStatusWithTLS(t *testing.T) {
	c := newTestContext(t, tlsInstanceSpec(t, "standalone", requireTLS()))
	p := New()
	require.NoError(t, p.Sync(c))

	cr := &milvusapi.Milvus{}
	require.NoError(t, c.Get(cr, c.Name()))
	security := cr.Spec.Conf["common"].(map[string]any)["security"].(map[string]any)
	assert.Equal(t, true, security["authorizationEnabled"])
	assert.EqualValues(t, 1, security["tlsMode"])
	assert.EqualValues(t, 8443, cr.Spec.Com.Standalone.ServiceRestfulPort)
	assert.NotEmpty(t, cr.Spec.Com.Standalone.PodAnnotations[tlsHashAnnotation])

	cr.Status.Status = milvusapi.StatusHealthy
	cr.Status.Endpoint = "test-milvus-milvus.db.svc.cluster.local:19530"
	require.NoError(t, c.Client().Update(c.Context(), cr))
	status, err := p.Status(c)
	require.NoError(t, err)
	assert.Equal(t, "https://test-milvus-milvus.db.svc.cluster.local:19530", status.ConnectionDetails.URI)
	secret := &corev1.Secret{}
	require.NoError(t, c.Get(secret, "test-milvus-tls"))
	assert.Equal(t, string(secret.Data[tlsKeyCA]), status.ConnectionDetails.AdditionalProperties["ca.crt"])

	endpoint, err := milvusInternalEndpoint(c)
	require.NoError(t, err)
	assert.Equal(t, milvusEndpoint{url: "https://test-milvus-milvus.db.svc:8443", ca: secret.Data[tlsKeyCA]}, endpoint)
}

func TestConnectionDetailsWithoutTLS(t *testing.T) {
	details := connectionDetails("h", "19530", "pw", tlsState{})
	assert.Equal(t, "http://h:19530", details.URI)
	assert.NotContains(t, details.AdditionalProperties, "ca.crt")
}

func TestMilvusBackupConfigTLS(t *testing.T) {
	store := backupObjectStore{Provider: "minio", Address: "minio", Port: 9000, Bucket: "b", RootPath: "r"}
	out, err := milvusBackupConfig("test-milvus", "db", store, store, tlsState{enabled: true, ca: []byte("ca")})
	require.NoError(t, err)
	var config map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(out), &config))
	milvus := config["milvus"].(map[string]any)
	assert.Equal(t, map[string]any{
		"address":    "test-milvus-milvus.db.svc",
		"port":       float64(19530),
		"tlsMode":    "server",
		"caCertPath": "/etc/milvus-backup/tls/ca.crt",
	}, milvus["grpc"])
	assert.Equal(t, map[string]any{"endpoint": "https://test-milvus-milvus.db.svc:8443"}, milvus["rest"])

	out, err = milvusBackupConfig("test-milvus", "db", store, store, tlsState{})
	require.NoError(t, err)
	assert.NotContains(t, out, "tlsMode")
	assert.NotContains(t, out, "rest:")
}

func TestBackupJobTemplateTLS(t *testing.T) {
	spec := backupJobTemplate(backupJobSpec{tlsCASecret: "test-milvus-tls", env: []corev1.EnvVar{{Name: "A", Value: "1"}}}, "cfg", nil)
	pod := spec.Template.Spec
	assert.Contains(t, pod.Volumes, corev1.Volume{Name: "tls-ca", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
		SecretName: "test-milvus-tls",
		Items:      []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
	}}})
	container := pod.Containers[0]
	assert.Contains(t, container.VolumeMounts, corev1.VolumeMount{Name: "tls-ca", MountPath: "/etc/milvus-backup/tls", ReadOnly: true})
	assert.Contains(t, container.Env, corev1.EnvVar{Name: "SSL_CERT_DIR", Value: "/etc/ssl/certs:/etc/milvus-backup/tls"})

	plain := backupJobTemplate(backupJobSpec{}, "cfg", nil)
	assert.Len(t, plain.Template.Spec.Volumes, 2)
	assert.Empty(t, plain.Template.Spec.Containers[0].Env)
}

func TestMilvusRESTClientTLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"data":[]}`))
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})

	require.NoError(t, newMilvusRESTClient().Authenticate(t.Context(), milvusEndpoint{url: server.URL, ca: ca}, "pw"))
	assert.Error(t, newMilvusRESTClient().Authenticate(t.Context(), milvusEndpoint{url: server.URL}, "pw"),
		"an unknown CA is rejected")
	assert.ErrorContains(t, newMilvusRESTClient().Authenticate(t.Context(), milvusEndpoint{url: server.URL, ca: []byte("junk")}, "pw"),
		"no PEM certificate")
}
