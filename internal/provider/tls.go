package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

const (
	// generatedTLSSecretSuffix names the certificate signed by the
	// instance's own CA, kept in generatedCASecretSuffix.
	generatedTLSSecretSuffix = "-tls"
	generatedCASecretSuffix  = "-tls-ca"
	// issuedTLSSuffix names the cert-manager Certificate and its Secret.
	issuedTLSSuffix = "-tls-issued"

	tlsVolumeName = "milvus-tls"
	tlsMountPath  = "/milvus/tls"

	// Milvus refuses TLS while REST shares the gRPC port, so REST moves to a
	// port of its own.
	milvusRESTTLSPort = 8443

	// tlsHashAnnotation rolls the pods that serve clients when their
	// certificate changes: Milvus reads it only at startup.
	tlsHashAnnotation = "tls.milvus.openeverest.io/hash"
	// tlsWatchLabel and tlsInstancesAnnotation mark a certificate Secret the
	// provider does not own, so its changes reach the instances using it.
	tlsWatchLabel          = "tls.milvus.openeverest.io/watched"
	tlsInstancesAnnotation = "tls.milvus.openeverest.io/instances"

	tlsKeyCert = corev1.TLSCertKey
	tlsKeyKey  = corev1.TLSPrivateKeyKey
	tlsKeyCA   = "ca.crt"

	// Milvus TLS mode 1: the server presents a certificate, clients do not.
	milvusTLSModeServer = 1
)

var certificateGVK = schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"}

type tlsSource int

const (
	tlsSourceGenerated tlsSource = iota
	tlsSourceIssuer
	tlsSourceSecret
)

// tlsSpec is the TLS configuration an Instance asks for.
type tlsSpec struct {
	enabled         bool
	source          tlsSource
	secretName      string
	issuer          definition.IssuerReference
	additionalNames []string
}

// tlsState is the certificate Milvus serves clients with.
type tlsState struct {
	enabled    bool
	secretName string
	// ca verifies the certificate; empty when a public CA signed it.
	ca   []byte
	hash string
}

func tlsParameters(in *corev1alpha1.Instance) *definition.TLSParameters {
	var params definition.Parameters
	if in.Spec.Parameters == nil || decodeRaw(in.Spec.Parameters, &params) != nil {
		return nil
	}
	return params.TLS
}

func instanceTLS(c *controller.Context) tlsSpec {
	params := tlsParameters(c.Instance())
	if params == nil || params.Mode != definition.TLSModeRequire {
		return tlsSpec{}
	}
	spec := tlsSpec{enabled: true, additionalNames: params.AdditionalNames}
	switch {
	case params.CertificateSecretName != "":
		spec.source = tlsSourceSecret
		spec.secretName = params.CertificateSecretName
	case params.Issuer != nil && params.Issuer.Name != "":
		spec.source = tlsSourceIssuer
		spec.secretName = c.Name() + issuedTLSSuffix
		spec.issuer = *params.Issuer
		if spec.issuer.Kind == "" {
			spec.issuer.Kind = definition.IssuerKindIssuer
		}
	default:
		spec.source = tlsSourceGenerated
		spec.secretName = c.Name() + generatedTLSSecretSuffix
	}
	return spec
}

// validateTLS checks the TLS parameters.
func validateTLS(in *corev1alpha1.Instance) error {
	params := tlsParameters(in)
	if params == nil {
		return nil
	}
	path := parametersPath.Child("tls")
	switch params.Mode {
	case "", definition.TLSModeDisabled, definition.TLSModeRequire:
	default:
		return fieldErrorf(path.Child("mode"), "tls.mode %q is not supported; expected %s or %s", params.Mode, definition.TLSModeDisabled, definition.TLSModeRequire)
	}
	if params.Issuer != nil {
		switch params.Issuer.Kind {
		case "", definition.IssuerKindIssuer, definition.IssuerKindClusterIssuer:
		default:
			return fieldErrorf(path.Child("issuer", "kind"), "tls.issuer.kind %q is not supported; expected %s or %s", params.Issuer.Kind, definition.IssuerKindIssuer, definition.IssuerKindClusterIssuer)
		}
		if params.Issuer.Name == "" && params.Issuer.Kind != "" {
			return field.Required(path.Child("issuer", "name"), "tls.issuer.name is required with tls.issuer.kind")
		}
	}
	issuer := params.Issuer != nil && params.Issuer.Name != ""
	if params.CertificateSecretName != "" && issuer {
		return field.Forbidden(path.Child("issuer"), "tls: use either certificateSecretName or issuer, not both")
	}
	if params.CertificateSecretName != "" && len(params.AdditionalNames) > 0 {
		return field.Forbidden(path.Child("additionalNames"), "tls.additionalNames applies to issued certificates; a certificateSecretName certificate carries its own names")
	}
	for i, name := range params.AdditionalNames {
		if net.ParseIP(name) != nil {
			continue
		}
		if errs := validation.IsDNS1123Subdomain(strings.TrimPrefix(name, "*.")); len(errs) > 0 {
			return fieldErrorf(path.Child("additionalNames").Index(i), "tls.additionalNames: %q is neither an IP address nor a DNS name: %s", name, strings.Join(errs, "; "))
		}
	}
	return nil
}

// validateTLSReferences checks that a certificate secret exists with a key
// pair.
func validateTLSReferences(c *controller.Context) (field.ErrorList, error) {
	params := tlsParameters(c.Instance())
	if params == nil || params.Mode != definition.TLSModeRequire || params.CertificateSecretName == "" {
		return nil, nil
	}
	path := parametersPath.Child("tls", "certificateSecretName")
	secret := &corev1.Secret{}
	if err := c.Get(secret, params.CertificateSecretName); err != nil {
		if apierrors.IsNotFound(err) {
			return field.ErrorList{field.NotFound(path, params.CertificateSecretName)}, nil
		}
		return nil, fmt.Errorf("tls: get certificate secret %q: %w", params.CertificateSecretName, err)
	}
	if len(secret.Data[tlsKeyCert]) == 0 || len(secret.Data[tlsKeyKey]) == 0 {
		return field.ErrorList{field.Invalid(path, params.CertificateSecretName,
			fmt.Sprintf("tls: certificate secret %q needs %s and %s", params.CertificateSecretName, tlsKeyCert, tlsKeyKey))}, nil
	}
	return nil, nil
}

// syncTLS makes sure the certificate the instance asks for exists and returns
// it. It waits while cert-manager has not issued it yet.
func syncTLS(c *controller.Context) (tlsState, error) {
	spec := instanceTLS(c)
	if spec.source != tlsSourceIssuer || !spec.enabled {
		if err := removeIssuedCertificate(c); err != nil {
			return tlsState{}, err
		}
	}
	if !spec.enabled {
		return tlsState{}, nil
	}
	switch spec.source {
	case tlsSourceSecret:
		if err := watchCertificateSecret(c, spec.secretName); err != nil {
			return tlsState{}, err
		}
	case tlsSourceIssuer:
		if err := applyIssuedCertificate(c, spec, certificateNames(c, spec)); err != nil {
			return tlsState{}, err
		}
	default:
		if err := ensureGeneratedCertificate(c, certificateNames(c, spec)); err != nil {
			return tlsState{}, err
		}
	}
	state, ready, err := readTLSState(c, spec)
	if err != nil {
		return tlsState{}, err
	}
	if !ready {
		return tlsState{}, controller.WaitForDuration(fmt.Sprintf("waiting for the TLS certificate in secret %q", spec.secretName), 10*time.Second)
	}
	return state, nil
}

// currentTLS reads the certificate the instance serves, without changing
// anything. A missing certificate leaves the CA empty.
func currentTLS(c *controller.Context) (tlsState, error) {
	spec := instanceTLS(c)
	if !spec.enabled {
		return tlsState{}, nil
	}
	state, _, err := readTLSState(c, spec)
	return state, err
}

func readTLSState(c *controller.Context, spec tlsSpec) (tlsState, bool, error) {
	state := tlsState{enabled: true, secretName: spec.secretName}
	secret := &corev1.Secret{}
	if err := c.Get(secret, spec.secretName); err != nil {
		if apierrors.IsNotFound(err) {
			return state, false, nil
		}
		return state, false, fmt.Errorf("get TLS secret %q: %w", spec.secretName, err)
	}
	cert, key := secret.Data[tlsKeyCert], secret.Data[tlsKeyKey]
	if len(cert) == 0 || len(key) == 0 {
		return state, false, nil
	}
	state.ca = secret.Data[tlsKeyCA]
	sum := sha256.New()
	for _, part := range [][]byte{cert, key, state.ca} {
		sum.Write(part)
		sum.Write([]byte{0})
	}
	state.hash = hex.EncodeToString(sum.Sum(nil))[:16]
	return state, true, nil
}

// certificateNames are the names an issued certificate covers: the Service in
// every form a client resolves it, the published host and the user's extras.
func certificateNames(c *controller.Context, spec tlsSpec) []string {
	service := c.Name() + "-milvus"
	names := []string{
		"localhost", "127.0.0.1",
		service,
		service + "." + c.Namespace(),
		service + "." + c.Namespace() + ".svc",
		service + "." + c.Namespace() + ".svc.cluster.local",
	}
	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err == nil {
		if host, _, ready, _ := resolveEndpoint(c, cr); ready && host != "" {
			names = append(names, host)
		}
	}
	names = append(names, spec.additionalNames...)
	slices.Sort(names)
	return slices.Compact(names)
}

func splitNames(names []string) (dns, ips []string) {
	for _, name := range names {
		if net.ParseIP(name) != nil {
			ips = append(ips, name)
		} else {
			dns = append(dns, name)
		}
	}
	return dns, ips
}

func applyIssuedCertificate(c *controller.Context, spec tlsSpec, names []string) error {
	dns, ips := splitNames(names)
	certSpec := map[string]any{
		"secretName": spec.secretName,
		"issuerRef": map[string]any{
			"name":  spec.issuer.Name,
			"kind":  string(spec.issuer.Kind),
			"group": certificateGVK.Group,
		},
		"dnsNames":   toAnySlice(dns),
		"privateKey": map[string]any{"rotationPolicy": "Always"},
		"secretTemplate": map[string]any{
			"labels":      map[string]any{tlsWatchLabel: "true"},
			"annotations": map[string]any{tlsInstancesAnnotation: c.Name()},
		},
	}
	if len(ips) > 0 {
		certSpec["ipAddresses"] = toAnySlice(ips)
	}
	certificate := &unstructured.Unstructured{Object: map[string]any{"spec": certSpec}}
	certificate.SetGroupVersionKind(certificateGVK)
	certificate.SetName(spec.secretName)
	certificate.SetNamespace(c.Namespace())
	certificate.SetLabels(c.ObjectMeta(spec.secretName).Labels)
	if err := c.Apply(certificate); err != nil {
		if meta.IsNoMatchError(err) {
			return errors.New("tls.issuer needs cert-manager, which is not installed")
		}
		return fmt.Errorf("apply TLS certificate: %w", err)
	}
	return nil
}

func toAnySlice(values []string) []any {
	out := make([]any, 0, len(values))
	for _, v := range values {
		out = append(out, v)
	}
	return out
}

// removeIssuedCertificate deletes the cert-manager Certificate and its Secret
// once the instance stops using an issuer. Their Secret tells whether there
// is anything to delete, so clusters without cert-manager are never asked.
func removeIssuedCertificate(c *controller.Context) error {
	name := c.Name() + issuedTLSSuffix
	secret := &corev1.Secret{}
	if err := c.Get(secret, name); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get TLS secret %q: %w", name, err)
	}
	certificate := &unstructured.Unstructured{}
	certificate.SetGroupVersionKind(certificateGVK)
	certificate.SetName(name)
	certificate.SetNamespace(c.Namespace())
	if err := c.Client().Delete(c.Context(), certificate); err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		return fmt.Errorf("delete TLS certificate %q: %w", name, err)
	}
	if err := c.Client().Delete(c.Context(), secret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete TLS secret %q: %w", name, err)
	}
	return nil
}

// watchCertificateSecret marks the user's certificate Secret so that its
// renewals roll the instance's pods.
func watchCertificateSecret(c *controller.Context, name string) error {
	secret := &corev1.Secret{}
	if err := c.Get(secret, name); err != nil {
		return fmt.Errorf("get TLS secret %q: %w", name, err)
	}
	instances := tlsSecretInstances(secret)
	if secret.Labels[tlsWatchLabel] == "true" && slices.Contains(instances, c.Name()) {
		return nil
	}
	instances = append(instances, c.Name())
	slices.Sort(instances)
	patch := map[string]any{"metadata": map[string]any{
		"labels":      map[string]any{tlsWatchLabel: "true"},
		"annotations": map[string]any{tlsInstancesAnnotation: strings.Join(slices.Compact(instances), ",")},
	}}
	raw, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if err := c.Client().Patch(c.Context(), secret, client.RawPatch(types.MergePatchType, raw)); err != nil {
		return fmt.Errorf("label TLS secret %q: %w", name, err)
	}
	return nil
}

func tlsSecretInstances(secret client.Object) []string {
	value := secret.GetAnnotations()[tlsInstancesAnnotation]
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

// tlsSecretChanged passes the certificate Secrets the provider does not own.
func tlsSecretChanged() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[tlsWatchLabel] == "true"
	})
}

// enqueueTLSSecretInstances reconciles the instances serving a certificate
// Secret when it changes.
func enqueueTLSSecretInstances(_ context.Context, obj client.Object) []reconcile.Request {
	var requests []reconcile.Request
	for _, instance := range tlsSecretInstances(obj) {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: instance}})
	}
	return requests
}

// applyTLS serves the gRPC and REST APIs over TLS with the certificate in
// state, mounted into the pods that serve clients.
func applyTLS(spec *milvusapi.MilvusSpec, state tlsState) {
	if !state.enabled {
		return
	}
	tlsConfig := map[string]any{
		"serverPemPath": tlsMountPath + "/" + tlsKeyCert,
		"serverKeyPath": tlsMountPath + "/" + tlsKeyKey,
	}
	if len(state.ca) > 0 {
		tlsConfig["caPemPath"] = tlsMountPath + "/" + tlsKeyCA
	}
	if spec.Conf == nil {
		spec.Conf = milvusapi.Values{}
	}
	deepMergeValues(spec.Conf, map[string]any{
		"tls":    tlsConfig,
		"common": map[string]any{"security": map[string]any{"tlsMode": milvusTLSModeServer}},
		"proxy":  map[string]any{"http": map[string]any{"port": milvusRESTTLSPort}},
	})

	sc := clientServiceComponent(spec)
	if sc == nil {
		return
	}
	sc.ServiceRestfulPort = milvusRESTTLSPort
	sc.Volumes = append(sc.Volumes, milvusapi.Values{
		"name": tlsVolumeName,
		// The API server's defaultMode, so the operator sees no change.
		"secret": map[string]any{"secretName": state.secretName, "defaultMode": int64(corev1.SecretVolumeSourceDefaultMode)},
	})
	sc.VolumeMounts = append(sc.VolumeMounts, corev1.VolumeMount{Name: tlsVolumeName, MountPath: tlsMountPath, ReadOnly: true})
	annotations := make(map[string]string, len(sc.PodAnnotations)+1)
	for k, v := range sc.PodAnnotations {
		annotations[k] = v
	}
	annotations[tlsHashAnnotation] = state.hash
	sc.PodAnnotations = annotations
}

// clientServiceComponent is the component serving clients: the standalone
// pod, or the proxy in cluster mode.
func clientServiceComponent(spec *milvusapi.MilvusSpec) *milvusapi.ServiceComponent {
	if spec.Mode == milvusapi.MilvusModeCluster {
		if spec.Com.Proxy == nil {
			return nil
		}
		return &spec.Com.Proxy.ServiceComponent
	}
	if spec.Com.Standalone == nil {
		return nil
	}
	return &spec.Com.Standalone.ServiceComponent
}
