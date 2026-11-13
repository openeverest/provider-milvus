// Package definition contains shared types used across the provider definition.
//
// +k8s:openapi-gen=true
package definition

// Parameters defines instance-wide structured parameters that apply to the
// entire instance (Instance.spec.parameters).
type Parameters struct {
	// TLS configures encryption in transit between clients and Milvus.
	// +optional
	TLS *TLSParameters `json:"tls,omitempty"`
}

// TLSMode controls whether clients must connect over TLS.
// +enum
type TLSMode string

const (
	// TLSModeDisabled serves clients in plain text. The default.
	TLSModeDisabled TLSMode = "disabled"
	// TLSModeRequire serves the gRPC and REST APIs over TLS only; clients
	// verify the server certificate and authenticate with their password.
	TLSModeRequire TLSMode = "requireTLS"
)

// TLSParameters configures TLS for the instance. The certificate comes from,
// in order of precedence: the certificate secret, the cert-manager issuer, or
// else a CA the provider creates for the instance.
type TLSParameters struct {
	// Mode controls whether clients must connect over TLS.
	// +default="disabled"
	// +optional
	Mode TLSMode `json:"mode,omitempty"`

	// CertificateSecretName names a Secret in the instance namespace with the
	// certificate clients connect to: tls.crt, tls.key and, unless a public CA
	// signed it, ca.crt. Its subject alternative names must cover the names
	// clients connect to, such as <instance>-milvus.<namespace>.svc.
	// +optional
	CertificateSecretName string `json:"certificateSecretName,omitempty"`

	// Issuer is the cert-manager issuer that signs the instance's
	// certificate, instead of a CA the provider creates for the instance.
	// +optional
	Issuer *IssuerReference `json:"issuer,omitempty"`

	// AdditionalNames are extra DNS names or IP addresses the issued
	// certificate covers, such as a DNS record pointing at the load balancer.
	// Not used with certificateSecretName.
	// +optional
	AdditionalNames []string `json:"additionalNames,omitempty"`
}

// IssuerKind is the kind of a cert-manager issuer.
// +enum
type IssuerKind string

const (
	// IssuerKindIssuer is an Issuer in the instance namespace.
	IssuerKindIssuer IssuerKind = "Issuer"
	// IssuerKindClusterIssuer is a cluster-wide ClusterIssuer.
	IssuerKindClusterIssuer IssuerKind = "ClusterIssuer"
)

// IssuerReference refers to a cert-manager issuer.
type IssuerReference struct {
	// Name of the issuer.
	// +optional
	Name string `json:"name,omitempty"`

	// Kind of the issuer.
	// +default="Issuer"
	// +optional
	Kind IssuerKind `json:"kind,omitempty"`
}
