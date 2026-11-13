package provider

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
)

const (
	generatedCAValidity   = 10 * 365 * 24 * time.Hour
	generatedCertValidity = 365 * 24 * time.Hour
	// A certificate is re-issued this long before it expires.
	generatedRenewBefore = 30 * 24 * time.Hour
	caKeyKey             = "ca.key"
)

// now is replaced in tests.
var now = time.Now

// ensureGeneratedCertificate keeps a certificate for names, signed by a CA
// created for the instance, in the instance's TLS Secret. The CA stays as
// long as it is valid, so clients keep trusting the instance.
func ensureGeneratedCertificate(c *controller.Context, names []string) error {
	caCert, caKey, caPEM, err := ensureGeneratedCA(c)
	if err != nil {
		return err
	}
	name := c.Name() + generatedTLSSecretSuffix
	existing := &corev1.Secret{}
	if err := c.Get(existing, name); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get TLS secret %q: %w", name, err)
	}
	if certificateCurrent(existing.Data, caCert, caPEM, names) {
		return nil
	}
	commonName := fmt.Sprintf("%s-milvus.%s.svc", c.Name(), c.Namespace())
	certPEM, keyPEM, err := issueCertificate(caCert, caKey, commonName, names)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: c.ObjectMeta(name),
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{tlsKeyCert: certPEM, tlsKeyKey: keyPEM, tlsKeyCA: caPEM},
	}
	if err := c.Apply(secret); err != nil {
		return fmt.Errorf("apply TLS secret: %w", err)
	}
	return nil
}

func ensureGeneratedCA(c *controller.Context) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	name := c.Name() + generatedCASecretSuffix
	existing := &corev1.Secret{}
	if err := c.Get(existing, name); err != nil && !apierrors.IsNotFound(err) {
		return nil, nil, nil, fmt.Errorf("get TLS CA secret %q: %w", name, err)
	}
	if cert, key, err := parseKeyPair(existing.Data[tlsKeyCA], existing.Data[caKeyKey]); err == nil &&
		cert.IsCA && now().Add(generatedRenewBefore).Before(cert.NotAfter) {
		return cert, key, existing.Data[tlsKeyCA], nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: fmt.Sprintf("%s/%s Milvus CA", c.Namespace(), c.Name())},
		NotBefore:             now().Add(-5 * time.Minute),
		NotAfter:              now().Add(generatedCAValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create TLS CA: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, err
	}
	certPEM, keyPEM, err := encodeKeyPair(der, key)
	if err != nil {
		return nil, nil, nil, err
	}
	secret := &corev1.Secret{
		ObjectMeta: c.ObjectMeta(name),
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{tlsKeyCA: certPEM, caKeyKey: keyPEM},
	}
	if err := c.Apply(secret); err != nil {
		return nil, nil, nil, fmt.Errorf("apply TLS CA secret: %w", err)
	}
	return cert, key, certPEM, nil
}

// certificateCurrent reports whether data holds a certificate the CA signed
// for exactly names, far enough from expiry.
func certificateCurrent(data map[string][]byte, ca *x509.Certificate, caPEM []byte, names []string) bool {
	if !bytes.Equal(data[tlsKeyCA], caPEM) {
		return false
	}
	cert, _, err := parseKeyPair(data[tlsKeyCert], data[tlsKeyKey])
	if err != nil || cert.CheckSignatureFrom(ca) != nil || !now().Add(generatedRenewBefore).Before(cert.NotAfter) {
		return false
	}
	return slices.Equal(certificateSANs(cert), names)
}

func certificateSANs(cert *x509.Certificate) []string {
	names := slices.Clone(cert.DNSNames)
	for _, ip := range cert.IPAddresses {
		names = append(names, ip.String())
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func issueCertificate(ca *x509.Certificate, caKey *ecdsa.PrivateKey, commonName string, names []string) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, err
	}
	dns, ips := splitNames(names)
	template := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    now().Add(-5 * time.Minute),
		NotAfter:     now().Add(generatedCertValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Subject:      pkix.Name{CommonName: commonName},
		DNSNames:     dns,
	}
	for _, ip := range ips {
		template.IPAddresses = append(template.IPAddresses, net.ParseIP(ip))
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("issue TLS certificate: %w", err)
	}
	return encodeKeyPair(der, key)
}

func encodeKeyPair(der []byte, key *ecdsa.PrivateKey) ([]byte, []byte, error) {
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

func parseKeyPair(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certBlock, _ := pem.Decode(certPEM)
	keyBlock, _ := pem.Decode(keyPEM)
	if certBlock == nil || keyBlock == nil {
		return nil, nil, errors.New("missing PEM data")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	parsed, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || !key.PublicKey.Equal(cert.PublicKey) {
		return nil, nil, errors.New("key does not match the certificate")
	}
	return cert, key, nil
}

func serialNumber() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}
