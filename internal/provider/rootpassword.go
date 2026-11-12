package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// rootPasswordAPI changes and checks the Milvus root password.
type rootPasswordAPI interface {
	UpdatePassword(ctx context.Context, endpoint milvusEndpoint, oldPassword, newPassword string) error
	Authenticate(ctx context.Context, endpoint milvusEndpoint, password string) error
}

// milvusEndpoint is where the Milvus REST API is served and, under TLS, the
// CA that verifies it; an empty CA means the system roots.
type milvusEndpoint struct {
	url string
	ca  []byte
}

// milvusRESTClient talks to the Milvus RESTful API, which the proxy serves on
// the gRPC port, or a port of its own under TLS.
type milvusRESTClient struct {
	timeout time.Duration
}

func newMilvusRESTClient() milvusRESTClient {
	return milvusRESTClient{timeout: 10 * time.Second}
}

// httpClient trusts only the endpoint's CA when it has one.
func (m milvusRESTClient) httpClient(endpoint milvusEndpoint) (*http.Client, error) {
	if len(endpoint.ca) == 0 {
		return &http.Client{Timeout: m.timeout}, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(endpoint.ca) {
		return nil, errors.New("the TLS CA has no PEM certificate")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &http.Client{Timeout: m.timeout, Transport: transport}, nil
}

func (m milvusRESTClient) UpdatePassword(ctx context.Context, endpoint milvusEndpoint, oldPassword, newPassword string) error {
	return m.post(ctx, endpoint, "/v2/vectordb/users/update_password", oldPassword, map[string]string{
		"userName":    rootUsername,
		"password":    oldPassword,
		"newPassword": newPassword,
	})
}

func (m milvusRESTClient) Authenticate(ctx context.Context, endpoint milvusEndpoint, password string) error {
	return m.post(ctx, endpoint, "/v2/vectordb/users/list", password, map[string]string{})
}

func (m milvusRESTClient) post(ctx context.Context, endpoint milvusEndpoint, path, password string, body any) error {
	return m.call(ctx, endpoint, path, password, body, &struct{}{})
}

// milvusInternalEndpoint is the in-cluster address of the instance's Milvus
// API, served by the same Service whatever the exposure type.
func milvusInternalEndpoint(c *controller.Context) (milvusEndpoint, error) {
	service := fmt.Sprintf("%s-milvus.%s.svc", c.Name(), c.Namespace())
	state, err := currentTLS(c)
	if err != nil {
		return milvusEndpoint{}, err
	}
	if !state.enabled {
		return milvusEndpoint{url: fmt.Sprintf("http://%s:%s", service, milvusPort)}, nil
	}
	return milvusEndpoint{url: fmt.Sprintf("https://%s:%d", service, milvusRESTTLSPort), ca: state.ca}, nil
}

// syncRootPassword rotates the Milvus root password to the one in the
// credentials Secret once Milvus is healthy, then records it as applied.
func (p *Provider) syncRootPassword(c *controller.Context, creds rootCredentials) error {
	if !creds.rotationPending() {
		return nil
	}
	cr := &milvusapi.Milvus{}
	if err := c.Get(cr, c.Name()); err != nil || cr.Status.Status != milvusapi.StatusHealthy {
		// The Milvus CR watch reconciles again once it turns healthy.
		return nil
	}

	endpoint, err := milvusInternalEndpoint(c)
	if err != nil {
		return err
	}
	if err := p.rootPassword.UpdatePassword(c.Context(), endpoint, creds.appliedPassword, creds.password); err != nil {
		// A rotation interrupted before it was recorded leaves the applied
		// password stale, and Milvus seeds a not-yet-created root user with
		// the new password directly.
		if p.rootPassword.Authenticate(c.Context(), endpoint, creds.password) != nil {
			return fmt.Errorf("rotate root password: %w", err)
		}
	}
	creds.appliedPassword = creds.password
	return applyCredentialsSecret(c, creds)
}

// credentialsSecretChanged limits the Secret watch to the root credentials, so
// edits to the password trigger a rotation.
func credentialsSecretChanged() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return strings.HasSuffix(obj.GetName(), credentialsSecretSuffix)
	})
}
