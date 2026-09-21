package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// milvusPort is the gRPC port the proxy/standalone service exposes.
const milvusPort = "19530"

// exposedComponentName returns the component that owns the client-facing
// service for the topology: standalone exposes itself, cluster exposes the proxy.
func exposedComponentName(topologyType string) string {
	if topologyType == "cluster" {
		return common.ComponentProxy
	}
	return common.ComponentStandalone
}

// applyServiceExposure copies the OpenEverest service exposure settings onto the
// Milvus service component.
func applyServiceExposure(sc *milvusapi.ServiceComponent, service *corev1alpha1.Service) {
	if service == nil {
		return
	}
	sc.ServiceType = service.ServiceType
	if len(service.Annotations) > 0 {
		sc.ServiceAnnotations = service.Annotations
	}
}

// resolveEndpoint derives the client host and port from the Milvus CR according
// to its service type. The boolean reports whether an endpoint is available;
// when false the returned message explains what the instance is waiting for.
func resolveEndpoint(c *controller.Context, cr *milvusapi.Milvus) (host, port string, ready bool, message string) {
	switch serviceTypeFromSpec(cr) {
	case corev1.ServiceTypeLoadBalancer:
		if cr.Status.Endpoint == "" {
			return "", "", false, "waiting for load balancer address"
		}
		host, port = splitEndpoint(cr.Status.Endpoint)
		return host, port, true, ""
	case corev1.ServiceTypeNodePort:
		host, port, err := nodePortEndpoint(c, cr.Name)
		if err != nil || host == "" || port == "" {
			return "", "", false, "waiting for NodePort address"
		}
		return host, port, true, ""
	default:
		endpoint := cr.Status.Endpoint
		if endpoint == "" {
			endpoint = fmt.Sprintf("%s-milvus.%s.svc.cluster.local:%s", cr.Name, cr.Namespace, milvusPort)
		}
		host, port = splitEndpoint(endpoint)
		return host, port, true, ""
	}
}

func serviceTypeFromSpec(cr *milvusapi.Milvus) corev1.ServiceType {
	if standalone := cr.Spec.Com.Standalone; standalone != nil && standalone.ServiceType != "" {
		return standalone.ServiceType
	}
	if proxy := cr.Spec.Com.Proxy; proxy != nil && proxy.ServiceType != "" {
		return proxy.ServiceType
	}
	return corev1.ServiceTypeClusterIP
}

func splitEndpoint(endpoint string) (host, port string) {
	host, port = endpoint, milvusPort
	for i := len(endpoint) - 1; i >= 0; i-- {
		if endpoint[i] == ':' {
			host = endpoint[:i]
			port = endpoint[i+1:]
			break
		}
	}
	if port == "" {
		port = milvusPort
	}
	return host, port
}

// nodePortEndpoint builds a <nodeIP>:<nodePort> endpoint for a NodePort service.
// The operator does not report NodePort endpoints in the CR status, so the
// provider reads the assigned nodePort from the Service and pairs it with a
// reachable node address.
func nodePortEndpoint(c *controller.Context, instanceName string) (host, port string, err error) {
	service := &corev1.Service{}
	if err := c.Get(service, instanceName+"-milvus"); err != nil {
		return "", "", err
	}

	nodePort := int32(0)
	for _, p := range service.Spec.Ports {
		if p.Name == "milvus" || p.Port == 19530 {
			nodePort = p.NodePort
			break
		}
	}
	if nodePort == 0 {
		return "", "", nil
	}

	nodeIP, err := firstNodeAddress(c)
	if err != nil || nodeIP == "" {
		return "", "", err
	}
	return nodeIP, fmt.Sprintf("%d", nodePort), nil
}

// firstNodeAddress returns a reachable node address, preferring an external IP
// and falling back to an internal IP.
func firstNodeAddress(c *controller.Context) (string, error) {
	nodes := &corev1.NodeList{}
	// Nodes are cluster-scoped, so bypass the Context's namespace-scoped List.
	if err := c.Client().List(c.Context(), nodes); err != nil {
		return "", err
	}
	internal := ""
	for i := range nodes.Items {
		for _, addr := range nodes.Items[i].Status.Addresses {
			switch addr.Type {
			case corev1.NodeExternalIP:
				if addr.Address != "" {
					return addr.Address, nil
				}
			case corev1.NodeInternalIP:
				if internal == "" {
					internal = addr.Address
				}
			}
		}
	}
	return internal, nil
}
