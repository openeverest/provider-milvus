package provider

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// freeForm encodes free-form Kubernetes objects as the parameters carry them.
func freeForm(objects []map[string]any) []apiextensionsv1.JSON {
	encoded := make([]apiextensionsv1.JSON, 0, len(objects))
	for _, object := range objects {
		raw, err := json.Marshal(object)
		if err != nil {
			panic(err)
		}
		encoded = append(encoded, apiextensionsv1.JSON{Raw: raw})
	}
	return encoded
}

func podParams(t *testing.T, pod components.PodCustomization) *runtime.RawExtension {
	t.Helper()
	raw, err := json.Marshal(components.MilvusParameters{Pod: &pod})
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: raw}
}

func TestBuildMilvusSpecPodCustomization(t *testing.T) {
	sriov := components.PodCustomization{
		Annotations: map[string]string{"k8s.v1.cni.cncf.io/networks": "sriov-rdma"},
		Env:         []corev1.EnvVar{{Name: "QUERYNODE_IP", Value: "10.10.0.5"}},
		Volumes: freeForm([]map[string]any{{
			"name":     "nvidia-fs",
			"hostPath": map[string]any{"path": "/dev/nvidia-fs0"},
		}}),
		VolumeMounts:    []corev1.VolumeMount{{Name: "nvidia-fs", MountPath: "/dev/nvidia-fs0"}},
		SecurityContext: map[string]any{"capabilities": map[string]any{"add": []any{"IPC_LOCK"}}},
		InitContainers:  freeForm([]map[string]any{{"name": "resolve-ip", "image": "busybox"}}),
	}
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentQueryNode: {Parameters: podParams(t, sriov)},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	queryNode := spec.Com.QueryNode.Component
	assert.Equal(t, sriov.Annotations, queryNode.PodAnnotations)
	assert.Equal(t, sriov.Env, queryNode.Env)
	require.Len(t, queryNode.Volumes, 1)
	assert.Equal(t, "nvidia-fs", queryNode.Volumes[0]["name"])
	assert.Equal(t, sriov.VolumeMounts, queryNode.VolumeMounts)
	assert.Equal(t, milvusapi.Values(sriov.SecurityContext), queryNode.SecurityContext)
	require.Len(t, queryNode.InitContainers, 1)
	assert.Equal(t, "busybox", queryNode.InitContainers[0]["image"])

	assert.Empty(t, spec.Com.Proxy.PodAnnotations, "customization stays on its own component")
}

// Live clusters showed endless query node rollouts when a server-defaulted
// field was left unset: the operator diffs against the defaulted Deployment.
func TestPodCustomizationMatchesServerDefaults(t *testing.T) {
	podIP := &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}
	pod := components.PodCustomization{
		Env: []corev1.EnvVar{{Name: "QUERYNODE_IP", ValueFrom: podIP}},
		Volumes: freeForm([]map[string]any{
			{"name": "dev", "hostPath": map[string]any{"path": "/dev/nvidia-fs0"}},
			{"name": "podinfo", "downwardAPI": map[string]any{"items": []any{map[string]any{
				"path": "network-status", "fieldRef": map[string]any{"fieldPath": "metadata.annotations"},
			}}}},
		}),
		InitContainers: freeForm([]map[string]any{{"name": "init", "image": "busybox", "env": []any{map[string]any{
			"name": "IP", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "status.podIP"}},
		}}}}),
	}
	component := milvusapi.Component{}
	applyPodCustomization(&component, &pod)

	assert.Equal(t, "v1", component.Env[0].ValueFrom.FieldRef.APIVersion)

	var hostPath, downwardAPI corev1.Volume
	require.NoError(t, decodeKubernetesObject(component.Volumes[0], &hostPath))
	require.NoError(t, decodeKubernetesObject(component.Volumes[1], &downwardAPI))
	assert.Equal(t, ptr.To(corev1.HostPathUnset), hostPath.HostPath.Type)
	assert.Equal(t, ptr.To(corev1.DownwardAPIVolumeSourceDefaultMode), downwardAPI.DownwardAPI.DefaultMode)
	assert.Equal(t, "v1", downwardAPI.DownwardAPI.Items[0].FieldRef.APIVersion)

	var initContainer corev1.Container
	require.NoError(t, decodeKubernetesObject(component.InitContainers[0], &initContainer))
	assert.Equal(t, "v1", initContainer.Env[0].ValueFrom.FieldRef.APIVersion)
}

func TestValidatePodCustomization(t *testing.T) {
	tests := map[string]struct {
		pod     components.PodCustomization
		wantErr string
	}{
		"valid": {pod: components.PodCustomization{
			Volumes:         freeForm([]map[string]any{{"name": "data", "emptyDir": map[string]any{}}}),
			SecurityContext: map[string]any{"privileged": true},
		}},
		"env without name": {
			pod:     components.PodCustomization{Env: []corev1.EnvVar{{Value: "x"}}},
			wantErr: "pod.env[0].name is required",
		},
		"volume with unknown field": {
			pod:     components.PodCustomization{Volumes: freeForm([]map[string]any{{"name": "data", "hostPth": map[string]any{}}})},
			wantErr: "pod.volumes[0] is invalid",
		},
		"volume without name": {
			pod:     components.PodCustomization{Volumes: freeForm([]map[string]any{{"emptyDir": map[string]any{}}})},
			wantErr: "pod.volumes[0].name is required",
		},
		"security context with wrong type": {
			pod:     components.PodCustomization{SecurityContext: map[string]any{"privileged": "yes"}},
			wantErr: "pod.securityContext is invalid",
		},
		"init container without image": {
			pod:     components.PodCustomization{InitContainers: freeForm([]map[string]any{{"name": "init"}})},
			wantErr: "pod.initContainers[0] requires name and image",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentStandalone: {Parameters: podParams(t, tt.pod)},
				},
			})
			err := validateInstance(c)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestValidateRejectsMalformedComponentParameters(t *testing.T) {
	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentStandalone: {Parameters: &runtime.RawExtension{Raw: []byte(`{"pod":{"env":"QUERYNODE_IP=1"}}`)}},
		},
	})
	err := validateInstance(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `component "standalone" has invalid parameters`)
}
