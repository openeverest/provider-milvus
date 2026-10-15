package provider

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
)

func probeParams(t *testing.T, probes *components.Probes) *runtime.RawExtension {
	t.Helper()
	raw, err := json.Marshal(components.MilvusParameters{Probes: probes})
	require.NoError(t, err)
	return &runtime.RawExtension{Raw: raw}
}

func TestBuildMilvusSpecProbes(t *testing.T) {
	t.Run("untuned components keep the operator's probes", func(t *testing.T) {
		spec, err := BuildMilvusSpec(newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
		}))
		require.NoError(t, err)
		assert.Nil(t, spec.Com.QueryNode.Probes)
	})

	t.Run("a tuned probe keeps the operator's check and every other default", func(t *testing.T) {
		spec, err := BuildMilvusSpec(newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentQueryNode: {Parameters: probeParams(t, &components.Probes{
					Startup: &components.ProbeTuning{FailureThreshold: ptr.To(int32(60))},
				})},
			},
		}))
		require.NoError(t, err)

		probes := spec.Com.QueryNode.Probes
		require.NotNil(t, probes)
		want := defaultStartupProbe()
		want.FailureThreshold = 60
		assert.Equal(t, &want, probes.StartupProbe)
		assert.Nil(t, probes.LivenessProbe)
		assert.Nil(t, probes.ReadinessProbe)
		assert.Nil(t, spec.Com.DataNode.Probes, "tuning is per component")
	})

	t.Run("an empty probes block changes nothing", func(t *testing.T) {
		spec, err := BuildMilvusSpec(newTestContext(t, corev1alpha1.InstanceSpec{
			Topology: &corev1alpha1.TopologySpec{Type: "standalone"},
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentStandalone: {Parameters: probeParams(t, &components.Probes{})},
			},
		}))
		require.NoError(t, err)
		assert.Nil(t, spec.Com.Standalone.Probes)
	})
}

func TestValidateProbes(t *testing.T) {
	tests := []struct {
		name    string
		probes  *components.Probes
		wantErr string
	}{
		{name: "tuned", probes: &components.Probes{Readiness: &components.ProbeTuning{
			InitialDelaySeconds: ptr.To(int32(0)), PeriodSeconds: ptr.To(int32(5)), TimeoutSeconds: ptr.To(int32(2)), FailureThreshold: ptr.To(int32(3)),
		}}},
		{
			name:    "zero period",
			probes:  &components.Probes{Liveness: &components.ProbeTuning{PeriodSeconds: ptr.To(int32(0))}},
			wantErr: `component "queryNode" probes.liveness.periodSeconds must be at least 1`,
		},
		{
			name:    "negative delay",
			probes:  &components.Probes{Startup: &components.ProbeTuning{InitialDelaySeconds: ptr.To(int32(-1))}},
			wantErr: `component "queryNode" probes.startup.initialDelaySeconds must not be negative`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestContext(t, corev1alpha1.InstanceSpec{
				Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
				Components: map[string]corev1alpha1.ComponentSpec{
					common.ComponentQueryNode: {Parameters: probeParams(t, tt.probes)},
				},
			})
			err := validateInstance(c)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}
