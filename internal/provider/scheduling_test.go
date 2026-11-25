package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/contract"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
)

func TestBuildMilvusSpecSchedulingPolicy(t *testing.T) {
	affinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "node-role", Operator: corev1.NodeSelectorOpIn, Values: []string{"db"},
					}},
				}},
			},
		},
	}
	tolerations := []corev1.Toleration{{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoSchedule}}
	spread := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       corev1.LabelTopologyZone,
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}}

	c := newTestContext(t, corev1alpha1.InstanceSpec{
		Topology: &corev1alpha1.TopologySpec{Type: "cluster"},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentQueryNode: {
				SchedulingPolicy: &commonv1alpha1.SchedulingPolicy{
					SchedulerName:             "custom",
					NodeSelector:              map[string]string{"disk": "ssd"},
					Affinity:                  affinity,
					Tolerations:               tolerations,
					TopologySpreadConstraints: &spread,
				},
			},
		},
	})
	spec, err := BuildMilvusSpec(c)
	require.NoError(t, err)

	t.Run("policy is copied onto its component", func(t *testing.T) {
		got := spec.Com.QueryNode.ComponentSpec
		assert.Equal(t, "custom", got.SchedulerName)
		assert.Equal(t, map[string]string{"disk": "ssd"}, got.NodeSelector)
		assert.Equal(t, affinity, got.Affinity)
		assert.Equal(t, tolerations, got.Tolerations)
		require.Len(t, got.TopologySpreadConstraints, 1)
		assert.Equal(t, corev1.LabelTopologyZone, got.TopologySpreadConstraints[0].TopologyKey)
	})

	t.Run("spread without a selector counts the component's own pods", func(t *testing.T) {
		assert.Equal(t, &metav1.LabelSelector{MatchLabels: map[string]string{
			contract.ProviderLabel:    common.ProviderName,
			controller.InstanceLabel:  "test-milvus",
			controller.ComponentLabel: common.ComponentQueryNode,
		}}, spec.Com.QueryNode.TopologySpreadConstraints[0].LabelSelector)
		assert.Nil(t, spread[0].LabelSelector, "the Instance's policy must not be mutated")
	})

	t.Run("components without a policy get no placement", func(t *testing.T) {
		got := spec.Com.DataNode.ComponentSpec
		assert.Empty(t, got.SchedulerName)
		assert.Nil(t, got.NodeSelector)
		assert.Nil(t, got.Affinity)
		assert.Nil(t, got.Tolerations)
		assert.Nil(t, got.TopologySpreadConstraints)
	})
}
