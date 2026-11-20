package provider

import (
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// groupableComponents are the components the operator can split into
// deployment groups.
var groupableComponents = []string{
	common.ComponentProxy,
	common.ComponentDataNode,
	common.ComponentQueryNode,
	common.ComponentStreaming,
}

// applyDeploymentGroups renders the component's groups. The operator ignores a
// grouped component's own replicas, so they are set to the groups' total to
// keep the Milvus CR self-consistent.
func applyDeploymentGroups(c *controller.Context, name string, component *milvusapi.Component) []milvusapi.DeploymentGroup {
	groups := componentParameters(c.Instance(), name).Groups
	if len(groups) == 0 {
		return nil
	}
	var total int32
	rendered := make([]milvusapi.DeploymentGroup, 0, len(groups))
	for _, group := range groups {
		total += group.Replicas
		deploymentGroup := milvusapi.DeploymentGroup{
			Name:        group.Name,
			Replicas:    ptr.To(group.Replicas),
			Annotations: group.Annotations,
			ExtraEnv:    withEnvDefaults(group.Env),
		}
		// Unset placement inherits the component's; the operator treats a set
		// (even empty) field as an override.
		if group.Affinity != nil {
			deploymentGroup.Affinity = &corev1.Affinity{}
			// Validate rejects affinities that do not decode.
			_ = decodeKubernetesObject(group.Affinity, deploymentGroup.Affinity)
		}
		if group.NodeSelector != nil {
			deploymentGroup.NodeSelector = &group.NodeSelector
		}
		if group.Tolerations != nil {
			deploymentGroup.Tolerations = &group.Tolerations
		}
		rendered = append(rendered, deploymentGroup)
	}
	component.Replicas = ptr.To(total)
	return rendered
}

func validateDeploymentGroups(name string, componentReplicas *int32, groups []components.DeploymentGroup) error {
	if len(groups) == 0 {
		return nil
	}
	if !slices.Contains(groupableComponents, name) {
		return fmt.Errorf("component %q does not support groups; use them on proxy, dataNode, queryNode or streamingNode", name)
	}
	seen := make(map[string]struct{}, len(groups))
	var total int32
	for i, group := range groups {
		if msgs := validation.IsDNS1123Label(group.Name); len(msgs) > 0 {
			return fmt.Errorf("component %q groups[%d].name %q is invalid: %s", name, i, group.Name, msgs[0])
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return fmt.Errorf("component %q has duplicate group %q", name, group.Name)
		}
		seen[group.Name] = struct{}{}
		if group.Replicas < 0 {
			return fmt.Errorf("component %q group %q replicas must be >= 0", name, group.Name)
		}
		for j, env := range group.Env {
			if env.Name == "" {
				return fmt.Errorf("component %q group %q env[%d].name is required", name, group.Name, j)
			}
		}
		if group.Affinity != nil {
			if err := decodeKubernetesObject(group.Affinity, &corev1.Affinity{}); err != nil {
				return fmt.Errorf("component %q group %q affinity is invalid: %w", name, group.Name, err)
			}
		}
		total += group.Replicas
	}
	if componentReplicas != nil && *componentReplicas != total {
		return fmt.Errorf("component %q replicas (%d) must equal the sum of its groups' replicas (%d)", name, *componentReplicas, total)
	}
	return nil
}
