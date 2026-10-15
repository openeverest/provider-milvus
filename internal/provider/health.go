package provider

import (
	"context"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

const (
	milvusInstanceLabel   = "app.kubernetes.io/instance"
	milvusNameLabel       = "app.kubernetes.io/name"
	milvusComponentLabel  = "app.kubernetes.io/component"
	helmReleaseAnnotation = "meta.helm.sh/release-name"
)

// dependencyConditions are the operator's checks on etcd, object storage and
// the message stream. Its Healthy status only covers the Milvus components.
var dependencyConditions = []string{"EtcdReady", "StorageReady", "MsgStreamReady"}

// dependencyReleaseSuffixes name the Helm releases (<instance>-<suffix>) the
// operator installs for bundled dependencies.
var dependencyReleaseSuffixes = []string{"etcd", "minio", "pulsar", "kafka"}

// failingDependency returns the first dependency check the operator reports
// as failed.
func failingDependency(cr *milvusapi.Milvus) (string, bool) {
	for _, conditionType := range dependencyConditions {
		for _, condition := range cr.Status.Conditions {
			if condition.Type != conditionType || condition.Status != corev1.ConditionFalse {
				continue
			}
			detail := condition.Message
			if detail == "" {
				detail = condition.Reason
			}
			return fmt.Sprintf("%s: %s", conditionType, detail), true
		}
	}
	return "", false
}

type workloadReplicas struct {
	desired, ready int32
}

// shortWorkloads describes the Milvus components and bundled dependencies
// running fewer ready pods than desired, e.g. "queryNode 1/2 ready"; down
// reports whether any of them has no ready pod at all.
func shortWorkloads(c *controller.Context) (short []string, down bool, err error) {
	replicas := map[string]*workloadReplicas{}
	count := func(name string, desired *int32, ready int32) {
		want := int32(1)
		if desired != nil {
			want = *desired
		}
		// Idle blue/green query node deployments are scaled to zero.
		if name == "" || want == 0 {
			return
		}
		if replicas[name] == nil {
			replicas[name] = &workloadReplicas{}
		}
		replicas[name].desired += want
		replicas[name].ready += ready
	}

	deployments := &appsv1.DeploymentList{}
	if err := c.List(deployments); err != nil {
		return nil, false, err
	}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		count(workloadName(c.Name(), d), d.Spec.Replicas, d.Status.ReadyReplicas)
	}
	statefulSets := &appsv1.StatefulSetList{}
	if err := c.List(statefulSets); err != nil {
		return nil, false, err
	}
	for i := range statefulSets.Items {
		s := &statefulSets.Items[i]
		count(workloadName(c.Name(), s), s.Spec.Replicas, s.Status.ReadyReplicas)
	}

	for name, r := range replicas {
		if r.ready < r.desired {
			short = append(short, fmt.Sprintf("%s %d/%d ready", name, r.ready, r.desired))
			down = down || r.ready == 0
		}
	}
	sort.Strings(short)
	return short, down, nil
}

// workloadName names a workload of this instance as users know it: the
// Instance component for Milvus workloads, the workload name without the
// instance prefix for bundled dependencies, and "" for anything else.
func workloadName(instance string, workload client.Object) string {
	labels := workload.GetLabels()
	if labels[milvusInstanceLabel] == instance && labels[milvusNameLabel] == "milvus" {
		return instanceComponentName(labels[milvusComponentLabel])
	}
	if dependencyInstance(workload.GetAnnotations()[helmReleaseAnnotation]) == instance {
		return strings.TrimPrefix(workload.GetName(), instance+"-")
	}
	return ""
}

// instanceComponentName maps the operator's lowercase component label to
// the Instance component name.
func instanceComponentName(operatorName string) string {
	for _, name := range []string{
		common.ComponentStandalone, common.ComponentProxy, common.ComponentMixCoord,
		common.ComponentDataNode, common.ComponentQueryNode, common.ComponentStreaming,
	} {
		if strings.EqualFold(name, operatorName) {
			return name
		}
	}
	return operatorName
}

// dependencyInstance returns the instance a bundled dependency release
// belongs to, or "".
func dependencyInstance(release string) string {
	for _, suffix := range dependencyReleaseSuffixes {
		if instance, ok := strings.CutSuffix(release, "-"+suffix); ok {
			return instance
		}
	}
	return ""
}

// enqueueDependencyInstance re-evaluates an instance when one of its bundled
// dependency workloads changes; the operator does not surface their replica
// counts on the Milvus CR.
func enqueueDependencyInstance(_ context.Context, obj client.Object) []reconcile.Request {
	instance := dependencyInstance(obj.GetAnnotations()[helmReleaseAnnotation])
	if instance == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: instance}}}
}
