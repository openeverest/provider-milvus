package provider

import (
	"cmp"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// Reasons of the problems the provider reports. A problem's reason becomes
// the status word users read, so each names one cause.
const (
	// reasonStoppedOutsideOpenEverest: the operator reports Milvus stopped.
	// An Instance cannot ask for that, so it was stopped by someone else.
	reasonStoppedOutsideOpenEverest = "StoppedOutsideOpenEverest"
)

// Reasons explaining why clients cannot connect, or why that is unknown.
const (
	reasonStopped            = "Stopped"
	reasonDependencyNotReady = "DependencyNotReady"
	reasonEndpointPending    = "EndpointPending"
	reasonComponentsNotReady = "ComponentsNotReady"
)

// Reasons of the operations the provider reports.
const (
	reasonUpgradingVersion = "UpgradingVersion"
	reasonRestoringBackup  = "RestoringBackup"
)

// Status reports what the provider observes about Milvus: whether it runs
// what the Instance asks for, whether clients can connect, which long
// operation is running, and how many pods each component will have. The
// runtime derives the Instance's phase and conditions from these facts.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	cr, err := uncachedMilvus(c)
	if apierrors.IsNotFound(err) {
		return withDesiredReplicas(c, controller.Progressing("waiting for Milvus CR to be created")), nil
	}
	if err != nil {
		return controller.Status{}, err
	}

	// The runtime reports the seeding of a new Instance as a restore during
	// which clients cannot connect, and the connection details are published
	// only once it has finished.
	if ds := c.GetDataSourceStatus(); ds != nil && !ds.Done {
		return withDesiredReplicas(c, controller.Progressing(cmp.Or(ds.Message, "seeding the instance from its data source"))), nil
	}

	status, err := observeMilvus(c, cr)
	if err != nil {
		return controller.Status{}, err
	}

	// milvus-backup restores collections through the running instance, so
	// clients can still connect while a Restore runs.
	restoring, err := activeRestore(c)
	if err != nil {
		return controller.Status{}, err
	}
	if restoring {
		status = status.WithOperation(corev1alpha1.ConditionRestoring, reasonRestoringBackup, "a restore is running")
		status.Message = cmp.Or(status.Message, "a restore is running")
	}

	if status.Message != "" {
		if reason, message, upgrading := upgrade(cr); upgrading {
			status = status.WithOperation(corev1alpha1.ConditionUpgrading, reason, message)
		}
	}

	status.Converged = status.Message == ""
	return withDesiredReplicas(c, status), nil
}

// uncachedMilvus reads the Milvus CR from the API server instead of the
// cache. Sync has just applied it, and the cache may still hold the object
// from before that: its status would then be compared with a spec that is no
// longer the latest, and the Instance would read as ready for a change the
// operator has not seen. The client reads unstructured objects without the
// cache.
func uncachedMilvus(c *controller.Context) (*milvusapi.Milvus, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(milvusapi.SchemeGroupVersion.WithKind("Milvus"))
	if err := c.Get(obj, c.Name()); err != nil {
		return nil, fmt.Errorf("get Milvus %q: %w", c.Name(), err)
	}
	cr := &milvusapi.Milvus{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, cr); err != nil {
		return nil, fmt.Errorf("decode Milvus %q: %w", c.Name(), err)
	}
	return cr, nil
}

// observeMilvus turns the Milvus CR and the workloads it runs into the facts
// the provider reports. A non-empty Message means Milvus does not run what
// the Instance asks for yet.
func observeMilvus(c *controller.Context, cr *milvusapi.Milvus) (controller.Status, error) {
	switch cr.Status.Status {
	case milvusapi.StatusHealthy:
		return observeHealthy(c, cr)
	case milvusapi.StatusPending, milvusapi.StatusDeleting:
		return controller.Progressing(notReadyMessage(cr, "Milvus is being initialized or updated")), nil
	case milvusapi.StatusStopped:
		return controller.Progressing("Milvus is stopped").
			WithUnavailable(reasonStopped, "Milvus is stopped").
			WithProblem(reasonStoppedOutsideOpenEverest,
				"the Milvus CR was stopped outside OpenEverest; start it again to serve clients"), nil
	case milvusapi.StatusUnhealthy:
		// Query nodes roll blue/green, so Milvus reports Unhealthy mid-rollout.
		if message, rolling := rolloutMessage(cr); rolling {
			return controller.Progressing(notReadyMessage(cr, message)), nil
		}
		return controller.Progressing(notReadyMessage(cr, "Milvus is unhealthy")), nil
	default:
		return controller.Progressing("Milvus is initializing"), nil
	}
}

// observeHealthy reports a Milvus the operator calls healthy. The operator's
// health covers the Milvus components only, and notices a workload with no
// ready pod only once its Deployment stops being Available, so the
// dependencies and the workloads are checked too.
func observeHealthy(c *controller.Context, cr *milvusapi.Milvus) (controller.Status, error) {
	if failure, failing := failingDependency(cr); failing {
		message := "a Milvus dependency is not ready: " + failure
		status := controller.Progressing(message)
		status.Available, status.AvailableReason, status.AvailableMessage =
			controller.AvailabilityUnknown, reasonDependencyNotReady, message
		return status, nil
	}
	host, port, ready, message := resolveEndpoint(c, cr)
	if !ready {
		return controller.Progressing(message).WithUnavailable(reasonEndpointPending, message), nil
	}

	creds, err := ensureCredentials(c)
	if err != nil {
		return controller.Status{}, err
	}
	tls, err := currentTLS(c)
	if err != nil {
		return controller.Status{}, err
	}
	// Clients keep the working password until a rotation lands.
	status := controller.Status{
		Available:         controller.Available,
		ConnectionDetails: connectionDetails(host, port, creds.appliedPassword, tls),
	}

	// A rollout can take a workload's last pod, as standalone Milvus is
	// recreated, so the workloads are checked before it.
	short, down, err := shortWorkloads(c)
	if err != nil {
		return controller.Status{}, err
	}
	if down {
		message := "Milvus is unavailable: " + strings.Join(short, ", ")
		return controller.Progressing(message).WithUnavailable(reasonComponentsNotReady, message), nil
	}
	// Otherwise Milvus keeps serving during a rollout, so the connection
	// stays published.
	if message, rolling := rolloutMessage(cr); rolling {
		status.Message = message
		return status, nil
	}
	if len(short) > 0 {
		status.Message = "serving with reduced redundancy: " + strings.Join(short, ", ")
	}
	return status, nil
}

// upgrade says whether Milvus is moving to another release. The operator
// records the image it runs as current only once every component is ready
// and updated, so until then the current image differs from the one asked
// for.
func upgrade(cr *milvusapi.Milvus) (reason, message string, upgrading bool) {
	current, desired := cr.Status.CurrentImage, cr.Spec.Com.Image
	if current == "" || desired == "" || current == desired {
		return "", "", false
	}
	from, to := cmp.Or(cr.Status.CurrentVersion, current), cmp.Or(cr.Spec.Com.Version, desired)
	return reasonUpgradingVersion, fmt.Sprintf("upgrading Milvus from %s to %s", from, to), true
}

// withDesiredReplicas reports how many pods each Milvus component will have
// once Milvus runs what the Instance asks for. The counts come from the
// Instance, with the defaults Sync applies. An autoscaled component is left
// out: its autoscaler, not the Instance, decides how many pods it runs.
func withDesiredReplicas(c *controller.Context, status controller.Status) controller.Status {
	in := c.Instance()
	for _, name := range allowedComponentsForTopology(instanceTopology(in)) {
		params := componentParameters(in, name)
		if params.Autoscaling != nil {
			continue
		}
		desired := ptr.Deref(in.Spec.Components[name].Replicas, 1)
		if len(params.Groups) > 0 {
			desired = 0
			for _, group := range params.Groups {
				desired += group.Replicas
			}
		}
		status = status.WithDesiredReplicas(name, desired)
	}
	return status
}
