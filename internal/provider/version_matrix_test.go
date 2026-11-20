package provider

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/mod/semver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller/validationtest"

	"github.com/openeverest/provider-milvus/internal/common"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// shippedProviderSpecPath is the Provider spec the chart installs.
const shippedProviderSpecPath = "../../charts/provider-milvus/generated/provider-spec.yaml"

// shippedProviderSpec is the Provider spec the chart installs, so the matrix
// covers exactly the versions users can pick.
func shippedProviderSpec(t *testing.T) corev1alpha1.ProviderSpec {
	t.Helper()
	data, err := os.ReadFile(shippedProviderSpecPath)
	require.NoError(t, err)
	var doc struct {
		Spec corev1alpha1.ProviderSpec `json:"spec"`
	}
	require.NoError(t, yaml.Unmarshal(data, &doc))
	require.NotEmpty(t, doc.Spec.Versions)
	return doc.Spec
}

func instanceAtVersion(spec corev1alpha1.ProviderSpec, topology, version string) corev1alpha1.InstanceSpec {
	components := map[string]corev1alpha1.ComponentSpec{}
	for name := range spec.Topologies[topology].Components {
		components[name] = corev1alpha1.ComponentSpec{Type: spec.Components[name].Type}
	}
	return corev1alpha1.InstanceSpec{
		Version:    version,
		Topology:   &corev1alpha1.TopologySpec{Type: topology},
		Components: components,
	}
}

// withProviderRef names this provider on spec, as every Instance does.
func withProviderRef(spec corev1alpha1.InstanceSpec) corev1alpha1.InstanceSpec {
	spec.ProviderRef.Name = common.ProviderName
	return spec
}

// runningDowngrades runs the downgrade check against the Milvus CR in c's
// cluster, if any.
func runningDowngrades(t *testing.T, c *controller.Context) error {
	t.Helper()
	existing := &milvusapi.Milvus{}
	if err := c.Get(existing, c.Name()); err != nil {
		return nil
	}
	desired, err := BuildMilvusSpec(c)
	require.NoError(t, err)
	return validateNoDowngrade(existing, desired).ToAggregate()
}

func TestShippedVersionsDeploy(t *testing.T) {
	spec := shippedProviderSpec(t)
	for _, bundle := range spec.Versions {
		for topology := range spec.Topologies {
			t.Run(bundle.Name+"/"+topology, func(t *testing.T) {
				instance := instanceAtVersion(spec, topology, bundle.Name)
				res := validationtest.New(t, New(), shippedProviderSpecPath).Run(&corev1alpha1.Instance{
					ObjectMeta: metav1.ObjectMeta{Name: "test-milvus", Namespace: "db"},
					Spec:       withProviderRef(instance),
				}, nil)
				require.Empty(t, res.Errors)

				milvus, err := BuildMilvusSpec(newTestContextWithProviderSpec(t, spec, instance))
				require.NoError(t, err)

				// e2e and users identify a release by its image tag.
				assert.True(t, strings.HasSuffix(milvus.Com.Image, ":v"+bundle.Name),
					"bundle %s deploys image %s", bundle.Name, milvus.Com.Image)
				assert.True(t, semver.IsValid("v"+milvus.Com.Version),
					"operator version %q is not semver", milvus.Com.Version)
				// The operator's ordered upgrade stalls unless every component shares one image.
				assert.Equal(t, milvusapi.ImageUpdateModeRollingUpgrade, milvus.Com.ImageUpdateMode)
				assert.Len(t, milvusComponentSpecs(milvus.Com), len(spec.Topologies[topology].Components))
			})
		}
	}
}

func TestShippedVersionUpgradePaths(t *testing.T) {
	spec := shippedProviderSpec(t)
	for _, from := range spec.Versions {
		for _, to := range spec.Versions {
			t.Run(from.Name+"->"+to.Name, func(t *testing.T) {
				deployed, err := BuildMilvusSpec(newTestContextWithProviderSpec(t, spec, instanceAtVersion(spec, "cluster", from.Name)))
				require.NoError(t, err)

				c := newTestContextWithProviderSpec(t, spec, instanceAtVersion(spec, "cluster", to.Name))
				require.NoError(t, c.Client().Create(context.Background(), &milvusapi.Milvus{
					ObjectMeta: metav1.ObjectMeta{Name: c.Name(), Namespace: c.Namespace()},
					Spec:       deployed,
				}))

				err = runningDowngrades(t, c)
				transition := validateNoDowngradeTransition(
					&corev1alpha1.Instance{Spec: instanceAtVersion(spec, "cluster", to.Name)},
					&corev1alpha1.Instance{Spec: instanceAtVersion(spec, "cluster", from.Name)}, &spec).ToAggregate()
				if semver.Compare("v"+operatorVersion(to.Name), "v"+operatorVersion(from.Name)) < 0 {
					assert.ErrorContains(t, err, "cannot be downgraded")
					assert.ErrorContains(t, transition, "cannot be downgraded")
					return
				}
				assert.NoError(t, err)
				assert.NoError(t, transition)
			})
		}
	}
}

func TestShippedDefaultVersionSupportsRestore(t *testing.T) {
	spec := shippedProviderSpec(t)
	assert.GreaterOrEqual(t, semver.Compare("v"+operatorVersion(spec.DefaultVersion), "v"+minRestoreVersion), 0,
		"default version %s cannot be restored into (needs %s)", spec.DefaultVersion, minRestoreVersion)
}
