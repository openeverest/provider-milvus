package provider

import (
	"testing"

	"github.com/openeverest/openeverest/v2/provider-runtime/conformance"
)

// The harness probes with a scalar, which is not a YAML mapping and so is
// rejected; TestMilvusEngineConfig covers how configuration reaches spec.config.
var engineConfigurationPaths = map[string]string{
	"spec.components.standalone.parameters.configuration": "takes a YAML mapping; covered by TestMilvusEngineConfig",
	"spec.components.proxy.parameters.configuration":      "takes a YAML mapping; covered by TestMilvusEngineConfig",
}

func TestUISchemaIsReconciled(t *testing.T) {
	conformance.UISchemaIsReconciled(t, conformance.Config{
		Provider:     New(),
		Unverifiable: engineConfigurationPaths,
	})
}

func TestSupportedFieldsAreReconciled(t *testing.T) {
	conformance.SupportedFieldsAreReconciled(t, conformance.Config{
		Provider: New(),
	})
}
