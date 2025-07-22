package pulsar

import (
	"go.k6.io/k6/js/modules"
)

// RootModule is the root module for the pulsar API
type RootModule struct{}

func init() {
	modules.Register("k6/x/pulsar", new(RootModule))
}

// PulsarAPI is the k6 extension implementing the Pulsar api
type PulsarAPI struct { //nolint:revive
	vu modules.VU
}

// NewModuleInstance implements the modules.Module interface and returns
// a new instance for each VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &PulsarAPI{vu: vu}
}

// Exports exposes the given object in ts
func (m *PulsarAPI) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]interface{}{
			"Client": m.client,
		},
	}
}

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &PulsarAPI{}
	_ modules.Module   = &RootModule{}
)
