package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"go.k6.io/k6/js/modules"
)

// RootModule is the root module for the Pulsar Client
type RootModule struct{}

func init() {
	modules.Register("k6/x/pulsar", new(RootModule))
}

// Pulsar is the k6 extension implementing
// Connect, Publish and Subscribe methods
// using the apache/pulsar-client-go
type Pulsar struct { //nolint:revive
	vu       modules.VU
	client   pulsar.Client
	producer pulsar.Producer
	consumer pulsar.Consumer
}

// NewModuleInstance implements the modules.Module interface and returns
// a new instance for each VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &Pulsar{vu: vu}
}

// Exports exposes the given object in ts
func (p *Pulsar) Exports() modules.Exports {
	return modules.Exports{
		Named: map[string]interface{}{
			"Connect":             p.Connect,
			"IsConnected":         p.IsConnected,
			"Send":                p.Send,
			"IsProducerConnected": p.IsProducerConnected,
			"Receive":             p.Receive,
			"IsConsumerConnected": p.IsConsumerConnected,
			"Close":               p.Close,
		},
	}
}

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Instance = &Pulsar{}
	_ modules.Module   = &RootModule{}
)
