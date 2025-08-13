package stage

import (
	"context"

	"datapotamus.com/internal/message"
)

// clojure async flow: No key may be present in both :ins and :outs, allowing for a uniform
// channel coordinate system of [:process-id :channel-id]. (For us, :stage-id :port-id)

type Stage interface {
	// Process an incoming message on an input port, emitting zero or more messages to output ports.
	// The context is used for external cancellation.
	// This function will never be called concurrently. It is permitted to spawn work that persists
	// past the return of the step, though any sends to the out channel need to ensure the channel
	// is not yet closed.
	HandleMessage(ctx context.Context, m *message.PortMsg, ch chan<- *message.PortMsg)
}
