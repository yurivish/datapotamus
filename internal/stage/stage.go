package stage

import (
	"context"

	"datapotamus.com/internal/message"
)

type Stage interface {
	// Process an incoming message on an input port, and emit zero or
	// more messages to output ports.
	// This function will never be called concurrently.
	// I am now thinking that yield should be a channel so we can race sends with ctx.Done()...
	Step(ctx context.Context, port string, m message.Msg, yield func(string, message.Msg))
}
