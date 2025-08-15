package flow

import (
	"context"
	"fmt"
	"sync"

	"datapotamus.com/internal/msg"
	"datapotamus.com/internal/pubsub"
)

// The coordinator is a suture Service that connects flow stages to each other through pubsub.
// It does not need to know about the stages directly; it concerns itself with plumbing
// messages from `out` channels to pubsub subjects, and from pubsub subjects to `in` channels.
// It also plumbs messages from flowIn and to flowOut, handling all communication within a flow.
type coordinator struct {
	flowID string
	ps     *pubsub.PubSub

	// Connections between stages
	conns []Conn
	// Channel on which the flow receives input messages
	flowIn <-chan msg.InMsg
	// Channel on which the flow sends output messages
	flowOut chan<- msg.OutMsg
	// Connections that expose internal stage ports as flow outputs.
	// The From field specifies the (stage, port) inside the flow and
	// the To field specifies the external name and port on the flow,
	// allowing us to decouple the internal stage structure from the
	// stages and ports presented by this flow to the outside world.
	flowOutputs []Conn
	// Map from stage ID to input channel for that stage
	stageIns map[string]chan msg.InMsg
	// Map from stage ID to output channel for that stage
	stageOuts map[string]chan msg.OutMsg
}

func (c *coordinator) Serve(ctx context.Context) error {
	// Connect stage output subjects to input channels
	for _, conn := range c.conns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.flowID, conn.From.Stage, conn.From.Port)
		in := c.stageIns[conn.To.Stage]
		defer pubsub.Sub(c.ps, subj, func(subj string, m msg.Msg) {
			in <- m.In(conn.To)
		})()
	}

	// Connect stage output subjects to the flow output channel.
	// Note that we could make flowOutputs a list of Conns so that you can re-map internal stage outputs/ports
	// to new stage/port names to present a cleaner abstraction to the world outside of the flow.
	for _, conn := range c.flowOutputs {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.flowID, conn.From.Stage, conn.From.Port)
		defer pubsub.Sub(c.ps, subj, func(subj string, m msg.Msg) {
			c.flowOut <- m.Out(conn.To)
		})()
	}

	var wg sync.WaitGroup

	// Connect output channels to their subjects. The waitgroup will finish when
	// the output channel is closed and remaining messages are processed.
	// todo: We might need to do something different and simply drain out unprocessed messages.
	for _, out := range c.stageOuts {
		wg.Go(func() {
			defer wg.Done()
			for m := range out {
				subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.flowID, m.Stage, m.Port)
				pubsub.Pub(c.ps, subj, m.Msg)
			}
		})
	}

	// Launch a goroutine to publish flow input messages to the appropriate stage subject
	// Note that this has to happen before the connections are wired up (above).
	wg.Go(func() {
		defer wg.Done()
		for m := range c.flowIn {
			c.stageIns[m.Stage] <- m
		}
	})

	// Use a defer block so that this runs even if this function panics... or something
	defer func() {
		// Close stage inputs
		for _, ch := range c.stageIns {
			close(ch)
		}

		// Close stage outputs
		for _, ch := range c.stageOuts {
			close(ch)
		}

		// Drain stage outputs
		wg.Wait()
	}()

	// Wait until the flow is finished
	<-ctx.Done()

	return nil
}
