package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"datapotamus.com/internal/msg"
	"datapotamus.com/internal/pubsub"
	"datapotamus.com/internal/stage"
	"github.com/thejerf/suture/v4"
)

// Represents a connection between a (stage, port) and another (stage, port).
type Conn struct {
	From msg.Addr
	To   msg.Addr
}

// Represents a flow, which is a collection of communicating processing stages
// connected together by a pubsub mechanism.
type Flow struct {
	stage.Base
	*suture.Supervisor
	Ready chan struct{}
}

func NewFlow(id string, ps *pubsub.PubSub, stages []stage.Stage, conns []Conn) (*Flow, error) {
	// Create maps from stage ID to input and output channel
	ins := map[string]chan msg.InMsg{}
	outs := map[string]chan msg.OutMsg{}
	for _, s := range stages {
		// todo:
		// these are only closed by the coordinator on successful completion.
		// do we need to close them in error cases?
		in := make(chan msg.InMsg, 100)
		out := make(chan msg.OutMsg, 100)
		s.Prepare(stage.Config{In: in, Out: out})
		id := s.ID()
		ins[id] = in
		outs[id] = out
	}

	// Validate that all connection endpoints are for stages that exist
	for _, conn := range conns {
		if _, ok := ins[conn.From.Stage]; !ok {
			return nil, fmt.Errorf("new flow: 'from' stage does not exist: %v", conn.From)
		}
		if _, ok := ins[conn.To.Stage]; !ok {
			return nil, fmt.Errorf("new flow: 'from' stage does not exist: %v", conn.From)
		}
	}

	sv := suture.NewSimple(id)
	// Create a coordinator to manage message flow between stages.
	// In the future we can also have a "tracker" which logs the
	// parentid-id relationships between messages and perhaps the
	// full set of message contents as well.
	c := NewCoordinator(id, ps, conns, ins, outs)

	// The coordinator is treated as another service, alongside the stages.
	sv.Add(c)

	for _, s := range stages {
		sv.Add(s)
	}

	return &Flow{
		Base:       stage.NewBase(id),
		Supervisor: sv,
		Ready:      c.Ready,
	}, nil
}

func (f *Flow) Serve(ctx context.Context) error {
	// We want flows to fail permanently if they fail, so
	// we override the Serve method of the embedded supervisor
	// to rethrow a different error.
	// The other option was ErrTerminateSupervisorTree but that seems a bit much.
	// Might be useful for stages to throw that, though.
	err := f.Supervisor.Serve(ctx)
	if err != nil {
		return errors.Join(suture.ErrDoNotRestart, err)
	}
	return nil
}

// The coordinator is a service that connects flow stages to each other through pubsub.
// This does not need to know about the stages directly; it concerns itself with plumbing
// messages from `out` channels to pubsub subjects, and from pubsub subjects to `in` channels.
type coordinator struct {
	ps    *pubsub.PubSub
	conns []Conn
	// ID of the flow
	id string
	// Map from stage ID to input channel for that stage
	ins map[string]chan msg.InMsg
	// Map from stage ID to output channel for that stage
	outs map[string]chan msg.OutMsg
	// Channel that is closed once the coordinator has created connections between stage channels,
	// signaling that the stages are ready to receive messages. Prior to this, messages sent to
	// input subjects would be lost due to there being no subscribers there to listen and
	// forward messages from pubsub subjects to the stage channels.
	Ready chan struct{}
}

func NewCoordinator(id string, ps *pubsub.PubSub, conns []Conn, ins map[string]chan msg.InMsg, outs map[string]chan msg.OutMsg) *coordinator {
	return &coordinator{
		id:    id,
		ps:    ps,
		conns: conns,
		ins:   ins,
		outs:  outs,
		Ready: make(chan struct{}),
	}
}

func (c *coordinator) Serve(ctx context.Context) error {
	// Connect output subjects to input channels, deferring unsubscription
	for _, conn := range c.conns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.id, conn.From.Stage, conn.From.Port)
		defer pubsub.Sub(c.ps, subj, func(subj string, m msg.Msg) {
			in := c.ins[conn.To.Stage] // all ports share the same in channel
			in <- m.In(conn.To)
		})()
	}

	// Signal that we are ready to receive outside messages to the "in" subjects.
	close(c.Ready)

	// Connect output channels to their subjects. The waitgroup will finish when
	// the output channel is closed and remaining messages are processed.
	// todo: We might need to do something different and simply drain out unprocessed messages.
	var wg sync.WaitGroup
	for _, out := range c.outs {
		wg.Go(func() {
			for m := range out {
				subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.id, m.Stage, m.Port)
				pubsub.Pub(c.ps, subj, m.Msg)
			}
		})
	}

	// Wait until the flow is finished
	<-ctx.Done()

	// Close the inputs, then outputs, then drain the outputs
	for _, ch := range c.ins {
		close(ch)
	}

	for _, ch := range c.outs {
		close(ch)
	}

	wg.Wait()

	return nil
}
