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

// A Flow is a collection of processing stages connected together by a coordinator.
type Flow struct {
	// A flow is a stage
	stage.Base

	// A flow is both a service and a supervisor of
	// individual stages and the coordinator.
	*suture.Supervisor

	ps     *pubsub.PubSub
	stages []stage.Stage
	conns  []Conn
	// Stage output ports that are also flow output ports.
	// The `From` field specifies the flow-internal stage/port address
	// and the `To` field specifies how to present that to the outside world.
	// If from and to are identical then the internal structure of the stage
	// will be mirrored in the output from on flow.Out.
	outputs []Conn

	stageIns  map[string]chan msg.InMsg
	stageOuts map[string]chan msg.OutMsg
}

// Conn represents a directed connection between two addresses.
type Conn struct{ From, To msg.Addr }

// A connection from an address to itself.
// Useful for exposing stage outputs as flow outputs with the same stage and port.
func IdentityConn(addr msg.Addr) Conn {
	return Conn{From: addr, To: addr}
}

func NewFlow(flowID string, ps *pubsub.PubSub, stages []stage.Stage, conns []Conn, outputs []Conn) (*Flow, error) {
	sv := suture.NewSimple(flowID)

	// Create maps from stage ID to input and output channel
	stageIns := map[string]chan msg.InMsg{}
	stageOuts := map[string]chan msg.OutMsg{}

	for _, s := range stages {
		// todo:
		// these are only closed by the coordinator on successful completion.
		// do we need to close them in error cases?
		in := make(chan msg.InMsg, 100)
		out := make(chan msg.OutMsg, 100)
		s.Init(stage.Config{In: in, Out: out})
		stageID := s.ID()
		stageIns[stageID] = in
		stageOuts[stageID] = out
	}

	// Validate that all connection stages exist. We do not yet validate ports.
	for _, conn := range conns {
		if _, ok := stageIns[conn.From.Stage]; !ok {
			return nil, fmt.Errorf("flow: 'from' stage does not exist: %v", conn.From)
		}
		if _, ok := stageIns[conn.To.Stage]; !ok {
			return nil, fmt.Errorf("flow: 'from' stage does not exist: %v", conn.From)
		}
	}

	for _, s := range stages {
		sv.Add(s)
	}

	return &Flow{
		Base:       stage.NewBase(flowID),
		Supervisor: sv,

		ps:      ps,
		stages:  stages,
		conns:   conns,
		outputs: outputs,

		stageIns:  stageIns,
		stageOuts: stageOuts,
	}, nil
}

func (f *Flow) Init(cfg stage.Config) {
	f.Base.Init(cfg)

	// Create a coordinator service to coordinate message
	// delivery between the flow and its stages.
	c := coordinator{
		flowID: f.ID(),
		ps:     f.ps,
		conns:  f.conns,

		flowIn:      f.In,
		flowOut:     f.Out,
		flowOutputs: f.outputs,

		stageIns:  f.stageIns,
		stageOuts: f.stageOuts,
	}

	// The coordinator is treated as another service, alongside the stages.
	// We add the coordinator in Init so that it doesn't get re-added if f.Serve re-runs.
	f.Add(&c)
}

func (f *Flow) Serve(ctx context.Context) error {
	err := f.Supervisor.Serve(ctx)
	if err != nil {
		// If the flow fails, do not automatically restart it.
		return errors.Join(suture.ErrDoNotRestart, err)
	}
	return nil
}

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
		defer pubsub.Sub(c.ps, subj, func(subj string, m msg.Msg) {
			fmt.Println("received stage message:", m)
			in := c.stageIns[conn.To.Stage] // all ports share the same in channel
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
			for m := range out {
				subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.flowID, m.Stage, m.Port)
				fmt.Println("got stage out", m, "sending to", subj)
				pubsub.Pub(c.ps, subj, m.Msg)
			}
		})
	}

	// Launch a goroutine to publish flow input messages to the appropriate stage subject
	// Note that this has to happen before the connections are wired up (above).
	wg.Go(func() {
		for m := range c.flowIn {
			fmt.Println("received flow in:", m, "sending to stage", c.stageIns[m.Stage])
			c.stageIns[m.Stage] <- m
		}
	})

	// Wait until the flow is finished
	<-ctx.Done()

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

	return nil
}
