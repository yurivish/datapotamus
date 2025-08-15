package flow

import (
	"context"
	"errors"
	"fmt"

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

// A connection from an address to itself, often used to expose
// a stage output as a flow output with the same address.
func SelfConn(addr msg.Addr) Conn {
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
