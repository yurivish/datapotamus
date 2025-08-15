package flow

import (
	"context"
	"errors"
	"fmt"

	"datapotamus.com/internal/core/msg"
	"datapotamus.com/internal/core/pubsub"
	"github.com/thejerf/suture/v4"
)

// A Flow is a collection of processing stages connected together by a coordinator.
type Flow struct {
	// A flow is a stage
	StageBase

	// A flow is both a service and a supervisor of
	// individual stages and the coordinator.
	*suture.Supervisor

	ps         *pubsub.PubSub
	stages     []Stage
	stageConns []Conn
	// Stage output ports that are also flow output ports.
	// The `From` field specifies the flow-internal stage/port address
	// and the `To` field specifies how to present that to the outside world.
	// If from and to are identical then the internal structure of the stage
	// will be mirrored in the output from on flow.Out.
	outputs []Conn

	stageIns    map[string]chan msg.MsgTo
	stageOuts   map[string]chan msg.MsgFrom
	stageTraces map[string]chan TraceEvent
}

// Conn represents a directed connection between two addresses.
type Conn struct{ From, To msg.Addr }

// A connection from an address to itself, often used to expose
// a stage output as a flow output with the same address.
func SelfConn(addr msg.Addr) Conn {
	return Conn{From: addr, To: addr}
}

func NewFlow(flowID string, ps *pubsub.PubSub, stages []Stage, stageConns []Conn, flowOutputs []Conn) (*Flow, error) {
	sv := suture.NewSimple(flowID)

	// Create maps from stage ID to input and output channel
	stageIns := map[string]chan msg.MsgTo{}
	stageOuts := map[string]chan msg.MsgFrom{}
	stageTraces := map[string]chan TraceEvent{}

	for _, s := range stages {
		stageID := s.ID()
		stageIns[stageID] = make(chan msg.MsgTo, 100)
		stageOuts[stageID] = make(chan msg.MsgFrom, 100)
		stageTraces[stageID] = make(chan TraceEvent, 100)
	}

	// Validate that all connection stages exist. We do not yet validate ports.
	for _, conn := range stageConns {
		if _, ok := stageIns[conn.From.Stage]; !ok {
			return nil, fmt.Errorf("flow: 'from' stage does not exist: %v", conn.From)
		}
		if _, ok := stageIns[conn.To.Stage]; !ok {
			return nil, fmt.Errorf("flow: 'from' stage does not exist: %v", conn.From)
		}
	}

	for _, s := range stages {
		stageID := s.ID()
		s.Init(StateConfig{
			In:    stageIns[stageID],
			Out:   stageOuts[stageID],
			Trace: stageTraces[stageID],
		})
		sv.Add(s)
	}

	return &Flow{
		StageBase:  NewStageBase(flowID),
		Supervisor: sv,

		ps:         ps,
		stages:     stages,
		stageConns: stageConns,
		outputs:    flowOutputs,

		stageIns:    stageIns,
		stageOuts:   stageOuts,
		stageTraces: stageTraces,
	}, nil
}

func (f *Flow) Init(cfg StateConfig) {
	f.StageBase.Init(cfg)

	// Create a coordinator service to coordinate message
	// delivery between the flow and its stages.
	c := coordinator{
		flowID:     f.ID(),
		ps:         f.ps,
		stageConns: f.stageConns,

		flowIn:    f.In,
		flowOut:   f.Out,
		flowTrace: f.Trace,

		flowOutputs: f.outputs,

		stageIns:    f.stageIns,
		stageOuts:   f.stageOuts,
		stageTraces: f.stageTraces,
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
