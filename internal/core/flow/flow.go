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
	flowConns []Conn

	stageConfigs map[string]StageConfig // todo: pointer? copying should be fine since this should never change
}

// Conn represents a directed connection between two addresses.
type Conn struct{ From, To msg.Addr }

// A connection from an address to itself, often used to expose
// a stage output as a flow output with the same address.
func SelfConn(addr msg.Addr) Conn {
	return Conn{From: addr, To: addr}
}

func NewFlow(flowID string, ps *pubsub.PubSub, stages []Stage, stageConns []Conn, flowConns []Conn) (*Flow, error) {
	sv := suture.NewSimple(flowID)

	// Create maps from stage ID to input and output channel
	stageConfigs := map[string]StageConfig{}

	for _, s := range stages {
		cfg := StageConfig{
			In:    make(chan msg.MsgTo, 100),
			Out:   make(chan msg.MsgFrom, 100),
			Trace: make(chan TraceEvent, 100),
		}
		stageConfigs[s.ID()] = cfg
	}

	// Validate that all stages referenced from stageConns exist. We do not yet validate ports.
	for _, conn := range stageConns {
		if _, ok := stageConfigs[conn.From.Stage]; !ok {
			return nil, fmt.Errorf("stage conn: 'from' stage does not exist: %v", conn.From)
		}
		if _, ok := stageConfigs[conn.To.Stage]; !ok {
			return nil, fmt.Errorf("stage conn: 'from' stage does not exist: %v", conn.From)
		}
	}

	for _, conn := range flowConns {
		if _, ok := stageConfigs[conn.From.Stage]; !ok {
			return nil, fmt.Errorf("flow conn: 'from' stage does not exist: %v", conn.From)
		}
	}

	for _, s := range stages {
		s.Connect(stageConfigs[s.ID()])
		sv.Add(s)
	}

	return &Flow{
		StageBase:  NewStageBase(flowID),
		Supervisor: sv,

		ps:         ps,
		stages:     stages,
		stageConns: stageConns,
		flowConns:  flowConns,

		stageConfigs: stageConfigs,
	}, nil
}

func (f *Flow) Connect(cfg StageConfig) {
	f.StageBase.Connect(cfg)

	// Create a coordinator service to coordinate message
	// delivery between the flow and its stages.
	c := coordinator{
		flowID:       f.ID(),
		ps:           f.ps,
		stageConns:   f.stageConns,
		flowConfig:   cfg,
		flowConns:    f.flowConns,
		stageConfigs: f.stageConfigs,
	}

	// The coordinator is treated as another service, alongside the stages.
	// We add the coordinator in Connect since it needs to know the flow config.
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
