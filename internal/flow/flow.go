package flow

import (
	"context"
	"fmt"

	"datapotamus.com/internal/msg"
	"datapotamus.com/internal/pubsub"
	"datapotamus.com/internal/stage"
	"github.com/thejerf/suture/v4"
)

type Flow struct {
	*suture.Supervisor
}

type PortSpec struct {
	Stage string
	Port  string
}

type Conn struct {
	src PortSpec
	dst PortSpec
}

// I think the idea is the caller, who has the super supervisor, starts this when it is added.
func NewFlow(id string, stages []stage.Stage, conns []Conn, ps *pubsub.PubSub) *Flow {
	// todo: figure out how to connect the stages together.
	// I think we want all outs to do a pub of the message to the port
	// I think we want all ins to be a SubChan
	// then we want to call stage.Connect(in,out)
	// flowID := ulid.Make().String()

	type StageConfig struct {
		stage stage.Stage
		in    chan msg.PortMessage
		out   chan msg.PortMessage
	}

	stageConfigs := map[string]StageConfig{}
	for _, s := range stages {
		stageConfigs[s.ID()] = StageConfig{s, make(chan msg.PortMessage, 100), make(chan msg.PortMessage, 100)}
	}

	for _, c := range conns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", id, c.src.Stage, c.src.Port)
		cancel := pubsub.Sub(ps, subj, func(subj string, m msg.Message) {
			cfg := stageConfigs[c.dst.Stage]
			cfg.in <- msg.PortMessage{Port: c.dst.Port, Message: m}
		})
		_ = cancel // i think we should do all this in Serve
	}

	// var wg sync.WaitGroup

	// wg.Go(func() {
	// 	for range m := s.out {
	// 		select {
	// 		case m, ok := <-s.in:
	// 			// if the input channel is closed then exit gracefully
	// 			if !ok {
	// 				return
	// 			}

	// 		case <-ctx.Done():
	// 			return
	// 		}
	// 	}
	// })
	//
	//

	sv := suture.NewSimple(id)
	for _, s := range stages {
		cfg := stageConfigs[s.ID()]
		s.Connect(cfg.in, cfg.out)
		sv.Add(s)
	}
	return &Flow{
		Supervisor: sv,
	}
}

func (f *Flow) Serve(ctx context.Context) error {

	return nil
}
