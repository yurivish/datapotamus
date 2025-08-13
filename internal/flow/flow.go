package flow

import (
	"context"
	"fmt"

	"datapotamus.com/internal/msg"
	"datapotamus.com/internal/pubsub"
	"datapotamus.com/internal/stage"
	"github.com/thejerf/suture/v4"
)

type PortSpec struct {
	Stage string
	Port  string
}

type Conn struct {
	src PortSpec
	dst PortSpec
}

type Flow struct {
	*suture.Supervisor
	id     string
	ps     *pubsub.PubSub
	stages []stage.Stage
	conns  []Conn
	ins    []chan msg.PortMessage
	outs   []chan msg.PortMessage
}

// I think the idea is the caller, who has the super supervisor, starts this when it is added.
func NewFlow(id string, ps *pubsub.PubSub, stages []stage.Stage, conns []Conn) *Flow {
	var ins, outs []chan msg.PortMessage
	sv := suture.NewSimple(id)
	for _, s := range stages {
		in := make(chan msg.PortMessage, 100)
		ins = append(ins, in)
		out := make(chan msg.PortMessage, 100)
		outs = append(outs, out)
		s.Connect(in, out)
		sv.Add(s)
	}
	return &Flow{
		Supervisor: sv,
		id:         id,
		stages:     stages,
		conns:      conns,
		ins:        ins,
		outs:       outs,
	}
}

func (f *Flow) Serve(ctx context.Context) error {
	// Publish stage outputs to their port's pubsub subjects
	for i, c := range f.conns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", f.id, c.src.Stage, c.src.Port)
		defer pubsub.Sub(f.ps, subj, func(subj string, m msg.Message) {
			// note: i is wrong here since it is the index of the conn not stage
			f.ins[i] <- msg.PortMessage{Port: c.dst.Port, Message: m}
		})
	}
	// Subscribe stage inputs to their connected subjects
	for i, s := range f.stages {
		go func() {
			for {
				select {
				case m, ok := <-f.outs[i]:
					// if the input channel is closed then exit gracefully
					if !ok {
						return
					}
					subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", f.id, s.ID(), m.Port)
					pubsub.Pub(f.ps, subj, m.Message)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return nil
}

// todo: figure out how to connect the stages together.
// I think we want all outs to do a pub of the message to the port
// I think we want all ins to be a SubChan
// then we want to call stage.Connect(in,out)
// flowID := ulid.Make().String()

// type StageConfig struct {
// 	stage stage.Stage
// 	in    chan msg.PortMessage
// 	out   chan msg.PortMessage
// }

// stageConfigs := map[string]StageConfig{}
// for _, s := range stages {
// 	stageConfigs[s.ID()] = StageConfig{s, make(chan msg.PortMessage, 100), make(chan msg.PortMessage, 100)}
// }

// for _, c := range conns {
// 	subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", id, c.src.Stage, c.src.Port)
// 	cancel := pubsub.Sub(ps, subj, func(subj string, m msg.Message) {
// 		cfg := stageConfigs[c.dst.Stage]
// 		cfg.in <- msg.PortMessage{Port: c.dst.Port, Message: m}
// 	})
// 	_ = cancel // i think we should do all this in Serve
// }

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
