package flow

import (
	"context"
	"fmt"
	"sync"

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

	stagesById := map[string]StageConfig{}
	for _, s := range stages {
		stagesById[s.ID()] = StageConfig{s, make(chan msg.PortMessage, 100), make(chan msg.PortMessage, 100)}
	}

	// what if we say flow ids have to be "flow.xxxx" - makes it easier to see in the suture log messages and we can use that as a component in the pubsub subjects
	for _, c := range conns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", id, c.src.Stage, c.src.Port)
		cancel := pubsub.Sub(ps, subj, func(subj string, m msg.Message) {
			cfg := stagesById[c.dst.Stage]
			cfg.in <- msg.PortMessage{Port: c.dst.Port, Message: m}
		})
		_ = cancel // i think we should do all this in Serve
	}

	sv := suture.NewSimple(id)
	for _, s := range stages {
		cfg := stagesById[s.ID()]
		s.Connect(cfg.in, cfg.out)
		sv.Add(s)
	}
	return &Flow{
		Supervisor: sv,
	}
}

func (f *Flow) Serve(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case m, ok := <-s.in:
				// if the input channel is closed then exit gracefully
				if !ok {
					return
				}
				results, err := s.Query(ctx, m.Data)
				// send the error, if any
				if err != nil {
					s.out <- msg.PortMessage{Port: "error", Message: msg.Message{Data: err}}
				} else {
					// otherwise send the results, if any.
					// todo: we could send partial results even in the face of an error, or
					// even multiple errors, if we decide that is the behavior we want.
					for _, result := range results {
						s.out <- msg.PortMessage{Port: "out", Message: msg.Message{Data: result}}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	})
	return nil
}
