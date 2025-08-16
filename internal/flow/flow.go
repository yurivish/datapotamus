package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"datapotamus.com/internal/flow/msg"
	"datapotamus.com/internal/flow/pubsub"
	"datapotamus.com/internal/flow/sublist"
	"github.com/thejerf/suture/v4"
)

// Conn represents a directed connection between two addresses.
type Conn struct {
	From msg.Addr
	To   msg.Addr
}

// A Flow is a collection of connected communicating stages
type Flow struct {
	Base                          // A flow is a stage
	sv         *suture.Supervisor // Flows supervise its substages
	ps         *pubsub.PubSub     // Flows manage inter-stage communication with pubsub
	stagesById map[string]Stage   // Stages indexed by their ID
	stageConns []Conn             // Connections between stages
	flowConns  []Conn             // Output connections from stages to the flow
}

func NewFlow(base *Base, ps *pubsub.PubSub, stages []Stage, stageConns []Conn, flowConns []Conn) (*Flow, error) {
	// Create maps from stage ID to input and output channel
	stagesById := map[string]Stage{}
	for _, s := range stages {
		if s.In() == nil {
			panic(fmt.Sprintf("flow: stage %q: stage In() channel must not be nil", s.ID()))
		}
		if s.Out() == nil {
			panic(fmt.Sprintf("flow: stage %q: stage Out() channel must not be nil", s.ID()))
		}
		if _, ok := stagesById[s.ID()]; ok {
			return nil, fmt.Errorf("flow: duplicate stage id: %q", s.ID())
		}
		stagesById[s.ID()] = s
	}

	// Validate that all stages referenced from stageConns exist.
	for _, conn := range stageConns {
		if _, ok := stagesById[conn.From.Stage]; !ok {
			return nil, fmt.Errorf("stage conn: 'from' stage does not exist: %v", conn.From)
		}
		if _, ok := stagesById[conn.To.Stage]; !ok {
			return nil, fmt.Errorf("stage conn: 'from' stage does not exist: %v", conn.From)
		}
	}

	// Validate that all stages referenced from flowConns exist.
	for _, conn := range flowConns {
		if _, ok := stagesById[conn.From.Stage]; !ok {
			return nil, fmt.Errorf("flow conn: 'from' stage does not exist: %v", conn.From)
		}
	}

	stageSupervisor := suture.NewSimple(base.ID())
	for _, s := range stages {
		stageSupervisor.Add(s)
	}

	return &Flow{
		Base:       *base,
		sv:         stageSupervisor,
		ps:         ps,
		stagesById: stagesById,
		stageConns: stageConns,
		flowConns:  flowConns,
	}, nil
}

func (f *Flow) Serve(ctx context.Context) error {
	// Create subscriptions and goroutines to coordinate message
	// delivery between the flow and its stages.
	//
	// Then, start the stage supervisor and wait for stages to complete.

	// todo: rewrite this whole "coordinator" bit -- i find this massively confusing!

	// Connect stage output subjects to stage input channels [todo: reword]
	// Listens to conn.From and forwards the results to the input channel designated by conn.To
	for _, conn := range f.stageConns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", f.ID(), conn.From.Stage, conn.From.Port)
		in := f.stagesById[conn.To.Stage].In()
		defer pubsub.Sub(f.ps, subj, func(subj string, m msg.Msg) {
			in <- m.To(conn.To)
		})()
	}

	// Connect stage output subjects to the flow output channel.
	// If `To` has a wildcard it will be dynamically set based on
	// the port each message was received on.
	for _, conn := range f.flowConns {
		toHasWildcards := conn.To.Stage == "*" || conn.To.Port == "*"
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", f.ID(), conn.From.Stage, conn.From.Port)
		defer pubsub.Sub(f.ps, subj, func(subj string, m msg.Msg) {
			to := conn.To
			if toHasWildcards {
				tsa := [3]string{} // tokenize the subject into a (hopefully) stack-allocated slice
				tts := sublist.TokenizeSubjectIntoSlice(tsa[:0], subj)
				stage, port := tts[1], tts[2]
				if conn.To.Stage == "*" {
					to.Stage = stage
				}
				if conn.To.Port == "*" {
					to.Port = port
				}
			}
			f.Ch.Out <- m.From(to)
		})()
	}

	var wg sync.WaitGroup

	// I am not confident that this the waitgroup closing logic is correct.
	flowTraceCh := f.Ch.Trace

	for _, s := range f.stagesById {
		// Connect output channels to their subjects
		wg.Go(func() {
			defer wg.Done()
			for m := range s.Out() {
				subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", f.ID(), m.Stage, m.Port)
				pubsub.Pub(f.ps, subj, m.Msg)
			}
		})

		// Connect trace channels to the flow trace output
		if flowTraceCh != nil {
			traceCh := s.Trace()
			if traceCh != nil {
				wg.Go(func() {
					defer wg.Done()
					for e := range traceCh {
						fmt.Printf("coord: got trace: %#v\n", e)
						flowTraceCh <- e
					}
				})
			}
		}
	}

	// Launch a goroutine to publish flow input messages to the appropriate stage subject
	// Note that this has to happen after the connections are wired up (above); otherwise
	// the stage In channels will be nil.
	wg.Go(func() {
		defer wg.Done()
		for m := range f.Ch.In {
			f.stagesById[m.Stage].In() <- m
		}
	})

	// Use a defer block so that this runs even if this function panics... or something
	defer func() {
		// Close stage inputs
		for _, s := range f.stagesById {
			close(s.In())
		}

		// Close stage outputs and traces
		// for _, s := range f.stagesById {
		// 	close(s.Out)
		// 	traceCh := s.Trace()
		// 	if traceCh != nil {
		// 		close(traceCh)
		// 	}
		// }

		// Drain stage outputs (ie. wait for output close)
		// todo: how do we do the same for trace?
		wg.Wait()
	}()

	// Wait until stages are finished
	err := <-f.sv.ServeBackground(ctx)
	if err != nil {
		// If the flow fails, do not automatically restart it.
		return errors.Join(suture.ErrDoNotRestart, err)
	}
	return nil
}

// Fulfill the HasSupervisor interface
func (f *Flow) GetSupervisor() *suture.Supervisor {
	return f.sv
}

// Returns a connection from an address to itself, which can be used
// to expose a stage output as a flow output with the same address.
func SelfConn(addr msg.Addr) Conn {
	return Conn{addr, addr}
}
