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

func (f *Flow) SubjectFor(addr msg.Addr) string {
	return fmt.Sprintf("flow.%s.stage.%s.port.%s", f.ID(), addr.Stage, addr.Port)
}

func (f *Flow) Serve(ctx context.Context) error {
	// Create subscriptions and goroutines to coordinate
	// message delivery between the flow and its stages.

	// For each stage-to-stage connection, subscribe to the appropriate "From" subject
	// and send messages to the appropriate "To" channel. From may contain wildcards.
	// If the stage output is closed, fail the message.
	for _, conn := range f.stageConns {
		// The input of the "To" stage
		in := f.stagesById[conn.To.Stage].In()
		// The subject onto which the "From" messages are published
		subj := f.SubjectFor(conn.From)
		defer pubsub.Sub(f.ps, subj, func(subj string, m msg.Msg) {
			in <- m.To(conn.To)
		})()
	}

	// For each stage-to-flow output connection, subscribe to the appropriate "From" subject
	// and send messages to the appropriate "To" address, remapping wildcards as necessary.
	for _, conn := range f.flowConns {
		// The subject onto which the "From" messages are published
		subj := f.SubjectFor(conn.From)
		defer pubsub.Sub(f.ps, subj, func(subj string, m msg.Msg) {
			// Copy the "To" address by value so that we can override its fields in the case of wildcards
			to := conn.To

			// If the "To" subject has wildcards then we want to dynamically send to a different
			// output stage and/or port on a per-message basis.
			if to.Stage == "*" || to.Port == "*" {
				tsa := [3]string{} // tokenize the subject into a (hopefully) stack-allocated slice
				tts := sublist.TokenizeSubjectIntoSlice(tsa[:0], subj)
				stage, port := tts[1], tts[2]
				if to.Stage == "*" {
					to.Stage = stage
				}
				if to.Port == "*" {
					to.Port = port
				}
			}
			// Send the message to this flow's output channel
			f.Ch.Out <- m.From(to)
		})()
	}

	// The code above handles the subscribers -- now we handle publishing.

	var wg sync.WaitGroup

	flowTraceCh := f.Ch.Trace
	for _, s := range f.stagesById {
		// Each stage publishes its outputs onto the pubsub subject for that stage and port.
		wg.Go(func() {
			for m := range s.Out() {
				subj := f.SubjectFor(m.Addr)
				pubsub.Pub(f.ps, subj, m.Msg)
			}
		})

		// Each stage forwards its trace vanles to the flow trace channel
		if flowTraceCh != nil && s.Trace() != nil {
			wg.Go(func() {
				for e := range s.Trace() {
					flowTraceCh <- e
				}
			})
		}
	}

	// Start the stages
	errCh := f.sv.ServeBackground(ctx)

	// Publish flow input messages to the appropriate stage subjects.
	// We do not trace messages inside the Flow stage.
loop:
	for {
		select {
		case m, ok := <-f.Ch.In:
			if !ok {
				// find some way to kick off graceful shutdown...
				break loop
			}
			f.stagesById[m.Stage].In() <- m

		case <-ctx.Done():
			break loop
		}
	}

	if err := <-errCh; err != nil {
		// If the stage supervisor failed, do not automatically restart the flow.
		// todo: is there a way for us to say "if any stage fails, fail the supervisor"?
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
