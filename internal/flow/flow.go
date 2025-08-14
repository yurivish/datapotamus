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

type PortSpec struct {
	Stage string
	Port  string
}

type Conn struct {
	Src PortSpec
	Dst PortSpec
}

type Flow struct {
	*suture.Supervisor
	Ready chan struct{}
}

// The coordinator is a service that connects flow stages through pubsub.
type coordinator struct {
	ps     *pubsub.PubSub
	stages []stage.Stage
	conns  []Conn
	id     string
	ins    map[string]chan msg.MsgOnPort
	outs   map[string]chan msg.MsgOnPort
	ready  chan struct{}
}

func NewCoordinator(id string, ps *pubsub.PubSub, stages []stage.Stage, conns []Conn, ready chan struct{}) *coordinator {
	ins := map[string]chan msg.MsgOnPort{}
	outs := map[string]chan msg.MsgOnPort{}

	for _, s := range stages {
		in := make(chan msg.MsgOnPort, 100)
		out := make(chan msg.MsgOnPort, 100)
		s.Connect(in, out)
		ins[s.ID()] = in
		outs[s.ID()] = out
	}

	return &coordinator{
		id:     id,
		ps:     ps,
		stages: stages,
		conns:  conns,
		ins:    ins,
		outs:   outs,
		ready:  ready,
	}
} // we want to iterate through {input,output,stage}. but honestly would be easier if we can just access the in and out from each stage, i think.

func (c *coordinator) Serve(ctx context.Context) error {
	// Create subscriptions that forward stage inputs to channels
	for _, conn := range c.conns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.id, conn.Src.Stage, conn.Src.Port)
		defer pubsub.Sub(c.ps, subj, func(subj string, m msg.Msg) {
			in := c.ins[conn.Dst.Stage]
			in <- msg.MsgOnPort{Port: conn.Dst.Port, Msg: m}
		})()
	}

	// Signal that we are ready to receive outside messages to the "in" subjects.
	close(c.ready)

	// Create goroutines to publish stage outputs to the pubsub system
	var wg sync.WaitGroup
	for _, s := range c.stages {
		out := c.outs[s.ID()]
		wg.Go(func() {
			for m := range out {
				subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.id, s.ID(), m.Port)
				pubsub.Pub(c.ps, subj, m.Msg)
			}
		})
	}

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
func NewFlow(id string, ps *pubsub.PubSub, stages []stage.Stage, conns []Conn) *Flow {
	ready := make(chan struct{})

	c := NewCoordinator(id, ps, stages, conns, ready)

	sv := suture.NewSimple(id)
	sv.Add(c)

	for _, s := range stages {
		sv.Add(s)
	}

	return &Flow{Supervisor: sv}
}

func (f *Flow) Serve(ctx context.Context) error {
	err := f.Supervisor.Serve(ctx)
	// For flows we want to fail the whole flow permanently if it fails.
	if err != nil {
		return suture.ErrDoNotRestart
	} // ErrTerminateSupervisorTree
	return nil
}

// todo: do we need to defer close the in and out channels?
// Subscribe stage inputs to their connected subjects.
// Note that this approach will block all future output on any given message that takes a long time to publish.
