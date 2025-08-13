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
	coord *coordinator
}

// The coordinator service connects flow stages to each other through
// the pubsub mechanism provided by our internal pubsub package.
// The basic idea is that each stage has an in channel and an out channel,
// and we subscribe to out channels in order to publish those messages
// on the pubsub message bus, and subscribe based on connections to the
// appropriate subject in order to ensure those outputs are routed to the
// appropriate inputs.
type coordinator struct {
	ps     *pubsub.PubSub
	stages []stage.Stage
	conns  []Conn
	id     string
	ins    map[string]chan msg.PortMessage
	outs   map[string]chan msg.PortMessage
	ready  chan struct{}
}

func (c *coordinator) Serve(ctx context.Context) error {
	// Create subscriptions that forward stage inputs to channels
	for _, conn := range c.conns {
		subj := fmt.Sprintf("flow.%s.stage.%s.port.%s", c.id, conn.Src.Stage, conn.Src.Port)
		fmt.Println("flow: subscribin", subj)
		defer pubsub.Sub(c.ps, subj, func(subj string, m msg.Message) {
			in := c.ins[conn.Dst.Stage]
			fmt.Println("flow: sub got", subj, m, in)
			in <- msg.PortMessage{Port: conn.Dst.Port, Message: m}
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
				pubsub.Pub(c.ps, subj, m.Message)
			}
		})
	}

	<-ctx.Done()
	fmt.Println("flow: context is dun")

	// close the in and out channels, then wait until the remaining messages are processed by the stage out goroutines
	for _, ch := range c.ins {
		close(ch)
	}

	for _, ch := range c.outs {
		close(ch)
	}

	wg.Wait()

	return nil
}

// I think the idea is the caller, who has the super supervisor, starts this when it is added.
func NewFlow(id string, ps *pubsub.PubSub, stages []stage.Stage, conns []Conn) *Flow {
	sv := suture.NewSimple(id)

	// Create in and out channels for each stage and store them so that we can link stages
	// together using the pubsub system. Since that requires context-based resource management
	// of goroutines and pubsub subscriptions, we do it the Serve method rather than here.
	ins := map[string]chan msg.PortMessage{}
	outs := map[string]chan msg.PortMessage{}
	for _, s := range stages {
		in := make(chan msg.PortMessage, 100)
		out := make(chan msg.PortMessage, 100)
		s.Connect(in, out)

		// Store the input and output channels by stage ID so that we can connect them to the pubsub system later
		ins[s.ID()] = in
		outs[s.ID()] = out

		sv.Add(s)
	}

	coord := &coordinator{
		id:     id,
		ps:     ps,
		stages: stages,
		conns:  conns,
		ins:    ins,
		outs:   outs,
		ready:  make(chan struct{}),
	}

	sv.Add(coord)

	return &Flow{
		Supervisor: sv,
		coord:      coord,
	}
}
func (f *Flow) Ready() chan struct{} {
	return f.coord.ready
}

// todo: do we need to defer close the in and out channels?
// Subscribe stage inputs to their connected subjects.
// Note that this approach will block all future output on any given message that takes a long time to publish.
