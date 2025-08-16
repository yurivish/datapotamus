package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"datapotamus.com/internal/flow"
	"datapotamus.com/internal/flow/msg"
	"datapotamus.com/internal/flow/pubsub"
	"datapotamus.com/internal/stage"
	"github.com/thejerf/suture/v4"
)

func main() {
	fmt.Println("Hi")

	super := suture.NewSimple("app")
	ps := pubsub.NewPubSub()
	s1, err := stage.JQFromConfig("s1", stage.JQConfig{Filter: ".[]", TimeoutMillis: 250})
	if err != nil {
		log.Fatal(err)
	}
	s2, err := stage.DelayFromConfig("s2", stage.DelayConfig{Millis: 1000})

	s3, err := stage.JQFromConfig("s3", stage.JQConfig{Filter: "[.]", TimeoutMillis: 250})
	if err != nil {
		log.Fatal(err)
	}

	f, err := flow.NewFlow(
		"flow1",
		ps,
		[]flow.Stage{s1, s2, s3},
		[]flow.Conn{
			{From: msg.NewAddr("s1", "out"), To: msg.NewAddr("s2", "in")},
			{From: msg.NewAddr("s2", "out"), To: msg.NewAddr("s3", "in")},
		},
		[]flow.Conn{flow.SelfConn(msg.NewAddr("s3", "out"))})
	if err != nil {
		log.Fatal(fmt.Errorf("failed to construct flow: %w", err))
	}
	f.Connect(flow.NewStageConfig(
		make(chan msg.MsgTo, 100),
		make(chan msg.MsgFrom, 100),
		make(chan flow.TraceEvent, 100),
	))
	super.Add(f)
	ctx := context.Background()
	super.ServeBackground(ctx) // returns err in a channel

	f.In() <- msg.Msg{Data: []any{1, 2}}.To(msg.NewAddr("s1", "in"))

	// note: I think we actually need to set up a bunch of the stuff in the constructor and therefore need to have a separate cleanup function just in case the flow never gets added to the supervisor.
	// Because otherwise, just because it's serving doesn't mean that it's actually done the subscription work yet.
	// pubsub.Pub(ps, "flow.flow1.stage.outside.port.input", msg.Msg{Data: []any{1}})
	fmt.Println("sent a mess and now we wait", err)

loop:
	for {
		select {
		case m, ok := <-f.Out():
			if !ok {
				break loop
			}
			fmt.Println("out:", m)

		case e, ok := <-f.Trace():
			if !ok {
				break loop
			}
			fmt.Println("trace:", e)

		case <-time.After(3 * time.Second):
			fmt.Println("done w loop due to timeout")
			break loop

		case <-ctx.Done():
			fmt.Println("done w loop due to ctx done")
			break loop
		}
	}

	fmt.Println("woopydoo")
}
