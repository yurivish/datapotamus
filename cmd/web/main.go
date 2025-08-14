package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"datapotamus.com/internal/flow"
	"datapotamus.com/internal/msg"
	"datapotamus.com/internal/pubsub"
	"datapotamus.com/internal/stage"
	"github.com/thejerf/suture/v4"
)

func main() {
	fmt.Println("Hi")
	super := suture.NewSimple("app")
	ps := pubsub.NewPubSub()
	s1, err := stage.NewJQStage("s1", stage.JQStageArgs{Filter: ".[]"})
	if err != nil {
		log.Fatal(err)
	}
	s2, err := stage.NewJQStage("s2", stage.JQStageArgs{Filter: "[.]"})
	if err != nil {
		log.Fatal(err)
	}
	f := flow.NewFlow("flow1", ps, []stage.Stage{s1, s2}, []flow.Conn{
		{From: msg.NewAddr("s1", "out"), To: msg.NewAddr("s2", "in")},
		{From: msg.NewAddr("outside", "input"), To: msg.NewAddr("s1", "in")},
	})
	super.Add(f)
	ctx := context.Background()
	super.ServeBackground(ctx) // returns err

	<-f.Ready

	// note: I think we actually need to set up a bunch of the stuff in the constructor and therefore need to have a separate cleanup function just in case the flow never gets added to the supervisor.
	// Because otherwise, just because it's serving doesn't mean that it's actually done the subscription work yet.
	pubsub.Pub(ps, "flow.flow1.stage.outside.port.input", msg.Msg{Data: []any{1, 2, 3}})
	fmt.Println("sent a mess and now we wait", err)
	// go func() {	super.Serve(context.Background()) }
	// <-ctx.Done()
	//
	time.Sleep(time.Second)
}
