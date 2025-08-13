package stage

import (
	"context"

	"datapotamus.com/internal/msg"
)

// clojure async flow: No key may be present in both :ins and :outs, allowing for a uniform
// channel coordinate system of [:process-id :channel-id]. (For us, :stage-id :port-id)

type Stage interface {
	ID() string // an ID / name for the stage, which has to be unique within its flow

	// Called once prior to Serve being called.
	Connect(in <-chan msg.MsgOnPort, out chan<- msg.MsgOnPort)

	// Run the stage, returning an error in case of unexpected failure,
	// which will restart the stage with exponential backoff.
	// This implements the suture.Service interface.
	Serve(ctx context.Context) error
}
