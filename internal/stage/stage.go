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
	Init(cfg Config)

	// Run the stage, returning an error in case of unexpected failure,
	// which will restart the stage with exponential backoff.
	// This implements the suture.Service interface.
	Serve(ctx context.Context) error
}

type Config struct {
	// Channel on which the stage will receive input messages
	In <-chan msg.InMsg
	// Channel on which the stage will send output messages
	Out chan<- msg.OutMsg
}

// Base stage implementation that implements a subset of the Stage interface
// and can be embedded to simplify the implementation of other stages
type Base struct {
	id string
}

func NewBase(id string) Base {
	// We return the struct rather than a pointer since this is quite small
	// and very immutable and I think it makes more sense to embed the struct directly.
	return Base{id: id}
}

func (s *Base) ID() string {
	return s.id
}
