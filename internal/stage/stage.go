package stage

import (
	"context"

	"datapotamus.com/internal/common"
	"datapotamus.com/internal/msg"
)

// todo: for validation:
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
	In chan msg.InMsg
	// Channel on which the stage will send output messages
	Out chan msg.OutMsg
}

// Base stage implementation that implements a subset of the Stage interface
// and can be embedded to simplify the implementation of other stages
type Base struct {
	id  string
	In  chan msg.InMsg
	Out chan msg.OutMsg
}

func NewBase(id string) Base {
	// We return the struct rather than a pointer since this is quite small
	// and very immutable and I think it makes more sense to embed the struct directly.
	return Base{id: id}
}

func (s *Base) ID() string {
	return s.id
}

func (s *Base) Init(cfg Config) {
	s.In = cfg.In
	s.Out = cfg.Out
}

// Send message `m` on port `port`.
func (s *Base) Send(m msg.Msg, port string) {
	s.Out <- m.Out(msg.NewAddr(s.id, port))
}

// Event types emitted onto stage "trace" ports
type (
	TraceEdge struct {
		ParentID msg.ID
		ID       msg.ID
	}
	TraceMerge struct {
		ParentIDs []msg.ID
		ID        msg.ID
	}

	// message was received by the stage
	TraceReceived struct {
		ID msg.ID
	}

	// messages successfully processed
	TraceSucceeded struct {
		ID msg.ID
	}

	// message processing failed. may include retry/snooze params here later.
	TraceFailed struct {
		ID    msg.ID
		Error error
	}
)

// Send a message with the given data to the trace port.
func (s *Base) Trace(data any) {
	s.Send(msg.New(data), "trace")
}

// Create a child message with the provided parent and data, and send it on the given port.
func (s *Base) TraceSend(parent msg.Msg, data any, port string) {
	child := parent.Child(data)
	s.TraceEdge(parent.ID, child.ID)
	s.Send(child, port)
}

// Records the given parent-child edge on the trace port.
func (s *Base) TraceEdge(parentID, id msg.ID) {
	s.Trace(TraceEdge{parentID, id})
}

// Records the given multi-parent merge edge on the trace port.
func (s *Base) TraceMerge(parentIDs []msg.ID) msg.ID {
	if len(parentIDs) == 0 {
		panic("TraceMerge: merge must have at least one parent ID")
	}
	id := msg.ID(common.NewID())
	s.Trace(TraceMerge{parentIDs, id})
	return id
}

func (s *Base) TraceReceived(id msg.ID) {
	s.Trace(TraceReceived{id})
}

func (s *Base) TraceFailed(id msg.ID, err error) {
	s.Trace(TraceFailed{ID: id, Error: err})
}

func (s *Base) TraceSucceeded(id msg.ID) {
	s.Trace(TraceSucceeded{id})
}
