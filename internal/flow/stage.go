package flow

import (
	"context"
	"time"

	"datapotamus.com/internal/common"
	"datapotamus.com/internal/flow/msg"
)

// todo: for validation:
// clojure async flow: No key may be present in both :ins and :outs, allowing for a uniform
// channel coordinate system of [:process-id :channel-id]. (For us, :stage-id :port-id)

type TraceEvent any // for now; later maybe Time() time.Time

type Stage interface {
	// Stage ID, which must be unique within its flow.
	// Unlike messages and flows, stage IDs are usually human-readable
	// and are displayed in the UI as stage names.
	ID() string

	// Returns a non-nil channel for input messages to this stage.
	// Closing the in channel signifies to the stage that input is done.
	In() chan<- msg.MsgTo

	// Returns a non-nil channel for output messages from this stage.
	// The stage closes the channel when it is done processing all input messages.
	Out() <-chan msg.MsgFrom

	// Returns a possibly-nil channel for trace messages from this stage
	Trace() <-chan TraceEvent

	// Runs the stage, returning an error in case of unexpected failure.
	Serve(ctx context.Context) error
}

type ConfigBase struct {
}

func DefaultStageChans() StageChans {
	return StageChans{
		In:    make(chan msg.MsgTo, 100),
		Out:   make(chan msg.MsgFrom, 100),
		Trace: make(chan TraceEvent, 100)}
}

type StageChans struct {
	// Channel on which the stage will receive input messages
	In chan msg.MsgTo
	// Channel on which the stage will send output messages
	Out chan msg.MsgFrom
	// Channel on which the stage will send TraceEvents, may be nil.
	Trace chan TraceEvent
}

// StageBase implements a subset of the Stage interface and is designed
// for embedding, simplifying the implementation of other stages
type StageBase struct {
	// ID of the stage
	id string

	// This is a public field intended for use by the embedding stage but
	// not outside of it. Consumers of the stage should access stage chans
	// only through the Stage interface.
	Ch StageChans
}

func NewStageBase(id string, chans StageChans) StageBase {
	// We return the struct rather than a pointer since this is quite small
	// and very immutable and I think it makes more sense to embed the struct directly.
	return StageBase{id, chans}
}

func (s *StageBase) ID() string {
	return s.id
}

func (s *StageBase) In() chan<- msg.MsgTo {
	return s.Ch.In
}

func (s *StageBase) Out() <-chan msg.MsgFrom {
	return s.Ch.Out
}

func (s *StageBase) Trace() <-chan TraceEvent {
	return s.Ch.Trace
}

// Send message `m` on port `port`.
func (s *StageBase) Send(m msg.Msg, port string) {
	s.Ch.Out <- m.From(msg.NewAddr(s.id, port))
}

// Event types emitted onto stage "trace" ports
type (
	// recorded right before the message was sent by the stage
	TraceSend struct {
		Time     time.Time
		ParentID msg.ID
		Message  msg.Msg
	}

	// created a new merge node independent of any messages
	TraceMerge struct {
		Time      time.Time
		ParentIDs []msg.ID
		ID        msg.ID
	}

	// recorded right after the message was received by the stage
	TraceRecv struct {
		Time time.Time
		ID   msg.ID
	}

	// messages successfully processed
	TraceSucceeded struct {
		Time time.Time
		ID   msg.ID
	}

	// message processing failed. may include retry/snooze params here later or maybe those should go elsewhere.
	TraceFailed struct {
		Time  time.Time
		ID    msg.ID
		Error error
	}
)

// Create a child message with the provided parent and data, and send it on the given port.
// I think passing the zero message as the parent will do the right thing and create a root.
func (s *StageBase) TraceSend(parent msg.Msg, data any, port string) {
	child := parent.Child(data)
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceSend{time.Now(), parent.ID, child}
	}
	s.Send(child, port)
}

// Records the given multi-parent merge edge on the "trace" port and returns its ID
func (s *StageBase) TraceMerge(parentIDs []msg.ID) msg.ID {
	if len(parentIDs) == 0 {
		panic("TraceMerge: merge must have at least one parent ID")
	}
	id := msg.ID(common.NewID())
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceMerge{time.Now(), parentIDs, id}
	}
	return id
}

func (s *StageBase) TraceRecv(id msg.ID) {
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceRecv{time.Now(), id}
	}
}

func (s *StageBase) TraceFailed(id msg.ID, err error) {
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceFailed{time.Now(), id, err}
	}
}

func (s *StageBase) TraceSucceeded(id msg.ID) {
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceSucceeded{time.Now(), id}
	}
}
