package flow

import (
	"context"
	"time"

	"datapotamus.com/internal/common"
	"datapotamus.com/internal/flow/msg"
)

// todo: stage instances should specify their ports and should allow validation.
// Validate that no port is present in both inputs and outputs for a stage, allowing for a
// uniform port coordinate system of Addr{stage, port} inspired by Clojure's async.flow.

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

// Contains the channels used by a stage
type Chans struct {
	// Channel on which the stage will receive input messages
	In chan msg.MsgTo
	// Channel on which the stage will send output messages
	Out chan msg.MsgFrom
	// Channel on which the stage will send TraceEvents. May be nil.
	Trace chan TraceEvent
}

// Base implements a subset of the Stage interface and is designed
// for embedding, simplifying the implementation of other stages
type Base struct {
	// ID of the stage
	id string

	// This is a public field intended for use by the embedding stage but
	// not outside of it. Consumers of the stage should access stage chans
	// only through the Stage interface.
	Ch Chans
}

// Construct a new Base with nil channels. Channels can
// be configured by the base.With[In|Out|Trace] methods.
func NewBase(id string) *Base {
	return &Base{id, Chans{}}
}

// Fluent construction methods
func (s *Base) WithIn(sz int) *Base {
	s.Ch.In = make(chan msg.MsgTo, sz)
	return s
}

func (s *Base) WithOut(sz int) *Base {
	s.Ch.Out = make(chan msg.MsgFrom, sz)
	return s
}

func (s *Base) WithInOut(sz int) *Base {
	return s.WithIn(sz).WithOut(sz)
}

func (s *Base) WithTrace(sz int) *Base {
	s.Ch.Trace = make(chan TraceEvent, sz)
	return s
}

// Partly implement the Stage interface
func (s *Base) ID() string {
	return s.id
}

func (s *Base) In() chan<- msg.MsgTo {
	return s.Ch.In
}

func (s *Base) Out() <-chan msg.MsgFrom {
	return s.Ch.Out
}

func (s *Base) Trace() <-chan TraceEvent {
	return s.Ch.Trace
}

// Send message `m` on port `port`.
func (s *Base) Send(m msg.Msg, port string) {
	s.Ch.Out <- m.From(msg.NewAddr(s.id, port))
}

// func (s *StageBase) WithChans(chans StageChans) *StageBase {
// 	s.Ch = chans
// 	return s
// }

// Event types emitted onto stage "trace" ports
type (
	// recorded right before the message was sent by the stage
	TraceSend struct {
		Time     time.Time
		ParentID msg.ID
		Message  msg.Msg
	}

	// recorded right after the message was received by the stage
	TraceRecv struct {
		Time time.Time
		ID   msg.ID
	}

	// message processing failed. not sure how we should count things like snooze/retry etc.
	TraceFailure struct {
		Time  time.Time
		ID    msg.ID
		Error error
	}

	// messages successfully processed
	TraceSuccess struct {
		Time time.Time
		ID   msg.ID
	}

	// created a new merge node independent of any messages
	TraceMerge struct {
		Time      time.Time
		ParentIDs []msg.ID
		ID        msg.ID
	}
)

// Create a child message with the provided parent and data, and send it on the given port.
// I think passing the zero message as the parent will do the right thing and create a root.
func (s *Base) TraceSend(parent msg.Msg, data any, port string) {
	child := parent.Child(data)
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceSend{time.Now(), parent.ID, child}
	}
	s.Send(child, port)
}

func (s *Base) TraceRecv(id msg.ID) {
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceRecv{time.Now(), id}
	}
}

func (s *Base) TraceFailure(id msg.ID, err error) {
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceFailure{time.Now(), id, err}
	}
}

func (s *Base) TraceSuccess(id msg.ID) {
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceSuccess{time.Now(), id}
	}
}

// Records the given multi-parent merge edge on the "trace" port and returns its ID
func (s *Base) TraceMerge(parentIDs []msg.ID) msg.ID {
	if len(parentIDs) == 0 {
		panic("TraceMerge: merge must have at least one parent ID")
	}
	id := msg.ID(common.NewID())
	if s.Ch.Trace != nil {
		s.Ch.Trace <- TraceMerge{time.Now(), parentIDs, id}
	}
	return id
}
