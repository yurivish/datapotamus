package flow

import (
	"context"
	"time"

	"datapotamus.com/internal/common"
	"datapotamus.com/internal/core/msg"
)

// todo: for validation:
// clojure async flow: No key may be present in both :ins and :outs, allowing for a uniform
// channel coordinate system of [:process-id :channel-id]. (For us, :stage-id :port-id)

type TraceEvent any // for now; later maybe Time() time.Time

type Stage interface {
	// Stage ID, which must be unique within its flow.
	// Unlike messages and flows, human-readable IDs are often used for stages
	// since we display them in the UI as the identifiers for a stage.
	ID() string

	// Called at most once, prior to Serve being called zero or more times.
	// We need this since the stage constructor does not know how to connect the stage
	// since that information is available only in the execution context, eg. inside a flow.
	// Should not create any resources that require Serve to run in order to be cleaned up.
	Connect(cfg StageConfig)

	// After Connect is called, returns a non-nil channel for input messages to this stage
	In() chan msg.MsgTo

	// After Connect is called, returns a non-nil channel for output messages from this stage
	Out() chan msg.MsgFrom

	// After Connect is called, returns a possibly-nil channel for trace messages from this stage
	Trace() chan TraceEvent

	// Run the stage, returning an error in case of unexpected failure.
	Serve(ctx context.Context) error
}

type StageConfig struct {
	// Channel on which the stage will receive input messages
	in chan msg.MsgTo
	// Channel on which the stage will send output messages
	out chan msg.MsgFrom
	// Channel on which the stage will send trace events
	trace chan TraceEvent
}

func NewStageConfig(
	in chan msg.MsgTo,
	out chan msg.MsgFrom,
	trace chan TraceEvent,
) StageConfig {
	return StageConfig{in, out, trace}
}

// StageBase stage implementation that implements a subset of the Stage interface
// and can be embedded to simplify the implementation of other stages
type StageBase struct {
	id string
	StageConfig
}

func NewStageBase(id string) StageBase {
	// We return the struct rather than a pointer since this is quite small
	// and very immutable and I think it makes more sense to embed the struct directly.
	return StageBase{id: id}
}

func (s *StageBase) ID() string {
	return s.id
}

func (s *StageBase) Connect(cfg StageConfig) {
	s.StageConfig = cfg
}

func (s *StageBase) In() chan msg.MsgTo {
	return s.StageConfig.in
}

func (s *StageBase) Out() chan msg.MsgFrom {
	return s.StageConfig.out
}

func (s *StageBase) Trace() chan TraceEvent {
	return s.StageConfig.trace
}

// Send message `m` on port `port`.
func (s *StageBase) Send(m msg.Msg, port string) {
	s.out <- m.From(msg.NewAddr(s.id, port))
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
	if s.trace != nil {
		s.trace <- TraceSend{time.Now(), parent.ID, child}
	}
	s.Send(child, port)
}

// Records the given multi-parent merge edge on the "trace" port and returns its ID
func (s *StageBase) TraceMerge(parentIDs []msg.ID) msg.ID {
	if len(parentIDs) == 0 {
		panic("TraceMerge: merge must have at least one parent ID")
	}
	id := msg.ID(common.NewID())
	if s.trace != nil {
		s.trace <- TraceMerge{time.Now(), parentIDs, id}
	}
	return id
}

func (s *StageBase) TraceRecv(id msg.ID) {
	if s.trace != nil {
		s.trace <- TraceRecv{time.Now(), id}
	}
}

func (s *StageBase) TraceFailed(id msg.ID, err error) {
	if s.trace != nil {
		s.trace <- TraceFailed{time.Now(), id, err}
	}
}

func (s *StageBase) TraceSucceeded(id msg.ID) {
	if s.trace != nil {
		s.trace <- TraceSucceeded{time.Now(), id}
	}
}
