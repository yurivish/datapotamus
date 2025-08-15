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

type Stage interface {
	// an ID for the stage, which has to be unique within its flow.
	// Unlike messages and flows, human-readable IDs are often used for stages
	// since we display them in the UI as the identifiers for a stage.
	ID() string

	// Called once prior to Serve being called.
	// Serve may be called multple times due to suture restarts, so any
	// one-time initialization that needs to be done "once the service is running"
	// should be done here as this function is called right before the stage is added
	// to a supervisor.
	// Actually, as I write this, I'm realizing this is kind of an incoherent idea.
	// Just because you're added to the supervisor doesn't mean you're actually going to start.
	// So maybe we need a Deinit to dispose of things if Serve() never gets called?
	Init(cfg StageConfig)

	// Run the stage, returning an error in case of unexpected failure,
	// which will restart the stage with exponential backoff.
	// This implements the suture.Service interface.
	Serve(ctx context.Context) error
}

type TraceEvent interface {
	// Time() time.Time
}

type StageConfig struct {
	// Channel on which the stage will receive input messages
	In chan msg.MsgTo
	// Channel on which the stage will send output messages
	Out chan msg.MsgFrom
	// Channel on which the stage will send trace events
	Trace chan TraceEvent
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

func (s *StageBase) Init(cfg StageConfig) {
	s.StageConfig = cfg
}

// Send message `m` on port `port`.
func (s *StageBase) Send(m msg.Msg, port string) {
	s.Out <- m.From(msg.NewAddr(s.id, port))
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
	s.Trace <- TraceSend{time.Now(), parent.ID, child}
	s.Send(child, port)
}

// Records the given multi-parent merge edge on the "trace" port and returns its ID
func (s *StageBase) TraceMerge(parentIDs []msg.ID) msg.ID {
	if len(parentIDs) == 0 {
		panic("TraceMerge: merge must have at least one parent ID")
	}
	id := msg.ID(common.NewID())
	s.Trace <- TraceMerge{time.Now(), parentIDs, id}
	return id
}

func (s *StageBase) TraceRecv(id msg.ID) {
	s.Trace <- TraceRecv{time.Now(), id}
}

func (s *StageBase) TraceFailed(id msg.ID, err error) {
	s.Trace <- TraceFailed{time.Now(), id, err}
}

func (s *StageBase) TraceSucceeded(id msg.ID) {
	s.Trace <- TraceSucceeded{time.Now(), id}
}
