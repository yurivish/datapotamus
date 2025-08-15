package stage

import (
	"context"
	"time"

	"datapotamus.com/internal/common"
	"datapotamus.com/internal/msg"
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
	Init(cfg Config)

	// Run the stage, returning an error in case of unexpected failure,
	// which will restart the stage with exponential backoff.
	// This implements the suture.Service interface.
	Serve(ctx context.Context) error
}

type Config struct {
	// Channel on which the stage will receive input messages
	In chan msg.MsgTo
	// Channel on which the stage will send output messages
	Out chan msg.MsgFrom
}

// Base stage implementation that implements a subset of the Stage interface
// and can be embedded to simplify the implementation of other stages
type Base struct {
	id  string
	In  chan msg.MsgTo
	Out chan msg.MsgFrom
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
	s.Out <- m.From(msg.NewAddr(s.id, port))
}

// Event types emitted onto stage "trace" ports
type (
	// a message is sent to a stage output channel
	// recorded right before the call to s.Send
	TraceSend struct {
		Time     time.Time
		ParentID msg.ID
		ID       msg.ID
	}

	// created a new merge node independent of any messages
	TraceMerge struct {
		Time      time.Time
		ParentIDs []msg.ID
		ID        msg.ID
	}

	// todo: TraceRecv to parallel send?
	// message was received by the stage
	TraceReceived struct {
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

// Send a message with the given data to the "trace" port
func (s *Base) Trace(data any) {
	s.Send(msg.New(data), "trace")
}

// Create a child message with the provided parent and data, and send it on the given port.
// I think passing the zero message as the parent will do the right thing and create a root.
func (s *Base) TraceSend(parent msg.Msg, data any, port string) {
	child := parent.Child(data)
	s.TraceEdge(parent.ID, child.ID)
	s.Send(child, port)
}

// Records the given parent-child edge on the "trace" port
func (s *Base) TraceEdge(parentID, id msg.ID) {
	s.Trace(TraceSend{time.Now(), parentID, id})
}

// Records the given multi-parent merge edge on the "trace" port and returns its ID
func (s *Base) TraceMerge(parentIDs []msg.ID) msg.ID {
	if len(parentIDs) == 0 {
		panic("TraceMerge: merge must have at least one parent ID")
	}
	id := msg.ID(common.NewID())
	s.Trace(TraceMerge{time.Now(), parentIDs, id})
	return id
}

func (s *Base) TraceReceived(id msg.ID) {
	s.Trace(TraceReceived{time.Now(), id})
}

func (s *Base) TraceFailed(id msg.ID, err error) {
	s.Trace(TraceFailed{time.Now(), id, err})
}

func (s *Base) TraceSucceeded(id msg.ID) {
	s.Trace(TraceSucceeded{time.Now(), id})
}
