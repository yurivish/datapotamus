package msg

import (
	"datapotamus.com/internal/common"
	"datapotamus.com/internal/core/token"
)

type ID string

// Message sent between processing stages.
// Must not be mutated once sent out from a stage,
// including the data -- the messages may be stored
// by the tracing system, which assumes immutability.
type Msg struct {
	ID     ID
	Data   any
	Tokens token.Tokens
}

type Addr struct {
	Stage string
	Port  string
}

// A message together with the stage/port address it is arriving to,
// so that the stage's in channel knows the port of arrival.
type MsgTo struct {
	Msg
	Addr
}

// A message together with the stage/port address it is being sent from,
// so that the stage's out channel can specify the port of departure.
type MsgFrom struct {
	Msg
	Addr
}

func NewAddr(stage, port string) Addr {
	return Addr{Stage: stage, Port: port}
}

func New(data any) Msg {
	return Msg{Data: data, ID: ID(common.NewID())}
}

func NewWithID(id ID, data any) Msg {
	return Msg{Data: data, ID: id}
}

// Returns a new message that is a child of the parent message.
func (m Msg) Child(data any) Msg {
	return Msg{
		Data:   data,
		ID:     ID(common.NewID()),
		Tokens: m.Tokens,
	}
}

// Returns the same message with the given Tokens merged in.
// note: value receiver; we may want to change this to pointer...
func (m Msg) MergeTokens(tokens token.Tokens) Msg {
	m.Tokens = m.Tokens.Merge(tokens)
	return m
}

// A message together with the stage/port address it is arriving on.
func (m Msg) To(addr Addr) MsgTo {
	return MsgTo{Addr: addr, Msg: m}
}

// A message together with the stage/port address it is being emitted from.
func (m Msg) From(addr Addr) MsgFrom {
	return MsgFrom{Addr: addr, Msg: m}
}
