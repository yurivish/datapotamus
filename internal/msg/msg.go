package msg

import (
	"datapotamus.com/internal/common"
	"datapotamus.com/internal/token"
)

// todo: do we need this or can we just represent merges in the trace dag?
// or maybe this should be a special message type or something...
type MergeGroup struct {
	ID        string
	ParentIDs []string
}

func Merge(ms []Msg) MergeGroup {
	var ids []string
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	return MergeGroup{ID: common.NewID(), ParentIDs: ids}
}

type Msg struct {
	Data     any
	ID       string
	ParentID string
	Tokens   token.Tokens
}

type Addr struct {
	Stage string
	Port  string
}

// A message together with the stage/port address it is arriving on.
// Used so that the stage's in channel knows the port of arrival.
type InMsg struct {
	Msg
	Addr
}

// A message together with the stage/port address it is being emitted from.
// Used so that the stage's out channel can specify the port of departure.
type OutMsg struct {
	Msg
	Addr
}

func NewAddr(stage, port string) Addr {
	return Addr{Stage: stage, Port: port}
}

func New(data any) Msg {
	return Msg{Data: data, ID: common.NewID()}
}

// Returns a new message that is a child of the parent message.
func (m Msg) Child(data any) Msg {
	return Msg{
		Data:     data,
		ID:       common.NewID(),
		ParentID: m.ID,
		Tokens:   m.Tokens,
	}
}

// Returns the same message with the given Tokens merged in.
// note: value receiver; we may want to change this to pointer...
func (m Msg) MergeTokens(tokens token.Tokens) Msg {
	m.Tokens = m.Tokens.Merge(tokens)
	return m
}

// A message together with the stage/port address it is arriving on.
func (m Msg) In(addr Addr) InMsg {
	return InMsg{Addr: addr, Msg: m}
}

// A message together with the stage/port address it is being emitted from.
func (m Msg) Out(addr Addr) OutMsg {
	return OutMsg{Addr: addr, Msg: m}
}
