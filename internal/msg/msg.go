package msg

import (
	"datapotamus.com/internal/common"
)

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
	Tokens   []Token
	// If the parent ID of this message points to a merge group, this field
	// points to that group. This allows for provenance tracking based purely
	// on observing message flows, since we allow only one level of grouping,
	// and including the group in the message allows it to be observed externally.
	// Otherwise this field will be set to nil.
	Parent *MergeGroup
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

type Token struct{}

func NewAddr(stage, port string) Addr {
	return Addr{Stage: stage, Port: port}
}

func New(data any) Msg {
	return Msg{Data: data, ID: common.NewID()}
}

// AddToken? AddTokenGroup?

func (m Msg) Child(data any) Msg {
	return Msg{
		Data:     data,
		ID:       common.NewID(),
		ParentID: m.ID,
		Tokens:   m.Tokens,
	}
}

// A message together with the stage/port address it is arriving on.
func (m Msg) In(addr Addr) InMsg {
	return InMsg{Addr: addr, Msg: m}
}

// A message together with the stage/port address it is being emitted from.
func (m Msg) Out(addr Addr) OutMsg {
	return OutMsg{Addr: addr, Msg: m}
}
