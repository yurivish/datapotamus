package msg

import (
	"datapotamus.com/internal/common"
)

type Msg struct {
	Data     any
	ID       string
	ParentID string
	Tokens   []Token
}

type Addr struct {
	Stage string
	Port  string
}

// A message together with the stage/port address it is being emitted from.
// Used so that the stage's out channel can specify the port of departure.
type OutMsg struct {
	Msg
	Addr
}

// A message together with the stage/port address it is arriving on.
// Used so that the stage's in channel knows the port of arrival.
type InMsg struct {
	Msg
	Addr
}

type Token struct{}

func NewAddr(stage, port string) Addr {
	return Addr{Stage: stage, Port: port}
}

// maybe functional options for parentid/tokens and just call this New
func New(data any) Msg {
	return Msg{Data: data, ID: common.NewID()}
}

func (m Msg) Child(data any) Msg {
	return Msg{
		Data:     data,
		ID:       common.NewID(),
		ParentID: m.ID,
		Tokens:   m.Tokens,
	}
}

func (m Msg) In(addr Addr) InMsg {
	return InMsg{Addr: addr, Msg: m}
}

func (m Msg) Out(addr Addr) OutMsg {
	return OutMsg{Addr: addr, Msg: m}
}
