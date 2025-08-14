package msg

import "github.com/oklog/ulid/v2"

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

// maybe functional options for parentid/tokens and just call this NewMsg
func RootMsg(data any) Msg {
	id := ulid.Make().String()
	return Msg{Data: data, ID: id}
}

func (m Msg) In(addr Addr) InMsg {
	return InMsg{Addr: addr, Msg: m}
}

func (m Msg) Out(addr Addr) OutMsg {
	return OutMsg{Addr: addr, Msg: m}
}
