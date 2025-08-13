package message

type Msg struct {
	Data     any
	ID       string
	ParentID string
	Tokens   []Token
}

type PortMsg struct {
	Msg
	Port string
}

type Token struct{}
