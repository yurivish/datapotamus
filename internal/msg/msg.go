package msg

type Message struct {
	Data     any
	ID       string
	ParentID string
	Tokens   []Token
}

type PortMessage struct {
	Message
	Port string
}

type Token struct{}
