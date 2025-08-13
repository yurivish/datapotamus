package message

type Message struct {
	Data     any
	ID       string
	ParentID string
	Tokens   []Token
}

type Token struct{}
