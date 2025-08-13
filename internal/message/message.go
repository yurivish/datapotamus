package message

type Msg struct {
	Data     any
	ID       string
	ParentID string
	Tokens   []Token
}

type Token struct{}
