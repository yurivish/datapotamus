package msg

type Msg struct {
	Data     any
	ID       string
	ParentID string
	Tokens   []Token
}

type MsgOnPort struct {
	Msg
	Port string
}

type Token struct{}
