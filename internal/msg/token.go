package msg

import "math/rand/v2"

type Token struct {
	// todo: should be u128 to reduce chances of collision (i think)
	value uint64
}

func ZeroToken() Token {
	return Token{0}
}

func NewToken() Token {
	return Token{rand.Uint64()}
}

func (t Token) Merge(other Token) Token {
	return Token{t.value ^ other.value}
}

// todo: test that splitting a token and merging repeatedly gives the original value back
func (t Token) Split(count int) []Token {
	if count <= 0 {
		panic("must split into at least one token")
	}
	var tokens []Token
	xor := t.value
	for range count - 1 {
		token := NewToken()
		xor ^= token.value
		tokens = append(tokens, token)
	}
	tokens = append(tokens, Token{xor})
	return tokens
}

func (t Token) IsZero() bool {
	return t.value == 0
}

func (t Token) Equal(other Token) bool {
	return t.value == other.value
}
