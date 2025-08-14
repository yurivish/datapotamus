package common

import "github.com/oklog/ulid/v2"

func NewID() string {
	return ulid.Make().String()
}

func Assert(cond bool, msg any) {
	if !cond {
		panic(msg)
	}
}
