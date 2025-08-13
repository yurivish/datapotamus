package stage

import (
	"iter"

	"datapotamus.com/internal/message"
)

type JQStage struct {
}

func (s *JQStage) Process() iter.Seq2[string, message.Message] {
	return func(yield func(port string, msg message.Message) bool) {
		yield("out", message.Message{Data: "hi"})
	}
}
