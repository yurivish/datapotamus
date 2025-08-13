package stage

import (
	"datapotamus.com/internal/message"
)

type JQStage struct {
}

func (s *JQStage) Step(port string, m message.Msg, yield func(string, message.Msg)) {
	yield("out", message.Msg{Data: "hi"})
}
