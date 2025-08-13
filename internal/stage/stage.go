package stage

import (
	"iter"

	"datapotamus.com/internal/message"
)

type Stage interface {
	// Process an incoming message on a given port, and emit zero or more messages to output ports.
	// The key specifies the port and the value specifies the message.
	Step(port string, m message.Message) iter.Seq2[string, message.Message]
}
