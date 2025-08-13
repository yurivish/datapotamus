package stage

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/message"
	"github.com/itchyny/gojq"
)

type JQStage struct {
	code *gojq.Code
}

type JQStageArgs struct {
	Filter string `json:"filter"`
}

func NewJQStage(cfg JQStageArgs) (*JQStage, error) {
	query, err := gojq.Parse(cfg.Filter)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JQ query: %w", err)
	}
	code, err := gojq.Compile(query)
	if err != nil {
		// todo: Use gojq.ParseError to get the error position and token of the parsing error.
		return nil, fmt.Errorf("failed to parse JQ query: %w", err)
	}
	return &JQStage{code: code}, nil
}

func (s *JQStage) Step(ctx context.Context, m message.PortMsg, ch chan<- message.PortMsg) {
	// Run the JQ query with the input data, eagerly materializing the results to stay within the timeout
	// note from README:
	// > You can reuse the *Code against multiple inputs to avoid compilation of the same query.
	// > But for arguments of code.Run, do not give values sharing same data between multiple calls.
	// Agh, this feels so so so hacky and I just want to try using Rust after all..........!
	qctx, _ := context.WithTimeout(ctx, 250*time.Millisecond) // todo: configurable timeout
	it := s.code.RunWithContext(qctx, m.Data)
	var xs []any
	// Loop structure inspired by the README example:
	// https://github.com/itchyny/gojq?tab=readme-ov-file#usage-as-a-library
	for {
		v, ok := it.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			if err, ok := err.(*gojq.HaltError); ok && err.Value() == nil {
				break
			}
			// note: the error is emitted prior to successful outputs.
			// we stop at the first error.
			ch <- message.PortMsg{Port: "error", Msg: message.Msg{Data: err}}
		}
		xs = append(xs, v)
	}

	for _, x := range xs {
		ch <- message.PortMsg{Port: "out", Msg: message.Msg{Data: x}}
	}
}
