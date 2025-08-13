package stage

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/msg"
	"github.com/itchyny/gojq"
	"github.com/thejerf/suture/v4"
)

type JQStage struct {
	id      string
	in      <-chan msg.PortMessage
	out     chan<- msg.PortMessage
	code    *gojq.Code
	timeout time.Duration
}

type JQStageArgs struct {
	Filter string `json:"filter"`
}

func NewJQStage(id string, cfg JQStageArgs) (*JQStage, error) {
	query, err := gojq.Parse(cfg.Filter)
	if err != nil {
		// todo: Use gojq.ParseError to get the error position and token of the parsing error
		return nil, fmt.Errorf("failed to parse JQ query: %w", err)
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JQ query: %w", err)
	}
	return &JQStage{id: id, code: code, timeout: 250 * time.Millisecond}, nil
}

func (s *JQStage) ID() string {
	return s.id
}

func (s *JQStage) Connect(in <-chan msg.PortMessage, out chan<- msg.PortMessage) {
	fmt.Println("jq: connecting")
	s.in = in
	s.out = out
}

func (s *JQStage) Serve(ctx context.Context) error {
	fmt.Println("jq:", s.id, "serving")
	for {
		select {
		case m, ok := <-s.in:
			fmt.Printf("jq: %v: received %v %v\n", s.id, m, ok)
			// if the input channel is closed then exit gracefully
			if !ok {
				return suture.ErrDoNotRestart
			}
			results, err := s.Query(ctx, m.Data)
			fmt.Println("jq results", results, err)
			// send the error, if any
			if err != nil {
				s.out <- msg.PortMessage{Port: "error", Message: msg.Message{Data: err}}
			} else {
				// otherwise send the results, if any.
				// todo: we could send partial results even in the face of an error, or
				// even multiple errors, if we decide that is the behavior we want.
				for _, result := range results {
					s.out <- msg.PortMessage{Port: "out", Message: msg.Message{Data: result}}
				}
			}
		case <-ctx.Done():
			fmt.Printf("jq: %v: ctx done", s.id)
			return ctx.Err()
		}
	}

}

// Run the JQ query with the input data, eagerly materializing the results to stay within the timeout
// If an error is encountered during execution, we return the partial results along with the error.
func (s *JQStage) Query(ctx context.Context, data any) ([]any, error) {
	// a note on code.Run from docs:
	// >  It is safe to call this method in goroutines, to reuse a compiled *Code.
	// > But for arguments, do not give values sharing same data between goroutines.
	qctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	it := s.code.RunWithContext(qctx, data)

	var results []any
	// Loop structure inspired by the README example:
	// https://github.com/itchyny/gojq?tab=readme-ov-file#usage-as-a-library
	for {
		result, ok := it.Next()
		if !ok {
			break
		}
		if err, ok := result.(error); ok {
			if err, ok := err.(*gojq.HaltError); ok && err.Value() == nil {
				break
			}
			return results, err
			// note: the error is emitted prior to successful outputs.
			// we stop at the first error.
			// ch <- message.PortMsg{Port: "error", Msg: message.Msg{Data: err}}
		}
		results = append(results, result)
	}
	return results, nil
}
