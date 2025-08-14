package stage

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/msg"
	"github.com/itchyny/gojq"
)

// todo: where if anywhere should we use suture.ErrTerminateSupervisorTree to fail the whole flow on unexpected errors?

type JQ struct {
	Base
	code    *gojq.Code
	timeout time.Duration
}

type JQConfig struct {
	Filter string `json:"filter"`
}

func NewJQ(id string, cfg JQConfig) (*JQ, error) {
	query, err := gojq.Parse(cfg.Filter)
	if err != nil {
		// todo: Use gojq.ParseError to get the error position and token of the parsing error
		return nil, fmt.Errorf("failed to parse JQ query: %w", err)
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JQ query: %w", err)
	}
	timeout := 250 * time.Millisecond
	return &JQ{Base: NewBase(id), code: code, timeout: timeout}, nil
}

func (s *JQ) Serve(ctx context.Context) error {
	fmt.Println("jq:", s.stage, "serving")
	for {
		select {
		case m, ok := <-s.in:
			fmt.Printf("jq: %v: received %v %v\n", s.stage, m, ok)
			// if the input channel is closed then exit gracefully
			if !ok {
				return nil
			}
			results, err := s.Query(ctx, m.Data)
			fmt.Println("jq results", results, err)
			// send the error, if any. otherwise send the results, if any.
			if err != nil {
				s.out <- m.Child(err).Out(msg.NewAddr(s.stage, "error"))
			} else {
				// todo: we could send partial results even in the face of an error, or
				// even multiple errors, if we decide that is the behavior we want.
				for _, result := range results {
					s.out <- m.Child(result).Out(msg.NewAddr(s.stage, "out"))
				}
			}
		case <-ctx.Done():
			fmt.Printf("jq: %v: ctx done", s.stage)
			return nil
		}
	}

}

// Run the JQ query with the input data, eagerly materializing the results to stay within the timeout
// If an error is encountered during execution, we return the partial results along with the error.
func (s *JQ) Query(ctx context.Context, data any) ([]any, error) {
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
			// note: we stop at the first error.
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}
