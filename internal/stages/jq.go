package stages

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/flow"
	"github.com/itchyny/gojq"
)

// todo: where if anywhere should we use suture.ErrTerminateSupervisorTree to fail the whole flow on unexpected errors?

type JQ struct {
	flow.Base
	code    *gojq.Code
	timeout time.Duration
}

func NewJQ(base *flow.Base, filter string, timeout time.Duration) (*JQ, error) {
	query, err := gojq.Parse(filter)
	if err != nil {
		return nil, fmt.Errorf("jq: failed to parse filter: %w", err)
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return nil, fmt.Errorf("jq: failed to compile filter: %w", err)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("jq config: TimeoutMillis should be greater than zero")
	}
	return &JQ{*base, code, timeout}, nil
}

func (s *JQ) Serve(ctx context.Context) error {
	for {
		select {
		case m, ok := <-s.Ch.In:
			if !ok {
				close(s.Ch.Out)
				return nil
			}
			fmt.Println("jq pre received")
			s.TraceRecv(m.ID)
			fmt.Println("jq received")
			results, err := s.Query(ctx, m.Data)
			if err != nil {
				s.TraceFailure(m.ID, err)
			} else {
				for _, result := range results {
					s.TraceSend("out", m.Msg, result)
				}
				s.TraceSuccess(m.ID)

				// for merge nodes:
				// id := s.TraceMerge(...)
				// s.Send(msg.NewWithID(id, ...), "out")
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// Run a JQ query on the input data, eagerly materializing the results to stay within the timeout
// If an error is encountered during execution, we return the partial results along with the error,
// though they're not used by the stage right now (we could also continue past the error).
func (s *JQ) Query(ctx context.Context, data any) ([]any, error) {
	// a note on code.Run from docs:
	// >  It is safe to call this method in goroutines, to reuse a compiled *Code.
	// > But for arguments, do not give values sharing same data between goroutines.
	qctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	it := s.code.RunWithContext(qctx, data)

	// Loop structure inspired by the README example:
	// https://github.com/itchyny/gojq?tab=readme-ov-file#usage-as-a-library
	var results []any
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
