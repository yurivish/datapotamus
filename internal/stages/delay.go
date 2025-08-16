package stages

import (
	"context"
	"time"

	"datapotamus.com/internal/flow"
)

type Delay struct {
	flow.Base
	duration time.Duration
}

func NewDelay(base *flow.Base, duration time.Duration) *Delay {
	return &Delay{*base, duration}
}

func (s *Delay) Serve(ctx context.Context) error {
	for {
		select {
		case m, ok := <-s.Ch.In:
			s.TraceRecv(m.ID)
			if !ok {
				close(s.Ch.Out)
				return nil
			}
			time.Sleep(s.duration)
			s.TraceSend("out", m.Msg, m.Data)
			s.TraceSuccess(m.ID)
		case <-ctx.Done():
			return nil
		}
	}
}
