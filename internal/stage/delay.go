package stage

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/core/flow"
)

type Delay struct {
	flow.StageBase
	dur time.Duration
}

type DelayConfig struct {
	// todo: assert positive duration via validator on the config
	Millis int64 `json:"millis"`
}

func NewDelay(id string, cfg DelayConfig) (*Delay, error) {
	dur := time.Duration(cfg.Millis) * time.Millisecond
	return &Delay{StageBase: flow.NewStageBase(id), dur: dur}, nil
}

func (s *Delay) Serve(ctx context.Context) error {
	for {
		select {
		case m, ok := <-s.In:
			s.TraceRecv(m.ID)
			if !ok {
				return nil
			}
			fmt.Println(s.ID(), "sleeping for", s.dur)
			time.Sleep(s.dur)
			// Send a child message with the same data (but a new ID)
			s.TraceSend(m.Msg, m.Data, "out")
			s.TraceSucceeded(m.ID)
		case <-ctx.Done():
			return nil
		}
	}
}
