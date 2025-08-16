package stages

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/flow"
)

type Delay struct {
	flow.StageBase
	duration time.Duration
}

func NewDelay(id string, duration time.Duration) *Delay {
	return &Delay{flow.NewStageBase(id), duration}
}

type DelayConfig struct {
	// todo: assert positive duration via validator on the config
	Millis int64 `json:"millis"`
}

func DelayFromConfig(id string, cfg DelayConfig) (*Delay, error) {
	duration := time.Duration(cfg.Millis) * time.Millisecond
	return NewDelay(id, duration), nil
}

func (s *Delay) Serve(ctx context.Context) error {
	for {
		select {
		case m, ok := <-s.InChan:
			s.TraceRecv(m.ID)
			if !ok {
				return nil
			}
			fmt.Println(s.ID(), "sleeping for", s.duration)
			time.Sleep(s.duration)
			// Send a child message with the same data (but a new ID)
			s.TraceSend(m.Msg, m.Data, "out")
			s.TraceSucceeded(m.ID)
		case <-ctx.Done():
			return nil
		}
	}
}
