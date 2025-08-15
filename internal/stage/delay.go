package stage

import (
	"context"
	"fmt"
	"time"
)

type Delay struct {
	Base
	dur time.Duration
}

type DelayConfig struct {
	// todo: assert positive duration via validator on the config
	Millis int64 `json:"millis"`
}

func NewDelay(id string, cfg DelayConfig) (*Delay, error) {
	dur := time.Duration(cfg.Millis) * time.Millisecond
	return &Delay{Base: NewBase(id), dur: dur}, nil
}

func (s *Delay) Serve(ctx context.Context) error {
	for {
		select {
		case m, ok := <-s.In:
			s.TraceReceived(m.ID)
			if !ok {
				return nil
			}
			fmt.Println(s.id, "sleeping for", s.dur)
			time.Sleep(s.dur)
			// Send a child message with the same data (but a new ID)
			s.SendChild(m.Msg, m.Data, "out")
			s.TraceSucceeded(m.ID)
		case <-ctx.Done():
			return nil
		}
	}
}
