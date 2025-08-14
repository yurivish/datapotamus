package stage

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/msg"
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
		case m, ok := <-s.in:
			if !ok {
				// shut down gracefully if the input channel is closed
				return nil
			}
			fmt.Println(s.stage, "sleeping for", s.dur)
			time.Sleep(s.dur)
			s.out <- m.Child(m.Data).Out(msg.NewAddr(s.stage, "out"))
		case <-ctx.Done():
			return nil
		}
	}
}
