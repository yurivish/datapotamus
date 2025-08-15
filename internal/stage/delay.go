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
		case m, ok := <-s.In:
			if !ok {
				// shut down gracefully if the input channel is closed
				return nil
			}
			fmt.Println(s.id, "sleeping for", s.dur)
			time.Sleep(s.dur)
			s.Out <- m.Child(m.Data).Out(msg.NewAddr(s.id, "out"))
		case <-ctx.Done():
			return nil
		}
	}
}
