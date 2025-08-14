package stage

import (
	"context"
	"fmt"
	"time"

	"datapotamus.com/internal/msg"
	"github.com/thejerf/suture/v4"
)

type Delay struct {
	Base
	duration time.Duration
}

type DelayConfig struct {
	// todo: assert positive duration via validator on the config
	Ms int64 `json:"ms"`
}

func NewDelay(id string, cfg DelayConfig) (*Delay, error) {
	return &Delay{Base: NewBase(id), duration: time.Duration(cfg.Ms) * time.Millisecond}, nil
}

func (s *Delay) Serve(ctx context.Context) error {
	for {
		select {
		case m, ok := <-s.in:
			if !ok {
				return suture.ErrDoNotRestart // todo: *should* we restart?
			}
			fmt.Println(s.id, "sleeping for", s.duration)
			time.Sleep(s.duration)
			s.out <- m.Child(m.Data).Out(msg.NewAddr(s.id, "out"))
		case <-ctx.Done():
			return nil
		}
	}
}
