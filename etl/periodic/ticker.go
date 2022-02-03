package periodic

import (
	"context"
	"log"
	"time"
)

type Ticker struct {
	C        <-chan time.Duration
	stopFunc context.CancelFunc
}

func NewTicker(starts []time.Duration, loc *time.Location) *Ticker {
	c := make(chan time.Duration)
	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		nextStart := 0
		now := time.Now().In(loc)
		y, m, d := now.Date()
		midnight := time.Date(y, m, d, 0, 0, 0, 0, loc)

		for {
			now := time.Now().In(loc)
			absStart := midnight.Add(starts[nextStart])
			pauseTime := absStart.Sub(now)
			if pauseTime >= 0 {
				log.Printf("pausing for %s\n", pauseTime)
				pauseTicker := time.NewTicker(pauseTime)
				select {
				case <-ctx.Done():
					pauseTicker.Stop()
					return
				case <-pauseTicker.C:
					pauseTicker.Stop()
				}

				select {
				case <-ctx.Done():
					return
				case c <- starts[nextStart]:
				}
			}

			nextStart += 1
			if nextStart == len(starts) {
				nextStart = 0
				tomorrowAfternoon := midnight.Add(32 * time.Hour)
				y, m, d := tomorrowAfternoon.Date()
				midnight = time.Date(y, m, d, 0, 0, 0, 0, loc)
			}
		}
	}()
	return &Ticker{
		C:        c,
		stopFunc: cancelFunc,
	}
}

func (t *Ticker) Stop() {
	t.stopFunc()
}
