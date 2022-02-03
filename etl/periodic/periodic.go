package periodic

import (
	"context"
	"fmt"
	"time"

	hconfig "github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/pipeline"
	"github.com/jamespfennell/subwaydata.nyc/etl/storage"
)

type Interval struct {
	Start time.Duration
	End   time.Duration
}

// HH:MM:SS-HH:MM:SS
func NewInterval(s string) (Interval, error) {
	if len(s) != 17 {
		return Interval{}, fmt.Errorf("string not in the correct form for interval")
	}
	if s[8] != '-' {
		return Interval{}, fmt.Errorf("string not in the correct form for interval")
	}
	start, err := time.Parse("15:04:05", s[:8])
	if err != nil {
		return Interval{}, fmt.Errorf("failed to parse %s as HH:MM:SS - %w", s[:8], err)
	}
	end, err := time.Parse("15:04:05", s[9:])
	if err != nil {
		return Interval{}, fmt.Errorf("failed to parse %s as HH:MM:SS - %w", s[9:], err)
	}
	midnight, _ := time.Parse("15:04:05", "00:00:00")
	return Interval{
		Start: start.Sub(midnight),
		End:   end.Sub(midnight),
	}, nil
}

func Run(ctx context.Context, ec *config.Config, hc *hconfig.Config, sc *storage.Client, intervals []Interval) {
	var starts []time.Duration
	startToTimeout := map[time.Duration]time.Duration{}
	for _, interval := range intervals {
		starts = append(starts, interval.Start)
		startToTimeout[interval.Start] = interval.End - interval.Start
	}
	ticker := NewTicker(starts, ec.Timezone.AsLoc())
	defer ticker.Stop()
	for {
		select {
		case start := <-ticker.C:
			//ctx, cancelFunc := context.WithTimeout(ctx, startToTimeout[start])
			fmt.Println("Running backlog for time", start)
			pipeline.Backlog(ec, hc, sc, pipeline.BacklogOptions{})
		case <-ctx.Done():
			return
		}
	}
}
