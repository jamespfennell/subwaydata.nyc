package periodic

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	hconfig "github.com/jamespfennell/hoard/config"
	"github.com/jamespfennell/subwaydata.nyc/etl"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/etl/status"
	"github.com/jamespfennell/subwaydata.nyc/etl/storage"
)

type Interval struct {
	Start time.Duration
	End   time.Duration
}

// HH:MM:SS-HH:MM:SS
func NewInterval(s string) (Interval, error) {
	s = strings.TrimSpace(s)
	if len(s) != 17 {
		return Interval{}, fmt.Errorf("string %q too long for internal (want 17, got %d)", s, len(s))
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

func (i Interval) String() string {
	return fmt.Sprintf("%s-%s", formatDuration(i.Start), formatDuration(i.End))
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}

func Run(ctx context.Context, ec *config.Config, hc *hconfig.Config, sc *storage.Client, intervals []Interval, st *status.Status) {
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
			st.RunStarted()
			err := etl.Backlog(ctx, ec, hc, sc, etl.BacklogOptions{})
			if err != nil {
				log.Printf("Backlog run failed: %s", err)
			}
			st.RunFinished(err)
		case <-ctx.Done():
			return
		}
	}
}
