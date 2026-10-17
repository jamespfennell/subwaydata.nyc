package periodic

import "testing"

func TestInterval_String(t *testing.T) {
	for _, s := range []string{"05:30:00-06:00:00", "00:00:00-23:59:59"} {
		interval, err := NewInterval(s)
		if err != nil {
			t.Fatalf("NewInterval(%q) failed: %s", s, err)
		}
		if got := interval.String(); got != s {
			t.Errorf("NewInterval(%q).String() = %q, want %q", s, got, s)
		}
	}
}
