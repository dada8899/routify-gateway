package routify

import (
	"testing"
	"time"

	"github.com/QuantumNous/new-api/model"
)

func TestDeriveHealth_Operational(t *testing.T) {
	now := time.Now().Unix()
	ch := &model.Channel{
		Status:       1,
		TestTime:     now - 60, // 1m ago
		ResponseTime: 250,
	}
	if got := deriveHealth(ch, now); got != HealthOperational {
		t.Errorf("want operational, got %q", got)
	}
}

func TestDeriveHealth_Disabled(t *testing.T) {
	now := time.Now().Unix()
	ch := &model.Channel{Status: 2, TestTime: now - 60, ResponseTime: 100}
	if got := deriveHealth(ch, now); got != HealthDown {
		t.Errorf("want down, got %q", got)
	}
}

func TestDeriveHealth_AutoDisabled(t *testing.T) {
	now := time.Now().Unix()
	ch := &model.Channel{Status: 3, TestTime: now - 60}
	if got := deriveHealth(ch, now); got != HealthDown {
		t.Errorf("want down, got %q", got)
	}
}

func TestDeriveHealth_NeverTested(t *testing.T) {
	now := time.Now().Unix()
	ch := &model.Channel{Status: 1, TestTime: 0}
	if got := deriveHealth(ch, now); got != HealthPending {
		t.Errorf("want pending, got %q", got)
	}
}

func TestDeriveHealth_StaleTest(t *testing.T) {
	now := time.Now().Unix()
	// 7 hours ago — beyond the 6h staleness limit.
	ch := &model.Channel{Status: 1, TestTime: now - 7*3600, ResponseTime: 200}
	if got := deriveHealth(ch, now); got != HealthPending {
		t.Errorf("want pending (stale), got %q", got)
	}
}

func TestDeriveHealth_SlowResponse(t *testing.T) {
	now := time.Now().Unix()
	ch := &model.Channel{Status: 1, TestTime: now - 60, ResponseTime: 7000}
	if got := deriveHealth(ch, now); got != HealthDegraded {
		t.Errorf("want degraded, got %q", got)
	}
}

func TestBucketRTT(t *testing.T) {
	for _, c := range []struct {
		in, want int
	}{
		{50, 200},
		{300, 500},
		{800, 1000},
		{1500, 2000},
		{4500, 5000},
		{8000, 10000},
	} {
		if got := bucketRTT(c.in); got != c.want {
			t.Errorf("bucketRTT(%d): want %d got %d", c.in, c.want, got)
		}
	}
}

func TestHumanize(t *testing.T) {
	for _, c := range []struct {
		in   int64
		want string
	}{
		{30, "just now"},
		{120, "2m ago"},
		{3600, "1h ago"},
		{7200, "2h ago"},
		{86400, "1d ago"},
		{172800, "2d ago"},
	} {
		if got := humanize(c.in); got != c.want {
			t.Errorf("humanize(%d): want %q got %q", c.in, c.want, got)
		}
	}
}
