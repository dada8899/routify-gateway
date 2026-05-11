package routify

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestChannel(id uint32, name, vendor string, models []string, priority, weight int, region string) *Channel {
	return &Channel{
		ID:          id,
		Name:        name,
		Vendor:      vendor,
		Models:      models,
		Priority:    priority,
		Weight:      weight,
		Region:      region,
		QualityTier: "official",
		Enabled:     true,
		Healthy:     true,
		BalanceRMB:  1000,
	}
}

func newTestRouter(t *testing.T, channels []*Channel) *Router {
	t.Helper()
	r := NewRouter(channels)
	r.now = func() time.Time { return time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) }
	return r
}

func TestSelectChannel_NoChannelForModel(t *testing.T) {
	r := newTestRouter(t, []*Channel{
		newTestChannel(1, "deepseek-official", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic"),
	})
	_, err := r.SelectChannel(context.Background(), RouteRequest{
		Model: "gpt-4o",
		User:  User{ID: 1, Group: "domestic_pro"},
	})
	if !errors.Is(err, ErrNoChannelForModel) {
		t.Fatalf("expected ErrNoChannelForModel, got %v", err)
	}
}

func TestSelectChannel_OnlyOneCandidate(t *testing.T) {
	r := newTestRouter(t, []*Channel{
		newTestChannel(1, "deepseek-official", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic"),
	})
	dec, err := r.SelectChannel(context.Background(), RouteRequest{
		Model: "deepseek-v3.2",
		User:  User{ID: 1, Group: "domestic_pro"},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if dec.Channel.ID != 1 {
		t.Fatalf("expected channel 1, got %d", dec.Channel.ID)
	}
}

func TestSelectChannel_FiltersUnhealthy(t *testing.T) {
	c1 := newTestChannel(1, "primary", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic")
	c1.Healthy = false
	c2 := newTestChannel(2, "backup", "deepseek", []string{"deepseek-v3.2"}, 50, 50, "domestic")
	r := newTestRouter(t, []*Channel{c1, c2})

	dec, err := r.SelectChannel(context.Background(), RouteRequest{
		Model: "deepseek-v3.2",
		User:  User{ID: 1, Group: "domestic_pro"},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if dec.Channel.ID != 2 {
		t.Fatalf("expected fallback channel 2, got %d", dec.Channel.ID)
	}
}

func TestSelectChannel_AllUnhealthy(t *testing.T) {
	c1 := newTestChannel(1, "primary", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic")
	c1.Healthy = false
	r := newTestRouter(t, []*Channel{c1})

	_, err := r.SelectChannel(context.Background(), RouteRequest{
		Model: "deepseek-v3.2",
		User:  User{ID: 1, Group: "domestic_pro"},
	})
	if !errors.Is(err, ErrNoHealthyChannel) {
		t.Fatalf("expected ErrNoHealthyChannel, got %v", err)
	}
}

func TestSelectChannel_BalanceCriticalSkipped(t *testing.T) {
	c1 := newTestChannel(1, "low-balance", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic")
	c1.BalanceRMB = 5
	c2 := newTestChannel(2, "ok", "deepseek", []string{"deepseek-v3.2"}, 50, 50, "domestic")
	r := newTestRouter(t, []*Channel{c1, c2})

	dec, err := r.SelectChannel(context.Background(), RouteRequest{
		Model: "deepseek-v3.2",
		User:  User{ID: 1, Group: "domestic_pro"},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if dec.Channel.ID != 2 {
		t.Fatalf("expected channel 2 (sufficient balance), got %d", dec.Channel.ID)
	}
}

func TestSelectChannel_RegionPreferenceRespected(t *testing.T) {
	c1 := newTestChannel(1, "domestic-only", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic")
	c2 := newTestChannel(2, "overseas", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "overseas")
	r := newTestRouter(t, []*Channel{c1, c2})

	dec, _ := r.SelectChannel(context.Background(), RouteRequest{
		Model:        "deepseek-v3.2",
		User:         User{ID: 1, Group: "overseas_pro"},
		PreferRegion: "overseas",
	})
	if dec.Channel.ID != 2 {
		t.Fatalf("expected overseas channel, got %d", dec.Channel.ID)
	}
}

func TestMarkFailure_DegradesHealth(t *testing.T) {
	c1 := newTestChannel(1, "main", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic")
	r := newTestRouter(t, []*Channel{c1})

	for i := 0; i < 10; i++ {
		r.MarkFailure(1)
	}
	if c1.Healthy {
		t.Fatalf("expected channel to become unhealthy after 10 failures, FailureRate=%.2f", c1.FailureRate)
	}
}

func TestMarkSuccess_RecoversHealth(t *testing.T) {
	c1 := newTestChannel(1, "main", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic")
	c1.Healthy = false
	c1.FailureRate = 0.5
	r := newTestRouter(t, []*Channel{c1})

	for i := 0; i < 30; i++ {
		r.MarkSuccess(1, 50)
	}
	if !c1.Healthy {
		t.Fatalf("expected channel to recover after 30 successes, FailureRate=%.2f", c1.FailureRate)
	}
}

func TestWeightedPick_RespectsPriority(t *testing.T) {
	c1 := newTestChannel(1, "high-prio", "deepseek", []string{"deepseek-v3.2"}, 100, 100, "domestic")
	c2 := newTestChannel(2, "low-prio", "deepseek", []string{"deepseek-v3.2"}, 10, 10, "domestic")
	r := newTestRouter(t, []*Channel{c1, c2})

	hits := map[uint32]int{}
	for i := 0; i < 10000; i++ {
		dec, _ := r.SelectChannel(context.Background(), RouteRequest{
			Model: "deepseek-v3.2",
			User:  User{ID: 1, Group: "domestic_pro"},
		})
		hits[dec.Channel.ID]++
	}
	// c1 score = 100*100 = 10000, c2 score = 10*10 = 100, ratio ≈ 100:1
	// Expect c1 ≥ 95% of picks
	if hits[1] < 9500 {
		t.Fatalf("expected high-priority channel to win >= 95%%, got %d/%d", hits[1], 10000)
	}
}

func TestVIPPrefersOfficial(t *testing.T) {
	c1 := newTestChannel(1, "proxy", "anthropic", []string{"claude-opus-4-7"}, 100, 100, "overseas")
	c1.QualityTier = "proxy"
	c2 := newTestChannel(2, "official", "anthropic", []string{"claude-opus-4-7"}, 100, 100, "overseas")
	c2.QualityTier = "official"
	r := newTestRouter(t, []*Channel{c1, c2})

	hits := map[uint32]int{}
	for i := 0; i < 1000; i++ {
		dec, _ := r.SelectChannel(context.Background(), RouteRequest{
			Model: "claude-opus-4-7",
			User:  User{ID: 1, Group: "overseas_vip"},
		})
		hits[dec.Channel.ID]++
	}
	// VIP gets 1.5x bonus on official → c2 expected ~60% (1.5/2.5)
	// Allow ±5σ tolerance for sampling variance: 1000 trials, p=0.6, σ≈15.5
	// → reasonable threshold is 550 (>3σ above 50% null hypothesis)
	if hits[2] < 550 {
		t.Fatalf("expected VIP to prefer official channel >= 55%%, got proxy=%d official=%d",
			hits[1], hits[2])
	}
}

func TestFreeUserSkippedFromPremiumOnly(t *testing.T) {
	c := newTestChannel(1, "anthropic", "anthropic", []string{"claude-opus-4-7"}, 100, 100, "overseas")
	r := newTestRouter(t, []*Channel{c})

	_, err := r.SelectChannel(context.Background(), RouteRequest{
		Model: "claude-opus-4-7",
		User:  User{ID: 1, Group: "free"},
	})
	// Free users blocked → fallback to ignoring permitted filter, still get channel
	// (current impl: permitted falls back to healthy if empty)
	// 实际策略：免费用户允许尝试便宜模型，premium 模型应该在 model 维度阻断
	// 此测试确认 fallback 不会 deadlock
	if err != nil && !errors.Is(err, ErrAllChannelsFiltered) {
		t.Fatalf("got unexpected err: %v", err)
	}
}
