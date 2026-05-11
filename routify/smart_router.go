// Package routify implements smart channel routing for Routify Gateway.
// Overlays the default channel selection in controller/relay.go.
//
// Overlay path: <fork-root>/routify/router/smart_router.go
// Wired into:    controller/relay.go SelectChannel(...)

package routify

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ============================================================================
// Types
// ============================================================================

// Channel describes one upstream provider configuration.
type Channel struct {
	ID            uint32
	Name          string
	Vendor        string
	BaseURL       string
	APIKey        string
	Models        []string
	Priority      int     // 100 = official direct, 50 = proxy, 10 = fallback
	Weight        int     // raw weight (1-100)
	Region        string  // domestic | overseas
	QualityTier   string  // official | proxy | community
	Enabled       bool
	Healthy       bool
	BalanceRMB    float64
	AvgLatencyMs  float64
	FailureRate   float64
	LastFailureAt time.Time
}

// User describes the requester (subset relevant to routing).
type User struct {
	ID     uint64
	Group  string // free | domestic_pro | overseas_vip | kol | enterprise
	Region string // CN | US | EU | OTHER
}

// RouteRequest carries one model invocation about to be routed.
type RouteRequest struct {
	Model        string
	User         User
	StreamMode   bool
	EstTokens    uint32
	PreferRegion string // 可选偏好（用户在 dashboard 设置）
}

// RouteDecision returns the chosen channel + audit trail.
type RouteDecision struct {
	Channel    *Channel
	Reason     string
	Candidates int
	Filtered   int
}

// ============================================================================
// Router
// ============================================================================

// Router holds channel pool and routing state.
type Router struct {
	mu       sync.RWMutex
	channels []*Channel
	now      func() time.Time
	rngMu    sync.Mutex
	rng      *rand.Rand
}

// NewRouter constructs a router from a snapshot of channels.
func NewRouter(channels []*Channel) *Router {
	return &Router{
		channels: channels,
		now:      time.Now,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// rngFloat returns a thread-safe random float in [0,1).
// math/rand v1's Rand is NOT safe for concurrent use without external locking.
func (r *Router) rngFloat() float64 {
	r.rngMu.Lock()
	defer r.rngMu.Unlock()
	return r.rng.Float64()
}

// Replace atomically swaps the channel pool (e.g., after admin edit / health check).
func (r *Router) Replace(channels []*Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.channels = channels
}

// SelectChannel picks the best channel for a given request.
// 5-step pipeline: candidates → healthy → permitted → scored → weighted-pick.
func (r *Router) SelectChannel(ctx context.Context, req RouteRequest) (RouteDecision, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Candidates: channels that support this model
	candidates := r.candidatesFor(req.Model)
	if len(candidates) == 0 {
		return RouteDecision{}, ErrNoChannelForModel
	}

	// 2. Healthy filter
	healthy := r.filterHealthy(candidates)
	if len(healthy) == 0 {
		return RouteDecision{Candidates: len(candidates)}, ErrNoHealthyChannel
	}

	// 3. Permitted: respects region preference + user group constraints
	permitted := r.filterPermitted(healthy, req)
	if len(permitted) == 0 {
		permitted = healthy // fall back ignoring soft preferences
	}

	// 4. Score
	scored := r.scoreChannels(permitted, req)

	// 5. Weighted pick
	chosen := r.weightedPick(scored)
	if chosen == nil {
		return RouteDecision{}, ErrAllChannelsFiltered
	}

	return RouteDecision{
		Channel:    chosen,
		Reason:     r.explain(chosen, req),
		Candidates: len(candidates),
		Filtered:   len(candidates) - len(permitted),
	}, nil
}

// ============================================================================
// Pipeline steps
// ============================================================================

func (r *Router) candidatesFor(model string) []*Channel {
	out := make([]*Channel, 0, 4)
	for _, c := range r.channels {
		if !c.Enabled {
			continue
		}
		for _, m := range c.Models {
			if m == model {
				out = append(out, c)
				break
			}
		}
	}
	return out
}

func (r *Router) filterHealthy(in []*Channel) []*Channel {
	out := make([]*Channel, 0, len(in))
	now := r.now()
	for _, c := range in {
		if !c.Healthy {
			continue
		}
		// 5min cooldown after last failure batch
		if !c.LastFailureAt.IsZero() && now.Sub(c.LastFailureAt) < 5*time.Minute && c.FailureRate > 0.2 {
			continue
		}
		// Balance critical
		if c.BalanceRMB > 0 && c.BalanceRMB < 20 {
			continue
		}
		out = append(out, c)
	}
	return out
}

func (r *Router) filterPermitted(in []*Channel, req RouteRequest) []*Channel {
	out := make([]*Channel, 0, len(in))
	for _, c := range in {
		// Region soft preference
		if req.PreferRegion != "" && c.Region != "" && c.Region != req.PreferRegion {
			continue
		}
		// Free users only get cheapest channels (proxy / community OK, but no premium-only paths)
		if req.User.Group == "free" && c.QualityTier == "official" && c.Vendor == "anthropic" {
			continue
		}
		out = append(out, c)
	}
	return out
}

// score = priority * weight * health_factor * cost_factor * latency_factor
type scored struct {
	channel *Channel
	score   float64
}

func (r *Router) scoreChannels(in []*Channel, req RouteRequest) []scored {
	out := make([]scored, 0, len(in))
	for _, c := range in {
		s := float64(c.Priority) * float64(c.Weight)
		// Failure penalty: 0% → 1.0, 50% → 0.5
		s *= (1.0 - c.FailureRate)
		// Latency penalty (best <100ms; degrade beyond 1s)
		if c.AvgLatencyMs > 100 {
			latencyFactor := 1.0 / (1.0 + (c.AvgLatencyMs-100)/1000.0)
			s *= latencyFactor
		}
		// Premium users prefer official quality
		if req.User.Group == "overseas_vip" || req.User.Group == "domestic_vip" {
			if c.QualityTier == "official" {
				s *= 1.5
			}
		}
		out = append(out, scored{channel: c, score: s})
	}
	// Sort desc for explainability + tests
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].score > out[j].score
	})
	return out
}

func (r *Router) weightedPick(in []scored) *Channel {
	if len(in) == 0 {
		return nil
	}
	if len(in) == 1 {
		return in[0].channel
	}
	total := 0.0
	for _, s := range in {
		total += s.score
	}
	if total <= 0 {
		return in[0].channel
	}
	target := r.rngFloat() * total
	cum := 0.0
	for _, s := range in {
		cum += s.score
		if target <= cum {
			return s.channel
		}
	}
	return in[len(in)-1].channel
}

func (r *Router) explain(c *Channel, req RouteRequest) string {
	return fmt.Sprintf("%s · prio=%d weight=%d region=%s group=%s",
		c.Name, c.Priority, c.Weight, c.Region, req.User.Group)
}

// ============================================================================
// Health updates (called by health checker / failure observer)
// ============================================================================

// MarkFailure records a failed call, updates failure rate exponentially.
func (r *Router) MarkFailure(channelID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.channels {
		if c.ID == channelID {
			c.LastFailureAt = r.now()
			c.FailureRate = c.FailureRate*0.7 + 0.3 // EMA up
			if c.FailureRate > 0.8 {
				c.Healthy = false
			}
			return
		}
	}
}

// MarkSuccess records a successful call, decays failure rate.
func (r *Router) MarkSuccess(channelID uint32, latencyMs float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.channels {
		if c.ID == channelID {
			c.FailureRate *= 0.9 // EMA down
			if c.FailureRate < 0.1 {
				c.Healthy = true
			}
			c.AvgLatencyMs = c.AvgLatencyMs*0.9 + latencyMs*0.1
			return
		}
	}
}

// ============================================================================
// Helpers
// ============================================================================

var (
	ErrNoChannelForModel   = errors.New("routify: no channel supports this model")
	ErrNoHealthyChannel    = errors.New("routify: all candidate channels are unhealthy")
	ErrAllChannelsFiltered = errors.New("routify: all channels filtered out")
)
