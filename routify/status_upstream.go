// Package routify · GET /api/status/upstream — public.
//
// Returns sanitized health snapshots for every channel (= upstream API) the
// gateway is currently configured to use. Designed to be safe to expose to
// the public /status page on app.routify.bytedance.city: emits ONLY the
// channel name, derived health bucket, and a coarse response-time bucket.
//
// Never returns: API keys, base URLs, raw status codes, or internal channel
// IDs.

package routify

import (
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"

	"github.com/gin-gonic/gin"
)

const (
	HealthOperational = "operational"
	HealthDegraded    = "degraded"
	HealthDown        = "down"
	HealthPending     = "pending"
)

type UpstreamStatus struct {
	Name             string `json:"name"`
	Health           string `json:"health"`
	ResponseTimeMs   int    `json:"response_time_ms,omitempty"`
	LastCheckedUnix  int64  `json:"last_checked,omitempty"`
	LastCheckedHuman string `json:"last_checked_human,omitempty"`
}

type StatusUpstreamResponse struct {
	Providers []UpstreamStatus `json:"providers"`
	CheckedAt int64            `json:"checked_at"`
}

// StatusUpstreamHandler implements GET /api/status/upstream.
//
// Public (no auth) — sanitized output. Cached lightly via Cache-Control to
// keep load off the DB if /status starts pulling on visit.
func StatusUpstreamHandler(c *gin.Context) {
	channels, err := model.GetAllChannels(0, 200, true, false)
	if err != nil {
		common.SysError("[routify] status/upstream: " + err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	now := time.Now().Unix()
	out := make([]UpstreamStatus, 0, len(channels))
	for _, ch := range channels {
		if ch == nil {
			continue
		}
		us := UpstreamStatus{
			Name:   ch.Name,
			Health: deriveHealth(ch, now),
		}
		if ch.TestTime > 0 {
			us.LastCheckedUnix = ch.TestTime
			us.LastCheckedHuman = humanize(now - ch.TestTime)
			// Response time only meaningful when we have a recent test.
			if ch.ResponseTime > 0 {
				us.ResponseTimeMs = bucketRTT(ch.ResponseTime)
			}
		}
		out = append(out, us)
	}

	c.Header("Cache-Control", "public, max-age=60")
	c.JSON(http.StatusOK, StatusUpstreamResponse{
		Providers: out,
		CheckedAt: now,
	})
}

// deriveHealth maps the channel's status / test_time / response_time tuple
// onto a public-facing bucket.
//
// Channel.Status convention (upstream new-api):
//
//	1 = enabled        2 = disabled (admin)        3 = auto-disabled
func deriveHealth(ch *model.Channel, now int64) string {
	if ch.Status != 1 {
		return HealthDown
	}
	if ch.TestTime == 0 {
		return HealthPending
	}
	staleSec := now - ch.TestTime
	const stalenessLimit int64 = 60 * 60 * 6 // 6 hours
	if staleSec > stalenessLimit {
		return HealthPending // we don't have recent enough data to claim ok
	}
	if ch.ResponseTime > 5000 {
		return HealthDegraded
	}
	return HealthOperational
}

// bucketRTT rounds to a coarse bucket so we don't accidentally publish
// SLA-grade numbers we can't back up.
func bucketRTT(ms int) int {
	switch {
	case ms < 200:
		return 200
	case ms < 500:
		return 500
	case ms < 1000:
		return 1000
	case ms < 2000:
		return 2000
	case ms < 5000:
		return 5000
	default:
		return 10000
	}
}

// humanize emits "2m ago" / "1h ago" / "3d ago" without dragging in a heavy
// formatter dep. English-only — the web side handles i18n if it cares.
func humanize(ageSec int64) string {
	switch {
	case ageSec < 60:
		return "just now"
	case ageSec < 3600:
		return formatDur(ageSec/60, "m")
	case ageSec < 86400:
		return formatDur(ageSec/3600, "h")
	default:
		return formatDur(ageSec/86400, "d")
	}
}

func formatDur(n int64, unit string) string {
	// Tiny strconv-free path keeps allocations low for what is a public hot path.
	return itoa(n) + unit + " ago"
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
