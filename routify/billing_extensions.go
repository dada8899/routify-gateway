// Package routify - billing extensions overlaid onto New-API billing engine.
// Implements:
//   - Three-tier rate (model × completion × group)
//   - Cost reservation pattern (TOCTOU-safe Redis decrement)
//   - Async DB writeback queue
//
// Wired into controller/billing.go DeductBalance / AsyncWrite.

package routify

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// Pricing inputs
// ============================================================================

type PricingTier struct {
	ModelRate      float64 // 模型基础倍率（USD per 1M token）
	CompletionRate float64 // 输出 vs 输入倍率（通常 2-5x）
	GroupRate      float64 // 用户分组倍率（VIP < free < overseas_pro）
}

// CostBreakdown carries the full computation for audit logging.
type CostBreakdown struct {
	PromptTokens     uint32
	CompletionTokens uint32
	PromptCost       float64
	CompletionCost   float64
	TotalCost        float64
	Tier             PricingTier
}

// CalculateCost computes cost from tokens + tier. Pure function, no I/O.
func CalculateCost(promptTokens, completionTokens uint32, tier PricingTier) CostBreakdown {
	pc := float64(promptTokens) * tier.ModelRate * tier.GroupRate / 1_000_000.0
	cc := float64(completionTokens) * tier.ModelRate * tier.CompletionRate * tier.GroupRate / 1_000_000.0
	return CostBreakdown{
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		PromptCost:       pc,
		CompletionCost:   cc,
		TotalCost:        pc + cc,
		Tier:             tier,
	}
}

// ============================================================================
// Reservation pattern (TOCTOU-safe)
// ============================================================================

// Reservation holds a tentative deduction; finalized on call success or rolled back on failure.
type Reservation struct {
	ID         string
	UserID     uint64
	AmountRMB  float64
	CreatedAt  time.Time
	FinalCost  float64
	Status     string // "held" | "consumed" | "released"
}

// Reserver implements the cost reservation flow.
type Reserver struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewReserver(rdb *redis.Client) *Reserver {
	return &Reserver{rdb: rdb, ttl: 10 * time.Minute}
}

// HoldKeyTTL returns the lifetime of a hold record (config knob for tests).
func (r *Reserver) HoldKeyTTL() time.Duration { return r.ttl }

// randomToken returns a 16-byte hex string for collision-resistant IDs.
// Avoids time.Now().UnixNano() collisions on multi-core / high concurrency.
func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Hold tentatively deducts the *estimated* cost. Atomic via Lua to prevent TOCTOU.
//
// Flow:
//   1. SCRIPT decrements balance:{userID} by estimated cost iff balance >= cost
//   2. SETs hold:{reservationID} with metadata + TTL
//   3. Returns reservation ID for later consume / release
//
// Why we hold instead of computing-then-deduct:
//   - Without hold, two concurrent requests can both pass the balance check,
//     both succeed, and overspend. Reservation is the standard fix.
func (r *Reserver) Hold(ctx context.Context, userID uint64, estimatedRMB float64) (*Reservation, error) {
	if estimatedRMB <= 0 {
		return nil, errors.New("routify: estimated cost must be positive")
	}
	tok, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("routify: random token: %w", err)
	}
	resID := fmt.Sprintf("rsv:%d:%s", userID, tok)
	bKey := fmt.Sprintf("balance:%d", userID)
	hKey := fmt.Sprintf("hold:%s", resID)

	// Lua: KEYS[1] = balance key, KEYS[2] = hold key
	// ARGV[1] = amount (in micro-RMB to avoid float in Redis)
	// ARGV[2] = ttl seconds
	// ARGV[3] = userID, ARGV[4] = createdAt
	script := redis.NewScript(`
		local b = tonumber(redis.call('GET', KEYS[1]) or '0')
		local need = tonumber(ARGV[1])
		if b < need then
			return {0, b}
		end
		redis.call('DECRBY', KEYS[1], need)
		redis.call('HMSET', KEYS[2], 'user_id', ARGV[3], 'amount', need, 'created_at', ARGV[4], 'status', 'held')
		redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
		return {1, b - need}
	`)

	micro := int64(estimatedRMB * 1_000_000)
	res, err := script.Run(ctx, r.rdb, []string{bKey, hKey},
		micro, int64(r.ttl.Seconds()), userID, time.Now().Unix()).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("routify: reserve script failed: %w", err)
	}
	if res[0] == 0 {
		return nil, ErrInsufficientBalance
	}
	return &Reservation{
		ID:        resID,
		UserID:    userID,
		AmountRMB: estimatedRMB,
		CreatedAt: time.Now(),
		Status:    "held",
	}, nil
}

// Consume finalizes a reservation with the actual cost.
//   - actual <= held: refund difference
//   - actual > held: deduct extra (may go negative; let billing reconciliation flag)
//
// Atomic via Lua: status check + balance adjustment + status update all in one
// Redis script. Without this, two goroutines holding the same *Reservation
// (e.g., retry storms) could each pass the in-memory `res.Status == "held"`
// check and both apply the refund — duplicating money out.
func (r *Reserver) Consume(ctx context.Context, res *Reservation, actualRMB float64) error {
	bKey := fmt.Sprintf("balance:%d", res.UserID)
	hKey := fmt.Sprintf("hold:%s", res.ID)
	delta := res.AmountRMB - actualRMB // positive = refund, negative = extra deduct
	micro := int64(delta * 1_000_000)

	// KEYS[1] balance, KEYS[2] hold
	// ARGV[1] delta micro (positive=refund, negative=extra deduct)
	// ARGV[2] final_cost (RMB)
	// ARGV[3] audit ttl seconds
	//
	// Returns all-string array so Go's redis client can decode via Slice().
	// Mixing int+string in a Lua table return decodes inconsistently across
	// client versions (the integration test caught this — caller used StringSlice()
	// and Lua's `1` came back as int64).
	script := redis.NewScript(`
		local s = redis.call('HGET', KEYS[2], 'status')
		if s ~= 'held' then return {'0', s or 'missing'} end
		redis.call('HSET', KEYS[2], 'status', 'consumed', 'final_cost', ARGV[2])
		local d = tonumber(ARGV[1])
		if d > 0 then
			redis.call('INCRBY', KEYS[1], d)
		elseif d < 0 then
			redis.call('DECRBY', KEYS[1], -d)
		end
		redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
		return {'1', 'consumed'}
	`)
	raw, err := script.Run(ctx, r.rdb, []string{bKey, hKey},
		micro, actualRMB, int64((24 * time.Hour).Seconds())).Result()
	if err != nil {
		return fmt.Errorf("routify: consume script failed: %w", err)
	}
	out, err := luaStringPair(raw)
	if err != nil {
		return fmt.Errorf("routify: consume script bad return: %w", err)
	}
	if out[0] != "1" {
		return fmt.Errorf("routify: reservation %s not in held state (was %s)", res.ID, out[1])
	}
	res.Status = "consumed"
	res.FinalCost = actualRMB
	return nil
}

// Release rolls back a reservation (call failed entirely).
// Atomic via Lua. Idempotent — second Release on same reservation is no-op.
func (r *Reserver) Release(ctx context.Context, res *Reservation) error {
	bKey := fmt.Sprintf("balance:%d", res.UserID)
	hKey := fmt.Sprintf("hold:%s", res.ID)
	micro := int64(res.AmountRMB * 1_000_000)

	script := redis.NewScript(`
		local s = redis.call('HGET', KEYS[2], 'status')
		if s ~= 'held' then return {'0', s or 'missing'} end
		redis.call('HSET', KEYS[2], 'status', 'released')
		redis.call('INCRBY', KEYS[1], tonumber(ARGV[1]))
		redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
		return {'1', 'released'}
	`)
	raw, err := script.Run(ctx, r.rdb, []string{bKey, hKey},
		micro, int64((24 * time.Hour).Seconds())).Result()
	if err != nil {
		return fmt.Errorf("routify: release script failed: %w", err)
	}
	out, err := luaStringPair(raw)
	if err != nil {
		return fmt.Errorf("routify: release script bad return: %w", err)
	}
	if out[0] == "1" {
		res.Status = "released"
	}
	// out[0] == "0" → idempotent no-op (already consumed/released/missing)
	return nil
}

// luaStringPair coerces the [int|string, string] return from our Lua scripts
// into a string pair, regardless of how go-redis decoded the integer.
func luaStringPair(raw any) ([2]string, error) {
	arr, ok := raw.([]any)
	if !ok || len(arr) < 2 {
		return [2]string{}, fmt.Errorf("expected 2-element array, got %T", raw)
	}
	var out [2]string
	for i := 0; i < 2; i++ {
		switch v := arr[i].(type) {
		case string:
			out[i] = v
		case int64:
			out[i] = fmt.Sprintf("%d", v)
		default:
			out[i] = fmt.Sprintf("%v", v)
		}
	}
	return out, nil
}

var ErrInsufficientBalance = errors.New("routify: insufficient balance")

// ============================================================================
// Sweeper — auto-release stuck reservations
// ============================================================================
//
// **Critical reliability invariant**: when Hold succeeds, balance is decremented
// immediately. If the caller never calls Consume or Release (process crashed,
// network partition, panic in middle of upstream call), the hold record will
// expire after ttl and the user's money is permanently locked unless someone
// sweeps it back.
//
// SweepStuck reconciles by scanning hold records whose age > maxAge AND status
// is still "held", then refunds them via Release. Run as a background goroutine.

// SweepStuck walks hold:* keys and refunds any held record older than maxAge.
// Returns (releasedCount, errCount).
//
// Recommended cadence: every 30-60s with maxAge = 2 * ttl (e.g., 20min if
// ttl=10min). Use a Go ticker in main.go:
//
//   reserver := NewReserver(rdb)
//   go func() {
//     t := time.NewTicker(30 * time.Second)
//     for range t.C {
//       n, _ := reserver.SweepStuck(ctx, 20*time.Minute)
//       metrics.Set("routify_sweeper_released", n)
//     }
//   }()
func (r *Reserver) SweepStuck(ctx context.Context, maxAge time.Duration) (released int, errs int) {
	cutoff := time.Now().Add(-maxAge).Unix()
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, "hold:*", 200).Result()
		if err != nil {
			errs++
			return
		}
		for _, k := range keys {
			ok, err := r.sweepOne(ctx, k, cutoff)
			if err != nil {
				errs++
				continue
			}
			if ok {
				released++
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return
}

// sweepOne atomically: if hold is still 'held' AND createdAt < cutoff →
// refund balance and mark released. Returns true if released.
func (r *Reserver) sweepOne(ctx context.Context, hKey string, cutoffUnix int64) (bool, error) {
	// HMGET status, amount, user_id, created_at
	vals, err := r.rdb.HMGet(ctx, hKey, "status", "amount", "user_id", "created_at").Result()
	if err != nil || len(vals) < 4 {
		return false, err
	}
	status, _ := vals[0].(string)
	if status != "held" {
		return false, nil
	}
	amountStr, _ := vals[1].(string)
	userIDStr, _ := vals[2].(string)
	createdStr, _ := vals[3].(string)
	if amountStr == "" || userIDStr == "" || createdStr == "" {
		return false, nil
	}
	var createdAt int64
	if _, err := fmt.Sscanf(createdStr, "%d", &createdAt); err != nil {
		return false, err
	}
	if createdAt >= cutoffUnix {
		return false, nil // not stale yet
	}

	bKey := fmt.Sprintf("balance:%s", userIDStr)
	script := redis.NewScript(`
		local s = redis.call('HGET', KEYS[2], 'status')
		if s ~= 'held' then return {'0', s or 'missing'} end
		redis.call('HSET', KEYS[2], 'status', 'released_swept')
		redis.call('INCRBY', KEYS[1], tonumber(ARGV[1]))
		redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
		return {'1', 'released_swept'}
	`)
	raw, err := script.Run(ctx, r.rdb, []string{bKey, hKey},
		amountStr, int64((24 * time.Hour).Seconds())).Result()
	if err != nil {
		return false, err
	}
	out, err := luaStringPair(raw)
	if err != nil {
		return false, err
	}
	return out[0] == "1", nil
}
