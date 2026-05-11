//go:build integration
// +build integration

// Integration tests against a real Redis (atomic Lua scripts can't be
// meaningfully tested against a mock). Run with:
//
//   REDIS_ADDR=127.0.0.1:16379 REDIS_PASSWORD=<pw> \
//     go test -tags=integration -race ./...
//
// Skips if REDIS_ADDR is unset, so unit-only `go test` stays fast and self-contained.

package routify

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping integration test")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       15, // dedicated DB for tests, never collides with prod (DB 0-14)
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	// Clean test DB
	rdb.FlushDB(context.Background())
	t.Cleanup(func() {
		rdb.FlushDB(context.Background())
		rdb.Close()
	})
	return rdb
}

func setBalance(t *testing.T, rdb *redis.Client, userID uint64, rmb float64) {
	t.Helper()
	micro := int64(rmb * 1_000_000)
	if err := rdb.Set(context.Background(),
		fmt.Sprintf("balance:%d", userID), micro, 0).Err(); err != nil {
		t.Fatalf("set balance: %v", err)
	}
}

func getBalanceRMB(t *testing.T, rdb *redis.Client, userID uint64) float64 {
	t.Helper()
	v, err := rdb.Get(context.Background(), fmt.Sprintf("balance:%d", userID)).Int64()
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	return float64(v) / 1_000_000
}

// ============================================================================
// Hold flow
// ============================================================================

func TestIntegration_Hold_Success(t *testing.T) {
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 100, 50.0)

	res, err := r.Hold(context.Background(), 100, 5.0)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	if res.Status != "held" {
		t.Errorf("status: want held, got %s", res.Status)
	}
	if got := getBalanceRMB(t, rdb, 100); got != 45.0 {
		t.Errorf("balance after hold: want 45.0, got %.6f", got)
	}
}

func TestIntegration_Hold_InsufficientBalance(t *testing.T) {
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 100, 1.0)

	_, err := r.Hold(context.Background(), 100, 5.0)
	if err != ErrInsufficientBalance {
		t.Errorf("want ErrInsufficientBalance, got %v", err)
	}
	if got := getBalanceRMB(t, rdb, 100); got != 1.0 {
		t.Errorf("balance must not change on rejected hold, got %.6f", got)
	}
}

// ============================================================================
// Consume / Release atomicity — this is the P0 fix from review
// ============================================================================

func TestIntegration_Consume_RefundsDifference(t *testing.T) {
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 100, 50.0)

	res, _ := r.Hold(context.Background(), 100, 10.0) // balance now 40
	if err := r.Consume(context.Background(), res, 7.5); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// 10 held - 7.5 actual = 2.5 refund -> 40 + 2.5 = 42.5
	if got := getBalanceRMB(t, rdb, 100); got != 42.5 {
		t.Errorf("balance after consume: want 42.5, got %.6f", got)
	}
}

func TestIntegration_Consume_DoubleConsumeIdempotent(t *testing.T) {
	// THE critical test for the P0 race condition fix.
	// Before the Lua rewrite, a goroutine retry could call Consume twice on the
	// same Reservation pointer and apply the refund twice. After the fix, the
	// second Consume returns an error because Redis state is no longer "held".
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 100, 50.0)

	res, _ := r.Hold(context.Background(), 100, 10.0)
	balBefore := getBalanceRMB(t, rdb, 100) // 40

	// First consume — refund 2.5
	if err := r.Consume(context.Background(), res, 7.5); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	balAfterFirst := getBalanceRMB(t, rdb, 100) // 42.5
	if balAfterFirst != balBefore+2.5 {
		t.Fatalf("first consume didn't refund 2.5: %.6f -> %.6f", balBefore, balAfterFirst)
	}

	// Second consume — must NOT change balance
	err := r.Consume(context.Background(), res, 7.5)
	if err == nil {
		t.Errorf("second Consume should error (status not held)")
	}
	balAfterSecond := getBalanceRMB(t, rdb, 100)
	if balAfterSecond != balAfterFirst {
		t.Errorf("DUPLICATE REFUND BUG: balance changed on second consume %.6f -> %.6f",
			balAfterFirst, balAfterSecond)
	}
}

func TestIntegration_Release_Idempotent(t *testing.T) {
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 100, 50.0)

	res, _ := r.Hold(context.Background(), 100, 10.0) // balance 40
	if err := r.Release(context.Background(), res); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if got := getBalanceRMB(t, rdb, 100); got != 50.0 {
		t.Errorf("balance after release: want 50, got %.6f", got)
	}

	// Second release must be no-op
	if err := r.Release(context.Background(), res); err != nil {
		t.Errorf("second release should be idempotent, got %v", err)
	}
	if got := getBalanceRMB(t, rdb, 100); got != 50.0 {
		t.Errorf("DUPLICATE REFUND on second release: %.6f", got)
	}
}

// ============================================================================
// Concurrent stress — proves Lua really is atomic under contention
// ============================================================================

func TestIntegration_ConcurrentHolds_NoOverdraft(t *testing.T) {
	// 100 goroutines all try to Hold $1 against a balance of $50.
	// Without atomicity, more than 50 could succeed (overdraft).
	// With Lua, exactly 50 succeed.
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 200, 50.0)

	var ok int64
	var insufficient int64
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			_, err := r.Hold(context.Background(), 200, 1.0)
			if err == nil {
				atomic.AddInt64(&ok, 1)
			} else if err == ErrInsufficientBalance {
				atomic.AddInt64(&insufficient, 1)
			}
		}()
	}
	wg.Wait()

	if ok != 50 {
		t.Errorf("expected exactly 50 successful holds, got ok=%d insufficient=%d", ok, insufficient)
	}
	if got := getBalanceRMB(t, rdb, 200); got != 0 {
		t.Errorf("balance should be 0 after 50×$1 holds, got %.6f", got)
	}
}

func TestIntegration_ConcurrentConsume_NoDuplicateRefund(t *testing.T) {
	// Spawn 50 goroutines all calling Consume on the same reservation.
	// Exactly one should succeed (refund applied once); the others should
	// fail with "not in held state".
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 300, 50.0)
	res, _ := r.Hold(context.Background(), 300, 10.0) // balance 40
	balBefore := getBalanceRMB(t, rdb, 300)

	var ok int64
	var failed int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.Consume(context.Background(), res, 5.0)
			if err == nil {
				atomic.AddInt64(&ok, 1)
			} else {
				atomic.AddInt64(&failed, 1)
			}
		}()
	}
	wg.Wait()

	if ok != 1 {
		t.Errorf("DUPLICATE CONSUME BUG: %d concurrent Consume calls succeeded (want exactly 1)", ok)
	}
	// Refund should be exactly 10-5=5 → balance should be balBefore + 5
	want := balBefore + 5.0
	if got := getBalanceRMB(t, rdb, 300); got != want {
		t.Errorf("balance: want %.6f (single refund applied), got %.6f", want, got)
	}
}

func TestIntegration_HoldUUID_NoCollision(t *testing.T) {
	// Hold 1000 reservations in tight loop; all IDs must be unique.
	// Pre-fix used time.Now().UnixNano() which COULD collide on multi-core hosts.
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 400, 1100.0)

	seen := make(map[string]bool, 1000)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := r.Hold(context.Background(), 400, 1.0)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[res.ID] {
				t.Errorf("duplicate reservation ID: %s", res.ID)
			}
			seen[res.ID] = true
		}()
	}
	wg.Wait()
	t.Logf("1000 concurrent holds: %d unique IDs", len(seen))
}

// ============================================================================
// Sweeper — proves the P0 reliability fix
// ============================================================================

func TestIntegration_SweepStuck_RefundsAbandonedHold(t *testing.T) {
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 500, 100.0)

	// Create a hold and rewind its created_at to look stale
	res, err := r.Hold(context.Background(), 500, 25.0)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	balAfterHold := getBalanceRMB(t, rdb, 500) // 75
	if balAfterHold != 75.0 {
		t.Fatalf("expected balance 75 after hold, got %.6f", balAfterHold)
	}

	// Rewind created_at by 1 hour
	hKey := fmt.Sprintf("hold:%s", res.ID)
	pastUnix := time.Now().Add(-1 * time.Hour).Unix()
	if err := rdb.HSet(context.Background(), hKey, "created_at", pastUnix).Err(); err != nil {
		t.Fatalf("rewind: %v", err)
	}

	// Sweep with maxAge = 30min — should release the 1h-old hold
	released, errs := r.SweepStuck(context.Background(), 30*time.Minute)
	if errs != 0 {
		t.Errorf("sweep errors: %d", errs)
	}
	if released != 1 {
		t.Fatalf("expected 1 release, got %d", released)
	}
	if got := getBalanceRMB(t, rdb, 500); got != 100.0 {
		t.Errorf("balance after sweep: want 100 (full refund), got %.6f", got)
	}

	// Re-running sweep is idempotent (status now 'released_swept')
	released2, _ := r.SweepStuck(context.Background(), 30*time.Minute)
	if released2 != 0 {
		t.Errorf("second sweep should be no-op, released=%d", released2)
	}
}

func TestIntegration_SweepStuck_PreservesFreshHolds(t *testing.T) {
	rdb := newTestRedis(t)
	r := NewReserver(rdb)
	setBalance(t, rdb, 600, 100.0)

	// Fresh hold (created now)
	_, err := r.Hold(context.Background(), 600, 30.0)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}

	// Sweep with maxAge = 30min — should NOT touch the fresh hold
	released, _ := r.SweepStuck(context.Background(), 30*time.Minute)
	if released != 0 {
		t.Errorf("fresh hold should not be swept, released=%d", released)
	}
	if got := getBalanceRMB(t, rdb, 600); got != 70.0 {
		t.Errorf("balance must remain 70 (still held), got %.6f", got)
	}
}

// ============================================================================
// CalculateCost end-to-end (no Redis needed but proves bundle compiles + runs)
// ============================================================================

func TestIntegration_CalculateCost_EndToEnd(t *testing.T) {
	// 5000 input + 800 output tokens at DeepSeek V3.2 prices ($0.14 / $0.28 per M)
	// Pro tier domestic_pro = 1.10× group rate
	tier := PricingTier{
		ModelRate:      0.14,
		CompletionRate: 2.0, // output is 2× input
		GroupRate:      1.10,
	}
	c := CalculateCost(5000, 800, tier)
	// promptCost = 5000 * 0.14 * 1.10 / 1e6 = 0.00077
	// completionCost = 800 * 0.14 * 2.0 * 1.10 / 1e6 = 0.0002464
	// total = 0.00102 (approximately)
	if abs(c.TotalCost-0.0010164) > 1e-6 {
		t.Errorf("total cost: want ~0.0010164, got %.7f", c.TotalCost)
	}
	t.Logf("5000+800 tokens DeepSeek pro = $%.6f", c.TotalCost)
	_ = time.Second // import used
}
