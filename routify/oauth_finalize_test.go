package routify

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// setupTestDB initializes an in-memory SQLite, sets model.DB, and migrates
// just the schema needed for oauth-finalize (User + RoutifyOAuthAccount).
//
// We deliberately do NOT call model.InitDB() to avoid bootstrapping every
// upstream table + the redis/option/etc. side effects. The handler only
// touches model.User and routify_oauth_accounts; that's what we migrate.
func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	// Per-test temp file backing — avoids :memory: per-connection isolation,
	// which would break the concurrency test (each goroutine sees an empty DB).
	dbFile := t.TempDir() + "/test.db"
	db, err := gorm.Open(sqlite.Open(dbFile+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"), &gorm.Config{
		Logger: logger.Discard,
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.User{}, &model.Log{}, &RoutifyOAuthAccount{}, &RoutifyOAuthAuditLog{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	prevDB := model.DB
	prevLogDB := model.LOG_DB
	prevQuotaNew := common.QuotaForNewUser
	prevQuotaInvitee := common.QuotaForInvitee
	prevQuotaInviter := common.QuotaForInviter
	prevRedisEnabled := common.RedisEnabled

	model.DB = db
	// Logging table shares the same in-memory DB to satisfy RecordLog.
	model.LOG_DB = db

	// Test-time defaults (would normally come from common.InitEnv).
	common.QuotaForNewUser = 500_000 // small free credit, e.g. 500k tokens
	common.QuotaForInvitee = 0
	common.QuotaForInviter = 0
	// Disable Redis-backed user cache writes (RDB is nil in tests).
	common.RedisEnabled = false

	t.Cleanup(func() {
		model.DB = prevDB
		model.LOG_DB = prevLogDB
		common.QuotaForNewUser = prevQuotaNew
		common.QuotaForInvitee = prevQuotaInvitee
		common.QuotaForInviter = prevQuotaInviter
		common.RedisEnabled = prevRedisEnabled
	})

	return db
}

// withDevMode routes a sub-test with the dev-mode env var set.
// Restores prior value automatically.
func withDevMode(t *testing.T, fn func()) {
	t.Helper()
	prev := os.Getenv(devModeEnv)
	if err := os.Setenv(devModeEnv, "1"); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	defer func() {
		if prev == "" {
			_ = os.Unsetenv(devModeEnv)
		} else {
			_ = os.Setenv(devModeEnv, prev)
		}
	}()
	fn()
}

// makeRequest builds a POST /api/auth/oauth-finalize request and returns the
// recorder + decoded body for assertions.
func makeRequest(t *testing.T, body any, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	apiRouter := r.Group("/api")
	RegisterRoutes(apiRouter)

	bs, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/auth/oauth-finalize", bytes.NewReader(bs))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	out := map[string]any{}
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode resp: %v (body=%q)", err, w.Body.String())
		}
	}
	return w, out
}

// ---------- generateOAuthUsername ----------

func TestGenerateOAuthUsername_LengthAndPrefix(t *testing.T) {
	for _, prov := range []string{"google", "github", "linkedin", "x"} {
		got, err := generateOAuthUsername(prov)
		if err != nil {
			t.Fatalf("provider=%s: %v", prov, err)
		}
		if len(got) > maxUsernameLen {
			t.Errorf("provider=%s: username %q len=%d > 20", prov, got, len(got))
		}
		if !strings.HasPrefix(got, usernamePrefix) {
			t.Errorf("provider=%s: missing prefix in %q", prov, got)
		}
	}
}

// ---------- find-or-create paths ----------

func TestFindOrCreate_FreshUser(t *testing.T) {
	setupTestDB(t)
	req := &OAuthFinalizeRequest{
		Provider:       "google",
		ProviderUserId: "google-sub-12345",
		Email:          "newuser@example.com",
		Name:           "New User",
	}
	u, err := findOrCreateOAuthUser(model.DB, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if u.Id == 0 {
		t.Fatalf("user.Id zero after insert")
	}
	if u.Email != req.Email {
		t.Errorf("email: want %q got %q", req.Email, u.Email)
	}
	if u.Quota != common.QuotaForNewUser {
		t.Errorf("free quota not granted: want %d got %d", common.QuotaForNewUser, u.Quota)
	}
	// Verify the linkage row exists.
	acc, hit, err := FindOAuthAccount(model.DB, "google", "google-sub-12345")
	if err != nil || !hit {
		t.Fatalf("oauth account not linked (hit=%v err=%v)", hit, err)
	}
	if acc.UserId != u.Id {
		t.Errorf("linkage user_id mismatch: want %d got %d", u.Id, acc.UserId)
	}
}

func TestFindOrCreate_ExistingOAuthAccount(t *testing.T) {
	setupTestDB(t)
	req := &OAuthFinalizeRequest{
		Provider:       "github",
		ProviderUserId: "12345",
		Email:          "user@example.com",
	}
	first, err := findOrCreateOAuthUser(model.DB, req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// Same provider+sub, different email (provider may have changed it). Should
	// return original user, not create a second.
	req2 := *req
	req2.Email = "different@example.com"
	second, err := findOrCreateOAuthUser(model.DB, &req2)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.Id != second.Id {
		t.Errorf("expected same user; first=%d second=%d", first.Id, second.Id)
	}
	// Count users — must be exactly 1.
	var n int64
	model.DB.Model(&model.User{}).Count(&n)
	if n != 1 {
		t.Errorf("expected 1 user, got %d", n)
	}
}

func TestFindOrCreate_LinkExistingEmail_OAuthOnlyUserAllowed(t *testing.T) {
	setupTestDB(t)
	// Pre-create an OAuth-only user (no password). Path-2 should auto-link a
	// new provider for the same email — this is the legitimate
	// "user adds a second provider with the same email" flow.
	existing := &model.User{
		Username:    "existinguser",
		Password:    "", // OAuth-only — no password
		DisplayName: "Existing",
		Email:       "shared@example.com",
		Role:        common.RoleCommonUser,
		Status:      common.UserStatusEnabled,
	}
	if err := existing.Insert(0); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	req := &OAuthFinalizeRequest{
		Provider:       "google",
		ProviderUserId: "google-link-test",
		Email:          "shared@example.com",
		Name:           "Shared",
	}
	u, err := findOrCreateOAuthUser(model.DB, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if u.Id != existing.Id {
		t.Errorf("expected to link to existing user %d, got %d", existing.Id, u.Id)
	}
	var n int64
	model.DB.Model(&model.User{}).Count(&n)
	if n != 1 {
		t.Errorf("expected 1 user, got %d (duplicate-create regression)", n)
	}
	acc, hit, err := FindOAuthAccount(model.DB, "google", "google-link-test")
	if err != nil || !hit {
		t.Fatalf("no linkage row (hit=%v err=%v)", hit, err)
	}
	if acc.UserId != existing.Id {
		t.Errorf("linkage user_id wrong: want %d got %d", existing.Id, acc.UserId)
	}
}

func TestFindOrCreate_PasswordUserRefusesAutoLink(t *testing.T) {
	setupTestDB(t)
	// Account-merge defense: a user with a password and no prior OAuth must
	// NOT be silently linked. Attacker scenario: someone gets a provider to
	// verify victim@example.com and tries to take over a password account.
	existing := &model.User{
		Username:    "passworduser",
		Password:    "fake-hash-not-used", // has a password
		DisplayName: "Password User",
		Email:       "victim@example.com",
		Role:        common.RoleCommonUser,
		Status:      common.UserStatusEnabled,
	}
	if err := existing.Insert(0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := &OAuthFinalizeRequest{
		Provider:       "google",
		ProviderUserId: "attacker-sub-1",
		Email:          "victim@example.com",
		Name:           "Attacker",
	}
	_, err := findOrCreateOAuthUser(model.DB, req)
	if !errors.Is(err, errPasswordAccountLoginRequired) {
		t.Errorf("expected errPasswordAccountLoginRequired, got %v", err)
	}
	// Linkage row must NOT exist.
	_, hit, _ := FindOAuthAccount(model.DB, "google", "attacker-sub-1")
	if hit {
		t.Error("attacker's OAuth identity should not be linked")
	}
}

// ---------- ensureAccessToken ----------

func TestEnsureAccessToken_GeneratesOnFirstCall(t *testing.T) {
	setupTestDB(t)
	u := &model.User{
		Username: "tokuser",
		Password: "x",
		Email:    "tok@example.com",
		Role:     common.RoleCommonUser,
		Status:   common.UserStatusEnabled,
	}
	if err := u.Insert(0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	tok1, err := ensureAccessToken(u)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if tok1 == "" {
		t.Fatalf("empty token")
	}
	tok2, err := ensureAccessToken(u)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if tok1 != tok2 {
		t.Errorf("token rotated unexpectedly: %q vs %q", tok1, tok2)
	}
}

// ---------- HTTP handler ----------

func TestHandler_DevModeAcceptsWithoutSecret(t *testing.T) {
	setupTestDB(t)
	withDevMode(t, func() {
		w, body := makeRequest(t, OAuthFinalizeRequest{
			Provider:       "google",
			ProviderUserId: "h-dev-1",
			Email:          "h1@example.com",
			Name:           "H1",
		}, nil)
		if w.Code != 200 {
			t.Fatalf("status: want 200 got %d (body=%v)", w.Code, body)
		}
		if body["token"] == "" || body["email"] != "h1@example.com" {
			t.Errorf("bad body: %v", body)
		}
		if _, ok := body["id"].(float64); !ok {
			t.Errorf("id missing or wrong type: %v", body["id"])
		}
	})
}

func TestHandler_RejectsWithoutSecretInProd(t *testing.T) {
	setupTestDB(t)
	// Ensure dev mode and secret are unset → fail closed.
	_ = os.Unsetenv(devModeEnv)
	prev := os.Getenv(internalSecretEnv)
	_ = os.Unsetenv(internalSecretEnv)
	defer func() {
		if prev != "" {
			_ = os.Setenv(internalSecretEnv, prev)
		}
	}()
	w, body := makeRequest(t, OAuthFinalizeRequest{
		Provider:       "google",
		ProviderUserId: "h-prod-1",
		Email:          "h2@example.com",
	}, nil)
	if w.Code != 401 {
		t.Errorf("want 401 got %d body=%v", w.Code, body)
	}
}

func TestHandler_AcceptsCorrectSecret(t *testing.T) {
	setupTestDB(t)
	_ = os.Unsetenv(devModeEnv)
	if err := os.Setenv(internalSecretEnv, "test-secret-32-chars-aaaaaaaaaaaa"); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	defer os.Unsetenv(internalSecretEnv)

	w, body := makeRequest(t, OAuthFinalizeRequest{
		Provider:       "github",
		ProviderUserId: "h-ok-1",
		Email:          "h3@example.com",
	}, map[string]string{
		internalSecretHeader: "test-secret-32-chars-aaaaaaaaaaaa",
	})
	if w.Code != 200 {
		t.Errorf("want 200 got %d body=%v", w.Code, body)
	}
}

func TestHandler_RejectsWrongSecret(t *testing.T) {
	setupTestDB(t)
	_ = os.Unsetenv(devModeEnv)
	if err := os.Setenv(internalSecretEnv, "real-secret"); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	defer os.Unsetenv(internalSecretEnv)

	w, _ := makeRequest(t, OAuthFinalizeRequest{
		Provider:       "github",
		ProviderUserId: "h-wrong-1",
		Email:          "h4@example.com",
	}, map[string]string{
		internalSecretHeader: "wrong-secret",
	})
	if w.Code != 401 {
		t.Errorf("want 401 got %d", w.Code)
	}
}

func TestHandler_RejectsUnknownProvider(t *testing.T) {
	setupTestDB(t)
	withDevMode(t, func() {
		w, body := makeRequest(t, OAuthFinalizeRequest{
			Provider:       "myspace",
			ProviderUserId: "x",
			Email:          "h5@example.com",
		}, nil)
		if w.Code != 400 {
			t.Errorf("want 400 got %d body=%v", w.Code, body)
		}
	})
}

func TestHandler_RejectsMissingProviderUserId(t *testing.T) {
	setupTestDB(t)
	withDevMode(t, func() {
		w, body := makeRequest(t, map[string]any{
			"provider": "google",
			"email":    "h6@example.com",
		}, nil)
		if w.Code != 400 {
			t.Errorf("want 400 got %d body=%v", w.Code, body)
		}
	})
}

func TestFindOrCreate_ConcurrentSameProviderSub(t *testing.T) {
	setupTestDB(t)
	// SQLite serializes writes, so this is more "rapid sequential" than truly
	// concurrent. The point is to exercise path-1 short-circuit on attempt 2+.
	const N = 4
	type result struct {
		userId int
		err    error
	}
	out := make(chan result, N)
	for i := 0; i < N; i++ {
		go func() {
			req := &OAuthFinalizeRequest{
				Provider:       "google",
				ProviderUserId: "concurrent-sub-1",
				Email:          "race@example.com",
				Name:           "Race",
			}
			u, err := findOrCreateOAuthUser(model.DB, req)
			if err != nil {
				out <- result{0, err}
				return
			}
			out <- result{u.Id, nil}
		}()
	}
	seen := map[int]int{}
	for i := 0; i < N; i++ {
		r := <-out
		if r.err != nil {
			t.Errorf("goroutine %d err: %v", i, r.err)
			continue
		}
		seen[r.userId]++
	}
	if len(seen) != 1 {
		t.Errorf("expected 1 distinct user_id across %d goroutines, got %d (map=%v)", N, len(seen), seen)
	}
	var n int64
	model.DB.Model(&model.User{}).Count(&n)
	if n != 1 {
		t.Errorf("expected 1 user row in DB, got %d (concurrent double-create regression)", n)
	}
}

func TestHandler_NullBody(t *testing.T) {
	setupTestDB(t)
	withDevMode(t, func() {
		gin.SetMode(gin.TestMode)
		r := gin.New()
		api := r.Group("/api")
		RegisterRoutes(api)
		req := httptest.NewRequest(http.MethodPost, "/api/auth/oauth-finalize", strings.NewReader(""))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Errorf("want 400 got %d body=%s", w.Code, w.Body.String())
		}
	})
}

func TestHandler_NotJsonContentType(t *testing.T) {
	setupTestDB(t)
	withDevMode(t, func() {
		gin.SetMode(gin.TestMode)
		r := gin.New()
		api := r.Group("/api")
		RegisterRoutes(api)
		req := httptest.NewRequest(http.MethodPost, "/api/auth/oauth-finalize", strings.NewReader("provider=google&provider_user_id=x&email=y@z.com"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Errorf("want 400 got %d", w.Code)
		}
	})
}

func TestAuditLog_Success(t *testing.T) {
	setupTestDB(t)
	withDevMode(t, func() {
		w, _ := makeRequest(t, OAuthFinalizeRequest{
			Provider:       "google",
			ProviderUserId: "audit-ok-1",
			Email:          "audit-ok@example.com",
		}, map[string]string{"User-Agent": "AuditTestUA/1.0"})
		if w.Code != 200 {
			t.Fatalf("setup failed: %d", w.Code)
		}
		var rows []RoutifyOAuthAuditLog
		if err := model.DB.Where("provider_user_id = ?", "audit-ok-1").Find(&rows).Error; err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("want 1 audit row, got %d", len(rows))
		}
		if rows[0].Outcome != AuditOutcomeOK {
			t.Errorf("outcome: want %q got %q", AuditOutcomeOK, rows[0].Outcome)
		}
		if rows[0].UserId == 0 {
			t.Error("user_id should be set on success")
		}
		if rows[0].UserAgent != "AuditTestUA/1.0" {
			t.Errorf("user_agent not captured: %q", rows[0].UserAgent)
		}
	})
}

func TestAuditLog_AuthFailure(t *testing.T) {
	setupTestDB(t)
	_ = os.Unsetenv(devModeEnv)
	prev := os.Getenv(internalSecretEnv)
	_ = os.Unsetenv(internalSecretEnv)
	defer func() {
		if prev != "" {
			_ = os.Setenv(internalSecretEnv, prev)
		}
	}()
	w, _ := makeRequest(t, OAuthFinalizeRequest{
		Provider:       "google",
		ProviderUserId: "audit-noauth-1",
		Email:          "ax@example.com",
	}, nil)
	if w.Code != 401 {
		t.Fatalf("setup: %d", w.Code)
	}
	var rows []RoutifyOAuthAuditLog
	if err := model.DB.Where("outcome = ?", AuditOutcomeAuthFailed).Find(&rows).Error; err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) < 1 {
		t.Errorf("expected at least 1 auth_failed audit row, got %d", len(rows))
	}
}

func TestHandler_DisabledUserBlocked(t *testing.T) {
	setupTestDB(t)
	// Pre-create a disabled user, then OAuth callback with the same email.
	disabled := &model.User{
		Username: "disableduser",
		Password: "x",
		Email:    "ban@example.com",
		Role:     common.RoleCommonUser,
		Status:   common.UserStatusDisabled,
	}
	if err := disabled.Insert(0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Insert flips Status back to default in some upstream code paths; force it.
	if err := model.DB.Model(&model.User{}).Where("id = ?", disabled.Id).
		Update("status", common.UserStatusDisabled).Error; err != nil {
		t.Fatalf("force disable: %v", err)
	}

	withDevMode(t, func() {
		w, body := makeRequest(t, OAuthFinalizeRequest{
			Provider:       "google",
			ProviderUserId: "ban-sub",
			Email:          "ban@example.com",
		}, nil)
		if w.Code != 403 {
			t.Errorf("want 403 got %d body=%v", w.Code, body)
		}
	})
}
