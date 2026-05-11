// Package routify · POST /api/auth/oauth-finalize
//
// Service-to-service endpoint called by routify-web AFTER it has exchanged
// the OAuth `code` with the provider and obtained a verified profile.
//
// Contract (matches apps/web/app/api/auth/oauth/[provider]/callback/route.ts):
//
//	Request:  { provider, provider_user_id, email, name, avatar }
//	Response: { id, email, token }   on success (HTTP 200)
//	          { error: string }       on failure (4xx / 5xx)
//
// `token` is the upstream user.access_token, which routify-web stores in the
// `routify-session` cookie. Subsequent gateway calls authenticate via the
// existing `Authorization` + `New-Api-User` header convention (see
// middleware/auth.go::authHelper).
//
// Auth: shared-secret header `X-Routify-Internal: <secret>` must match the
// gateway's ROUTIFY_INTERNAL_SECRET env var. Required in non-dev mode.
//
// Find-or-create flow (in a single transaction):
//  1. (provider, provider_user_id) hit  → return that user
//  2. email hit                          → link account to that user, return
//  3. fresh user                         → insert user + link account, return

package routify

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// OAuthFinalizeRequest is the JSON body sent by routify-web's callback.
type OAuthFinalizeRequest struct {
	Provider       string `json:"provider" binding:"required"`
	ProviderUserId string `json:"provider_user_id" binding:"required"`
	Email          string `json:"email"` // may be empty for some providers
	Name           string `json:"name"`
	Avatar         string `json:"avatar"`
}

// OAuthFinalizeResponse is the JSON body returned to routify-web.
type OAuthFinalizeResponse struct {
	Id    int    `json:"id"`
	Email string `json:"email"`
	Token string `json:"token"`
}

const (
	internalSecretHeader = "X-Routify-Internal"
	internalSecretEnv    = "ROUTIFY_INTERNAL_SECRET"
	devModeEnv           = "ROUTIFY_DEV_MODE" // when "1", skip secret check (local only)

	maxUsernameLen = 20 // matches model.User Username `validate:"max=20"`
	usernamePrefix = "ox_"
)

// supportedProviders gates which providers we accept. Add here as we add
// callback handlers in routify-web.
var supportedProviders = map[string]bool{
	"google": true,
	"github": true,
	// "wechat": true, // pending business license
}

// OAuthFinalizeHandler implements POST /api/auth/oauth-finalize.
//
// Defensive: every error path returns JSON with "error" field so routify-web
// can show a useful message at /login?error=... .
func OAuthFinalizeHandler(c *gin.Context) {
	if !checkInternalSecret(c) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "internal secret invalid"})
		return
	}

	var req OAuthFinalizeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	req.Provider = strings.ToLower(strings.TrimSpace(req.Provider))
	req.ProviderUserId = strings.TrimSpace(req.ProviderUserId)
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))

	if !supportedProviders[req.Provider] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported provider: " + req.Provider})
		return
	}
	if req.ProviderUserId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "provider_user_id required"})
		return
	}

	user, err := findOrCreateOAuthUser(model.DB, &req)
	if err != nil {
		if errors.Is(err, errAccountDisabled) {
			c.JSON(http.StatusForbidden, gin.H{"error": "account disabled"})
			return
		}
		// Log full error server-side; return generic message to caller to avoid
		// leaking schema / SQL fragments via /login?error=... reflection.
		common.SysError(fmt.Sprintf("[routify] oauth-finalize failed: provider=%s err=%v", req.Provider, err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	// Path-1 fallback: linkage existed, but admin disabled the account after.
	if user.Status == common.UserStatusDisabled {
		c.JSON(http.StatusForbidden, gin.H{"error": "account disabled"})
		return
	}

	token, err := ensureAccessToken(user)
	if err != nil {
		common.SysError(fmt.Sprintf("[routify] ensureAccessToken failed: user=%d err=%v", user.Id, err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "token generation failed"})
		return
	}

	model.UpdateUserLastLoginAt(user.Id)

	c.JSON(http.StatusOK, OAuthFinalizeResponse{
		Id:    user.Id,
		Email: user.Email,
		Token: token,
	})
}

// findOrCreateOAuthUser implements the 3-path find-or-create flow inside a
// transaction so concurrent callbacks don't double-create users. Outer retry
// re-runs the whole flow when a unique-constraint violation hints that a
// concurrent caller won the race (path-1 will hit on re-run).
func findOrCreateOAuthUser(db *gorm.DB, req *OAuthFinalizeRequest) (*model.User, error) {
	if db == nil {
		return nil, errors.New("db not initialized")
	}
	const maxAttempts = 8
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		user, err := tryFindOrCreate(db, req)
		if err == nil {
			return user, nil
		}
		lastErr = err
		// Retry on race-lost signals: unique-constraint violation (real DB)
		// or transient SQLite write-lock (test + small-deploy SQLite).
		if !isUniqueConstraintErr(err) && !isLockErr(err) {
			return nil, err
		}
		common.SysLog(fmt.Sprintf("[routify] oauth-finalize race (attempt %d/%d): %v", attempt, maxAttempts, err))
	}
	return nil, fmt.Errorf("exceeded %d attempts: %w", maxAttempts, lastErr)
}

func tryFindOrCreate(db *gorm.DB, req *OAuthFinalizeRequest) (*model.User, error) {
	var resultUser *model.User
	err := db.Transaction(func(tx *gorm.DB) error {
		// Path 1: existing OAuth account → re-fetch user
		acc, hit, err := FindOAuthAccount(tx, req.Provider, req.ProviderUserId)
		if err != nil {
			return err
		}
		if hit {
			u, err := getUserByIdTx(tx, acc.UserId)
			if err != nil {
				return err
			}
			if u == nil || u.Id == 0 {
				return errors.New("oauth account points to deleted user")
			}
			resultUser = u
			return nil
		}

		// Path 2: existing user with this email → link
		if req.Email != "" {
			u, err := getUserByEmailTx(tx, req.Email)
			if err != nil {
				return err
			}
			if u != nil && u.Id != 0 {
				// Block linkage to disabled accounts BEFORE writing the linkage row,
				// so attackers cannot probe "is this email registered (banned)?"
				if u.Status == common.UserStatusDisabled {
					return errAccountDisabled
				}
				if err := LinkOAuthAccount(tx, u.Id, req.Provider, req.ProviderUserId, req.Email, req.Name, req.Avatar); err != nil {
					return err
				}
				resultUser = u
				return nil
			}
		}

		// Path 3: create fresh user. Username collisions are rare but real, so
		// retry locally on collision (cheap — pre-insert) before bubbling.
		var newUser *model.User
		var insertErr error
		for tries := 0; tries < 5; tries++ {
			newUser, insertErr = newOAuthUser(req)
			if insertErr != nil {
				return insertErr
			}
			insertErr = newUser.InsertWithTx(tx, 0)
			if insertErr == nil {
				break
			}
			if !isUniqueConstraintErr(insertErr) {
				return insertErr
			}
			// Username collided with a concurrent insert; regenerate and retry.
		}
		if insertErr != nil {
			return insertErr
		}
		if newUser.Id == 0 {
			// Refetch by username (unique).
			var fetched model.User
			if err := tx.Where("username = ?", newUser.Username).First(&fetched).Error; err != nil {
				return fmt.Errorf("post-insert fetch failed: %w", err)
			}
			newUser.Id = fetched.Id
		}
		if err := LinkOAuthAccount(tx, newUser.Id, req.Provider, req.ProviderUserId, req.Email, req.Name, req.Avatar); err != nil {
			return err
		}
		resultUser = newUser
		return nil
	})
	if err != nil {
		return nil, err
	}
	if resultUser == nil {
		return nil, errors.New("internal: no user produced by transaction")
	}
	return resultUser, nil
}

// errAccountDisabled is returned when a request would otherwise grant a
// session to an admin-disabled account.
var errAccountDisabled = errors.New("account disabled")

// isLockErr identifies transient busy/lock errors worth retrying. SQLite
// reports SQLITE_BUSY on contended writes; production PG/MySQL rarely raise
// this (true tx isolation), so the retry path is mostly for SQLite deploys
// and tests.
func isLockErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") || // SQLite
		strings.Contains(msg, "sqlite_busy") ||
		strings.Contains(msg, "could not serialize access") // PG SERIALIZABLE
}

// isUniqueConstraintErr matches the various wordings each driver uses for a
// uniqueness violation, since GORM/driver layers don't expose a portable
// sentinel. Cheap string match — false-positive cost is one extra retry.
func isUniqueConstraintErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") || // SQLite, PostgreSQL
		strings.Contains(msg, "duplicate entry") || // MySQL
		strings.Contains(msg, "duplicate key") || // PostgreSQL
		strings.Contains(msg, "uniqueviolation")
}

// newOAuthUser constructs an in-memory User with a stable, unique username
// and a random password (never used — OAuth-only login). Caller must Insert.
func newOAuthUser(req *OAuthFinalizeRequest) (*model.User, error) {
	username, err := generateOAuthUsername(req.Provider)
	if err != nil {
		return nil, err
	}
	display := req.Name
	if display == "" {
		display = req.Provider + " user"
	}
	if len(display) > maxUsernameLen {
		display = display[:maxUsernameLen]
	}
	// Random password the user will never see — they cannot log in via password.
	pw, err := common.GenerateRandomKey(32)
	if err != nil {
		return nil, err
	}
	return &model.User{
		Username:    username,
		Password:    pw,
		DisplayName: display,
		Email:       req.Email,
		Role:        common.RoleCommonUser,
		Status:      common.UserStatusEnabled,
	}, nil
}

// generateOAuthUsername returns a unique username ≤ 20 chars matching
// model.User's `validate:"max=20"`. Format: `ox_<provider:6>_<rand:lower>`
// e.g. `ox_google_a3f9k1`. Provider is truncated to 6 chars.
func generateOAuthUsername(provider string) (string, error) {
	prov := strings.ToLower(provider)
	if len(prov) > 6 {
		prov = prov[:6]
	}
	// remaining = 20 - len(prefix=3) - len(prov) - len("_"=1)
	// for provider=google (6): remaining = 20 - 3 - 6 - 1 = 10
	// for provider=github (6): remaining = 10
	remaining := maxUsernameLen - len(usernamePrefix) - len(prov) - 1
	if remaining < 4 {
		remaining = 4
	}
	if remaining > 12 {
		remaining = 12
	}
	suffix, err := common.GenerateRandomKey(remaining)
	if err != nil {
		return "", err
	}
	suffix = strings.ToLower(suffix)
	if len(suffix) > remaining {
		suffix = suffix[:remaining]
	}
	return usernamePrefix + prov + "_" + suffix, nil
}

// ensureAccessToken returns a non-empty access_token for the user, generating
// one if none exists. Uses a conditional UPDATE so two concurrent OAuth
// callbacks for the same user can't both write a fresh token (last-writer-wins
// would invalidate the first caller's session immediately).
func ensureAccessToken(user *model.User) (string, error) {
	if t := user.GetAccessToken(); t != "" {
		return t, nil
	}
	if model.DB == nil {
		return "", errors.New("db not initialized")
	}
	for tries := 0; tries < 3; tries++ {
		fresh, err := model.GetUserById(user.Id, true)
		if err != nil {
			return "", err
		}
		if t := fresh.GetAccessToken(); t != "" {
			user.SetAccessToken(t)
			return t, nil
		}
		key, err := common.GenerateRandomKey(32)
		if err != nil {
			return "", err
		}
		// Conditional update: only set access_token if it is still NULL/empty.
		res := model.DB.Model(&model.User{}).
			Where("id = ? AND (access_token IS NULL OR access_token = '')", user.Id).
			Update("access_token", key)
		if res.Error != nil {
			return "", res.Error
		}
		if res.RowsAffected == 1 {
			user.SetAccessToken(key)
			return key, nil
		}
		// 0 rows: someone else won — re-read on next loop iteration.
	}
	return "", errors.New("ensureAccessToken: token contention exceeded retry budget")
}

// checkInternalSecret enforces the X-Routify-Internal shared secret unless
// dev mode is on. In production the env var must be set; missing env var
// triggers fail-closed behavior to prevent accidental "auth-less" deploys.
func checkInternalSecret(c *gin.Context) bool {
	if os.Getenv(devModeEnv) == "1" {
		return true
	}
	expected := os.Getenv(internalSecretEnv)
	if expected == "" {
		// Fail closed: never accept the endpoint without a configured secret.
		common.SysError("[routify] " + internalSecretEnv + " is not set; rejecting oauth-finalize")
		return false
	}
	got := c.GetHeader(internalSecretHeader)
	if got == "" {
		return false
	}
	return constantTimeEq(got, expected)
}

// constantTimeEq compares strings in constant time to defeat timing attacks.
func constantTimeEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

// getUserByIdTx is a tx-aware variant of model.GetUserById (which uses the
// global model.DB). Uses model.User struct to stay consistent.
func getUserByIdTx(tx *gorm.DB, id int) (*model.User, error) {
	if id == 0 {
		return nil, errors.New("id 0")
	}
	var u model.User
	err := tx.First(&u, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// getUserByEmailTx looks up a user by email within the given tx. Empty Id
// in the returned struct means "not found". Uses LOWER(email) so that
// PostgreSQL (case-sensitive by default) finds historical rows that may
// have been stored with mixed case before email normalization landed.
func getUserByEmailTx(tx *gorm.DB, email string) (*model.User, error) {
	if email == "" {
		return nil, nil
	}
	var u model.User
	err := tx.Where("LOWER(email) = ?", strings.ToLower(email)).First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}
