// Package routify · runtime registrar.
//
// Init() must be called from main.go's InitResources() before any HTTP
// traffic is served. It wires our smart router and billing extensions
// into New-API's service layer via global function pointers.
//
// Upstream patch surface (minimal — kept to single-line hooks so that
// `git merge upstream/main` rarely conflicts):
//
//   1. main.go::InitResources()  — add `routify.Init()` near top
//   2. service/cache.go::CacheGetRandomSatisfiedChannel
//      — wrap with `if routify.SelectorOverride != nil { return routify.SelectorOverride(...) }`
//   3. controller/relay.go::computePostQuota  (or wherever quota is finalised)
//      — wrap with `if routify.QuotaOverride != nil { return routify.QuotaOverride(...) }`
//
// Anything more invasive belongs inside this package, not in upstream files.

package routify

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/QuantumNous/new-api/common"

	"github.com/gin-gonic/gin"
)

// SelectorOverride, when non-nil, replaces the default channel selection
// logic in service.CacheGetRandomSatisfiedChannel. The integration adapter
// loads channel state from upstream's model.Channel and feeds RouteRequest.
//
// Returning (0, nil) signals "no override; fall back to upstream selector"
// — used during gradual rollout when only a subset of (group, model) pairs
// have been migrated.
var SelectorOverride func(ctx context.Context, group, modelName string, userID uint64) (channelID uint32, err error)

// QuotaOverride, when non-nil, replaces the default quota calculation
// before it is debited from the user account.
var QuotaOverride func(ctx context.Context, modelName, group string, promptTokens, completionTokens uint32) (costRMB float64, err error)

var (
	initOnce sync.Once
	router   *Router
	enabled  bool
)

// Init wires Routify overlays into the running process. Idempotent.
//
// Disable via environment: ROUTIFY_OVERLAY=off (keeps upstream behavior;
// useful for A/B comparison and emergency rollback).
func Init() {
	initOnce.Do(func() {
		if os.Getenv("ROUTIFY_OVERLAY") == "off" {
			fmt.Println("[routify] overlay disabled via ROUTIFY_OVERLAY=off")
			return
		}

		// Stage B (next session): load channels.yaml from /data/routify/ and
		// hydrate from model.GetEnabledChannels(). For now NewRouter([]) gives
		// us an empty registry and the override returns (0, nil) → fallback.
		router = NewRouter(nil)

		SelectorOverride = func(ctx context.Context, group, modelName string, userID uint64) (uint32, error) {
			req := RouteRequest{
				Model: modelName,
				User:  User{ID: userID, Group: group},
			}
			decision, err := router.SelectChannel(ctx, req)
			if err != nil {
				// Empty router or no candidates → tell caller to fall back to
				// upstream's CacheGetRandomSatisfiedChannel.
				return 0, nil
			}
			if decision.Channel == nil {
				return 0, nil
			}
			return decision.Channel.ID, nil
		}

		// Quota override stays nil until pricing/Reserver are wired (Stage C).
		// Until then upstream billing applies.

		// Migrate routify-owned side tables (oauth_accounts, etc).
		if err := migrateOAuthAccountTable(); err != nil {
			common.SysError("[routify] failed to migrate routify_oauth_accounts: " + err.Error())
		}

		enabled = true
		fmt.Println("[routify] overlay active (selector ready, quota=upstream)")
	})
}

// RegisterRoutes mounts routify-owned HTTP endpoints onto the upstream
// `/api` router group. Called from router/api-router.go with a single line
// to keep the upstream patch surface minimal.
func RegisterRoutes(apiRouter *gin.RouterGroup) {
	if apiRouter == nil {
		return
	}
	auth := apiRouter.Group("/auth")
	{
		auth.POST("/oauth-finalize", OAuthFinalizeHandler)
	}
}

// IsEnabled reports whether the overlay is wired in. Useful for smoke tests
// and admin diagnostic endpoints.
func IsEnabled() bool { return enabled }
