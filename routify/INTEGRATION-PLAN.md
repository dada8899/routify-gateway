# Routify Overlay · Integration Plan

> **Status**: Stage A complete. Stage B/C blocked on docker env (need to run inside the multi-stage Dockerfile that builds bun frontends).

---

## Stage A — Code overlay (done 2026-05-11)

- [x] `data/routify/` configs copied (channels.yaml, rate-limits.yaml, risk-rules.yaml, model_pricing.json)
- [x] `routify/` Go package copied (smart_router + billing_extensions + tests)
- [x] `web/public/routify-brand/` brand assets copied (logos + strings.json)
- [x] `routify/init.go` runtime registrar written (SelectorOverride + QuotaOverride globals)
- [x] `go test ./routify/` passes (23 tests, 0.4s)
- [x] `go vet ./routify/` clean

---

## Stage B — Wire upstream patch points (next session)

Three single-line patches into upstream files. Keep them minimal so weekly `git merge upstream/main` rarely conflicts.

### B.1 main.go — call Init() at boot

```go
// In InitResources(), near the top after common.Init() ~ line 60
import _ "github.com/QuantumNous/new-api/routify"  // OR explicit import

routify.Init()  // single line, idempotent
```

### B.2 service/cache.go — selector hook

Find `CacheGetRandomSatisfiedChannel(retryParam *RetryParam)` and wrap:

```go
func CacheGetRandomSatisfiedChannel(retryParam *RetryParam) (*model.Channel, string, error) {
    // ── routify overlay ──────────────────────────────────────────
    if routify.SelectorOverride != nil {
        chID, err := routify.SelectorOverride(
            context.Background(),
            retryParam.Group,
            retryParam.OriginModelName,
            uint64(retryParam.UserID),
        )
        if err == nil && chID > 0 {
            ch, _ := model.GetChannelByID(int(chID))  // existing helper
            if ch != nil {
                return ch, retryParam.Group, nil
            }
        }
        // chID==0 ⇒ overlay declined ⇒ fall through to upstream logic
    }
    // ── end overlay ──────────────────────────────────────────────

    // ... rest of original implementation untouched ...
}
```

### B.3 controller/relay.go — quota hook (optional, can defer to Stage C)

Wrap the quota debit point with `if routify.QuotaOverride != nil { ... }`.

---

## Stage C — Docker build + e2e curl (VPS session)

```bash
# On VPS (43.156.233.71) after pushing this branch:
cd /root/Projects/routify-gateway
docker compose -f docker-compose.dev.yml up -d --build new-api postgres redis
# Wait ~5 min for first build (bun installs + go compile)

# Smoke 1: gateway alive
curl -s http://127.0.0.1:3000/api/status | jq .

# Smoke 2: routify overlay loaded (need an admin endpoint we add)
docker logs new-api-dev 2>&1 | grep "routify"
# Expected: [routify] overlay active (selector ready, quota=upstream)

# Smoke 3: provision a channel pointing at chat.b.ai
# (use the admin UI on :3000, OR curl POST /api/channel/ with the testkey)

# Smoke 4: route a real chat completion
curl -s -X POST http://127.0.0.1:3000/v1/chat/completions \
  -H "Authorization: Bearer <user-issued-routify-key>" \
  -H "Content-Type: application/json" \
  -d '{"model":"kimi-k2.5","messages":[{"role":"user","content":"hi"}]}'
```

Then nginx reverse proxy: `gateway.routify.bytedance.city` → `:3000`.

---

## Rollback / disable

`ROUTIFY_OVERLAY=off` env var bypasses the overlay entirely. Upstream behavior is preserved. Useful for A/B comparison or emergency disable without re-deploy.

---

## Files touched (overlay only — never modified upstream files)

```
routify/                                 ← new package
├── init.go                              ← runtime registrar (this session)
├── smart_router.go + smart_router_test.go
├── billing_extensions.go + tests
└── INTEGRATION-PLAN.md (this file)

data/routify/*.{yaml,json}               ← runtime configs
web/public/routify-brand/*.svg           ← brand assets
```

Stage B will *patch* `main.go` + `service/cache.go` (~5 lines each). Those are the only upstream files we touch.
