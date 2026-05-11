// Package routify · OAuth audit log.
//
// Records every oauth-finalize attempt (success or failure) for forensics.
// Lives in the same DB as the rest of the routify_* side tables; can be
// migrated to ClickHouse later by switching the writer behind a Sink
// interface (see TODO at top of WriteAuditLog).

package routify

import (
	"errors"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"

	"gorm.io/gorm"
)

// RoutifyOAuthAuditLog is one finalize attempt.
type RoutifyOAuthAuditLog struct {
	ID             int64  `gorm:"primaryKey;autoIncrement"`
	Provider       string `gorm:"type:varchar(32);column:provider;index"`
	ProviderUserId string `gorm:"type:varchar(128);column:provider_user_id;index"`
	Email          string `gorm:"type:varchar(255);column:email;index"`
	UserId         int    `gorm:"index;column:user_id"` // 0 if request was rejected before user resolution
	Outcome        string `gorm:"type:varchar(32);column:outcome;index"`
	ErrorMessage   string `gorm:"type:varchar(512);column:error_message"`
	IPAddress      string `gorm:"type:varchar(64);column:ip_address;index"`
	UserAgent      string `gorm:"type:varchar(512);column:user_agent"`
	CreatedAt      int64  `gorm:"autoCreateTime;column:created_at;index"`
}

func (RoutifyOAuthAuditLog) TableName() string { return "routify_oauth_audit_logs" }

// Outcome constants — keep terse; an analytics SELECT will use these.
const (
	AuditOutcomeOK             = "ok"
	AuditOutcomeBadRequest     = "bad_request"
	AuditOutcomeAuthFailed     = "auth_failed"
	AuditOutcomeAccountBanned  = "account_banned"
	AuditOutcomeUnsupported    = "unsupported_provider"
	AuditOutcomeInternalError  = "internal_error"
	AuditOutcomeTokenIssueFail = "token_issue_failed"
)

// WriteAuditLog inserts one audit row. Errors are logged but never propagate
// to the caller — audit logging must never block a successful (or failed)
// login response from being delivered.
//
// TODO: when ClickHouse becomes available on VPS, swap this for a Sink
// interface (DB-write fallback + ClickHouse async insert).
func WriteAuditLog(row *RoutifyOAuthAuditLog) {
	if row == nil {
		return
	}
	if model.DB == nil {
		common.SysError("[routify] audit: DB nil; dropping log row")
		return
	}
	if row.CreatedAt == 0 {
		row.CreatedAt = time.Now().Unix()
	}
	// Truncate over-long fields to fit varchar(512) — defensive for future
	// User-Agent strings that some clients pump full of crap.
	row.UserAgent = truncate(row.UserAgent, 512)
	row.ErrorMessage = truncate(row.ErrorMessage, 512)
	if err := model.DB.Create(row).Error; err != nil {
		common.SysError("[routify] audit: " + err.Error())
	}
}

// migrateOAuthAuditLogTable creates routify_oauth_audit_logs if not exists.
// Idempotent. Same lifecycle as migrateOAuthAccountTable.
func migrateOAuthAuditLogTable() error {
	if model.DB == nil {
		return errors.New("model.DB not initialized")
	}
	return model.DB.AutoMigrate(&RoutifyOAuthAuditLog{})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// touch silences linter if gorm import becomes unused after a refactor.
var _ = gorm.ErrRecordNotFound
