// Package routify · OAuth account linkage model.
//
// Maintains a side-table `routify_oauth_accounts` mapping
// (provider, provider_user_id) → upstream model.User.Id, so that we can
// support providers that upstream new-api does not have a dedicated column
// for (e.g. Google) without touching upstream user schema (Rule 5).
//
// AutoMigrated by routify.Init() against model.DB. Keeps the overlay
// principle: upstream tables stay untouched.

package routify

import (
	"errors"
	"time"

	"github.com/QuantumNous/new-api/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// RoutifyOAuthAccount links one external OAuth identity to one upstream user.
type RoutifyOAuthAccount struct {
	ID             int64  `gorm:"primaryKey;autoIncrement"`
	UserId         int    `gorm:"index;not null;column:user_id"`
	Provider       string `gorm:"type:varchar(32);not null;column:provider;uniqueIndex:idx_routify_oauth_provider_subject"`
	ProviderUserId string `gorm:"type:varchar(128);not null;column:provider_user_id;uniqueIndex:idx_routify_oauth_provider_subject"`
	Email          string `gorm:"type:varchar(255);column:email;index"`
	Name           string `gorm:"type:varchar(128);column:name"`
	Avatar         string `gorm:"type:varchar(512);column:avatar"`
	CreatedAt      int64  `gorm:"autoCreateTime;column:created_at"`
	UpdatedAt      int64  `gorm:"autoUpdateTime;column:updated_at"`
}

// TableName overrides GORM's default pluralization. Keeps a clear
// `routify_*` namespace so it is obvious which tables belong to the overlay.
func (RoutifyOAuthAccount) TableName() string { return "routify_oauth_accounts" }

// FindOAuthAccount returns (account, true, nil) on hit,
// (nil, false, nil) on clean miss, (nil, false, err) on real DB error.
func FindOAuthAccount(db *gorm.DB, provider, providerUserId string) (*RoutifyOAuthAccount, bool, error) {
	if db == nil {
		return nil, false, errors.New("nil db")
	}
	if provider == "" || providerUserId == "" {
		return nil, false, errors.New("provider and provider_user_id required")
	}
	var acc RoutifyOAuthAccount
	err := db.Where("provider = ? AND provider_user_id = ?", provider, providerUserId).First(&acc).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &acc, true, nil
}

// LinkOAuthAccount upserts an OAuth account linkage. Used both for fresh
// users (just-inserted) and for linking an existing email-matched user to a
// new provider. Updates email/name/avatar on conflict to keep them fresh.
func LinkOAuthAccount(db *gorm.DB, userId int, provider, providerUserId, email, name, avatar string) error {
	if db == nil {
		return errors.New("nil db")
	}
	if userId <= 0 || provider == "" || providerUserId == "" {
		return errors.New("invalid linkage args")
	}
	now := time.Now().Unix()
	row := RoutifyOAuthAccount{
		UserId:         userId,
		Provider:       provider,
		ProviderUserId: providerUserId,
		Email:          email,
		Name:           name,
		Avatar:         avatar,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	// Cross-DB upsert via GORM clause.OnConflict (works on SQLite / MySQL / PG).
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "provider"}, {Name: "provider_user_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"email", "name", "avatar", "updated_at",
		}),
	}).Create(&row).Error
}

// migrateOAuthAccountTable creates routify_oauth_accounts if not exists.
// Idempotent. Uses model.DB which is set up by upstream model.InitDB().
func migrateOAuthAccountTable() error {
	if model.DB == nil {
		return errors.New("model.DB not initialized — call routify.Init() AFTER model.InitDB()")
	}
	return model.DB.AutoMigrate(&RoutifyOAuthAccount{})
}
