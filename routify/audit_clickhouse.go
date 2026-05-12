// Package routify · ClickHouse sink for OAuth audit log.
//
// Optional. Activated by `CLICKHOUSE_URL` env var. The relational DB write
// (see oauth_audit_log_model.go) remains the primary, durable sink so this
// path can fail without losing forensic data; CH is treated as the analytics
// projection.
//
// Wire-up:
//   1. set CLICKHOUSE_URL=http://routify-clickhouse:8123 (and optionally
//      CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
//   2. routify.Init() reads env, pings CH, creates the table if missing
//   3. WriteAuditLog now fires an async goroutine to insert one row
//
// Why HTTP and not the Go driver: avoids adding a new top-level Go dep to
// the upstream new-api module — keeps `go.mod` diff small for upstream
// merges. ClickHouse's HTTP interface accepts JSONEachRow inserts directly.

package routify

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
)

type clickhouseSink struct {
	httpClient *http.Client
	endpoint   string // e.g. http://routify-clickhouse:8123/
	database   string // e.g. routify_logs
	table      string // e.g. routify_oauth_audit_logs
	user       string
	password   string
	// dropped counts CH write failures since startup; surfaced via SysError
	// at most once per minute so an outage doesn't spam logs.
	dropped      atomic.Uint64
	lastWarnedAt atomic.Int64
}

var (
	chSinkRef   atomic.Pointer[clickhouseSink]
	chInitOnce  sync.Once
	chTableName = "routify_oauth_audit_logs"
)

// initClickHouseSink reads env, pings CH, creates the audit-log table if
// missing, and registers the sink. Safe to call when env is unset (no-op).
// Errors are logged but never panic — gateway must still serve when CH is
// down.
func initClickHouseSink() {
	chInitOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv("CLICKHOUSE_URL"))
		if raw == "" {
			return
		}
		// Normalize trailing slash.
		if !strings.HasSuffix(raw, "/") {
			raw += "/"
		}
		if _, err := url.Parse(raw); err != nil {
			common.SysError("[routify] CLICKHOUSE_URL invalid: " + err.Error())
			return
		}
		db := strings.TrimSpace(os.Getenv("CLICKHOUSE_DB"))
		if db == "" {
			db = "routify_logs"
		}
		s := &clickhouseSink{
			httpClient: &http.Client{Timeout: 4 * time.Second},
			endpoint:   raw,
			database:   db,
			table:      chTableName,
			user:       os.Getenv("CLICKHOUSE_USER"),
			password:   os.Getenv("CLICKHOUSE_PASSWORD"),
		}
		if err := s.ensureSchema(); err != nil {
			common.SysError("[routify] CH ensureSchema failed: " + err.Error())
			return
		}
		chSinkRef.Store(s)
		fmt.Println("[routify] ClickHouse audit sink active → " + s.endpoint + s.database + "/" + s.table)
	})
}

// ensureSchema creates the database (if missing) and the audit log table
// with a partitioning + TTL strategy that keeps disk use bounded.
func (s *clickhouseSink) ensureSchema() error {
	if err := s.execDDL("CREATE DATABASE IF NOT EXISTS " + quoteIdent(s.database)); err != nil {
		return fmt.Errorf("create db: %w", err)
	}
	ddl := "CREATE TABLE IF NOT EXISTS " + quoteIdent(s.database) + "." + quoteIdent(s.table) + ` (
  id UUID DEFAULT generateUUIDv4(),
  provider LowCardinality(String),
  provider_user_id String,
  email String,
  user_id Int32,
  outcome LowCardinality(String),
  error_message String,
  ip_address String,
  user_agent String,
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (created_at, outcome, ip_address)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 1 YEAR`
	return s.execDDL(ddl)
}

func (s *clickhouseSink) execDDL(stmt string) error {
	q := s.endpoint + "?" + url.Values{"query": []string{stmt}}.Encode()
	req, err := http.NewRequest("POST", q, nil)
	if err != nil {
		return err
	}
	if s.user != "" || s.password != "" {
		req.SetBasicAuth(s.user, s.password)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		buf := make([]byte, 512)
		n, _ := resp.Body.Read(buf)
		return fmt.Errorf("CH HTTP %d: %s", resp.StatusCode, string(buf[:n]))
	}
	return nil
}

// Write is async fire-and-forget. We swallow errors so audit failures
// never block the caller (oauth-finalize must always respond on time).
func (s *clickhouseSink) Write(row *RoutifyOAuthAuditLog) {
	if s == nil || row == nil {
		return
	}
	go s.insertOne(row)
}

func (s *clickhouseSink) insertOne(row *RoutifyOAuthAuditLog) {
	ts := row.CreatedAt
	if ts == 0 {
		ts = time.Now().Unix()
	}
	// Hand-rolled JSONEachRow line — avoids dragging encoding/json into this
	// path. Strings are JSON-escaped via common.Marshal of a per-field shim
	// to stay consistent with the project's JSON rule.
	body, err := buildJSONEachRow(row, ts)
	if err != nil {
		s.recordDrop("marshal", err)
		return
	}
	query := "INSERT INTO " + quoteIdent(s.database) + "." + quoteIdent(s.table) + " FORMAT JSONEachRow"
	u := s.endpoint + "?" + url.Values{"query": []string{query}}.Encode()
	req, err := http.NewRequest("POST", u, bytes.NewReader(body))
	if err != nil {
		s.recordDrop("new-request", err)
		return
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if s.user != "" || s.password != "" {
		req.SetBasicAuth(s.user, s.password)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.recordDrop("post", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		buf := make([]byte, 256)
		n, _ := resp.Body.Read(buf)
		s.recordDrop("http", fmt.Errorf("status=%d body=%s", resp.StatusCode, string(buf[:n])))
		return
	}
}

func (s *clickhouseSink) recordDrop(stage string, err error) {
	s.dropped.Add(1)
	now := time.Now().Unix()
	last := s.lastWarnedAt.Load()
	if now-last < 60 {
		return
	}
	if s.lastWarnedAt.CompareAndSwap(last, now) {
		common.SysError(fmt.Sprintf("[routify] CH audit drop stage=%s err=%v dropped_total=%d", stage, err, s.dropped.Load()))
	}
}

func buildJSONEachRow(row *RoutifyOAuthAuditLog, ts int64) ([]byte, error) {
	payload := struct {
		Provider       string `json:"provider"`
		ProviderUserId string `json:"provider_user_id"`
		Email          string `json:"email"`
		UserId         int    `json:"user_id"`
		Outcome        string `json:"outcome"`
		ErrorMessage   string `json:"error_message"`
		IPAddress      string `json:"ip_address"`
		UserAgent      string `json:"user_agent"`
		CreatedAt      string `json:"created_at"`
	}{
		Provider:       row.Provider,
		ProviderUserId: row.ProviderUserId,
		Email:          row.Email,
		UserId:         row.UserId,
		Outcome:        row.Outcome,
		ErrorMessage:   row.ErrorMessage,
		IPAddress:      row.IPAddress,
		UserAgent:      row.UserAgent,
		CreatedAt:      time.Unix(ts, 0).UTC().Format("2006-01-02 15:04:05"),
	}
	out, err := common.Marshal(payload)
	if err != nil {
		return nil, err
	}
	// JSONEachRow expects newline-delimited rows.
	out = append(out, '\n')
	return out, nil
}

// quoteIdent backtick-quotes a ClickHouse identifier. CH's preferred quote
// is backtick for table/db identifiers; we don't permit user-supplied
// values here so escaping is trivial (no embedded backticks).
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
