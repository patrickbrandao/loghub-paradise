package config

import (
	"fmt"
	"sort"
	"strings"
)

// removedEnv are the environment names of the parent go-syslog-server that
// this server does not have (SPEC §1.2): other sinks and rings, TLS, the
// filter, the rate limiter and the facility-severity subjects.
//
// A removed key on the command line or in the INI is exit 1 (unknown
// parameter). In the environment it would be ignored in silence -- the
// environment is read per registry name, never scanned -- and an operator who
// swapped one server for the other, keeping SYSLOG_LISTEN_TLS or
// SYSLOG_FILTER_MIN_SEVERITY, would believe a listener or a filter exists. It
// is a WARN rather than an error because environments are shared: the same
// .env may feed both servers.
var removedEnv = []string{
	"SYSLOG_FILTER_FACILITIES",
	"SYSLOG_FILTER_MIN_SEVERITY",
	"SYSLOG_LISTEN_TLS",
	"SYSLOG_LISTEN_TLS_LISTENERS",
	"SYSLOG_RATELIMIT_BURST",
	"SYSLOG_RATELIMIT_ENABLED",
	"SYSLOG_RATELIMIT_EXEMPT",
	"SYSLOG_RATELIMIT_MAX_BUCKETS",
	"SYSLOG_RATELIMIT_RATE",
	"SYSLOG_RATELIMIT_SWEEP_INTERVAL",
	"SYSLOG_RATELIMIT_V4_MASK",
	"SYSLOG_RATELIMIT_V6_MASK",
	"SYSLOG_RING_MAX_INFLIGHT",
	"SYSLOG_RING_MODE",
	"SYSLOG_RING_REDIS_ADDRS",
	"SYSLOG_RING_REDIS_CLAIM_IDLE",
	"SYSLOG_RING_REDIS_DB",
	"SYSLOG_RING_REDIS_GROUP",
	"SYSLOG_RING_REDIS_PASSWORD",
	"SYSLOG_RING_REDIS_SENTINEL_MASTER",
	"SYSLOG_RING_REDIS_STREAM",
	"SYSLOG_RING_REDIS_TLS",
	"SYSLOG_RING_REDIS_TLS_CA",
	"SYSLOG_RING_REDIS_TLS_INSECURE",
	"SYSLOG_RING_REDIS_USERNAME",
	"SYSLOG_RING_SQLITE_BUSY_TIMEOUT",
	"SYSLOG_RING_SQLITE_COMMIT_BATCH",
	"SYSLOG_RING_SQLITE_COMMIT_MAX_DELAY",
	"SYSLOG_RING_SQLITE_PATH",
	"SYSLOG_RING_SQLITE_SYNCHRONOUS",
	"SYSLOG_RING_SQLITE_WAL_AUTOCHECKPOINT",
	"SYSLOG_SINK_CLICKHOUSE_ADDRS",
	"SYSLOG_SINK_CLICKHOUSE_DATABASE",
	"SYSLOG_SINK_CLICKHOUSE_PASSWORD",
	"SYSLOG_SINK_CLICKHOUSE_POOL_SIZE",
	"SYSLOG_SINK_CLICKHOUSE_TABLE",
	"SYSLOG_SINK_CLICKHOUSE_TLS",
	"SYSLOG_SINK_CLICKHOUSE_TLS_CA",
	"SYSLOG_SINK_CLICKHOUSE_TLS_INSECURE",
	"SYSLOG_SINK_CLICKHOUSE_USERNAME",
	"SYSLOG_SINK_MYSQL_ADDR",
	"SYSLOG_SINK_MYSQL_DATABASE",
	"SYSLOG_SINK_MYSQL_PASSWORD",
	"SYSLOG_SINK_MYSQL_POOL_SIZE",
	"SYSLOG_SINK_MYSQL_TABLE",
	"SYSLOG_SINK_MYSQL_TLS",
	"SYSLOG_SINK_MYSQL_TLS_CA",
	"SYSLOG_SINK_MYSQL_TLS_INSECURE",
	"SYSLOG_SINK_MYSQL_USERNAME",
	"SYSLOG_SINK_NATS_SUBJECTS",
	"SYSLOG_SINK_NATS_TLS",
	"SYSLOG_SINK_NATS_TLS_CA",
	"SYSLOG_SINK_NATS_TLS_INSECURE",
	"SYSLOG_SINK_POSTGRES_ADDR",
	"SYSLOG_SINK_POSTGRES_DATABASE",
	"SYSLOG_SINK_POSTGRES_PASSWORD",
	"SYSLOG_SINK_POSTGRES_POOL_SIZE",
	"SYSLOG_SINK_POSTGRES_TABLE",
	"SYSLOG_SINK_POSTGRES_TLS",
	"SYSLOG_SINK_POSTGRES_TLS_CA",
	"SYSLOG_SINK_POSTGRES_TLS_INSECURE",
	"SYSLOG_SINK_POSTGRES_USERNAME",
	"SYSLOG_SINK_REDIS_ADDRS",
	"SYSLOG_SINK_REDIS_DB",
	"SYSLOG_SINK_REDIS_MAXLEN",
	"SYSLOG_SINK_REDIS_PASSWORD",
	"SYSLOG_SINK_REDIS_PIPELINE_MULTIPLEX",
	"SYSLOG_SINK_REDIS_READ_BUFFER_EACH_CONN",
	"SYSLOG_SINK_REDIS_RING_SCALE_EACH_CONN",
	"SYSLOG_SINK_REDIS_SENTINEL_MASTER",
	"SYSLOG_SINK_REDIS_STREAM",
	"SYSLOG_SINK_REDIS_TLS",
	"SYSLOG_SINK_REDIS_TLS_CA",
	"SYSLOG_SINK_REDIS_TLS_INSECURE",
	"SYSLOG_SINK_REDIS_USERNAME",
	"SYSLOG_SINK_REDIS_WRITE_BUFFER_EACH_CONN",
	"SYSLOG_SINK_SQLITE_BUSY_TIMEOUT",
	"SYSLOG_SINK_SQLITE_KEEP_FILES",
	"SYSLOG_SINK_SQLITE_PATH",
	"SYSLOG_SINK_SQLITE_ROTATE",
	"SYSLOG_SINK_SQLITE_SYNCHRONOUS",
	"SYSLOG_SINK_SQLITE_WAL_AUTOCHECKPOINT",
	"SYSLOG_SINK_TYPE",
	"SYSLOG_TLS_BACKPRESSURE_TIMEOUT",
	"SYSLOG_TLS_CERT_FILE",
	"SYSLOG_TLS_CLIENT_AUTH",
	"SYSLOG_TLS_CLIENT_CA",
	"SYSLOG_TLS_HANDSHAKE_TIMEOUT",
	"SYSLOG_TLS_IDLE_TIMEOUT",
	"SYSLOG_TLS_KEY_FILE",
	"SYSLOG_TLS_MAX_CONNS",
	"SYSLOG_TLS_MAX_CONNS_PER_IP",
	"SYSLOG_TLS_MIN_VERSION",
}

// removedEnvWarnings names every removed variable that is set, in one line.
func removedEnvWarnings(lookup func(string) (string, bool)) []string {
	var set []string
	for _, k := range removedEnv {
		if _, ok := lookup(k); ok {
			set = append(set, k)
		}
	}
	if len(set) == 0 {
		return nil
	}
	sort.Strings(set)
	return []string{fmt.Sprintf("%s: set in the environment, but go-syslog-nats-server has no such "+
		"parameter (no TLS, no other sink or ring, no filter, rate limiter or facility-severity "+
		"subjects; SPEC §1.2). "+
		"Ignored", strings.Join(set, ", "))}
}

// removedSections are the sections of go-syslog-server that this server does
// not have at all (SPEC §1.2).
var removedSections = map[string]bool{
	"filter":          true,
	"ratelimit":       true,
	"ring.redis":      true,
	"ring.sqlite":     true,
	"sink.clickhouse": true,
	"sink.mysql":      true,
	"sink.postgres":   true,
	"sink.redis":      true,
	"sink.sqlite":     true,
	"tls":             true,
}

// removedParams are the dotted names of removedEnv: go-syslog-server's keys
// that this server does not have. On the command line or in the INI they are
// exit 1 like any unknown parameter; the error says where they come from.
// TestRemovedParamsAreTheRemovedEnv pins it to removedEnv.
var removedParams = map[string]bool{
	"filter.facilities":                 true,
	"filter.min_severity":               true,
	"listen.tls":                        true,
	"listen.tls_listeners":              true,
	"ratelimit.burst":                   true,
	"ratelimit.enabled":                 true,
	"ratelimit.exempt":                  true,
	"ratelimit.max_buckets":             true,
	"ratelimit.rate":                    true,
	"ratelimit.sweep_interval":          true,
	"ratelimit.v4_mask":                 true,
	"ratelimit.v6_mask":                 true,
	"ring.max_inflight":                 true,
	"ring.mode":                         true,
	"ring.redis.addrs":                  true,
	"ring.redis.claim_idle":             true,
	"ring.redis.db":                     true,
	"ring.redis.group":                  true,
	"ring.redis.password":               true,
	"ring.redis.sentinel_master":        true,
	"ring.redis.stream":                 true,
	"ring.redis.tls":                    true,
	"ring.redis.tls_ca":                 true,
	"ring.redis.tls_insecure":           true,
	"ring.redis.username":               true,
	"ring.sqlite.busy_timeout":          true,
	"ring.sqlite.commit_batch":          true,
	"ring.sqlite.commit_max_delay":      true,
	"ring.sqlite.path":                  true,
	"ring.sqlite.synchronous":           true,
	"ring.sqlite.wal_autocheckpoint":    true,
	"sink.clickhouse.addrs":             true,
	"sink.clickhouse.database":          true,
	"sink.clickhouse.password":          true,
	"sink.clickhouse.pool_size":         true,
	"sink.clickhouse.table":             true,
	"sink.clickhouse.tls":               true,
	"sink.clickhouse.tls_ca":            true,
	"sink.clickhouse.tls_insecure":      true,
	"sink.clickhouse.username":          true,
	"sink.mysql.addr":                   true,
	"sink.mysql.database":               true,
	"sink.mysql.password":               true,
	"sink.mysql.pool_size":              true,
	"sink.mysql.table":                  true,
	"sink.mysql.tls":                    true,
	"sink.mysql.tls_ca":                 true,
	"sink.mysql.tls_insecure":           true,
	"sink.mysql.username":               true,
	"sink.nats.subjects":                true,
	"sink.nats.tls":                     true,
	"sink.nats.tls_ca":                  true,
	"sink.nats.tls_insecure":            true,
	"sink.postgres.addr":                true,
	"sink.postgres.database":            true,
	"sink.postgres.password":            true,
	"sink.postgres.pool_size":           true,
	"sink.postgres.table":               true,
	"sink.postgres.tls":                 true,
	"sink.postgres.tls_ca":              true,
	"sink.postgres.tls_insecure":        true,
	"sink.postgres.username":            true,
	"sink.redis.addrs":                  true,
	"sink.redis.db":                     true,
	"sink.redis.maxlen":                 true,
	"sink.redis.password":               true,
	"sink.redis.pipeline_multiplex":     true,
	"sink.redis.read_buffer_each_conn":  true,
	"sink.redis.ring_scale_each_conn":   true,
	"sink.redis.sentinel_master":        true,
	"sink.redis.stream":                 true,
	"sink.redis.tls":                    true,
	"sink.redis.tls_ca":                 true,
	"sink.redis.tls_insecure":           true,
	"sink.redis.username":               true,
	"sink.redis.write_buffer_each_conn": true,
	"sink.sqlite.busy_timeout":          true,
	"sink.sqlite.keep_files":            true,
	"sink.sqlite.path":                  true,
	"sink.sqlite.rotate":                true,
	"sink.sqlite.synchronous":           true,
	"sink.sqlite.wal_autocheckpoint":    true,
	"sink.type":                         true,
	"tls.backpressure_timeout":          true,
	"tls.cert_file":                     true,
	"tls.client_auth":                   true,
	"tls.client_ca":                     true,
	"tls.handshake_timeout":             true,
	"tls.idle_timeout":                  true,
	"tls.key_file":                      true,
	"tls.max_conns":                     true,
	"tls.max_conns_per_ip":              true,
	"tls.min_version":                   true,
}
