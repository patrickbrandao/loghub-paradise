package config

import (
	"fmt"
	"sort"
	"strings"
)

// removedEnv are the environment names of go-relplog-server that this server
// does not have (SPEC §1.2): other sinks and rings, TLS (the listen.tls
// transport, [tls] and the NATS client's), the filter, the rate limiter and the
// facility-severity subjects. The list is the difference between the two
// registries, taken from the --help of go-relplog-server v1.0.0.
//
// A removed key on the command line or in the INI is exit 1 (unknown
// parameter). In the environment it would be ignored in silence -- the
// environment is read per registry name, never scanned -- and an operator who
// swapped one server for the other, keeping RELPLOG_LISTEN_TLS or
// RELPLOG_FILTER_MIN_SEVERITY, would believe a listener or a filter exists. It
// is a WARN rather than an error because environments are shared: the same
// .env may feed both servers (the rule of go-syslog-nats-server, SPEC §13.2,
// §16.2).
var removedEnv = []string{
	"RELPLOG_FILTER_FACILITIES",
	"RELPLOG_FILTER_MIN_SEVERITY",
	"RELPLOG_LISTEN_TLS",
	"RELPLOG_LISTEN_TLS_LISTENERS",
	"RELPLOG_RATELIMIT_BURST",
	"RELPLOG_RATELIMIT_ENABLED",
	"RELPLOG_RATELIMIT_EXEMPT",
	"RELPLOG_RATELIMIT_MAX_BUCKETS",
	"RELPLOG_RATELIMIT_RATE",
	"RELPLOG_RATELIMIT_SWEEP_INTERVAL",
	"RELPLOG_RATELIMIT_V4_MASK",
	"RELPLOG_RATELIMIT_V6_MASK",
	"RELPLOG_RING_MAX_INFLIGHT",
	"RELPLOG_RING_MODE",
	"RELPLOG_RING_REDIS_ADDRS",
	"RELPLOG_RING_REDIS_CLAIM_IDLE",
	"RELPLOG_RING_REDIS_DB",
	"RELPLOG_RING_REDIS_GROUP",
	"RELPLOG_RING_REDIS_PASSWORD",
	"RELPLOG_RING_REDIS_SENTINEL_MASTER",
	"RELPLOG_RING_REDIS_STREAM",
	"RELPLOG_RING_REDIS_TLS",
	"RELPLOG_RING_REDIS_TLS_CA",
	"RELPLOG_RING_REDIS_TLS_INSECURE",
	"RELPLOG_RING_REDIS_USERNAME",
	"RELPLOG_RING_SQLITE_BUSY_TIMEOUT",
	"RELPLOG_RING_SQLITE_COMMIT_BATCH",
	"RELPLOG_RING_SQLITE_COMMIT_MAX_DELAY",
	"RELPLOG_RING_SQLITE_PATH",
	"RELPLOG_RING_SQLITE_SYNCHRONOUS",
	"RELPLOG_RING_SQLITE_WAL_AUTOCHECKPOINT",
	"RELPLOG_SINK_CLICKHOUSE_ADDRS",
	"RELPLOG_SINK_CLICKHOUSE_DATABASE",
	"RELPLOG_SINK_CLICKHOUSE_PASSWORD",
	"RELPLOG_SINK_CLICKHOUSE_POOL_SIZE",
	"RELPLOG_SINK_CLICKHOUSE_TABLE",
	"RELPLOG_SINK_CLICKHOUSE_TLS",
	"RELPLOG_SINK_CLICKHOUSE_TLS_CA",
	"RELPLOG_SINK_CLICKHOUSE_TLS_INSECURE",
	"RELPLOG_SINK_CLICKHOUSE_USERNAME",
	"RELPLOG_SINK_MYSQL_ADDR",
	"RELPLOG_SINK_MYSQL_DATABASE",
	"RELPLOG_SINK_MYSQL_PASSWORD",
	"RELPLOG_SINK_MYSQL_POOL_SIZE",
	"RELPLOG_SINK_MYSQL_TABLE",
	"RELPLOG_SINK_MYSQL_TLS",
	"RELPLOG_SINK_MYSQL_TLS_CA",
	"RELPLOG_SINK_MYSQL_TLS_INSECURE",
	"RELPLOG_SINK_MYSQL_USERNAME",
	"RELPLOG_SINK_NATS_SUBJECTS",
	"RELPLOG_SINK_NATS_TLS",
	"RELPLOG_SINK_NATS_TLS_CA",
	"RELPLOG_SINK_NATS_TLS_INSECURE",
	"RELPLOG_SINK_POSTGRES_ADDR",
	"RELPLOG_SINK_POSTGRES_DATABASE",
	"RELPLOG_SINK_POSTGRES_PASSWORD",
	"RELPLOG_SINK_POSTGRES_POOL_SIZE",
	"RELPLOG_SINK_POSTGRES_TABLE",
	"RELPLOG_SINK_POSTGRES_TLS",
	"RELPLOG_SINK_POSTGRES_TLS_CA",
	"RELPLOG_SINK_POSTGRES_TLS_INSECURE",
	"RELPLOG_SINK_POSTGRES_USERNAME",
	"RELPLOG_SINK_REDIS_ADDRS",
	"RELPLOG_SINK_REDIS_DB",
	"RELPLOG_SINK_REDIS_MAXLEN",
	"RELPLOG_SINK_REDIS_PASSWORD",
	"RELPLOG_SINK_REDIS_PIPELINE_MULTIPLEX",
	"RELPLOG_SINK_REDIS_READ_BUFFER_EACH_CONN",
	"RELPLOG_SINK_REDIS_RING_SCALE_EACH_CONN",
	"RELPLOG_SINK_REDIS_SENTINEL_MASTER",
	"RELPLOG_SINK_REDIS_STREAM",
	"RELPLOG_SINK_REDIS_TLS",
	"RELPLOG_SINK_REDIS_TLS_CA",
	"RELPLOG_SINK_REDIS_TLS_INSECURE",
	"RELPLOG_SINK_REDIS_USERNAME",
	"RELPLOG_SINK_REDIS_WRITE_BUFFER_EACH_CONN",
	"RELPLOG_SINK_SQLITE_BUSY_TIMEOUT",
	"RELPLOG_SINK_SQLITE_KEEP_FILES",
	"RELPLOG_SINK_SQLITE_PATH",
	"RELPLOG_SINK_SQLITE_ROTATE",
	"RELPLOG_SINK_SQLITE_SYNCHRONOUS",
	"RELPLOG_SINK_SQLITE_WAL_AUTOCHECKPOINT",
	"RELPLOG_SINK_TYPE",
	"RELPLOG_TLS_CERT_FILE",
	"RELPLOG_TLS_CLIENT_AUTH",
	"RELPLOG_TLS_CLIENT_CA",
	"RELPLOG_TLS_HANDSHAKE_TIMEOUT",
	"RELPLOG_TLS_IDLE_TIMEOUT",
	"RELPLOG_TLS_KEY_FILE",
	"RELPLOG_TLS_MAX_CONNS",
	"RELPLOG_TLS_MAX_CONNS_PER_IP",
	"RELPLOG_TLS_MIN_VERSION",
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
	return []string{fmt.Sprintf("%s: set in the environment, but go-relplog-nats-server has no such "+
		"parameter (no TLS, no other sink or ring, no filter, rate limiter or facility-severity "+
		"subjects; SPEC §1.2). "+
		"Ignored", strings.Join(set, ", "))}
}

// removedSections are the sections of go-relplog-server that this server does
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

// removedParams are the dotted names of removedEnv: go-relplog-server's keys
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
