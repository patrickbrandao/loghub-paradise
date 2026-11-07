#!/bin/sh
# test-redis-valkey.sh -- the Redis suites of S, R, G, W and F against Redis
# and every Valkey version the monorepo declares (rodada Valkey de 2026-11-07;
# go-syslog-server SPEC §10.3.1, docs/TESTING.md §6.1a of each module).
#
# For each image it starts ONE pair of servers -- a normal one (noeviction)
# and an allkeys-lru twin for the eviction refusal -- runs the modules IN
# SERIES against it (the suites use fixed ports such as G's :12202, and two
# modules or two images in parallel on one machine give false FAILs), and
# removes the pair before the next image.
#
# Usage (from anywhere; Docker and Go required):
#
#   scripts/test-redis-valkey.sh                 # the whole matrix
#   RV_IMAGES="valkey/valkey:9.0-alpine" RV_MODULES="go-syslog-server" scripts/test-redis-valkey.sh
#
# Environment:
#   RV_IMAGES    images to test (default: redis:7.4-alpine and valkey/valkey:{7.2,8.1,9.0}-alpine)
#   RV_MODULES   modules to test (default: the five with a Redis suite)
#   RV_PORT      host port of the normal server; the LRU twin takes RV_PORT+1 (default 36379)
#   RV_PREFIX    container name prefix (default lhp-rv)
#   RV_RACE      "-race" (default) or "" to run without the race detector
#   RV_TIMEOUT   go test -timeout (default 1800s)
#
# Exit status: 0 when every module passed against every image, 1 otherwise.
# The log of each run is under ${TMPDIR:-/tmp}/test-redis-valkey/<image>/<module>.log.

set -u
root=$(cd "$(dirname "$0")/.." && pwd)
images=${RV_IMAGES:-"redis:7.4-alpine valkey/valkey:7.2-alpine valkey/valkey:8.1-alpine valkey/valkey:9.0-alpine"}
modules=${RV_MODULES:-"go-syslog-server go-relplog-server go-gelf-server go-webhook-server go-webhook-fast-server"}
port=${RV_PORT:-36379}
lru=$((port + 1))
prefix=${RV_PREFIX:-lhp-rv}
race=${RV_RACE--race}
timeout=${RV_TIMEOUT:-1800s}
logs=${TMPDIR:-/tmp}/test-redis-valkey

# env prefix and packages of each module
envprefix() {
	case $1 in
	go-syslog-server) echo SYSLOG ;;
	go-relplog-server) echo RELPLOG ;;
	go-gelf-server) echo GELF ;;
	go-webhook-server | go-webhook-fast-server) echo WHS ;;
	*) echo "unknown module $1" >&2; exit 1 ;;
	esac
}
packages() {
	case $1 in
	go-webhook-server | go-webhook-fast-server) echo "./internal/sink/ ./internal/app/" ;;
	*) echo "./internal/rediscli/ ./internal/ring/ ./internal/sink/ ./internal/app/" ;;
	esac
}

# valkey-server for Valkey images (its entrypoint drops privileges only for
# that name), redis-server otherwise.
server_bin() {
	case $1 in
	*valkey*) echo valkey-server ;;
	*) echo redis-server ;;
	esac
}
cli_bin() {
	case $1 in
	*valkey*) echo valkey-cli ;;
	*) echo redis-cli ;;
	esac
}

stop_pair() { docker rm -f "$prefix-main" "$prefix-lru" >/dev/null 2>&1 || true; }
trap stop_pair EXIT INT TERM

start_pair() {
	img=$1
	srv=$(server_bin "$img")
	stop_pair
	docker run -d --name "$prefix-main" -p "127.0.0.1:$port:6379" "$img" "$srv" >/dev/null || return 1
	docker run -d --name "$prefix-lru" -p "127.0.0.1:$lru:6379" "$img" "$srv" \
		--maxmemory 64mb --maxmemory-policy allkeys-lru >/dev/null || return 1
	cli=$(cli_bin "$img")
	for c in "$prefix-main" "$prefix-lru"; do
		i=0
		until docker exec "$c" "$cli" ping >/dev/null 2>&1; do
			i=$((i + 1))
			[ $i -gt 50 ] && { echo "$c did not answer PING" >&2; return 1; }
			sleep 0.2
		done
	done
	docker exec "$prefix-main" "$cli" INFO server | grep -E '^(redis_version|server_name|valkey_version):' | tr -d '\r' | tr '\n' ' '
	echo
}

summary=""
failed=0
for img in $images; do
	echo "=== $img"
	if ! start_pair "$img"; then
		summary="$summary\n$img: could not start the servers"
		failed=1
		continue
	fi
	dir="$logs/$(echo "$img" | tr '/:' '__')"
	mkdir -p "$dir"
	for m in $modules; do
		p=$(envprefix "$m")
		log="$dir/$m.log"
		printf '  %-24s ' "$m"
		# shellcheck disable=SC2046 # the package list is meant to split
		(cd "$root/$m" && env "${p}_TEST_REDIS_ADDR=127.0.0.1:$port" "${p}_TEST_REDIS_LRU_ADDR=127.0.0.1:$lru" \
			go test $race -count=1 -timeout "$timeout" $(packages "$m")) >"$log" 2>&1
		rc=$?
		if [ $rc -eq 0 ]; then
			res=ok
		else
			res="FAIL (rc=$rc, $log)"
			failed=1
			if grep -q "address already in use" "$log"; then
				res="$res -- a fixed port was taken: rerun the package alone before concluding"
			fi
		fi
		echo "$res"
		summary="$summary\n$img $m: $res"
	done
	stop_pair
done

printf '\n=== summary%b\n' "$summary"
exit $failed
