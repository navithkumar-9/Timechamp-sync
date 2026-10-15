#!/usr/bin/env bash
set -e

echo "=== [Timechamp Engine] Initializing Container Startup ==="

# Function to wait for a TCP port or HTTP endpoint
wait_for_service() {
    local host="$1"
    local port="$2"
    local name="$3"
    local max_retries=20
    local count=0

    echo "Verifying connectivity to ${name} (${host}:${port})..."
    until nc -z -v -w3 "$host" "$port" > /dev/null 2>&1 || curl -s "http://${host}:${port}/ping" > /dev/null 2>&1; do
        count=$((count + 1))
        if [ "$count" -ge "$max_retries" ]; then
            echo "Warning: Could not establish immediate connection to ${name} at ${host}:${port}. Proceeding anyway..."
            return 0
        fi
        sleep 2
    done
    echo "✓ ${name} is accessible."
}

# Wait for essential infrastructure
if [ -n "$CLICKHOUSE_HOST" ] && [ -n "$CLICKHOUSE_PORT" ]; then
    wait_for_service "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT" "ClickHouse"
fi

if [ -n "$REDIS_HOST" ] && [ -n "$REDIS_PORT" ]; then
    wait_for_service "$REDIS_HOST" "$REDIS_PORT" "Redis"
fi

# Run ClickHouse migrations ONLY if explicitly enabled
if [ "$RUN_MIGRATIONS" = "true" ]; then
    echo "Running ClickHouse schema setup / migrations..."
    python -m app.db.migrations || echo "Warning: ClickHouse migration notice."
else
    echo "Skipping migrations (RUN_MIGRATIONS is not set to 'true')."
fi

echo "=== [Timechamp Engine] Launching Process: $@ ==="
exec "$@"
