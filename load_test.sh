#!/usr/bin/env bash
# ============================================================
# load_test.sh — нагрузочный тест для каждого этапа
#
# Использование:
#   ./load_test.sh            # базовый тест (10 сек, 50 соединений)
#   ./load_test.sh --heavy    # тяжёлый тест (30 сек, 200 соединений)
#
# Требует: oha (cargo install oha)
#          или curl для ручной проверки
# ============================================================

BASE_URL="http://localhost:3000"
DURATION=${DURATION:-10}
CONNECTIONS=${CONNECTIONS:-50}

if [[ "$1" == "--heavy" ]]; then
    DURATION=30
    CONNECTIONS=200
fi

echo "============================================"
echo "  Load Test"
echo "  URL:         $BASE_URL/jobs"
echo "  Duration:    ${DURATION}s"
echo "  Connections: $CONNECTIONS"
echo "============================================"
echo ""

# Проверяем что сервер живой
health=$(curl -s "$BASE_URL/health")
if [[ -z "$health" ]]; then
    echo "ERROR: server not responding at $BASE_URL"
    echo "Run: cargo run --bin server"
    exit 1
fi
echo "Health check: $health"
echo ""

# Проверяем наличие oha
if ! command -v oha &> /dev/null; then
    echo "oha not found. Install: cargo install oha"
    echo ""
    echo "Falling back to manual curl test..."
    echo ""
    # Простой ручной тест через curl
    for i in {1..5}; do
        resp=$(curl -s -w "\n%{http_code} %{time_total}s" \
            -X POST "$BASE_URL/jobs" \
            -H "Content-Type: application/json" \
            -d "{\"payload\": \"test-$i\"}")
        echo "$resp"
    done
    exit 0
fi

# Нагрузочный тест через oha
echo "--- POST /jobs (create + process) ---"
oha \
    -z "${DURATION}s" \
    -c "$CONNECTIONS" \
    -d '{"payload":"benchmark-payload-hello-world", "kind": "cpu"}' \
    -m POST \
    -T "application/json" \
    "$BASE_URL/jobs"

echo ""
echo "--- GET /health (baseline latency) ---"
oha \
    -z 5s \
    -c 100 \
    "$BASE_URL/health"

echo ""
echo "--- GET /jobs (read all) ---"
oha \
    -z "${DURATION}s" \
    -c "$CONNECTIONS" \
    "$BASE_URL/jobs"
