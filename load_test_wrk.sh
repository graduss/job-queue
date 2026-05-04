#!/usr/bin/env bash
# ============================================================
# load_test_wrk.sh — нагрузочный тест через wrk
#
# Использование:
#   ./load_test_wrk.sh              # стандартный тест
#   ./load_test_wrk.sh --heavy      # тяжёлый тест
#   ./load_test_wrk.sh --stage 0    # запустить конкретный сценарий
#
# Требует: wrk (apt install wrk / brew install wrk)
# ============================================================

BASE_URL="http://localhost:3000"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LUA_DIR="$SCRIPT_DIR/wrk_scripts"

# ─── Параметры по умолчанию ──────────────────────────────────
DURATION=10
THREADS=4
CONNECTIONS=50
STAGE="all"

# ─── Разбор аргументов ───────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --heavy)    DURATION=60; CONNECTIONS=2000; THREADS=12; shift ;;
        --quick)    DURATION=5;  CONNECTIONS=20;  THREADS=2; shift ;;
        --stage)    STAGE="$2"; shift 2 ;;
        --url)      BASE_URL="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ─── Проверки ────────────────────────────────────────────────
if ! command -v wrk &> /dev/null; then
    echo "ERROR: wrk not found"
    echo ""
    echo "Install:"
    echo "  Ubuntu/Debian:  sudo apt install wrk"
    echo "  macOS:          brew install wrk"
    echo "  From source:    https://github.com/wg/wrk"
    exit 1
fi

health=$(curl -s --max-time 2 "$BASE_URL/health")
if [[ -z "$health" ]]; then
    echo "ERROR: server not responding at $BASE_URL"
    echo "Run:  cargo run --bin server"
    exit 1
fi

mkdir -p "$LUA_DIR"

# ─── Lua скрипты ─────────────────────────────────────────────

# POST /jobs с JSON-телом
cat > "$LUA_DIR/post_job.lua" << 'LUA'
wrk.method  = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.body    = '{"payload":"benchmark-payload-hello-world"}'

local threads = {}

function setup(thread)
    thread:set("counter", 0)
    thread:set("errors", 0)
    table.insert(threads, thread)
end

function response(status, headers, body)
    local counter = wrk.thread:get("counter")
    local errors  = wrk.thread:get("errors")

    if status ~= 201 and status ~= 202 then
        errors = errors + 1
    end
    counter = counter + 1

    wrk.thread:set("counter", counter)
    wrk.thread:set("errors", errors)
end

function done(summary, latency, requests)
    local total_counter = 0
    local total_errors = 0
    
    for _, thread in ipairs(threads) do
        total_counter = total_counter + thread:get("counter")
        total_errors = total_errors + thread:get("errors")
    end

    io.write(string.format(
        "\n  Errors (non-2xx): %d / %d  (%.1f%%)\n",
        total_errors, total_counter, total_errors / math.max(total_counter, 1) * 100
    ))
end
LUA

# GET /jobs с рандомным uuid (будет 404, но тестирует read path)
cat > "$LUA_DIR/get_job.lua" << 'LUA'
local uuids = {
    "00000000-0000-0000-0000-000000000001",
    "00000000-0000-0000-0000-000000000002",
    "00000000-0000-0000-0000-000000000003",
}

local idx = 0

function request()
    idx = (idx % #uuids) + 1
    local path = "/jobs/" .. uuids[idx]
    return wrk.format("GET", path)
end
LUA

# Смешанная нагрузка: 80% reads, 20% writes
cat > "$LUA_DIR/mixed.lua" << 'LUA'
wrk.headers["Content-Type"] = "application/json"

local counter = 0

function request()
    counter = counter + 1
    if counter % 5 == 0 then
        -- 20% — запись
        return wrk.format(
            "POST", "/jobs", nil,
            '{"payload":"mixed-load-write-test"}'
        )
    else
        -- 80% — чтение health (всегда отвечает)
        return wrk.format("GET", "/health")
    end
end
LUA

# ─── Helpers ─────────────────────────────────────────────────

separator() {
    echo ""
    echo "──────────────────────────────────────────────"
}

run_test() {
    local name="$1"
    local desc="$2"
    local script="$3"
    local url="$4"
    local conns="${5:-$CONNECTIONS}"

    separator
    echo "  $name"
    echo "  $desc"
    echo "  Threads: $THREADS  |  Connections: $conns  |  Duration: ${DURATION}s"
    echo ""

    if [[ -n "$script" ]]; then
        wrk -t"$THREADS" -c"$conns" -d"${DURATION}s" \
            --latency \
            -s "$LUA_DIR/$script" \
            "$url"
    else
        wrk -t"$THREADS" -c"$conns" -d"${DURATION}s" \
            --latency \
            "$url"
    fi
}

# ─── Тесты ───────────────────────────────────────────────────

echo "============================================"
echo "  wrk Load Test  —  Stage: $STAGE"
echo "  Server: $BASE_URL"
echo "  Health: $health"
echo "============================================"

case "$STAGE" in

    0|all)
        run_test \
            "1. POST /jobs  (write + process)" \
            "Основной сценарий: создать и выполнить задачу" \
            "post_job.lua" \
            "$BASE_URL/jobs"

        run_test \
            "2. GET /health  (baseline latency)" \
            "Нулевая нагрузка: сколько стоит просто ответить" \
            "" \
            "$BASE_URL/health" \
            #100

        run_test \
            "3. Mixed  (80% reads / 20% writes)" \
            "Реалистичный сценарий: чтения преобладают" \
            "mixed.lua" \
            "$BASE_URL/jobs"

        if [[ "$STAGE" == "0" ]]; then break; fi
        ;;&

    read|all)
        run_test \
            "4. GET /jobs  (list all)" \
            "Чтение всего хранилища под нагрузкой" \
            "" \
            "$BASE_URL/jobs" \
            #50
        ;;
esac

separator
echo ""
echo "Что смотреть в результатах:"
echo "  Requests/sec  — пропускная способность"
echo "  Latency avg   — средняя задержка"
echo "  Latency 99%   — хвостовая задержка (p99)"
echo "  Socket errors — потери под нагрузкой"
echo ""
echo "Сохрани цифры — они нужны для сравнения после Stage 1."
