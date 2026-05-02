# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A learning project exploring Rust concurrency and performance through a job queue HTTP server. Each stage replaces one component and benchmarks the result. Stages build on each other and live on separate git branches.

## Commands

```bash
cargo build --release          # build
cargo run --bin server         # run server (port 3000)
cargo check                    # fast type-check without building
cargo clippy                   # lint

./load_test_wrk.sh             # wrk load test (standard, 10s)
./load_test_wrk.sh --heavy     # 30s, 200 connections
./load_test_wrk.sh --quick     # 5s, 20 connections
./load_test_wrk.sh > ./results/stage-N.txt  # save results
```

## Architecture

Single binary (`src/main.rs`) — Axum router + shared `AppState` passed via `.with_state()`.

**Endpoints:** `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `GET /health`

**Stage progression** (each is a branch):

| Stage | Store | Workers | Key concept |
|-------|-------|---------|-------------|
| 0 (current) | `std::sync::Mutex<HashMap>` | none — work done in handler | baseline, handler blocks on `simulate_work` |
| 1 | `tokio::sync::RwLock<HashMap>` | none | don't block the async executor |
| 2 | `DashMap` | none | sharded locks reduce write contention |
| 3 | `DashMap` + `mpsc` channel | N tokio tasks | handler returns `202 Accepted` immediately |
| 4 | Stage 3 + `spawn_blocking`/rayon | thread pool | CPU work off the async executor |
| 5 | Stage 4 + sqlx + PostgreSQL | thread pool | persistence, connection pooling |
| 6 | Stage 5 + tracing + prometheus | — | observability |
| 7 | Stage 6 + `CancellationToken` | — | graceful shutdown |

**`simulate_work`** (stage 0): 10 ms `tokio::time::sleep` + trivial hash computation — intentionally holds the handler to make contention visible in benchmarks.

**Measuring:** compare `Requests/sec` and `Latency 99%` across stages using saved `results/stage-N.txt` files.
