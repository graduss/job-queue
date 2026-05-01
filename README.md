# Job Queue — учебный проект по многопоточности в Rust

Поэтапный проект: от простейшего синхронного сервера до production-ready очереди задач.
На каждом этапе — замер производительности, чтобы видеть реальный эффект каждого изменения.

## Быстрый старт

```bash
cargo run --bin server        # запустить сервер
./load_test.sh                # нагрузочный тест (нужен oha)
cargo install oha             # установить oha если нет
```

### Попробовать руками

```bash
# Создать задачу
curl -X POST http://localhost:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{"payload": "hello world"}'

# Получить задачу по id
curl http://localhost:3000/jobs/<id>

# Список всех задач
curl http://localhost:3000/jobs

# Проверка сервера
curl http://localhost:3000/health
```

---

## Этапы

### ✅ Stage 0 — Baseline (текущий)

**Что:** задача выполняется прямо в хэндлере, `Mutex<HashMap>` как хранилище.  
**Зачем:** получить точку отсчёта, понять где узкое место.  
**Ожидаемая проблема:** при конкурентных запросах всё выстраивается в очередь.

```
Store:   std::sync::Mutex<HashMap<Uuid, Job>>
Workers: нет, обработка в хэндлере
```

---

### Stage 1 — tokio::sync::RwLock

**Что:** заменяем `std::Mutex` на `tokio::sync::RwLock`.  
**Зачем:** `std::Mutex` блокирует поток tokio-runtime целиком. `RwLock` позволяет
многим читателям работать одновременно и не блокирует async executor.  
**Ожидание:** улучшение latency на read-heavy нагрузке.

---

### Stage 2 — DashMap (lock-free шарды)

**Что:** заменяем `RwLock<HashMap>` на `DashMap`.  
**Зачем:** один глобальный lock — contention при записи. DashMap делит данные
на N шардов, каждый со своим мьютексом. Несколько потоков пишут параллельно.  
**Ожидание:** p99 latency улучшается, RPS растёт при write-heavy нагрузке.

---

### Stage 3 — mpsc канал + пул воркеров

**Что:** хэндлер кладёт задачу в bounded channel и сразу отвечает `202 Accepted`.
Отдельные tokio-задачи (воркеры) читают из канала и обрабатывают.  
**Зачем:** хэндлер перестаёт ждать окончания работы. Throughput растёт кратно.  
**Ожидание:** RPS взлетает, latency хэндлера падает до микросекунд.

```
Store:   DashMap
Channel: tokio::sync::mpsc (bounded)
Workers: N tokio::spawn задач
```

---

### Stage 4 — CPU-задачи: spawn_blocking / rayon

**Что:** CPU-heavy работу выносим из async в `tokio::task::spawn_blocking`
или rayon threadpool.  
**Зачем:** тяжёлые вычисления в async-задаче блокируют tokio executor и
ухудшают latency для всех остальных запросов.  
**Ожидание:** async IO и CPU-работа не мешают друг другу.

---

### Stage 5 — Персистентность: sqlx + PostgreSQL

**Что:** добавляем пул соединений с БД через sqlx. DashMap становится кэшем.  
**Зачем:** результаты переживают рестарт сервера.  
**Новые концепции:** connection pool, async transactions, backpressure от БД.

---

### Stage 6 — Observability: tracing + метрики

**Что:** `tracing` spans через всю цепочку, `prometheus-client` для метрик.  
**Зачем:** видеть систему изнутри. Queue depth, worker utilization, latency гистограммы.

---

### Stage 7 — Graceful shutdown

**Что:** `CancellationToken` + `SIGTERM` handler.  
**Зачем:** деплой без потери задач. При остановке ждём завершения текущих задач.

---

## Что измеряем на каждом этапе

| Метрика | Инструмент |
|---------|-----------|
| RPS (requests/sec) | oha |
| Latency p50 / p99 | oha |
| CPU usage | htop / `cargo flamegraph` |
| Contention | tokio-console (этап 3+) |
