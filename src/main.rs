use axum::{
    Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use dotenvy::dotenv;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{error, info, instrument, warn};

mod models;
use models::*;

mod worker;
use worker::worker;

mod handlers;
use handlers::*;

pub const WORKER_COUNT: usize = 4;
pub const CHANNEL_CAPACITY: usize = 50_000;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

// AppState
pub type SharedRx = Arc<Mutex<mpsc::Receiver<Uuid>>>;

pub enum AppError {
    JobNotFound,
    InternalServerError,
    ServiceUnavailable,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::JobNotFound => StatusCode::NOT_FOUND.into_response(),
            AppError::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            AppError::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    db: PgPool,
    tx: Arc<mpsc::Sender<Uuid>>,
    prom: PrometheusHandle,
}

// --- recower jobs ----

#[instrument(skip(store, tx))]
async fn recover_jobs(store: &PgPool, tx: &mpsc::Sender<Uuid>) {
    let recovered = sqlx::query_as::<_, Job>(
        "WITH updated AS (
            UPDATE jobs
            SET status = 'pending', updated_at = NOW()
            WHERE status IN ('pending', 'running')
            RETURNING *
        )
        SELECT * FROM updated ORDER BY created_at ASC LIMIT $1",
    )
    .bind(500)
    .fetch_all(store)
    .await
    .unwrap_or_default();

    warn!(count = recovered.len(), "recovering unfinished jobs");

    for job in recovered.iter() {
        match tx.try_send(job.id) {
            Ok(_) => (),
            Err(e) => warn!("Failed to send job: {e}"),
        }
    }
}

// --- INITs ---

fn init_tracing() {
    // RUST_LOG управляет фильтрацией.
    // Дефолт: наш код на info, sqlx на warn (иначе логирует каждый SQL)
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|e| {
        eprint!("{e}");
        "job_queue=info,tower_http=info,sqlx=warn".parse().unwrap()
    });

    tracing_subscriber::fmt()
        // Для разработки: pretty() — человекочитаемо с цветами и отступами.
        // Для production: .json() — структурированный JSON для Loki/Datadog.
        // .pretty()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .init();
}

fn init_metrics() -> PrometheusHandle {
    // PrometheusBuilder::new().build() возвращает (recorder, handle).
    // recorder устанавливается как глобальный — все вызовы counter!/gauge!/histogram!
    // пишут в него. handle.render() собирает итоговый текст для /metrics.
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder).unwrap();

    // Инициализируем метрики нулём — чтобы они сразу появились в /metrics
    // даже если событий ещё не было. Удобно для алертов: "метрика исчезла" = проблема.
    counter!("jobs_created_total", "status" => "ok", "kind" => "io").absolute(0);
    counter!("jobs_created_total", "status" => "ok", "kind" => "cpu").absolute(0);
    counter!("jobs_processed_total", "status" => "done", "kind" => "io").absolute(0);
    counter!("jobs_processed_total", "status" => "done", "kind" => "cpu").absolute(0);
    counter!("jobs_processed_total", "status" => "failed", "kind" => "io").absolute(0);
    counter!("jobs_queue_full_total").absolute(0);
    gauge!("jobs_queue_depth").set(0.0);
    gauge!("db_pool_size").set(0.0);
    gauge!("db_pool_idle").set(0.0);

    handle
}

async fn init_db() -> PgPool {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(15))
        .connect(&database_url)
        .await
        .expect("Failed to connect to database")
}

// ─── Обработчик сигналов ──────────────────────────────────────

// Ждём SIGTERM (от Docker/Kubernetes) или SIGINT (Ctrl+C).
// Как только получен — отменяем токен → все компоненты начинают shutdown.
pub async fn shutdown_signal(token: CancellationToken) {
    // ctrl_c ловит SIGINT (Ctrl+C) на всех платформах
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl+C");
    };

    // SIGTERM — стандартный сигнал от Docker stop, Kubernetes, systemd
    // Доступен только на Unix
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM")
            .recv()
            .await;
    };

    // На Windows SIGTERM не поддерживается — ждём только Ctrl+C
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    // Ждём первого из двух сигналов
    tokio::select! {
        _ = ctrl_c    => info!("received SIGINT (Ctrl+C)"),
        _ = terminate => info!("received SIGTERM"),
    }

    info!("initiating graceful shutdown...");
    token.cancel();
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    init_tracing();
    let prom = init_metrics();

    info!("starting job-queue stage 7");
    info!(
        workers = WORKER_COUNT,
        channel_capacity = CHANNEL_CAPACITY,
        "configuration"
    );

    let db = init_db().await;
    info!(
        pool_size = db.size(),
        pool_idle = db.num_idle(),
        "database connected"
    );

    let (tx, rx) = mpsc::channel::<Uuid>(CHANNEL_CAPACITY);
    let tx = Arc::new(tx);
    let rx = Arc::new(Mutex::new(rx));

    recover_jobs(&db, &tx).await;

    // Один токен — все компоненты держат его клон.
    // token.cancel() разбудит всех одновременно.
    let token = CancellationToken::new();
    let mut worker_set = JoinSet::new();

    // Запускаем воркеры через JoinSet — он позволяет ждать завершения всех
    for i in 0..WORKER_COUNT {
        worker_set.spawn(worker(
            i,
            Arc::clone(&rx),
            Arc::clone(&tx),
            db.clone(),
            token.clone(),
        ));
    }

    let state = AppState {
        db: db.clone(),
        tx,
        prom,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler)) // ← Prometheus scrape
        .route("/jobs", get(list_jobs))
        .route("/jobs", post(create_job))
        .route("/jobs/{id}", get(get_job))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = "0.0.0.0:3000";

    info!(addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal(token.clone()));

    if let Err(e) = server.await {
        error!(error = %e, "Server error");
    }

    info!("http server stopped, waiting for workers...");

    // Ждём завершения воркеров с таймаутом.
    // tokio::time::timeout обернёт future и вернёт Err если истечёт время.
    let shutdown_result = tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
        // join_next() ждёт завершения ОДНОГО воркера за раз.
        // Цикл продолжается пока все не завершатся.
        while let Some(result) = worker_set.join_next().await {
            match result {
                Ok(_) => info!("worker finished"),
                Err(e) => error!(error = %e, "worker panicked"),
            }
        }
    })
    .await;

    match shutdown_result {
        Ok(_) => info!("all workers stopped cleanly"),
        Err(e) => warn!(
            timeout_secs = e.to_string(),
            "shutdown timeout — some workers did not finish in time"
        ),
    }

    // Закрываем пул БД последним — воркеры могли делать запросы до конца
    info!("closing database pool...");
    db.close().await;

    info!("shutdown complete ✓");
}
