use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

const WORKER_COUNT: usize = 10;
const CHANNEL_CAPACITY: usize = 1_000;

const CPU_WORK_ITERATIONS: usize = 5_000;

// Models

#[derive(Debug, Clone, Serialize, Deserialize, Default, sqlx::Type, PartialEq)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "job_kind", rename_all = "snake_case")]
pub enum JobKind {
    #[default]
    Io,
    Cpu,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(rename_all = "snake_case", type_name = "job_status")]
pub enum JobStatus {
    #[default]
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Job {
    pub id: Uuid,
    pub payload: String,
    pub kind: JobKind,
    pub status: JobStatus,
    pub result: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    pub payload: String,
    #[serde(default)]
    pub kind: JobKind,
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub id: Uuid,
    pub status: JobStatus,
    pub kind: JobKind,
}

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
    tx: mpsc::Sender<Uuid>,
}

// Handlers

async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<impl IntoResponse, AppError> {
    let mut tx = state.db.begin().await.map_err(|e| {
        eprintln!("{:?}", e);
        AppError::InternalServerError
    })?;

    let job = sqlx::query_as::<_, Job>(
        "INSERT INTO jobs (payload, kind)
         VALUES ($1, $2)
         RETURNING *",
    )
    .bind(&req.payload)
    .bind(&req.kind)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        eprintln!("{:?}", e);
        AppError::InternalServerError
    })?;

    match state.tx.try_send(job.id) {
        Ok(_) => {
            tx.commit().await.map_err(|e| {
                eprintln!("{:?}", e);
                AppError::InternalServerError
            })?;
            Ok((
                StatusCode::ACCEPTED,
                Json(CreateJobResponse {
                    id: job.id,
                    status: job.status,
                    kind: job.kind,
                }),
            ))
        }
        Err(_) => {
            tx.rollback().await.map_err(|e| {
                eprintln!("{:?}", e);
                AppError::InternalServerError
            })?;
            return Err(AppError::ServiceUnavailable);
        }
    }
}

async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, AppError> {
    let job = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.db)
        .await
        .map_err(|_| AppError::InternalServerError)?
        .ok_or(AppError::JobNotFound)?;

    Ok((StatusCode::OK, Json(job)))
}

async fn list_jobs(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let jobs = sqlx::query_as::<_, Job>("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 5000")
        .fetch_all(&state.db)
        .await
        .map_err(|_| AppError::InternalServerError)?;

    Ok((StatusCode::OK, Json(jobs)))
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let queued = state.tx.max_capacity() - state.tx.capacity();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "stage": 0,
            "workers": WORKER_COUNT,
            "queue_capacity": CHANNEL_CAPACITY,
            "CPU_WORK_ITERATIONS": CPU_WORK_ITERATIONS,
            "queue_used": queued,
        })),
    )
}

// Wrker

async fn worker(id: usize, rx: SharedRx, store: PgPool) {
    println!("Worker {id} started");

    loop {
        let job_id = {
            let mut rx_guard = rx.lock().await;
            match rx_guard.recv().await {
                Some(id) => id,
                None => {
                    println!("Worker {id}: channel closed, exiting");
                    break;
                }
            }
        };

        let row = sqlx::query_as::<_, Job>(
            "UPDATE jobs SET status = 'running', updated_at = NOW()
             WHERE id = $1 AND status = 'pending'
             RETURNING *",
        )
        .bind(&job_id)
        .fetch_optional(&store)
        .await;

        let job = match row {
            Ok(Some(job)) => job,
            Ok(None) => {
                println!("Worker {id}: job not found or already processed");
                continue;
            }
            Err(e) => {
                println!("Worker {id}: database error: {e}");
                continue;
            }
        };

        let result = match job.kind {
            JobKind::Io => Ok(pocess_io_job(&job.payload).await),
            JobKind::Cpu => tokio::task::spawn_blocking(move || process_cpu_job(&job.payload))
                .await
                .map_err(|e| e.to_string()),
        };

        let update_result = match result {
            Ok(response) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'done', result = $1, updated_at = NOW()
                     WHERE id = $2",
                )
                .bind(response)
                .bind(job_id)
                .execute(&store)
                .await
            }
            Err(e) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'failed', result = $1, updated_at = NOW()
                     WHERE id = $2",
                )
                .bind(e)
                .bind(job_id)
                .execute(&store)
                .await
            }
        };

        if let Err(e) = update_result {
            eprintln!("Database error: {}", e);
        }
    }
}

// Simulator

async fn pocess_io_job(payload: &str) -> String {
    tokio::time::sleep(Duration::from_millis(10)).await;
    format!("processed: {}", payload.to_uppercase())
}

fn process_cpu_job(payload: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for _ in 0..CPU_WORK_ITERATIONS {
        for &byte in payload.as_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    format!("hash: {hash:x}:{}", payload.to_uppercase())
}

// --- recower jobs ----

async fn recover_jobs(store: &PgPool, tx: &mpsc::Sender<Uuid>) {
    let recovered = sqlx::query_as::<_, Job>(
        "WITH updated AS (
            UPDATE jobs
            SET status = 'pending', updated_at = NOW()
            WHERE status IN ('pending', 'running')
            RETURNING *
        )
        SELECT * FROM updated ORDER BY created_at ASC",
    )
    .fetch_all(store)
    .await
    .unwrap_or_default();

    println!("Recovering {} jobs...", recovered.len());

    for job in recovered.iter() {
        match tx.try_send(job.id) {
            Ok(_) => (),
            Err(e) => eprintln!("Failed to send job: {e}"),
        }
    }
}

// --- INIT DB ---

async fn init_db() -> PgPool {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url)
        .await
        .expect("Failed to connect to database")
}

#[tokio::main]
async fn main() {
    println!("Stage 5 — sqlx + PostgreSQL");
    println!("Store:   PostgreSQL (единственное хранилище, без кэша)");
    println!("Workers: {WORKER_COUNT} tokio tasks");
    println!("Channel: bounded({CHANNEL_CAPACITY})");
    println!();

    dotenv().ok();

    let db = init_db().await;

    let tx = {
        let (tx, rx) = mpsc::channel::<Uuid>(CHANNEL_CAPACITY);
        let rx = Arc::new(Mutex::new(rx));

        for i in 0..WORKER_COUNT {
            tokio::spawn(worker(i, Arc::clone(&rx), db.clone()));
        }

        tx
    };

    recover_jobs(&db, &tx).await;

    let state = AppState { db, tx };

    let app = Router::new()
        .route("/health", get(health))
        .route("/jobs", get(list_jobs))
        .route("/jobs", post(create_job))
        .route("/jobs/{id}", get(get_job))
        .with_state(state);

    let addr = "0.0.0.0:3000";
    println!("Listening on http://{}", addr);
    println!();
    println!("Endpoints:");
    println!("  POST   /jobs       {{\"payload\": \"hello\"}}");
    println!("  GET    /jobs/:id");
    println!("  GET    /jobs");
    println!("  GET    /health");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
