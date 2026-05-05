use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

const WORKER_COUNT: usize = 4;
const CHANNEL_CAPACITY: usize = 100;

const CPU_WORK_ITERATIONS: usize = 5_000_000;

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

#[derive(Clone)]
pub struct AppState {
    db: PgPool,
    tx: mpsc::Sender<Uuid>,
}

// Handlers

async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<impl IntoResponse, impl IntoResponse> {
    let job = sqlx::query_as::<_, Job>(
        "INSERT INTO jobs (payload, kind)
         VALUES ($1, $2)
         RETURNING *",
    )
    .bind(&req.payload)
    .bind(&req.kind)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        eprintln!("{:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    })?;

    state.tx.send(job.id).await.map_err(|e| {
        eprintln!("{:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
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

async fn get_job(State(state): State<AppState>, Path(id): Path<Uuid>) -> impl IntoResponse {
    match state.store.get(&id) {
        Some(job) => (StatusCode::OK, Json(Some(job.clone()))).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn list_jobs(State(state): State<AppState>) -> impl IntoResponse {
    let jobs: Vec<Job> = state
        .store
        .iter()
        .take(500)
        .map(|entry| entry.value().clone())
        .collect();
    (StatusCode::OK, Json(jobs))
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

async fn worker(id: usize, rx: SharedRx, store: Store) {
    println!("Worker {id} started");

    loop {
        let job_id = match rx.lock().await.recv().await {
            Some(id) => id,
            None => {
                println!("Worker {id}: channel closed, exiting");
                break;
            }
        };

        let (kind, pyload) = {
            if let Some(mut job) = store.get_mut(&job_id) {
                job.status = JobStatus::Pending;
                job.updated_at = Utc::now();
                (job.kind.clone(), job.payload.clone())
            } else {
                continue;
            }
        };

        let result = match kind {
            JobKind::Io => pocess_io_job(&pyload).await,
            JobKind::Cpu => tokio::task::spawn_blocking(move || process_cpu_job(&pyload))
                .await
                .unwrap_or_else(|e| format!("Worker panicked: {e}")),
        };

        if let Some(mut job) = store.get_mut(&job_id) {
            job.status = JobStatus::Done;
            job.result = Some(result);
            job.updated_at = Utc::now();
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

#[tokio::main]
async fn main() {
    println!("Stage 3 — mpsc канал + пул воркеров");
    println!("Store:    DashMap<Uuid, Job>");
    println!("Workers:  {WORKER_COUNT} tokio tasks");
    println!("Channel:  bounded({CHANNEL_CAPACITY})");
    println!();
    println!("Отличие от Stage 2:");
    println!("  POST /jobs → 202 Accepted мгновенно (~мкс вместо 10ms)");
    println!("  Обработка асинхронна: GET /jobs/:id опрашивает статус");
    println!("  При переполнении очереди → 503 (backpressure)");
    println!();

    let store: Store = Arc::new(DashMap::new());

    let tx = {
        let (tx, rx) = mpsc::channel::<Uuid>(CHANNEL_CAPACITY);
        let rx = Arc::new(Mutex::new(rx));

        for i in 0..WORKER_COUNT {
            tokio::spawn(worker(i, Arc::clone(&rx), Arc::clone(&store)));
        }

        tx
    };

    let state = AppState { store, tx };

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
