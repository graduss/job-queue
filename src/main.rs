use std::{
    sync::Arc,
    time::Duration
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use tokio::sync::{ mpsc, Mutex };
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const WORKER_COUNT: usize = 4;
const CHANNEL_CAPACITY: usize = 100;

// Models

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub payload: String,
    pub status: JobStatus,
    pub result: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    pub payload: String,
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub id: Uuid,
    pub status: JobStatus,
}

// AppState

pub type Store = Arc<DashMap<Uuid, Job>>;
pub type SharedRx = Arc<Mutex<mpsc::Receiver<Uuid>>>;

#[derive(Clone)]
pub struct AppState {
    store: Store,
    tx: mpsc::Sender<Uuid>,
}

// Handlers

async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> impl IntoResponse {
    let id = Uuid::new_v4();

    state.store.insert(id, Job {
        id,
        payload: req.payload.clone(),
        status: JobStatus::Pending,
        result: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    });

    match state.tx.send(id).await {
        Ok(_) => (
            StatusCode::ACCEPTED,
            Json(CreateJobResponse {id, status: JobStatus::Pending }),
        ).into_response(),

        Err(e) => {
            state.store.remove(&id);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": format!("queue full: {e}"),
                    "capacity": CHANNEL_CAPACITY,
                })),
            ).into_response()
        }
    }
}

async fn get_job(State(state): State<AppState>, Path(id): Path<Uuid>) -> impl IntoResponse {
    match state.store.get(&id) {
        Some(job) => (StatusCode::OK, Json(Some(job.clone()))),
        None => (StatusCode::NOT_FOUND, Json(None)),
    }
}

async fn list_jobs(State(state): State<AppState>) -> impl IntoResponse {
    let jobs: Vec<Job> = state.store.iter().take(5000)
        .map(|entry| entry.value().clone())
        .collect();
    (
        StatusCode::OK,
        Json(jobs),
    )
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

        if let Some(mut job) = store.get_mut(&job_id) {
            job.status = JobStatus::Pending;
            job.updated_at = Utc::now();
        }

        let payload = store.get(&job_id)
            .map(|job| job.payload.clone());

        let result = match payload {
            Some(payload) => Some(process_job(&payload).await),
            None => None,
        };

        if let Some(mut job) = store.get_mut(&job_id) {
            job.status = JobStatus::Done;
            job.result = result;
            job.updated_at = Utc::now();
        }
    }
}

// Simulator

async fn process_job(payload: &String) -> String {
    tokio::time::sleep(Duration::from_millis(10)).await;

    let _hash: u64 = payload
        .bytes()
        .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));

    format!("processed: {}", payload.to_uppercase())
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

    let state = AppState {
        store,
        tx,
    };

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
