use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

pub type Store = Arc<Mutex<HashMap<Uuid, Job>>>;

#[derive(Clone)]
pub struct AppState {
    store: Store,
}

// Handlers

async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> impl IntoResponse {
    let id = Uuid::new_v4();

    let job = Job {
        id,
        payload: req.payload.clone(),
        status: JobStatus::Pending,
        result: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    {
        let mut store = state.store.lock().unwrap();
        store.insert(id, job);
    }

    simulate_work(&req.payload).await;

    {
        let mut store = state.store.lock().unwrap();
        if let Some(job) = store.get_mut(&id) {
            job.status = JobStatus::Done;
            job.result = Some(format!("processed: {}", job.payload.to_uppercase()));
            job.updated_at = Utc::now();
        }
    }

    (
        StatusCode::CREATED,
        Json(CreateJobResponse {
            id,
            status: JobStatus::Done,
        }),
    )
}

async fn get_job(State(state): State<AppState>, Path(id): Path<Uuid>) -> impl IntoResponse {
    let store = state.store.lock().unwrap();

    match store.get(&id) {
        Some(job) => (StatusCode::OK, Json(Some(job.clone()))),
        None => (StatusCode::NOT_FOUND, Json(None)),
    }
}

async fn list_jobs(State(state): State<AppState>) -> impl IntoResponse {
    let store = state.store.lock().unwrap();

    (
        StatusCode::OK,
        Json(store.values().cloned().collect::<Vec<Job>>()),
    )
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "ok", "stage": 0 })),
    )
}

// Simulator

async fn simulate_work(payload: &String) {
    tokio::time::sleep(Duration::from_millis(10)).await;

    let _hash: u64 = payload
        .bytes()
        .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
}

#[tokio::main]
async fn main() {
    println!("Stage 0 — Baseline server");
    println!("Store: std::sync::Mutex<HashMap>");
    println!("Workers: none (synchronous in handler)");
    println!();

    let state = AppState {
        store: Arc::new(Mutex::new(HashMap::new())),
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
