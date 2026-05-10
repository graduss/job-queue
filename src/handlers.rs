use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use metrics::{counter, gauge};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use crate::{
    AppError, AppState, CHANNEL_CAPACITY, CreateJobRequest, CreateJobResponse, Job, WORKER_COUNT,
};

// --- Handlers ---

// #[instrument] создаёт span при входе в функцию.
// skip(state)     — не логируем AppState
// fields(kind=..) — добавляем поле прямо в заголовок span
#[instrument(skip(state), fields(kind = %req.kind))]
pub async fn create_job(
    State(state): State<AppState>,
    Json(req): Json<CreateJobRequest>,
) -> Result<impl IntoResponse, AppError> {
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
        error!(error = %e, "db insert failed");
        counter!("jobs_created_total", "status" => "db_error").increment(1);
        AppError::InternalServerError
    })?;

    let queued = (state.tx.max_capacity() - state.tx.capacity()) as f64;
    gauge!("jobs_queue_depth").set(queued);

    match state.tx.try_send(job.id) {
        Ok(_) => Ok((
            StatusCode::ACCEPTED,
            Json(CreateJobResponse {
                id: job.id,
                status: job.status,
                kind: job.kind,
            }),
        )),
        Err(_) => {
            warn!(job_id = %job.id, "queue full, job saved to db");
            counter!("jobs_queue_full_total").increment(1);
            return Err(AppError::ServiceUnavailable);
        }
    }
}

#[instrument(skip(state))]
pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, AppError> {
    let job = sqlx::query_as::<_, Job>("SELECT * FROM jobs WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| {
            error!(job_id = %id, error = %e, "db error on get");
            AppError::InternalServerError
        })?
        .ok_or({
            warn!(job_id = %id, "job not found");
            AppError::JobNotFound
        })?;

    info!(job_id = %id, status = %job.status, "job fetched");
    Ok((StatusCode::OK, Json(job)))
}

#[instrument(skip(state))]
pub async fn list_jobs(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let jobs = sqlx::query_as::<_, Job>("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 5000")
        .fetch_all(&state.db)
        .await
        .map_err(|e| {
            error!(error = %e, "db error on list");
            AppError::InternalServerError
        })?;

    info!(count = jobs.len(), "jobs listed");
    Ok((StatusCode::OK, Json(jobs)))
}

pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let db_ok = sqlx::query("SELECT 1").execute(&state.db).await.is_ok();
    let pool_size = state.db.size();
    let pool_idle = state.db.num_idle();
    let queued = state.tx.max_capacity() - state.tx.capacity();

    // Обновляем gauge при каждом health-check
    gauge!("jobs_queue_depth").set(queued as f64);
    gauge!("db_pool_size").set(pool_size as f64);
    gauge!("db_pool_idle").set(pool_idle as f64);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": if db_ok { "ok" } else { "degraded" },
            "stage": 6,
            "db": if db_ok { "ok" } else { "error" },
            "pool_size": pool_size,
            "pool_idle": pool_idle,
            "workers": WORKER_COUNT,
            "queue_capacity": CHANNEL_CAPACITY,
            "queue_used": queued,
        })),
    )
}

// GET /metrics — Prometheus scrape endpoint
// Prometheus обращается сюда каждые ~15 секунд и забирает все метрики.
// handle.render() возвращает текст в формате OpenMetrics/Prometheus.
pub async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        state.prom.render(),
    )
}
