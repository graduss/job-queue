use metrics::{counter, gauge, histogram};
use sqlx::PgPool;
use std::time::Instant;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

const CPU_WORK_ITERATIONS: usize = 5_000_000;

use crate::{Job, JobKind, SharedRx};

#[instrument(skip(rx, store, tx, token))]
pub async fn worker(
    id: usize,
    rx: SharedRx,
    tx: Arc<mpsc::Sender<Uuid>>,
    store: PgPool,
    token: CancellationToken,
) {
    info!("started");

    loop {
        let job_id = tokio::select! {
            biased;
            _ = token.cancelled() => {
                info!(worker_id = id, "channel closed, exiting");
                break;
            },

            maybe_id = async {
                let mut rx_guard = rx.lock().await;
                rx_guard.recv().await
            } => {
                match maybe_id {
                    Some(id) => id,
                    _ => {
                        info!("channel closed, exiting");
                        break;
                    }
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
                warn!(job_id = %job_id, "job not found or already claimed");
                counter!("jobs_processed_total", "status" => "skipped").increment(1);
                continue;
            }
            Err(e) => {
                error!(job_id = %job_id, error = %e, "db error on claim");
                counter!("jobs_processed_total", "status" => "db_error").increment(1);
                continue;
            }
        };

        info!(job_id = %job.id, kind = %job.kind, "processing");
        // Замеряем время обработки для histogram
        let started_at = Instant::now();

        let result = match job.kind {
            JobKind::Io => Ok(pocess_io_job(&job.payload).await),
            JobKind::Cpu => {
                tokio::task::spawn_blocking(move || process_cpu_job(&job.payload)).await
            }
        };

        let elapsed = started_at.elapsed();
        // histogram! — Prometheus вычислит p50, p90, p99 из этих значений
        histogram!(
            "job_processing_duration_seconds",
            "kind" => job.kind.to_string()
        )
        .record(elapsed.as_secs_f64());

        let update_result = match result {
            Ok(response) => {
                info!(job_id = %job.id, duration_ms = elapsed.as_millis(), "done");
                counter!("jobs_processed_total",
                    "status" => "done", "kind" => job.kind.to_string())
                .increment(1);

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
                error!(job_id = %job.id, error = %e, duration_ms = elapsed.as_millis(), "failed");
                counter!("jobs_processed_total",
                    "status" => "failed", "kind" => job.kind.to_string())
                .increment(1);

                sqlx::query(
                    "UPDATE jobs SET status = 'failed', result = $1, updated_at = NOW()
                     WHERE id = $2",
                )
                .bind(e.to_string())
                .bind(job_id)
                .execute(&store)
                .await
            }
        };

        if let Err(e) = update_result {
            error!(job_id = %job_id, error = %e, "db error on update");
        }

        let queued = tx.max_capacity() - tx.capacity();
        // Обновляем gauge при каждом health-check
        gauge!("jobs_queue_depth").set(queued as f64);
    }

    info!("worker stopped cleanly");
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
