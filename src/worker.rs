use lapin::{
    Connection,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
};
use metrics::{counter, histogram};
use sqlx::PgPool;
use std::time::Instant;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

const CPU_WORK_ITERATIONS: usize = 50_000;

// Сколько сообщений воркер держит in-flight.
// 1 = fair dispatch: следующее сообщение только после ack предыдущего.
const PREFETCH_COUNT: u16 = 1;

use crate::{Job, JobKind, JobMessage, QUEUE_NAME};

#[instrument(skip(store, amqp, token))]
pub async fn worker(id: usize, store: PgPool, amqp: Arc<Connection>, token: CancellationToken) {
    let span = tracing::info_span!("worker", id);
    let _enter = span.enter();
    info!("started");

    // Каждый воркер создаёт свой Channel внутри общего Connection.
    // Channel — lightweight, не надо переиспользовать между потоками.
    let channel = match amqp.create_channel().await {
        Ok(ch) => ch,
        Err(e) => {
            error!(error = %e, "failed to create amqp channel");
            return;
        }
    };

    // QoS: prefetch_count=1 — fair dispatch.
    // Следующее сообщение воркер получит только после ack предыдущего.
    // Без этого RabbitMQ мог бы отдать все сообщения одному быстрому воркеру.
    if let Err(e) = channel
        .basic_qos(PREFETCH_COUNT, BasicQosOptions::default())
        .await
    {
        error!(error = %e, "failed to set qos");
        return;
    }

    // Подписываемся на очередь — получаем Consumer (AsyncIterator)
    let mut consumer = match channel
        .basic_consume(
            QUEUE_NAME.into(),
            format!("worker-{id}").into(), // уникальный тег консьюмера
            BasicConsumeOptions {
                no_ack: false, // false = manual ack (мы сами подтверждаем)
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to start consuming");
            return;
        }
    };

    info!("consuming from queue '{QUEUE_NAME}'");

    loop {
        let delivery = tokio::select! {
            biased;

            _ = token.cancelled() => {
                info!(worker_id = id, "channel closed, exiting");
                break;
            },

            msg = futures_lite::StreamExt::next(&mut consumer) => msg
        };

        let delivery = match delivery {
            Some(Ok(d)) => d,
            Some(Err(e)) => {
                error!(error = %e, "consumer error");
                // При ошибке — небольшая пауза и продолжаем
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            None => {
                // Consumer закрыт (соединение разорвано)
                warn!("consumer stream ended");
                break;
            }
        };

        // Десериализуем сообщение
        let msg: JobMessage = match serde_json::from_slice(&delivery.data) {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "failed to deserialize message — nack without requeue");
                // Отравленное сообщение — отклоняем без повторной постановки в очередь.
                // В production: настрой Dead Letter Exchange чтобы такие сообщения
                // попадали в отдельную очередь для анализа.
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: false,
                        ..Default::default()
                    })
                    .await;
                counter!("jobs_processed_total", "status" => "poison").increment(1);
                continue;
            }
        };

        let job_id = msg.job_id;

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
                // Ack — сообщение обработано (пусть и вхолостую)
                let _ = delivery.ack(BasicAckOptions::default()).await;
                counter!("jobs_processed_total", "status" => "skipped").increment(1);
                continue;
            }
            Err(e) => {
                error!(job_id = %job_id, error = %e, "db error on claim");
                // БД недоступна — возвращаем сообщение в очередь
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await;
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

        match update_result {
            Ok(_) => {
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
            Err(e) => {
                error!(job_id = %job_id, error = %e, "db error on update");
                // Ack — сообщение обработано (пусть и вхолостую)
                let _ = delivery
                    .nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    })
                    .await;
            }
        }
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
