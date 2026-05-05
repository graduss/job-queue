CREATE TYPE job_kind AS ENUM ('io', 'cpu');
CREATE TYPE job_status AS ENUM ('pending', 'running', 'done', 'failed');

CREATE TABLE IF NOT EXISTS jobs (
    id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    payload    TEXT        NOT NULL,
    kind       job_kind    NOT NULL DEFAULT 'io',
    status     job_status  NOT NULL DEFAULT 'pending',
    result     TEXT,
    error      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Воркеры при старте ищут незавершённые задачи
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);

-- Список задач: сортировка по времени
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);
