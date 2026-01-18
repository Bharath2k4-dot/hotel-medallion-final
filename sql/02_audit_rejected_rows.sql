CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.rejected_rows (
  rejected_id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  layer TEXT NOT NULL,
  source_table TEXT NOT NULL,
  target_table TEXT NOT NULL,
  reason TEXT NOT NULL,
  rejected_at TIMESTAMP NOT NULL DEFAULT now(),
  row_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_rejected_rows_run_id ON audit.rejected_rows(run_id);
CREATE INDEX IF NOT EXISTS idx_rejected_rows_reason ON audit.rejected_rows(reason);
