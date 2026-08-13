
CREATE TABLE patients (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  bed TEXT NOT NULL,
  baseline_hr INT NOT NULL DEFAULT 75,
  baseline_spo2 INT NOT NULL DEFAULT 97
);

CREATE TABLE vitals (
  time TIMESTAMPTZ NOT NULL,
  patient_id TEXT NOT NULL REFERENCES patients(id),
  hr REAL, spo2 REAL, bp_sys REAL, bp_dia REAL, resp_rate REAL
);
CREATE INDEX ON vitals (patient_id, time DESC);

INSERT INTO patients (id, name, bed, baseline_hr, baseline_spo2) VALUES
  ('p1', 'Sim Patient 1', 'ICU-01', 72, 97),
  ('p2', 'Sim Patient 2', 'ICU-02', 85, 96),
  ('p3', 'Sim Patient 3', 'ICU-03', 64, 98);

CREATE TABLE IF NOT EXISTS alerts (
  id SERIAL PRIMARY KEY,
  time TIMESTAMPTZ NOT NULL DEFAULT now(),
  patient_id TEXT NOT NULL REFERENCES patients(id),
  severity TEXT NOT NULL,
  rule TEXT NOT NULL,
  detail TEXT,
  acknowledged_at TIMESTAMPTZ
);
