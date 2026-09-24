
# ICU Telemetry & Alerting Platform

A real-time patient monitoring platform that ingests simulated ICU vital signs, detects patient deterioration, and delivers prioritized alerts to a live dashboard.

Built as an event-driven system with independent services for ingestion, persistence, anomaly detection, and real-time delivery.

**Python · FastAPI · Kafka · PostgreSQL · Redis · React · TypeScript · Docker**

> All patient data is synthetic. This project is for educational and software engineering purposes only and is not intended for clinical use.

## Demo

![ICU dashboard showing patient deterioration and alert escalation](docs/dashboard-escalation.png)

The simulator generates vital-sign readings for 30 patients at approximately 1 Hz per patient.

After 90 seconds, patient `p2` begins a scripted deterioration: oxygen saturation decreases while heart rate rises. The alert engine detects abnormal readings and publishes warning and critical alerts to the dashboard.

## Architecture

```text
                Patient Simulator
                 30 patients, 1 Hz
                        |
                        | POST /readings
                        v
                FastAPI Ingestion
                        |
                        | Validate + timestamp
                        v
                Kafka / Redpanda
                  Topic: vitals
                  3 partitions
                        |
          +-------------+-------------+
          |             |             |
          v             v             v
       Writer      Alert Engine    WebSocket
          |             |          Consumer
          |        +----+----+        |
          |        |         |        |
          v        v         v        |
      PostgreSQL  Alerts    Redis      |
                 (Postgres) Pub/Sub    |
                             |        |
                             +----+---+
                                  |
                                  v
                           WebSocket API
                                  |
                                  v
                           React Dashboard
```

### Service Responsibilities

| Service | Responsibility |
|---|---|
| Simulator | Generates synthetic vital signs and scripted deterioration events |
| Ingest API | Validates incoming readings, assigns timestamps, and publishes Kafka events |
| Writer | Consumes readings and persists them to PostgreSQL |
| Alert Engine | Evaluates readings against clinical-inspired thresholds and statistical baselines |
| WebSocket Service | Consumes live readings and Redis alerts, then broadcasts them to connected clients |
| Dashboard | Displays current patient vitals and prioritized alerts |

Three independent Kafka consumer groups process the same `vitals` topic.

| Consumer Group | Purpose |
|---|---|
| `writer` | Database persistence |
| `alerts` | Anomaly detection |
| `ws` | Real-time delivery |

This separation allows ingestion, storage, detection, and dashboard delivery to operate as independent components.

## Tech Stack

| Layer | Technologies |
|---|---|
| Backend | Python, FastAPI, asyncio, Pydantic |
| Event Streaming | Kafka via Redpanda, aiokafka |
| Database | PostgreSQL, asyncpg |
| Messaging | Redis Pub/Sub |
| Frontend | React, TypeScript, Vite |
| Real-Time Communication | WebSockets |
| Infrastructure | Docker Compose |
| Testing | pytest, k6, ESLint |

## Project Structure

```text
icu-telemetry/
├── services/
│   ├── ingest/
│   │   └── main.py          # FastAPI ingestion and Kafka producer
│   ├── writer/
│   │   └── writer.py        # Kafka consumer for PostgreSQL persistence
│   ├── alerts/
│   │   └── engine.py        # Threshold detection, trends, cooldowns
│   ├── ws/
│   │   └── server.py        # WebSocket server and event broadcasting
│   └── simulator/
│       └── simulator.py     # Synthetic patients and deterioration scenario
│
├── dashboard/
│   └── src/
│       ├── App.tsx          # Live patient cards and alert feed
│       ├── App.css
│       └── index.css
│
├── db/
│   └── init.sql             # Database schema and simulated patients
│
├── tests/
│   └── test_alerts.py       # Alert engine unit tests
│
├── load/
│   └── ingest.js            # k6 ingestion load test
│
├── docs/
│   └── dashboard-escalation.png
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup

### Prerequisites

- Python 3.12+
- Node.js and npm
- Docker and Docker Compose

### 1. Clone the Repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd icu-telemetry
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

Docker Compose starts three services:

| Service | Port |
|---|---|
| PostgreSQL | 5432 |
| Redis | 6379 |
| Redpanda | 9092 |

On first initialization, `db/init.sql` creates the database tables and registers the simulated patients.

**Note:** Database initialization scripts only execute when PostgreSQL initializes a new data volume. Updating `init.sql` does not automatically modify an existing database.

### 3. Install Python Dependencies

```bash
python -m venv .venv
source .venv/bin/activate

python -m pip install -r requirements.txt
```

Activate the virtual environment in each terminal running a Python service.

## Running

Start each application service in a separate terminal from the repository root.

**1. Ingestion API**

```bash
python -m uvicorn services.ingest.main:app --port 8000
```

**2. Database Writer**

```bash
python services/writer/writer.py
```

**3. Alert Engine**

```bash
python services/alerts/engine.py
```

**4. WebSocket Server**

```bash
python -m uvicorn services.ws.server:app --port 8001
```

**5. React Dashboard**

```bash
cd dashboard
npm install
npm run dev
```

Open http://localhost:5173.

**6. Patient Simulator**

Return to the repository root and run:

```bash
python services/simulator/simulator.py
```

Start the simulator after the consumers and dashboard are connected so the demonstration begins with a complete pipeline.

## API

### `POST /readings`

Submit a patient vital-sign reading for asynchronous processing.

**Endpoint**

```text
http://localhost:8000/readings
```

**Request**

```bash
curl -X POST http://127.0.0.1:8000/readings \
  -H "Content-Type: application/json" \
  -d '{
    "patient_id": "p2",
    "hr": 85,
    "spo2": 97,
    "bp_sys": 120,
    "bp_dia": 80,
    "resp_rate": 16
  }'
```

**Response — HTTP 202 Accepted**

```json
{
  "ok": true
}
```

After validation, the API assigns a UTC timestamp and publishes the reading to the Kafka `vitals` topic.

The patient ID is used as the Kafka message key to preserve ordering for each patient's readings within a partition.

### Request Schema

| Field | Type | Validation |
|---|---|---|
| `patient_id` | string | Required |
| `hr` | float | 0 < hr < 300 |
| `spo2` | float | 0 < spo2 <= 100 |
| `bp_sys` | float | 0 < bp_sys < 300 |
| `bp_dia` | float | 0 < bp_dia < 200 |
| `resp_rate` | float | 0 < resp_rate < 80 |

### Error Responses

**HTTP 422 — Validation Error**

Returned when a required field is missing, has an incompatible type, or violates the configured numeric constraints.

Example invalid reading:

```json
{
  "patient_id": "p2",
  "hr": -10,
  "spo2": 105,
  "bp_sys": 120,
  "bp_dia": 80,
  "resp_rate": 16
}
```

The API rejects this request before publishing it to Kafka.

The current ingestion API validates reading structure and numeric ranges but does not verify patient registration. An unknown patient ID can therefore be accepted at ingestion and fail downstream during database persistence.

## WebSocket API

### `WS /ws`

Subscribe to live patient readings and alerts.

**Endpoint**

```text
ws://127.0.0.1:8001/ws
```

The server broadcasts two event types: `vitals` and `alert`.

### Vitals Event

Example message:

```json
{
  "type": "vitals",
  "data": {
    "patient_id": "p2",
    "hr": 85,
    "spo2": 97,
    "bp_sys": 120,
    "bp_dia": 80,
    "resp_rate": 16,
    "time": "2026-09-24T15:00:00+00:00"
  }
}
```

### Alert Event

Example message:

```json
{
  "type": "alert",
  "data": {
    "patient_id": "p2",
    "severity": "warning",
    "rule": "threshold:spo2_lt_93",
    "detail": "spo2=92.6 (limit 93)",
    "time": "2026-09-24T15:00:00+00:00"
  }
}
```

The dashboard maintains the latest received readings for each patient and displays the 20 most recent alerts received during the current browser session.

It automatically attempts to reconnect when the WebSocket connection closes.

## Alert Detection

The detection engine combines fixed thresholds with per-patient statistical baselines.

### Threshold Detection

| Vital Sign | Condition | Severity |
|---|---|---|
| SpO₂ | < 93% | Warning |
| SpO₂ | < 90% | Critical |
| Heart Rate | > 115 bpm | Warning |
| Heart Rate | > 130 bpm | Critical |
| Systolic BP | < 90 mmHg | Critical |

Only the most severe matching threshold is emitted for each vital sign during a single evaluation.

These thresholds are simulation rules and are not validated clinical decision criteria.

### Trend Detection

The engine maintains a rolling window of the 60 most recent readings per patient for heart rate and SpO₂.

Once the window is full, it compares each new reading against the previous window's mean and population standard deviation.

```text
z = (current_value - mean) / standard_deviation
```

A warning is generated when:

- Heart rate rises at least 4.5 standard deviations above baseline.
- SpO₂ falls at least 4.5 standard deviations below baseline.

Directional checks help avoid flagging non-dangerous changes, such as unusually high oxygen saturation.

### Alert Cooldown

```text
Cooldown: 30 seconds per (patient_id, rule)
```

Repeated alerts for the same patient and rule are suppressed within the cooldown window.

Different patients and different rules are tracked independently.

This reduces repeated notifications while allowing distinct conditions to generate separate alerts.

## Testing

### Backend Unit Tests

The alert engine is tested using pytest.

Install pytest if it is not already available:

```bash
python -m pip install pytest
```

Run:

```bash
python -m pytest tests/test_alerts.py -v
```

### Test Coverage

| Test | Expected Behavior |
|---|---|
| Normal vitals | No alerts generated |
| Low SpO₂ warning | Warning generated |
| Low SpO₂ critical | Critical alert generated |
| Multiple critical conditions | Independent alerts generated |
| Threshold boundaries | Strict comparison behavior preserved |
| Insufficient trend history | No premature trend alerts |
| Trend detection | Significant deviations detected |
| Alert cooldown | Duplicate alerts suppressed within 30 seconds |
| Patient isolation | Cooldowns tracked independently per patient |

**Latest result: 9 tests passed.**

These are unit tests for the alert detection logic. They do not constitute full end-to-end failure-recovery or database integration testing.

### Frontend Checks

```bash
cd dashboard

npm run build
npm run lint
```

Both checks passed during application verification.

## Performance

Load testing was performed using k6 against the FastAPI ingestion endpoint.

**Configuration**

- Single Uvicorn process
- Kafka acknowledgment per request
- Redpanda running through Docker
- Apple Silicon laptop

### Results

| Target Rate | Achieved Throughput | p50 | p95 | Request Failures |
|---|---|---|---|---|
| 500/s | 499/s | 8.9 ms | 91 ms | 0% |
| 1,000/s | 990/s | 145 ms | 276 ms | 0% |
| 2,000/s | 938/s | 509 ms | 2.01 s | 0% |

The ingestion endpoint achieved approximately 990 accepted readings per second at the 1,000/s target with p95 latency of 276 ms.

At a target of 2,000/s, throughput fell below the requested rate and latency increased substantially, indicating saturation in this configuration.

These measurements describe ingestion performance, not guaranteed end-to-end database persistence throughput.

The load-testing script is available at `load/ingest.js`.

## Engineering Decisions

### Event-Driven Processing

Kafka separates the ingestion API from downstream processing.

The API acknowledges requests after Kafka confirms publication, allowing the writer, alert engine, and WebSocket service to consume events independently.

This avoids coupling ingestion latency directly to database writes and anomaly detection.

### Per-Patient Message Ordering

Kafka messages are keyed by `patient_id`.

Messages for the same patient are routed to the same partition, preserving their order within the stream.

This is important for maintaining meaningful rolling statistical windows.

### Managing False Alarms

An earlier version of the trend detector used a z-score threshold of 3.0 and generated frequent alerts during healthy simulations.

The detector was adjusted to use:

- A 4.5 standard deviation threshold
- A complete 60-reading baseline
- Directional checks for HR and SpO₂
- Per-patient, per-rule cooldowns

The revised configuration reduced unnecessary alerts during simulated healthy readings while preserving the scripted deterioration scenario.

### Consumer Recovery

Kafka retains events independently of application consumers.

During development, stopping and restarting the database writer demonstrated recovery through replay of retained readings.

The current implementation uses automatic Kafka offset management and does not implement exactly-once database writes or comprehensive recovery guarantees.

## Known Limitations

- Unknown patient IDs are not rejected by the ingestion API.
- Consumers use automatic offset commits.
- Database writes are not idempotent.
- Redis Pub/Sub does not retain alerts for disconnected subscribers.
- The dashboard stores live state in memory rather than loading historical readings on startup.
- Service URLs and development credentials are configured for local execution.

## Future Improvements

- Patient registration validation at ingestion
- Idempotent database writes and explicit offset commits
- Integration tests for service interruptions and recovery
- Historical vitals API and dashboard views
- Alert acknowledgment and escalation workflows
- Containerization of application services
- Monitoring with Prometheus and Grafana