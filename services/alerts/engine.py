import asyncio, json
from collections import deque
from datetime import datetime
import statistics
import asyncpg
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "vitals"
DB_DSN = "postgresql://icu:icu_dev@localhost:5432/icu"

WINDOW = 60          # rolling window size (readings) per patient per vital
COOLDOWN = 30        # seconds before re-alerting same patient+rule
Z_LIMIT = 3.0        # trend alert if value drifts 3 std devs from baseline

THRESHOLDS = [       # (vital, op, limit, severity) - most severe first per vital
    ("spo2", "lt", 90, "critical"),
    ("spo2", "lt", 93, "warning"),
    ("hr",   "gt", 130, "critical"),
    ("hr",   "gt", 115, "warning"),
    ("bp_sys", "lt", 90, "critical"),
]

windows = {}         # (patient_id, vital) -> deque of recent values
last_fired = {}      # (patient_id, rule) -> last fire time

def check_thresholds(e):
    hits = []
    fired_vitals = set()
    for vital, op, limit, sev in THRESHOLDS:
        if vital in fired_vitals:
            continue  # only the most severe hit per vital
        v = e[vital]
        if (op == "lt" and v < limit) or (op == "gt" and v > limit):
            hits.append((sev, f"threshold:{vital}_{op}_{limit}",
                         f"{vital}={v} (limit {limit})"))
            fired_vitals.add(vital)
    return hits

def check_trend(e):
    hits = []
    for vital in ("hr", "spo2"):
        key = (e["patient_id"], vital)
        w = windows.setdefault(key, deque(maxlen=WINDOW))
        if len(w) >= 30:  # need enough history for a meaningful baseline
            mean = statistics.mean(w)
            stdev = statistics.pstdev(w) or 0.1
            z = (e[vital] - mean) / stdev
            if abs(z) >= Z_LIMIT:
                hits.append(("warning", f"trend:{vital}_z",
                             f"{vital}={e[vital]} is {z:+.1f} std devs from baseline {mean:.1f}"))
        w.append(e[vital])
    return hits

def deduped(patient_id, hits, now):
    out = []
    for sev, rule, detail in hits:
        key = (patient_id, rule)
        if now - last_fired.get(key, 0) >= COOLDOWN:
            last_fired[key] = now
            out.append((sev, rule, detail))
    return out

async def main():
    pool = await asyncpg.create_pool(DB_DSN)
    r = redis.Redis()
    consumer = AIOKafkaConsumer(
        TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alerts", auto_offset_reset="latest",
    )
    await consumer.start()
    print("alert engine: watching 'vitals'...")
    try:
        async for msg in consumer:
            e = json.loads(msg.value)
            now = asyncio.get_event_loop().time()
            hits = check_thresholds(e) + check_trend(e)
            for sev, rule, detail in deduped(e["patient_id"], hits, now):
                await pool.execute(
                    "INSERT INTO alerts (time, patient_id, severity, rule, detail) VALUES ($1,$2,$3,$4,$5)",
                    datetime.fromisoformat(e["time"]), e["patient_id"], sev, rule, detail,
                )
                await r.publish("alerts", json.dumps({
                    "patient_id": e["patient_id"], "severity": sev,
                    "rule": rule, "detail": detail, "time": e["time"],
                }))
                icon = "!!" if sev == "critical" else "!"
                print(f"{icon}  {sev.upper()}  {e['patient_id']}  {rule}  --  {detail}")
    finally:
        await consumer.stop()
        await r.aclose()
        await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nalert engine stopped.")