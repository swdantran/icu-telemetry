import asyncio, json
from datetime import datetime
import asyncpg
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "vitals"
DB_DSN = "postgresql://icu:icu_dev@localhost:5432/icu"

async def main():
    pool = await asyncpg.create_pool(DB_DSN)
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="writer",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print("writer: consuming from 'vitals'...")
    try:
        async for msg in consumer:
            e = json.loads(msg.value)
            await pool.execute(
                """INSERT INTO vitals (time, patient_id, hr, spo2, bp_sys, bp_dia, resp_rate)
                   VALUES ($1,$2,$3,$4,$5,$6,$7)""",
                datetime.fromisoformat(e["time"]), e["patient_id"], e["hr"],
                e["spo2"], e["bp_sys"], e["bp_dia"], e["resp_rate"],
            )
            print(f"wrote {e['patient_id']} hr={e['hr']}")
    finally:
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nwriter stopped.")