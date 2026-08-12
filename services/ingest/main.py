import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from pydantic import BaseModel, Field

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "vitals"

class Reading(BaseModel):
    patient_id: str
    hr: float = Field(gt=0, lt=300)
    spo2: float = Field(gt=0, le=100)
    bp_sys: float = Field(gt=0, lt=300)
    bp_dia: float = Field(gt=0, lt=200)
    resp_rate: float = Field(gt=0, lt=80)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await app.state.producer.start()
    yield
    await app.state.producer.stop()

app = FastAPI(lifespan=lifespan)

@app.post("/readings", status_code=202)
async def ingest(r: Reading):
    event = r.model_dump()
    event["time"] = datetime.now(timezone.utc).isoformat()
    await app.state.producer.send_and_wait(
        TOPIC,
        json.dumps(event).encode(),
        key=r.patient_id.encode(),   # same patient -> same partition -> ordered
    )
    return {"ok": True}
