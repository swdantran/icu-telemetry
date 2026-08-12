from contextlib import asynccontextmanager
from datetime import datetime, timezone
import asyncpg
from fastapi import FastAPI
from pydantic import BaseModel, Field

DB_DSN = "postgresql://icu:icu_dev@localhost:5432/icu"

class Reading(BaseModel):
    patient_id: str
    hr: float = Field(gt=0, lt=300)
    spo2: float = Field(gt=0, le=100)
    bp_sys: float = Field(gt=0, lt=300)
    bp_dia: float = Field(gt=0, lt=200)
    resp_rate: float = Field(gt=0, lt=80)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DB_DSN)
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)

@app.post("/readings", status_code=202)
async def ingest(r: Reading):
    await app.state.pool.execute(
        """INSERT INTO vitals (time, patient_id, hr, spo2, bp_sys, bp_dia, resp_rate)
           VALUES ($1,$2,$3,$4,$5,$6,$7)""",
        datetime.now(timezone.utc), r.patient_id, r.hr, r.spo2,
        r.bp_sys, r.bp_dia, r.resp_rate,
    )
    return {"ok": True}
