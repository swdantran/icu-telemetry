import asyncio, math, random, time
import numpy as np
import httpx

INGEST_URL = "http://localhost:8000/readings"

PATIENTS = [
    {"id": "p1", "hr": 72, "spo2": 97, "bp_sys": 118, "resp": 14},
    {"id": "p2", "hr": 85, "spo2": 96, "bp_sys": 132, "resp": 16},
    {"id": "p3", "hr": 64, "spo2": 98, "bp_sys": 110, "resp": 12},
]

# Scripted event: 90s after launch, p2 begins desaturating over ~2 minutes
EVENT = {"patient_id": "p2", "start": 90, "duration": 120,
         "spo2_drop": 10, "hr_rise": 30}

START = time.monotonic()

def event_progress(p_id):
    """0.0 -> 1.0 as the scripted deterioration unfolds for this patient."""
    if p_id != EVENT["patient_id"]:
        return 0.0
    elapsed = time.monotonic() - START - EVENT["start"]
    if elapsed <= 0:
        return 0.0
    return min(1.0, elapsed / EVENT["duration"])

def vitals_for(p, t):
    drift = math.sin(t / 60) * 3
    prog = event_progress(p["id"])
    hr = p["hr"] + drift + prog * EVENT["hr_rise"] + np.random.normal(0, 1.5)
    spo2 = min(100, p["spo2"] - prog * EVENT["spo2_drop"] + np.random.normal(0, 0.4))
    bp_sys = p["bp_sys"] + drift * 2 + np.random.normal(0, 3)
    return {
        "patient_id": p["id"],
        "hr": round(hr, 1),
        "spo2": round(spo2, 1),
        "bp_sys": round(bp_sys, 1),
        "bp_dia": round(bp_sys * 0.65 + np.random.normal(0, 2), 1),
        "resp_rate": round(p["resp"] + prog * 6 + np.random.normal(0, 0.8), 1),
    }

async def run_patient(client, p):
    t_offset = random.uniform(0, 60)
    while True:
        reading = vitals_for(p, time.monotonic() + t_offset)
        tag = " [EVENT]" if event_progress(p["id"]) > 0 else ""
        print(f"{p['id']}  hr={reading['hr']}  spo2={reading['spo2']}{tag}")
        try:
            await client.post(INGEST_URL, json=reading, timeout=2)
        except httpx.HTTPError as e:
            print(f"{p['id']}  SEND FAILED: {type(e).__name__}")
        await asyncio.sleep(1)

async def main():
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*(run_patient(client, p) for p in PATIENTS))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
