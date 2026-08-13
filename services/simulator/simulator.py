import asyncio, math, random, time
import numpy as np
import httpx
import string

INGEST_URL = "http://localhost:8000/readings"

def make_ward(n=30):
    rng = random.Random(42)  
    ward = []
    for i in range(1, n + 1):
        ward.append({
            "id": f"p{i}",
            "hr": rng.randint(58, 92),
            "spo2": rng.randint(95, 99),
            "bp_sys": rng.randint(105, 140),
            "resp": rng.randint(11, 18),
        })
    return ward

PATIENTS = make_ward(30)

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
