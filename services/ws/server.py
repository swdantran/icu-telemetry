import asyncio, json
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as redis
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "vitals"

app = FastAPI()
clients: set[WebSocket] = set()

async def broadcast(payload: dict):
    dead = []
    for ws in clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)

async def consume_vitals():
    consumer = AIOKafkaConsumer(
        TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="ws", auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await broadcast({"type": "vitals", "data": json.loads(msg.value)})
    finally:
        await consumer.stop()

async def consume_alerts():
    r = redis.Redis()
    ps = r.pubsub()
    await ps.subscribe("alerts")
    async for msg in ps.listen():
        if msg["type"] == "message":
            await broadcast({"type": "alert", "data": json.loads(msg["data"])})

@app.on_event("startup")
async def startup():
    asyncio.create_task(consume_vitals())
    asyncio.create_task(consume_alerts())

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()   # keepalive; we don't expect client messages
    except WebSocketDisconnect:
        clients.discard(ws)