import asyncio
import json
import logging
from contextlib import asynccontextmanager

import strawberry
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from strawberry.fastapi import GraphQLRouter
from aiokafka import AIOKafkaConsumer
import os

from db import get_pool, close_pool
from schema import Query

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# Active WebSocket connections for live stream
_live_connections: set[WebSocket] = set()


async def _kafka_to_websockets():
    """Reads posts.enriched and fans out to all connected WebSocket clients."""
    consumer = AIOKafkaConsumer(
        "posts.enriched",
        bootstrap_servers=KAFKA,
        group_id="api-live-stream",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="latest",
    )
    await consumer.start()
    log.info("Live-stream Kafka consumer started")
    try:
        async for msg in consumer:
            if not _live_connections:
                continue
            payload = json.dumps(msg.value)
            dead: set[WebSocket] = set()
            for ws in list(_live_connections):
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            _live_connections -= dead
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()
    asyncio.create_task(_kafka_to_websockets())
    yield
    await close_pool()


schema = strawberry.Schema(query=Query)
graphql_router = GraphQLRouter(schema, graphiql=True)

app = FastAPI(title="Social Pulse API", lifespan=lifespan)
app.include_router(graphql_router, prefix="/graphql")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.websocket("/ws/live")
async def live_stream(ws: WebSocket):
    """Push each enriched post to connected clients in real time."""
    await ws.accept()
    _live_connections.add(ws)
    log.info("WebSocket client connected (%d total)", len(_live_connections))
    try:
        while True:
            await ws.receive_text()  # keep connection open; ignore client msgs
    except WebSocketDisconnect:
        _live_connections.discard(ws)
        log.info("WebSocket client disconnected (%d remaining)", len(_live_connections))
