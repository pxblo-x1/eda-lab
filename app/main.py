import asyncio
import json
import logging
import os
import random
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

class _ColorFormatter(logging.Formatter):
    _RESET = "\033[0m"
    _BOLD  = "\033[1m"
    _DIM   = "\033[2m"

    # prefix label → color
    _LABEL_COLORS = {
        "PRODUCER":  "\033[38;5;205m",   # magenta/rosa
        "BROKER":    "\033[38;5;69m",    # navy/azul
        "CONSUMER":  "\033[38;5;79m",    # teal/verde
        "Redis":     "\033[38;5;196m",   # rojo (errores)
    }

    # event type → color
    _TYPE_COLORS = {
        "deploy":    "\033[38;5;82m",    # verde
        "cpu_alert": "\033[38;5;208m",   # naranja
        "error_500": "\033[38;5;196m",   # rojo
    }

    def format(self, record: logging.LogRecord) -> str:
        ts    = self.formatTime(record, "%H:%M:%S")
        msg   = record.getMessage()
        dim   = self._DIM
        reset = self._RESET
        bold  = self._BOLD

        # Colorear el label (primera palabra del mensaje)
        label = msg.split()[0] if msg else ""
        label_color = next(
            (c for k, c in self._LABEL_COLORS.items() if label.startswith(k)),
            "\033[37m",
        )

        # Colorear el tipo de evento si aparece en el mensaje
        for etype, color in self._TYPE_COLORS.items():
            msg = msg.replace(f"type={etype}", f"type={color}{bold}{etype}{reset}")

        # Colorear IDs de Redis (patrón digits-digits)
        import re
        msg = re.sub(
            r"(id=)(\d+-\d+)",
            rf"\1{dim}\2{reset}",
            msg,
        )

        return (
            f"{dim}{ts}{reset} "
            f"{label_color}{bold}{label}{reset} "
            f"{msg[len(label):]}"
        )


_handler = logging.StreamHandler()
_handler.setFormatter(_ColorFormatter())
logging.root.setLevel(logging.INFO)
logging.root.addHandler(_handler)
log = logging.getLogger("eda")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
STREAM_NAME = "eda:events"
MAX_EVENTS = 50

# In-memory state
events: deque = deque(maxlen=MAX_EVENTS)
sse_queues: list[asyncio.Queue] = []
last_id = "$"


def get_redis():
    return aioredis.from_url(REDIS_URL, decode_responses=True)


async def consumer_loop():
    global last_id
    r = get_redis()
    try:
        while True:
            try:
                results = await r.xread({STREAM_NAME: last_id}, count=10, block=1000)
                if not results:
                    continue

                for _, messages in results:
                    for msg_id, fields in messages:
                        last_id = msg_id

                        event = {
                            "id": msg_id,
                            "type": fields["type"],
                            "timestamp": fields["timestamp"],
                            "payload": json.loads(fields["payload"]),
                            "status": "pending",
                            "broker_received_at": fields["timestamp"],
                        }
                        events.append(event)
                        log.info("BROKER    → received   type=%-10s id=%s", event["type"], msg_id)

                        # Notify SSE clients: broker received
                        await broadcast("broker_received", event)

                        # Simulate processing
                        delay = random.uniform(0.5, 1.5)
                        log.info("CONSUMER  → processing type=%-10s id=%s (%.2fs)", event["type"], msg_id, delay)
                        await asyncio.sleep(delay)

                        event["status"] = "processed"
                        event["processed_at"] = datetime.now(timezone.utc).isoformat()
                        log.info("CONSUMER  → processed  type=%-10s id=%s", event["type"], msg_id)

                        # Notify SSE clients: consumer processed
                        await broadcast("consumer_processed", event)

            except aioredis.RedisError as e:
                log.error("Redis error: %s — reconnecting in 2s", e)
                await asyncio.sleep(2)
                r = get_redis()
    finally:
        await r.aclose()


async def broadcast(event_type: str, data: dict):
    payload = json.dumps(data)
    dead = []
    for q in sse_queues:
        try:
            q.put_nowait((event_type, payload))
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        sse_queues.remove(q)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consumer_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


class PublishRequest(BaseModel):
    type: str
    payload: dict = {}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/publish")
async def publish(body: PublishRequest):
    r = get_redis()
    try:
        log.info("PRODUCER  → publishing type=%-10s payload=%s", body.type, body.payload)
        msg_id = await r.xadd(
            STREAM_NAME,
            {
                "type": body.type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload": json.dumps(body.payload),
                "status": "pending",
            },
        )
        log.info("PRODUCER  → published  type=%-10s id=%s", body.type, msg_id)
        return {"id": msg_id}
    finally:
        await r.aclose()


@app.get("/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    sse_queues.append(q)

    async def generator():
        try:
            while True:
                try:
                    event_type, data = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"event: {event_type}\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if q in sse_queues:
                sse_queues.remove(q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
