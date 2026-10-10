"""Шлюз мессенджеров CRM walead (crm.walead.ru).

Держит аккаунты менеджеров: Telegram (Telethon), WhatsApp (WAHA), Max (PyMax). Своего
состояния нет — каналы и сессии берёт из бэкенда при старте, события шлёт туда же.
Все ручки — по токену X-Gateway-Token (общий с бэкендом), шлюз доступен только во
внутренней сети Coolify.
"""
import asyncio
import logging
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from . import backend, config, tg_dpi
from .base import Channel, GatewayError
from .telegram import TelegramChannel
from .whatsapp import WhatsAppChannel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("gateway")

app = FastAPI(title="WALEAD messenger gateway")
channels: dict[int, Channel] = {}


def auth(x_gateway_token: str = Header(default="")) -> None:
    if not config.GATEWAY_TOKEN or x_gateway_token != config.GATEWAY_TOKEN:
        raise HTTPException(403, "forbidden")


@app.exception_handler(GatewayError)
async def gateway_error(_: Request, error: GatewayError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status_code=400)


def make_channel(data: dict[str, Any]) -> Channel:
    kind = data.get("type")
    if kind == "TELEGRAM":
        return TelegramChannel(data)
    if kind == "WHATSAPP":
        return WhatsAppChannel(data)
    if kind == "MAX":
        from .max import MaxChannel  # PyMax подгружаем только когда есть каналы Max

        return MaxChannel(data)
    raise GatewayError(f"Неизвестный мессенджер: {kind}")


def upsert(data: dict[str, Any]) -> Channel:
    channel_id = int(data["id"])
    existing = channels.get(channel_id)
    if existing and existing.type == data.get("type"):
        existing.update(data)
        return existing
    channel = make_channel(data)
    channels[channel_id] = channel
    return channel


def get(channel_id: int) -> Channel:
    channel = channels.get(channel_id)
    if not channel:
        raise GatewayError("Канал не подключён к шлюзу — переподключите его в админке")
    return channel


# ── жизненный цикл ──────────────────────────────────────────────────────────


async def load_channels() -> None:
    """При старте: все каналы с сессией из бэкенда — подключаем (бэкенд может подниматься дольше)"""
    for attempt in range(60):
        try:
            items = await backend.fetch_channels()
            break
        except Exception as error:  # noqa: BLE001
            log.warning("каналы не получены (%s), попытка %d", error, attempt + 1)
            await asyncio.sleep(10)
    else:
        return
    for data in items:
        try:
            channel = upsert(data)
            await channel.start()
        except Exception as error:  # noqa: BLE001
            log.error("канал #%s не поднялся: %s", data.get("id"), error)
    log.info("каналов подключено: %d", len(channels))


@app.on_event("startup")
async def startup() -> None:
    tg_dpi.install()
    asyncio.create_task(backend.sender_loop())
    asyncio.create_task(load_channels())


@app.get("/health")
async def health(_: None = Depends(auth)) -> dict[str, Any]:
    return {"channels": [channel.health() for channel in channels.values()]}


@app.post("/channels/{channel_id}/start")
async def start(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    channel = upsert({**body["channel"], "id": channel_id})
    await channel.start()
    return channel.health()


@app.post("/channels/{channel_id}/stop")
async def stop(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    channel = channels.pop(channel_id, None)
    if channel:
        await channel.stop(logout=bool(body.get("logout")))
    return {"ok": True}


# ── вход ────────────────────────────────────────────────────────────────────


@app.post("/channels/{channel_id}/login")
async def login(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    channel = upsert({**body["channel"], "id": channel_id})
    return await channel.login_start(body.get("method") or "qr")


@app.get("/channels/{channel_id}/login")
async def login_state(channel_id: int, _: None = Depends(auth)) -> dict[str, Any]:
    channel = get(channel_id)
    refresh = getattr(channel, "login_refresh", None)
    return await refresh() if refresh else channel.login_state()


@app.post("/channels/{channel_id}/login/code")
async def login_code(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    return await get(channel_id).login_code(str(body.get("code") or ""))


@app.post("/channels/{channel_id}/login/password")
async def login_password(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    return await get(channel_id).login_password(str(body.get("password") or ""))


# ── работа ──────────────────────────────────────────────────────────────────


@app.post("/channels/{channel_id}/resolve")
async def resolve(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    return await get(channel_id).resolve(body.get("peer") or {}, body.get("name"))


@app.post("/channels/{channel_id}/send")
async def send(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    return await get(channel_id).send(body.get("peer") or {}, str(body.get("text") or ""), body.get("files") or [])


@app.post("/channels/{channel_id}/check")
async def check(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    return await get(channel_id).check(str(body.get("phone") or ""))


@app.post("/channels/{channel_id}/groups")
async def create_group(channel_id: int, body: dict[str, Any], _: None = Depends(auth)) -> dict[str, Any]:
    channel = get(channel_id)
    title = str(body.get("title") or "Рабочая группа")
    members = body.get("members") or []
    if isinstance(channel, WhatsAppChannel):
        return await channel.create_group(title, members)
    return await channel.create_group(title)


# ── вебхук WAHA ─────────────────────────────────────────────────────────────


@app.post("/waha/webhook")
async def waha_webhook(request: Request, _: None = Depends(auth)) -> dict[str, Any]:
    event = await request.json()
    session = str(event.get("session") or "")
    channel = next(
        (c for c in channels.values() if isinstance(c, WhatsAppChannel) and c.name == session),
        None,
    )
    if channel is None and session.startswith("crm") and session[3:].isdigit():
        channel = channels.get(int(session[3:]))
    if isinstance(channel, WhatsAppChannel):
        try:
            await channel.on_webhook(event.get("event") or "", event.get("payload") or {}, event.get("me"))
        except Exception as error:  # noqa: BLE001
            log.warning("waha: событие %s не разобрано: %s", event.get("event"), error)
    return {"ok": True}
