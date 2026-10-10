"""Связь с бэкендом walead: события пачками (с повтором), список каналов при старте."""
import asyncio
import logging
from typing import Any

import httpx

from . import config

log = logging.getLogger(__name__)

_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()


def headers() -> dict[str, str]:
    return {"X-Gateway-Token": config.GATEWAY_TOKEN}


def emit(event: dict[str, Any]) -> None:
    """Событие в бэкенд (сообщение, статус канала, участник группы). Не ждём доставки."""
    _queue.put_nowait(event)


async def fetch_channels() -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(f"{config.BACKEND_URL}/api/crm-internal/gateway/channels", headers=headers())
        response.raise_for_status()
        return response.json().get("channels", [])


async def sender_loop() -> None:
    """Шлём накопленное раз в полсекунды пачками до 50; бэкенд недоступен — держим и повторяем."""
    async with httpx.AsyncClient(timeout=60) as client:
        pending: list[dict[str, Any]] = []
        delay = 1.0
        while True:
            if not pending:
                pending.append(await _queue.get())
            while not _queue.empty() and len(pending) < 50:
                pending.append(_queue.get_nowait())
            try:
                response = await client.post(
                    f"{config.BACKEND_URL}/api/crm-internal/gateway/events",
                    json={"events": pending},
                    headers=headers(),
                )
                response.raise_for_status()
                pending = []
                delay = 1.0
            except Exception as error:  # noqa: BLE001 — сеть, 5xx: повторим
                log.warning("события не доставлены (%d): %s", len(pending), error)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            await asyncio.sleep(0.5)
