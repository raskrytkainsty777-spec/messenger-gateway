"""Общий интерфейс канала: Telegram, WhatsApp и Max реализуют одно и то же."""
import base64
import io
from typing import Any

import httpx
import qrcode

from . import config


class GatewayError(Exception):
    """Ошибка, понятная человеку: уходит в ответ 400 с этим текстом."""


class Channel:
    type: str = ""

    def __init__(self, data: dict[str, Any]):
        self.id: int = int(data["id"])
        self.update(data)
        self.status = "NEW"
        self.status_text: str | None = None
        self.login: dict[str, Any] = {"step": None}

    def update(self, data: dict[str, Any]) -> None:
        self.phone: str | None = data.get("phone")
        self.session: str | None = data.get("session")
        self.proxy: dict[str, Any] | None = data.get("proxy")
        self.groups: set[str] = set(data.get("groups") or [])

    # ── жизненный цикл ──
    async def start(self) -> None: ...
    async def stop(self, logout: bool = False) -> None: ...

    # ── вход ──
    async def login_start(self, method: str) -> dict[str, Any]: ...
    async def login_code(self, code: str) -> dict[str, Any]:
        raise GatewayError("Вход по коду здесь не нужен")
    async def login_password(self, password: str) -> dict[str, Any]:
        raise GatewayError("Пароль здесь не нужен")

    def login_state(self) -> dict[str, Any]:
        return {**self.login, "status": self.status, "statusText": self.status_text}

    # ── работа ──
    async def resolve(self, peer: dict[str, Any], name: str | None = None) -> dict[str, Any]: ...
    async def send(self, peer: dict[str, Any], text: str, files: list[dict[str, Any]]) -> dict[str, Any]: ...
    async def check(self, phone: str) -> dict[str, Any]: ...
    async def create_group(self, title: str) -> dict[str, Any]: ...

    def health(self) -> dict[str, Any]:
        return {"id": self.id, "type": self.type, "status": self.status, "statusText": self.status_text}


def qr_data_url(payload: str) -> str:
    image = qrcode.make(payload, box_size=8, border=2)
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode()


async def download(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        if len(response.content) > 50 * 1024 * 1024:
            raise GatewayError("Файл больше 50 МБ")
        return response.content


def file_event(name: str, mime: str | None, data: bytes | None) -> dict[str, Any]:
    """Файл из мессенджера для события: маленький — содержимым (бэкенд положит в S3)."""
    if data is None or len(data) > config.MAX_FILE_BYTES:
        return {"name": name, "mime": mime, "size": len(data) if data else None}
    return {"name": name, "mime": mime, "size": len(data), "b64": base64.b64encode(data).decode()}
