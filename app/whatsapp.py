"""WhatsApp-канал через WAHA (контейнер рядом, движок GOWS): сессия WAHA = «crm<id канала>».

Сессию WhatsApp хранит сам WAHA (свой том), в CRM как «сессия» лежит её имя. Вход — QR
(WhatsApp → Связанные устройства → Привязка устройства) или код привязки по номеру.
События WAHA (сообщения, статус сессии, участники групп) приходят на /waha/webhook и
переводятся в события шлюза.
"""
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from . import backend, config
from .base import Channel, GatewayError, file_event

log = logging.getLogger(__name__)

STATUS = {"WORKING": "ACTIVE", "SCAN_QR_CODE": "LOGIN", "STARTING": "LOGIN", "FAILED": "ERROR", "STOPPED": "OFF"}


def waha_headers() -> dict[str, str]:
    return {"X-Api-Key": config.WAHA_API_KEY} if config.WAHA_API_KEY else {}


async def waha(method: str, path: str, **kwargs: Any) -> Any:
    async with httpx.AsyncClient(timeout=kwargs.pop("timeout", 60)) as client:
        response = await client.request(method, f"{config.WAHA_URL}{path}", headers={**waha_headers(), **kwargs.pop("headers", {})}, **kwargs)
    if response.status_code >= 400:
        try:
            detail = response.json().get("message") or response.text
        except Exception:  # noqa: BLE001
            detail = response.text
        raise GatewayError(f"WhatsApp: {str(detail)[:300]}")
    if not response.content:
        return None
    try:
        return response.json()
    except ValueError:
        return response.text


def digits(value: str | None) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def chat_id_of(peer: dict[str, Any]) -> str:
    if peer.get("peerId"):
        return peer["peerId"]
    phone = digits(peer.get("phone"))
    if not phone:
        raise GatewayError("Нет номера собеседника")
    return f"{phone}@c.us"


def serialized(value: Any) -> str:
    if isinstance(value, dict):
        return value.get("_serialized") or value.get("id") or ""
    return str(value or "")


class WhatsAppChannel(Channel):
    type = "WHATSAPP"

    @property
    def name(self) -> str:
        return self.session or f"crm{self.id}"

    def _config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {
            "webhooks": [{
                "url": f"{config.SELF_URL}/waha/webhook",
                "events": ["message.any", "session.status", "group.v2.participants"],
                "customHeaders": [{"name": "X-Gateway-Token", "value": config.GATEWAY_TOKEN}],
            }],
            "metadata": {"channelId": str(self.id)},
        }
        if self.proxy and self.proxy.get("host"):
            # WAHA принимает прокси без схемы: host:port (+ логин/пароль)
            cfg["proxy"] = {
                "server": f"{self.proxy['host']}:{self.proxy.get('port') or 3128}",
                **({"username": self.proxy["user"]} if self.proxy.get("user") else {}),
                **({"password": self.proxy["pass"]} if self.proxy.get("pass") else {}),
            }
        return cfg

    async def _ensure_session(self) -> dict[str, Any]:
        try:
            info = await waha("GET", f"/api/sessions/{self.name}")
            await waha("PUT", f"/api/sessions/{self.name}", json={"name": self.name, "config": self._config()})
            await waha("POST", f"/api/sessions/{self.name}/start")
            return info
        except GatewayError:
            return await waha("POST", "/api/sessions", json={"name": self.name, "start": True, "config": self._config()})

    async def _refresh(self) -> dict[str, Any]:
        info = await waha("GET", f"/api/sessions/{self.name}")
        self._apply(info.get("status"), info.get("me"))
        return info

    def _apply(self, waha_status: str | None, me: dict[str, Any] | None = None) -> None:
        status = STATUS.get(waha_status or "", "ERROR")
        if status == "LOGIN" and self.status in ("ACTIVE",):
            status = "RELOGIN"
        changed = status != self.status
        self.status = status
        self.status_text = None if status == "ACTIVE" else waha_status
        if status == "ACTIVE":
            self.login = {"step": "done"}
        if changed:
            account = None
            if me:
                account = {"id": me.get("id"), "phone": digits(serialized(me.get("id"))), "name": me.get("pushName")}
            backend.emit({"type": "status", "channelId": self.id, "status": status, "statusText": self.status_text,
                          "session": self.name, "account": account})

    async def start(self) -> None:
        try:
            await self._ensure_session()
            await self._refresh()
        except GatewayError as error:
            self.status = "ERROR"
            backend.emit({"type": "status", "channelId": self.id, "status": "ERROR", "statusText": str(error)})

    async def stop(self, logout: bool = False) -> None:
        try:
            await waha("POST", f"/api/sessions/{self.name}/{'logout' if logout else 'stop'}")
            if logout:
                await waha("DELETE", f"/api/sessions/{self.name}")
        except GatewayError:
            pass
        self.status = "OFF"

    async def login_start(self, method: str) -> dict[str, Any]:
        self.session = f"crm{self.id}"
        await self._ensure_session()
        if method == "code":
            if not self.phone:
                raise GatewayError("Укажите номер аккаунта")
            result = await waha("POST", f"/api/{self.name}/auth/request-code", json={"phoneNumber": digits(self.phone)})
            self.login = {
                "step": "pair",
                "pairCode": (result or {}).get("code"),
                "hint": "WhatsApp → Связанные устройства → Привязка устройства → Связать по номеру телефона → введите этот код",
            }
            return self.login
        return await self._qr()

    async def _qr(self) -> dict[str, Any]:
        info = await self._refresh()
        if info.get("status") == "WORKING":
            return self.login
        try:
            image = await waha("GET", f"/api/{self.name}/auth/qr", params={"format": "image"}, headers={"Accept": "application/json"})
            self.login = {
                "step": "qr",
                "qr": f"data:{image.get('mimetype', 'image/png')};base64,{image.get('data')}",
                "hint": "WhatsApp → Связанные устройства → Привязка устройства",
            }
        except GatewayError as error:
            self.login = {"step": "wait", "hint": f"QR ещё не готов: {error}"}
        return self.login

    def login_state(self) -> dict[str, Any]:
        return {**self.login, "status": self.status, "statusText": self.status_text}

    async def login_refresh(self) -> dict[str, Any]:
        """QR WhatsApp живёт 20–60 с — при каждом опросе админки отдаём свежий"""
        if self.login.get("step") == "qr" or self.login.get("step") == "wait":
            return await self._qr()
        await self._refresh()
        return self.login_state()

    # ── работа ──

    def _require(self) -> None:
        if self.status != "ACTIVE":
            raise GatewayError("Канал WhatsApp не подключён")

    async def resolve(self, peer: dict[str, Any], name: str | None = None) -> dict[str, Any]:
        self._require()
        if peer.get("peerId"):
            return {"peerId": peer["peerId"], "kind": peer.get("kind") or "user", "phone": digits(peer["peerId"].split("@")[0])}
        phone = digits(peer.get("phone"))
        if not phone:
            raise GatewayError("Нет номера собеседника")
        check = await waha("GET", "/api/contacts/check-exists", params={"phone": phone, "session": self.name})
        if not check or not check.get("numberExists"):
            raise GatewayError("У номера нет WhatsApp")
        chat_id = check.get("chatId") or f"{phone}@c.us"
        return {"peerId": chat_id, "kind": "user", "phone": phone, "title": name}

    async def send(self, peer: dict[str, Any], text: str, files: list[dict[str, Any]]) -> dict[str, Any]:
        self._require()
        chat = await self.resolve(peer)
        ids: list[str] = []
        if files:
            for index, item in enumerate(files):
                mime = item.get("mime") or "application/octet-stream"
                endpoint = "/api/sendImage" if mime.startswith("image/") else "/api/sendFile"
                result = await waha("POST", endpoint, json={
                    "session": self.name,
                    "chatId": chat["peerId"],
                    "file": {"url": item["url"], "filename": item.get("name") or "file", "mimetype": mime},
                    "caption": text if index == 0 and text else None,
                }, timeout=120)
                ids.append(serialized((result or {}).get("id")))
        elif text:
            result = await waha("POST", "/api/sendText", json={"session": self.name, "chatId": chat["peerId"], "text": text})
            ids.append(serialized((result or {}).get("id")))
        return {"chat": chat, "externalIds": [i for i in ids if i]}

    async def check(self, phone: str) -> dict[str, Any]:
        self._require()
        check = await waha("GET", "/api/contacts/check-exists", params={"phone": digits(phone), "session": self.name})
        exists = bool(check and check.get("numberExists"))
        return {"exists": exists, "chat": {"peerId": check.get("chatId"), "kind": "user", "phone": digits(phone)} if exists else None}

    async def create_group(self, title: str, members: list[str] | None = None) -> dict[str, Any]:
        self._require()
        participants = [{"id": f"{digits(m)}@c.us"} for m in (members or []) if digits(m)]
        group = await waha("POST", f"/api/{self.name}/groups", json={"name": title[:100], "participants": participants})
        group_id = serialized((group or {}).get("id") or (group or {}).get("gid"))
        if not group_id:
            raise GatewayError("WhatsApp не вернул id группы")
        self.groups.add(group_id)
        invite = await waha("GET", f"/api/{self.name}/groups/{group_id}/invite-code")
        link = invite if isinstance(invite, str) else (invite or {}).get("link") or (invite or {}).get("code")
        if link and not str(link).startswith("http"):
            link = f"https://chat.whatsapp.com/{link}"
        return {"chat": {"peerId": group_id, "kind": "group", "title": title}, "inviteLink": link}

    # ── вебхук WAHA ──

    async def on_webhook(self, event: str, payload: dict[str, Any], me: dict[str, Any] | None) -> None:
        if event == "session.status":
            self._apply(payload.get("status"), me)
            return
        if event == "group.v2.participants":
            group_id = serialized((payload.get("group") or {}).get("id"))
            if group_id not in self.groups:
                return
            action = {"join": "joined", "leave": "left"}.get(payload.get("type") or "")
            if not action:
                return
            for member in payload.get("participants") or []:
                member_id = serialized(member.get("id"))
                backend.emit({
                    "type": "member", "channelId": self.id,
                    "chat": {"peerId": group_id, "kind": "group"},
                    "member": {"peerId": member_id, "username": None, "name": digits(member_id.split("@")[0])},
                    "action": action,
                })
            return
        if event not in ("message", "message.any"):
            return
        from_me = bool(payload.get("fromMe"))
        if from_me and payload.get("source") == "api":
            return  # своё из CRM: бэкенд уже записал его при отправке
        remote = payload.get("to") if from_me else payload.get("from")
        remote = serialized(remote)
        if not remote or remote.endswith("@broadcast") or remote.endswith("@newsletter"):
            return
        group = remote.endswith("@g.us")
        if group and remote not in self.groups:
            return
        data = payload.get("_data") or {}
        sender = serialized(payload.get("participant")) if group else remote
        sender_name = payload.get("notifyName") or data.get("notifyName") or (data.get("Info") or {}).get("PushName")
        files = []
        media = payload.get("media") or {}
        if payload.get("hasMedia"):
            content = None
            if media.get("url"):
                try:
                    async with httpx.AsyncClient(timeout=120) as client:
                        response = await client.get(media["url"], headers=waha_headers())
                        if response.status_code < 400:
                            content = response.content
                except Exception:  # noqa: BLE001
                    content = None
            files.append(file_event(media.get("filename") or "file", media.get("mimetype"), content))
        phone = None if group else digits(remote.split("@")[0])
        backend.emit({
            "type": "message",
            "channelId": self.id,
            "chat": {"peerId": remote, "kind": "group" if group else "user", "phone": phone,
                     "title": None if group or from_me else sender_name},
            "message": {
                "externalId": serialized(payload.get("id")),
                "direction": "out" if from_me else "in",
                "senderId": sender,
                "senderName": sender_name,
                "text": payload.get("body") or None,
                "files": files,
                "date": datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc).isoformat()
                if isinstance(payload.get("timestamp"), (int, float)) else None,
            },
        })
