"""Max-канал: пользовательский аккаунт через PyMax (неофициальный протокол — риск блокировки).

Вход: номер + код из SMS (Client, TCP) или QR (WebClient, WebSocket), при 2FA — пароль.
Сессия (SessionInfo: токен, device_id, user-agent, sync) хранится строкой JSON в CrmChannel.session
вместе с видом клиента: {"kind": "tcp"|"web", "info": {...}} — восстанавливать надо тем же клиентом.
Прокси Max не нужен (сервер в РФ); если задан — только http (загрузка файлов PyMax socks5 не умеет).
"""
import asyncio
import json
import logging
import time
from typing import Any

from pymax import ApiError, Client, ExtraConfig, File, Photo, PyMaxError, WebClient
from pymax.auth.models import AuthResult
from pymax.session import SessionInfo

from . import backend
from .base import Channel, GatewayError, download, file_event, qr_data_url

log = logging.getLogger(__name__)

PHOTO_EXT = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")


class ChannelStore:
    """Хранилище сессии PyMax поверх строки канала: сохранение → событие в бэкенд"""

    def __init__(self, channel: "MaxChannel", kind: str):
        self.channel = channel
        self.kind = kind

    def _info(self) -> SessionInfo | None:
        raw = self.channel.session
        if not raw:
            return None
        try:
            data = json.loads(raw)
            if data.get("kind") != self.kind:
                return None
            return SessionInfo.model_validate(data["info"])
        except Exception:  # noqa: BLE001
            return None

    async def save_session(self, session_info: SessionInfo) -> None:
        self.channel.session = json.dumps({"kind": self.kind, "info": json.loads(session_info.model_dump_json())})
        self.channel.persist()

    async def update_token(self, old_token: str, new_token: str, /) -> None:
        info = self._info()
        if info and info.token == old_token:
            info.token = new_token
            await self.save_session(info)

    async def load_session(self) -> SessionInfo | None:
        return self._info()

    async def load_session_by_device_id(self, device_id: str) -> SessionInfo | None:
        info = self._info()
        return info if info and info.device_id == device_id else None

    async def load_session_by_phone(self, phone: str) -> SessionInfo | None:
        info = self._info()
        return info if info and info.phone == phone else None

    async def delete_session(self, token: str, /) -> None:
        info = self._info()
        if info and info.token == token:
            self.channel.session = None

    async def close(self) -> None:
        return None


class FailFlow:
    """Фоновая работа: сессию отозвали — не шлём новое SMS сами, просим войти заново"""

    async def authenticate(self, app: Any) -> AuthResult:
        raise RuntimeError("relogin")


class SmsFlow:
    """Вход по SMS: код приходит позже ручкой /login/code, пароль 2FA — /login/password"""

    def __init__(self, channel: "MaxChannel"):
        self.channel = channel
        self.code: asyncio.Queue[str] = asyncio.Queue()
        self.password: asyncio.Queue[str] = asyncio.Queue()

    async def authenticate(self, app: Any) -> AuthResult:
        started = await app.api.auth.request_code(app.config.phone)
        self.channel.login = {"step": "code", "hint": "Код пришёл в SMS на номер аккаунта Max"}
        while True:
            code = await self.code.get()
            try:
                result = await app.api.auth.send_code(started.token, code)
                break
            except ApiError as error:
                self.channel.login = {"step": "code", "error": f"Код не подошёл: {error}"}
        return await _finish_auth(self.channel, app, result, self.password)


class QrFlow:
    """Вход по QR (WebClient): показываем ссылку QR, ждём подтверждения на телефоне"""

    def __init__(self, channel: "MaxChannel"):
        self.channel = channel
        self.password: asyncio.Queue[str] = asyncio.Queue()

    async def authenticate(self, app: Any) -> AuthResult:
        for _ in range(20):
            qr = await app.api.auth.request_qr()
            self.channel.login = {
                "step": "qr",
                "qr": qr_data_url(qr.qr_link),
                "hint": "Max на телефоне → Настройки → Устройства → Подключить устройство",
            }
            while time.time() * 1000 < qr.expires_at:
                status = await app.api.auth.check_qr(qr.track_id)
                if status.status.login_available:
                    result = await app.api.auth.confirm_qr(qr.track_id)
                    return await _finish_auth(self.channel, app, result, self.password)
                await asyncio.sleep(max(qr.polling_interval / 1000, 1))
        raise RuntimeError("QR не отсканировали — начните вход заново")


async def _finish_auth(channel: "MaxChannel", app: Any, result: Any, password: asyncio.Queue[str]) -> AuthResult:
    if getattr(result, "login_token", None):
        return AuthResult(token=result.login_token)
    challenge = getattr(result, "password_challenge", None)
    if challenge:
        channel.login = {"step": "password", "hint": f"Облачный пароль Max{f' (подсказка: {challenge.hint})' if challenge.hint else ''}"}
        while True:
            value = await password.get()
            try:
                checked = await app.api.auth.check_password(challenge.track_id, value)
            except ApiError as error:
                channel.login = {"step": "password", "error": f"Пароль не подошёл: {error}"}
                continue
            if checked.login_token:
                return AuthResult(token=checked.login_token)
    raise RuntimeError("Номер не зарегистрирован в Max")


class MaxChannel(Channel):
    type = "MAX"

    def __init__(self, data: dict[str, Any]):
        super().__init__(data)
        self.client: Client | WebClient | None = None
        self._task: asyncio.Task | None = None
        self._login_task: asyncio.Task | None = None
        self._flow: SmsFlow | QrFlow | None = None
        self._sent: set[str] = set()

    # ── сессия ──

    def _kind(self) -> str:
        try:
            return json.loads(self.session or "{}").get("kind") or "tcp"
        except ValueError:
            return "tcp"

    def persist(self) -> None:
        backend.emit({"type": "status", "channelId": self.id, "status": self.status, "statusText": self.status_text, "session": self.session})

    def _extra(self, kind: str, relogin: bool) -> ExtraConfig:
        proxy = None
        if self.proxy and self.proxy.get("host"):
            auth = f"{self.proxy.get('user')}:{self.proxy.get('pass')}@" if self.proxy.get("user") else ""
            proxy = f"{(self.proxy.get('type') or 'http').lower()}://{auth}{self.proxy['host']}:{self.proxy.get('port')}"
        return ExtraConfig(store=ChannelStore(self, kind), persist_session=True, proxy=proxy, relogin=relogin, telemetry=False)

    def _make(self, kind: str, flow: Any, relogin: bool) -> Client | WebClient:
        if kind == "web":
            return WebClient(extra_config=self._extra(kind, relogin), auth_flow=flow)
        phone = "+" + (self.phone or "70000000000").lstrip("+")
        return Client(phone=phone, extra_config=self._extra(kind, relogin), auth_flow=flow)

    def _set(self, status: str, text: str | None = None, **extra: Any) -> None:
        self.status = status
        self.status_text = text
        backend.emit({"type": "status", "channelId": self.id, "status": status, "statusText": text, **extra})

    def _account(self) -> dict[str, Any] | None:
        me = getattr(self.client, "me", None) if self.client else None
        if not me:
            return None
        contact = me.contact
        name = contact.names[0] if contact.names else None
        return {
            "id": str(contact.id),
            "phone": str(contact.phone) if contact.phone else None,
            "name": " ".join(filter(None, [getattr(name, "first_name", None), getattr(name, "last_name", None)])) or None,
            "username": None,
        }

    # ── жизненный цикл ──

    async def start(self) -> None:
        await self.stop()
        if not self.session:
            self._set("RELOGIN", "Нет сессии — нужен вход")
            return
        client = self._make(self._kind(), FailFlow(), relogin=True)
        self._attach(client)
        self.client = client
        self._task = asyncio.create_task(self._run(client))

    async def _run(self, client: Client | WebClient) -> None:
        try:
            await client.start()
        except Exception as error:  # noqa: BLE001
            if str(error) == "relogin":
                self._set("RELOGIN", "Max отозвал сессию — войдите заново")
            else:
                self._set("ERROR", f"Max: {error}")

    async def stop(self, logout: bool = False) -> None:
        if self.client:
            try:
                await self.client.stop()
            except Exception:  # noqa: BLE001
                pass
        if self._task and not self._task.done():
            self._task.cancel()
        self.client = None

    # ── вход ──

    async def login_start(self, method: str) -> dict[str, Any]:
        await self.stop()
        if self._login_task and not self._login_task.done():
            self._login_task.cancel()
        kind = "tcp" if method == "code" else "web"
        if kind == "tcp" and not self.phone:
            raise GatewayError("Укажите номер аккаунта")
        self.session = None
        self._flow = SmsFlow(self) if kind == "tcp" else QrFlow(self)
        client = self._make(kind, self._flow, relogin=False)
        self.login = {"step": "wait", "hint": "Подключаемся к Max…"}
        self._login_task = asyncio.create_task(self._login(client))
        for _ in range(30):  # ждём, пока Max выдаст QR или отправит SMS
            await asyncio.sleep(0.5)
            if self.login.get("step") != "wait":
                break
        return self.login

    async def _login(self, client: Client | WebClient) -> None:
        try:
            await client.connect()  # вход: SessionInfo сохраняется в ChannelStore
            self.client = client
            account = self._account()
            await client.close()
            self.login = {"step": "done"}
            self._set("ACTIVE", None, session=self.session, account=account)
            await self.start()
        except Exception as error:  # noqa: BLE001
            self.login = {"step": "error", "error": str(error)}
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass

    async def login_code(self, code: str) -> dict[str, Any]:
        if not isinstance(self._flow, SmsFlow):
            raise GatewayError("Сначала запросите код")
        await self._flow.code.put(code)
        await asyncio.sleep(2)
        return self.login

    async def login_password(self, password: str) -> dict[str, Any]:
        if not self._flow:
            raise GatewayError("Сначала начните вход")
        await self._flow.password.put(password)
        await asyncio.sleep(2)
        return self.login

    # ── события ──

    def _attach(self, client: Client | WebClient) -> None:
        @client.on_start()
        async def started(_client: Any) -> None:
            self._set("ACTIVE", None, account=self._account())

        @client.on_message()
        async def incoming(message: Any, _client: Any) -> None:
            await self._on_message(message)

    async def _on_message(self, message: Any) -> None:
        client = self.client
        if not client or message.chat_id is None:
            return
        my_id = client.me.contact.id if client.me else None
        if str(message.id) in self._sent:
            self._sent.discard(str(message.id))
            return
        try:
            chat = await client.get_chat(message.chat_id)
        except Exception:  # noqa: BLE001
            chat = None
        kind = getattr(getattr(chat, "type", None), "value", getattr(chat, "type", None))
        group = kind not in ("DIALOG", None)
        if kind == "CHANNEL":
            return
        if group and str(message.chat_id) not in self.groups:
            return
        outgoing = my_id is not None and message.sender == my_id
        files = []
        for attach in message.attaches or []:
            name = getattr(attach, "name", None) or "file"
            url = getattr(attach, "base_url", None) or getattr(attach, "url", None)
            if not url and getattr(attach, "file_id", None):
                try:
                    request = await client.get_file_by_id(message.chat_id, message.id, attach.file_id)
                    url = getattr(request, "url", None)
                except Exception:  # noqa: BLE001
                    url = None
            if not url:
                continue
            try:
                data = await download(url)
            except Exception:  # noqa: BLE001
                data = None
            files.append(file_event(name if "." in name else f"{name}.jpg" if getattr(attach, "base_url", None) else name, None, data))
        peer_user = None
        if not group:
            other = next((uid for uid in (getattr(chat, "participants", None) or {}) if uid != my_id), None)
            peer_user = other if other is not None else (message.sender if not outgoing else None)
        sender_name = None
        if not outgoing and message.sender:
            try:
                user = await client.get_user(message.sender)
                first = user.names[0] if user and user.names else None
                sender_name = " ".join(filter(None, [getattr(first, "first_name", None), getattr(first, "last_name", None)])) or None
            except Exception:  # noqa: BLE001
                pass
        backend.emit({
            "type": "message",
            "channelId": self.id,
            "chat": {
                "peerId": str(message.chat_id),
                "accessHash": str(peer_user) if peer_user else None,  # у Max — id собеседника
                "kind": "group" if group else "user",
                "title": getattr(chat, "title", None) if group else sender_name,
            },
            "message": {
                "externalId": str(message.id),
                "direction": "out" if outgoing else "in",
                "senderId": str(message.sender) if message.sender else None,
                "senderName": sender_name,
                "text": message.text or None,
                "files": files,
                "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(message.time / 1000)) if getattr(message, "time", None) else None,
            },
        })

    # ── работа ──

    def _client(self) -> Client | WebClient:
        if not self.client or self.status != "ACTIVE" or not getattr(self.client, "me", None):
            raise GatewayError("Канал Max не подключён")
        return self.client

    async def _user_by_phone(self, phone: str) -> Any:
        client = self._client()
        digits = "".join(ch for ch in phone if ch.isdigit())
        try:
            return await client.search_by_phone("+" + digits)
        except PyMaxError as error:
            raise GatewayError("У номера нет Max") from error

    async def resolve(self, peer: dict[str, Any], name: str | None = None) -> dict[str, Any]:
        client = self._client()
        if peer.get("peerId"):
            return {"peerId": peer["peerId"], "accessHash": peer.get("accessHash"), "kind": peer.get("kind") or "user"}
        if not peer.get("phone"):
            raise GatewayError("Нет номера собеседника")
        user = await self._user_by_phone(peer["phone"])
        chat_id = client.get_chat_id(client.me.contact.id, user.id)
        first = user.names[0] if user.names else None
        title = " ".join(filter(None, [getattr(first, "first_name", None), getattr(first, "last_name", None)])) or name
        return {"peerId": str(chat_id), "accessHash": str(user.id), "kind": "user", "title": title,
                "phone": "".join(ch for ch in peer["phone"] if ch.isdigit())}

    async def send(self, peer: dict[str, Any], text: str, files: list[dict[str, Any]]) -> dict[str, Any]:
        client = self._client()
        chat = await self.resolve(peer)
        attachments = []
        for item in files:
            data = await download(item["url"])
            name = item.get("name") or "file"
            attachments.append(Photo(raw=data, name=name) if name.lower().endswith(PHOTO_EXT) else File(raw=data, name=name))
        try:
            message = await client.send_message(int(chat["peerId"]), text or None, attachments=attachments or None)
        except PyMaxError as error:
            raise GatewayError(f"Max: {error}") from error
        self._sent.add(str(message.id))
        return {"chat": chat, "externalIds": [str(message.id)]}

    async def check(self, phone: str) -> dict[str, Any]:
        try:
            chat = await self.resolve({"phone": phone})
            return {"exists": True, "chat": chat}
        except GatewayError:
            return {"exists": False, "chat": None}

    async def create_group(self, title: str) -> dict[str, Any]:
        client = self._client()
        try:
            created = await client.create_group(title[:100], participant_ids=[])
        except PyMaxError as error:
            raise GatewayError(f"Max: {error}") from error
        if not created:
            raise GatewayError("Max не вернул группу")
        chat = created[0]
        link = getattr(chat, "link", None)
        if not link:
            try:
                link = (await client.rework_invite_link(chat.id)).link
            except Exception:  # noqa: BLE001
                link = None
        self.groups.add(str(chat.id))
        return {"chat": {"peerId": str(chat.id), "kind": "group", "title": title}, "inviteLink": link}
