"""Telegram-канал: пользовательский аккаунт через Telethon (StringSession в базе walead).

Вход — QR (Telegram на телефоне → Устройства → Подключить) или номер + код, при 2FA — пароль.
Сессия живёт строкой: после входа уходит в бэкенд событием status и хранится в CrmChannel.session.
Кэш сущностей StringSession не переживает перезапуск, поэтому доступ к собеседнику — по паре
peerId + accessHash, которую бэкенд хранит у чата и присылает при отправке.
"""
import asyncio
import io
import logging
import random
from typing import Any

from telethon import TelegramClient, events, functions, types, utils
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PeerFloodError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    SessionPasswordNeededError,
    UserDeactivatedBanError,
    AuthKeyUnregisteredError,
)
from telethon.sessions import StringSession

from . import backend, config
from .base import Channel, GatewayError, download, file_event, qr_data_url

log = logging.getLogger(__name__)

SERVICE_USER_ID = 777000  # служебные сообщения Telegram (коды входа) в CRM не тянем


def chat_info(entity: Any) -> dict[str, Any]:
    if isinstance(entity, types.User):
        name = " ".join(filter(None, [entity.first_name, entity.last_name])) or None
        return {
            "peerId": str(entity.id),
            "accessHash": str(entity.access_hash) if entity.access_hash else None,
            "kind": "user",
            "title": name,
            "username": entity.username,
            "phone": entity.phone,
        }
    if isinstance(entity, types.Channel):
        return {
            "peerId": str(entity.id),
            "accessHash": str(entity.access_hash) if entity.access_hash else None,
            "kind": "group",
            "title": entity.title,
            "username": entity.username,
            "phone": None,
        }
    if isinstance(entity, types.Chat):
        return {"peerId": str(entity.id), "accessHash": None, "kind": "group", "title": entity.title, "username": None, "phone": None}
    raise GatewayError("Неизвестный тип собеседника")


def human_error(error: Exception) -> str:
    if isinstance(error, FloodWaitError):
        return f"Telegram просит подождать {error.seconds} с"
    if isinstance(error, PeerFloodError):
        return "Telegram ограничил аккаунт за спам (PEER_FLOOD) — проверьте через @SpamBot"
    text = str(error)
    if "PRIVACY" in text.upper():
        return "Человек запретил писать ему незнакомым (настройки приватности)"
    return text[:300]


class TelegramChannel(Channel):
    type = "TELEGRAM"

    def __init__(self, data: dict[str, Any]):
        super().__init__(data)
        self.client: TelegramClient | None = None
        self.me: types.User | None = None
        self._login_client: TelegramClient | None = None
        self._login_task: asyncio.Task | None = None
        self._phone_code_hash: str | None = None
        self._sent: set[int] = set()  # id своих отправок из CRM — их эхо не шлём обратно
        self._lock = asyncio.Lock()

    # ── клиент ──────────────────────────────────────────────────────────────

    def _proxy(self) -> dict[str, Any] | None:
        if not self.proxy or not self.proxy.get("host"):
            return None
        kind = (self.proxy.get("type") or "socks5").lower()
        return {
            "proxy_type": "socks5" if kind == "socks5" else ("socks4" if kind == "socks4" else "http"),
            "addr": self.proxy["host"],
            "port": int(self.proxy.get("port") or 1080),
            "username": self.proxy.get("user") or None,
            "password": self.proxy.get("pass") or None,
            "rdns": True,
        }

    def _make_client(self, session: str | None) -> TelegramClient:
        if not config.TELEGRAM_API_ID or not config.TELEGRAM_API_HASH:
            raise GatewayError("У шлюза не заданы TELEGRAM_API_ID / TELEGRAM_API_HASH")
        return TelegramClient(
            StringSession(session or ""),
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
            proxy=self._proxy(),
            device_model=config.TELEGRAM_DEVICE,
            system_version="1.0",
            app_version="1.0",
            lang_code="ru",
            system_lang_code="ru",
            connection_retries=None,  # переподключаемся сами, сколько потребуется
            retry_delay=5,
            auto_reconnect=True,
            flood_sleep_threshold=20,
        )

    def _set_status(self, status: str, text: str | None = None, **extra: Any) -> None:
        self.status = status
        self.status_text = text
        backend.emit({"type": "status", "channelId": self.id, "status": status, "statusText": text, **extra})

    def _account(self) -> dict[str, Any] | None:
        if not self.me:
            return None
        return {
            "id": str(self.me.id),
            "username": self.me.username,
            "phone": self.me.phone,
            "name": " ".join(filter(None, [self.me.first_name, self.me.last_name])) or None,
        }

    async def start(self) -> None:
        async with self._lock:
            await self._disconnect()
            if not self.session:
                self._set_status("RELOGIN", "Нет сессии — нужен вход")
                return
            client = self._make_client(self.session)
            try:
                await asyncio.wait_for(client.connect(), timeout=60)
                if not await client.is_user_authorized():
                    await client.disconnect()
                    self._set_status("RELOGIN", "Сессия больше не действует — войдите заново")
                    return
                self.me = await client.get_me()
            except (AuthKeyUnregisteredError, UserDeactivatedBanError, PhoneNumberBannedError) as error:
                await client.disconnect()
                self._set_status("BANNED" if not isinstance(error, AuthKeyUnregisteredError) else "RELOGIN", str(error))
                return
            except Exception as error:  # noqa: BLE001
                await client.disconnect()
                self._set_status("ERROR", f"Не подключился: {error}")
                return
            self.client = client
            self._attach(client)
            self._set_status("ACTIVE", None, account=self._account())
            log.info("telegram #%s подключён как %s", self.id, self.me.username or self.me.id)

    async def _disconnect(self) -> None:
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self.client = None

    async def stop(self, logout: bool = False) -> None:
        async with self._lock:
            if logout and self.client:
                try:
                    await self.client.log_out()
                except Exception:  # noqa: BLE001
                    pass
            await self._disconnect()
            self.status = "OFF"

    # ── вход ────────────────────────────────────────────────────────────────

    async def _cancel_login(self) -> None:
        if self._login_task and not self._login_task.done():
            self._login_task.cancel()
        if self._login_client and self._login_client is not self.client:
            try:
                await self._login_client.disconnect()
            except Exception:  # noqa: BLE001
                pass
        self._login_client = None

    async def login_start(self, method: str) -> dict[str, Any]:
        await self._cancel_login()
        client = self._make_client(None)
        try:
            await asyncio.wait_for(client.connect(), timeout=60)
        except Exception as error:  # noqa: BLE001
            raise GatewayError(f"Не удалось подключиться к Telegram (проверьте прокси): {error}") from error
        self._login_client = client
        if method == "code":
            if not self.phone:
                raise GatewayError("Укажите номер аккаунта")
            try:
                sent = await client.send_code_request("+" + self.phone.lstrip("+"))
            except Exception as error:  # noqa: BLE001
                raise GatewayError(human_error(error)) from error
            self._phone_code_hash = sent.phone_code_hash
            self.login = {"step": "code", "hint": "Код пришёл в Telegram на другом устройстве (или SMS)"}
            return self.login
        qr = await client.qr_login()
        self.login = {"step": "qr", "qr": qr_data_url(qr.url), "hint": "Telegram → Настройки → Устройства → Подключить устройство"}
        self._login_task = asyncio.create_task(self._wait_qr(client, qr))
        return self.login

    async def _wait_qr(self, client: TelegramClient, qr: Any) -> None:
        for _ in range(40):  # ~20 минут, QR обновляется каждые ~30 с
            try:
                await qr.wait(timeout=25)
                await self._finish_login(client)
                return
            except asyncio.TimeoutError:
                await qr.recreate()
                self.login = {**self.login, "qr": qr_data_url(qr.url)}
            except SessionPasswordNeededError:
                self.login = {"step": "password", "hint": "У аккаунта облачный пароль (2FA) — введите его"}
                return
            except Exception as error:  # noqa: BLE001
                self.login = {"step": "error", "error": human_error(error)}
                return
        self.login = {"step": "error", "error": "QR не отсканировали — начните вход заново"}

    async def login_code(self, code: str) -> dict[str, Any]:
        client = self._login_client
        if not client or not self._phone_code_hash:
            raise GatewayError("Сначала запросите код")
        try:
            await client.sign_in(phone="+" + (self.phone or "").lstrip("+"), code=code, phone_code_hash=self._phone_code_hash)
        except SessionPasswordNeededError:
            self.login = {"step": "password", "hint": "У аккаунта облачный пароль (2FA) — введите его"}
            return self.login
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as error:
            raise GatewayError("Код не подошёл или истёк — запросите новый") from error
        await self._finish_login(client)
        return self.login

    async def login_password(self, password: str) -> dict[str, Any]:
        client = self._login_client
        if not client:
            raise GatewayError("Сначала начните вход")
        try:
            await client.sign_in(password=password)
        except PasswordHashInvalidError as error:
            raise GatewayError("Неверный пароль") from error
        await self._finish_login(client)
        return self.login

    async def _finish_login(self, client: TelegramClient) -> None:
        async with self._lock:
            await self._disconnect()
            self.session = client.session.save()
            self.me = await client.get_me()
            self.client = client
            self._login_client = None
            self._attach(client)
            self.login = {"step": "done"}
            self._set_status("ACTIVE", None, session=self.session, account=self._account())
            log.info("telegram #%s: вход выполнен (%s)", self.id, self.me.username or self.me.id)

    # ── события ─────────────────────────────────────────────────────────────

    def _attach(self, client: TelegramClient) -> None:
        client.add_event_handler(self._on_message, events.NewMessage())
        client.add_event_handler(self._on_action, events.ChatAction())

    def _tracked(self, chat: Any) -> bool:
        """Личные диалоги — все; группы — только рабочие группы CRM (не чужие чаты аккаунта)"""
        if isinstance(chat, types.User):
            return not chat.bot and chat.id != SERVICE_USER_ID and not chat.is_self
        return str(getattr(chat, "id", "")) in self.groups

    async def _on_message(self, event: events.NewMessage.Event) -> None:
        message = event.message
        if message.out and message.id in self._sent:
            self._sent.discard(message.id)
            return
        try:
            chat = await event.get_chat()
            if not self._tracked(chat):
                return
            sender = await event.get_sender() if not message.out else self.me
            files = []
            if message.media and not isinstance(message.media, (types.MessageMediaWebPage,)):
                files.append(await self._download(message))
            backend.emit({
                "type": "message",
                "channelId": self.id,
                "chat": chat_info(chat),
                "message": {
                    "externalId": str(message.id),
                    "direction": "out" if message.out else "in",
                    "senderId": str(sender.id) if sender else None,
                    "senderName": (" ".join(filter(None, [getattr(sender, "first_name", None), getattr(sender, "last_name", None)]))
                                   or getattr(sender, "title", None)) if sender else None,
                    "text": message.message or None,
                    "files": files,
                    "date": message.date.isoformat() if message.date else None,
                },
            })
        except Exception as error:  # noqa: BLE001
            log.warning("telegram #%s: сообщение не разобрано: %s", self.id, error)

    async def _download(self, message: Any) -> dict[str, Any]:
        name = "photo.jpg" if message.photo else (message.file.name if message.file and message.file.name else "file")
        mime = message.file.mime_type if message.file else None
        size = message.file.size if message.file else None
        if size and size > config.MAX_FILE_BYTES:
            return file_event(name, mime, None)
        try:
            data = await message.download_media(file=bytes)
        except Exception:  # noqa: BLE001
            data = None
        return file_event(name, mime, data)

    async def _on_action(self, event: events.ChatAction.Event) -> None:
        if not (event.user_joined or event.user_added or event.user_left or event.user_kicked):
            return
        try:
            chat = await event.get_chat()
            if str(chat.id) not in self.groups:
                return
            users = await event.get_users()
            for user in users or []:
                backend.emit({
                    "type": "member",
                    "channelId": self.id,
                    "chat": chat_info(chat),
                    "member": {
                        "peerId": str(user.id),
                        "username": user.username,
                        "name": " ".join(filter(None, [user.first_name, user.last_name])) or None,
                    },
                    "action": "joined" if (event.user_joined or event.user_added) else "left",
                })
        except Exception as error:  # noqa: BLE001
            log.warning("telegram #%s: событие группы не разобрано: %s", self.id, error)

    # ── работа ──────────────────────────────────────────────────────────────

    def _client(self) -> TelegramClient:
        if not self.client or self.status != "ACTIVE":
            raise GatewayError("Канал не подключён")
        return self.client

    async def _entity(self, peer: dict[str, Any], name: str | None = None) -> Any:
        client = self._client()
        peer_id, access_hash = peer.get("peerId"), peer.get("accessHash")
        if peer_id and access_hash:
            if peer.get("kind") == "group":
                return await client.get_entity(types.InputPeerChannel(int(peer_id), int(access_hash)))
            return await client.get_entity(types.InputPeerUser(int(peer_id), int(access_hash)))
        if peer_id and peer.get("kind") == "group":
            return await client.get_entity(types.PeerChat(int(peer_id)))
        if peer.get("username"):
            try:
                return await client.get_entity(peer["username"])
            except Exception:  # noqa: BLE001 — username сменился: попробуем по номеру
                if not peer.get("phone"):
                    raise
        if peer.get("phone"):
            return await self._import_phone(peer["phone"], name)
        raise GatewayError("Нет ни id, ни username, ни номера собеседника")

    async def _import_phone(self, phone: str, name: str | None) -> types.User:
        client = self._client()
        digits = "".join(ch for ch in phone if ch.isdigit())
        result = await client(functions.contacts.ImportContactsRequest([
            types.InputPhoneContact(client_id=random.randrange(1, 2**62), phone="+" + digits,
                                    first_name=(name or digits)[:60], last_name="")
        ]))
        if not result.users:
            raise GatewayError("У номера нет Telegram или он скрыт настройками приватности")
        return result.users[0]

    async def resolve(self, peer: dict[str, Any], name: str | None = None) -> dict[str, Any]:
        try:
            return chat_info(await self._entity(peer, name))
        except GatewayError:
            raise
        except Exception as error:  # noqa: BLE001
            raise GatewayError(human_error(error)) from error

    async def send(self, peer: dict[str, Any], text: str, files: list[dict[str, Any]]) -> dict[str, Any]:
        client = self._client()
        try:
            entity = await self._entity(peer)
            ids: list[str] = []
            if files:
                payload = []
                for item in files:
                    data = io.BytesIO(await download(item["url"]))
                    data.name = item.get("name") or "file"
                    payload.append(data)
                caption = text if text and len(text) <= 1000 else None
                if text and not caption:
                    sent = await client.send_message(entity, text)
                    self._sent.add(sent.id)
                    ids.append(str(sent.id))
                for index, data in enumerate(payload):
                    sent = await client.send_file(entity, data, caption=caption if index == 0 else None,
                                                  force_document=not data.name.lower().endswith((".jpg", ".jpeg", ".png", ".webp")))
                    self._sent.add(sent.id)
                    ids.append(str(sent.id))
            elif text:
                sent = await client.send_message(entity, text)
                self._sent.add(sent.id)
                ids.append(str(sent.id))
            return {"chat": chat_info(entity), "externalIds": ids}
        except GatewayError:
            raise
        except Exception as error:  # noqa: BLE001
            raise GatewayError(human_error(error)) from error

    async def check(self, phone: str) -> dict[str, Any]:
        try:
            user = await self._import_phone(phone, None)
            return {"exists": True, "chat": chat_info(user)}
        except GatewayError:
            return {"exists": False, "chat": None}

    async def create_group(self, title: str) -> dict[str, Any]:
        client = self._client()
        try:
            result = await client(functions.channels.CreateChannelRequest(title=title[:128], about="", megagroup=True))
            channel = result.chats[0]
            self.groups.add(str(channel.id))
            invite = await client(functions.messages.ExportChatInviteRequest(peer=types.InputPeerChannel(channel.id, channel.access_hash)))
            return {"chat": chat_info(channel), "inviteLink": getattr(invite, "link", None)}
        except Exception as error:  # noqa: BLE001
            raise GatewayError(human_error(error)) from error


__all__ = ["TelegramChannel", "utils"]
