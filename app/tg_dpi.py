"""Обход фильтра провайдера прода walead для Telegram через прокси (взято из бот-поиска).

Прод walead не пускает к Telegram ни напрямую, ни через socks5-прокси, если в запросе CONNECT
адрес дата-центра Telegram указан IP-адресом. Тот же прокси соединяет, если вместо IP прислать
имя хоста — его разрешает уже сам прокси. Поэтому IP Telegram подменяем на имя, которое
резолвится в тот же IP: по умолчанию `<a-b-c-d>.sslip.io`. Шаблон — env TG_PROXY_HOST_TEMPLATE
(`{dashed}` — IP через дефисы, `{ip}` — как есть), пусто — выключено.
"""
import ipaddress
import logging
import os

log = logging.getLogger(__name__)

TEMPLATE = os.getenv("TG_PROXY_HOST_TEMPLATE", "{dashed}.sslip.io").strip()

TELEGRAM_NETS = [ipaddress.ip_network(n) for n in (
    "149.154.160.0/20", "91.108.0.0/16", "91.105.192.0/23", "185.76.151.0/24", "95.161.64.0/20",
)]


def proxied_host(host: str) -> str:
    if not TEMPLATE:
        return host
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return host
    if ip.version != 4 or not any(ip in net for net in TELEGRAM_NETS):
        return host
    return TEMPLATE.format(dashed=str(ip).replace(".", "-"), ip=str(ip))


def install() -> None:
    if not TEMPLATE:
        log.info("tg_dpi: выключен")
        return
    from python_socks.async_.asyncio._proxy import AsyncioProxy

    original = AsyncioProxy.connect

    async def connect(self, dest_host, dest_port, timeout=None, **kwargs):
        return await original(self, proxied_host(dest_host), dest_port, timeout=timeout, **kwargs)

    AsyncioProxy.connect = connect
    log.info("tg_dpi: адреса Telegram через прокси идут именем хоста (%s)", TEMPLATE)
