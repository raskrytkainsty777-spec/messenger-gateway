"""Настройки шлюза мессенджеров CRM — всё из env."""
import os


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


# Бэкенд walead: события шлют сюда, каналы при старте берут отсюда
BACKEND_URL = _env("BACKEND_URL", "http://xleads-backend:3000").rstrip("/")
# Общий токен с бэкендом (CRM_GATEWAY_TOKEN): заголовок X-Gateway-Token в обе стороны
GATEWAY_TOKEN = _env("GATEWAY_TOKEN")

# Telegram: приложение (my.telegram.org) — те же, что у бот-поиска
TELEGRAM_API_ID = int(_env("TELEGRAM_API_ID", "0") or 0)
TELEGRAM_API_HASH = _env("TELEGRAM_API_HASH")
# Как представляемся Telegram — видно в «Устройствах» аккаунта
TELEGRAM_DEVICE = _env("TELEGRAM_DEVICE", "WALEAD CRM")

# WhatsApp: WAHA рядом в compose (движок GOWS), его ключ API и куда он шлёт вебхуки
WAHA_URL = _env("WAHA_URL", "http://waha:3000").rstrip("/")
WAHA_API_KEY = _env("WAHA_API_KEY")
SELF_URL = _env("SELF_URL", "http://gateway:8080").rstrip("/")

# Файлы из мессенджеров больше этого не пересылаем в CRM (остаётся подпись «файл»)
MAX_FILE_BYTES = int(_env("MAX_FILE_BYTES", str(15 * 1024 * 1024)))

# Каталог для служебных файлов (сессии Max — у PyMax только SQLite-файл)
DATA_DIR = _env("DATA_DIR", "/data")
