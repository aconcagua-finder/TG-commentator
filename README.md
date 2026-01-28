# Commentator

Инструмент автоматизации для Telegram: комментарии, реакции и мониторинг с админ‑ботом и веб‑панелью.

## Компоненты
- `commentator.py` — основной воркер (комменты/реакции/сценарии).
- `admin_bot.py` — Telegram админ‑бот для управления настройками и аккаунтами.
- `admin_web/` — веб‑панель (FastAPI) для конфигурации и мониторинга.

## Быстрый старт (Docker)
1) Подготовьте локальные конфиги:
```bash
cp config.example.ini config.ini
cp .env.example .env
cp data/ai_settings.example.json data/ai_settings.json
cp data/accounts.example.json data/accounts.json
cp data/proxies.example.txt data/proxies.txt
```

2) Заполните:
- `config.ini` (токен бота, ID админов, `api_id`/`api_hash`).
- `.env` (логин/пароль веб‑панели и секрет, опционально).
- `data/accounts.json` (Telethon session strings аккаунтов).
- `data/ai_settings.json` (провайдер AI, ключи, цели).

3) Запуск:
```bash
docker compose up --build
```

## Локальный запуск (Python)
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export APP_DATA_DIR=./data
python commentator.py
# в другом терминале
python admin_bot.py
# веб‑панель
uvicorn admin_web.main:app --host 0.0.0.0 --port 8080
```

## Конфигурация
- `config.ini` — токен бота + Telegram API креды.
- `data/ai_settings.json` — AI провайдеры, модели, цели, роли, и т.д.
- `data/accounts.json` — Telethon session strings аккаунтов.
- `data/proxies.txt` — список прокси (опционально).
- `data/actions.sqlite` — runtime БД (создаётся автоматически).

Шаблоны:
- `config.example.ini`
- `data/ai_settings.example.json`
- `data/accounts.example.json`
- `data/proxies.example.txt`

## Безопасность
- Никогда не коммитьте `config.ini`, `.env` и `data/*.json` с реальными ключами.
- `.gitignore` уже исключает runtime данные и секреты.
- Если секреты уже были опубликованы — немедленно отзовите/замените их.

## Примечания
- `APP_DATA_DIR` задаёт путь к runtime данным (по умолчанию корень репозитория).
- `ADMIN_WEB_DISABLE_AUTH=1` отключает авторизацию веб‑панели (только для локальной разработки).
