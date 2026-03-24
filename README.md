# Commentator

Инструмент автоматизации для Telegram: комментарии, реакции, дискуссии, мониторинг и веб‑панель управления.

## Архитектура

```
commentator.py          — главный воркер (оркестрация сервисов)
services/               — бизнес-логика (25 модулей)
  commenting.py          — генерация и отправка комментариев
  sending.py             — отправка сообщений с имитацией набора
  message_actions.py     — edit/delete сообщений через Telegram API
  discussions.py         — AI-дискуссии между аккаунтами
  client.py              — обёртка Telethon-клиента, обработка событий
  connection.py          — управление подключениями, reconnect
  outbound.py            — очередь исходящих сообщений (DM, ответы)
  inbox.py               — входящие сообщения, реакции
  reactions.py           — автоматические реакции
  scenarios.py           — сценарии комментирования
  triggers.py            — триггеры по ключевым словам
  monitoring.py          — мониторинг каналов
  comments.py            — генерация текста через AI
  text_analysis.py       — фильтрация постов, дедупликация, diversity
  text_processing.py     — постобработка текста (гуманизация)
  db_queries.py          — запросы к БД (логи, планы, сессии)
  account_utils.py       — загрузка и фильтрация аккаунтов
  profile.py             — управление профилями аккаунтов
  joining.py             — вступление в чаты/каналы
  manual_tasks.py        — ручные задачи из админки
  post_utils.py          — утилиты для работы с постами
  project.py             — мультипроект: фильтрация по project_id
  rebrand.py             — массовое переименование аккаунтов
  replies.py             — обработка ответов на комментарии
  dialogue.py            — AI-ответы в диалогах
admin_web/              — веб-панель (FastAPI)
  routes/                — роуты (13 модулей)
    dashboard.py          — дашборд, предупреждения, старт/стоп
    stats.py              — статистика, экспорт Excel, ребрендинг
    dialogs.py            — DM-переписки и цитаты
    message_actions.py    — edit/delete отправленных сообщений
    discussions.py        — управление AI-дискуссиями
    accounts.py           — управление аккаунтами
    targets.py            — цели комментирования
    settings.py           — глобальные настройки
    personas.py           — роли/персоны для аккаунтов
    reactions.py          — настройка реакций
    monitors.py           — мониторинг каналов
    proxies.py            — управление прокси
    auth.py               — авторизация
  templates/             — Jinja2 шаблоны (Bootstrap 5.3)
  static/                — CSS, JS
db/
  schema.py              — схема БД (SQLite + PostgreSQL)
  connection.py          — подключение к БД
role_engine.py           — движок ролей/персон
tg_device.py             — fingerprinting Telegram-устройств
```

## Возможности веб-панели

- **Дашборд** — статус системы, предупреждения, старт/стоп
- **Статистика** — лог действий с фильтрацией по периоду, экспорт в Excel
- **Диалоги** — DM-переписки аккаунтов, отправка ответов
- **Цитаты** — входящие упоминания и ответы пользователей
- **Edit/Delete** — редактирование и удаление отправленных сообщений прямо из интерфейса (Статистика, Диалоги)
- **Дискуссии** — запуск и управление AI-дискуссиями между аккаунтами
- **Аккаунты** — добавление, проверка статуса, профили
- **Цели** — настройка каналов/чатов для комментирования
- **Настройки** — AI-провайдер, гуманизация, интервалы
- **Персоны** — роли и стили для аккаунтов
- **Реакции, мониторинг, прокси** — дополнительные модули

## Быстрый старт (Docker)

1) Подготовьте конфиги:
```bash
cp config.example.ini config.ini
cp .env.example .env
cp data/ai_settings.example.json data/ai_settings.json
cp data/accounts.example.json data/accounts.json
cp data/proxies.example.txt data/proxies.txt
```

2) Заполните:
- `config.ini` — `api_id`/`api_hash` для Telethon
- `.env` — логин/пароль веб-панели, секрет сессии
- `data/accounts.json` — Telethon session strings аккаунтов
- `data/ai_settings.json` — AI-провайдер, ключи, цели

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
# веб-панель
uvicorn admin_web.main:app --host 0.0.0.0 --port 8080
```

## Тесты

```bash
# в Docker
docker compose run --rm commentator python -m unittest discover -s tests -v

# локально (при наличии .venv)
./.venv/bin/python -m unittest discover -s tests -v
```

## Конфигурация

| Файл | Назначение |
|---|---|
| `config.ini` | Telegram API креды для Telethon |
| `data/ai_settings.json` | AI-провайдеры, модели, цели, роли |
| `data/accounts.json` | Telethon session strings аккаунтов |
| `data/proxies.txt` | Список прокси (опционально) |
| `data/actions.sqlite` | Runtime БД (создаётся автоматически) |

Шаблоны: `config.example.ini`, `data/ai_settings.example.json`, `data/accounts.example.json`, `data/proxies.example.txt`

## База данных

Поддерживается SQLite (по умолчанию) и PostgreSQL (через `DB_URL` в `.env`).

Ключевые таблицы:
- `logs` — история действий (комментарии, реакции, пропуски) с `msg_id` для edit/delete
- `inbox_messages` — входящие/исходящие DM и цитаты
- `outbound_queue` — очередь отправки сообщений
- `discussion_sessions` / `discussion_messages` — AI-дискуссии
- `scenarios`, `triggers`, `post_scenarios` — автоматизация

Миграции применяются автоматически в `init_database()`.

## Безопасность

- Никогда не коммитьте `config.ini`, `.env` и `data/*.json` с реальными ключами
- `.gitignore` исключает runtime данные и секреты
- `ADMIN_WEB_DISABLE_AUTH=1` отключает авторизацию (только для локальной разработки)
