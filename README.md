# Commentator

Инструмент автоматизации для Telegram: комментарии, реакции, дискуссии, мониторинг и веб‑панель управления.

## Архитектура

```
commentator.py          — главный воркер (оркестрация сервисов)
services/               — бизнес-логика (29 модулей)
  commenting.py          — генерация и отправка комментариев
  sending.py             — отправка сообщений с имитацией набора
  message_actions.py     — edit/delete сообщений через Telegram API
  discussions.py         — AI-дискуссии между аккаунтами (сцены, антиповтор, реакции, digest)
  discussions_director.py — «театральная постановка»: cast map, фазы, quote/reaction routing
  client.py              — обёртка Telethon-клиента, обработка событий
  connection.py          — управление подключениями, reconnect
  outbound.py            — очередь исходящих сообщений (DM, ответы)
  inbox.py               — входящие сообщения, реакции
  reactions.py           — автоматические реакции на посты + одиночные лайки в обсуждениях
  scenarios.py           — сценарии комментирования
  triggers.py            — триггеры по ключевым словам
  monitoring.py          — мониторинг каналов
  antispam.py            — антиспам: ключевые слова, AI-проверка, удаление и бан
  comments.py            — генерация текста через AI (generate_comment, generate_digest)
  text_analysis.py       — фильтрация постов, дедупликация, similarity, diversity hints
  text_processing.py     — постобработка текста (гуманизация, умный split по границам предложений с учётом русских сокращений)
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
  telegram_bot.py        — уведомления через Telegram Bot API
  warning_notifier.py    — отправка уведомлений о новых предупреждениях
admin_web/              — веб-панель (FastAPI + Bootstrap 5.3)
  routes/                — роуты (15 модулей)
    dashboard.py          — дашборд, последняя активность, старт/стоп
    stats.py              — статистика, экспорт Excel, ручная очередь, ребрендинг
    dialogs.py            — DM-переписки и цитаты (+ bulk-операции)
    message_actions.py    — edit/delete отправленных сообщений
    discussions.py        — управление AI-дискуссиями и сессиями
    antispam.py           — правила антиспама, логи, разблокировка
    notifications.py      — настройки Telegram-бота для уведомлений
    accounts.py           — управление аккаунтами
    targets.py            — цели комментирования (включая сценарии и триггеры)
    settings.py           — глобальные настройки и AI
    personas.py           — роли/персоны для аккаунтов
    reactions.py          — настройка реакций
    monitors.py           — мониторинг каналов
    proxies.py            — управление прокси
    auth.py               — авторизация
  templates/             — Jinja2 шаблоны
  static/                — CSS, JS (включая list_sort.js, message_actions.js)
  helpers.py             — общие хелперы для роутов
  activity_helpers.py    — обогащение action-логов для дашборда/stats
  sort_helpers.py        — единый каталог сортировок для списочных страниц
db/
  schema.py              — схема БД (PostgreSQL)
  connection.py          — пул подключений (psycopg2 sync, asyncpg async)
role_engine.py           — движок ролей/персон
tg_device.py             — fingerprinting Telegram-устройств
```

## Возможности веб-панели

- **Дашборд** — статус системы, быстрый старт/стоп, **обогащённая лента последней активности** с прямыми ссылками на посты в Telegram, ссылками на цели в админке, бейджами роли/настроения и значками результата
- **Статистика** — действия за день/неделю/месяц со **сводными карточками по типам** (комментарии, ответы, реакции, ошибки, антиспам, мониторинг), кликабельный фильтр по типу, экспорт в Excel
- **Сортировки** — на всех списочных страницах (`/accounts`, `/targets`, `/reaction-targets`, `/monitor-targets`, `/discussions`, `/antispam-targets`, `/proxies`, `/personas`) есть выпадающий список сортировок (новые/старые, А→Я, статус, последняя активность). Выбор запоминается в localStorage и переживает перезагрузки
- **Предупреждения** — сигналы об ошибках и блокировках, ручное «Прочитать все» и массовое скрытие (скрытые возвращаются при новой ошибке того же типа)
- **Уведомления** — настройка Telegram-бота, выбор событий (ошибки, DM, цитаты, мониторинг, спам)
- **Диалоги** — DM-переписки аккаунтов, отправка ответов, ручное «Прочитать все» и массовое удаление переписок
- **Цитаты** — входящие упоминания и ответы пользователей, ручное «Прочитать все» и массовое удаление
- **Антиспам** — фильтрация по ключевым словам и именам отправителей, AI-проверка, автоматическое удаление и бан, ручное повторное сканирование, разблокировка через UI. **Различает удачное удаление и провал** (`spam_deleted` / `spam_failed`): провал даёт отдельный тип лога с красной иконкой и предупреждение в Telegram-уведомлении вместо «успешно удалено»
- **Edit/Delete** — редактирование и удаление отправленных сообщений прямо из интерфейса (Статистика, Диалоги)
- **Дискуссии** — запуск и управление AI-дискуссиями между аккаунтами, история сессий и сообщений. Поддерживают сцены со своими операторами, антиповтор (similarity + retry + emergency fallback), «театральную постановку» (управляемое цитирование, эпизодические эмодзи-реакции между ботами, свёрнутую «летопись» сцены через LLM-digest), автоисключение оператора сцены из её участников
- **Аккаунты** — добавление, проверка статуса, профили
- **Цели** — настройка каналов/чатов для комментирования, сценарии и триггеры
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
- `.env` — креды PostgreSQL (`POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`), логин/пароль веб-панели, секрет сессии
- `data/accounts.json` — Telethon session strings аккаунтов
- `data/ai_settings.json` — AI-провайдер, ключи, цели

3) Запуск:
```bash
docker compose up --build
```

Compose поднимает три контейнера: `postgres` (БД), `commentator` (бот) и `admin_web` (веб-панель на http://127.0.0.1:8080). Бот и админка ждут готовности Postgres перед стартом, а схема таблиц создаётся автоматически при первом запуске через `init_database()`.

## Локальный запуск (Python)

PostgreSQL обязателен — поднимите его отдельно или запустите только postgres-контейнер из compose:

```bash
docker compose up -d postgres
```

Затем:
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export APP_DATA_DIR=./data
export DB_URL=postgres://commentator:change_me@localhost:5432/commentator
python commentator.py
# веб-панель в отдельном терминале (с теми же env)
uvicorn admin_web.main:app --host 0.0.0.0 --port 8080
```

## Тесты

В `tests/` лежат standalone unit-тесты, которые не требуют ни базы, ни Telethon, ни сети — они покрывают чистые хелперы:

- `test_activity_helpers.py` — обогащение action-логов (парсинг роли/настроения, построение Telegram-ссылок, мета типов)
- `test_sort_helpers.py` — каталог сортировок для списочных страниц + защита SQL ORDER BY от инъекций
- `test_spam_notification.py` — формирование Telegram-уведомлений антиспама (удалено / не удалено)
- `test_admin_web_utils.py` — утилиты `admin_web/helpers.py`
- `test_role_engine.py` — движок ролей/персон
- `test_smoke_imports.py` — smoke-импорты приложения
- `test_text_processing.py` — умный split текста с защитой от сплита по русским сокращениям (`ст. 437`, `т.д.`, `т.е.`, инициалы и т.п.)

```bash
# в Docker
docker compose run --rm commentator python -m unittest discover -s tests -v

# локально (при наличии .venv)
./.venv/bin/python -m unittest discover -s tests -v
```

## Конфигурация

| Файл / переменная | Назначение |
|---|---|
| `config.ini` | Telegram API креды для Telethon (`api_id`, `api_hash`) |
| `.env` → `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Креды PostgreSQL |
| `.env` → `DB_URL` (compose выставляет автоматически) | Connection string PostgreSQL |
| `.env` → `ADMIN_WEB_USERNAME` / `ADMIN_WEB_PASSWORD` / `ADMIN_WEB_SECRET` | Авторизация веб-панели |
| `data/ai_settings.json` | AI-провайдеры, модели, цели, роли, настройки антиспама |
| `data/accounts.json` | Telethon session strings аккаунтов |
| `data/proxies.txt` | Список прокси (опционально, обычно проще заводить их через `/proxies`) |

Шаблоны: `config.example.ini`, `.env.example`, `data/ai_settings.example.json`, `data/accounts.example.json`, `data/proxies.example.txt`

## База данных

**Только PostgreSQL.** SQLite-fallback удалён — `DB_URL` обязателен. В Docker-compose адрес базы выставляется автоматически на основе `POSTGRES_*` переменных.

Ключевые таблицы:
- `logs` — история действий (комментарии, реакции, пропуски, удаление спама) с `msg_id` для edit/delete
- `inbox_messages` — входящие/исходящие DM и цитаты (`is_read` управляется кнопкой «Прочитать все», автоотметка отключена)
- `outbound_queue` — очередь отправки сообщений
- `discussion_sessions` / `discussion_messages` — AI-дискуссии
- `scenarios`, `triggers`, `post_scenarios` — автоматизация
- `manual_tasks` — ручные задачи комментирования из админки
- `warning_seen` / `warning_history` / `warning_dismissed` — состояние предупреждений (прочитанные / история / скрытые вручную)
- `spam_rules` / `spam_log` / `spam_bans` — антиспам-правила, журнал срабатываний, баны
- `proxies` — прокси, привязываемые к аккаунтам
- `account_failures` / `account_failure_log` — счётчики и лог сбоев по аккаунтам
- `join_status` — медленные вступления в чаты с расписанием повторов

Миграции применяются автоматически в `init_database()` при старте: `CREATE TABLE IF NOT EXISTS` + идемпотентные `ALTER TABLE ADD COLUMN IF NOT EXISTS` для колонок, добавленных по ходу разработки.

## Безопасность

- Никогда не коммитьте `config.ini`, `.env` и `data/*.json` с реальными ключами
- `.gitignore` исключает runtime-данные и секреты
- `ADMIN_WEB_DISABLE_AUTH=1` отключает авторизацию (только для локальной разработки)
