# Task-001: Исправить ошибку 500 на странице «Статистика»

## Статус: ВЫПОЛНЕНА, ОЖИДАЕТ ДЕПЛОЯ

## Что было сделано

1. Ветвление `_is_postgres()` для period_filter — PostgreSQL использует `timestamp::timestamptz >= NOW() - INTERVAL`, SQLite — `datetime('now', ...)`
2. Подзапрос `MIN(id) + JOIN` вместо некорректного `SELECT * ... GROUP BY` — работает на обоих диалектах
3. Доработка: замена `(NOW() - INTERVAL)::text` на `timestamp::timestamptz >= NOW() - INTERVAL` — типизированное сравнение вместо хрупкого строкового

## Проверка на проде

- `docker-compose up --build`, открыть `/stats`
- Проверить периоды day/week/month, пагинацию, экспорт
