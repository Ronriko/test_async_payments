# Async Payment Processing Service

Микросервис асинхронной обработки платежей. API принимает платёж и сразу отвечает `202 Accepted`.
Обработка идёт в фоне: эмуляция платёжного шлюза (2-5 с, 90% успешных), запись статуса в БД и
уведомление клиента webhook'ом. Доставка событий гарантирована outbox-паттерном, повторы - с
экспоненциальной задержкой, окончательно упавшие сообщения уходят в Dead Letter Queue.

## Быстрый старт

```bash
cp .env.example .env
# заполнить пустые API_KEY, POSTGRES_PASSWORD и RABBITMQ_PASSWORD, например значением
# `openssl rand -hex 32` (пароли подставляются в строки подключения как есть, нужен URL-safe вид)
docker compose up --build -d
docker compose ps         # api и consumer в состоянии healthy
```

Пока секреты пустые, `docker compose` не запустится и скажет, какой переменной не хватает.
Строки подключения `DATABASE_URL`, `TEST_DATABASE_URL` и `RABBITMQ_URL` compose собирает сам из
этих значений. PostgreSQL и RabbitMQ применяют учётные данные только при первом создании томов:
после смены пароля нужен `docker compose down -v` (данные будут удалены).

- Swagger UI: http://localhost:8000/docs (кнопка Authorize - заголовок `X-API-Key`)
- RabbitMQ management: http://localhost:15672 (логин и пароль из `RABBITMQ_USER` / `RABBITMQ_PASSWORD`)

Миграции применяет одноразовый сервис `migrate` до старта `api` и `consumer`.

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key`. `/health`, `/health/ready`, `/docs`,
`/redoc` и `/openapi.json` открыты намеренно: это служебные эндпоинты для оркестратора и документации.

Создание платежа (`set -a; . ./.env; set +a` подставит `$API_KEY` из `.env`):

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: 6b1f0c1e-order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://shop.example.com/webhooks/payments"
      }'
```

```json
{"payment_id": "0b8e5c6a-3f0e-4a8e-9a43-0f7b6c1f9d2e", "status": "pending", "created_at": "2026-10-20T12:00:00.123456Z"}
```

Получение платежа:

```bash
curl http://localhost:8000/api/v1/payments/0b8e5c6a-3f0e-4a8e-9a43-0f7b6c1f9d2e -H "X-API-Key: $API_KEY"
```

```json
{
  "payment_id": "0b8e5c6a-3f0e-4a8e-9a43-0f7b6c1f9d2e",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Оплата заказа 42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "webhook_url": "https://shop.example.com/webhooks/payments",
  "created_at": "2026-10-20T12:00:00.123456Z",
  "processed_at": "2026-10-20T12:00:03.402113Z"
}
```

Webhook на `webhook_url` (POST, JSON) приходит и для `succeeded`, и для `failed`:

```json
{"payment_id": "0b8e5c6a-...", "status": "succeeded", "amount": "1500.00", "currency": "RUB", "processed_at": "2026-10-20T12:00:03.402113Z"}
```

Валюта принимается в любом регистре и хранится как `RUB`/`USD`/`EUR`; `metadata` можно не
передавать или передать `null` - сохранится `{}`.

Коды ответов:

| Код | Когда |
|---|---|
| 202 | платёж принят; повтор с тем же `Idempotency-Key` и тем же телом возвращает тот же платёж |
| 401 | нет или неверный `X-API-Key` |
| 404 | платёж не найден |
| 413 | тело больше `MAX_REQUEST_BODY_BYTES` (64 КБ) |
| 422 | невалидное тело или заголовок (нет `Idempotency-Key`, сумма не `>0` или больше 2 знаков после запятой, валюта не RUB/USD/EUR, `webhook_url` во внутренней сети) либо `Idempotency-Key` уже использован с другим телом; формат всегда `{"detail": [{"type", "loc", "msg"}]}` |
| 503 | недоступна БД |

## Архитектура

```
POST /payments ─► payments + outbox (одна транзакция)
                        │
           outbox relay (SELECT ... FOR UPDATE SKIP LOCKED)
                        │ publish (persistent, publisher confirms)
                        ▼
   exchange payments ─► payments.new ─► consumer ─► шлюз ─► статус в БД ─► webhook
                          │   ▲                        │ временная ошибка
              reject      │   └── TTL ── payments.new.retry.N ◄┘ (2 с, 4 с, ...)
                          ▼
   exchange payments.dlx ─► payments.dlq   (после MAX_ATTEMPTS или при постоянной ошибке)
```

Слои кода (`app/`):

| Слой | Пакет | Ответственность |
|---|---|---|
| Routes | `api/` | HTTP, валидация, аутентификация, обработчики исключений, лимит тела |
| Services | `services/` | создание платежа с идемпотентностью, обработка платежа, outbox relay |
| Repositories | `repositories/` | запросы к PostgreSQL |
| External | `broker/`, `clients/` | топология RabbitMQ (FastStream), публикация, шлюз, webhook-клиент |

Гарантии и решения:

- **Outbox.** Платёж и событие пишутся в одной транзакции. Relay забирает пачку через
  `FOR UPDATE SKIP LOCKED`, поэтому несколько relay не публикуют одно событие дважды. Неудачная
  публикация откладывается с backoff до `OUTBOX_MAX_BACKOFF_SECONDS` и не сдаётся никогда; при
  остановке relay дописывает текущую пачку, чтобы не публиковать её повторно. Опубликованные
  события удаляются через `OUTBOX_RETENTION_HOURS`.
- **Идемпотентность API.** `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` плюс sha256 тела
  запроса: параллельные повторы получают один и тот же платёж, тот же ключ с другим телом - 422.
- **Идемпотентность consumer.** Доставка at-least-once. Результат шлюза записывается условным
  `UPDATE ... WHERE status = 'pending'`, повторная доставка не вызывает шлюз для уже проведённого
  платежа и не перезаписывает результат. Webhook отправляется заново (at-least-once), получатель
  должен дедуплицировать по `payment_id` + `status`.
- **Retry и DLQ.** Ошибка БД, сети или ответ получателя 5xx/408/429 - повтор через очередь
  `payments.new.retry.N` с задержкой `RETRY_BASE_DELAY_SECONDS * 2^(N-1)`. После `MAX_ATTEMPTS` (3)
  или при постоянной ошибке (платёж не найден, ответ 3xx/4xx кроме 408/429, адрес во внутренней
  сети) consumer перекладывает сообщение в `payments.dlx` → `payments.dlq` с заголовками `attempt`
  и `error` (ошибка последней попытки). Сообщение, которое нельзя даже разобрать, отклоняется, и
  RabbitMQ сам кладёт его в DLQ через DLX (заголовок `x-death`).
- **Отказ шлюза (10%)** - это бизнес-результат: статус `failed` и webhook клиенту, не DLQ.
- **Webhook.** Один `httpx.AsyncClient` на процесс, общий дедлайн `WEBHOOK_TIMEOUT_SECONDS` на
  проверку адреса и запрос, тело ответа не читается, redirect'ы не выполняются, cookies не
  сохраняются. Адреса во внутренних сетях запрещены (защита от SSRF): IP-литералы и `localhost`
  отклоняются уже при создании платежа, DNS-имена проверяются после резолва перед отправкой.
- **Старт и остановка.** Если при старте RabbitMQ недоступен, consumer завершается, и Docker
  перезапускает его, пока брокер не поднимется. Процессы получают SIGTERM напрямую (exec-форма
  команд), consumer дожидается текущих обработчиков; незавершённое сообщение не подтверждено и
  будет доставлено снова.

Ограничения:
- от DNS rebinding полностью защищает только egress-прокси;
- webhook не подписывается (HMAC): этого нет в ТЗ;
- `Retry-After` в ответе 429 не учитывается: очереди повторов FIFO с одной задержкой на ступень;
- для локального приёмника webhook вне Docker (`host.docker.internal`, Docker Desktop) нужен
  `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true`.

## Переменные окружения

Шаблон - `.env.example`, реальные значения - только в `.env` (он в `.gitignore`).

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `API_KEY` | обязательна | статический ключ для `X-API-Key`, не короче 16 символов |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | обязательны | учётка PostgreSQL, из неё compose собирает `DATABASE_URL` и `TEST_DATABASE_URL` (`<db>_test`) |
| `RABBITMQ_USER`, `RABBITMQ_PASSWORD` | обязательны | учётка RabbitMQ, из неё compose собирает `RABBITMQ_URL` |
| `DATABASE_URL`, `TEST_DATABASE_URL`, `RABBITMQ_URL` | собирает compose | задавать вручную только для запуска на хосте |
| `LOG_LEVEL` | `INFO` | уровень JSON-логов |
| `API_WORKERS` | `2` | число воркеров uvicorn у api |
| `API_PORT`, `RABBITMQ_MANAGEMENT_PORT` | `8000`, `15672` | порты на `127.0.0.1` хоста |
| `MAX_REQUEST_BODY_BYTES` | `65536` | лимит тела запроса |
| `DB_POOL_SIZE`, `DB_MAX_OVERFLOW` | `10`, `10` | пул соединений SQLAlchemy |
| `CONSUMER_PREFETCH` | `10` | сколько сообщений consumer обрабатывает одновременно |
| `MAX_ATTEMPTS`, `RETRY_BASE_DELAY_SECONDS` | `3`, `2.0` | попытки обработки и база экспоненциальной задержки |
| `GATEWAY_MIN_DELAY_SECONDS`, `GATEWAY_MAX_DELAY_SECONDS`, `GATEWAY_SUCCESS_RATE` | `2`, `5`, `0.9` | эмуляция шлюза |
| `WEBHOOK_TIMEOUT_SECONDS`, `WEBHOOK_ALLOW_PRIVATE_NETWORKS` | `10`, `false` | webhook |
| `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL_SECONDS`, `OUTBOX_MAX_BACKOFF_SECONDS`, `OUTBOX_RETENTION_HOURS` | `100`, `0.5`, `60`, `168` | outbox relay |
| `PAYMENTS_EXCHANGE`, `PAYMENTS_QUEUE`, `DEAD_LETTER_EXCHANGE`, `DEAD_LETTER_QUEUE` | `payments`, `payments.new`, `payments.dlx`, `payments.dlq` | имена топологии RabbitMQ |

## Разработка на хосте

```bash
uv sync                                    # Python 3.13, зависимости из uv.lock
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d postgres rabbitmq
# в .env раскомментировать DATABASE_URL, TEST_DATABASE_URL и RABBITMQ_URL (хост localhost)
uv run alembic upgrade head                # миграциям нужен только DATABASE_URL
uv run uvicorn app.main:create_app --factory --reload       # API
uv run uvicorn app.worker:create_app --factory --port 8001  # consumer + outbox relay
```

Новая миграция: `uv run alembic revision --autogenerate -m "..."`.

## Тесты и проверки

```bash
docker compose --profile test run --rm --build tests   # pytest на настоящих PostgreSQL и RabbitMQ
uv run pytest                                          # на хосте, нужен TEST_DATABASE_URL
uv run ruff check . && uv run ruff format --check .
uv run mypy app tests                                  # strict
```

pre-commit и CI не настроены: эти команды запускаются вручную перед коммитом.

Тесты работают только с отдельной БД из `TEST_DATABASE_URL`: её имя должно оканчиваться на
`_test` и не совпадать с `DATABASE_URL`, иначе pytest останавливается до любых изменений (набор
откатывает миграции и очищает таблицы). Из окружения берутся только подключения, остальные
настройки в тестах - значения по умолчанию, поэтому тюнинг в `.env` на результат не влияет. Покрыты: API (авторизация, валидация, лимит тела,
идемпотентность, включая параллельные запросы), обработка платежа, outbox relay (backoff, несколько
relay одновременно, остановка, очистка), политика retry/DLQ в consumer и экспоненциальная задержка,
падение старта consumer без брокера, путь сообщения в DLQ и сквозной путь POST → outbox → relay →
consumer → webhook на настоящем RabbitMQ, соответствие моделей миграциям.

## Стек

Python 3.13, FastAPI 0.152, Pydantic 2.14 + pydantic-settings 2.16, SQLAlchemy 2.1 (asyncio,
asyncpg), Alembic 1.20, FastStream 0.7 (RabbitMQ), httpx 0.28, uvicorn 0.54, PostgreSQL 18.6,
RabbitMQ 4.3.6, Docker Compose, uv, ruff, mypy, pytest + pytest-asyncio.
