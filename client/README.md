# redis-service-client

Спільний async-клієнт до `redis-service` — доменного порту для Redis pub/sub
та розподілених локів. Той самий підхід, що `account-service-client`: один
встановлюваний пакет замість того, щоб кожен застосунок тримав власну копію
`redis.asyncio`-обгортки.

## Навіщо

`EventDrivenScheduler` (core-service) раніше мав власний **in-process**
`EventBus` (`asyncio`, без мережі) — події `emit`/`subscribe` бачив тільки
той самий процес. Це працювало, поки був один процес на всю систему.

`RedisEventBus` замінює транспорт на Redis pub/sub, лишаючи публічний
інтерфейс (`subscribe`, `unsubscribe`, `unsubscribe_owner`, `emit`)
незмінним — код `Profession`/`Monitor`, який раніше викликав
`scheduler.subscribe(...)` / `scheduler.emit_event(...)`, не змінюється
взагалі.

## Встановлення

```toml
dependencies = [
    "redis-service-client @ git+ssh://git@github.com/Red1ngs/redis-service.git@v0.1.0#subdirectory=client",
]
```

## Використання

```python
from redis_service_client import RedisEventBus

bus = RedisEventBus()  # REDIS_URL з env, дефолт redis://redis:6379/0
await bus.start()

async def on_ready(payload: dict) -> None:
    ...

bus.subscribe("loader.chapters_ready", on_ready)
await bus.emit("loader.chapters_ready", {"empty": True}, source="acc_01")

bus.unsubscribe_owner(some_owner_instance)
await bus.stop()
```

### Розподілений лок

```python
lock = bus.lock("scheduler:catalog_loader", ttl_ms=120_000)
if await lock.try_acquire():
    try:
        ...
    finally:
        await lock.release()
```

## Конфігурація

| ENV | Дефолт | Опис |
|---|---|---|
| `REDIS_URL` | `redis://redis:6379/0` | адреса Redis |
| `SCHEDULER_EVENTS_PREFIX` | `scheduler_events` | префікс каналів (`{prefix}:{event_name}`) |
