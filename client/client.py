"""Єдиний клієнт-порт до redis-service.

За тим самим патерном, що ``account-service-client`` (один пакет замість
copy-paste EventBus-подібних класів у кожному застосунку): бізнес-сервіс
не тримає власного ``redis.asyncio`` коду і не знає деталей канальної
топології — він імпортує ``RedisEventBus``/``RedisLock`` і користується
ними як звичайним pub/sub та розподіленим локом.

RedisEventBus навмисно повторює публічний інтерфейс попереднього
in-process ``EventBus`` (``subscribe`` / ``unsubscribe`` /
``unsubscribe_owner`` / ``emit``) — це дозволяє замінити транспорт
(in-memory asyncio → Redis pub/sub) без жодної зміни коду, що ним
користується (``EventDrivenScheduler``, ``Profession``, ``Monitor``).

Різниця з in-process EventBus:
    • emit() публікує подію в Redis-канал — її отримають підписники
      цього процесу І будь-яких інших процесів/подів, що слухають той
      самий Redis.
    • callback'и як і раніше виконуються ЛОКАЛЬНО, у процесі, що їх
      зареєстрував — RedisEventBus лише доставляє payload, диспетчеризація
      лишається на боці клієнта.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections import defaultdict
from typing import Any, Awaitable, Callable, Optional

import redis.asyncio as aioredis

EventCallback = Callable[[dict[str, Any]], Awaitable[None]]

_DEFAULT_PREFIX = "scheduler_events"

_RELEASE_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class RedisServiceError(Exception):
    """Загальна помилка redis-service (з'єднання, конфігурація)."""


class RedisEventBus:
    """
    Розподілений async pub/sub event-bus поверх Redis.

    Один екземпляр на процес-споживач (як ``account_client``). Підписка
    на Redis-канали відбувається лениво — при першому ``subscribe()``/
    ``emit()`` або явному ``start()``.

    Використання (ідентичне колишньому EventBus):

        bus = RedisEventBus()
        await bus.start()

        async def on_ready(payload: dict) -> None:
            ...

        bus.subscribe("loader.chapters_ready", on_ready)
        await bus.emit("loader.chapters_ready", {"empty": True}, source="acc_01")
        bus.unsubscribe_owner(some_profession_instance)
        await bus.stop()
    """

    def __init__(self, redis_url: Optional[str] = None, prefix: Optional[str] = None) -> None:
        self._redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis:6379/0")
        self._prefix = prefix or os.getenv("SCHEDULER_EVENTS_PREFIX", _DEFAULT_PREFIX)
        self._redis: Optional["aioredis.Redis"] = None
        self._pubsub: Any = None
        self._task: Optional[asyncio.Task[None]] = None
        self._starting: Optional[asyncio.Lock] = None
        # event_name -> [callbacks] (локальні, у межах цього процесу)
        self._subs: dict[str, list[EventCallback]] = defaultdict(list)

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Ідемпотентно піднімає з'єднання і фоновий psubscribe-listener."""
        if self._task is not None:
            return
        if self._starting is None:
            self._starting = asyncio.Lock()
        async with self._starting:
            if self._task is not None:
                return
            self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
            self._pubsub = self._redis.pubsub()
            await self._pubsub.psubscribe(f"{self._prefix}:*")
            self._task = asyncio.create_task(self._listen(), name="redis-event-bus")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None
        if self._pubsub is not None:
            await self._pubsub.close()
            self._pubsub = None
        if self._redis is not None:
            await self._redis.close()
            self._redis = None

    async def _ensure_started(self) -> None:
        if self._redis is None:
            await self.start()

    async def _listen(self) -> None:
        assert self._pubsub is not None
        try:
            async for message in self._pubsub.listen():
                if message.get("type") != "pmessage":
                    continue
                try:
                    payload = json.loads(message["data"])
                except (TypeError, ValueError):
                    continue
                event_name = payload.pop("_event", None)
                if not event_name:
                    continue
                await self._dispatch(event_name, payload)
        except asyncio.CancelledError:
            pass

    async def _dispatch(self, event_name: str, payload: dict[str, Any]) -> None:
        listeners = list(self._subs.get(event_name, []))
        if not listeners:
            return
        await asyncio.gather(
            *(self._call_subscriber(cb, payload) for cb in listeners),
            return_exceptions=True,
        )

    async def _call_subscriber(self, cb: EventCallback, payload: dict[str, Any]) -> None:
        # Клієнтський пакет навмисно не тягне логер застосунку — виклик
        # огортається у споживача (scheduler.py), якщо потрібне логування.
        await cb(payload)

    # ── Public API (сумісний зі старим in-process EventBus) ────────────────

    def subscribe(self, event_name: str, callback: EventCallback) -> None:
        if callback not in self._subs[event_name]:
            self._subs[event_name].append(callback)

    def unsubscribe(self, event_name: str, callback: EventCallback) -> None:
        try:
            self._subs[event_name].remove(callback)
        except ValueError:
            pass

    def unsubscribe_all(self, callback: EventCallback) -> None:
        for listeners in self._subs.values():
            try:
                listeners.remove(callback)
            except ValueError:
                pass

    def unsubscribe_owner(self, owner: object) -> None:
        """Видаляє всі callbacks, що належать owner (за __self__)."""
        for listeners in self._subs.values():
            listeners[:] = [
                cb for cb in listeners
                if getattr(cb, "__self__", None) is not owner
            ]

    async def emit(
        self,
        event_name: str,
        payload:    dict[str, Any],
        *,
        source: str = "system",
    ) -> int:
        """
        Публікує подію в Redis. Повертає кількість ЛОКАЛЬНИХ підписників
        цього процесу (як орієнтир для логів викликача) — підписники
        інших процесів у це число не входять, бо доставка асинхронна.
        """
        await self._ensure_started()
        enriched = {**payload, "_event": event_name, "_source": source}
        assert self._redis is not None
        await self._redis.publish(f"{self._prefix}:{event_name}", json.dumps(enriched))
        return len(self._subs.get(event_name, []))

    # ── Розподілений лок (першим-встиг) ─────────────────────────────────────

    def lock(self, key: str, ttl_ms: int = 60_000) -> "RedisLock":
        """Фабрика RedisLock, що ділить з'єднання цього bus."""
        return RedisLock(self, key, ttl_ms=ttl_ms)


class RedisLock:
    """
    Розподілений неблокуючий лок (``SET key token NX PX ttl``).

    Заміна ``asyncio.Lock`` для сценаріїв на кшталт «перший хто встиг
    парсить каталог, решта пропускають» — раніше цей лок був локальним
    для одного процесу (``asyncio.Lock`` в EventDrivenScheduler), тепер
    він розподілений на весь кластер через Redis.

        loader_lock = bus.lock("scheduler:catalog_loader", ttl_ms=120_000)
        if await loader_lock.try_acquire():
            try:
                ...
            finally:
                await loader_lock.release()
    """

    def __init__(self, bus: RedisEventBus, key: str, ttl_ms: int = 60_000) -> None:
        self._bus = bus
        self._key = key
        self._ttl_ms = ttl_ms
        self._token: Optional[str] = None

    async def try_acquire(self) -> bool:
        await self._bus._ensure_started()
        assert self._bus._redis is not None
        token = uuid.uuid4().hex
        acquired = await self._bus._redis.set(self._key, token, nx=True, px=self._ttl_ms)
        if acquired:
            self._token = token
            return True
        return False

    async def release(self) -> None:
        if self._token is None or self._bus._redis is None:
            return
        try:
            await self._bus._redis.eval(_RELEASE_LOCK_SCRIPT, 1, self._key, self._token)
        finally:
            self._token = None

    @property
    def locked(self) -> bool:
        return self._token is not None
