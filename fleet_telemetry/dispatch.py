"""Consumer-facing event dispatch: filtered listeners and a record iterator.

The :class:`Dispatcher` is the library's public event API. Consumers register
callbacks with :meth:`~Dispatcher.add_listener` (or the topic-preset
:meth:`~Dispatcher.on_data`/:meth:`~Dispatcher.on_alert`/… wrappers) and receive
each matching :class:`~fleet_telemetry.records.Record` as it arrives. Callbacks
may be plain functions or coroutine functions; both are supported.

Alternatively, consumers can pull records with :meth:`~Dispatcher.records`, an
async iterator backed by a bounded per-iterator queue. The transport layer feeds
records in via :meth:`~Dispatcher.dispatch`, which fans a record out to every
matching listener and every active iterator queue.

Isolation is the guiding principle: one listener raising, or one iterator
falling behind, must never stall ingestion or starve the other consumers.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import Union

from fleet_telemetry.records import Record, Topic

__all__ = ["Dispatcher", "Listener"]

_LOGGER = logging.getLogger(__name__)

#: A consumer callback. Invoked with each matching record; may be synchronous
#: (returning ``None``) or a coroutine function (whose result is awaited).
Listener = Callable[[Record], Union[Awaitable[None], None]]


def _normalize(value: str | Iterable[str] | None) -> frozenset[str] | None:
    """Coerce a single-value / iterable / ``None`` filter spec to a set or ``None``.

    ``None`` means "no constraint". A lone ``str`` becomes a one-element set — a
    ``str`` must be treated as a single value, never split into its characters.
    (:class:`~fleet_telemetry.records.Topic` is a ``str`` subclass, so topic
    specs flow through here unchanged.)
    """
    if value is None:
        return None
    if isinstance(value, str):
        return frozenset((value,))
    return frozenset(value)


class _RecordStream:
    """An async iterator over dispatched records, backed by one bounded queue.

    The queue registers with the dispatcher's fan-out set at construction — not
    lazily on first iteration — so records dispatched before the first ``anext``
    are still captured (up to ``queue_maxsize``, drop-oldest beyond). The queue
    is unregistered on :meth:`aclose` or garbage collection, so an abandoned
    iterator stops receiving fan-out and is not leaked.
    """

    __slots__ = ("_queue", "_queues")

    def __init__(self, queues: set[asyncio.Queue[Record]], maxsize: int) -> None:
        self._queues = queues
        self._queue: asyncio.Queue[Record] = asyncio.Queue(maxsize=maxsize)
        queues.add(self._queue)

    def __aiter__(self) -> _RecordStream:
        return self

    async def __anext__(self) -> Record:
        return await self._queue.get()

    async def aclose(self) -> None:
        """Unregister this iterator's queue from the dispatcher's fan-out."""
        self._queues.discard(self._queue)

    def __del__(self) -> None:
        self._queues.discard(self._queue)


class _Registration:
    """One registered listener plus its normalized match constraints."""

    __slots__ = ("callback", "fields", "topics", "vins")

    def __init__(
        self,
        callback: Listener,
        vins: frozenset[str] | None,
        topics: frozenset[str] | None,
        fields: frozenset[str] | None,
    ) -> None:
        self.callback = callback
        self.vins = vins
        self.topics = topics
        self.fields = fields

    def matches(self, record: Record) -> bool:
        """Return whether ``record`` satisfies every present constraint (AND)."""
        if self.vins is not None and record.vin not in self.vins:
            return False
        if self.topics is not None and record.topic not in self.topics:
            return False
        if self.fields is not None and self.fields.isdisjoint(record.fields().keys()):
            return False
        return True


class Dispatcher:
    """Routes records to filtered listeners and to async record iterators.

    :param queue_maxsize: Bound on each :meth:`records` iterator's backlog. When
        a queue is full, the oldest record is dropped to make room, so a slow or
        abandoned iterator can never block :meth:`dispatch`.
    """

    def __init__(self, *, queue_maxsize: int = 1000) -> None:
        self._queue_maxsize = queue_maxsize
        self._registrations: dict[int, _Registration] = {}
        self._queues: set[asyncio.Queue[Record]] = set()
        self._ids = itertools.count()

    def add_listener(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        topic: Topic | Iterable[Topic] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` to receive records matching the given filters.

        Each of ``vin``/``topic``/``field`` accepts a single value, an iterable
        of values, or ``None`` (no constraint on that dimension). A record
        matches when every present constraint holds: its vin is among ``vin``,
        its topic is among ``topic``, and its field set intersects ``field``.

        Returns an unsubscribe callable. Calling it removes the listener; calling
        it again is a safe no-op.
        """
        registration = _Registration(
            callback,
            _normalize(vin),
            _normalize(topic),
            _normalize(field),
        )
        key = next(self._ids)
        self._registrations[key] = registration

        def unsubscribe() -> None:
            self._registrations.pop(key, None)

        return unsubscribe

    def on_data(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for DATA records (topic preset)."""
        return self.add_listener(callback, topic=Topic.DATA, vin=vin, field=field)

    def on_alert(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for ALERTS records (topic preset)."""
        return self.add_listener(callback, topic=Topic.ALERTS, vin=vin, field=field)

    def on_error(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for ERRORS records (topic preset)."""
        return self.add_listener(callback, topic=Topic.ERRORS, vin=vin, field=field)

    def on_connectivity(
        self,
        callback: Listener,
        *,
        vin: str | Iterable[str] | None = None,
        field: str | Iterable[str] | None = None,
    ) -> Callable[[], None]:
        """Register ``callback`` for CONNECTIVITY records (topic preset)."""
        return self.add_listener(
            callback, topic=Topic.CONNECTIVITY, vin=vin, field=field
        )

    async def dispatch(self, record: Record) -> None:
        """Deliver ``record`` to every matching listener and iterator queue.

        Listeners are invoked over a snapshot of the registrations, so a callback
        that unsubscribes mid-dispatch cannot mutate the set being iterated. Each
        listener's exception is caught and logged, isolating it from the other
        listeners and from the iterator fan-out. Sync callbacks run inline;
        coroutine results are awaited.

        Every active :meth:`records` queue then receives the record. If a queue is
        full its oldest record is discarded (drop-oldest) and a warning is logged.
        """
        for registration in list(self._registrations.values()):
            if not registration.matches(record):
                continue
            try:
                result = registration.callback(record)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                _LOGGER.exception(
                    "fleet-telemetry listener raised while handling record"
                )

        for queue in list(self._queues):
            try:
                queue.put_nowait(record)
            except asyncio.QueueFull:
                _LOGGER.warning(
                    "fleet-telemetry records() queue full; dropping oldest record"
                )
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:  # pragma: no cover - concurrent drain
                    pass
                queue.put_nowait(record)

    def records(self) -> AsyncIterator[Record]:
        """Return an async iterator that yields records as they are dispatched.

        Each call creates an independent queue, so multiple concurrent iterators
        each receive every dispatched record. The queue is registered eagerly, so
        records dispatched before the first ``anext`` are still delivered. Closing
        the iterator (``aclose``) or letting it be garbage-collected removes its
        queue from the fan-out set.
        """
        return _RecordStream(self._queues, self._queue_maxsize)
