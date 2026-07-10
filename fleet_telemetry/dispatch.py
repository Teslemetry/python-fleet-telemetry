"""Consumer-facing event dispatch: filtered listeners and a record iterator.

The :class:`Dispatcher` is the library's public event API. Consumers register
callbacks with :meth:`~Dispatcher.add_listener` (or the topic-preset
:meth:`~Dispatcher.on_data`/:meth:`~Dispatcher.on_alert`/… wrappers) and receive
each matching :class:`~fleet_telemetry.records.Record` as it arrives. Callbacks
may be plain functions or coroutine functions; both are supported.

Alternatively, consumers can pull records with :meth:`~Dispatcher.records`, an
async iterator backed by a bounded per-iterator queue. The recommended form is::

    async with dispatcher.records() as stream:
        async for record in stream:
            ...

The transport layer feeds records in via :meth:`~Dispatcher.dispatch`, which
fans a record out to every active iterator queue *first*, then invokes every
matching listener. On shutdown, :meth:`~Dispatcher.close` ends all active
iterators so parked consumers unblock cleanly.

Isolation is the guiding principle: one listener raising, one listener hanging,
or one iterator falling behind must never stall ingestion or starve the other
consumers.
"""

from __future__ import annotations

import inspect
import itertools
import logging
from asyncio import Queue, QueueEmpty, QueueFull
from collections.abc import Awaitable, Callable, Iterable, KeysView
from types import TracebackType
from typing import Union

from fleet_telemetry.records import Record, Topic

__all__ = ["Dispatcher", "Listener"]

_LOGGER = logging.getLogger(__name__)

#: A consumer callback. Invoked with each matching record; may be synchronous
#: (returning ``None``) or return an awaitable (coroutine/Task/Future), which is
#: awaited before the next listener runs.
Listener = Callable[[Record], Union[Awaitable[None], None]]


class _Sentinel:
    """A marker queued to signal end-of-stream to a :class:`_RecordStream`."""

    __slots__ = ()


#: Singleton pushed onto each iterator queue by :meth:`Dispatcher.close`.
_CLOSED = _Sentinel()

#: What an iterator queue may hold: a record, or the end-of-stream sentinel.
_Item = Union[Record, _Sentinel]


def _normalize(value: str | Iterable[str] | None) -> frozenset[str] | None:
    """Coerce a single-value / iterable / ``None`` filter spec to a set or ``None``.

    ``None`` means "no constraint". A lone ``str`` becomes a one-element set — a
    ``str`` must be treated as a single value, never split into its characters.
    (:class:`~fleet_telemetry.records.Topic` is a ``str`` subclass, so topic
    specs flow through here unchanged.) An empty iterable normalizes to an empty
    set, i.e. a constraint that nothing can satisfy — the listener never fires.
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
    are still captured (up to ``queue_maxsize``, drop-oldest beyond).

    Iteration ends when the stream is closed: via :meth:`aclose` (or the
    ``async with`` context manager), or when :meth:`Dispatcher.close` queues the
    end-of-stream sentinel. Either way the queue is deregistered from the
    dispatcher's fan-out. :meth:`__del__` is a backstop for an iterator dropped
    without being closed.
    """

    __slots__ = ("_closed", "_queue", "_queues")

    def __init__(
        self,
        queues: set[Queue[_Item]],
        maxsize: int,
        *,
        closed: bool = False,
    ) -> None:
        self._queues = queues
        self._queue: Queue[_Item] = Queue(maxsize=maxsize)
        self._closed = closed
        if not closed:
            queues.add(self._queue)

    def __aiter__(self) -> _RecordStream:
        return self

    async def __anext__(self) -> Record:
        if self._closed:
            raise StopAsyncIteration
        item = await self._queue.get()
        if isinstance(item, _Sentinel):
            self._closed = True
            self._queues.discard(self._queue)
            raise StopAsyncIteration
        return item

    async def aclose(self) -> None:
        """End the stream and unregister its queue from the dispatcher fan-out."""
        self._closed = True
        self._queues.discard(self._queue)

    async def __aenter__(self) -> _RecordStream:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

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

    def matches(self, record: Record, field_keys: KeysView[str]) -> bool:
        """Return whether ``record`` satisfies every present constraint (AND).

        ``field_keys`` is ``record.fields().keys()`` computed once per dispatch
        and shared across registrations, so field-filtered listeners don't each
        re-decode the payload.
        """
        if self.vins is not None and record.vin not in self.vins:
            return False
        if self.topics is not None and record.topic not in self.topics:
            return False
        if self.fields is not None and self.fields.isdisjoint(field_keys):
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
        self._queues: set[Queue[_Item]] = set()
        self._ids = itertools.count()
        self._closed = False

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
        its topic is among ``topic``, and its field set intersects ``field``. An
        empty iterable (e.g. ``vin=[]``) is a constraint nothing satisfies, so
        the listener never fires.

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
        """Deliver ``record`` to every active iterator queue and matching listener.

        Iterator fan-out runs *first* and without awaiting, so a slow or hung
        listener can neither back-pressure a vehicle's stream nor delay the
        ``records()`` consumers. If a queue is full its oldest record is dropped
        (drop-oldest) and a warning is logged.

        Listeners are then invoked over a snapshot of the registrations, so a
        callback that unsubscribes mid-dispatch cannot mutate the set being
        iterated. Each listener's exception is caught and logged, isolating it
        from the other listeners. Sync callbacks run inline; a returned awaitable
        is awaited before the next listener (preserving order and giving natural
        per-callback backpressure that does not reach the iterator fan-out).
        """
        # Fan out to iterator queues first (no await between here and the loop
        # end keeps this atomic against concurrent dispatch calls).
        for queue in list(self._queues):
            try:
                queue.put_nowait(record)
            except QueueFull:
                _LOGGER.warning(
                    "fleet-telemetry records() queue full; dropping oldest record"
                )
                try:
                    queue.get_nowait()
                except QueueEmpty:
                    # Unreachable in practice: this queue's only consumer is its
                    # own iterator, and nothing awaits between the QueueFull above
                    # and here, so the queue cannot have drained. Defensive only.
                    pass
                queue.put_nowait(record)

        # Decode the field set once; matches() shares it across registrations.
        field_keys = record.fields().keys()
        for registration in list(self._registrations.values()):
            if not registration.matches(record, field_keys):
                continue
            try:
                result = registration.callback(record)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                _LOGGER.exception(
                    "fleet-telemetry listener raised while handling record"
                )

    def records(self) -> _RecordStream:
        """Return an async iterator that yields records as they are dispatched.

        Recommended usage is the context-manager form, which guarantees the
        iterator's queue is deregistered on exit (including on ``break`` or
        error)::

            async with dispatcher.records() as stream:
                async for record in stream:
                    ...

        Each call creates an independent queue, so multiple concurrent iterators
        each receive every dispatched record. The queue registers eagerly, so
        records dispatched before the first ``anext`` are still delivered.
        :meth:`close` ends every active iterator. Calling ``records()`` after
        :meth:`close` returns an already-exhausted stream.
        """
        return _RecordStream(
            self._queues, self._queue_maxsize, closed=self._closed
        )

    def close(self) -> None:
        """Shut the dispatcher down, ending every active :meth:`records` iterator.

        Each active iterator receives an end-of-stream sentinel, so a parked
        consumer's ``async for`` / ``anext`` unblocks and raises
        :class:`StopAsyncIteration`. Idempotent. After ``close`` a new
        ``records()`` call returns an already-exhausted stream. Listeners are
        unaffected; this only tears down the record iterators.
        """
        self._closed = True
        for queue in list(self._queues):
            # Guarantee the sentinel lands even if the queue is full.
            while True:
                try:
                    queue.put_nowait(_CLOSED)
                    break
                except QueueFull:
                    try:
                        queue.get_nowait()
                    except QueueEmpty:  # pragma: no cover - see dispatch()
                        break
        self._queues.clear()
