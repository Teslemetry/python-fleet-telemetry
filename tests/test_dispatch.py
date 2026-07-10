"""Tests for the dispatch layer (:mod:`fleet_telemetry.dispatch`)."""

import asyncio
from datetime import datetime, timezone

from fleet_telemetry.dispatch import Dispatcher
from fleet_telemetry.proto import vehicle_data_pb2 as vd
from fleet_telemetry.records import Record, Topic

_EPOCH = datetime.now(timezone.utc)


def data_record(vin: str = "v1", speed: float = 1.0) -> Record:
    msg = vd.Payload(
        vin=vin,
        data=[vd.Datum(key=vd.Field.VehicleSpeed, value=vd.Value(float_value=speed))],
    )
    return Record(
        vin=vin,
        topic=Topic.DATA,
        created_at=_EPOCH,
        txid="t",
        message=msg,
        raw=b"",
    )


def alert_record(vin: str = "v1") -> Record:
    from fleet_telemetry.proto import vehicle_alert_pb2 as va

    msg = va.VehicleAlerts(vin=vin, alerts=[va.VehicleAlert(name="AlertX")])
    return Record(
        vin=vin,
        topic=Topic.ALERTS,
        created_at=_EPOCH,
        txid="t",
        message=msg,
        raw=b"",
    )


async def test_sync_callback_fires_on_match() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append)
    rec = data_record()
    await disp.dispatch(rec)
    assert seen == [rec]


async def test_async_callback_is_awaited() -> None:
    disp = Dispatcher()
    seen: list[Record] = []

    async def cb(record: Record) -> None:
        seen.append(record)

    disp.add_listener(cb)
    rec = data_record()
    await disp.dispatch(rec)
    assert seen == [rec]


async def test_vin_filter_single() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, vin="v1")
    await disp.dispatch(data_record(vin="v1"))
    await disp.dispatch(data_record(vin="v2"))
    assert [r.vin for r in seen] == ["v1"]


async def test_vin_filter_iterable() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, vin=["v1", "v3"])
    await disp.dispatch(data_record(vin="v1"))
    await disp.dispatch(data_record(vin="v2"))
    await disp.dispatch(data_record(vin="v3"))
    assert [r.vin for r in seen] == ["v1", "v3"]


async def test_topic_filter() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, topic=Topic.ALERTS)
    await disp.dispatch(data_record())
    await disp.dispatch(alert_record())
    assert [r.topic for r in seen] == [Topic.ALERTS]


async def test_field_filter_present() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, field="VehicleSpeed")
    await disp.dispatch(data_record())
    assert len(seen) == 1


async def test_field_filter_absent() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, field="Soc")
    await disp.dispatch(data_record())
    assert seen == []


async def test_combined_filters_and_semantics() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, vin="v1", topic=Topic.DATA, field="VehicleSpeed")
    await disp.dispatch(data_record(vin="v1"))  # matches all three
    await disp.dispatch(data_record(vin="v2"))  # wrong vin
    await disp.dispatch(alert_record(vin="v1"))  # wrong topic + no field
    assert [r.vin for r in seen] == ["v1"]


async def test_str_vin_not_split_by_character() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, vin="v1")
    # "v" is a single character of the vin string; must NOT match.
    await disp.dispatch(data_record(vin="v"))
    assert seen == []


async def test_single_topic_not_split() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.add_listener(seen.append, topic=Topic.DATA)
    await disp.dispatch(data_record())
    assert len(seen) == 1


async def test_unsubscribe_stops_delivery_and_is_idempotent() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    unsub = disp.add_listener(seen.append)
    await disp.dispatch(data_record())
    unsub()
    await disp.dispatch(data_record())
    unsub()  # safe to call twice
    assert len(seen) == 1


async def test_raising_listener_is_isolated() -> None:
    disp = Dispatcher()
    seen: list[Record] = []

    def boom(_: Record) -> None:
        raise RuntimeError("boom")

    disp.add_listener(boom)
    disp.add_listener(seen.append)
    # dispatch must not raise despite the first listener blowing up.
    await disp.dispatch(data_record())
    assert len(seen) == 1


async def test_on_data_presets_topic() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.on_data(seen.append)
    await disp.dispatch(data_record())
    await disp.dispatch(alert_record())
    assert [r.topic for r in seen] == [Topic.DATA]


async def test_on_alert_presets_topic() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.on_alert(seen.append)
    await disp.dispatch(data_record())
    await disp.dispatch(alert_record())
    assert [r.topic for r in seen] == [Topic.ALERTS]


async def test_on_data_still_accepts_vin_kwarg() -> None:
    disp = Dispatcher()
    seen: list[Record] = []
    disp.on_data(seen.append, vin="v1")
    await disp.dispatch(data_record(vin="v1"))
    await disp.dispatch(data_record(vin="v2"))
    assert [r.vin for r in seen] == ["v1"]


async def test_records_iterator_yields_in_order() -> None:
    disp = Dispatcher()
    it = disp.records()

    r1, r2 = data_record(speed=1.0), data_record(speed=2.0)
    await disp.dispatch(r1)
    await disp.dispatch(r2)

    got1 = await asyncio.wait_for(anext(it), timeout=1)
    got2 = await asyncio.wait_for(anext(it), timeout=1)
    assert got1 is r1
    assert got2 is r2
    await it.aclose()


async def test_two_concurrent_iterators_both_receive() -> None:
    disp = Dispatcher()
    it1 = disp.records()
    it2 = disp.records()

    rec = data_record()
    await disp.dispatch(rec)

    got1 = await asyncio.wait_for(anext(it1), timeout=1)
    got2 = await asyncio.wait_for(anext(it2), timeout=1)
    assert got1 is rec
    assert got2 is rec
    await it1.aclose()
    await it2.aclose()


async def test_queue_drop_oldest_under_maxsize_one() -> None:
    disp = Dispatcher(queue_maxsize=1)
    it = disp.records()

    r1, r2 = data_record(speed=1.0), data_record(speed=2.0)
    await disp.dispatch(r1)  # fills queue
    await disp.dispatch(r2)  # drops oldest (r1), keeps r2

    got = await asyncio.wait_for(anext(it), timeout=1)
    assert got is r2
    await it.aclose()
