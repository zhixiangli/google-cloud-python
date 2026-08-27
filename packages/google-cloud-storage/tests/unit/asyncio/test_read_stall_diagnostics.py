# Copyright 2026 Google LLC

import asyncio
import json
import logging

import pytest

from google.cloud.storage.asyncio import _read_stall_diagnostics as diagnostics


def _records(caplog):
    return [
        json.loads(record.message[len(diagnostics.MARKER) :])
        for record in caplog.records
        if record.message.startswith(diagnostics.MARKER)
    ]


def test_diagnostics_are_disabled_by_default(monkeypatch, caplog):
    monkeypatch.setattr(diagnostics, "ENABLED", False)

    with caplog.at_level(logging.WARNING):
        diagnostics.emit("unexpected")

    assert _records(caplog) == []


def test_emit_produces_stable_bounded_json(monkeypatch, caplog):
    monkeypatch.setattr(diagnostics, "ENABLED", True)

    with caplog.at_level(logging.WARNING):
        diagnostics.emit(
            "recv_error",
            multiplexer_id="mux-1",
            stream_generation=2,
            error="x" * 2000,
        )

    (record,) = _records(caplog)
    assert record["event"] == "recv_error"
    assert record["multiplexer_id"] == "mux-1"
    assert record["stream_generation"] == 2
    assert record["pid"] > 0
    assert record["thread_name"]
    assert len(caplog.records[-1].message.encode()) < diagnostics.MAX_RECORD_BYTES


def test_multiplexer_state_uses_its_bounded_snapshot():
    class Multiplexer:
        def diagnostic_state(self):
            return {
                "stream_generation": 3,
                "registered_read_ids": 999,
                "recv_task_state": "pending",
            }

    assert diagnostics.multiplexer_state(Multiplexer()) == {
        "stream_generation": 3,
        "registered_read_ids": 999,
        "recv_task_state": "pending",
    }


@pytest.mark.asyncio
async def test_wait_observer_logs_without_consuming_or_cancelling_queue_get(
    monkeypatch, caplog
):
    monkeypatch.setattr(diagnostics, "ENABLED", True)
    monkeypatch.setattr(diagnostics, "WAIT_LOG_INTERVAL_SECONDS", 0.01)
    queue = asyncio.Queue()
    state = {"recv_count": 7, "recv_task_state": "pending"}

    async def put_later():
        await asyncio.sleep(0.035)
        await queue.put("response")

    put_task = asyncio.create_task(put_later())
    with caplog.at_level(logging.WARNING):
        async with diagnostics.observe_wait(
            "download.queue_get",
            snapshot=lambda: state,
            pending_read_ids=4,
            attempt=1,
        ):
            item = await queue.get()
    await put_task

    assert item == "response"
    records = _records(caplog)
    assert records
    assert all(record["event"] == "wait_stalled" for record in records)
    assert records[0]["operation"] == "download.queue_get"
    assert records[0]["pending_read_ids"] == 4
    assert records[0]["recv_count"] == 7
    assert records[0]["elapsed_s"] >= 0.01
