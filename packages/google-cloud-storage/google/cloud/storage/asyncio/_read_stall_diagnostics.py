# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Opt-in diagnostics for asynchronous bidi-read stalls."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import threading
import time


MARKER = "GCS_STORAGE_READ_DIAG "
MAX_RECORD_BYTES = 4096
_MAX_STRING_LENGTH = 512
ENABLED = os.getenv("GCS_STORAGE_READ_DIAG_ENABLED", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
try:
    WAIT_LOG_INTERVAL_SECONDS = max(
        0.1, float(os.getenv("GCS_STORAGE_READ_DIAG_INTERVAL_S", "30"))
    )
except ValueError:
    WAIT_LOG_INTERVAL_SECONDS = 30.0

logger = logging.getLogger(__name__)


def _task_name() -> str | None:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    return task.get_name() if task is not None else None


def _safe_value(value):
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:_MAX_STRING_LENGTH]
    return repr(value)[:_MAX_STRING_LENGTH]


def emit(event: str, **fields) -> None:
    """Emit one bounded JSON warning when explicitly enabled."""
    if not ENABLED:
        return
    record = {
        "event": event,
        "timestamp_s": round(time.time(), 6),
        "pid": os.getpid(),
        "thread_id": threading.get_ident(),
        "thread_name": threading.current_thread().name,
        "task_name": _task_name(),
    }
    record.update({name: _safe_value(value) for name, value in fields.items()})
    rendered = json.dumps(record, sort_keys=True, separators=(",", ":"))
    if len((MARKER + rendered).encode()) > MAX_RECORD_BYTES:
        for key in list(record):
            if isinstance(record[key], str) and len(record[key]) > 128:
                record[key] = record[key][:128]
        record["truncated"] = True
        rendered = json.dumps(record, sort_keys=True, separators=(",", ":"))
    logger.warning("%s%s", MARKER, rendered)


def multiplexer_state(multiplexer) -> dict:
    """Return the multiplexer-owned bounded state vector."""
    try:
        return multiplexer.diagnostic_state()
    except Exception as error:  # diagnostics must not change download behavior
        return {"snapshot_error": type(error).__name__}


@contextlib.asynccontextmanager
async def observe_wait(operation: str, *, snapshot, **fields):
    """Log periodically while the enclosed await remains pending.

    The observer never owns, cancels, or consumes the awaited operation. This
    preserves the queue-get semantics under investigation.
    """
    if not ENABLED:
        yield
        return

    started = time.monotonic()

    async def _observe():
        while True:
            await asyncio.sleep(WAIT_LOG_INTERVAL_SECONDS)
            try:
                state = snapshot()
            except Exception as error:  # diagnostic only
                state = {"snapshot_error": type(error).__name__}
            emit(
                "wait_stalled",
                operation=operation,
                elapsed_s=round(time.monotonic() - started, 3),
                **fields,
                **state,
            )

    observer = asyncio.create_task(_observe(), name="gcs-storage-read-wait-observer")
    try:
        yield
    finally:
        observer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await observer
