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

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Awaitable, Callable, Dict, Optional, Set

import grpc

from google.cloud import _storage_v2
from google.cloud.storage.asyncio.async_read_object_stream import (
    _AsyncReadObjectStream,
)
from google.cloud.storage.asyncio import _read_stall_diagnostics as diagnostics

logger = logging.getLogger(__name__)

_DEFAULT_QUEUE_MAX_SIZE = 100
_DEFAULT_PUT_TIMEOUT_SECONDS = 20.0


class _StreamError:
    """Wraps an error with the stream generation that produced it."""

    def __init__(self, exception: Exception, generation: int):
        self.exception = exception
        self.generation = generation


class _StreamEnd:
    """Signals the stream closed normally."""

    pass


class _StreamMultiplexer:
    """Multiplexes concurrent download tasks over a single bidi-gRPC stream.

    Routes responses from a background recv loop to per-task asyncio.Queues
    keyed by read_id. Coordinates stream reopening via generation-gated
    locking.

    A slow consumer on one task will slow down the entire shared connection
    due to bounded queue backpressure propagating through gRPC flow control.
    """

    def __init__(
        self,
        stream: _AsyncReadObjectStream,
        queue_max_size: int = _DEFAULT_QUEUE_MAX_SIZE,
    ):
        self._stream = stream
        self._stream_generation: int = 0
        self._queues: Dict[int, asyncio.Queue] = {}
        self._reopen_lock = asyncio.Lock()
        self._recv_task: Optional[asyncio.Task] = None
        self._queue_max_size = queue_max_size
        self._diagnostic_id = f"{os.getpid()}-{id(self):x}"
        self._recv_count = 0
        self._response_range_count = 0
        self._range_end_count = 0
        self._send_count = 0
        self._last_event = "init"
        self._last_event_at = time.monotonic()
        self._last_response_at = None
        self._last_recv_error_type = None

    @property
    def stream_generation(self) -> int:
        return self._stream_generation

    def _note(self, event: str) -> None:
        self._last_event = event
        self._last_event_at = time.monotonic()

    def diagnostic_state(self) -> dict:
        """Return bounded state suitable for a stalled queue-wait record."""
        now = time.monotonic()
        queues = self._get_unique_queues()
        depths = [queue.qsize() for queue in queues]
        if self._recv_task is None:
            recv_task_state = "not_started"
        elif self._recv_task.cancelled():
            recv_task_state = "cancelled"
        elif self._recv_task.done():
            recv_task_state = "done"
        else:
            recv_task_state = "pending"
        return {
            "multiplexer_id": self._diagnostic_id,
            "stream_generation": self._stream_generation,
            "registered_read_ids": len(self._queues),
            "unique_queues": len(queues),
            "queue_depth_total": sum(depths),
            "queue_depth_max": max(depths, default=0),
            "queue_capacity": self._queue_max_size,
            "recv_task_state": recv_task_state,
            "recv_count": self._recv_count,
            "response_range_count": self._response_range_count,
            "range_end_count": self._range_end_count,
            "send_count": self._send_count,
            "last_event": self._last_event,
            "last_event_age_s": round(now - self._last_event_at, 3),
            "last_response_age_s": (
                None
                if self._last_response_at is None
                else round(now - self._last_response_at, 3)
            ),
            "last_recv_error_type": self._last_recv_error_type,
        }

    def register(self, read_ids: Set[int]) -> asyncio.Queue:
        """Register read_ids for a task and return its response queue."""
        queue = asyncio.Queue(maxsize=self._queue_max_size)
        for read_id in read_ids:
            self._queues[read_id] = queue
        self._note("register")
        return queue

    def unregister(self, read_ids: Set[int]) -> None:
        """Remove read_ids from routing."""
        for read_id in read_ids:
            self._queues.pop(read_id, None)
        self._note("unregister")

    def _get_unique_queues(self) -> Set[asyncio.Queue]:
        return set(self._queues.values())

    async def _put_with_timeout(self, queue: asyncio.Queue, item) -> None:
        """Slow-path put: wait up to _DEFAULT_PUT_TIMEOUT_SECONDS, else drop.

        Callers should attempt ``queue.put_nowait(item)`` first and only call
        this when it raises :class:`asyncio.QueueFull`.
        """
        try:
            diagnostics.emit(
                "queue_full_wait",
                **self.diagnostic_state(),
            )
            await asyncio.wait_for(
                queue.put(item), timeout=_DEFAULT_PUT_TIMEOUT_SECONDS
            )
            self._note("queue_full_recovered")
        except asyncio.TimeoutError:
            if queue not in self._get_unique_queues():
                logger.debug("Dropped item for unregistered queue.")
            else:
                logger.warning(
                    "Queue full for too long. Dropping item to prevent multiplexer hang."
                )
                self._note("queue_item_dropped")
                diagnostics.emit(
                    "queue_item_dropped",
                    **self.diagnostic_state(),
                )

    async def _put_to_queues(self, queues, item) -> None:
        """Deliver ``item`` to each queue.

        Fast path: ``put_nowait`` for queues with capacity (no Task, no
        timer handle, no coroutine yield). Slow path: ``_put_with_timeout``
        only for queues that were full, and a single direct ``await`` when
        exactly one queue needs the slow path (skips ``asyncio.gather``).
        """
        slow_queues = None
        for q in queues:
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                if slow_queues is None:
                    slow_queues = [q]
                else:
                    slow_queues.append(q)
        if slow_queues is None:
            return
        if len(slow_queues) == 1:
            await self._put_with_timeout(slow_queues[0], item)
        else:
            await asyncio.gather(
                *(self._put_with_timeout(q, item) for q in slow_queues)
            )

    def _ensure_recv_loop(self) -> None:
        if self._recv_task is None or self._recv_task.done():
            self._recv_task = asyncio.create_task(self._recv_loop())
            self._note("recv_task_started")

    def _stop_recv_loop(self) -> None:
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()

    def _put_error_nowait(self, queue: asyncio.Queue, error: _StreamError) -> None:
        while True:
            try:
                queue.put_nowait(error)
                break
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass

    async def _recv_loop(self) -> None:
        diagnostics.emit("recv_loop_started", **self.diagnostic_state())
        try:
            while True:
                self._note("recv_wait")
                response = await self._stream.recv()
                if response == grpc.aio.EOF:
                    self._note("recv_eof")
                    diagnostics.emit("recv_eof", **self.diagnostic_state())
                    await self._put_to_queues(self._get_unique_queues(), _StreamEnd())
                    return

                self._recv_count += 1
                self._last_response_at = time.monotonic()
                if response.object_data_ranges:
                    self._response_range_count += len(response.object_data_ranges)
                    self._range_end_count += sum(
                        bool(data_range.range_end)
                        for data_range in response.object_data_ranges
                    )
                    self._note("recv_response")
                    queues_to_notify: Set[asyncio.Queue] = set()
                    for data_range in response.object_data_ranges:
                        read_id = data_range.read_range.read_id
                        queue = self._queues.get(read_id)
                        if queue:
                            queues_to_notify.add(queue)
                        else:
                            logger.warning(
                                f"Received data for unregistered read_id: {read_id}"
                            )
                    await self._put_to_queues(queues_to_notify, response)
                else:
                    self._note("recv_metadata")
                    await self._put_to_queues(self._get_unique_queues(), response)
        except asyncio.CancelledError:
            self._note("recv_cancelled")
            diagnostics.emit("recv_cancelled", **self.diagnostic_state())
            raise
        except Exception as e:
            self._last_recv_error_type = type(e).__name__
            self._note("recv_error")
            logger.warning(f"Stream multiplexer recv loop failed: {e}", exc_info=True)
            diagnostics.emit(
                "recv_error",
                error_type=type(e).__name__,
                error=str(e),
                **self.diagnostic_state(),
            )
            error = _StreamError(e, self._stream_generation)
            for queue in self._get_unique_queues():
                self._put_error_nowait(queue, error)

    async def send(self, request: _storage_v2.BidiReadObjectRequest) -> int:
        self._ensure_recv_loop()
        self._send_count += 1
        self._note("send_wait")
        try:
            async with diagnostics.observe_wait(
                "multiplexer.send",
                snapshot=self.diagnostic_state,
                requested_ranges=len(request.read_ranges),
            ):
                await self._stream.send(request)
        except BaseException as error:
            self._note("send_error")
            diagnostics.emit(
                "send_error",
                error_type=type(error).__name__,
                error=str(error),
                **self.diagnostic_state(),
            )
            raise
        self._note("send_complete")
        return self._stream_generation

    async def reopen_stream(
        self,
        broken_generation: int,
        stream_factory: Callable[[], Awaitable[_AsyncReadObjectStream]],
    ) -> None:
        async with self._reopen_lock:
            if self._stream_generation != broken_generation:
                diagnostics.emit(
                    "reopen_skipped",
                    broken_generation=broken_generation,
                    **self.diagnostic_state(),
                )
                return
            self._note("reopen_started")
            diagnostics.emit(
                "reopen_started",
                broken_generation=broken_generation,
                **self.diagnostic_state(),
            )
            self._stop_recv_loop()
            if self._recv_task:
                try:
                    await self._recv_task
                except (asyncio.CancelledError, Exception):
                    pass
            error = _StreamError(Exception("Stream reopening"), self._stream_generation)
            for queue in self._get_unique_queues():
                self._put_error_nowait(queue, error)
            try:
                await self._stream.close()
            except Exception:
                pass
            self._stream = await stream_factory()
            self._stream_generation += 1
            self._last_recv_error_type = None
            self._ensure_recv_loop()
            self._note("reopen_complete")
            diagnostics.emit("reopen_complete", **self.diagnostic_state())

    async def close(self) -> None:
        self._note("close_started")
        self._stop_recv_loop()
        if self._recv_task:
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):
                pass
        error = _StreamError(Exception("Multiplexer closed"), self._stream_generation)
        for queue in self._get_unique_queues():
            self._put_error_nowait(queue, error)
        self._note("close_complete")
        diagnostics.emit("close_complete", **self.diagnostic_state())
