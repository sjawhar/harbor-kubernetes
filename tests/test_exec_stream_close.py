"""The close of an exec stream, which can block for seconds, while the event loop runs on.

``WSClient.close`` sends a close frame and then waits up to 3 s for the server's. A server
whose command still runs does not send it. The stand-in stream here marks itself closed and
then blocks for a set time in the same way.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncGenerator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import override

import pytest

from harbor_kubernetes.environment import KubernetesEnvironment
from harbor_kubernetes.testing import FakeCluster, FakeExecStream, fake_environment

# How long the stand-in close blocks after it marks the stream closed.
_CLOSE_SEC = 1.0
# How often the other coroutine of the loop wakes.
_TICK_SEC = 0.05
# Far shorter than the command, which runs until its stream closes.
_CLIENT_TIMEOUT_SEC = 0.2
# About 20 wakes fit in the close. A close on the loop's thread lets none happen.
_MIN_WAKES_DURING_CLOSE = 5
# What a close on a thread of its own leaves for the rest of _run.
_SLACK_SEC = 2.0

type _Answer = Callable[[list[str]], tuple[int, bytes, bytes]]


class _SlowCloseStream(FakeExecStream):
    """An exec stream whose close marks it closed, then blocks as ``WSClient.close`` does.

    Without an answer, the command runs until the stream closes.
    """

    def __init__(self, answer: _Answer | None = None) -> None:
        super().__init__(["sh", "-c", "sleep 30"], answer)
        self.closes: int = 0
        self.close_started: float = 0.0
        self.close_ended: float = 0.0
        self.running: threading.Event = threading.Event()
        self.closing: threading.Event = threading.Event()
        self.closed: threading.Event = threading.Event()
        self.status_read_before_close_ended: bool = False
        self._lock: threading.Lock = threading.Lock()

    @override
    def update(self, timeout: float | None = 0) -> None:
        self.running.set()
        if self.answer is None:
            # The command still runs: wait for output, as a websocket read does.
            self.closing.wait(timeout)
            return
        super().update(timeout)

    @property
    @override
    def returncode(self) -> int | None:
        if not self.closed.is_set():
            self.status_read_before_close_ended = True
        return super().returncode

    @override
    def close(self) -> None:
        with self._lock:
            self.closes += 1
        self.close_started = time.monotonic()
        super().close()
        self.closing.set()
        time.sleep(_CLOSE_SEC)
        self.close_ended = time.monotonic()
        self.closed.set()

    def end_command(self) -> None:
        """End the command without the slow close, so a failed test leaves no worker busy."""
        super().close()
        self.closing.set()


class _OneStreamCluster(FakeCluster):
    def __init__(self, stream: _SlowCloseStream) -> None:
        super().__init__()
        self.stream: _SlowCloseStream = stream

    @override
    def open_exec(
        self, pod: str, namespace: str, command: list[str], stdin: bool
    ) -> FakeExecStream:
        del pod, namespace, command, stdin
        return self.stream


class _Environment(KubernetesEnvironment):
    async def run_command(
        self, *, client_timeout_sec: float | None = None
    ) -> tuple[int, bytes, bytes]:
        return await self._run(
            ["sh", "-c", "sleep 30"], client_timeout_sec=client_timeout_sec
        )


def _environment(tmp_path: Path, stream: _SlowCloseStream) -> _Environment:
    environment = fake_environment(_Environment, tmp_path, _OneStreamCluster(stream))
    assert isinstance(environment, _Environment)
    return environment


def _use_one_default_worker() -> None:
    """Give the running loop a default executor of one worker.

    After a timeout or a cancellation, that worker still runs ``collect_output`` until the
    stream closes, so a close queued on the default executor would never start.
    """
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))


async def _tick(wakes: list[float]) -> None:
    while True:
        await asyncio.sleep(_TICK_SEC)
        wakes.append(time.monotonic())


@asynccontextmanager
async def _ticking() -> AsyncGenerator[list[float]]:
    """Run another coroutine of the loop that notes the time of each of its wakes."""
    wakes: list[float] = []
    ticker = asyncio.create_task(_tick(wakes))
    try:
        yield wakes
    finally:
        ticker.cancel()


def _wakes_during_close(stream: _SlowCloseStream, wakes: list[float]) -> int:
    assert stream.closed.is_set()
    return sum(stream.close_started < wake < stream.close_ended for wake in wakes)


async def _until(event: threading.Event, within_sec: float) -> None:
    deadline = time.monotonic() + within_sec
    while not event.is_set():
        if time.monotonic() > deadline:
            pytest.fail(f"waited {within_sec} s for the stand-in stream")
        await asyncio.sleep(0.01)


async def test_a_timed_out_command_closes_its_stream_while_the_loop_runs(
    tmp_path: Path,
) -> None:
    stream = _SlowCloseStream()
    environment = _environment(tmp_path, stream)

    async with _ticking() as wakes:
        with pytest.raises(TimeoutError):
            await environment.run_command(client_timeout_sec=_CLIENT_TIMEOUT_SEC)

    assert _wakes_during_close(stream, wakes) >= _MIN_WAKES_DURING_CLOSE
    assert stream.closes == 1


async def test_a_timed_out_command_closes_its_stream_while_the_default_executor_is_full(
    tmp_path: Path,
) -> None:
    _use_one_default_worker()
    stream = _SlowCloseStream()
    environment = _environment(tmp_path, stream)

    try:
        async with _ticking() as wakes:
            run = asyncio.create_task(
                environment.run_command(client_timeout_sec=_CLIENT_TIMEOUT_SEC)
            )
            # The close's time plus 2 s after the timeout expires.
            done, _ = await asyncio.wait(
                {run}, timeout=_CLIENT_TIMEOUT_SEC + _CLOSE_SEC + _SLACK_SEC
            )
            assert done, (
                "_run did not raise its timeout within the close's time plus 2 s"
            )
            with pytest.raises(TimeoutError):
                run.result()
    finally:
        stream.end_command()

    assert _wakes_during_close(stream, wakes) >= _MIN_WAKES_DURING_CLOSE
    assert stream.closes == 1


async def test_a_second_cancellation_during_the_close_leaves_one_close(
    tmp_path: Path,
) -> None:
    _use_one_default_worker()
    stream = _SlowCloseStream()
    environment = _environment(tmp_path, stream)
    run = asyncio.create_task(environment.run_command())

    try:
        await _until(stream.running, within_sec=_SLACK_SEC)
        run.cancel()
        await _until(stream.closing, within_sec=_SLACK_SEC)
        assert not stream.closed.is_set(), "the close held the loop until it ended"
        run.cancel()
        done, _ = await asyncio.wait({run}, timeout=_CLOSE_SEC + _SLACK_SEC)
        await _until(stream.closed, within_sec=_CLOSE_SEC + _SLACK_SEC)
    finally:
        stream.end_command()

    assert run in done
    assert run.cancelled()
    assert stream.closes == 1


async def test_a_command_that_exits_returns_its_status_and_output_after_one_close(
    tmp_path: Path,
) -> None:
    def exits(argv: list[str]) -> tuple[int, bytes, bytes]:
        del argv
        return 3, b"out", b"err"

    stream = _SlowCloseStream(exits)
    environment = _environment(tmp_path, stream)

    assert await environment.run_command() == (3, b"out", b"err")
    assert stream.closes == 1
    assert not stream.status_read_before_close_ended
