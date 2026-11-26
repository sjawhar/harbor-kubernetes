"""The close of an exec stream, which can block for seconds, while the event loop runs on.

``WSClient.close`` sends a close frame and then waits up to 3 s for the server's. A server
whose command still runs does not send it. The stand-in stream here marks itself closed and
then blocks for a set time in the same way.
"""

from __future__ import annotations

import asyncio
import gc
import threading
import time
from collections.abc import AsyncGenerator, Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, final, override

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
# The name of the default executor's one worker, so a test can tell its thread apart.
_DEFAULT_WORKER_PREFIX = "default-exec-test"

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
        self.close_thread_id: int | None = None
        self.close_thread_name: str | None = None
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
            self.close_thread_id = threading.get_ident()
            self.close_thread_name = threading.current_thread().name
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


class _BlockedOpenCluster(_OneStreamCluster):
    """A cluster whose ``open_exec`` blocks until released, as a slow exec upgrade does."""

    def __init__(self, stream: _SlowCloseStream) -> None:
        super().__init__(stream)
        self.opening: threading.Event = threading.Event()
        self.release_open: threading.Event = threading.Event()
        self.open_returned: threading.Event = threading.Event()
        self.open_thread_id: int | None = None

    @override
    def open_exec(
        self, pod: str, namespace: str, command: list[str], stdin: bool
    ) -> FakeExecStream:
        self.open_thread_id = threading.get_ident()
        self.opening.set()
        self.release_open.wait()
        stream = super().open_exec(pod, namespace, command, stdin)
        self.open_returned.set()
        return stream


@final
class _FailingOpenCluster(_BlockedOpenCluster):
    """A cluster whose blocked ``open_exec`` raises once released."""

    def __init__(self, stream: _SlowCloseStream, error: BaseException) -> None:
        super().__init__(stream)
        self._error: BaseException = error

    @override
    def open_exec(
        self, pod: str, namespace: str, command: list[str], stdin: bool
    ) -> FakeExecStream:
        self.open_thread_id = threading.get_ident()
        self.opening.set()
        self.release_open.wait()
        self.open_returned.set()
        raise self._error


@final
class _FailingCloseStream(_SlowCloseStream):
    """A stream whose ``close`` records the thread and raises."""

    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self._error: BaseException = error
        self.close_thread: threading.Thread | None = None

    @override
    def close(self) -> None:
        with self._lock:
            self.closes += 1
            self.close_thread = threading.current_thread()
            self.close_thread_id = threading.get_ident()
            self.close_thread_name = self.close_thread.name
        FakeExecStream.close(self)
        self.closing.set()
        self.closed.set()
        raise self._error


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
    """Give the running loop a default executor of one worker, named by a set prefix.

    After a timeout or a cancellation, that worker still runs ``collect_output`` until the
    stream closes, so a close queued on the default executor would never start.
    """
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=1, thread_name_prefix=_DEFAULT_WORKER_PREFIX)
    )


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


@contextmanager
def _capture_unhandled_exceptions() -> Generator[list[dict[str, Any]]]:
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    unhandled: list[dict[str, Any]] = []

    def record(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        unhandled.append(context)

    loop.set_exception_handler(record)
    try:
        yield unhandled
    finally:
        loop.set_exception_handler(previous)


async def _until_tasks_in(
    loop: asyncio.AbstractEventLoop,
    baseline: set[asyncio.Task[Any]],
    *,
    within_sec: float,
) -> None:
    deadline = time.monotonic() + within_sec
    while not asyncio.all_tasks(loop) <= baseline:
        if time.monotonic() > deadline:
            pytest.fail(f"waited {within_sec} s for background tasks to settle")
        await asyncio.sleep(0.01)


async def _until_thread_exits(thread: threading.Thread, *, within_sec: float) -> None:
    deadline = time.monotonic() + within_sec
    while thread.is_alive():
        if time.monotonic() > deadline:
            pytest.fail(f"waited {within_sec} s for close thread to exit")
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


async def test_a_cancellation_during_open_exec_closes_the_stream_off_the_loop(
    tmp_path: Path,
) -> None:
    _use_one_default_worker()
    loop_thread_id = threading.get_ident()
    stream = _SlowCloseStream()
    cluster = _BlockedOpenCluster(stream)
    environment = fake_environment(_Environment, tmp_path, cluster)
    assert isinstance(environment, _Environment)
    run = asyncio.create_task(environment.run_command())

    try:
        await _until(cluster.opening, within_sec=_SLACK_SEC)
        run.cancel()
        done, _ = await asyncio.wait({run}, timeout=_SLACK_SEC)
        assert run in done, (
            "_run did not raise its cancellation while open_exec blocked"
        )
        with pytest.raises(asyncio.CancelledError):
            await run
        assert not cluster.release_open.is_set()
        assert not cluster.open_returned.is_set(), (
            "_run raised its cancellation only after open_exec returned"
        )

        cluster.release_open.set()
        await _until(cluster.open_returned, within_sec=_SLACK_SEC)
        # The close's time fits in this wait; a stream nobody closes never sets closed.
        deadline = time.monotonic() + _SLACK_SEC
        while not stream.closed.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
    finally:
        cluster.release_open.set()
        stream.end_command()

    assert stream.closes == 1, (
        "the stream returned by open_exec after cancellation was left unclosed "
        + f"(closes={stream.closes})"
    )
    assert stream.closed.is_set(), (
        f"the stream close did not finish within {_SLACK_SEC} s"
    )
    assert stream.close_thread_id is not None
    assert stream.close_thread_id != loop_thread_id
    assert stream.close_thread_id != cluster.open_thread_id
    assert stream.close_thread_name is not None
    assert not stream.close_thread_name.startswith(_DEFAULT_WORKER_PREFIX)


async def test_a_cancellation_during_open_exec_that_raises_leaves_no_unhandled_exception(
    tmp_path: Path,
) -> None:
    _use_one_default_worker()
    stream = _SlowCloseStream()
    cluster = _FailingOpenCluster(stream, RuntimeError("exec upgrade failed"))
    environment = fake_environment(_Environment, tmp_path, cluster)
    assert isinstance(environment, _Environment)
    loop = asyncio.get_running_loop()
    baseline = asyncio.all_tasks(loop)

    with _capture_unhandled_exceptions() as unhandled:
        run = asyncio.create_task(environment.run_command())
        try:
            await _until(cluster.opening, within_sec=_SLACK_SEC)
            run.cancel()
            done, _ = await asyncio.wait({run}, timeout=_SLACK_SEC)
            assert run in done
            with pytest.raises(asyncio.CancelledError):
                await run

            cluster.release_open.set()
            await _until(cluster.open_returned, within_sec=_SLACK_SEC)
            await _until_tasks_in(loop, baseline, within_sec=_SLACK_SEC)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            gc.collect()
        finally:
            cluster.release_open.set()
            stream.end_command()

    assert stream.closes == 0
    assert unhandled == []


async def test_a_cancellation_during_open_exec_whose_close_raises_leaves_no_unhandled_exception(
    tmp_path: Path,
) -> None:
    _use_one_default_worker()
    loop_thread_id = threading.get_ident()
    stream = _FailingCloseStream(RuntimeError("close failed"))
    cluster = _BlockedOpenCluster(stream)
    environment = fake_environment(_Environment, tmp_path, cluster)
    assert isinstance(environment, _Environment)
    loop = asyncio.get_running_loop()
    baseline = asyncio.all_tasks(loop)

    with _capture_unhandled_exceptions() as unhandled:
        run = asyncio.create_task(environment.run_command())
        try:
            await _until(cluster.opening, within_sec=_SLACK_SEC)
            run.cancel()
            done, _ = await asyncio.wait({run}, timeout=_SLACK_SEC)
            assert run in done
            with pytest.raises(asyncio.CancelledError):
                await run

            cluster.release_open.set()
            await _until(cluster.open_returned, within_sec=_SLACK_SEC)
            await _until(stream.closed, within_sec=_SLACK_SEC)
            await _until_tasks_in(loop, baseline, within_sec=_SLACK_SEC)
            assert stream.close_thread is not None
            await _until_thread_exits(stream.close_thread, within_sec=_SLACK_SEC)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            gc.collect()
        finally:
            cluster.release_open.set()
            stream.end_command()

    assert stream.closes == 1
    assert stream.close_thread_id is not None
    assert stream.close_thread_id != loop_thread_id
    assert stream.close_thread_id != cluster.open_thread_id
    assert stream.close_thread_name is not None
    assert not stream.close_thread_name.startswith(_DEFAULT_WORKER_PREFIX)
    assert unhandled == []


def test_a_stream_that_open_exec_returns_after_the_loop_ends_is_closed(
    tmp_path: Path,
) -> None:
    """A caller cancels ``_run`` while ``open_exec`` blocks, and then its loop ends.

    ``asyncio.run`` cancels every task that is left when its coroutine returns, as a
    shutdown that cancels all tasks does, and only then waits for the default executor's
    workers. So ``open_exec`` returns its stream after the loop has stopped running
    callbacks, and nothing on the loop can close that stream any more.
    """
    caller_thread_id = threading.get_ident()
    stream = _SlowCloseStream()
    cluster = _BlockedOpenCluster(stream)
    environment = fake_environment(_Environment, tmp_path, cluster)
    assert isinstance(environment, _Environment)
    # Far longer than the loop takes to cancel its tasks once the coroutine returns.
    release = threading.Timer(0.5, cluster.release_open.set)

    async def cancel_while_open_exec_blocks() -> None:
        _use_one_default_worker()
        run = asyncio.create_task(environment.run_command())
        await _until(cluster.opening, within_sec=_SLACK_SEC)
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run
        assert not cluster.open_returned.is_set()
        release.start()

    try:
        asyncio.run(cancel_while_open_exec_blocks())
        assert cluster.open_returned.wait(_SLACK_SEC)
        closed = stream.closed.wait(_CLOSE_SEC + _SLACK_SEC)
    finally:
        release.cancel()
        cluster.release_open.set()
        stream.end_command()

    assert closed, (
        "the stream that open_exec returned after the loop ended was left unclosed "
        + f"(closes={stream.closes})"
    )
    assert stream.closes == 1
    assert stream.close_thread_id is not None
    assert stream.close_thread_id != caller_thread_id
    assert stream.close_thread_id != cluster.open_thread_id
    assert stream.close_thread_name is not None
    assert not stream.close_thread_name.startswith(_DEFAULT_WORKER_PREFIX)
