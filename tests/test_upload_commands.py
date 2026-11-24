"""The upload commands under a real /bin/sh, with input that stays open after its last byte.

A Kubernetes exec keeps the command's input open until the command exits, so an upload
command that stops reading before its advertised byte count leaves the exec waiting.
"""

from __future__ import annotations

import errno
import os
import random
import signal
import subprocess
import threading
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import pytest

from harbor_kubernetes.transport import (
    UPLOAD_DIR_SCRIPT,
    UPLOAD_FILE_SCRIPT,
    pack_directory,
)

# 1.5 MiB, far more than the 64 KiB a Linux pipe holds, so bytes a command leaves unread
# stay visible to the test rather than fitting in the pipe.
_PAYLOAD_BYTES = 3 * 512 * 1024
# Each case ends in well under a second. Only a command that waits for more input,
# which never comes, reaches this.
_EXIT_WITHIN_SEC = 10
# 512 KiB in the shell's 512-byte blocks: a write of the payload fails part way.
_FILE_SIZE_LIMIT_BLOCKS = 1024


@dataclass(frozen=True)
class _Finished:
    returncode: int
    stderr: str
    unread: int


def _payload() -> bytes:
    return random.Random(0).randbytes(_PAYLOAD_BYTES)


def _file_upload(count: int, target: Path) -> list[str]:
    return ["/bin/sh", "-c", UPLOAD_FILE_SCRIPT, "sh", str(count), str(target), "644"]


def _dir_upload(count: int, target: Path) -> list[str]:
    return ["/bin/sh", "-c", UPLOAD_DIR_SCRIPT, "sh", str(count), str(target)]


def _with_file_size_limit(argv: list[str]) -> list[str]:
    """Run *argv* under a file-size limit, where a write past it fails with EFBIG.

    The ignored SIGXFSZ passes to every child, so the write fails with an error
    message instead of killing the writer without one.
    """
    limit = f"ulimit -f {_FILE_SIZE_LIMIT_BLOCKS}; trap '' XFSZ; exec \"$@\""
    return ["/bin/sh", "-c", limit, "sh", *argv]


def _count_unread(read_end: int, writer: threading.Thread) -> int:
    """Read and count what an exited command left of its input."""
    os.set_blocking(read_end, False)
    unread = 0
    while True:
        sent = not writer.is_alive()
        try:
            while chunk := os.read(read_end, 64 * 1024):
                unread += len(chunk)
        except BlockingIOError:
            pass
        if sent:
            return unread
        writer.join(0.01)


def _send(write_end: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        view = view[os.write(write_end, view) :]


def _run_with_open_input(
    argv: list[str],
    payload: bytes,
    tmp_path: Path,
    exit_within_sec: float = _EXIT_WITHIN_SEC,
) -> _Finished:
    """Run *argv* with *payload* on a pipe that stays open after the last byte.

    The test keeps the pipe's read end as well, so the bytes the command did not read
    are still there after it exits, and are counted.
    """
    read_end, write_end = os.pipe()
    writer = threading.Thread(target=partial(_send, write_end, payload), daemon=True)
    try:
        # A session of its own lets a timeout kill the whole pipeline: its children
        # hold stderr open after the shell itself is gone.
        with subprocess.Popen(
            argv,
            stdin=read_end,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env={**os.environ, "TMPDIR": str(tmp_path)},
            errors="replace",
            start_new_session=True,
        ) as process:
            writer.start()
            try:
                stderr: str = process.communicate(timeout=exit_within_sec)[1]
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
                pytest.fail(f"the command did not exit within {exit_within_sec} s")
            finally:
                # The writer finishes before either end of its pipe closes.
                unread = _count_unread(read_end, writer)
    finally:
        os.close(write_end)
        os.close(read_end)
    return _Finished(process.returncode, stderr, unread)


@pytest.mark.parametrize("consumed", [0, 17])
def test_the_input_harness_counts_bytes_the_command_does_not_read(
    tmp_path: Path, consumed: int
) -> None:
    payload = _payload()
    argv = ["/bin/sh", "-c", 'head -c "$1" > /dev/null', "sh", str(consumed)]

    finished = _run_with_open_input(argv, payload, tmp_path)

    assert finished.returncode == 0, finished.stderr
    assert finished.unread == len(payload) - consumed


def test_a_timed_out_upload_terminates_its_pipeline(tmp_path: Path) -> None:
    payload = _payload()
    # Both heads wait for the missing byte while the input pipe stays open.
    argv = _file_upload(len(payload) + 1, tmp_path / "received")

    with pytest.raises(pytest.fail.Exception, match="the command did not exit within"):
        _run_with_open_input(argv, payload, tmp_path, exit_within_sec=0.1)


def _archive_of(payload: bytes, tmp_path: Path) -> bytes:
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.bin").write_bytes(payload)
    return pack_directory(source)


@pytest.mark.parametrize("directory", [False, True])
def test_an_upload_whose_first_step_fails_reads_its_bytes_and_fails_at_once(
    tmp_path: Path, directory: bool
) -> None:
    sent = _archive_of(_payload(), tmp_path) if directory else _payload()
    # mkdir -p cannot make a directory under a regular file.
    blocker = tmp_path / "a-file"
    blocker.touch()
    target = blocker / "received"
    argv = (
        _dir_upload(len(sent), target)
        if directory
        else _file_upload(len(sent), target / "data.bin")
    )

    finished = _run_with_open_input(argv, sent, tmp_path)

    assert finished.unread == 0, (
        f"the command left {finished.unread} of its {len(sent)} advertised bytes unread"
    )
    assert finished.returncode != 0
    assert "mkdir" in finished.stderr
    assert os.strerror(errno.ENOTDIR) in finished.stderr


@pytest.mark.parametrize("directory", [False, True])
def test_an_upload_whose_write_fails_reads_its_bytes_and_fails_at_once(
    tmp_path: Path, directory: bool
) -> None:
    if directory:
        # The archive is staged in an ordinary temporary file, so a file-size limit
        # makes that write fail.
        sent = _archive_of(_payload(), tmp_path)
        argv = _with_file_size_limit(_dir_upload(len(sent), tmp_path / "received"))
        error = os.strerror(errno.EFBIG)
    else:
        # /dev/full refuses every write, as a full disk does.
        sent = _payload()
        argv = _file_upload(len(sent), Path("/dev/full"))
        error = os.strerror(errno.ENOSPC)

    finished = _run_with_open_input(argv, sent, tmp_path)

    assert finished.unread == 0, (
        f"the command left {finished.unread} of its {len(sent)} advertised bytes unread"
    )
    assert finished.returncode != 0
    assert error in finished.stderr


@pytest.mark.parametrize("directory", [False, True])
def test_an_upload_arrives_whole_without_waiting_for_its_input_to_end(
    tmp_path: Path, directory: bool
) -> None:
    payload = _payload()
    if directory:
        sent = _archive_of(payload, tmp_path)
        target = tmp_path / "pod" / "received"
        argv = _dir_upload(len(sent), target)
        received = target / "data.bin"
    else:
        sent = payload
        received = tmp_path / "pod" / "data.bin"
        argv = _file_upload(len(sent), received)

    finished = _run_with_open_input(argv, sent, tmp_path)

    assert finished.returncode == 0, finished.stderr
    assert finished.unread == 0
    assert received.read_bytes() == payload
