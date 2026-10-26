"""Binary Kubernetes exec and tar-transfer primitives."""

from __future__ import annotations

import io
import tarfile
import time
from pathlib import Path
from typing import Protocol

_MISSING_PATH_EXIT = 44
_DIRECTORY_PATH_EXIT = 45
_TIMEOUT_EXIT_CODE = 124
_TIMEOUT_KILL_AFTER_SEC = 10
_EXEC_KEEPALIVE_SEC = 30.0

UPLOAD_FILE_SCRIPT = (
    'umask 022 && mkdir -p "$(dirname "$2")" && head -c "$1" > "$2" && chmod "$3" "$2"'
)
UPLOAD_DIR_SCRIPT = 'umask 022 && mkdir -p "$2" && head -c "$1" | tar -xf - -C "$2"'
DOWNLOAD_FILE_SCRIPT = (
    f'[ -e "$1" ] || exit {_MISSING_PATH_EXIT}; '
    + f'[ -d "$1" ] && exit {_DIRECTORY_PATH_EXIT}; '
    + 'exec cat -- "$1"'
)
DOWNLOAD_DIR_SCRIPT = (
    f'[ -d "$1" ] || exit {_MISSING_PATH_EXIT}; cd "$1" && exec tar -cf - .'
)


class KubernetesExecStreamClosedError(RuntimeError):
    """Raised when an exec stream closes without reporting a command status."""


class _Pingable(Protocol):
    def ping(self) -> None: ...


class ExecStream(Protocol):
    """The websocket surface used by a Kubernetes exec operation."""

    @property
    def sock(self) -> _Pingable: ...

    def is_open(self) -> bool: ...

    def update(self, timeout: float | None = 0) -> None: ...

    def peek_stdout(self, timeout: float = 0) -> str | bytes: ...

    def read_stdout(self, timeout: float | None = None) -> str | bytes: ...

    def peek_stderr(self, timeout: float = 0) -> str | bytes: ...

    def read_stderr(self, timeout: float | None = None) -> str | bytes: ...

    def write_stdin(self, data: str | bytes) -> None: ...

    @property
    def returncode(self) -> int | None: ...

    def close(self) -> None: ...


def as_bytes(data: str | bytes) -> bytes:
    """Return an exec frame as bytes without corrupting binary transfers."""
    return data.encode("utf-8") if isinstance(data, str) else data


def _drain(exec_stream: ExecStream, stdout: bytearray, stderr: bytearray) -> None:
    if exec_stream.peek_stdout():
        stdout += as_bytes(exec_stream.read_stdout(timeout=0))
    if exec_stream.peek_stderr():
        stderr += as_bytes(exec_stream.read_stderr(timeout=0))


def collect_output(exec_stream: ExecStream) -> tuple[bytes, bytes]:
    """Read a stream to its end while keeping long Kubernetes websockets alive."""
    stdout = bytearray()
    stderr = bytearray()
    last_ping = time.monotonic()
    while exec_stream.is_open():
        exec_stream.update(timeout=1)
        _drain(exec_stream, stdout, stderr)
        now = time.monotonic()
        if now - last_ping >= _EXEC_KEEPALIVE_SEC and exec_stream.is_open():
            exec_stream.sock.ping()
            last_ping = now
    _drain(exec_stream, stdout, stderr)
    return bytes(stdout), bytes(stderr)


def return_code(exec_stream: ExecStream) -> int:
    """Read a completed stream's command status or report a transport failure."""
    try:
        code = exec_stream.returncode
    except (TypeError, KeyError, IndexError, ValueError) as error:
        raise KubernetesExecStreamClosedError(
            "exec stream closed without a readable exit status"
        ) from error
    if code is None:
        raise KubernetesExecStreamClosedError(
            "exec stream closed before the command reported its exit status"
        )
    return code


def _root_owned(member: tarfile.TarInfo) -> tarfile.TarInfo:
    member.uid = member.gid = 0
    member.uname = member.gname = "root"
    return member


def pack_directory(source: Path) -> bytes:
    """Pack directory entries for root-owned extraction in a trial pod."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for path in sorted(source.rglob("*")):
            archive.add(
                path,
                arcname=path.relative_to(source).as_posix(),
                recursive=False,
                filter=_root_owned,
            )
    return buffer.getvalue()


def decode_output(data: bytes) -> str:
    """Decode command output while preserving an observable replacement marker."""
    return data.decode("utf-8", errors="replace")
