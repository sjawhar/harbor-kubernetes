"""Task-definition checks for the real-cluster Harbor proof."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import override

import pytest
from e2e import test_harbor_run
from e2e.test_harbor_run import write_task
from kubernetes import client as k8s_client
from kubernetes import watch as k8s_watch


def test_no_network_task_checks_dns_during_environment_healthcheck(
    tmp_path: Path,
) -> None:
    task_dir = write_task(
        tmp_path / "task", network_mode="no-network", cluster_dns_ip="10.96.0.10"
    )

    task_toml = (task_dir / "task.toml").read_text(encoding="utf-8")

    assert "[environment.healthcheck]" in task_toml
    assert "socket.create_connection" in task_toml
    assert "retries = 1" in task_toml


def test_policy_observer_watches_from_the_initial_list_resource_version() -> None:
    class RecordingApi(k8s_client.CustomObjectsApi):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[dict[str, object]] = []

        @override
        def list_namespaced_custom_object(
            self,
            group: str,
            version: str,
            namespace: str,
            plural: str,
        ) -> dict[str, object]:
            self.calls.append(
                {
                    "group": group,
                    "version": version,
                    "namespace": namespace,
                    "plural": plural,
                }
            )
            return {"metadata": {"resourceVersion": "42"}, "items": []}

    class RecordingWatch(k8s_watch.Watch):
        """Yields one ADDED event, then blocks until stopped, as a real watch does."""

        def __init__(self) -> None:
            super().__init__()
            self.calls: list[dict[str, object]] = []
            self._stopped: threading.Event = threading.Event()

        @override
        def stream(
            self,
            func: Callable[..., dict[str, object]],
            *args: object,
            **kwargs: object,
        ) -> Iterator[dict[str, object]]:
            del func, args
            self.calls.append(kwargs)
            yield {
                "type": "ADDED",
                "object": {"metadata": {"name": "created-after-watch"}},
            }
            _ = self._stopped.wait(5)

        @override
        def stop(self) -> None:
            self._stopped.set()

    api = RecordingApi()
    watch = RecordingWatch()
    observer = test_harbor_run.CiliumPolicyObserver(api, "scratch", watch)

    observer.start()

    assert observer.stop() == frozenset({"created-after-watch"})
    assert api.calls == [
        {
            "group": "cilium.io",
            "version": "v2",
            "namespace": "scratch",
            "plural": "ciliumnetworkpolicies",
        }
    ]
    assert watch.calls == [
        {
            "group": "cilium.io",
            "version": "v2",
            "namespace": "scratch",
            "plural": "ciliumnetworkpolicies",
            "resource_version": "42",
            "timeout_seconds": 2,
        }
    ]


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_a_policy_watch_that_dies_before_stop_fails_the_observation() -> None:
    class ListingApi(k8s_client.CustomObjectsApi):
        @override
        def list_namespaced_custom_object(
            self,
            group: str,
            version: str,
            namespace: str,
            plural: str,
        ) -> dict[str, object]:
            del group, version, namespace, plural
            return {"metadata": {"resourceVersion": "42"}, "items": []}

    class FailingWatch(k8s_watch.Watch):
        """A watch whose connection fails with an error the observer does not catch."""

        @override
        def stream(
            self,
            func: Callable[..., dict[str, object]],
            *args: object,
            **kwargs: object,
        ) -> Iterator[dict[str, object]]:
            del func, args, kwargs

            def reset_connection() -> dict[str, object]:
                raise ConnectionResetError("connection reset by peer")

            return iter(reset_connection, {})

        @override
        def stop(self) -> None:
            return None

    observer = test_harbor_run.CiliumPolicyObserver(
        ListingApi(), "scratch", FailingWatch()
    )
    observer.start()

    with pytest.raises(RuntimeError, match="incomplete"):
        _ = observer.stop()
