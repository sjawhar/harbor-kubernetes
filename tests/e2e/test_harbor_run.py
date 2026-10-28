"""Opt-in staging proofs driven through stock ``harbor run``."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Protocol, TypeGuard

import pytest
from kubernetes import client as k8s_client
from kubernetes import watch as k8s_watch

from harbor_kubernetes.network_policy import (
    CILIUM_POLICY_GROUP,
    CILIUM_POLICY_PLURAL,
    CILIUM_POLICY_VERSION,
)

_IMAGE: Final = "python:3.13-slim"
_HARBOR_RUN_TIMEOUT_SEC: Final = 300
_WATCH_READY_TIMEOUT_SEC: Final = 10


class E2ECluster(Protocol):
    """Live-cluster fixture fields used by the plain-Harbor proof."""

    kube_context: str
    namespace: str
    cluster_dns_ip: str
    custom_objects_api: k8s_client.CustomObjectsApi


@dataclass(frozen=True)
class HarborRun:
    """Observable result of one plain Harbor invocation."""

    elapsed_sec: float
    policy_names: frozenset[str]
    trial_result: dict[str, object]


class CiliumPolicyObserver:
    """Record Cilium policies added after a resource-versioned initial list."""

    def __init__(
        self,
        custom_objects_api: k8s_client.CustomObjectsApi,
        namespace: str,
        watch: k8s_watch.Watch | None = None,
    ) -> None:
        self._custom_objects_api: k8s_client.CustomObjectsApi = custom_objects_api
        self._namespace: str = namespace
        self._stop: threading.Event = threading.Event()
        self._ready: threading.Event = threading.Event()
        self._watch: k8s_watch.Watch = k8s_watch.Watch() if watch is None else watch
        self._thread: threading.Thread = threading.Thread(
            target=self._watch_policies, daemon=True
        )
        self._names: set[str] = set()
        self._error: Exception | None = None

    def start(self) -> None:
        """Begin watching after the initial list's resource version is established."""
        self._thread.start()
        if not self._ready.wait(_WATCH_READY_TIMEOUT_SEC):
            self._stop.set()
            self._watch.stop()
            self._thread.join(timeout=_WATCH_READY_TIMEOUT_SEC)
            raise RuntimeError("Cilium policy observer did not establish its watch")
        if self._error is not None:
            raise self._error

    def stop(self) -> frozenset[str]:
        """Stop recording and return every policy name that was added."""
        self._stop.set()
        self._watch.stop()
        self._thread.join(timeout=_WATCH_READY_TIMEOUT_SEC)
        if self._thread.is_alive():
            raise RuntimeError("Cilium policy observer did not stop")
        if self._error is not None:
            raise self._error
        return frozenset(self._names)

    def _watch_policies(self) -> None:
        try:
            resource_version = _resource_version(
                _policy_list(self._custom_objects_api, self._namespace)
            )
            self._ready.set()
            for event in self._watch.stream(
                self._custom_objects_api.list_namespaced_custom_object,
                group=CILIUM_POLICY_GROUP,
                version=CILIUM_POLICY_VERSION,
                namespace=self._namespace,
                plural=CILIUM_POLICY_PLURAL,
                resource_version=resource_version,
            ):
                if self._stop.is_set():
                    return
                if event.get("type") != "ADDED":
                    continue
                name = _policy_name(event.get("object"))
                if name is not None:
                    self._names.add(name)
        except Exception as error:
            self._error = error
            self._ready.set()


def _object_mapping(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict)


def _resource_version(policy_list: dict[str, object]) -> str:
    metadata = policy_list.get("metadata")
    if not _object_mapping(metadata):
        raise RuntimeError("Cilium policy list has no metadata.resourceVersion")
    resource_version = metadata.get("resourceVersion")
    if not isinstance(resource_version, str):
        raise RuntimeError("Cilium policy list has no metadata.resourceVersion")
    return resource_version


def _policy_name(value: object) -> str | None:
    if not _object_mapping(value):
        return None
    metadata = value.get("metadata")
    if not _object_mapping(metadata):
        return None
    name = metadata.get("name")
    return name if isinstance(name, str) else None


@pytest.mark.e2e
def test_public_harbor_run_never_creates_a_cilium_network_policy(
    e2e_cluster: E2ECluster, tmp_path: Path
) -> None:
    """Harbor's default public task completes without a Cilium policy."""
    run = _run_harbor(e2e_cluster, tmp_path, network_mode="public")

    assert _reward(run.trial_result) == 1
    assert run.policy_names == frozenset()
    assert _list_policy_names(e2e_cluster) == frozenset()
    print(f"public Harbor trial wall time: {run.elapsed_sec:.2f}s")


@pytest.mark.e2e
def test_no_network_harbor_run_waits_for_closed_dns_and_removes_its_policy(
    e2e_cluster: E2ECluster, tmp_path: Path
) -> None:
    """A no-network task completes only after DNS TCP egress is closed."""
    run = _run_harbor(e2e_cluster, tmp_path, network_mode="no-network")

    assert _reward(run.trial_result) == 1
    assert run.policy_names
    assert _list_policy_names(e2e_cluster) == frozenset()
    assert run.trial_result["exception_info"] is None
    environment_setup = run.trial_result["environment_setup"]
    assert _object_mapping(environment_setup)
    assert environment_setup["started_at"] is not None
    assert environment_setup["finished_at"] is not None
    print(f"no-network Harbor trial wall time: {run.elapsed_sec:.2f}s")


def _run_harbor(
    e2e_cluster: E2ECluster, tmp_path: Path, *, network_mode: str
) -> HarborRun:
    task_dir = write_task(
        tmp_path / "task",
        network_mode=network_mode,
        cluster_dns_ip=e2e_cluster.cluster_dns_ip,
    )
    jobs_dir = tmp_path / "jobs"
    config_path = tmp_path / "job.json"
    config_path.write_text(
        json.dumps(
            {
                "job_name": f"harbor-kubernetes-e2e-{network_mode}",
                "jobs_dir": str(jobs_dir),
                "n_attempts": 1,
                "n_concurrent_trials": 1,
                "agents": [{"name": "oracle"}],
                "tasks": [{"path": str(task_dir)}],
                "environment": {
                    "import_path": "harbor_kubernetes:KubernetesEnvironment",
                    # No `cluster_dns_ip`: run from outside the cluster, the environment must
                    # find the kube-dns Service itself, the path a plain `harbor run` takes.
                    "kwargs": {
                        "namespace": e2e_cluster.namespace,
                        "kube_context": e2e_cluster.kube_context,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    harbor = Path(sys.executable).parent / "harbor"
    version = subprocess.run(
        [str(harbor), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert version in {"0.23.0", "0.24.0"}
    observer = CiliumPolicyObserver(
        e2e_cluster.custom_objects_api, e2e_cluster.namespace
    )
    observer.start()
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [str(harbor), "run", "--config", str(config_path), "--yes", "--quiet"],
            check=False,
            capture_output=True,
            text=True,
            timeout=_HARBOR_RUN_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired as error:
        raise AssertionError(
            f"harbor run for {network_mode} exceeded {_HARBOR_RUN_TIMEOUT_SEC}s: "
            + f"{error.stdout or ''}{error.stderr or ''}"
        ) from error
    finally:
        policy_names = observer.stop()
    elapsed_sec = time.monotonic() - started
    assert completed.returncode == 0, completed.stdout + completed.stderr

    trial_result = _trial_result(jobs_dir)
    return HarborRun(
        elapsed_sec=elapsed_sec,
        policy_names=policy_names,
        trial_result=trial_result,
    )


def write_task(task_dir: Path, *, network_mode: str, cluster_dns_ip: str) -> Path:
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "solution").mkdir()
    (task_dir / "tests").mkdir()
    (task_dir / "instruction.md").write_text(
        "Write the expected completion marker.\n", encoding="utf-8"
    )
    task_toml = [
        "[environment]",
        f'docker_image = "{_IMAGE}"',
        f'network_mode = "{network_mode}"',
    ]
    if network_mode == "no-network":
        task_toml.extend(
            [
                "",
                "[environment.healthcheck]",
                f"command = {json.dumps(_no_network_healthcheck(cluster_dns_ip))}",
                "retries = 1",
                "timeout_sec = 5",
            ]
        )
    task_toml.extend(
        [
            "",
            "[agent]",
            "timeout_sec = 60",
            "",
            "[verifier]",
            "timeout_sec = 60",
            "",
            "[verifier.env]",
            f'CLUSTER_DNS_IP = "{cluster_dns_ip}"',
            f'EXPECT_NO_NETWORK = "{"1" if network_mode == "no-network" else "0"}"',
            "",
        ]
    )
    (task_dir / "task.toml").write_text(
        "\n".join(task_toml),
        encoding="utf-8",
    )
    (task_dir / "solution" / "solve.sh").write_text(
        "#!/bin/sh\nset -eu\nprintf 'complete\\n' > /tmp/harbor-kubernetes-e2e-solution\n",
        encoding="utf-8",
    )
    (task_dir / "tests" / "test.sh").write_text(
        """#!/bin/sh
set -eu

test "$(cat /tmp/harbor-kubernetes-e2e-solution)" = "complete"

if [ "$EXPECT_NO_NETWORK" = "1" ]; then
    python - "$CLUSTER_DNS_IP" <<'PY'
import socket
import sys

try:
    socket.create_connection((sys.argv[1], 53), timeout=4)
except OSError:
    pass
else:
    raise SystemExit(f"cluster DNS TCP 53 remains reachable at {sys.argv[1]}")
PY
    printf 'cluster DNS TCP 53 closed for %s\\n' "$CLUSTER_DNS_IP"
fi

printf '1\\n' > /logs/verifier/reward.txt
""",
        encoding="utf-8",
    )
    return task_dir


def _no_network_healthcheck(cluster_dns_ip: str) -> str:
    return (
        "python -c "
        + json.dumps(
            "import socket; "
            + f"socket.create_connection(({cluster_dns_ip!r}, 53), timeout=4)"
        )
        + " && exit 1 || exit 0"
    )




def _list_policy_names(e2e_cluster: E2ECluster) -> frozenset[str]:
    policy_list = _policy_list(
        e2e_cluster.custom_objects_api,
        e2e_cluster.namespace,
    )
    return frozenset(
        name
        for item in _policy_items(policy_list)
        if (name := _policy_name(item)) is not None
    )


def _policy_list(
    custom_objects_api: k8s_client.CustomObjectsApi, namespace: str
) -> dict[str, object]:
    return custom_objects_api.list_namespaced_custom_object(
        group=CILIUM_POLICY_GROUP,
        version=CILIUM_POLICY_VERSION,
        namespace=namespace,
        plural=CILIUM_POLICY_PLURAL,
    )


def _object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _policy_items(policy_list: dict[str, object]) -> list[dict[str, object]]:
    raw_items = policy_list.get("items")
    if not _object_list(raw_items):
        raise RuntimeError("Cilium policy list did not contain an items array")
    return [item for item in raw_items if _object_mapping(item)]


def _trial_result(jobs_dir: Path) -> dict[str, object]:
    trial_results: list[dict[str, object]] = []
    for path in jobs_dir.glob("*/**/result.json"):
        result: object = json.loads(path.read_text(encoding="utf-8"))
        if _object_mapping(result) and "verifier_result" in result:
            trial_results.append(result)
    assert len(trial_results) == 1
    return trial_results[0]


def _reward(trial_result: dict[str, object]) -> int | float:
    verifier_result = trial_result["verifier_result"]
    assert _object_mapping(verifier_result)
    rewards = verifier_result["rewards"]
    assert _object_mapping(rewards)
    reward = rewards["reward"]
    assert isinstance(reward, (int, float))
    return reward
