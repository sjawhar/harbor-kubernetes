"""Opt-in staging proofs driven through stock ``harbor run``."""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pytest
from kubernetes import client as k8s_client

from harbor_kubernetes.network_policy import (
    CILIUM_POLICY_GROUP,
    CILIUM_POLICY_PLURAL,
    CILIUM_POLICY_VERSION,
)

_IMAGE = "python:3.13-slim"
_POLICY_POLL_INTERVAL_SEC = 0.2
_HARBOR_RUN_TIMEOUT_SEC = 300


@dataclass(frozen=True)
class HarborRun:
    """Observable result of one plain Harbor invocation."""

    elapsed_sec: float
    policy_names: frozenset[str]
    trial_result: dict[str, object]


class CiliumPolicyObserver:
    """Record policies seen while a single Harbor trial runs in its namespace."""

    def __init__(
        self, custom_objects_api: k8s_client.CustomObjectsApi, namespace: str
    ) -> None:
        self._custom_objects_api = custom_objects_api
        self._namespace = namespace
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._names: set[str] = set()
        self._error: k8s_client.ApiException | RuntimeError | None = None

    def start(self) -> None:
        """Begin recording CiliumNetworkPolicy names."""
        self._thread.start()

    def stop(self) -> frozenset[str]:
        """Stop recording and return every policy name that was observed."""
        self._stop.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError("Cilium policy observer did not stop")
        if self._error is not None:
            raise self._error
        return frozenset(self._names)

    def _poll(self) -> None:
        try:
            while not self._stop.is_set():
                for item in _policy_items(self._custom_objects_api, self._namespace):
                    metadata = item.get("metadata")
                    if not isinstance(metadata, dict):
                        continue
                    name = metadata.get("name")
                    if isinstance(name, str):
                        self._names.add(name)
                self._stop.wait(_POLICY_POLL_INTERVAL_SEC)
        except (k8s_client.ApiException, RuntimeError) as error:
            self._error = error
            self._stop.set()


@pytest.mark.e2e
def test_public_harbor_run_never_creates_a_cilium_network_policy(
    e2e_cluster, tmp_path: Path
) -> None:
    """Harbor's default public task completes without a Cilium policy."""
    run = _run_harbor(e2e_cluster, tmp_path, network_mode="public")

    assert _reward(run.trial_result) == 1
    assert run.policy_names == frozenset()
    assert _list_policy_names(e2e_cluster) == frozenset()
    print(f"public Harbor trial wall time: {run.elapsed_sec:.2f}s")


@pytest.mark.e2e
def test_no_network_harbor_run_waits_for_closed_dns_and_removes_its_policy(
    e2e_cluster, tmp_path: Path
) -> None:
    """A no-network task completes only after DNS TCP egress is closed."""
    run = _run_harbor(e2e_cluster, tmp_path, network_mode="no-network")

    assert _reward(run.trial_result) == 1
    assert run.policy_names
    assert _list_policy_names(e2e_cluster) == frozenset()
    assert run.trial_result["exception_info"] is None
    environment_setup = run.trial_result["environment_setup"]
    assert isinstance(environment_setup, dict)
    assert environment_setup["started_at"] is not None
    assert environment_setup["finished_at"] is not None
    print(f"no-network Harbor trial wall time: {run.elapsed_sec:.2f}s")


def _run_harbor(e2e_cluster, tmp_path: Path, *, network_mode: str) -> HarborRun:
    task_dir = _write_task(
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
            f"{error.stdout or ''}{error.stderr or ''}"
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


def _write_task(task_dir: Path, *, network_mode: str, cluster_dns_ip: str) -> Path:
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "solution").mkdir()
    (task_dir / "tests").mkdir()
    (task_dir / "instruction.md").write_text(
        "Write the expected completion marker.\n", encoding="utf-8"
    )
    (task_dir / "task.toml").write_text(
        "\n".join(
            [
                "[environment]",
                f'docker_image = "{_IMAGE}"',
                f'network_mode = "{network_mode}"',
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
        ),
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


def _list_policy_names(e2e_cluster) -> frozenset[str]:
    return frozenset(
        name
        for item in _policy_items(e2e_cluster.custom_objects_api, e2e_cluster.namespace)
        if isinstance(metadata := item.get("metadata"), dict)
        and isinstance(name := metadata.get("name"), str)
    )


def _policy_items(
    custom_objects_api: k8s_client.CustomObjectsApi, namespace: str
) -> list[dict[str, object]]:
    result = custom_objects_api.list_namespaced_custom_object(
        group=CILIUM_POLICY_GROUP,
        version=CILIUM_POLICY_VERSION,
        namespace=namespace,
        plural=CILIUM_POLICY_PLURAL,
    )
    if not isinstance(result, dict):
        raise RuntimeError(
            "list_namespaced_custom_object returned "
            + f"{type(result).__name__}, not a Cilium object list"
        )
    items = result.get("items")
    if not isinstance(items, list):
        raise RuntimeError("Cilium object list did not contain an items array")
    return [item for item in items if isinstance(item, dict)]


def _trial_result(jobs_dir: Path) -> dict[str, object]:
    trial_results: list[dict[str, object]] = []
    for path in jobs_dir.glob("*/**/result.json"):
        result = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(result, dict) and "verifier_result" in result:
            trial_results.append(result)
    assert len(trial_results) == 1
    return trial_results[0]


def _reward(trial_result: dict[str, object]) -> int | float:
    verifier_result = trial_result["verifier_result"]
    assert isinstance(verifier_result, dict)
    rewards = verifier_result["rewards"]
    assert isinstance(rewards, dict)
    reward = rewards["reward"]
    assert isinstance(reward, (int, float))
    return reward
