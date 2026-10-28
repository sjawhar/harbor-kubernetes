"""Public fake-cluster support for extension consumers."""

from __future__ import annotations

from pathlib import Path

from harbor_kubernetes.environment import KubernetesEnvironment
from harbor_kubernetes.testing import (
    IMAGE,
    NAMESPACE,
    POD,
    POD_UID,
    POLICY,
    SESSION,
    FakeCluster,
    FakeExecStream,
    fake_environment,
)


async def test_public_fake_environment_runs_the_requested_environment_class(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()

    environment = fake_environment(KubernetesEnvironment, tmp_path, cluster)
    await environment.start(False)

    assert (IMAGE, NAMESPACE, POD, POLICY, SESSION, POD_UID) == (
        "registry.example/harbor-task:sha-123",
        "harbor-trials",
        "task-7-ab3de-env",
        "task-7-ab3de-env-sandbox-egress",
        "task-7__Ab3dE__env",
        "0f6c2a8e-5d1b-4f7a-9c3e-2b8d4a6f1e90",
    )
    assert cluster.calls[-2:] == [("create_pod", POD), ("patch_policy", POLICY)]
    assert FakeExecStream(["true"], lambda _argv: (0, b"", b"")).returncode is None
