"""Fake Kubernetes surfaces for testing Harbor environment extensions."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Final

from harbor.models.task.config import EnvironmentConfig, NetworkMode, NetworkPolicy
from harbor.models.trial.config import ServiceVolumeConfig
from harbor.models.trial.paths import TrialPaths
from kubernetes import client as k8s_client

from harbor_kubernetes.environment import KubernetesApis, KubernetesEnvironment
from harbor_kubernetes.network_policy import (
    CiliumNetworkPolicyManifest,
    CiliumPolicyOwnerPatch,
    SESSION_ID_ANNOTATION,
)

__all__ = [
    "IMAGE",
    "NAMESPACE",
    "POD",
    "POD_UID",
    "POLICY",
    "SESSION",
    "FakeCluster",
    "FakeExecStream",
    "fake_environment",
    "network_policy",
]

IMAGE: Final = "registry.example/harbor-task:sha-123"
NAMESPACE: Final = "harbor-trials"
SESSION: Final = "task-7__Ab3dE__env"
POD: Final = "task-7-ab3de-env"
POLICY: Final = f"{POD}-sandbox-egress"
POD_UID: Final = "0f6c2a8e-5d1b-4f7a-9c3e-2b8d4a6f1e90"

type Answer = Callable[[list[str]], tuple[int, bytes, bytes]]


class FakeExecStream:
    """A Kubernetes exec websocket that runs the provider's shell protocol locally."""

    def __init__(self, argv: list[str], answer: Answer | None) -> None:
        self.argv = argv
        self.answer = answer
        self.stdin = bytearray()
        self.open = True
        self.channels: dict[int, bytes] = {}
        self.status: int | None = None
        self.pings = 0

    @property
    def sock(self) -> FakeExecStream:
        return self

    def ping(self) -> None:
        self.pings += 1

    def write_stdin(self, data: str | bytes) -> None:
        self.stdin += data.encode("utf-8") if isinstance(data, str) else data

    def is_open(self) -> bool:
        return self.open

    def update(self, timeout: float | None = 0) -> None:
        del timeout
        if not self.open:
            return
        if self.answer is None:
            complete = subprocess.run(
                self.argv,
                input=bytes(self.stdin),
                capture_output=True,
                check=False,
                timeout=120,
            )
            self.status, stdout, stderr = (
                complete.returncode,
                complete.stdout,
                complete.stderr,
            )
        else:
            self.status, stdout, stderr = self.answer(self.argv)
        self.channels = {1: stdout, 2: stderr}
        self.open = False

    def peek_stdout(self, timeout: float = 0) -> bytes:
        self.update(timeout)
        return self.channels.get(1, b"")

    def read_stdout(self, timeout: float | None = None) -> bytes:
        self.update(timeout)
        return self.channels.pop(1, b"")

    def peek_stderr(self, timeout: float = 0) -> bytes:
        self.update(timeout)
        return self.channels.get(2, b"")

    def read_stderr(self, timeout: float | None = None) -> bytes:
        self.update(timeout)
        return self.channels.pop(2, b"")

    @property
    def returncode(self) -> int | None:
        return None if self.open else self.status

    def close(self) -> None:
        self.open = False


def _conflict() -> k8s_client.ApiException:
    return k8s_client.ApiException(status=409, reason="AlreadyExists")


def _not_found() -> k8s_client.ApiException:
    return k8s_client.ApiException(status=404, reason="NotFound")


class FakeCluster:
    """The pod, Cilium custom-resource, service, and exec surfaces a trial uses."""

    def __init__(
        self,
        *,
        egress_closes_after: int = 0,
        waiting_reason: str | None = None,
        cilium_create_error: k8s_client.ApiException | None = None,
        cilium_reports_status: bool = False,
    ) -> None:
        self.pods: dict[str, k8s_client.V1Pod] = {}
        self.policies: dict[str, CiliumNetworkPolicyManifest] = {}
        self.resource_versions: dict[str, int] = {}
        self.status_writes_pending: dict[str, int] = {}
        self.status_writes = 0
        self._cilium_reports_status = cilium_reports_status
        self.calls: list[tuple[str, str]] = []
        self.pod_deletions: list[k8s_client.V1DeleteOptions | None] = []
        self.egress_closes_after = egress_closes_after
        self.probes = 0
        self.waiting_reason = waiting_reason
        self.cilium_enabled = True
        self.cilium_create_error = cilium_create_error
        self.service_dns_ip: str | None = "10.96.0.10"

    def create_namespaced_pod(
        self, namespace: str, body: k8s_client.V1Pod
    ) -> k8s_client.V1Pod:
        del namespace
        metadata = body.metadata
        assert metadata is not None
        assert metadata.name is not None
        if metadata.name in self.pods:
            raise _conflict()
        self.calls.append(("create_pod", metadata.name))
        metadata.uid = POD_UID
        self.pods[metadata.name] = body
        return body

    def read_namespaced_pod(self, name: str, namespace: str) -> k8s_client.V1Pod:
        del namespace
        pod = self.pods.get(name)
        if pod is None:
            raise _not_found()
        waiting = (
            k8s_client.V1ContainerStateWaiting(
                reason=self.waiting_reason, message="pull access denied"
            )
            if self.waiting_reason is not None
            else None
        )
        pod.status = k8s_client.V1PodStatus(
            phase="Pending" if waiting else "Running",
            container_statuses=[
                k8s_client.V1ContainerStatus(
                    name="main",
                    image=IMAGE,
                    image_id=IMAGE,
                    ready=waiting is None,
                    restart_count=0,
                    state=k8s_client.V1ContainerState(waiting=waiting),
                )
            ],
        )
        return pod

    def delete_namespaced_pod(
        self,
        name: str,
        namespace: str,
        *,
        body: k8s_client.V1DeleteOptions | None = None,
    ) -> object:
        del namespace
        if self.pods.pop(name, None) is None:
            raise _not_found()
        self.calls.append(("delete_pod", name))
        self.pod_deletions.append(body)
        return None

    def read_namespaced_service(
        self, name: str, namespace: str
    ) -> k8s_client.V1Service:
        assert (name, namespace) == ("kube-dns", "kube-system")
        if self.service_dns_ip is None:
            raise _not_found()
        return k8s_client.V1Service(
            spec=k8s_client.V1ServiceSpec(cluster_ip=self.service_dns_ip)
        )

    def _require_cilium(self, group: str, version: str, plural: str) -> None:
        assert (group, version, plural) == (
            "cilium.io",
            "v2",
            "ciliumnetworkpolicies",
        )
        if not self.cilium_enabled:
            raise _not_found()

    def create_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        body: CiliumNetworkPolicyManifest,
    ) -> object:
        if self.cilium_create_error is not None:
            raise self.cilium_create_error
        self._require_cilium(group, version, plural)
        metadata = body["metadata"]
        assert metadata["namespace"] == namespace
        name = metadata["name"]
        if name in self.policies:
            raise _conflict()
        self.calls.append(("create_policy", name))
        self.policies[name] = body
        self.resource_versions[name] = 1
        self._spec_written(name)
        self.probes = 0
        return body

    def _spec_written(self, name: str) -> None:
        if self._cilium_reports_status:
            self.status_writes_pending[name] = (
                self.status_writes_pending.get(name, 0) + 1
            )

    def _land_status_write(self, name: str) -> None:
        if self.status_writes_pending.get(name, 0) > 0:
            self.status_writes_pending[name] -= 1
            self.status_writes += 1
            self.resource_versions[name] += 1

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> object:
        self._require_cilium(group, version, plural)
        del namespace
        if name not in self.policies:
            raise _not_found()
        policy = self.policies[name]
        live: object = {
            **policy,
            "metadata": {
                **policy["metadata"],
                "resourceVersion": str(self.resource_versions[name]),
            },
        }
        self._land_status_write(name)
        return live

    def patch_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: CiliumPolicyOwnerPatch,
    ) -> object:
        self._require_cilium(group, version, plural)
        del namespace
        owner_references = body.get("metadata", {}).get("ownerReferences")
        assert isinstance(owner_references, list)
        assert all(isinstance(owner, dict) for owner in owner_references)
        if name not in self.policies:
            raise _not_found()
        self._land_status_write(name)
        self.calls.append(("patch_policy", name))
        policy = self.policies[name]
        self.policies[name] = {
            **policy,
            "metadata": {
                **policy["metadata"],
                "ownerReferences": owner_references,
            },
        }
        self.resource_versions[name] += 1
        return body

    def replace_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: CiliumNetworkPolicyManifest,
    ) -> object:
        self._require_cilium(group, version, plural)
        del namespace
        if body["metadata"].get("resourceVersion") != str(self.resource_versions[name]):
            self.calls.append(("replace_policy_conflict", name))
            raise k8s_client.ApiException(status=409, reason="Conflict")
        self.calls.append(("replace_policy", name))
        self.policies[name] = body
        self.resource_versions[name] += 1
        self.probes = 0
        self._spec_written(name)
        return body

    def delete_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> object:
        self._require_cilium(group, version, plural)
        del namespace
        if self.policies.pop(name, None) is None:
            raise _not_found()
        self.calls.append(("delete_policy", name))
        return None

    def _denies_egress(self) -> bool:
        return any(
            policy["spec"].get("egressDeny") == [{"toEntities": ["all"]}]
            for policy in self.policies.values()
        )

    def _answer_probe(self, argv: list[str]) -> tuple[int, bytes, bytes]:
        del argv
        self.probes += 1
        closed = self._denies_egress() and self.probes > self.egress_closes_after
        return 0, (b"CLOSED\n" if closed else b"OPEN\n"), b""

    def open_exec(
        self, pod: str, namespace: str, command: list[str], stdin: bool
    ) -> FakeExecStream:
        del namespace, stdin
        if pod not in self.pods:
            raise _not_found()
        probe = "/dev/tcp/" in command[-1]
        return FakeExecStream(command, self._answer_probe if probe else None)


def network_policy(mode: NetworkMode) -> NetworkPolicy:
    """Return a minimal policy for the named network mode."""
    return NetworkPolicy(network_mode=mode)


def fake_environment(
    environment_class: type[KubernetesEnvironment],
    tmp_path: Path,
    cluster: FakeCluster,
    *,
    baseline: NetworkMode = NetworkMode.ALLOWLIST,
    phase: NetworkMode = NetworkMode.NO_NETWORK,
    pod_labels: dict[str, str] | None = None,
    runtime_class_name: str | None = None,
    annotations: dict[str, str] | None = None,
    docker_image: str | None = IMAGE,
    workdir: Path | None = None,
    mounts: list[ServiceVolumeConfig] | None = None,
    ephemeral_storage_request: str | None = None,
    ephemeral_storage_limit: str | None = None,
    allow_public_network: bool = True,
    cluster_dns_ip: str | None = None,
) -> KubernetesEnvironment:
    """Build *environment_class* against *cluster* with fast polling for tests."""
    environment_dir = tmp_path / "task" / "environment"
    environment_dir.mkdir(parents=True, exist_ok=True)
    environment = environment_class(
        environment_dir=environment_dir,
        environment_name="task-7",
        session_id=SESSION,
        trial_paths=TrialPaths(trial_dir=tmp_path / "trial"),
        task_env_config=EnvironmentConfig(
            docker_image=docker_image,
            cpus=2,
            memory_mb=4096,
            workdir=None if workdir is None else str(workdir),
        ),
        namespace=NAMESPACE,
        pod_labels=pod_labels,
        runtime_class_name=runtime_class_name,
        annotations=annotations,
        network_policy=network_policy(baseline),
        phase_network_policies=[network_policy(phase)],
        mounts=mounts,
        ephemeral_storage_request=ephemeral_storage_request,
        ephemeral_storage_limit=ephemeral_storage_limit,
        allow_public_network=allow_public_network,
        cluster_dns_ip=cluster_dns_ip,
        apis=KubernetesApis(cluster, cluster, cluster.open_exec),
    )
    environment.POLL_INTERVAL_SEC = 0.0
    environment.POLICY_PROBE_INTERVAL_SEC = 0.0
    return environment
