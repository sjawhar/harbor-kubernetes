"""Kubernetes Harbor environment unit tests against an in-process fake cluster."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path
from typing import Callable, Final, override

import pytest
from harbor.models.task.config import EnvironmentConfig, NetworkMode, NetworkPolicy
from harbor.models.trial.config import ServiceVolumeConfig
from harbor.models.trial.paths import TrialPaths
from kubernetes import client as k8s_client
from pydantic import TypeAdapter

from harbor_kubernetes.environment import (
    RUNNER_RESOLV_CONF,
    KubernetesApis,
    KubernetesEnvironment,
    KubernetesPodGoneError,
    egress_probe,
)
from harbor_kubernetes.network_policy import (
    SESSION_ID_ANNOTATION,
    SESSION_LABEL,
    CiliumNetworkPolicyManifest,
    CiliumPolicyOwnerPatch,
)

_IMAGE = "registry.example/harbor-task:sha-123"
_NAMESPACE = "harbor-trials"
_SESSION = "task-7__Ab3dE__env"
_POD = "task-7-ab3de-env"
_POLICY = f"{_POD}-sandbox-egress"
_POD_UID = "0f6c2a8e-5d1b-4f7a-9c3e-2b8d4a6f1e90"

_OWNER_PATCH: Final = TypeAdapter(CiliumPolicyOwnerPatch)


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
        # Cilium status writes that have not landed, per policy. Each lands at the
        # worst moment for this provider: after its next read and before its replace,
        # or immediately before an owner patch.
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
        metadata.uid = _POD_UID
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
                    image=_IMAGE,
                    image_id=_IMAGE,
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
        """Land one pending Cilium status write for *name*, if present."""
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
        live = {
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
        """Apply the owner-only JSON merge patch this provider sends."""
        self._require_cilium(group, version, plural)
        del namespace
        patch = _OWNER_PATCH.validate_python(body)
        assert body == {
            "metadata": {"ownerReferences": patch["metadata"]["ownerReferences"]}
        }, body
        if name not in self.policies:
            raise _not_found()
        self._land_status_write(name)
        self.calls.append(("patch_policy", name))
        policy = self.policies[name]
        self.policies[name] = {
            **policy,
            "metadata": {
                **policy["metadata"],
                "ownerReferences": patch["metadata"]["ownerReferences"],
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


def _policy(mode: NetworkMode) -> NetworkPolicy:
    return NetworkPolicy(network_mode=mode)


def _environment(
    tmp_path: Path,
    cluster: FakeCluster,
    *,
    baseline: NetworkMode = NetworkMode.ALLOWLIST,
    phase: NetworkMode = NetworkMode.NO_NETWORK,
    pod_labels: dict[str, str] | None = None,
    runtime_class_name: str | None = None,
    annotations: dict[str, str] | None = None,
    docker_image: str | None = _IMAGE,
    workdir: Path | None = None,
    mounts: list[ServiceVolumeConfig] | None = None,
    ephemeral_storage_request: str | None = None,
    ephemeral_storage_limit: str | None = None,
    allow_public_network: bool = True,
    cluster_dns_ip: str | None = None,
) -> KubernetesEnvironment:
    class FakeClusterEnvironment(KubernetesEnvironment):
        POLL_INTERVAL_SEC = 0.0
        POLICY_PROBE_INTERVAL_SEC = 0.0

        @override
        def _connect(self) -> KubernetesApis:
            return KubernetesApis(cluster, cluster, cluster.open_exec)

        @override
        def cluster_dns_probe_target(self) -> str:
            return "172.20.0.10"

    environment_dir = tmp_path / "task" / "environment"
    environment_dir.mkdir(parents=True, exist_ok=True)
    return FakeClusterEnvironment(
        environment_dir=environment_dir,
        environment_name="task-7",
        session_id=_SESSION,
        trial_paths=TrialPaths(trial_dir=tmp_path / "trial"),
        task_env_config=EnvironmentConfig(
            docker_image=docker_image,
            cpus=2,
            memory_mb=4096,
            workdir=None if workdir is None else str(workdir),
        ),
        namespace=_NAMESPACE,
        pod_labels=pod_labels,
        runtime_class_name=runtime_class_name,
        annotations=annotations,
        network_policy=_policy(baseline),
        phase_network_policies=[_policy(phase)],
        mounts=mounts,
        ephemeral_storage_request=ephemeral_storage_request,
        ephemeral_storage_limit=ephemeral_storage_limit,
        allow_public_network=allow_public_network,
        cluster_dns_ip=cluster_dns_ip,
    )


async def test_start_creates_a_root_pod_with_safety_controls(tmp_path: Path) -> None:
    cluster = FakeCluster()
    environment = _environment(
        tmp_path,
        cluster,
        pod_labels={"pool": "shared"},
        runtime_class_name="gvisor",
    )

    await environment.start(False)

    assert cluster.calls == [
        ("create_policy", _POLICY),
        ("create_pod", _POD),
        ("patch_policy", _POLICY),
    ]
    pod = cluster.pods[_POD]
    metadata = pod.metadata
    spec = pod.spec
    assert metadata is not None
    assert spec is not None
    assert metadata.labels == {"pool": "shared", SESSION_LABEL: _POD}
    assert metadata.annotations == {SESSION_ID_ANNOTATION: _SESSION}
    assert spec.automount_service_account_token is False
    assert spec.enable_service_links is False
    assert spec.runtime_class_name == "gvisor"
    assert spec.dns_policy == "None"
    assert spec.dns_config is not None
    assert spec.dns_config.nameservers == ["127.0.0.1"]
    [container] = spec.containers
    assert container.args == ["sleep", "infinity"]
    assert container.security_context is not None
    assert container.security_context.run_as_user == 0
    assert container.resources is not None
    assert container.resources.requests == {"cpu": "2", "memory": "4096Mi"}
    assert container.resources.limits == {"cpu": "2", "memory": "4096Mi"}


async def test_replaces_only_a_pod_owned_by_this_session(tmp_path: Path) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    cluster.pods[_POD] = environment.pod_manifest()

    await environment.start(False)

    assert cluster.calls[:3] == [
        ("create_policy", _POLICY),
        ("delete_pod", _POD),
        ("create_pod", _POD),
    ]


async def test_never_replaces_a_pod_owned_by_another_session(tmp_path: Path) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    existing = environment.pod_manifest()
    assert existing.metadata is not None
    existing.metadata.annotations = {SESSION_ID_ANNOTATION: "other-session"}
    cluster.pods[_POD] = existing

    with pytest.raises(RuntimeError, match="not this trial's to replace"):
        await environment.start(False)

    assert ("delete_pod", _POD) not in cluster.calls


def test_image_mount_is_read_only_and_subclass_can_rewrite_reference(
    tmp_path: Path,
) -> None:
    class RewritingEnvironment(KubernetesEnvironment):
        def resolve_image_mount_reference(self, reference: str) -> str:
            return f"mirror.example/{reference}"

    environment = RewritingEnvironment(
        environment_dir=tmp_path / "environment",
        environment_name="task",
        session_id=_SESSION,
        trial_paths=TrialPaths(tmp_path / "trial"),
        task_env_config=EnvironmentConfig(docker_image=_IMAGE),
        namespace=_NAMESPACE,
        network_policy=_policy(NetworkMode.ALLOWLIST),
        mounts=[
            ServiceVolumeConfig(
                type="image",
                source="registry.example/cli:1",
                target="/opt/cli",
                read_only=True,
            )
        ],
    )

    pod = environment.pod_manifest()
    spec = pod.spec
    assert spec is not None
    assert spec.volumes is not None
    [volume] = spec.volumes
    assert volume.image is not None
    assert volume.image.reference == "mirror.example/registry.example/cli:1"
    [container] = spec.containers
    assert container.volume_mounts is not None
    assert container.volume_mounts[0].read_only is True


async def test_no_network_policy_exists_before_the_pod_and_waits_for_enforcement(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster(egress_closes_after=2)
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)

    await environment.start(False)

    assert cluster.calls[:3] == [
        ("create_policy", _POLICY),
        ("create_pod", _POD),
        ("patch_policy", _POLICY),
    ]
    assert cluster.probes == 4
    assert cluster.policies[_POLICY]["spec"].get("egressDeny") == [
        {"toEntities": ["all"]}
    ]


async def test_policy_switch_replaces_in_place_and_waits_for_no_network(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster(egress_closes_after=1)
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    cluster.resource_versions[_POLICY] += 3

    await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))
    await environment.set_network_policy(_policy(NetworkMode.ALLOWLIST))

    assert "delete_policy" not in [operation for operation, _ in cluster.calls]
    assert cluster.calls[-1] == ("replace_policy", _POLICY)


def test_policy_cidrs_and_invalid_entries_are_handled_without_weakening(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path, FakeCluster())
    manifest = environment.network_policy_manifest(
        NetworkPolicy(
            network_mode=NetworkMode.ALLOWLIST,
            allowed_hosts=["2001:db8::7", "2001:db8:42::/48", "203.0.113.7"],
        )
    )
    assert manifest is not None
    assert {
        "toCIDR": ["2001:db8::7/128", "2001:db8:42::/48", "203.0.113.7/32"]
    } in manifest["spec"].get("egress", [])
    with pytest.raises(ValueError):
        environment.network_policy_manifest(
            NetworkPolicy(
                network_mode=NetworkMode.ALLOWLIST, allowed_hosts=["example.test"]
            )
        )


def test_public_network_is_refused_when_the_host_disables_it(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="public"):
        _environment(
            tmp_path,
            FakeCluster(),
            baseline=NetworkMode.PUBLIC,
            allow_public_network=False,
        )


def test_egress_probe_targets_a_real_dns_address() -> None:
    probe = egress_probe("172.20.0.10")
    assert "/dev/tcp/172.20.0.10/53" in probe
    assert "/etc/resolv.conf" not in probe


async def test_cni_failure_fails_closed(tmp_path: Path) -> None:
    environment = _environment(tmp_path, FakeCluster(egress_closes_after=1_000_000))
    await environment.start(False)
    environment.POLICY_CONVERGE_TIMEOUT_SEC = 0.0

    with pytest.raises(RuntimeError, match="still not closed"):
        await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))


async def test_exec_preserves_streams_cwd_env_and_timeout(tmp_path: Path) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)

    result = await environment.exec(
        'printf "%s|%s" "$GREETING" "$PWD"',
        cwd=str(tmp_path),
        env={"GREETING": "hello there"},
    )
    timed = await environment.exec("sleep 30", timeout_sec=1)

    assert (result.stdout, result.stderr, result.return_code) == (
        f"hello there|{tmp_path}",
        "",
        0,
    )
    assert timed.return_code == 124


async def test_unknown_uid_cannot_become_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    record = tmp_path / "su-record"
    record.touch()
    su = bin_dir / "su"
    su.write_text(
        "#!/bin/sh\nprintf '[%s]\\n' \"$1\" >> "
        + str(record)
        + '\nshift; exec "$3" -c "$5"\n',
        encoding="utf-8",
    )
    su.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)

    result = await environment.exec("true", user=2**31 - 7)

    assert result.return_code != 0
    assert record.read_text(encoding="utf-8") == ""


async def test_file_and_directory_transfer_are_byte_exact(tmp_path: Path) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)
    source = tmp_path / "source.bin"
    source.write_bytes(bytes(range(256)))
    source.chmod(0o750)
    pod_root = tmp_path / "pod"

    await environment.upload_file(source, str(pod_root / "deep" / "source.bin"))
    await environment.download_file(
        str(pod_root / "deep" / "source.bin"), tmp_path / "back.bin"
    )
    directory = tmp_path / "directory"
    (directory / "nested").mkdir(parents=True)
    (directory / "nested" / "data.bin").write_bytes(b"\x00\xff")
    await environment.upload_dir(directory, str(pod_root / "directory"))
    await environment.download_dir(str(pod_root / "directory"), tmp_path / "back")

    assert (tmp_path / "back.bin").read_bytes() == bytes(range(256))
    assert (pod_root / "deep" / "source.bin").stat().st_mode & 0o777 == 0o750
    assert (tmp_path / "back" / "nested" / "data.bin").read_bytes() == b"\x00\xff"


async def test_missing_download_paths_keep_their_distinct_errors(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)

    with pytest.raises(FileNotFoundError):
        await environment.download_file(str(tmp_path / "missing"), tmp_path / "out")
    with pytest.raises(IsADirectoryError):
        await environment.download_file(str(tmp_path), tmp_path / "out")
    with pytest.raises(FileNotFoundError):
        await environment.download_dir(str(tmp_path / "missing"), tmp_path / "out")


async def test_stop_deletes_pod_before_policy_and_respects_delete_false(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    await environment.stop(False)
    assert _POD in cluster.pods

    await environment.stop(True)

    assert cluster.calls[-2:] == [("delete_pod", _POD), ("delete_policy", _POLICY)]
    [deletion] = cluster.pod_deletions
    assert deletion is not None
    assert deletion.propagation_policy == "Orphan"


async def test_start_errors_for_image_pull_failure_and_pod_gone(tmp_path: Path) -> None:
    failing = _environment(tmp_path, FakeCluster(waiting_reason="ImagePullBackOff"))
    with pytest.raises(RuntimeError, match="ImagePullBackOff"):
        await failing.start(False)

    cluster = FakeCluster()
    environment = _environment(tmp_path / "gone", cluster)
    await environment.start(False)
    cluster.pods.clear()
    with pytest.raises(KubernetesPodGoneError):
        await environment.exec("true")


def test_rejects_compose_tasks_missing_images_and_reserved_labels(
    tmp_path: Path,
) -> None:
    compose = tmp_path / "task" / "environment" / "docker-compose.yaml"
    compose.parent.mkdir(parents=True)
    compose.write_text("services: {}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="compose"):
        _environment(tmp_path, FakeCluster())
    with pytest.raises(ValueError, match="docker_image"):
        _environment(tmp_path / "image", FakeCluster(), docker_image=None)
    with pytest.raises(ValueError, match=SESSION_LABEL):
        _environment(tmp_path / "label", FakeCluster(), pod_labels={SESSION_LABEL: "x"})


def test_preflight_accepts_a_kubeconfig_and_refuses_no_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from harbor_kubernetes import environment as module

    kubeconfig = tmp_path / "kubeconfig"
    monkeypatch.setattr(
        module, "SERVICE_TOKEN_FILENAME", str(tmp_path / "missing-token")
    )
    monkeypatch.setattr(module, "KUBE_CONFIG_DEFAULT_LOCATION", str(kubeconfig))
    with pytest.raises(SystemExit, match="found neither"):
        KubernetesEnvironment.preflight()
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    KubernetesEnvironment.preflight()


@pytest.mark.parametrize("kind", ["file", "directory"])
async def test_uploads_create_private_modes_under_a_permissive_umask(
    tmp_path: Path, kind: str
) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_text("content", encoding="utf-8")
    pod = tmp_path / "pod"
    previous = os.umask(0)
    try:
        if kind == "file":
            await environment.upload_file(source / "data", str(pod / "deep" / "data"))
        else:
            await environment.upload_dir(source, str(pod / "deep"))
    finally:
        _ = os.umask(previous)
    assert stat.S_IMODE((pod / "deep").stat().st_mode) == 0o755


def test_in_cluster_dns_target_skips_loopback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from harbor_kubernetes import environment as module

    token = tmp_path / "token"
    token.write_text("token", encoding="utf-8")
    resolver = tmp_path / "resolv.conf"
    resolver.write_text(
        "nameserver 127.0.0.1\nnameserver 172.20.0.10\n", encoding="utf-8"
    )
    monkeypatch.setattr(module, "SERVICE_TOKEN_FILENAME", str(token))
    monkeypatch.setattr(module, "RUNNER_RESOLV_CONF", resolver)
    environment = _environment(tmp_path, FakeCluster())

    assert KubernetesEnvironment.cluster_dns_probe_target(environment) == "172.20.0.10"
    assert RUNNER_RESOLV_CONF.name == "resolv.conf"
