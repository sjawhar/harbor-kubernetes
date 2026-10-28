"""The Kubernetes environment's pod lifecycle, exec and transfer, and network-policy behavior."""

from __future__ import annotations

import os
import pwd
import stat
import time
from pathlib import Path
from typing import override

import pytest
from harbor.models.task.config import NetworkMode, NetworkPolicy
from harbor.models.trial.config import ServiceVolumeConfig
from kubernetes import client as k8s_client
from test_environment import (
    _IMAGE,
    _NAMESPACE,
    _POD,
    _POD_UID,
    _POLICY,
    _SESSION,
    FakeCluster,
    _environment,
    _policy,
)

from harbor_kubernetes.environment import KubernetesEnvironment, KubernetesPodGoneError
from harbor_kubernetes.network_policy import (
    PUBLIC_NETWORK_REFUSAL,
    SESSION_ID_ANNOTATION,
    SESSION_LABEL,
)


async def test_start_boots_one_root_pod_carrying_the_labels_and_runtime_class(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    workdir = tmp_path / "pod-workdir"
    environment = _environment(
        tmp_path,
        cluster,
        pod_labels={"sandbox-pool": "generic"},
        runtime_class_name="gvisor",
        workdir=workdir,
        ephemeral_storage_request="8Gi",
        ephemeral_storage_limit="16Gi",
    )
    staged = environment.environment_dir / ".harbor-kubernetes" / "setup.sh"
    staged.parent.mkdir()
    staged.write_text("echo setup\n", encoding="utf-8")

    await environment.start(False)

    assert cluster.calls == [
        ("create_policy", _POLICY),
        ("create_pod", _POD),
        ("patch_policy", _POLICY),
    ]
    policy = cluster.policies[_POLICY]
    assert policy["metadata"]["labels"] == {
        "sandbox-pool": "generic",
        SESSION_LABEL: _POD,
    }
    assert [sorted(rule) for rule in policy["spec"].get("egress", [])] == [
        ["toEndpoints", "toPorts"],
        ["toEndpoints"],
    ]
    assert policy["metadata"].get("ownerReferences") == [
        {"apiVersion": "v1", "kind": "Pod", "name": _POD, "uid": _POD_UID}
    ]
    pod = cluster.pods[_POD]
    metadata = pod.metadata
    spec = pod.spec
    assert metadata is not None
    assert spec is not None
    assert metadata.namespace == _NAMESPACE
    assert metadata.labels == {"sandbox-pool": "generic", SESSION_LABEL: _POD}
    assert spec.runtime_class_name == "gvisor"
    assert spec.automount_service_account_token is False
    assert spec.restart_policy == "Never"
    assert spec.dns_policy == "None"
    assert spec.dns_config is not None
    assert spec.dns_config.nameservers == ["127.0.0.1"]
    [container] = spec.containers
    assert container.image == _IMAGE
    assert container.args == ["sleep", "infinity"]
    assert container.security_context is not None
    assert container.security_context.run_as_user == 0
    assert container.resources is not None
    assert container.resources.requests == {
        "cpu": "2",
        "memory": "4096Mi",
        "ephemeral-storage": "8Gi",
    }
    assert container.resources.limits == {
        "cpu": "2",
        "memory": "4096Mi",
        "ephemeral-storage": "16Gi",
    }
    assert spec.volumes is None
    assert container.volume_mounts is None
    assert (workdir / ".harbor-kubernetes" / "setup.sh").read_text(
        encoding="utf-8"
    ) == "echo setup\n"


async def test_a_pod_left_by_an_earlier_attempt_at_the_same_trial_is_replaced(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    cluster.pods[_POD] = environment.pod_manifest()

    await environment.start(False)

    assert cluster.calls == [
        ("create_policy", _POLICY),
        ("delete_pod", _POD),
        ("create_pod", _POD),
        ("patch_policy", _POLICY),
    ]
    metadata = cluster.pods[_POD].metadata
    assert metadata is not None
    assert metadata.annotations == {SESSION_ID_ANNOTATION: _SESSION}


async def test_a_policy_left_by_an_earlier_attempt_is_replaced_and_deleted_at_stop(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    leftover = environment.network_policy_manifest(environment.network_policy)
    assert leftover is not None
    cluster.policies[_POLICY] = leftover
    cluster.resource_versions[_POLICY] = 4

    await environment.start(False)
    await environment.stop(delete=True)

    assert cluster.calls[0] == ("replace_policy", _POLICY)
    assert cluster.calls[-1] == ("delete_policy", _POLICY)
    assert cluster.policies == {}


async def test_a_pod_another_session_folds_onto_is_never_deleted(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    other = environment.pod_manifest()
    other_session = "task-7__aB3De__env"
    assert other.metadata is not None
    other.metadata.annotations = {SESSION_ID_ANNOTATION: other_session}
    cluster.pods[_POD] = other

    with pytest.raises(RuntimeError, match="not this trial's to replace") as raised:
        await environment.start(False)

    assert other_session in str(raised.value)
    assert ("delete_pod", _POD) not in cluster.calls
    assert cluster.pods[_POD] is other


def test_an_image_mount_is_the_pods_read_only_image_volume(tmp_path: Path) -> None:
    mounts: list[ServiceVolumeConfig] = [
        {
            "type": "bind",
            "source": str(tmp_path / "agent"),
            "target": "/logs/agent",
        },
        {
            "type": "image",
            "source": "registry.example/cli:1",
            "target": "/opt/agent-cli",
            "read_only": True,
        },
    ]
    environment = _environment(tmp_path, FakeCluster(), mounts=mounts)

    pod = environment.pod_manifest()
    assert pod.spec is not None
    assert pod.spec.volumes is not None
    [volume] = pod.spec.volumes
    assert volume.image is not None
    assert {
        "reference": volume.image.reference,
        "pullPolicy": volume.image.pull_policy,
    } == {"reference": "registry.example/cli:1", "pullPolicy": "IfNotPresent"}
    [container] = pod.spec.containers
    assert container.volume_mounts is not None
    [mount] = container.volume_mounts
    assert (mount.name, mount.mount_path, mount.read_only) == (
        volume.name,
        "/opt/agent-cli",
        True,
    )


async def test_a_no_network_baseline_is_in_force_before_the_pod_exists(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster(egress_closes_after=2)
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)

    await environment.start(False)

    assert cluster.calls == [
        ("create_policy", _POLICY),
        ("create_pod", _POD),
        ("patch_policy", _POLICY),
    ]
    policy = cluster.policies[_POLICY]
    assert policy["apiVersion"] == "cilium.io/v2"
    assert policy["kind"] == "CiliumNetworkPolicy"
    assert policy["spec"]["endpointSelector"] == {
        "matchLabels": {
            "io.kubernetes.pod.namespace": _NAMESPACE,
            SESSION_LABEL: _POD,
        }
    }
    assert policy["spec"].get("egressDeny") == [{"toEntities": ["all"]}]
    assert "egress" not in policy["spec"]
    assert cluster.probes == 4


async def test_switching_to_no_network_returns_once_the_cni_has_closed_egress(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster(egress_closes_after=3)
    environment = _environment(tmp_path, cluster)
    await environment.start(False)

    await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))

    assert cluster.calls[-1] == ("replace_policy", _POLICY)
    assert cluster.policies[_POLICY]["spec"].get("egressDeny") == [
        {"toEntities": ["all"]}
    ]
    assert cluster.probes == 5

    await environment.set_network_policy(_policy(NetworkMode.ALLOWLIST))

    assert cluster.calls[-1] == ("replace_policy", _POLICY)
    assert "delete_policy" not in [operation for operation, _ in cluster.calls]
    baseline = cluster.policies[_POLICY]["spec"]
    assert "egressDeny" not in baseline
    assert [sorted(rule) for rule in baseline.get("egress", [])] == [
        ["toEndpoints", "toPorts"],
        ["toEndpoints"],
    ]


def test_a_trial_that_asks_for_the_world_is_handled_by_the_public_network_setting(
    tmp_path: Path,
) -> None:
    for baseline, phase in (
        (NetworkMode.PUBLIC, NetworkMode.NO_NETWORK),
        (NetworkMode.ALLOWLIST, NetworkMode.PUBLIC),
    ):
        environment = _environment(
            tmp_path / f"allowed-{baseline.value}-{phase.value}",
            FakeCluster(),
            baseline=baseline,
            phase=phase,
        )
        assert environment.allow_public_network is True

    for baseline, phase in (
        (NetworkMode.PUBLIC, NetworkMode.NO_NETWORK),
        (NetworkMode.ALLOWLIST, NetworkMode.PUBLIC),
    ):
        cluster = FakeCluster()
        with pytest.raises(ValueError) as refused:
            _environment(
                tmp_path / f"refused-{baseline.value}-{phase.value}",
                cluster,
                baseline=baseline,
                phase=phase,
                allow_public_network=False,
            )
        assert str(refused.value) == PUBLIC_NETWORK_REFUSAL
        assert cluster.calls == []


async def test_switching_to_public_deletes_the_restricted_policy_and_leaves_the_pod_running(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(
        tmp_path,
        cluster,
        baseline=NetworkMode.NO_NETWORK,
        phase=NetworkMode.PUBLIC,
    )
    await environment.start(False)
    assert environment.network_policy_manifest(_policy(NetworkMode.PUBLIC)) is None

    await environment.set_network_policy(_policy(NetworkMode.PUBLIC))

    assert cluster.calls[-1] == ("delete_policy", _POLICY)
    assert _POLICY not in cluster.policies
    assert _POD in cluster.pods
    assert environment.network_policy.network_mode == NetworkMode.PUBLIC


def test_the_trial_pod_links_no_service_into_its_environment(tmp_path: Path) -> None:
    pod = _environment(tmp_path, FakeCluster()).pod_manifest()
    assert pod.spec is not None
    assert pod.spec.enable_service_links is False


async def test_a_policy_another_writer_updated_is_still_replaced_in_place(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    cluster.resource_versions[_POLICY] += 7

    await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))

    assert cluster.calls[-1] == ("replace_policy", _POLICY)
    assert "delete_policy" not in [operation for operation, _ in cluster.calls]
    assert cluster.policies[_POLICY]["spec"].get("egressDeny") == [
        {"toEntities": ["all"]}
    ]


@pytest.mark.parametrize(
    "baseline",
    [NetworkMode.ALLOWLIST, NetworkMode.NO_NETWORK],
    ids=["allowlist-baseline", "no-network-baseline"],
)
async def test_ciliums_status_write_after_the_policy_is_created_never_fails_the_start(
    tmp_path: Path, baseline: NetworkMode
) -> None:
    """A Cilium status write after policy creation must not fail trial startup."""
    cluster = FakeCluster(cilium_reports_status=True, egress_closes_after=1)
    environment = _environment(tmp_path, cluster, baseline=baseline)
    created = environment.network_policy_manifest(environment.network_policy)
    assert created is not None

    await environment.start(False)

    assert cluster.status_writes == 1
    policy = cluster.policies[_POLICY]
    assert policy["metadata"].get("ownerReferences") == [
        {"apiVersion": "v1", "kind": "Pod", "name": _POD, "uid": _POD_UID}
    ]
    assert policy["spec"] == created["spec"]
    assert policy["metadata"]["labels"] == created["metadata"]["labels"]
    assert cluster.calls == [
        ("create_policy", _POLICY),
        ("create_pod", _POD),
        ("patch_policy", _POLICY),
    ]


async def test_a_policy_gone_before_its_owner_is_named_is_written_whole_again(
    tmp_path: Path,
) -> None:
    """A policy deleted after the first apply is written again with its pod owner."""

    class PolicyLostOnPodCreate(FakeCluster):
        @override
        def create_namespaced_pod(
            self, namespace: str, body: k8s_client.V1Pod
        ) -> k8s_client.V1Pod:
            created = super().create_namespaced_pod(namespace=namespace, body=body)
            del self.policies[_POLICY]
            return created

    cluster = PolicyLostOnPodCreate()
    environment = _environment(tmp_path, cluster)

    await environment.start(False)

    assert cluster.calls == [
        ("create_policy", _POLICY),
        ("create_pod", _POD),
        ("create_policy", _POLICY),
    ]
    policy = cluster.policies[_POLICY]
    assert policy["metadata"].get("ownerReferences") == [
        {"apiVersion": "v1", "kind": "Pod", "name": _POD, "uid": _POD_UID}
    ]
    created = environment.network_policy_manifest(environment.network_policy)
    assert created is not None
    assert policy["spec"] == created["spec"]


async def test_a_phase_switch_racing_ciliums_status_write_replaces_at_the_fresh_version(
    tmp_path: Path,
) -> None:
    """A phase switch retries a 409 at a freshly read policy resource version."""
    cluster = FakeCluster(cilium_reports_status=True)
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    cluster.status_writes_pending[_POLICY] = 2

    await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))

    assert cluster.calls[3:] == [
        ("replace_policy_conflict", _POLICY),
        ("replace_policy_conflict", _POLICY),
        ("replace_policy", _POLICY),
    ]
    policy = cluster.policies[_POLICY]
    desired = environment.network_policy_manifest(_policy(NetworkMode.NO_NETWORK))
    assert desired is not None
    assert policy["spec"] == desired["spec"]
    assert policy["metadata"].get("ownerReferences") == [
        {"apiVersion": "v1", "kind": "Pod", "name": _POD, "uid": _POD_UID}
    ]


async def test_a_replace_refused_on_every_attempt_fails_the_phase_switch(
    tmp_path: Path,
) -> None:
    """A phase switch stops after its bounded number of conflicting replaces."""
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    before = cluster.policies[_POLICY]
    attempts = environment.POLICY_REPLACE_ATTEMPTS
    cluster.status_writes_pending[_POLICY] = attempts

    with pytest.raises(k8s_client.ApiException) as refused:
        await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))

    assert refused.value.status == 409
    assert cluster.calls[3:] == [("replace_policy_conflict", _POLICY)] * attempts
    assert cluster.policies[_POLICY] == before


def test_an_allowlist_reaches_ipv6_addresses_and_ranges_as_cidrs(
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


@pytest.mark.parametrize("entry", ["api.example.com", "198.51.100.1/24"])
async def test_an_allowlist_entry_a_cidr_policy_cannot_hold_is_refused(
    tmp_path: Path, entry: str
) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)
    await environment.start(False)
    before = cluster.policies[_POLICY]
    calls = list(cluster.calls)
    allowlist = NetworkPolicy(
        network_mode=NetworkMode.ALLOWLIST, allowed_hosts=["203.0.113.7", entry]
    )

    with pytest.raises(ValueError):
        environment.network_policy_manifest(allowlist)
    with pytest.raises(ValueError):
        await environment.set_network_policy(allowlist)

    assert cluster.policies[_POLICY] == before
    assert cluster.calls == calls


async def test_a_no_network_policy_the_cni_never_enforces_fails_closed(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster(egress_closes_after=1_000_000)
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    environment.POLICY_CONVERGE_TIMEOUT_SEC = 0.0

    with pytest.raises(RuntimeError, match="still not closed"):
        await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))


def test_the_egress_probe_targets_an_explicit_cluster_dns_ip_not_the_pods_resolver() -> (
    None
):
    from harbor_kubernetes.environment import egress_probe

    probe = egress_probe("172.20.0.10")
    assert "/dev/tcp/172.20.0.10/53" in probe
    assert "/etc/resolv.conf" not in probe


def test_the_convergence_target_is_the_runners_non_loopback_nameserver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from harbor_kubernetes import environment as module

    environment = _environment(tmp_path, FakeCluster())
    token = tmp_path / "token"
    token.write_text("in-cluster", encoding="utf-8")
    monkeypatch.setattr(module, "SERVICE_TOKEN_FILENAME", str(token))
    resolver = tmp_path / "resolv.conf"
    resolver.write_text(
        "nameserver 127.0.0.1\nnameserver 172.20.0.10\n", encoding="utf-8"
    )
    monkeypatch.setattr(module, "RUNNER_RESOLV_CONF", resolver)
    assert KubernetesEnvironment.cluster_dns_probe_target(environment) == "172.20.0.10"

    resolver.write_text("nameserver 127.0.0.53\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="no non-loopback nameserver"):
        KubernetesEnvironment.cluster_dns_probe_target(environment)

    monkeypatch.setattr(module, "SERVICE_TOKEN_FILENAME", str(tmp_path / "absent"))
    environment._connected_apis = None
    with pytest.raises(RuntimeError, match="cluster_dns_ip"):
        KubernetesEnvironment.cluster_dns_probe_target(environment)


async def test_exec_keeps_stdout_stderr_and_status_apart_with_cwd_and_env(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)

    failed = await environment.exec("printf out; printf err >&2; exit 3")
    located = await environment.exec(
        'printf "%s|%s" "$GREETING" "$PWD"',
        cwd=str(tmp_path),
        env={"GREETING": "hello there"},
    )

    assert (failed.stdout, failed.stderr, failed.return_code) == ("out", "err", 3)
    assert located.stdout == f"hello there|{tmp_path}"
    assert located.return_code == 0


async def test_a_timed_out_exec_is_killed_in_the_pod_and_reports_124(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)

    started = time.monotonic()
    result = await environment.exec("sleep 30", timeout_sec=1)

    assert result.return_code == 124
    assert time.monotonic() - started < 15


def _shadow_su(directory: Path, record: Path) -> None:
    directory.mkdir()
    su = directory / "su"
    su.write_text(
        "#!/bin/sh\n"
        + f'printf "[%s]\\n" "$1" >> {record}\n'
        + 'shift; shell=$2; shift 2; exec "$shell" -c "$2"\n',
        encoding="utf-8",
    )
    su.chmod(0o755)


async def test_a_uid_with_no_account_never_runs_the_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = tmp_path / "su-names"
    record.touch()
    _shadow_su(tmp_path / "bin", record)
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}{os.pathsep}{os.environ['PATH']}")
    marker = tmp_path / "ran"
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)

    unassigned = await environment.exec(f"touch {marker}", user=2**31 - 7)
    assigned = await environment.exec("printf ran", user=os.getuid())
    with pytest.raises(ValueError):
        await environment.exec(f"touch {marker}", user="")

    assert unassigned.return_code != 0
    assert str(2**31 - 7) in (unassigned.stderr or "")
    assert not marker.exists()
    assert (assigned.stdout, assigned.return_code) == ("ran", 0)
    assert (
        record.read_text(encoding="utf-8") == f"[{pwd.getpwuid(os.getuid()).pw_name}]\n"
    )


async def test_a_file_round_trips_byte_exact_with_its_mode(tmp_path: Path) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)
    payload = bytes(range(256)) * 64
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    source.chmod(0o750)
    in_pod = tmp_path / "pod" / "deep" / "path" / "file.bin"

    await environment.upload_file(source, str(in_pod))
    await environment.download_file(str(in_pod), tmp_path / "back" / "file.bin")

    assert (tmp_path / "back" / "file.bin").read_bytes() == payload
    assert in_pod.stat().st_mode & 0o777 == 0o750


async def test_downloading_a_missing_file_raises_file_not_found(tmp_path: Path) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)
    target = tmp_path / "x"

    with pytest.raises(FileNotFoundError):
        await environment.download_file(str(tmp_path / "absent"), target)
    with pytest.raises(IsADirectoryError):
        await environment.download_file(str(tmp_path), tmp_path / "y")
    assert not target.exists()


async def test_a_directory_round_trips_and_a_missing_one_raises(tmp_path: Path) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)
    source = tmp_path / "logs"
    (source / "nested").mkdir(parents=True)
    (source / "top.txt").write_text("top", encoding="utf-8")
    (source / "nested" / "data.bin").write_bytes(b"\x00\xff\x10")
    in_pod = tmp_path / "pod" / "logs"

    await environment.upload_dir(source, str(in_pod))
    await environment.download_dir(str(in_pod), tmp_path / "back")

    assert (tmp_path / "back" / "top.txt").read_text(encoding="utf-8") == "top"
    assert (tmp_path / "back" / "nested" / "data.bin").read_bytes() == b"\x00\xff\x10"
    with pytest.raises(FileNotFoundError):
        await environment.download_dir(str(tmp_path / "absent"), tmp_path / "z")


async def test_an_upload_creates_its_directories_0755_whatever_the_execs_umask(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path, FakeCluster())
    await environment.start(False)
    source = tmp_path / "tests"
    (source / "verdict").mkdir(parents=True)
    (source / "verdict").chmod(0o755)
    script = source / "test.sh"
    script.write_text("#!/bin/sh\n", encoding="utf-8")
    script.chmod(0o755)
    single = tmp_path / "single.json"
    single.write_text("{}", encoding="utf-8")
    single.chmod(0o644)
    pod = tmp_path / "pod"

    previous = os.umask(0)
    try:
        await environment.upload_dir(source, str(pod / "tests"))
        await environment.upload_file(single, str(pod / "deep" / "single.json"))
    finally:
        _ = os.umask(previous)

    modes = {
        path.relative_to(pod).as_posix(): stat.S_IMODE(path.stat().st_mode)
        for path in (pod, *sorted(pod.rglob("*")))
    }
    assert modes == {
        ".": 0o755,
        "deep": 0o755,
        "deep/single.json": 0o644,
        "tests": 0o755,
        "tests/test.sh": 0o755,
        "tests/verdict": 0o755,
    }


async def test_stop_deletes_the_pod_before_lifting_its_network_policy(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)
    await environment.start(False)

    await environment.stop(delete=True)

    assert cluster.calls[-2:] == [("delete_pod", _POD), ("delete_policy", _POLICY)]
    assert cluster.pods == {}
    assert cluster.policies == {}
    [deletion] = cluster.pod_deletions
    assert deletion is not None
    assert deletion.propagation_policy == "Orphan"


async def test_stop_without_delete_leaves_the_pod_running(tmp_path: Path) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    await environment.start(False)

    await environment.stop(delete=False)

    assert _POD in cluster.pods


async def test_an_image_pull_failure_fails_start_without_waiting_out_the_timeout(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path, FakeCluster(waiting_reason="ImagePullBackOff"))

    with pytest.raises(RuntimeError, match="ImagePullBackOff"):
        await environment.start(False)


async def test_exec_into_a_deleted_pod_raises_pod_gone(tmp_path: Path) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    cluster.pods.clear()

    with pytest.raises(KubernetesPodGoneError):
        await environment.exec("true")


def test_a_compose_task_is_refused(tmp_path: Path) -> None:
    compose = tmp_path / "task" / "environment" / "docker-compose.yaml"
    compose.parent.mkdir(parents=True)
    compose.write_text("services: {}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="compose"):
        _environment(tmp_path, FakeCluster())


def test_a_task_without_a_prebuilt_image_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="docker_image"):
        _environment(tmp_path, FakeCluster(), docker_image=None)


def test_pod_labels_may_not_take_the_providers_session_label(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=SESSION_LABEL):
        _environment(tmp_path, FakeCluster(), pod_labels={SESSION_LABEL: "x"})


def test_preflight_accepts_a_kubeconfig_and_refuses_no_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from harbor_kubernetes import environment as module

    kubeconfig = tmp_path / "kubeconfig"
    monkeypatch.setattr(module, "SERVICE_TOKEN_FILENAME", str(tmp_path / "no"))
    monkeypatch.setattr(module, "KUBE_CONFIG_DEFAULT_LOCATION", str(kubeconfig))
    with pytest.raises(SystemExit, match="found neither"):
        KubernetesEnvironment.preflight()

    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    KubernetesEnvironment.preflight()


class _PodDeletionFails(FakeCluster):
    @override
    def delete_namespaced_pod(
        self,
        name: str,
        namespace: str,
        *,
        body: k8s_client.V1DeleteOptions | None = None,
    ) -> object:
        del name, namespace, body
        raise k8s_client.ApiException(status=500, reason="InternalError")


async def test_a_failed_pod_delete_keeps_its_policy_and_surfaces_the_error(
    tmp_path: Path,
) -> None:
    cluster = _PodDeletionFails()
    environment = _environment(tmp_path, cluster)
    await environment.start(False)

    with pytest.raises(k8s_client.ApiException) as raised:
        await environment.stop(delete=True)

    assert raised.value.status == 500
    assert _POD in cluster.pods
    assert _POLICY in cluster.policies


async def test_downloading_from_a_deleted_pod_raises_pod_gone(tmp_path: Path) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    await environment.start(False)
    cluster.pods.clear()

    with pytest.raises(KubernetesPodGoneError):
        await environment.download_file("/etc/hostname", tmp_path / "hostname")


@pytest.mark.parametrize(
    ("annotations", "expected"),
    [({"example.com/owner": "team"}, {"example.com/owner": "team"}), (None, None)],
)
def test_the_trial_pod_carries_caller_annotations_beside_its_session(
    tmp_path: Path,
    annotations: dict[str, str] | None,
    expected: dict[str, str] | None,
) -> None:
    pod = _environment(tmp_path, FakeCluster(), annotations=annotations).pod_manifest()
    assert pod.metadata is not None
    assert pod.metadata.annotations == {
        **(expected or {}),
        SESSION_ID_ANNOTATION: _SESSION,
    }
    assert pod.metadata.labels == {SESSION_LABEL: _POD}


async def test_public_baseline_then_no_network_phase_creates_a_policy_and_waits_closed(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    environment = _environment(
        tmp_path,
        cluster,
        baseline=NetworkMode.PUBLIC,
        phase=NetworkMode.NO_NETWORK,
    )
    await environment.start(False)
    assert cluster.calls == [("create_pod", _POD)]

    await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))

    assert cluster.calls[-1] == ("create_policy", _POLICY)
    assert cluster.policies[_POLICY]["spec"].get("egressDeny") == [
        {"toEntities": ["all"]}
    ]
    assert cluster.probes == 2


async def test_public_transitions_are_refused_when_public_network_is_disabled(
    tmp_path: Path,
) -> None:
    for baseline, phase in (
        (NetworkMode.PUBLIC, NetworkMode.NO_NETWORK),
        (NetworkMode.NO_NETWORK, NetworkMode.PUBLIC),
    ):
        cluster = FakeCluster()
        with pytest.raises(ValueError, match="network_mode='public'"):
            _environment(
                tmp_path / f"{baseline.value}-{phase.value}",
                cluster,
                baseline=baseline,
                phase=phase,
                allow_public_network=False,
            )
        assert cluster.calls == []
        assert cluster.pods == {}
        assert cluster.policies == {}


async def test_missing_dns_target_refuses_before_creating_the_pod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster = FakeCluster()
    cluster.service_dns_ip = None
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)
    monkeypatch.setattr(
        type(environment),
        "cluster_dns_probe_target",
        KubernetesEnvironment.cluster_dns_probe_target,
    )

    with pytest.raises(RuntimeError, match="cluster_dns_ip"):
        await environment.start(False)

    assert ("create_pod", _POD) not in cluster.calls
    assert cluster.pods == {}
