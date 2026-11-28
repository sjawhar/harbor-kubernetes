"""The label network backend: no-network by a pod label the cluster's NetworkPolicy enforces."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from harbor.models.task.config import NetworkMode, NetworkPolicy
from kubernetes import client as k8s_client

from harbor_kubernetes.environment import KubernetesEnvironment
from harbor_kubernetes.network_policy import (
    NO_NETWORK_LABEL,
    SESSION_LABEL,
    NetworkPolicyBackend,
)
from harbor_kubernetes.testing import POD as _POD
from harbor_kubernetes.testing import FakeCluster, fake_environment
from harbor_kubernetes.testing import network_policy as _policy

_CUSTOM_OBJECT_METHODS = (
    "create_namespaced_custom_object",
    "get_namespaced_custom_object",
    "patch_namespaced_custom_object",
    "replace_namespaced_custom_object",
    "delete_namespaced_custom_object",
)


class _DirectApplyEnvironment(KubernetesEnvironment):
    """Reaches the policy hook the way a caller that skips Harbor's checks would."""

    async def apply(self, network_policy: NetworkPolicy) -> None:
        await self._apply_network_policy(network_policy)


def _refuse_custom_objects(
    cluster: FakeCluster, monkeypatch: pytest.MonkeyPatch
) -> list[str]:
    requests: list[str] = []

    def refuse(**kwargs: object) -> object:
        requests.append(str(kwargs.get("name", kwargs.get("plural"))))
        raise k8s_client.ApiException(status=404, reason="NotFound")

    for method in _CUSTOM_OBJECT_METHODS:
        monkeypatch.setattr(cluster, method, refuse)
    return requests


def _environment(
    tmp_path: Path,
    cluster: FakeCluster,
    *,
    baseline: NetworkMode = NetworkMode.NO_NETWORK,
    phase: NetworkMode = NetworkMode.NO_NETWORK,
    pod_labels: dict[str, str] | None = None,
    network_policy_backend: NetworkPolicyBackend = "label",
    environment_class: type[KubernetesEnvironment] = KubernetesEnvironment,
) -> KubernetesEnvironment:
    return fake_environment(
        environment_class,
        tmp_path,
        cluster,
        baseline=baseline,
        phase=phase,
        pod_labels=pod_labels,
        network_policy_backend=network_policy_backend,
    )


def _pod_labels(cluster: FakeCluster) -> dict[str, str] | None:
    metadata = cluster.pods[_POD].metadata
    assert metadata is not None
    return metadata.labels


async def test_a_no_network_trial_is_a_labelled_pod_and_no_policy_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster = FakeCluster(egress_closes_after=2)
    requests = _refuse_custom_objects(cluster, monkeypatch)
    environment = _environment(tmp_path, cluster, pod_labels={"pool": "grade"})

    await environment.start(False)

    assert cluster.calls == [("create_pod", _POD)]
    assert _pod_labels(cluster) == {
        "pool": "grade",
        SESSION_LABEL: _POD,
        NO_NETWORK_LABEL: "true",
    }
    assert cluster.probes == 4

    await environment.stop(delete=True)

    assert cluster.calls == [("create_pod", _POD), ("delete_pod", _POD)]
    assert requests == []
    assert cluster.policies == {}


async def test_a_public_trial_is_an_unlabelled_pod_and_no_policy_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster = FakeCluster()
    requests = _refuse_custom_objects(cluster, monkeypatch)
    environment = _environment(
        tmp_path, cluster, baseline=NetworkMode.PUBLIC, phase=NetworkMode.PUBLIC
    )

    await environment.start(False)
    assert _pod_labels(cluster) == {SESSION_LABEL: _POD}
    await environment.stop(delete=True)

    assert cluster.calls == [("create_pod", _POD), ("delete_pod", _POD)]
    assert requests == []
    assert cluster.probes == 0


async def test_a_cluster_without_the_label_policy_fails_the_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster = FakeCluster(no_network_label_policy=False)
    requests = _refuse_custom_objects(cluster, monkeypatch)
    environment = _environment(tmp_path, cluster)
    environment.POLICY_CONVERGE_TIMEOUT_SEC = 0.0

    with pytest.raises(RuntimeError, match="still not closed") as refused:
        await environment.start(False)

    assert f"{NO_NETWORK_LABEL}=true" in str(refused.value)
    assert "'OPEN'" in str(refused.value)
    assert requests == []


async def test_the_dns_target_is_resolved_before_the_labelled_pod_is_created(
    tmp_path: Path,
) -> None:
    cluster = FakeCluster()
    cluster.service_dns_ip = None
    environment = _environment(tmp_path, cluster)

    with pytest.raises(RuntimeError, match="cluster_dns_ip"):
        await environment.start(False)

    assert cluster.pods == {}


def test_the_capabilities_follow_the_backend(tmp_path: Path) -> None:
    label = _environment(tmp_path / "label", FakeCluster()).capabilities
    assert label.disable_internet is True
    assert label.dynamic_network_policy is False
    assert not any(
        (
            label.network_allowlist,
            label.network_allowlist_ipv4_addresses,
            label.network_allowlist_ipv6_addresses,
            label.network_allowlist_ipv4_cidrs,
            label.network_allowlist_ipv6_cidrs,
        )
    )

    cilium = _environment(
        tmp_path / "cilium", FakeCluster(), network_policy_backend="cilium"
    ).capabilities
    assert cilium.disable_internet is True
    assert cilium.dynamic_network_policy is True
    assert all(
        (
            cilium.network_allowlist,
            cilium.network_allowlist_ipv4_addresses,
            cilium.network_allowlist_ipv6_addresses,
            cilium.network_allowlist_ipv4_cidrs,
            cilium.network_allowlist_ipv6_cidrs,
        )
    )


@pytest.mark.parametrize(
    ("baseline", "phase"),
    [
        (NetworkMode.ALLOWLIST, NetworkMode.ALLOWLIST),
        (NetworkMode.NO_NETWORK, NetworkMode.ALLOWLIST),
    ],
)
def test_an_allowlist_is_refused_before_the_trial_starts(
    tmp_path: Path, baseline: NetworkMode, phase: NetworkMode
) -> None:
    cluster = FakeCluster()

    with pytest.raises(ValueError, match="network_policy_backend='label'"):
        _environment(tmp_path, cluster, baseline=baseline, phase=phase)

    assert cluster.calls == []


async def test_a_mode_switch_after_start_is_refused_and_the_pod_keeps_its_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster = FakeCluster()
    requests = _refuse_custom_objects(cluster, monkeypatch)
    environment = _environment(
        tmp_path,
        cluster,
        environment_class=_DirectApplyEnvironment,
    )
    assert isinstance(environment, _DirectApplyEnvironment)
    await environment.start(False)

    with pytest.raises(ValueError, match="cannot change network policy after start"):
        await environment.set_network_policy(_policy(NetworkMode.PUBLIC))
    with pytest.raises(ValueError, match="network_policy_backend='label'"):
        await environment.set_network_policy(
            NetworkPolicy(
                network_mode=NetworkMode.ALLOWLIST, allowed_hosts=["198.51.100.7"]
            )
        )
    with pytest.raises(ValueError, match="cannot change a running pod's labels"):
        await environment.apply(_policy(NetworkMode.PUBLIC))

    assert environment.network_policy.network_mode == NetworkMode.NO_NETWORK
    labels = _pod_labels(cluster)
    assert labels is not None
    assert labels[NO_NETWORK_LABEL] == "true"
    assert cluster.calls == [("create_pod", _POD)]
    assert requests == []


async def test_a_public_pod_is_never_switched_to_no_network(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster = FakeCluster()
    requests = _refuse_custom_objects(cluster, monkeypatch)
    environment = _environment(
        tmp_path,
        cluster,
        baseline=NetworkMode.PUBLIC,
        phase=NetworkMode.PUBLIC,
        environment_class=_DirectApplyEnvironment,
    )
    assert isinstance(environment, _DirectApplyEnvironment)
    await environment.start(False)

    with pytest.raises(ValueError, match="cannot change a running pod's labels"):
        await environment.apply(_policy(NetworkMode.NO_NETWORK))

    assert NO_NETWORK_LABEL not in (_pod_labels(cluster) or {})
    assert requests == []


async def test_reapplying_no_network_checks_egress_again_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster = FakeCluster()
    requests = _refuse_custom_objects(cluster, monkeypatch)
    environment = _environment(
        tmp_path, cluster, environment_class=_DirectApplyEnvironment
    )
    assert isinstance(environment, _DirectApplyEnvironment)
    await environment.start(False)
    probes = cluster.probes

    await environment.set_network_policy(_policy(NetworkMode.NO_NETWORK))
    assert cluster.probes == probes

    await environment.apply(_policy(NetworkMode.NO_NETWORK))
    assert cluster.probes == probes + 2

    cluster.no_network_label_policy = False
    environment.POLICY_CONVERGE_TIMEOUT_SEC = 0.0
    with pytest.raises(RuntimeError, match="still not closed"):
        await environment.apply(_policy(NetworkMode.NO_NETWORK))

    assert cluster.calls == [("create_pod", _POD)]
    assert requests == []


@pytest.mark.parametrize("backend", ["cilium", "label"])
def test_pod_labels_may_not_take_the_no_network_label(
    tmp_path: Path, backend: NetworkPolicyBackend
) -> None:
    with pytest.raises(ValueError, match=NO_NETWORK_LABEL):
        _environment(
            tmp_path,
            FakeCluster(),
            pod_labels={NO_NETWORK_LABEL: "false"},
            network_policy_backend=backend,
        )


@pytest.mark.parametrize("configured", ["calico", "Label"])
def test_an_unknown_backend_is_refused(tmp_path: Path, configured: str) -> None:
    with pytest.raises(ValueError, match="network_policy_backend must be one of"):
        _environment(
            tmp_path,
            FakeCluster(),
            network_policy_backend=cast(NetworkPolicyBackend, configured),
        )


async def test_the_cilium_backend_never_labels_the_pod(tmp_path: Path) -> None:
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster, network_policy_backend="cilium")

    await environment.start(False)

    assert NO_NETWORK_LABEL not in (_pod_labels(cluster) or {})
    assert [operation for operation, _ in cluster.calls] == [
        "create_policy",
        "create_pod",
        "patch_policy",
    ]
