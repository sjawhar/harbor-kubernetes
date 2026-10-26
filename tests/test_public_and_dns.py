"""Regression tests for plain-Harbor public and off-cluster Kubernetes runs."""

from __future__ import annotations

import pytest
from harbor.models.task.config import NetworkMode
from kubernetes import client as k8s_client
from test_environment import FakeCluster, _environment

from harbor_kubernetes.environment import KubernetesApis, KubernetesEnvironment


async def test_public_network_is_allowed_without_creating_a_cilium_policy(
    tmp_path,
) -> None:
    """A plain Harbor public task does not require Cilium."""
    cluster = FakeCluster()
    environment = _environment(
        tmp_path,
        cluster,
        baseline=NetworkMode.PUBLIC,
        phase=NetworkMode.PUBLIC,
    )

    await environment.start(False)

    assert cluster.calls == [("create_pod", environment.pod_name)]
    assert cluster.policies == {}


def test_allow_public_network_false_restores_the_public_network_refusal(
    tmp_path,
) -> None:
    """Hosts can retain a restricted posture for Harbor public tasks."""
    enabled = _environment(
        tmp_path / "enabled",
        FakeCluster(),
        baseline=NetworkMode.PUBLIC,
        allow_public_network=True,
    )
    assert enabled.allow_public_network is True
    with pytest.raises(ValueError, match="network_mode='public'"):
        _environment(
            tmp_path / "disabled",
            FakeCluster(),
            baseline=NetworkMode.PUBLIC,
            allow_public_network=False,
        )


def test_off_cluster_dns_target_uses_kube_dns_service_or_an_explicit_override(
    tmp_path,
) -> None:
    """No-network convergence finds a cluster address without reading a laptop resolver."""
    cluster = FakeCluster()
    environment = _environment(tmp_path, cluster)
    environment._connected_apis = KubernetesApis(cluster, cluster, cluster.open_exec)

    assert KubernetesEnvironment.cluster_dns_probe_target(environment) == "10.96.0.10"

    override = _environment(tmp_path / "override", cluster, cluster_dns_ip="10.0.0.53")
    override._connected_apis = KubernetesApis(cluster, cluster, cluster.open_exec)
    assert KubernetesEnvironment.cluster_dns_probe_target(override) == "10.0.0.53"

    cluster.service_dns_ip = None
    unavailable = _environment(tmp_path / "missing", cluster)
    unavailable._connected_apis = KubernetesApis(cluster, cluster, cluster.open_exec)
    with pytest.raises(RuntimeError, match="cluster_dns_ip"):
        KubernetesEnvironment.cluster_dns_probe_target(unavailable)


async def test_missing_cilium_refuses_a_restricted_trial_before_creating_its_pod(
    tmp_path,
) -> None:
    """A missing Cilium CRD has an actionable error and never launches the trial."""
    cluster = FakeCluster()
    cluster.cilium_enabled = False
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)

    with pytest.raises(RuntimeError, match="Cilium"):
        await environment.start(False)

    assert cluster.pods == {}


def _not_found_with_body(body: str) -> k8s_client.ApiException:
    error = k8s_client.ApiException(status=404, reason="Not Found")
    error.body = body
    return error


async def test_missing_cilium_crd_body_raises_the_cilium_refusal_before_pod_creation(
    tmp_path,
) -> None:
    error = _not_found_with_body("404 page not found\n")
    cluster = FakeCluster(cilium_create_error=error)
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)

    with pytest.raises(RuntimeError, match="Cilium") as refused:
        await environment.start(False)

    assert refused.value.__cause__ is error
    assert cluster.pods == {}


async def test_missing_namespace_status_body_reraises_the_original_api_exception(
    tmp_path,
) -> None:
    error = _not_found_with_body(
        '{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",'
        '"message":"namespaces \\"no-such-ns\\" not found","reason":"NotFound",'
        '"details":{"name":"no-such-ns","kind":"namespaces"},"code":404}'
    )
    cluster = FakeCluster(cilium_create_error=error)
    environment = _environment(tmp_path, cluster, baseline=NetworkMode.NO_NETWORK)

    with pytest.raises(k8s_client.ApiException) as raised:
        await environment.start(False)

    assert raised.value is error
    assert cluster.pods == {}
