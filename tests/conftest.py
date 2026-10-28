"""Shared pytest configuration and opt-in staging E2E lifecycle fixtures."""

from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import uuid4

import pytest
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

_IMAGE = "python:3.13-slim"
_NAMESPACE_PREFIX = "harbor-kubernetes-e2e"
_POLL_INTERVAL_SEC = 1
_NAMESPACE_DELETE_TIMEOUT_SEC = 180
_POD_READY_TIMEOUT_SEC = 180
_POD_DELETE_TIMEOUT_SEC = 60
_IMAGE_PULL_FAILURES = frozenset(
    {"ErrImagePull", "ImagePullBackOff", "InvalidImageName"}
)


@dataclass(frozen=True)
class E2ECluster:
    """Live cluster clients and the scratch namespace exclusive to this test session."""

    kube_context: str
    namespace: str
    cluster_dns_ip: str
    custom_objects_api: k8s_client.CustomObjectsApi


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register explicit opt-in flags for tests that mutate a real cluster."""
    group = parser.getgroup("harbor-kubernetes e2e")
    group.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help="run opt-in E2E tests against the Kubernetes context named by --kube-context",
    )
    group.addoption(
        "--kube-context",
        default=None,
        metavar="CONTEXT",
        help="kubeconfig context for --e2e; required when E2E tests are enabled",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Declare the marker and reject unsafe E2E invocations before collection."""
    config.addinivalue_line(
        "markers", "e2e: run a real plain-Harbor trial against a Kubernetes cluster"
    )
    if config.getoption("--e2e") and not config.getoption("--kube-context"):
        raise pytest.UsageError("--e2e requires --kube-context CONTEXT")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Keep real-cluster tests out of normal local and CI unit runs."""
    if config.getoption("--e2e"):
        return
    skip = pytest.mark.skip(
        reason="pass --e2e --kube-context CONTEXT to run real-cluster E2E"
    )
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def e2e_cluster(request: pytest.FixtureRequest):
    """Create, preflight, and always delete one session-scoped scratch namespace."""
    kube_context = request.config.getoption("--kube-context")
    assert isinstance(kube_context, str)
    k8s_config.load_kube_config(context=kube_context)
    core_api = k8s_client.CoreV1Api()
    custom_objects_api = k8s_client.CustomObjectsApi()
    namespace = f"{_NAMESPACE_PREFIX}-{uuid4().hex[:12]}"
    namespace_created = False

    try:
        core_api.create_namespace(
            k8s_client.V1Namespace(metadata=k8s_client.V1ObjectMeta(name=namespace))
        )
        namespace_created = True
        _wait_for_namespace_active(core_api, namespace)
        cluster_dns_ip = _cluster_dns_ip(core_api)
        _preflight_image_pull(core_api, namespace)
        print(f"E2E image preflight pulled {_IMAGE} in {namespace}")
        yield E2ECluster(
            kube_context=kube_context,
            namespace=namespace,
            cluster_dns_ip=cluster_dns_ip,
            custom_objects_api=custom_objects_api,
        )
    finally:
        if namespace_created:
            _delete_namespace_and_wait(core_api, namespace)
            print(f"E2E scratch namespace deleted: {namespace}")


def _wait_for_namespace_active(core_api: k8s_client.CoreV1Api, namespace: str) -> None:
    deadline = time.monotonic() + _POD_READY_TIMEOUT_SEC
    while True:
        current = _read_namespace(core_api, namespace)
        if current.status is not None and current.status.phase == "Active":
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"scratch namespace {namespace} did not become Active")
        time.sleep(_POLL_INTERVAL_SEC)


def _cluster_dns_ip(core_api: k8s_client.CoreV1Api) -> str:
    service = _read_service(core_api, "kube-dns", "kube-system")
    cluster_dns_ip = None if service.spec is None else service.spec.cluster_ip
    if not cluster_dns_ip:
        raise AssertionError("kube-system/kube-dns has no ClusterIP")
    return cluster_dns_ip


def _read_namespace(
    core_api: k8s_client.CoreV1Api, namespace: str
) -> k8s_client.V1Namespace:
    return core_api.read_namespace(namespace)


def _read_service(
    core_api: k8s_client.CoreV1Api, name: str, namespace: str
) -> k8s_client.V1Service:
    return core_api.read_namespaced_service(name, namespace)


def _read_pod(
    core_api: k8s_client.CoreV1Api, name: str, namespace: str
) -> k8s_client.V1Pod:
    return core_api.read_namespaced_pod(name, namespace)


def _preflight_image_pull(core_api: k8s_client.CoreV1Api, namespace: str) -> None:
    pod_name = "image-pull"
    core_api.create_namespaced_pod(
        namespace,
        k8s_client.V1Pod(
            metadata=k8s_client.V1ObjectMeta(name=pod_name),
            spec=k8s_client.V1PodSpec(
                restart_policy="Never",
                automount_service_account_token=False,
                enable_service_links=False,
                containers=[
                    k8s_client.V1Container(
                        name="main",
                        image=_IMAGE,
                        command=["python", "-c", "import time; time.sleep(300)"],
                    )
                ],
            ),
        ),
    )
    try:
        deadline = time.monotonic() + _POD_READY_TIMEOUT_SEC
        while True:
            pod = _read_pod(core_api, pod_name, namespace)
            status = pod.status
            container_statuses = (
                [] if status is None else status.container_statuses or []
            )
            for container in container_statuses:
                waiting = None if container.state is None else container.state.waiting
                if waiting is not None and waiting.reason in _IMAGE_PULL_FAILURES:
                    pytest.fail(
                        f"staging cannot pull {_IMAGE}: {waiting.reason}: "
                        + f"{waiting.message or 'no message'}"
                    )
            if (
                status is not None
                and status.phase == "Running"
                and container_statuses
                and all(container.ready for container in container_statuses)
            ):
                return
            if status is not None and status.phase in {
                "Failed",
                "Succeeded",
                "Unknown",
            }:
                pytest.fail(
                    f"staging cannot pull {_IMAGE}: pod ended {status.phase}: "
                    + f"{status.message or status.reason or 'no reason'}"
                )
            if time.monotonic() >= deadline:
                pytest.fail(
                    f"staging did not pull {_IMAGE} within {_POD_READY_TIMEOUT_SEC}s"
                )
            time.sleep(_POLL_INTERVAL_SEC)
    finally:
        _delete_pod_and_wait(core_api, namespace, pod_name)


def _delete_pod_and_wait(
    core_api: k8s_client.CoreV1Api, namespace: str, pod_name: str
) -> None:
    core_api.delete_namespaced_pod(
        pod_name,
        namespace,
        body=k8s_client.V1DeleteOptions(grace_period_seconds=0),
    )
    deadline = time.monotonic() + _POD_DELETE_TIMEOUT_SEC
    while True:
        try:
            core_api.read_namespaced_pod(pod_name, namespace)
        except k8s_client.ApiException as error:
            if error.status == 404:
                return
            raise
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"image pull preflight pod {pod_name} was not deleted within "
                + f"{_POD_DELETE_TIMEOUT_SEC}s"
            )
        time.sleep(_POLL_INTERVAL_SEC)


def _delete_namespace_and_wait(core_api: k8s_client.CoreV1Api, namespace: str) -> None:
    try:
        core_api.delete_namespace(namespace)
    except k8s_client.ApiException as error:
        if error.status != 404:
            raise
        return

    deadline = time.monotonic() + _NAMESPACE_DELETE_TIMEOUT_SEC
    while True:
        try:
            core_api.read_namespace(namespace)
        except k8s_client.ApiException as error:
            if error.status == 404:
                return
            raise
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"scratch namespace {namespace} was not deleted within "
                + f"{_NAMESPACE_DELETE_TIMEOUT_SEC}s"
            )
        time.sleep(_POLL_INTERVAL_SEC)
