"""One plain Kubernetes pod per Harbor trial."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import shlex
import tarfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from ipaddress import ip_address
from pathlib import Path
from typing import Protocol, Required, TypedDict, Unpack, cast, override

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.environments.capabilities import (
    EnvironmentCapabilities,
    EnvironmentResourceCapabilities,
)
from harbor.models.task.config import (
    EnvironmentConfig,
    HealthcheckConfig,
    NetworkMode,
    NetworkPolicy,
    TpuSpec,
)
from harbor.models.trial.config import ResourceMode, ServiceVolumeConfig
from harbor.models.trial.paths import TrialPaths
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.config.incluster_config import SERVICE_TOKEN_FILENAME
from kubernetes.config.kube_config import KUBE_CONFIG_DEFAULT_LOCATION
from kubernetes.stream import stream
from pydantic import BaseModel, ConfigDict

from harbor_kubernetes.network_policy import (
    CILIUM_POLICY_GROUP,
    CILIUM_POLICY_KIND,
    CILIUM_POLICY_PLURAL,
    CILIUM_POLICY_VERSION,
    PUBLIC_NETWORK_REFUSAL,
    SESSION_ID_ANNOTATION,
    SESSION_LABEL,
    CiliumNetworkPolicyManifest,
    OwnerReference,
    build_network_policy_manifest,
    validated_pod_labels,
)
from harbor_kubernetes.transport import (
    _DIRECTORY_PATH_EXIT,
    _MISSING_PATH_EXIT,
    _TIMEOUT_EXIT_CODE,
    _TIMEOUT_KILL_AFTER_SEC,
    DOWNLOAD_DIR_SCRIPT,
    DOWNLOAD_FILE_SCRIPT,
    UPLOAD_DIR_SCRIPT,
    UPLOAD_FILE_SCRIPT,
    ExecStream,
    KubernetesExecStreamClosedError,
    collect_output,
    decode_output,
    pack_directory,
    return_code,
)

ENVIRONMENT_TYPE = "kubernetes"
CONTAINER_NAME = "main"
DEFAULT_IMAGE_PULL_POLICY = "IfNotPresent"
DEFAULT_POD_READY_TIMEOUT_SEC = 900.0
LOOPBACK_NAMESERVER = "127.0.0.1"
RUNNER_RESOLV_CONF = Path("/etc/resolv.conf")
DNS_PORT = "53"
K8S_NAME_MAX_LENGTH = 63
COMPOSE_FILE_NAME = "docker-compose.yaml"
POD_START_FAILURES = frozenset(
    {
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "CreateContainerConfigError",
        "CreateContainerError",
    }
)


def _is_namespace_not_found(error: k8s_client.ApiException) -> bool:
    """Whether a 404 body is Kubernetes' JSON Status for a missing namespace."""
    if error.status != 404 or not isinstance(error.body, str):
        return False
    try:
        document: object = json.loads(error.body)
    except json.JSONDecodeError:
        return False
    if not isinstance(document, dict) or document.get("kind") != "Status":
        return False
    details = document.get("details")
    return isinstance(details, dict) and details.get("kind") == "namespaces"


class KubernetesEnvironmentKwargs(TypedDict, total=False):
    """The job-level ``environment.kwargs`` accepted by this environment."""

    namespace: Required[str]
    kube_context: str
    runtime_class_name: str
    pod_labels: dict[str, str]
    annotations: dict[str, str]
    image_pull_policy: str
    pod_ready_timeout_sec: float
    ephemeral_storage_request: str
    ephemeral_storage_limit: str
    allow_public_network: bool
    cluster_dns_ip: str


class _BaseEnvironmentKwargs(TypedDict, total=False):
    """The standard kwargs passed by Harbor's environment factory."""

    logger: logging.Logger | None
    override_cpus: int | None
    override_memory_mb: int | None
    override_storage_mb: int | None
    override_gpus: int | None
    override_tpu: TpuSpec | None
    cpu_enforcement_policy: ResourceMode
    memory_enforcement_policy: ResourceMode
    persistent_env: dict[str, str] | None
    mounts: list[ServiceVolumeConfig] | None
    network_policy: NetworkPolicy | None
    phase_network_policies: Sequence[NetworkPolicy] | None
    extra_docker_compose: Sequence[Path | str] | None


class KubernetesPodGoneError(RuntimeError):
    """Raised when the trial's pod no longer exists."""


class PodApi(Protocol):
    """Core Kubernetes API calls used by a trial pod."""

    def create_namespaced_pod(
        self, namespace: str, body: k8s_client.V1Pod
    ) -> k8s_client.V1Pod: ...

    def read_namespaced_pod(self, name: str, namespace: str) -> k8s_client.V1Pod: ...

    def delete_namespaced_pod(
        self,
        name: str,
        namespace: str,
        *,
        body: k8s_client.V1DeleteOptions | None = None,
    ) -> object: ...

    def read_namespaced_service(
        self, name: str, namespace: str
    ) -> k8s_client.V1Service: ...


class CiliumPolicyApi(Protocol):
    """Custom-object API calls used by a trial Cilium policy."""

    def create_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        body: CiliumNetworkPolicyManifest,
    ) -> object: ...

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> object: ...

    def replace_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: CiliumNetworkPolicyManifest,
    ) -> object: ...

    def delete_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> object: ...


class _LiveObjectMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    resourceVersion: str


class _LiveObject(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    metadata: _LiveObjectMetadata


type ExecOpener = Callable[[str, str, list[str], bool], ExecStream]


@dataclass(frozen=True)
class KubernetesApis:
    """The Kubernetes APIs one environment instance uses."""

    pods: PodApi
    cilium_policies: CiliumPolicyApi
    open_exec: ExecOpener


def _k8s_name(name: str) -> str:
    """Return a deterministic RFC 1123 resource name."""
    sanitized = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "harbor"
    if len(sanitized) <= K8S_NAME_MAX_LENGTH:
        return sanitized
    digest = sha256(name.encode("utf-8")).hexdigest()[:8]
    prefix = sanitized[: K8S_NAME_MAX_LENGTH - len(digest) - 1].rstrip("-")
    return f"{prefix}-{digest}"


def egress_probe(target: str) -> str:
    """Return a command that reports whether TCP DNS egress remains reachable."""
    return (
        "command -v bash >/dev/null 2>&1 || { echo NO_BASH; exit 0; }; "
        + f'if timeout 4 bash -c "</dev/tcp/{target}/{DNS_PORT}" >/dev/null 2>&1; '
        + "then echo OPEN; else echo CLOSED; fi"
    )


class KubernetesEnvironment(BaseEnvironment):
    """Single-pod Harbor environment on any Kubernetes cluster.

    The environment creates a plain pod from a task's prebuilt image and executes commands
    or transfers files over Kubernetes exec. Network-restricted tasks require Cilium because
    the policy uses CiliumNetworkPolicy. A public task uses Harbor's public policy only after
    the public-network cutover in this module.
    """

    POLL_INTERVAL_SEC = 1.0
    POLICY_PROBE_INTERVAL_SEC = 2.0
    POLICY_CONVERGE_TIMEOUT_SEC = 60.0
    POD_DELETE_TIMEOUT_SEC = 60.0
    EXEC_READY_ATTEMPTS = 60

    def __init__(
        self,
        environment_dir: Path,
        environment_name: str,
        session_id: str,
        trial_paths: TrialPaths,
        task_env_config: EnvironmentConfig,
        *,
        namespace: str,
        kube_context: str | None = None,
        runtime_class_name: str | None = None,
        pod_labels: Mapping[str, str] | None = None,
        annotations: Mapping[str, str] | None = None,
        image_pull_policy: str = DEFAULT_IMAGE_PULL_POLICY,
        pod_ready_timeout_sec: float = DEFAULT_POD_READY_TIMEOUT_SEC,
        ephemeral_storage_request: str | None = None,
        ephemeral_storage_limit: str | None = None,
        allow_public_network: bool = True,
        cluster_dns_ip: str | None = None,
        **kwargs: Unpack[_BaseEnvironmentKwargs],
    ) -> None:
        if not namespace:
            raise ValueError("namespace must name the namespace trial pods run in")
        self.namespace = namespace
        self.kube_context = kube_context
        self.runtime_class_name = runtime_class_name
        self.pod_labels = validated_pod_labels(dict(pod_labels or {}))
        self.annotations = dict(annotations or {})
        self.image_pull_policy = image_pull_policy
        self.pod_ready_timeout_sec = pod_ready_timeout_sec
        self.ephemeral_storage_request = ephemeral_storage_request
        self.ephemeral_storage_limit = ephemeral_storage_limit
        self.allow_public_network = allow_public_network
        self.cluster_dns_ip = cluster_dns_ip
        self.pod_name = _k8s_name(session_id)
        self.network_policy_name = f"{self.pod_name}-sandbox-egress"
        self._apis_lock = asyncio.Lock()
        self._connected_apis: KubernetesApis | None = None
        self._pod_started = False
        self._pod_owner: OwnerReference | None = None
        self._policy_created = False
        self._cluster_dns_target: str | None = None
        super().__init__(
            environment_dir=environment_dir,
            environment_name=environment_name,
            session_id=session_id,
            trial_paths=trial_paths,
            task_env_config=task_env_config,
            **kwargs,
        )

    @staticmethod
    @override
    def type() -> str:
        """Return Harbor's provider type name."""
        return ENVIRONMENT_TYPE

    @classmethod
    @override
    def preflight(cls) -> None:
        """Refuse a run when neither in-cluster credentials nor a kubeconfig exists."""
        if Path(SERVICE_TOKEN_FILENAME).exists():
            return
        kubeconfigs = [
            Path(location).expanduser()
            for location in KUBE_CONFIG_DEFAULT_LOCATION.split(os.pathsep)
            if location
        ]
        if not any(path.is_file() for path in kubeconfigs):
            raise SystemExit(
                "KubernetesEnvironment needs in-cluster service-account credentials "
                + f"({SERVICE_TOKEN_FILENAME}) or a kubeconfig; found neither "
                + f"(looked for {', '.join(str(path) for path in kubeconfigs)})."
            )

    @classmethod
    @override
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        """Advertise Kubernetes CPU and memory request and limit support."""
        return EnvironmentResourceCapabilities(
            cpu_limit=True, cpu_request=True, memory_limit=True, memory_request=True
        )

    @property
    @override
    def capabilities(self) -> EnvironmentCapabilities:
        """Advertise the supported one-pod and Cilium network features."""
        return EnvironmentCapabilities(
            disable_internet=True,
            network_allowlist=True,
            network_allowlist_ipv4_addresses=True,
            network_allowlist_ipv6_addresses=True,
            network_allowlist_ipv4_cidrs=True,
            network_allowlist_ipv6_cidrs=True,
            dynamic_network_policy=True,
            docker_compose=False,
        )

    @override
    def _validate_definition(self) -> None:
        if (self.environment_dir / COMPOSE_FILE_NAME).exists():
            raise ValueError(
                f"{self.type()} runs one container per trial; this task defines a "
                f"compose environment ({self.environment_dir / COMPOSE_FILE_NAME})."
            )
        if not self.task_env_config.docker_image:
            raise ValueError(
                f"{self.type()} boots prebuilt images only; set [environment] "
                + "docker_image (this provider has no image builder)."
            )

    @override
    def validate_network_policy_support(
        self, network_policy: NetworkPolicy | None = None
    ) -> None:
        """Validate public policy support against the configured host posture."""
        policy = self.network_policy if network_policy is None else network_policy
        if policy.network_mode == NetworkMode.PUBLIC and not self.allow_public_network:
            raise ValueError(PUBLIC_NETWORK_REFUSAL)
        super().validate_network_policy_support(network_policy)

    def _connect(self) -> KubernetesApis:
        """Load credentials and construct isolated Kubernetes API clients."""
        configuration = k8s_client.Configuration()
        if Path(SERVICE_TOKEN_FILENAME).exists():
            k8s_config.load_incluster_config(client_configuration=configuration)
        else:
            k8s_config.load_kube_config(
                context=self.kube_context, client_configuration=configuration
            )
        rest_client = k8s_client.ApiClient(configuration)

        def open_exec(
            pod: str, namespace: str, command: list[str], stdin: bool
        ) -> ExecStream:
            api = k8s_client.CoreV1Api(k8s_client.ApiClient(configuration))
            return stream(
                api.connect_get_namespaced_pod_exec,
                pod,
                namespace,
                command=command,
                stderr=True,
                stdin=stdin,
                stdout=True,
                tty=False,
                binary=True,
                _preload_content=False,
            )

        return KubernetesApis(
            pods=cast(PodApi, k8s_client.CoreV1Api(rest_client)),
            cilium_policies=cast(
                CiliumPolicyApi, k8s_client.CustomObjectsApi(rest_client)
            ),
            open_exec=open_exec,
        )

    async def _apis(self) -> KubernetesApis:
        async with self._apis_lock:
            if self._connected_apis is None:
                self._connected_apis = await asyncio.to_thread(self._connect)
            return self._connected_apis

    def resolve_image_mount_reference(self, reference: str) -> str:
        """Return an image-volume reference; subclasses may rewrite it for their registry."""
        return reference

    def pod_manifest(self) -> k8s_client.V1Pod:
        """Return the pod manifest created for this trial."""
        image = self.task_env_config.docker_image
        if not image:
            raise ValueError(f"{self.type()} boots prebuilt images only")
        requests: dict[str, str] = {}
        limits: dict[str, str] = {}
        cpu_request = self._resource_request_value(
            "cpu", auto_mode=ResourceMode.GUARANTEE
        )
        cpu_limit = self._resource_limit_value("cpu", auto_mode=ResourceMode.GUARANTEE)
        memory_request = self._resource_request_value(
            "memory", auto_mode=ResourceMode.GUARANTEE
        )
        memory_limit = self._resource_limit_value(
            "memory", auto_mode=ResourceMode.GUARANTEE
        )
        if cpu_request is not None:
            requests["cpu"] = str(cpu_request)
        if cpu_limit is not None:
            limits["cpu"] = str(cpu_limit)
        if memory_request is not None:
            requests["memory"] = f"{memory_request}Mi"
        if memory_limit is not None:
            limits["memory"] = f"{memory_limit}Mi"
        if self.ephemeral_storage_request is not None:
            requests["ephemeral-storage"] = self.ephemeral_storage_request
        elif (storage_mb := self._effective_storage_mb) is not None:
            requests["ephemeral-storage"] = f"{storage_mb}Mi"
        if self.ephemeral_storage_limit is not None:
            limits["ephemeral-storage"] = self.ephemeral_storage_limit
        volumes, volume_mounts = self._image_volumes()
        return k8s_client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s_client.V1ObjectMeta(
                name=self.pod_name,
                namespace=self.namespace,
                labels={**self.pod_labels, SESSION_LABEL: self.pod_name},
                annotations={
                    **self.annotations,
                    SESSION_ID_ANNOTATION: self.session_id,
                },
            ),
            spec=k8s_client.V1PodSpec(
                containers=[
                    k8s_client.V1Container(
                        name=CONTAINER_NAME,
                        image=image,
                        image_pull_policy=self.image_pull_policy,
                        args=["sleep", "infinity"],
                        env=[
                            k8s_client.V1EnvVar(name=name, value=value)
                            for name, value in self._startup_env().items()
                        ],
                        security_context=k8s_client.V1SecurityContext(
                            run_as_user=0, run_as_group=0
                        ),
                        resources=k8s_client.V1ResourceRequirements(
                            requests=requests or None, limits=limits or None
                        ),
                        volume_mounts=volume_mounts or None,
                    )
                ],
                restart_policy="Never",
                automount_service_account_token=False,
                enable_service_links=False,
                runtime_class_name=self.runtime_class_name,
                dns_policy="None",
                dns_config=k8s_client.V1PodDNSConfig(nameservers=[LOOPBACK_NAMESERVER]),
                volumes=volumes or None,
            ),
        )

    def _image_volumes(
        self,
    ) -> tuple[list[k8s_client.V1Volume], list[k8s_client.V1VolumeMount]]:
        volumes: list[k8s_client.V1Volume] = []
        volume_mounts: list[k8s_client.V1VolumeMount] = []
        for mount in self._mounts:
            if mount["type"] != "image":
                continue
            name = _k8s_name(f"image{mount['target']}")
            volumes.append(
                k8s_client.V1Volume(
                    name=name,
                    image=k8s_client.V1ImageVolumeSource(
                        reference=self.resolve_image_mount_reference(mount["source"]),
                        pull_policy=DEFAULT_IMAGE_PULL_POLICY,
                    ),
                )
            )
            volume_mounts.append(
                k8s_client.V1VolumeMount(
                    name=name, mount_path=mount["target"], read_only=True
                )
            )
        return volumes, volume_mounts

    def network_policy_manifest(
        self, network_policy: NetworkPolicy
    ) -> CiliumNetworkPolicyManifest | None:
        """Return the Cilium policy manifest for one restricted network mode."""
        if network_policy.network_mode == NetworkMode.PUBLIC:
            return None
        return build_network_policy_manifest(
            namespace=self.namespace,
            pod_name=self.pod_name,
            policy_name=self.network_policy_name,
            pod_labels=self.pod_labels,
            network_policy=network_policy,
            pod_owner=self._pod_owner,
        )

    @override
    async def start(self, force_build: bool) -> None:
        """Create a policy, create the pod, and wait until exec is ready."""
        if force_build:
            raise ValueError(
                f"{self.type()} boots prebuilt images only and cannot force a build"
            )
        if self._requires_dns_probe():
            await self._prepare_dns_probe_target()
        await self._apply_policy_object(self.network_policy)
        await self._create_pod()
        self._pod_started = True
        await self._apply_policy_object(self.network_policy)
        await self._wait_for_pod_ready()
        await self._wait_for_exec_ready()
        if self.network_policy.network_mode == NetworkMode.NO_NETWORK:
            await self._wait_for_egress_closed()
        await self.create_trial_directories()
        await self._upload_environment_dir_after_start()

    async def create_trial_directories(self) -> None:
        """Create Harbor writable mount targets before the trial starts."""
        results = [await self.ensure_dirs(self._mount_targets(writable_only=True))]
        for result in results:
            if result is not None and result.return_code != 0:
                raise RuntimeError(
                    f"Could not create the trial's directories in pod {self.pod_name}: "
                    + (result.stderr or result.stdout or "no output")
                )

    @override
    async def run_healthcheck(
        self, healthcheck: HealthcheckConfig | None = None
    ) -> None:
        """Run Harbor's task healthcheck without host-specific side effects."""
        await super().run_healthcheck(healthcheck)

    @override
    async def stop(self, delete: bool) -> None:
        """Delete the pod before lifting its policy, or leave both when asked."""
        if delete and self._connected_apis is not None:
            await self._delete_pod_then_policy()

    async def _delete_pod_then_policy(self) -> None:
        apis = await self._apis()
        try:
            await asyncio.to_thread(
                apis.pods.delete_namespaced_pod,
                name=self.pod_name,
                namespace=self.namespace,
                body=k8s_client.V1DeleteOptions(
                    grace_period_seconds=0, propagation_policy="Orphan"
                ),
            )
        except k8s_client.ApiException as error:
            if error.status != 404:
                raise
        await self._wait_for_pod_gone()
        self._pod_started = False
        self._pod_owner = None
        if self._policy_created:
            await self._delete_policy_object()

    @override
    async def _apply_network_policy(self, network_policy: NetworkPolicy) -> None:
        if network_policy.network_mode == NetworkMode.NO_NETWORK:
            await self._prepare_dns_probe_target()
        await self._apply_policy_object(network_policy)
        if self._pod_started and network_policy.network_mode == NetworkMode.NO_NETWORK:
            await self._wait_for_egress_closed()

    async def _apply_policy_object(self, network_policy: NetworkPolicy) -> None:
        desired = self.network_policy_manifest(network_policy)
        if desired is None:
            if self._policy_created:
                await self._delete_policy_object()
            return
        apis = await self._apis()
        try:
            await asyncio.to_thread(
                apis.cilium_policies.create_namespaced_custom_object,
                group=CILIUM_POLICY_GROUP,
                version=CILIUM_POLICY_VERSION,
                namespace=self.namespace,
                plural=CILIUM_POLICY_PLURAL,
                body=desired,
            )
        except k8s_client.ApiException as error:
            if error.status == 404 and not _is_namespace_not_found(error):
                raise RuntimeError(
                    f"{CILIUM_POLICY_KIND} is unavailable: restricted Harbor trials "
                    + "require Cilium on the target cluster."
                ) from error
            if error.status != 409:
                raise
            live = await asyncio.to_thread(
                apis.cilium_policies.get_namespaced_custom_object,
                group=CILIUM_POLICY_GROUP,
                version=CILIUM_POLICY_VERSION,
                namespace=self.namespace,
                plural=CILIUM_POLICY_PLURAL,
                name=self.network_policy_name,
            )
            desired["metadata"]["resourceVersion"] = _LiveObject.model_validate(
                live
            ).metadata.resourceVersion
            await asyncio.to_thread(
                apis.cilium_policies.replace_namespaced_custom_object,
                group=CILIUM_POLICY_GROUP,
                version=CILIUM_POLICY_VERSION,
                namespace=self.namespace,
                plural=CILIUM_POLICY_PLURAL,
                name=self.network_policy_name,
                body=desired,
            )
        self._policy_created = True

    async def _delete_policy_object(self) -> None:
        apis = await self._apis()
        try:
            await asyncio.to_thread(
                apis.cilium_policies.delete_namespaced_custom_object,
                group=CILIUM_POLICY_GROUP,
                version=CILIUM_POLICY_VERSION,
                namespace=self.namespace,
                plural=CILIUM_POLICY_PLURAL,
                name=self.network_policy_name,
            )
        except k8s_client.ApiException as error:
            if error.status != 404:
                raise
        self._policy_created = False

    async def _create_pod(self) -> None:
        apis = await self._apis()
        pod = self.pod_manifest()
        try:
            created = await asyncio.to_thread(
                apis.pods.create_namespaced_pod, namespace=self.namespace, body=pod
            )
        except k8s_client.ApiException as error:
            if error.status != 409:
                raise
            existing = await asyncio.to_thread(
                apis.pods.read_namespaced_pod,
                name=self.pod_name,
                namespace=self.namespace,
            )
            owner = (
                cast(k8s_client.V1ObjectMeta, existing.metadata).annotations or {}
            ).get(SESSION_ID_ANNOTATION)
            if owner != self.session_id:
                raise RuntimeError(
                    f"Pod {self.pod_name} already exists for Harbor session {owner!r}, "
                    + f"not {self.session_id!r}: both fold to one pod name, so it is "
                    + "not this trial's to replace."
                ) from error
            await asyncio.to_thread(
                apis.pods.delete_namespaced_pod,
                name=self.pod_name,
                namespace=self.namespace,
                body=k8s_client.V1DeleteOptions(
                    grace_period_seconds=0, propagation_policy="Orphan"
                ),
            )
            await self._wait_for_pod_gone()
            created = await asyncio.to_thread(
                apis.pods.create_namespaced_pod, namespace=self.namespace, body=pod
            )
        uid = cast(k8s_client.V1ObjectMeta, created.metadata).uid
        if not uid:
            raise RuntimeError(
                f"The API server created pod {self.pod_name} but returned no uid for it, "
                + f"so its {CILIUM_POLICY_KIND} cannot name it as owner."
            )
        self._pod_owner = {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": self.pod_name,
            "uid": uid,
        }

    async def _read_pod(self) -> k8s_client.V1Pod | None:
        apis = await self._apis()
        try:
            return await asyncio.to_thread(
                apis.pods.read_namespaced_pod,
                name=self.pod_name,
                namespace=self.namespace,
            )
        except k8s_client.ApiException as error:
            if error.status == 404:
                return None
            raise

    async def _wait_for_pod_gone(self) -> None:
        deadline = time.monotonic() + self.POD_DELETE_TIMEOUT_SEC
        while await self._read_pod() is not None:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Pod {self.pod_name} in {self.namespace} was not deleted within "
                    + f"{self.POD_DELETE_TIMEOUT_SEC:.0f}s; its {CILIUM_POLICY_KIND} "
                    + f"{self.network_policy_name} is left in place."
                )
            await asyncio.sleep(self.POLL_INTERVAL_SEC)

    async def _wait_for_pod_ready(self) -> None:
        deadline = time.monotonic() + self.pod_ready_timeout_sec
        while True:
            pod = await self._read_pod()
            if pod is not None:
                status = cast(k8s_client.V1PodStatus, pod.status)
                container_statuses = status.container_statuses or []
                if (
                    status.phase == "Running"
                    and container_statuses
                    and all(container.ready for container in container_statuses)
                ):
                    return
                if status.phase in ("Failed", "Succeeded", "Unknown"):
                    raise RuntimeError(
                        f"Pod {self.pod_name} ended before it was ready ({status.phase}): "
                        + (status.message or status.reason or "no reason given")
                    )
                for container in container_statuses:
                    waiting = container.state.waiting if container.state else None
                    if waiting is not None and waiting.reason in POD_START_FAILURES:
                        raise RuntimeError(
                            f"Pod {self.pod_name} cannot start ({waiting.reason}): "
                            + (waiting.message or "no message")
                        )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Pod {self.pod_name} was not ready within "
                    + f"{self.pod_ready_timeout_sec:.0f}s"
                )
            await asyncio.sleep(self.POLL_INTERVAL_SEC)

    async def _wait_for_exec_ready(self) -> None:
        last_error: Exception | None = None
        for _ in range(self.EXEC_READY_ATTEMPTS):
            try:
                code, _, _ = await self._run(["true"])
            except (k8s_client.ApiException, KubernetesExecStreamClosedError) as error:
                last_error = error
            else:
                if code == 0:
                    return
            await asyncio.sleep(self.POLL_INTERVAL_SEC)
        raise RuntimeError(
            f"Pod {self.pod_name} never accepted an exec "
            + f"({self.EXEC_READY_ATTEMPTS} attempts)"
        ) from last_error

    def _requires_dns_probe(self) -> bool:
        return any(
            policy.network_mode == NetworkMode.NO_NETWORK
            for policy in (self.network_policy, *self._phase_network_policies)
        )

    async def _prepare_dns_probe_target(self) -> None:
        if self._cluster_dns_target is None:
            await self._apis()
            self._cluster_dns_target = self.cluster_dns_probe_target()

    def cluster_dns_probe_target(self) -> str:
        """Return the cluster DNS address used by no-network convergence probes."""
        if self.cluster_dns_ip is not None:
            return self.cluster_dns_ip
        if Path(SERVICE_TOKEN_FILENAME).exists():
            try:
                text = RUNNER_RESOLV_CONF.read_text(encoding="utf-8")
            except OSError as error:
                raise RuntimeError(
                    f"cannot read {RUNNER_RESOLV_CONF} to find the cluster DNS for the "
                    + "no-network convergence probe"
                ) from error
            for line in text.splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[0] == "nameserver":
                    try:
                        if not ip_address(parts[1]).is_loopback:
                            return parts[1]
                    except ValueError:
                        continue
            raise RuntimeError(
                f"no non-loopback nameserver in {RUNNER_RESOLV_CONF}; cannot target the "
                + "cluster DNS for the no-network convergence probe"
            )
        if self._connected_apis is None:
            raise RuntimeError(
                "cannot read the kube-system/kube-dns Service before cluster access is "
                + "configured; set cluster_dns_ip explicitly."
            )
        try:
            service_result = self._connected_apis.pods.read_namespaced_service(
                name="kube-dns", namespace="kube-system"
            )
        except k8s_client.ApiException as error:
            raise RuntimeError(
                "cannot read the kube-system/kube-dns Service for the no-network "
                + "convergence probe; set cluster_dns_ip explicitly."
            ) from error
        if not isinstance(service_result, k8s_client.V1Service):
            raise RuntimeError(
                "the kube-system/kube-dns Service returned an invalid object; set "
                + "cluster_dns_ip explicitly."
            )
        service = service_result
        cluster_ip = None if service.spec is None else service.spec.cluster_ip
        if cluster_ip:
            return cluster_ip
        raise RuntimeError(
            "the kube-system/kube-dns Service has no ClusterIP for the no-network "
            + "convergence probe; set cluster_dns_ip explicitly."
        )

    async def _wait_for_egress_closed(self) -> None:
        await self._prepare_dns_probe_target()
        target = self._cluster_dns_target
        if target is None:
            raise RuntimeError(
                "cluster_dns_ip is required for the no-network convergence probe"
            )
        probe_command = egress_probe(target)
        deadline = time.monotonic() + self.POLICY_CONVERGE_TIMEOUT_SEC
        agreeing = 0
        observed = "(never probed)"
        while True:
            result = await self.exec(probe_command, user="root", timeout_sec=15)
            observed = (result.stdout or "").strip() or (result.stderr or "").strip()
            agreeing = agreeing + 1 if observed == "CLOSED" else 0
            if agreeing >= 2:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Egress from pod {self.pod_name} was still not closed "
                    + f"{self.POLICY_CONVERGE_TIMEOUT_SEC:.0f}s after {CILIUM_POLICY_KIND} "
                    + f"{self.network_policy_name} set no-network (last probe: "
                    + f"{observed!r}); refusing to run a phase that requires it."
                )
            await asyncio.sleep(self.POLICY_PROBE_INTERVAL_SEC)

    async def _run(
        self,
        argv: list[str],
        *,
        stdin: bytes | None = None,
        client_timeout_sec: float | None = None,
    ) -> tuple[int, bytes, bytes]:
        apis = await self._apis()
        try:
            exec_stream = await asyncio.to_thread(
                apis.open_exec, self.pod_name, self.namespace, argv, stdin is not None
            )
        except k8s_client.ApiException as error:
            if error.status == 404:
                raise KubernetesPodGoneError(
                    f"Pod {self.pod_name} no longer exists in {self.namespace}"
                ) from error
            raise
        try:
            if stdin is not None:
                await asyncio.to_thread(exec_stream.write_stdin, stdin)
            collected = asyncio.to_thread(collect_output, exec_stream)
            if client_timeout_sec is None:
                stdout, stderr = await collected
            else:
                stdout, stderr = await asyncio.wait_for(collected, client_timeout_sec)
        finally:
            exec_stream.close()
        return return_code(exec_stream), stdout, stderr

    def remote_command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        user: str | int | None = None,
        timeout_sec: int | None = None,
    ) -> str:
        """Build the root or task-user shell command executed through Kubernetes exec."""
        script = f"bash -c {shlex.quote(command)}"
        if env:
            assignments = " ".join(
                shlex.quote(f"{name}={value}") for name, value in env.items()
            )
            script = f"env {assignments} {script}"
        if cwd:
            script = f"cd {shlex.quote(cwd)} && {script}"
        if isinstance(user, str) and not user:
            raise ValueError("an empty user names no account: `su ''` would be root")
        if user is not None and user not in ("root", 0):
            if isinstance(user, int):
                script = (
                    f'account="$(getent passwd {user} | cut -d: -f1)" '
                    + '&& [ -n "$account" ] '
                    + f"|| {{ echo 'uid {user} has no account in this image' >&2; "
                    + "exit 1; }; "
                    + f'su "$account" -s /bin/bash -c {shlex.quote(script)}'
                )
            else:
                script = f"su {shlex.quote(user)} -s /bin/bash -c {shlex.quote(script)}"
        if timeout_sec:
            script = (
                f"timeout -k {_TIMEOUT_KILL_AFTER_SEC} {timeout_sec} "
                + f"sh -c {shlex.quote(script)}"
            )
        return script

    @override
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        script = self.remote_command(
            command,
            cwd=cwd or self.task_env_config.workdir,
            env=self._merge_env(env),
            user=self._resolve_user(user),
            timeout_sec=timeout_sec,
        )
        client_timeout = (
            None if not timeout_sec else timeout_sec + _TIMEOUT_KILL_AFTER_SEC + 15
        )
        try:
            code, stdout, stderr = await self._run(
                ["sh", "-c", script], client_timeout_sec=client_timeout
            )
        except TimeoutError:
            return ExecResult(
                stdout=None,
                stderr=f"Command timed out after {timeout_sec} seconds",
                return_code=_TIMEOUT_EXIT_CODE,
            )
        return ExecResult(
            stdout=decode_output(stdout), stderr=decode_output(stderr), return_code=code
        )

    @override
    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        source = Path(source_path)
        data = source.read_bytes()
        mode = f"{source.stat().st_mode & 0o7777:o}"
        code, _, stderr = await self._run(
            ["sh", "-c", UPLOAD_FILE_SCRIPT, "sh", str(len(data)), target_path, mode],
            stdin=data,
        )
        if code != 0:
            raise RuntimeError(
                f"Upload of {source} to {self.pod_name}:{target_path} failed "
                + f"(exit {code}): {decode_output(stderr).strip()}"
            )

    @override
    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        source = Path(source_dir)
        if not source.is_dir():
            raise FileNotFoundError(f"{source} is not a directory")
        data = pack_directory(source)
        code, _, stderr = await self._run(
            ["sh", "-c", UPLOAD_DIR_SCRIPT, "sh", str(len(data)), target_dir],
            stdin=data,
        )
        if code != 0:
            raise RuntimeError(
                f"Upload of {source} to {self.pod_name}:{target_dir} failed "
                + f"(exit {code}): {decode_output(stderr).strip()}"
            )

    @override
    async def download_file(self, source_path: str, target_path: Path | str) -> None:
        code, stdout, stderr = await self._run(
            ["sh", "-c", DOWNLOAD_FILE_SCRIPT, "sh", source_path]
        )
        if code == _MISSING_PATH_EXIT:
            raise FileNotFoundError(f"{self.pod_name}:{source_path} does not exist")
        if code == _DIRECTORY_PATH_EXIT:
            raise IsADirectoryError(f"{self.pod_name}:{source_path} is a directory")
        if code != 0:
            raise RuntimeError(
                f"Download of {self.pod_name}:{source_path} failed "
                + f"(exit {code}): {decode_output(stderr).strip()}"
            )
        target = Path(target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(stdout)

    @override
    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        code, stdout, stderr = await self._run(
            ["sh", "-c", DOWNLOAD_DIR_SCRIPT, "sh", source_dir]
        )
        if code == _MISSING_PATH_EXIT:
            raise FileNotFoundError(f"{self.pod_name}:{source_dir} is not a directory")
        if code != 0:
            raise RuntimeError(
                f"Download of {self.pod_name}:{source_dir} failed "
                + f"(exit {code}): {decode_output(stderr).strip()}"
            )
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(stdout), mode="r") as archive:
            archive.extractall(target, filter="data")
