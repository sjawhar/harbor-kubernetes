from collections.abc import Mapping
from datetime import datetime

class Configuration:
    def __init__(self) -> None: ...

class ApiClient:
    configuration: Configuration
    def __init__(self, configuration: Configuration | None = None) -> None: ...
    def close(self) -> None: ...

class ApiException(Exception):
    status: int | None
    reason: str | None
    body: str | bytes | None
    def __init__(
        self, status: int | None = None, reason: str | None = None
    ) -> None: ...

class V1ObjectMeta:
    name: str | None
    namespace: str | None
    labels: dict[str, str] | None
    annotations: dict[str, str] | None
    deletion_timestamp: datetime | None
    uid: str | None
    def __init__(
        self,
        *,
        name: str | None = None,
        namespace: str | None = None,
        labels: dict[str, str] | None = None,
        annotations: dict[str, str] | None = None,
        deletion_timestamp: datetime | None = None,
    ) -> None: ...

class V1EnvVar:
    name: str
    value: str | None
    def __init__(self, *, name: str, value: str | None = None) -> None: ...

class V1SecurityContext:
    run_as_user: int | None
    run_as_group: int | None
    def __init__(
        self, *, run_as_user: int | None = None, run_as_group: int | None = None
    ) -> None: ...

class V1ResourceRequirements:
    requests: dict[str, str] | None
    limits: dict[str, str] | None
    def __init__(
        self,
        *,
        requests: Mapping[str, str] | None = None,
        limits: Mapping[str, str] | None = None,
    ) -> None: ...

class V1VolumeMount:
    name: str
    mount_path: str
    read_only: bool | None
    def __init__(
        self, *, name: str, mount_path: str, read_only: bool | None = None
    ) -> None: ...

class V1ImageVolumeSource:
    reference: str | None
    pull_policy: str | None
    def __init__(
        self, *, reference: str | None = None, pull_policy: str | None = None
    ) -> None: ...

class V1Volume:
    name: str
    image: V1ImageVolumeSource | None
    def __init__(
        self, *, name: str, image: V1ImageVolumeSource | None = None
    ) -> None: ...

class V1Container:
    name: str
    image: str | None
    image_pull_policy: str | None
    args: list[str] | None
    command: list[str] | None
    env: list[V1EnvVar] | None
    security_context: V1SecurityContext | None
    resources: V1ResourceRequirements | None
    volume_mounts: list[V1VolumeMount] | None
    def __init__(
        self,
        *,
        name: str,
        image: str | None = None,
        image_pull_policy: str | None = None,
        args: list[str] | None = None,
        command: list[str] | None = None,
        env: list[V1EnvVar] | None = None,
        security_context: V1SecurityContext | None = None,
        resources: V1ResourceRequirements | None = None,
        volume_mounts: list[V1VolumeMount] | None = None,
    ) -> None: ...

class V1PodDNSConfig:
    nameservers: list[str] | None
    def __init__(self, *, nameservers: list[str] | None = None) -> None: ...

class V1PodSpec:
    containers: list[V1Container]
    restart_policy: str | None
    automount_service_account_token: bool | None
    enable_service_links: bool | None
    runtime_class_name: str | None
    dns_policy: str | None
    dns_config: V1PodDNSConfig | None
    volumes: list[V1Volume] | None
    def __init__(
        self,
        *,
        containers: list[V1Container],
        restart_policy: str | None = None,
        automount_service_account_token: bool | None = None,
        enable_service_links: bool | None = None,
        runtime_class_name: str | None = None,
        dns_policy: str | None = None,
        dns_config: V1PodDNSConfig | None = None,
        volumes: list[V1Volume] | None = None,
    ) -> None: ...

class V1ContainerStateWaiting:
    reason: str | None
    message: str | None
    def __init__(
        self, *, reason: str | None = None, message: str | None = None
    ) -> None: ...

class V1ContainerState:
    waiting: V1ContainerStateWaiting | None
    def __init__(self, *, waiting: V1ContainerStateWaiting | None = None) -> None: ...

class V1ContainerStatus:
    name: str
    image: str
    image_id: str
    ready: bool
    restart_count: int
    state: V1ContainerState | None
    def __init__(
        self,
        *,
        name: str,
        image: str,
        image_id: str,
        ready: bool,
        restart_count: int,
        state: V1ContainerState | None = None,
    ) -> None: ...

class V1PodStatus:
    phase: str | None
    reason: str | None
    message: str | None
    container_statuses: list[V1ContainerStatus] | None
    def __init__(
        self,
        *,
        phase: str | None = None,
        reason: str | None = None,
        message: str | None = None,
        container_statuses: list[V1ContainerStatus] | None = None,
    ) -> None: ...

class V1Pod:
    metadata: V1ObjectMeta | None
    spec: V1PodSpec | None
    status: V1PodStatus | None
    def __init__(
        self,
        *,
        api_version: str | None = None,
        kind: str | None = None,
        metadata: V1ObjectMeta | None = None,
        spec: V1PodSpec | None = None,
        status: V1PodStatus | None = None,
    ) -> None: ...

class V1DeleteOptions:
    grace_period_seconds: int | None
    propagation_policy: str | None
    def __init__(
        self,
        *,
        grace_period_seconds: int | None = None,
        propagation_policy: str | None = None,
    ) -> None: ...

class V1ServiceSpec:
    cluster_ip: str | None
    def __init__(self, *, cluster_ip: str | None = None) -> None: ...

class V1Service:
    spec: V1ServiceSpec | None
    def __init__(self, *, spec: V1ServiceSpec | None = None) -> None: ...

class V1NamespaceStatus:
    phase: str | None
    def __init__(self, *, phase: str | None = None) -> None: ...

class V1Namespace:
    metadata: V1ObjectMeta | None
    status: V1NamespaceStatus | None
    def __init__(
        self,
        *,
        metadata: V1ObjectMeta | None = None,
        status: V1NamespaceStatus | None = None,
    ) -> None: ...

class CoreV1Api:
    api_client: ApiClient
    def __init__(self, api_client: ApiClient | None = None) -> None: ...
    def create_namespaced_pod(self, namespace: str, body: V1Pod) -> V1Pod: ...
    def read_namespaced_pod(self, name: str, namespace: str) -> V1Pod: ...
    def delete_namespaced_pod(
        self, name: str, namespace: str, *, body: V1DeleteOptions | None = None
    ) -> object: ...
    def read_namespaced_service(self, name: str, namespace: str) -> V1Service: ...
    def create_namespace(self, body: V1Namespace) -> V1Namespace: ...
    def read_namespace(self, name: str) -> V1Namespace: ...
    def delete_namespace(self, name: str) -> object: ...
    def connect_get_namespaced_pod_exec(
        self, name: str, namespace: str, **kwargs: object
    ) -> object: ...

class CustomObjectsApi:
    def __init__(self, api_client: ApiClient | None = None) -> None: ...
    def create_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, body: object
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
        body: object,
    ) -> object: ...
    def patch_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: object,
    ) -> object: ...
    def delete_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> object: ...
    def list_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str
    ) -> dict[str, object]: ...
