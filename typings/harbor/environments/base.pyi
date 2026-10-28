import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path, PurePath
from typing import Literal

from pydantic import BaseModel

from harbor.environments.capabilities import (
    EnvironmentCapabilities,
    EnvironmentResourceCapabilities,
)
from harbor.models.task.config import EnvironmentConfig, NetworkPolicy, TpuSpec
from harbor.models.trial.config import ResourceMode, ServiceVolumeConfig
from harbor.models.trial.paths import TrialPaths


EnvironmentPath = str | PurePath


class ExecResult(BaseModel):
    stdout: str | None
    stderr: str | None
    return_code: int


class BaseEnvironment(ABC):
    environment_dir: Path
    environment_name: str
    session_id: str
    trial_paths: TrialPaths
    task_env_config: EnvironmentConfig
    logger: logging.Logger
    default_user: str | int | None
    _mounts: list[ServiceVolumeConfig]
    _phase_network_policies: list[NetworkPolicy]
    def __init__(
        self,
        environment_dir: Path,
        environment_name: str,
        session_id: str,
        trial_paths: TrialPaths,
        task_env_config: EnvironmentConfig,
        logger: logging.Logger | None = None,
        override_cpus: int | None = None,
        override_memory_mb: int | None = None,
        override_storage_mb: int | None = None,
        override_gpus: int | None = None,
        override_tpu: TpuSpec | None = None,
        cpu_enforcement_policy: ResourceMode = ...,
        memory_enforcement_policy: ResourceMode = ...,
        persistent_env: dict[str, str] | None = None,
        mounts: list[ServiceVolumeConfig] | None = None,
        network_policy: NetworkPolicy | None = None,
        phase_network_policies: Sequence[NetworkPolicy] | None = None,
        extra_docker_compose: Sequence[Path | str] | None = None,
    ) -> None: ...
    @property
    def network_policy(self) -> NetworkPolicy: ...
    def _startup_env(self) -> dict[str, str]: ...
    def _resource_request_value(
        self, resource: Literal["cpu", "memory"], *, auto_mode: ResourceMode
    ) -> int | None: ...
    def _resource_limit_value(
        self, resource: Literal["cpu", "memory"], *, auto_mode: ResourceMode
    ) -> int | None: ...
    @property
    def _effective_storage_mb(self) -> int | None: ...
    def _resolve_user(self, user: str | int | None) -> str | int | None: ...
    def _merge_env(self, env: dict[str, str] | None) -> dict[str, str] | None: ...
    async def ensure_dirs(
        self, dirs: Sequence[EnvironmentPath], *, chmod: bool = True
    ) -> ExecResult | None: ...
    def _mount_targets(self, *, writable_only: bool = False) -> list[str]: ...
    @staticmethod
    @abstractmethod
    def type() -> str: ...
    @property
    def capabilities(self) -> EnvironmentCapabilities: ...
    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities | None: ...
    @abstractmethod
    def _validate_definition(self) -> None: ...
    def validate_network_policy_support(
        self, network_policy: NetworkPolicy | None = None
    ) -> None: ...
    async def set_network_policy(self, network_policy: NetworkPolicy) -> None: ...
    async def _apply_network_policy(self, network_policy: NetworkPolicy) -> None: ...
    @classmethod
    def preflight(cls) -> None: ...
    async def _upload_environment_dir_after_start(self) -> None: ...
    @abstractmethod
    async def start(self, force_build: bool) -> None: ...
    @abstractmethod
    async def stop(self, delete: bool) -> None: ...
    @abstractmethod
    async def upload_file(self, source_path: Path | str, target_path: str) -> None: ...
    @abstractmethod
    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None: ...
    @abstractmethod
    async def download_file(self, source_path: str, target_path: Path | str) -> None: ...
    @abstractmethod
    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None: ...
    @abstractmethod
    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult: ...
