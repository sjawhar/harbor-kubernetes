from typing import Any, Final

KUBE_CONFIG_DEFAULT_LOCATION: Final[str]

class ConfigNode:
    value: Any

class KubeConfigMerger:
    paths: list[str]
    def __init__(self, paths: str) -> None: ...
    @property
    def config(self) -> ConfigNode | None: ...
