from kubernetes.client import Configuration

class ConfigException(Exception): ...

def load_incluster_config(
    client_configuration: Configuration | None = None, try_refresh_token: bool = True
) -> None: ...
def load_kube_config(
    config_file: str | None = None,
    context: str | None = None,
    client_configuration: Configuration | None = None,
    persist_config: bool = True,
) -> None: ...
