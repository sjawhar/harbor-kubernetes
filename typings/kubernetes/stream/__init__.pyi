from collections.abc import Callable

from kubernetes.stream.ws_client import WSClient

def stream(
    api_method: Callable[..., object], *args: object, **kwargs: object
) -> WSClient: ...
