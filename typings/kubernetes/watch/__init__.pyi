from collections.abc import Callable, Iterator

class Watch:
    def stream(
        self, func: Callable[..., dict[str, object]], *args: object, **kwargs: object
    ) -> Iterator[dict[str, object]]: ...
    def stop(self) -> None: ...
