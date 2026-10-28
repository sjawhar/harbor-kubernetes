"""Package metadata regression tests."""

from importlib.resources import files


def test_distribution_marks_harbor_kubernetes_as_typed() -> None:
    assert files("harbor_kubernetes").joinpath("py.typed").is_file()


def test_package_exports_environment_constructor_keyword_types() -> None:
    from harbor_kubernetes import (
        BaseEnvironmentKwargs,
        KubernetesEnvironmentInitKwargs,
        KubernetesEnvironmentKwargs,
    )

    assert "namespace" in KubernetesEnvironmentKwargs.__annotations__
    assert "logger" in BaseEnvironmentKwargs.__annotations__
    assert {
        "namespace",
        "logger",
    } <= KubernetesEnvironmentInitKwargs.__annotations__.keys()
