"""Package metadata regression tests."""

from importlib.resources import files


def test_distribution_marks_harbor_kubernetes_as_typed() -> None:
    assert files("harbor_kubernetes").joinpath("py.typed").is_file()
