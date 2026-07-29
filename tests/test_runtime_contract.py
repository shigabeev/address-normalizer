from importlib.resources import files
from pathlib import Path

from address_normalizer import parse


FORBIDDEN_RUNTIME_IMPORTS = (
    "elasticsearch",
    "pandas",
    "numpy",
    "scipy",
    "sklearn",
    "torch",
    "tensorflow",
    "onnxruntime",
    "requests",
)


def test_model_is_bundled_and_small():
    model = files("address_normalizer").joinpath("data/model.json")
    assert model.is_file()
    assert model.stat().st_size < 1_000_000


def test_package_declares_inline_types():
    assert files("address_normalizer").joinpath("py.typed").is_file()


def test_runtime_has_no_forbidden_imports():
    package = Path(__file__).parents[1] / "src/address_normalizer"
    source = "\n".join(path.read_text() for path in package.rglob("*.py"))
    for dependency in FORBIDDEN_RUNTIME_IMPORTS:
        assert f"import {dependency}" not in source
        assert f"from {dependency}" not in source


def test_repeated_parsing_is_deterministic():
    raw = "Самара Авроры 7 12"
    assert parse(raw).as_dict() == parse(raw).as_dict()
