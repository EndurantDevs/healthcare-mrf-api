# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Isolated module loaders for hospital-price control tests."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def native_module() -> Any:
    return _load_path(
        "hospital_price_native_control_test",
        "process/hospital_price_native.py",
        {},
    )


def store_module() -> tuple[Any, Any]:
    native = native_module()
    fake_db = SimpleNamespace()
    replacement_by_name = {
        "db.models": _module("db.models", db=fake_db),
        "process.hospital_hpt_locator": _module(
            "process.hospital_hpt_locator",
            normalized_hospital_location_name=(
                lambda value: " ".join(value.split()).casefold()
            ),
        ),
        "process.hospital_price_acquisition": _module(
            "process.hospital_price_acquisition",
            REGISTRY_VERSION=1, Attempt=object, Candidate=object,
            schema_name=lambda: "mrf",
        ),
        "process.hospital_price_native": native,
        "process.ptg_parts.db_tables": _module(
            "process.ptg_parts.db_tables",
            _quote_ident=lambda value: f'"{value}"',
        ),
    }
    replacement_by_name["process.hospital_price_store_copy"] = _load_path(
        "process.hospital_price_store_copy",
        "process/hospital_price_store_copy.py",
        replacement_by_name,
    )
    return (
        _load_path(
            "hospital_price_store_control_test",
            "process/hospital_price_store.py",
            replacement_by_name,
        ),
        native,
    )


def acquisition_module() -> Any:
    native = native_module()
    locator = _load_path(
        "hospital_hpt_locator_control_test",
        "process/hospital_hpt_locator.py",
        {},
    )

    class HospitalPriceVersion:
        __table__ = SimpleNamespace(schema="mrf")

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    replacement_by_name = {
        "db.models": _module(
            "db.models", HospitalPriceVersion=HospitalPriceVersion,
            db=SimpleNamespace(),
        ),
        "process.control_cancel": _module(
            "process.control_cancel", ImportCancelledError=RuntimeError
        ),
        "process.hospital_hpt_locator": locator,
        "process.hospital_hpt_registry": _module(
            "process.hospital_hpt_registry",
            load_hospital_hpt_registry=lambda: (),
        ),
        "process.hospital_price_native": native,
        "process.ptg_parts.artifacts": _module(
            "process.ptg_parts.artifacts", PTG2ArtifactStore=object
        ),
        "process.ptg_parts.db_tables": _module(
            "process.ptg_parts.db_tables", _quote_ident=lambda value: value
        ),
        "process.ptg_parts.rust_scanner": _module(
            "process.ptg_parts.rust_scanner",
            _ptg2_rust_scanner_binary=lambda: None,
            _ptg2_scanner_binary_profile=lambda _path: "release",
            _subprocess_session_options=lambda _spawn: {},
            _terminate_asyncio_subprocess_group=noop,
        ),
        "process.ptg_parts.source_download": _module(
            "process.ptg_parts.source_download",
            PTG2_DEFAULT_MAX_BYTES=64 * 1024**3,
            download_raw_artifact=noop,
        ),
    }
    return _load_path(
        "hospital_price_acquisition_control_test",
        "process/hospital_price_acquisition.py",
        replacement_by_name,
    )


def _load_path(
    name: str, relative_path: str, replacements: dict[str, types.ModuleType]
) -> Any:
    prior_module_by_name = {
        module_name: sys.modules.get(module_name)
        for module_name in (name, *replacements)
    }
    sys.modules.update(replacements)
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        for module_name, prior_module in prior_module_by_name.items():
            if prior_module is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = prior_module


def _module(name: str, **attributes: Any) -> types.ModuleType:
    module = types.ModuleType(name)
    for attribute, value in attributes.items():
        setattr(module, attribute, value)
    return module
