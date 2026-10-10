# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retention capacity identities include the exact retained-source mapper code."""

import importlib
import json
from pathlib import Path

import pytest

from tests.test_provider_directory_cms_address import (
    _admission,
    _factory,
    _retention_policy,
)
from tests.test_provider_directory_cms_address import (
    address_build_case as address_build_case,
)

address = importlib.import_module("process.provider_directory_cms_address")
_MAPPING_DIGEST = address._mapping_digest


@pytest.mark.parametrize("module_name", address._REGISTRY_SOURCE_MODULES)
def test_retention_code_changes_invalidate_affected_inputs(address_build_case, monkeypatch, module_name):
    """Reject changed retention code and any ordinary mapping that shares it."""
    monkeypatch.setattr(address, "_mapping_digest", _MAPPING_DIGEST)
    ordinary = _factory(address_build_case)
    retained = ordinary.with_registry_source_retention(_retention_policy(address_build_case))
    target = Path(importlib.import_module(module_name).__file__)
    read_bytes = Path.read_bytes

    def changed_bytes(path):
        source = read_bytes(path)
        return source + b"\n# changed retention mapping\n" if path == target else source

    monkeypatch.setattr(Path, "read_bytes", changed_bytes)
    changed_ordinary = _factory(address_build_case)
    if module_name in address._MODULES:
        assert changed_ordinary.input_hash != ordinary.input_hash
        with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
            ordinary._assert_inputs(address_build_case[2], _admission(ordinary))
    else:
        assert changed_ordinary.input_json == ordinary.input_json
        assert ordinary._assert_inputs(address_build_case[2], _admission(ordinary))
    with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
        retained._assert_inputs(address_build_case[2], _admission(retained))


def test_retention_mapper_is_bound_to_the_existing_input_field(address_build_case, monkeypatch):
    monkeypatch.setattr(address, "_mapping_digest", _MAPPING_DIGEST)
    ordinary = _factory(address_build_case)
    retained = ordinary.with_registry_source_retention(_retention_policy(address_build_case))
    original = json.loads(ordinary.input_json)
    enrolled = json.loads(retained.input_json)
    assert set(enrolled) == set(original) | {"registry_source_retention"}
    assert enrolled["mapper_digest"] == address._mapping_digest(registry_source=True)
    assert enrolled["mapper_digest"] != original["mapper_digest"]
