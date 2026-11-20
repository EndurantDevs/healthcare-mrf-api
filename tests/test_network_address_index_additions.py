# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional indexes do not change the logical data of a retained address edition."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import network_address_artifact as artifact
from process import network_compact_dependencies as dependencies
from process import network_compact_readiness as compact_readiness
from process import network_indexed_address_artifact as indexed
from process import network_membership_serving_indexes as serving_indexes
from process.network_compact_readiness import NetworkCompactReadinessError
from process.network_membership_serving_indexes import NetworkServingIndexesError
from tests.test_network_indexed_address_artifact import _descriptor, _named_descriptor


def _changed_diagnostic(descriptor):
    changed = deepcopy(descriptor)
    changed["index_definition_sha256"] = "f" * 64
    changed["descriptor_sha256"] = artifact._digest(
        {name: entry for name, entry in changed.items() if name != "descriptor_sha256"}
    )
    return changed


@pytest.mark.parametrize("encoding", ["legacy", "named"])
def test_index_diagnostics_do_not_identify_address_data(encoding):
    saved = _descriptor() if encoding == "legacy" else _named_descriptor()
    changed = _changed_diagnostic(saved)
    before = deepcopy((saved, changed))

    assert indexed.address_dependency_data(saved) == indexed.address_dependency_data(changed)

    assert (saved, changed) == before
    changed["generation_id"] = "c" * 64
    changed["descriptor_sha256"] = artifact._digest(
        {name: entry for name, entry in changed.items() if name != "descriptor_sha256"}
    )
    assert indexed.address_dependency_data(saved) != indexed.address_dependency_data(changed)


def test_legacy_local_catalog_fields_remain_ignored():
    saved = _descriptor()
    copied = deepcopy(saved)
    for name in ("database", "protected_owner_oid", "table", "catalog", "runtime_roles", "dependency_oids"):
        copied[name] = None
    copied["namespace"] = {"name": saved["namespace"]["name"], "oid": None, "owner_oid": None}
    before = deepcopy(copied)

    assert indexed.address_dependency_data(copied) == indexed.address_dependency_data(saved)

    assert copied == before


@pytest.mark.asyncio
async def test_missing_required_index_still_refuses_address_use(monkeypatch):
    descriptor = _named_descriptor()
    readiness = AsyncMock(side_effect=NetworkServingIndexesError("required index is missing"))
    monkeypatch.setattr(indexed, "_index_readiness", readiness)
    monkeypatch.setattr(indexed.bootstrap, "verify_bootstrap_address_artifact", AsyncMock())

    with pytest.raises(NetworkServingIndexesError, match="required index is missing"):
        await indexed.verify_indexed_address_artifact(object(), descriptor)

    readiness.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_generation", [False, True])
async def test_compact_recapture_distinguishes_indexes_from_data(monkeypatch, changed_generation):
    from process import network_compact_address_origin as origin

    saved = _named_descriptor()
    captured = _changed_diagnostic(saved)
    if changed_generation:
        captured["generation_id"] = "c" * 64
        captured["descriptor_sha256"] = artifact._digest(
            {name: entry for name, entry in captured.items() if name != "descriptor_sha256"}
        )
    candidate_by_field = {
        "validation_json": {
            "address_source": {
                "schema_name": saved["namespace"]["name"],
                "table_name": "entity_address_unified",
                "generation_id": saved["generation_id"],
            },
            "serving_readiness": {"address_artifact": saved},
        }
    }
    before = deepcopy(candidate_by_field)
    base = SimpleNamespace(schema_name=saved["namespace"]["name"], table_name="entity_address_unified")
    monkeypatch.setattr(origin, "resolve_compact_address_origin", AsyncMock(return_value=(captured, base)))
    layout = SimpleNamespace(base=base)
    validate_layout = AsyncMock(return_value=layout)
    monkeypatch.setattr(dependencies, "_verified_layout", validate_layout)

    if changed_generation:
        with pytest.raises(NetworkCompactReadinessError):
            await dependencies.capture_compact_dependencies(object(), candidate_by_field, control_schema="registry")
        validate_layout.assert_not_awaited()
    else:
        assert (
            await dependencies.capture_compact_dependencies(object(), candidate_by_field, control_schema="registry")
            is layout
        )
        validate_layout.assert_awaited_once()
    assert candidate_by_field == before


def _index_record(name, valid=True):
    return {
        "relname": name,
        "indisvalid": valid,
        "indisready": valid,
        "indislive": True,
        "index_definition": "CREATE INDEX " + name,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("optional", sorted(serving_indexes._OPTIONAL_ADDRESS_INDEXES))
@pytest.mark.parametrize("optional_state", ["missing", "ready", "building"])
async def test_optional_address_index_is_not_a_retained_profile_requirement(monkeypatch, optional, optional_state):
    required = _index_record("required_address_lookup")
    records = AsyncMock(return_value=[required])
    monkeypatch.setattr(serving_indexes, "_serving_index_records", records)
    index_specs = [(required["relname"], "required DDL"), (optional, "optional DDL")]
    baseline = await serving_indexes._index_digest(object(), "example", index_specs)
    if optional_state != "missing":
        records.return_value = [required, _index_record(optional, valid=optional_state == "ready")]
    assert await serving_indexes._index_digest(object(), "example", index_specs) == baseline


@pytest.mark.asyncio
@pytest.mark.parametrize("required_state", ["missing", "invalid"])
async def test_optional_count_index_does_not_relax_existing_profile_requirements(monkeypatch, required_state):
    required = _index_record("required_address_lookup", valid=False)
    records = [_index_record(serving_indexes._OPTIONAL_SOURCE_COUNT_INDEX, valid=False)]
    if required_state == "invalid":
        records.append(required)
    monkeypatch.setattr(serving_indexes, "_serving_index_records", AsyncMock(return_value=records))
    with pytest.raises(NetworkServingIndexesError):
        await serving_indexes._index_digest(object(), "example", [(required["relname"], "required DDL")])


@pytest.mark.asyncio
@pytest.mark.parametrize("optional", sorted(serving_indexes._OPTIONAL_ADDRESS_INDEXES))
@pytest.mark.parametrize("optional_table", ["entity_address_unified", "network_office_edge"])
async def test_compact_optional_index_exception_is_confined_to_the_address_heap(monkeypatch, optional, optional_table):
    required_index_by_field = {
        "table_name": "network_office_edge",
        "index_name": "edge_lookup",
        "indisvalid": True,
        "indisready": True,
        "indislive": True,
    }
    paths = Mock()
    monkeypatch.setattr(compact_readiness, "_require_index_paths", paths)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=[required_index_by_field]))
    baseline = await compact_readiness.compact_index_digest(connection, "example")
    optional_index_by_field = {
        **required_index_by_field,
        "table_name": optional_table,
        "index_name": optional,
        "indisvalid": False,
        "indisready": False,
    }
    connection.fetchval.return_value = [required_index_by_field, optional_index_by_field]
    if optional_table == "entity_address_unified":
        assert await compact_readiness.compact_index_digest(connection, "example") == baseline
        paths.assert_called_with([required_index_by_field], None)
    else:
        with pytest.raises(NetworkCompactReadinessError):
            await compact_readiness.compact_index_digest(connection, "example")
