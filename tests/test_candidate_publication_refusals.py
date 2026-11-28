# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate controls refuse changed custody, metadata and publication catalogs."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import sqlalchemy as sa

from db import migration_ptg2_v4_attempt_audit as attempt_audit
from db import models
from db.migration_adoption import _expected_foreign_key_identity
from db.migration_expression_adoption import _normalized_expression
from db.migration_ptg2_legacy_v3_guard_sql import common_attempt_guard_sql
from db.migration_ptg2_v4_attempt_fence import _LIFECYCLE_FUNCTION_BODY
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_validation as validation
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from process.ptg_parts.result_archive_published_authority import _authority_from_identity
from process.ptg_parts.result_archive_published_identity import validate_published_result_identity
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthority
from tests.ptg2_v4_attempt_migration_postgres_support import FENCE_MIGRATION, migration
from tests.test_ptg2_local_physical_read_view import _installed_fixture, _view_fixture
from tests.test_ptg2_local_preparation_authority import (
    _control_fixture,
    _ownership,
    _published_control_fixture,
)
from tests.test_ptg2_physical_binding import _binding
from tests.test_result_archive_candidate_preparation_postgres import _candidate_manifest, _frozen_params
from tests.test_result_archive_published_identity import _published_row


def _result(rows=(), scalar=None):
    result = MagicMock()
    result.__iter__.side_effect = lambda: iter(rows)
    result.mappings.return_value = result
    result.all.return_value = rows
    result.first.return_value = rows[0] if rows else None
    result.one_or_none.return_value = rows[0] if rows else None
    result.scalar_one.return_value = scalar
    result.scalar_one_or_none.return_value = scalar
    return result


def _session(*rows, scalars=()):
    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(side_effect=[_result(value) for value in rows]),
        scalar=AsyncMock(side_effect=scalars),
    )


def _cleanup_fixture(*, staged=False):
    physical_binding, candidate, _scope, initialized = _control_fixture()
    snapshot = "snapshot-archive-11111111-2222-4333-8444-555555555555"
    physical_binding = replace(physical_binding, snapshot_id=snapshot)
    initialized = replace(initialized, destination_snapshot_id=snapshot)
    candidate["snapshot_id"] = snapshot
    if staged:
        attributes = validation._candidate_attributes(
            candidate,
            source_key="source_a",
            serving_index=candidate["layout_manifest"]["serving_index"],
            exact_published=True,
        )
        candidate.update(attributes, run_status="validated", run_report=attributes["manifest"])
    evidence_by_field = {
        "ownership": {"dataset_id": str(physical_binding.dataset_id)},
        "data": {
            "payload_snapshot_id": physical_binding.payload_snapshot_id,
            "payload_snapshot_key": physical_binding.payload_snapshot_key,
        },
        "initialization": asdict(initialized),
        "control_sha256": initialization._local_control_sha256(
            candidate["manifest"], candidate["options"], (("12-3456789", "group"),)
        ),
    }
    if staged:
        evidence_by_field["activation_evidence"] = {"control_sha256": evidence_by_field["control_sha256"]}
    return physical_binding, candidate, evidence_by_field


@pytest.mark.parametrize("layout_key", [None, 1, 2**63 - 1, True, 0, -1, 2**63])
def test_cleanup_marker_preserves_payload_and_only_accepts_native_metadata_keys(layout_key):
    _physical, _candidate, evidence = _cleanup_fixture()
    evidence["initialization"]["destination_layout_key"] = layout_key
    if layout_key is not None and (type(layout_key) is not int or not 0 < layout_key < 2**63):
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="layout key differs"):
            initialization._local_cleanup_marker(evidence, evidence["initialization"])
    else:
        marker, observed = initialization._local_cleanup_marker(evidence, evidence["initialization"])
        assert observed == layout_key and marker["payload_snapshot_key"] == 19
        assert ("destination_layout_key" in marker) is (layout_key is not None)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 256, 257])
async def test_serving_source_dictionary_is_bounded_and_reports_complete_key_statistics(count):
    physical = _binding()
    rows = [{"source_key": index} for index in range(count)]
    session = _session(rows)
    if count in {0, 257}:
        with pytest.raises(native.PTG2PhysicalBindingError, match="source dictionary differs"):
            await validation._local_serving_source_identity(session, physical)
    else:
        result = await validation._local_serving_source_identity(session, physical)
        assert result["source_identity_rows"] == rows
        assert result["source_row_count"] == result["distinct_source_key_count"] == count
        assert result["minimum_source_key"] == 0 and result["maximum_source_key"] == count - 1
    assert "ORDER BY source_key LIMIT 257" in str(session.execute.await_args.args[0])


@pytest.mark.parametrize(
    "drift", [None, "snapshot", "contract", "audit-contract", "audit-identity", "audit-model", "content", "control"]
)
def test_read_authority_preserves_complete_protected_validation(drift):
    physical, authority = _view_fixture()
    evidence = authority["native_validation"]
    changes_by_drift = {
        "snapshot": (evidence["initialization"], "destination_snapshot_id", "other-snapshot"),
        "contract": (evidence, "contract", "other-contract"),
        "audit-contract": (evidence["native_audit"], "contract", "other-contract"),
        "audit-identity": (evidence["native_audit"], "identity", []),
        "audit-model": (evidence["native_audit"], "model_sha256", "0" * 64),
        "content": (evidence, "metadata_sha256", "not-a-checksum"),
        "control": (evidence["activation_evidence"], "control_sha256", "0" * 64),
    }
    if drift:
        target, field, value = changes_by_drift[drift]
        target[field] = value
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError, match="read data differs|evidence differs"):
            validation._local_read_authority_binding(authority, physical.snapshot_id, is_prepared=True)
    else:
        assert (
            validation._local_read_authority_binding(authority, physical.snapshot_id, is_prepared=True)[0] == physical
        )


@pytest.mark.parametrize("drift", [None, "layout", "schema"])
def test_installed_read_binding_rejoins_logical_publication_inventory(drift):
    physical, installed_by_field, _candidate = _installed_fixture()
    publication = installed_by_field["native_publication"]
    if drift == "layout":
        publication["destination_layout_key"] += 1
    elif drift == "schema":
        publication["schema_name"] = "other_snapshot"
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError):
            validation._local_read_authority_binding(installed_by_field, physical.snapshot_id, is_prepared=False)
    else:
        assert (
            validation._local_read_authority_binding(installed_by_field, physical.snapshot_id, is_prepared=False)[0]
            == physical
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared,count", [(False, 0), (True, 2), (True, 1)])
async def test_read_requires_one_row_from_fixed_named_view(prepared, count):
    row_by_field = {"destination_snapshot_id": "local-snapshot"}
    session = _session([row_by_field] * count)
    if count != 1:
        with pytest.raises(native.PTG2PhysicalBindingError, match="named read is unavailable"):
            await validation._local_read_authority(session, "local-snapshot", is_prepared=prepared)
    else:
        assert await validation._local_read_authority(session, "local-snapshot", is_prepared=prepared) == row_by_field
    session.scalar.assert_not_awaited()
    query = str(session.execute.await_args.args[0])
    assert "LIMIT 2" in query and ("prepared_physical_binding" in query) is prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction", [False, None])
async def test_qualified_payload_reader_requires_an_open_caller_transaction(monkeypatch, transaction):
    session = _session()
    session.in_transaction = (lambda: False) if transaction is False else None
    with pytest.raises(native.PTG2PhysicalBindingError, match="caller transaction"):
        await validation.local_data_physical_read_state(session, "local-snapshot", is_prepared=True)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_payload_reader_refuses_absent_control_before_serving(monkeypatch):
    physical, authority = _view_fixture()
    control = AsyncMock(return_value=None)
    monkeypatch.setattr(native, "_local_candidate_control", control)
    session = _session([authority], *[[] for _ in physical.relations])
    session.info = {}
    with pytest.raises(native.PTG2PhysicalBindingError, match="control is unavailable"):
        await validation.local_data_physical_read_state(session, physical.snapshot_id, is_prepared=True)
    assert session.info == {}
    control.assert_awaited_once()
    locks = [
        str(call.args[0]) for call in session.execute.await_args_list if str(call.args[0]).startswith("LOCK TABLE ONLY")
    ]
    assert len(locks) == len(physical.relations)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepared,witness", [(False, None), (True, None), (True, {"contract": "witness-contract", "provider_count": 2})]
)
async def test_serving_row_uses_the_pinned_payload_source_dictionary(monkeypatch, prepared, witness):
    physical, candidate, _evidence, _publication = _published_control_fixture()
    attested_by_field = {
        "attested_source_key": "source_a",
        "attested_coverage_scope_id": b"s" * 32,
        "attested_source_set_digest": b"r" * 32,
        "attested_audit_sample_digest": b"a" * 32,
    }
    candidate.update(attested_by_field)
    monkeypatch.setattr(
        validation, "local_data_physical_read_state", AsyncMock(return_value=({}, {}, physical, candidate))
    )
    session = _session(([witness] if witness else []) if prepared else [{"source_key": 0}])
    original_by_field = {"snapshot_id": physical.snapshot_id}
    row, observed = await validation.local_data_serving_row(
        session, physical.snapshot_id, original_by_field, is_prepared=prepared
    )
    assert observed == physical and original_by_field == {"snapshot_id": physical.snapshot_id}
    assert row["bound_snapshot_key"] == physical.destination_layout_key
    if prepared:
        assert row.get("persisted_witness_contract") == (witness["contract"] if witness else None)
        assert session.execute.await_args.args[1]["snapshot_key"] == physical.payload_snapshot_key
    else:
        assert row["source_identity_rows"] == [{"source_key": 0}]
        assert session.execute.await_args.args[1]["snapshot_id"] == physical.payload_snapshot_id
    assert all(row[key] == value for key, value in attested_by_field.items())
    assert physical.schema_name in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_installed_reader_checks_the_published_control_before_caching(monkeypatch):
    physical, installed_by_field, candidate = _installed_fixture()
    reader = AsyncMock(return_value=candidate)
    monkeypatch.setattr(native, "_local_candidate_control", reader)
    session = _session(
        [installed_by_field],
        *[[] for _ in physical.relations],
        [{"plan_id": "12-3456789", "plan_market_type": "group"}],
        scalars=[False],
    )
    session.info = {}
    assert (await validation.local_data_physical_read_state(session, physical.snapshot_id, is_prepared=False))[
        3
    ] == candidate
    assert session.info["ptg2_local_read_bindings"][physical.schema_name] == physical
    assert reader.await_args.kwargs == {"lock_controls": False}
