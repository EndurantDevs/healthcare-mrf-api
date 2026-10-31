# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS evidence comparison cannot turn a common enrollment into authority."""

import json
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_preparation_custody as custody
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

_NATIVE_OBSERVE = custody._observe


@pytest.fixture
def evidence(monkeypatch):
    policy = GenericSourcePolicy("private_source", "preparation_owner", "preparation_publisher", ("ordinary_reader",))
    storage = FHIRSourceStorageBinding("catalog", policy.schema, 1, 2, 3, 4, 5, "a" * 64)
    actor_by_field = dict(
        session=policy.publisher_role,
        current=policy.owner_role,
        backend=91,
        database_oid=1,
        replication="origin",
        transaction="1/1",
    )
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        get_server_pid=Mock(return_value=91),
        fetchrow=AsyncMock(side_effect=lambda *args: actor_by_field.copy()),
    )
    observed_by_field = {field: str(index) * 64 for index, field in enumerate(custody._DIGEST_FIELDS, 1)}
    observe = AsyncMock(return_value=observed_by_field)
    monkeypatch.setattr(custody, "_observe", observe)
    profile_by_field = dict(
        contract=custody.CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(storage),
        policy=json.loads(json.dumps(asdict(policy))),
        **observed_by_field,
    )
    return SimpleNamespace(
        policy=policy,
        storage=storage,
        actor=actor_by_field,
        connection=connection,
        observed=observed_by_field,
        observe=observe,
        profile=profile_by_field,
    )


async def _compare(case, profile):
    return await custody.compare_cms_preparation_custody(
        case.connection,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=profile,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", [None, {}, {"version": 2, "publication": {}}, {"document": {}, "sha256": "a" * 64}])
async def test_common_enrollment_or_handoff_is_refused_before_any_sql(evidence, profile):
    with pytest.raises(ValueError, match="cms_profile_required"):
        await _compare(evidence, profile)
    evidence.connection.fetchrow.assert_not_awaited()
    evidence.observe.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["contract", "source", "extra", "coordinate", "policy", "digest", "digest_type"])
async def test_closed_cms_scope_refuses_before_any_sql(evidence, change):
    profile = deepcopy(evidence.profile)
    match change:
        case "contract":
            profile["contract"] = "generic-source-detached-v1"
        case "source":
            profile["source_ids"] = ["ordinary-source"]
        case "extra":
            profile["expected_queued_runs"] = []
        case "coordinate":
            profile["storage"]["owner_oid"] = 99
        case "policy":
            profile["policy"]["owner_role"] = "source_owner"
        case "digest":
            profile["roles_sha256"] = "x" * 64
        case "digest_type":
            profile["roles_sha256"] = True
    with pytest.raises(ValueError, match="cms_preparation_custody_"):
        await _compare(evidence, profile)
    evidence.connection.fetchrow.assert_not_awaited()
    evidence.observe.assert_not_awaited()


@pytest.mark.asyncio
async def test_comparison_returns_only_evidence_and_does_not_create_finalizer(evidence):
    assert await _compare(evidence, evidence.profile) == evidence.observed
    assert evidence.connection.fetchrow.await_count == 2
    evidence.observe.assert_awaited_once_with(evidence.connection, evidence.storage, evidence.policy)
    assert not hasattr(custody, "finalize_frozen_cms_source_candidate")
    assert not hasattr(custody, "read_frozen_cms_source_candidate")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("session", "source_publisher"),
        ("current", "source_owner"),
        ("backend", 92),
        ("database_oid", 2),
        ("replication", "replica"),
        ("transaction", None),
    ],
)
async def test_replaced_native_actor_refuses_before_catalog_scan(evidence, field, value):
    evidence.actor[field] = value
    with pytest.raises(ValueError, match="original_publisher_required"):
        await _compare(evidence, evidence.profile)
    evidence.observe.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("digest", custody._DIGEST_FIELDS)
async def test_any_native_layout_guard_acl_or_role_drift_refuses(evidence, digest):
    evidence.observe.return_value = {**evidence.observed, digest: "9" * 64}
    with pytest.raises(ValueError, match="native_evidence_changed"):
        await _compare(evidence, evidence.profile)


@pytest.mark.asyncio
async def test_original_transaction_must_survive_scan(evidence):
    evidence.connection.fetchrow.side_effect = [evidence.actor.copy(), {**evidence.actor, "transaction": "1/2"}]
    with pytest.raises(ValueError, match="original_publisher_changed"):
        await _compare(evidence, evidence.profile)


@pytest.mark.asyncio
async def test_caller_cannot_rewrite_expected_pin_during_native_scan(evidence):
    async def changed(*arguments):
        evidence.profile["canonical_sha256"] = "9" * 64
        return {**evidence.observed, "canonical_sha256": "9" * 64}

    evidence.observe.side_effect = changed
    with pytest.raises(ValueError, match="native_evidence_changed"):
        await _compare(evidence, evidence.profile)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("database_oid", 99),
        ("catalog_schema_oid", 99),
        ("storage_schema_oid", 99),
        ("namespace_owner_oid", 99),
        ("owner_oid", 4),
        ("owner_login", True),
        ("publisher_login", False),
    ],
)
async def test_prepared_actor_and_coordinates_are_distinct_from_source_before_catalog_io(evidence, field, value):
    coordinates_by_field = dict(
        database_oid=1,
        catalog_schema_oid=2,
        storage_schema_oid=3,
        namespace_owner_oid=5,
        owner_oid=6,
        owner_login=False,
        publisher_login=True,
    )
    coordinates_by_field[field] = value
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=coordinates_by_field), fetch=AsyncMock())
    with pytest.raises(ValueError, match="protected_coordinates_changed"):
        await _NATIVE_OBSERVE(connection, evidence.storage, evidence.policy)
    connection.fetch.assert_not_awaited()


def _callees(signatures):
    return [
        dict(signature=signature, oid=1000 + index, function=True, overloads=1)
        for index, signature in enumerate(signatures)
    ]


@pytest.mark.asyncio
async def test_fixed_frontier_keeps_qualified_serving_and_transitive_callees():
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=lambda query, signatures: _callees(signatures)))
    rows = await custody._callee_frontier(connection, "catalog")
    signatures = {row["signature"] for row in rows}
    assert {
        '"catalog"."cms_serving_archive_result_matches"(pg_catalog.jsonb)',
        '"catalog"."cms_serving_native_matches"(pg_catalog.jsonb)',
        '"catalog"."cms_serving_native_snapshot"()',
        '"catalog"."cms_serving_oids"(pg_catalog.text[])',
        '"catalog"."provider_directory_subset_terminal_disposition_v5_valid"(pg_catalog.text)',
        '"catalog"."pd_dataset_candidate_tables"(pg_catalog.text)',
        '"catalog"."provider_directory_endpoint_dataset_admission_metadata_sha256"'
        "(pg_catalog.jsonb,pg_catalog.int2,pg_catalog.text,pg_catalog.text,pg_catalog.varchar[])",
    } <= signatures
    assert len(signatures) == len(custody._CALLEE_FRONTIER) == 32
    connection.fetch.assert_awaited_once_with(custody._CALLEES_SQL, [row["signature"] for row in rows])


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("missing", "absent", "overload", "procedure", "duplicate"))
async def test_fixed_callee_lookup_rejects_absent_or_ambiguous_native_identity(fault):
    async def fetch(query, signatures):
        rows = _callees(signatures)
        match fault:
            case "missing":
                rows.pop()
            case "absent":
                rows[0]["oid"] = None
            case "overload":
                rows[0]["overloads"] = 2
            case "procedure":
                rows[0]["function"] = False
            case "duplicate":
                rows[0]["oid"] = rows[1]["oid"]
        return rows

    with pytest.raises(ValueError, match="callees_missing_or_ambiguous"):
        await custody._callee_frontier(SimpleNamespace(fetch=AsyncMock(side_effect=fetch)), "catalog")


@pytest.mark.asyncio
async def test_callee_replacement_during_routine_scan_is_refused(evidence, monkeypatch):
    coordinates_by_field = dict(
        database_oid=1,
        catalog_schema_oid=2,
        storage_schema_oid=3,
        namespace_owner_oid=5,
        owner_oid=6,
        owner_login=False,
        publisher_login=True,
    )
    canonical_relations = [dict(oid=10, nspname="catalog", relname="canonical")]
    private_relations = [dict(oid=11, nspname="private_source", relname="private")]
    inventory = AsyncMock(side_effect=[canonical_relations, private_relations, canonical_relations, private_relations])
    monkeypatch.setattr(custody, "_inventory", inventory)
    frontier_rows = [dict(oid=1000)]
    lookup = AsyncMock(side_effect=[frontier_rows, [dict(oid=1001)]])
    monkeypatch.setattr(custody, "_callee_frontier", lookup)
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=coordinates_by_field),
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=[dict(oid=1000, native={"body": "old callee"})]),
    )
    with pytest.raises(ValueError, match="callees_changed"):
        await _NATIVE_OBSERVE(connection, evidence.storage, evidence.policy)
    connection.fetch.assert_awaited_once_with(custody._ROUTINES_SQL, [10, 11], [1000])
