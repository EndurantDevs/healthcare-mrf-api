# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed source retention metadata and unchanged legacy candidate contracts."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_retention import (
    SUMMARY_KEY,
    RegistrySourceRetentionError,
    _require_source_retention_lock,
    registry_source_retention_sha256,
    validate_registry_source_retention_receipt,
    verify_registry_source_retention,
    verify_registry_source_retention_rows,
)


def retention_receipt(**changes):
    return {
        "component": SUMMARY_KEY,
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": "a" * 64,
        "source_rows": 2,
        "add_rows": 1,
        "remove_rows": 1,
        "suppressed_source_rows": 1,
        "effective_rows": 2,
        **changes,
    }


def candidate_receipt(receipt=None):
    receipt = retention_receipt() if receipt is None else receipt
    return {
        "source_generations": {SUMMARY_KEY: registry_source_retention_sha256(receipt)},
        "validation_json": {SUMMARY_KEY: receipt},
        "expected_rows": receipt["effective_rows"],
    }


def test_source_retention_canonical_roundtrip_and_legacy():
    receipt = retention_receipt()
    assert validate_registry_source_retention_receipt(json.dumps(receipt)) == receipt
    assert registry_source_retention_sha256(dict(reversed(list(receipt.items())))) == registry_source_retention_sha256(
        receipt
    )
    assert verify_registry_source_retention(candidate_receipt()) == receipt
    assert verify_registry_source_retention({"source_generations": {}, "validation_json": None}) is None
    assert (
        verify_registry_source_retention({"source_generations": "{}", "validation_json": '{"unrelated":true}'}) is None
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"component": "other"},
        {"revision": True},
        {"revision": 2},
        {"table_name": "network_membership"},
        {"source_generation_sha256": "A" * 64},
        {"source_rows": True},
        {"source_rows": -1},
        {"add_rows": 2**63},
        {"remove_rows": False},
        {"effective_rows": 3},
        {"suppressed_source_rows": 3},
        {"extra": 1},
    ],
)
def test_source_retention_rejects_invalid_closed_receipts(changes):
    with pytest.raises(RegistrySourceRetentionError):
        validate_registry_source_retention_receipt(retention_receipt(**changes))


def test_source_retention_rejects_missing_duplicate_and_oversized_fields():
    receipt = retention_receipt()
    for field in receipt:
        incomplete_by_field = dict(receipt)
        incomplete_by_field.pop(field)
        with pytest.raises(RegistrySourceRetentionError):
            validate_registry_source_retention_receipt(incomplete_by_field)
    duplicate = json.dumps(receipt)[:-1] + ', "revision": 1}'
    for invalid in (duplicate, " " * 4097, [], None):
        with pytest.raises(RegistrySourceRetentionError):
            validate_registry_source_retention_receipt(invalid)


@pytest.mark.parametrize("mutation", ["missing_hash", "missing_receipt", "hash", "expected", "count_bool"])
def test_source_retention_rejects_orphan_or_changed_candidate_metadata(mutation):
    candidate = candidate_receipt()
    if mutation == "missing_hash":
        candidate["source_generations"].clear()
    elif mutation == "missing_receipt":
        candidate["validation_json"].clear()
    elif mutation == "hash":
        candidate["source_generations"][SUMMARY_KEY] = "b" * 64
    else:
        candidate["expected_rows"] = True if mutation == "count_bool" else 3
    with pytest.raises(RegistrySourceRetentionError):
        verify_registry_source_retention(candidate)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_permitted", [False, True])
async def test_retention_read_lock_uses_select_privilege(is_permitted):
    failure = None if is_permitted else asyncpg.InsufficientPrivilegeError("SELECT required")
    connection = SimpleNamespace(execute=AsyncMock(side_effect=failure), fetchval=AsyncMock())
    if is_permitted:
        assert await _require_source_retention_lock(connection, '"snapshot".network_membership_source') is None
    else:
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as raised:
            await _require_source_retention_lock(connection, '"snapshot".network_membership_source')
        assert raised.value is failure
    connection.execute.assert_awaited_once_with('LOCK TABLE "snapshot".network_membership_source IN ACCESS SHARE MODE')
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("difference", [None, "count", "multiset"])
async def test_retention_reader_preserves_count_and_multiset(difference):
    candidate_id = UUID(int=4)
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=number)) for number in (1, 2, 3)),
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )
    fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    candidate = candidate_receipt() | {field: getattr(copy_target, field) for field in fields} | {"state": "sealed"}
    stage = RegistrySourceRecipeStage("registry_source_page_" + UUID(int=5).hex, 2, 0, "a" * 64)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetchval=AsyncMock(side_effect=[1 if difference == "count" else 2, difference == "multiset"]),
    )
    if difference is None:
        assert (
            await verify_registry_source_retention_rows(connection, copy_target, candidate, source_stage=stage)
            == retention_receipt()
        )
    else:
        refusal = "accounting_invalid" if difference == "count" else "parity_invalid"
        with pytest.raises(RegistrySourceRetentionError, match=refusal):
            await verify_registry_source_retention_rows(connection, copy_target, candidate, source_stage=stage)
    connection.execute.assert_awaited_once_with(
        f'LOCK TABLE "{copy_target.schema_name}".network_membership_source IN ACCESS SHARE MODE'
    )
    queries = [call.args[0] for call in connection.fetchval.await_args_list]
    assert queries[0] == f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership_source'
    if difference == "count":
        assert len(queries) == 1
    else:
        assert len(queries) == 2 and queries[1].count("EXCEPT ALL") == 2
