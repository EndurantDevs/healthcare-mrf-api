# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete source accounting, immutable replay and atomic invalid-ledger rejection."""

import csv
import hashlib
import io
import json
from dataclasses import replace
from uuid import uuid4

import pytest

from process import registry_required_target_store as store
from process.registry_required_target_store import RegistryRequiredTargetEdition, admit_registry_required_targets
from tests.test_network_serving_schema_postgres import serving_schema

pytestmark = pytest.mark.asyncio
_HEADERS = (
    "COMPANY_ALIAS",
    "COMPANY_NAME",
    "PLAN_NAME",
    "PLAN_TYPE",
    "PLAN_CARRIER",
    "FIND_CARE_NETWORK_NAME",
    "NETWORK_FC_ID",
    "RIBBON_IDS",
)


def _input(rows=2, suffix=""):
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(_HEADERS)
    for ordinal in range(rows):
        writer.writerow(
            (
                f'Example, "Company" {ordinal}{suffix}',
                " Example Company ",
                "Example Plan",
                "PPO",
                "Example Carrier",
                "Example Network",
                "123",
                '["12345678-1234-5678-8123-123456789abc"]',
            )
        )
    return output.getvalue().encode()


def _edition(input_bytes):
    return RegistryRequiredTargetEdition(
        uuid4(), "https://example.test/required-networks.csv", hashlib.sha256(input_bytes).hexdigest()
    )


async def _admit(connection, schema, input_bytes, edition):
    async with connection.transaction():
        return await admit_registry_required_targets(connection, input_bytes, edition, control_schema=schema)


async def test_all_7338_rows_survive_one_physical_artifact_and_replay(serving_schema):
    connection, schema, _ = serving_schema
    await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=17,approved_revision=5')
    input_bytes = _input(7338)
    edition = _edition(input_bytes)
    receipt = await _admit(connection, schema, input_bytes, edition)
    assert (receipt["source_rows"], receipt["target_count"], receipt["physical_records"], receipt["replayed"]) == (
        7338,
        1,
        1,
        False,
    )
    document = json.loads(
        await connection.fetchval(
            f'SELECT observation_json::text FROM "{schema}".registry_source_observation WHERE snapshot_id=$1',
            edition.snapshot_id,
        )
    )
    ledger = document["ledger"]
    assert len(ledger["observations"]) == 7338
    assert ledger["observations"][-1]["source_row_ordinal"] == 7338
    assert ledger["observations"][-1]["raw_cells"][0] == 'Example, "Company" 7337'
    assert ledger["observations"][-1]["raw_cells"][1] == " Example Company "
    assert ledger["targets"][0]["source_row_ordinals"] == list(range(1, 7339))
    assert ledger["source_sha256"] == edition.input_sha256
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert hashlib.sha256(canonical).hexdigest() == receipt["artifact_sha256"]
    replay = await _admit(connection, schema, input_bytes, edition)
    assert replay == {**receipt, "replayed": True}
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity') == 0
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (17, 5)
    assert await connection.fetchval(f'SELECT generation_id FROM "{schema}".network_serving_control') is None


async def test_invalid_final_csv_record_leaves_caller_and_old_ledger_intact(serving_schema):
    connection, schema, _ = serving_schema
    input_bytes = _input()
    edition = _edition(input_bytes)
    await _admit(connection, schema, input_bytes, edition)
    invalid = input_bytes + b'"unterminated final record'
    async with connection.transaction():
        await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=9')
        with pytest.raises(store.RegistryRequiredTargetError, match="registry_required_target_input_invalid"):
            await admit_registry_required_targets(connection, invalid, _edition(invalid), control_schema=schema)
        assert await connection.fetchval("SELECT 42") == 42
        assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 9
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 1


async def test_immutable_edition_and_artifact_conflicts_never_overwrite(serving_schema):
    connection, schema, _ = serving_schema
    input_bytes = _input()
    edition = _edition(input_bytes)
    await _admit(connection, schema, input_bytes, edition)
    with pytest.raises(store.RegistryRequiredTargetError, match="registry_required_target_edition_conflict"):
        await _admit(connection, schema, input_bytes, replace(edition, source_url="https://example.test/other.csv"))
    assert (
        await connection.fetchval(f'SELECT source_url FROM "{schema}".registry_source_snapshot') == edition.source_url
    )
    await connection.execute(
        f"UPDATE \"{schema}\".registry_source_observation SET observation_json=jsonb_set(observation_json,'{{ledger,row_count}}','99') WHERE snapshot_id=$1",
        edition.snapshot_id,
    )
    with pytest.raises(store.RegistryRequiredTargetError, match="registry_required_target_artifact_conflict"):
        await _admit(connection, schema, input_bytes, edition)
    assert (
        await connection.fetchval(
            f"SELECT observation_json->'ledger'->>'row_count' FROM \"{schema}\".registry_source_observation"
        )
        == "99"
    )
    assert await connection.fetchval("SELECT 42") == 42


async def test_changed_csv_retains_both_editions_without_advancing_custom_revision(serving_schema):
    connection, schema, _ = serving_schema
    first = _input()
    second = _input(suffix=" updated")
    await _admit(connection, schema, first, _edition(first))
    await _admit(connection, schema, second, _edition(second))
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 2
    assert (
        await connection.fetchval(
            f"SELECT sum((observation_json->'ledger'->>'row_count')::integer) FROM \"{schema}\".registry_source_observation"
        )
        == 4
    )
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)


async def test_copy_descriptor_mismatch_rolls_back_private_landing(serving_schema, monkeypatch):
    connection, schema, _ = serving_schema
    input_bytes = _input()
    edition = _edition(input_bytes)
    original = store._encode

    def changed_count(input_bytes, edition):
        copy_bytes, descriptor = original(input_bytes, edition)
        return copy_bytes, {**descriptor, "source_rows": 3}

    monkeypatch.setattr(store, "_encode", changed_count)
    async with connection.transaction():
        with pytest.raises(store.RegistryRequiredTargetError, match="registry_required_target_landing_invalid"):
            await admit_registry_required_targets(connection, input_bytes, edition, control_schema=schema)
        assert await connection.fetchval("SELECT 42") == 42
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_required_targets_%'"
            )
            == 0
        )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 0
