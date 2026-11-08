# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import inspect
import json
import os
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from db.models import FacilityAddressContribution, db
from process import facility_address_contribution_capture as capture
from process.address_numeric_grid_alias_revoke import revoke_numeric_grid_alias
from process.ext import address_canon

anchors = importlib.import_module("process.facility_anchors")


def test_capture_hook_precedes_alias_projection_and_uses_validated_inputs():
    source = inspect.getsource(address_canon.resolve_into_archive)
    assert source.index("if mismatch:") < source.index("await source_capture(")
    assert source.index("await source_capture(") < source.index("await _apply_persisted_address_aliases(")
    assert set(capture.CANONICAL_COLUMNS).isdisjoint(
        {"source_bits", "strict_source_bits", "lat", "long", "rn", "source_ctid"}
    )
    assert list(FacilityAddressContribution.__table__.primary_key.columns.keys()) == ["kind", "address_key"]


async def test_protected_adoption_rejected_before_shared_address_writes():
    source = inspect.getsource(anchors.publish_facility_anchors_generation)
    assert source.index("await require_local_family_publication(") < source.index("await resolve_into_archive(")
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[False, False, "facility-anchors", False, False]), execute=AsyncMock()
    )
    with pytest.raises(RuntimeError, match="protected native publication bridge"):
        await capture.require_local_family_publication(session, schema="mrf")


async def test_disabled_capture_is_explicit_and_does_not_read_aliases():
    calls = []

    async def execute(statement, params):
        calls.append((str(statement), params))

    await capture.capture_metadata(
        SimpleNamespace(execute=execute),
        schema="mrf",
        contribution_table="source_capture",
        enabled=False,
    )
    assert len(calls) == 1
    payload = json.loads(calls[0][1]["payload"])
    assert payload == {
        "contract": capture.CONTRACT,
        "enabled": False,
        "canon_version": address_canon.current_canon_version(),
        "alias_semantics": None,
        "source_bit": 8,
        "priority": 4,
    }


async def test_capture_rejects_excessive_artifact():
    async def execute(statement, params):
        assert params == {"max_rows": 2_000_000, "max_bytes": 16_384}
        return SimpleNamespace(scalar=lambda: True)

    with pytest.raises(RuntimeError, match="capture bounds"):
        await capture.require_capture_bounds(
            SimpleNamespace(execute=execute), schema="mrf", contribution_table="source_capture"
        )


def test_geocode_winner_query_is_materialized_only_once():
    query = capture.captured_geocode_cte(
        schema="mrf",
        contribution_table="source_capture",
        winner_sql="SELECT DISTINCT ON (address_key) address_key, lat, long FROM incoming",
    )
    assert query.count("SELECT DISTINCT ON") == 1
    assert "AS MATERIALIZED" in query
    assert "FROM captured_geocodes" in query
    assert "RETURNING address_key, payload" in query


async def _seed_aliased_facility_source(
    schema,
    stage,
    artifact,
    field_map,
    source_line="777 Source Way",
    target_line="888 Target Way",
    run_id="11111111-1111-4111-8111-111111111111",
):
    await db.status(
        f"CREATE TABLE {schema}.{stage} (address_key uuid, address_line1 text, city text, state text, zip_code text, latitude numeric, longitude numeric, facility_type text)"
    )
    await db.status(
        f"CREATE TABLE {schema}.{artifact} (kind varchar(16), address_key uuid, payload jsonb, PRIMARY KEY(kind,address_key))"
    )
    await db.status(
        f"INSERT INTO {schema}.{stage} VALUES (NULL,'{source_line}','Austin','TX','78702',30,-97,'Hospital'), (NULL,'{target_line}','Austin','TX','78702',31,-98,'Hospital')"
    )
    await address_canon.stamp_address_keys(stage, field_map, schema=schema, shards=1)
    await address_canon.resolve_into_archive(stage, field_map, schema=schema, source_bit=2, priority=1)
    await db.status(
        f"UPDATE {schema}.address_archive_v2 SET first_line='Accumulated target display', source_bits=6, strict_source_bits=6 WHERE address_key=(SELECT address_key FROM {schema}.{stage} WHERE address_line1='{target_line}')"
    )
    await db.status(
        f"INSERT INTO {schema}.address_alias_run_v1 (run_id,alias_kind,ruleset_version,mode,status) VALUES ('{run_id}','numeric_grid_direction_v1',1,'shadow','running')"
    )
    await db.status(f"""
        INSERT INTO {schema}.address_alias_v1 (
            source_address_key,source_identity_key,target_address_key,target_identity_key,
            alias_kind,ruleset_version,target_strict_source_bits,target_strict_source_count,
            candidate_count,shadow_run_id,apply_run_id,reviewed_candidate_digest)
        SELECT source.address_key,source.identity_key,target.address_key,target.identity_key,
            'numeric_grid_direction_v1',1,6,2,1,
            '{run_id}','{run_id}',repeat('a',64)
        FROM {schema}.address_archive_v2 source CROSS JOIN {schema}.address_archive_v2 target
        WHERE source.address_key=(SELECT address_key FROM {schema}.{stage} WHERE address_line1='{source_line}')
          AND target.address_key=(SELECT address_key FROM {schema}.{stage} WHERE address_line1='{target_line}')
    """)
    await db.status(f"DELETE FROM {schema}.{stage} WHERE address_line1='{target_line}'")
    await db.status(
        f"INSERT INTO {schema}.{stage} SELECT address_key,address_line1,city,state,zip_code,32,-99,facility_type FROM {schema}.{stage}"
    )


async def _assert_native_source_capture(schema, stage, artifact, field_map, source_line, monkeypatch):
    stats = await address_canon.resolve_into_archive(
        stage,
        field_map,
        schema=schema,
        source_bit=8,
        priority=4,
        source_capture=partial(capture.capture_canonical_observations, contribution_table=artifact),
    )
    assert stats.reason_buckets["persisted_aliases_applied"] == 1
    observation = await db.scalar(f"SELECT payload FROM {schema}.{artifact} WHERE kind='canonical'")
    assert observation["first_line"] == source_line
    assert set(observation) == set(capture.CANONICAL_COLUMNS)
    assert "Accumulated target display" not in json.dumps(observation)
    metadata = await db.scalar(f"SELECT payload FROM {schema}.{artifact} WHERE kind='metadata'")
    assert metadata["alias_semantics"]["active_alias_count"] == 1
    assert "local_generation" not in metadata["alias_semantics"]
    updated = await anchors._refresh_archive_geocodes_from_facility_anchors(stage, schema, contribution_table=artifact)
    assert updated == 1
    winner = await db.scalar(f"SELECT payload FROM {schema}.{artifact} WHERE kind='geocode'")
    archive_observation = await db.first(
        f"SELECT lat,long,strict_source_bits FROM {schema}.address_archive_v2 WHERE address_key=(SELECT address_key FROM {schema}.{stage} LIMIT 1)"
    )
    assert (float(archive_observation.lat), float(archive_observation.long)) == (
        float(winner["lat"]),
        float(winner["long"]),
    )
    assert archive_observation.strict_source_bits == 0
    await db.status(f"DELETE FROM {schema}.{artifact} WHERE kind='geocode'")
    await db.status(
        f"UPDATE {schema}.address_archive_v2 SET lat=NULL,long=NULL WHERE address_key=(SELECT address_key FROM {schema}.{stage} LIMIT 1)"
    )
    monkeypatch.setattr(capture, "MAX_ROWS", 0)
    with pytest.raises(RuntimeError, match="capture bounds"):
        await anchors._refresh_archive_geocodes_from_facility_anchors(stage, schema, contribution_table=artifact)
    assert await db.scalar(f"SELECT count(*) FROM {schema}.{artifact} WHERE kind='geocode'") == 0
    assert (
        await db.scalar(
            f"SELECT lat FROM {schema}.address_archive_v2 WHERE address_key=(SELECT address_key FROM {schema}.{stage} LIMIT 1)"
        )
        is None
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_native_source_capture_excludes_accumulated_alias_fields_and_reuses_geocode_winner(monkeypatch):
    if os.getenv("HP_FACILITY_CAPTURE_POSTGRES_TEST") != "1" or "test" not in os.getenv("HLTHPRT_DB_DATABASE", ""):
        pytest.skip("requires an explicitly enabled disposable migrated test database")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE", "false")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_ZIP_RESTORE_ENABLED", "false")
    schema = os.environ.get("HLTHPRT_DB_SCHEMA", "mrf")
    fixture_id = uuid4().hex
    stage = f"facility_capture_source_{fixture_id}"
    artifact = f"facility_capture_artifact_{fixture_id}"
    source_line = f"{int(fixture_id[:8], 16)} Source Way"
    target_line = f"{int(fixture_id[8:16], 16)} Target Way"
    run_id = uuid4()
    field_map = {
        "first_line": "address_line1",
        "second_line": "NULL",
        "city": "city",
        "state": "state",
        "zip": "zip_code",
        "country": "'US'",
    }
    try:
        await _seed_aliased_facility_source(schema, stage, artifact, field_map, source_line, target_line, run_id)
        await _assert_native_source_capture(schema, stage, artifact, field_map, source_line, monkeypatch)
    finally:
        try:
            alias = await db.first(
                f"SELECT source_address_key::text AS source_key, target_address_key::text AS target_key "
                f"FROM {schema}.address_alias_v1 WHERE apply_run_id=:run_id AND revoked_at IS NULL",
                run_id=run_id,
            )
            if alias is not None:
                await revoke_numeric_grid_alias(
                    schema=schema,
                    source_address_key=alias.source_key,
                    expected_target_address_key=alias.target_key,
                    reason="fixture cleanup",
                    reviewed_by="native-test",
                )
        finally:
            try:
                await db.status(f"DROP TABLE IF EXISTS {schema}.{artifact}, {schema}.{stage}")
            finally:
                await db.disconnect()
