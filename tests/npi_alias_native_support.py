# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic native witnessed-alias cases collected by the archive database lane."""

import asyncio
import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import mrf_address_publication as canonical
from process import npi_alias_witness as witnesses
from process import npi_isolated_addresses as addresses
from process import npi_owned_publication as owned
from process import npi_result_archive as archive
from process.npi_isolated_execution import INPUT_NAMES, NpiInputRelation
from tests.npi_native_source_support import create_native_npi_family
from tests.npi_owned_publication_postgres_support import _builder_output
from tests.test_npi_alias_addresses import _key
from tests.test_npi_result_archive_postgres import _dump_npi_stage, _native_archive_environment


async def _inputs(runtime, output, *, strict_exists):
    schema = "alias_inputs_" + uuid4().hex
    runtime.schemas.add(schema)
    strict_key, effective_key = _key("100 OAK ST"), _key("102 OAK ST")
    async with runtime.administrator.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.execute(
            text(
                f'CREATE TABLE "{schema}".address_archive_v2 (LIKE "{output.schema_name}".npi_canonical_address INCLUDING ALL)'
            )
        )
        await connection.execute(text(f'ALTER TABLE "{schema}".address_archive_v2 ADD PRIMARY KEY(address_key)'))
        await connection.execute(
            text(
                f'CREATE TABLE "{schema}".address_alias_v1(source_address_key uuid PRIMARY KEY,source_identity_key text NOT NULL,target_address_key uuid NOT NULL,target_identity_key text NOT NULL,revoked_at timestamptz)'
            )
        )
        await connection.execute(
            text(
                f'CREATE TABLE "{schema}".address_alias_state_v1(singleton boolean PRIMARY KEY,schema_version integer NOT NULL,active_ruleset_version integer NOT NULL,generation bigint NOT NULL)'
            )
        )
        await connection.execute(text(f'INSERT INTO "{schema}".address_alias_state_v1 VALUES(true,2,1,3)'))
        for street in ("102 OAK ST", "100 OAK ST") if strict_exists else ("102 OAK ST",):
            key = _key(street)
            identity = addresses.canon.identity_key_v1(street, None, "EXAMPLE", "PA", "19000", "US")
            await connection.execute(
                text(
                    f"INSERT INTO \"{schema}\".address_archive_v2(address_key,identity_key,line1_norm,unit_norm,city_norm,state_code,zip5,country_code,first_line,city_name,state_name,postal_code,source_bits,strict_source_bits,display_priority,first_seen_at,last_seen_at,formatted_address,formatted_address_version) VALUES(:key,:identity,:line,'','EXAMPLE','PA','19000','US',:line,'EXAMPLE','PA','19000',16,16,5,'2020-01-01','2020-01-02',:line,1)"
                ),
                {"key": key, "identity": identity, "line": street},
            )
        await connection.execute(
            text(f'INSERT INTO "{schema}".address_alias_v1 VALUES(:s,:si,:t,:ti,NULL)'),
            {
                "s": strict_key,
                "si": addresses.canon.identity_key_v1("100 OAK ST", None, "EXAMPLE", "PA", "19000", "US"),
                "t": effective_key,
                "ti": addresses.canon.identity_key_v1("102 OAK ST", None, "EXAMPLE", "PA", "19000", "US"),
            },
        )
        for role in (runtime.publisher, runtime.builder):
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
            await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"'))
        relation_map = {}
        for name in witnesses.INPUTS:
            address_row = (
                await connection.execute(
                    text("SELECT oid::bigint,relfilenode::bigint FROM pg_class WHERE oid=to_regclass(:relation)"),
                    {"relation": f'"{schema}"."{name}"'},
                )
            ).one()
            relation_map[name] = (NpiInputRelation(schema, name, *address_row),)
    return {name: relation_map.get(name, ()) for name in INPUT_NAMES}, strict_key, effective_key


async def _prepare(runtime, *, strict_exists, mutate_inputs=None, observed_at=None):
    output, _owner, _builder = await _builder_output(runtime)
    input_map, strict_key, effective_key = await _inputs(runtime, output, strict_exists=strict_exists)
    if mutate_inputs is not None:
        await mutate_inputs(runtime, input_map)
    async with runtime.builders() as session, session.begin():
        await session.execute(text(f'INSERT INTO "{output.schema_name}".npi(npi) VALUES(1000000001)'))
        await session.execute(
            text(
                f"INSERT INTO \"{output.schema_name}\".npi_address(npi,type,checksum,first_line,city_name,state_name,postal_code,country_code) VALUES(1000000001,'primary',1,'100 OAK ST','EXAMPLE','PA','19000','US')"
            )
        )
        await session.execute(
            text(
                f"INSERT INTO \"{output.schema_name}\".npi_address(npi,type,checksum,first_line,city_name,state_name,postal_code,country_code) VALUES(1000000001,'secondary',2,'102 OAK STREET','EXAMPLE','PA','19000','US'),(1000000001,'secondary',3,'104 OAK ST','EXAMPLE','PA','19000','US')"
            )
        )
        observed = observed_at or await session.scalar(text("SELECT transaction_timestamp()"))
        context_map = {
            "context": {
                "_control_attempt_id": "run_example:attempt",
                "_control_attempt_started_at": (observed - dt.timedelta(seconds=1)).isoformat(),
            }
        }
        scope = SimpleNamespace(
            run_id="run_example", output_schema=output.schema_name, inputs=input_map, verify=AsyncMock()
        )
        preparation = await addresses.prepare_addresses(
            context_map,
            scope=scope,
            connection=await witnesses.raw_connection(session),
            staging_table="npi_address",
            field_map=addresses._FIELDS,
            output_schema=output.schema_name,
            pinned_inputs=input_map,
            observed_at=observed,
            input_receipt_sha256="a" * 64,
        )
    return output, input_map, strict_key, effective_key, preparation


@pytest.mark.asyncio
@pytest.mark.parametrize("strict_exists", [True, False])
async def test_native_witnessed_alias_preserves_strict_incumbent_or_absence(native_runtime, strict_exists):
    output, input_map, strict_key, effective_key, prepared = await _prepare(native_runtime, strict_exists=strict_exists)
    schema = output.schema_name
    async with native_runtime.sessions() as session, session.begin():
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=schema,
            verify_witness=AsyncMock(return_value=True),
        )
        await archive.complete_npi_restore(session, output, npi_alias_authority=cap)
        address_rows = (
            (
                await session.execute(
                    text(
                        f'SELECT address_key,source_bits,strict_source_bits,display_priority,last_seen_at,first_line FROM "{schema}".npi_canonical_address ORDER BY address_key'
                    )
                )
            )
            .mappings()
            .all()
        )
        actual_by_key = {address_row["address_key"]: address_row for address_row in address_rows}
        assert (strict_key in actual_by_key) is strict_exists
        assert (
            actual_by_key[effective_key]["source_bits"] == 17
            and actual_by_key[effective_key]["strict_source_bits"] == 16
        )
        assert actual_by_key[effective_key]["display_priority"] == 0
        assert actual_by_key[effective_key]["first_line"] == "102 OAK STREET"
        assert actual_by_key[_key("104 OAK ST")]["source_bits"] == 1
        assert actual_by_key[_key("104 OAK ST")]["strict_source_bits"] == 0
        if strict_exists:
            assert actual_by_key[strict_key]["source_bits"] == actual_by_key[strict_key]["strict_source_bits"] == 16
            assert actual_by_key[strict_key]["display_priority"] == 5
            assert actual_by_key[strict_key]["last_seen_at"].year == 2020
        assert (
            await session.scalar(text(f'SELECT address_key FROM "{schema}".npi_address WHERE checksum=1')) == strict_key
        )
        manifest = await archive.capture_npi_alias_manifest(
            session, output, {witnesses.METADATA_KEY: prepared.address_witness}, npi_alias_authority=cap
        )
        assert manifest.contract == witnesses.MODEL_CONTRACT
        assert archive.validate_npi_result_manifest(manifest) == manifest
        with pytest.raises(RuntimeError):
            async with session.begin_nested():
                await canonical.validate_canonical_closure(session, "npi", schema, addresses.qualified)
        for mutation in ["source_bits=source_bits | 2", "last_seen_at=last_seen_at + interval '1 second'"]:
            with pytest.raises(RuntimeError):
                async with session.begin_nested():
                    await session.execute(
                        text(f'UPDATE "{schema}".npi_canonical_address SET {mutation} WHERE address_key=:key'),
                        {"key": strict_key if strict_exists else effective_key},
                    )
                    await witnesses.validate_npi_alias_contribution(session, schema, cap)


async def _canonical_rows(session, relation):
    return (
        (await session.execute(text(f"SELECT address_key,source_bits,first_line,last_seen_at FROM {relation}")))
        .mappings()
        .all()
    )


async def _canonical_row_json(session, relation, key):
    return await session.scalar(
        text(f"SELECT to_jsonb(a)::text FROM {relation} a WHERE address_key=:key"), {"key": key}
    )


@pytest.mark.asyncio
async def test_native_projection_and_attachment_observe_effective_target_without_touching_strict_source(native_runtime):
    output, input_map, strict_key, effective_key, prepared = await _prepare(native_runtime, strict_exists=True)
    live = "alias_live_" + uuid4().hex
    await create_native_npi_family(native_runtime, live, populated=False)
    baseline = input_map["address_archive_v2"][0].qualified
    async with native_runtime.sessions() as session, session.begin():
        await session.execute(text(f'INSERT INTO "{live}".address_archive_v2 SELECT * FROM {baseline}'))
        other = uuid4()
        await session.execute(
            text(
                f"INSERT INTO \"{live}\".address_archive_v2(address_key,identity_key,source_bits) VALUES(:key,'unrelated',32)"
            ),
            {"key": other},
        )
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        stage = f'"{output.schema_name}"."npi_canonical_address"'
        canonical_target = f'"{live}"."address_archive_v2"'
        await canonical.validate_canonical_contributions(
            session, {"npi": stage}, canonical_target, npi_alias_authority=cap
        )
        projection = canonical.canonical_archive_projection({"npi": stage}, canonical_target, npi_alias_authority=cap)
        expected = await _canonical_rows(session, "(" + projection + ") p")
        before = await _canonical_row_json(session, canonical_target, strict_key)
        receipt = await canonical.merge_canonical_contribution(
            session, "npi", stage, canonical_target, npi_alias_authority=cap
        )
        assert (
            canonical.validate_canonical_publication(
                receipt, "npi", dict(output.relation_oids)["npi_canonical_address"]
            )
            == receipt
        )
        actual = await _canonical_rows(session, canonical_target)
        assert {address_row["address_key"]: dict(address_row) for address_row in actual} == {
            address_row["address_key"]: dict(address_row) for address_row in expected
        }
        assert await _canonical_row_json(session, canonical_target, strict_key) == before
        assert (
            await session.scalar(
                text(f"SELECT source_bits FROM {canonical_target} WHERE address_key=:key"), {"key": effective_key}
            )
            == 17
        )
        assert (
            await session.scalar(
                text(f"SELECT source_bits FROM {canonical_target} WHERE address_key=:key"), {"key": other}
            )
            == 32
        )


@pytest.mark.asyncio
async def test_native_witness_replay_rejects_retargeted_alias_and_expired_session(native_runtime):
    output, input_map, _s, effective_key, prepared = await _prepare(native_runtime, strict_exists=False)
    cap = None
    async with native_runtime.sessions() as session, session.begin():
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        await witnesses.validate_npi_alias_contribution(session, output.schema_name, cap)
        with pytest.raises(RuntimeError):
            await witnesses.authenticate_npi_alias_witness(
                session,
                prepared.address_witness,
                schema_name=output.schema_name,
                verify_witness=AsyncMock(return_value=False),
            )
    async with native_runtime.sessions() as session, session.begin():
        with pytest.raises(RuntimeError):
            await witnesses.require_npi_alias_authority(session, cap)
    async with native_runtime.administrator.begin() as connection:
        await connection.execute(text(f"UPDATE {input_map['address_alias_v1'][0].qualified} SET revoked_at=now()"))
    async with native_runtime.sessions() as session, session.begin():
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        with pytest.raises(RuntimeError):
            await witnesses.validate_npi_alias_contribution(session, output.schema_name, cap)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["missing_target", "source_identity", "target_identity", "multi_hop", "state_version"])
async def test_native_invalid_issued_alias_refuses_without_partial_address_output(native_runtime, kind):
    async def mutate(runtime, input_map):
        alias = input_map["address_alias_v1"][0].qualified
        async with runtime.administrator.begin() as connection:
            if kind == "missing_target":
                await connection.execute(text(f"UPDATE {alias} SET target_address_key=:key"), {"key": uuid4()})
            elif kind in {"source_identity", "target_identity"}:
                await connection.execute(text(f"UPDATE {alias} SET {kind}_key='mismatch'"))
            elif kind == "multi_hop":
                await connection.execute(
                    text(
                        f"INSERT INTO {alias} SELECT target_address_key,target_identity_key,:key,'downstream',NULL FROM {alias}"
                    ),
                    {"key": uuid4()},
                )
            else:
                await connection.execute(
                    text(f"UPDATE {input_map['address_alias_state_v1'][0].qualified} SET schema_version=999")
                )

    with pytest.raises(RuntimeError):
        await _prepare(native_runtime, strict_exists=True, mutate_inputs=mutate)


@pytest.mark.asyncio
async def test_native_true_noop_keeps_timestamp_and_formatter(native_runtime):
    async def already_observed(runtime, input_map):
        async with runtime.administrator.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {input_map['address_archive_v2'][0].qualified} SET source_bits=17,display_priority=0 WHERE address_key=:key"
                ),
                {"key": _key("102 OAK ST")},
            )

    output, _inputs_value, _s, effective_key, prepared = await _prepare(
        native_runtime, strict_exists=True, mutate_inputs=already_observed
    )
    assert prepared.stats.provenance_updates == 0
    async with native_runtime.sessions() as session, session.begin():
        address_row = (
            await session.execute(
                text(
                    f'SELECT first_line,last_seen_at,formatted_address_version FROM "{output.schema_name}".npi_canonical_address WHERE address_key=:key'
                ),
                {"key": effective_key},
            )
        ).one()
        assert (
            address_row.first_line == "102 OAK ST"
            and address_row.last_seen_at.year == 2020
            and address_row.formatted_address_version == 1
        )
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        await witnesses.validate_npi_alias_contribution(session, output.schema_name, cap)


@pytest.mark.asyncio
async def test_native_retains_genuine_strict_redirect_closure(native_runtime):
    successor = _key("106 OAK ST")

    async def redirect(runtime, input_map):
        baseline = input_map["address_archive_v2"][0].qualified
        async with runtime.administrator.begin() as connection:
            await connection.execute(
                text(
                    f"INSERT INTO {baseline}(address_key,identity_key,source_bits,strict_source_bits) VALUES(:key,:identity,32,32)"
                ),
                {
                    "key": successor,
                    "identity": addresses.canon.identity_key_v1("106 OAK ST", None, "EXAMPLE", "PA", "19000", "US"),
                },
            )
            await connection.execute(
                text(f"UPDATE {baseline} SET merged_into=:target WHERE address_key=:source"),
                {"target": successor, "source": _key("100 OAK ST")},
            )

    output, input_map, strict_key, effective_key, prepared = await _prepare(
        native_runtime, strict_exists=True, mutate_inputs=redirect
    )
    async with native_runtime.sessions() as session, session.begin():
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        await archive.complete_npi_restore(session, output, npi_alias_authority=cap)
        for key in (strict_key, successor):
            assert (
                await session.scalar(
                    text(
                        f'SELECT (SELECT to_jsonb(s) FROM "{output.schema_name}".npi_canonical_address s WHERE address_key=:key)='
                        f"(SELECT to_jsonb(b) FROM {input_map['address_archive_v2'][0].qualified} b WHERE address_key=:key)"
                    ),
                    {"key": key},
                )
                is True
            )
        assert (
            await session.scalar(
                text(f'SELECT source_bits FROM "{output.schema_name}".npi_canonical_address WHERE address_key=:key'),
                {"key": effective_key},
            )
            == 17
        )


@pytest.mark.asyncio
async def test_native_cancel_rolls_back_and_retry_reuses_sealed_observation(native_runtime):
    output, input_map, _s, _t, prepared = await _prepare(native_runtime, strict_exists=False)
    schema = output.schema_name
    observed = witnesses.observation_time(prepared.address_witness["observation_time"])
    context_map = {
        "context": {
            "_control_attempt_id": "run_example:attempt",
            "_control_attempt_started_at": prepared.address_witness["attempt_started_at"],
        }
    }
    scope = SimpleNamespace(run_id="run_example", output_schema=schema, inputs=input_map, verify=AsyncMock())
    async with native_runtime.builders() as session, session.begin():
        await session.execute(text(f'UPDATE "{schema}".npi_address SET address_key=NULL'))
        connection = await witnesses.raw_connection(session)
        checks = iter((False, True))

        async def cancel_after_key_stamp():
            if next(checks):
                raise asyncio.CancelledError

        argument_map = dict(
            ctx=context_map,
            scope=scope,
            connection=connection,
            staging_table="npi_address",
            field_map=addresses._FIELDS,
            output_schema=schema,
            pinned_inputs=input_map,
            observed_at=observed,
            input_receipt_sha256="a" * 64,
        )
        with pytest.raises(asyncio.CancelledError):
            await addresses.prepare_addresses(**argument_map, cancel_check=cancel_after_key_stamp)
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".npi_address WHERE address_key IS NULL')) == 3
        retried = await addresses.prepare_addresses(**argument_map)
        repeated = await addresses.prepare_addresses(**argument_map)
        assert retried.address_witness == repeated.address_witness == prepared.address_witness
        assert retried.relation == repeated.relation == prepared.relation


@pytest.mark.asyncio
async def test_native_witness_restores_only_with_fresh_local_authority(native_runtime):
    output, _inputs_value, _s, _t, prepared = await _prepare(native_runtime, strict_exists=False)
    async with native_runtime.sessions() as session, session.begin():
        old = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        await archive.complete_npi_restore(session, output, npi_alias_authority=old)
        manifest = await archive.capture_npi_alias_manifest(
            session, output, {witnesses.METADATA_KEY: prepared.address_witness}, npi_alias_authority=old
        )
        dataset_id = uuid4()
        native_runtime.schemas.add(archive.npi_stage_schema(dataset_id))
        restored = await owned.precreate_npi_output(session, dataset_id=dataset_id)
        for name, _ in output.relation_oids:
            await session.execute(
                text(f'INSERT INTO "{restored.schema_name}"."{name}" SELECT * FROM "{output.schema_name}"."{name}"')
            )
        with pytest.raises(RuntimeError):
            await witnesses.validate_npi_alias_contribution(session, restored.schema_name, old)
        fresh = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=restored.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        await archive.complete_npi_restore(session, restored, npi_alias_authority=fresh)
        await archive.validate_npi_stage(session, ownership=restored, manifest=manifest, npi_alias_authority=fresh)
        with pytest.raises(RuntimeError):
            await archive.validate_npi_stage(session, ownership=restored, manifest=manifest)
        with pytest.raises(RuntimeError):
            await asyncio.create_task(witnesses.require_npi_alias_authority(session, fresh))


async def _zip_inputs(native_runtime, output):
    input_map, _s, _t = await _inputs(native_runtime, output, strict_exists=False)
    schema = input_map["address_archive_v2"][0].schema_name
    async with native_runtime.administrator.begin() as connection:
        await connection.execute(text(f'CREATE TABLE "{schema}".zcta_fixture(zcta5ce text,the_geom public.geometry)'))
        await connection.execute(text(f'CREATE TABLE "{schema}".zip_fixture(zip_code text,state text)'))
        await connection.execute(
            text(
                f"INSERT INTO \"{schema}\".zcta_fixture VALUES('19000',public.ST_MakeEnvelope(-1,-1,3,3,4269)),('19001',public.ST_MakeEnvelope(1,1,3,3,4269))"
            )
        )
        await connection.execute(text(f"INSERT INTO \"{schema}\".zip_fixture VALUES('19000','PA'),('19001','PA')"))
        for key, table in (("tiger", "zcta_fixture"), ("geo_zip_lookup", "zip_fixture")):
            address_row = (
                await connection.execute(
                    text("SELECT oid::bigint,relfilenode::bigint FROM pg_class WHERE oid=to_regclass(:relation)"),
                    {"relation": f'"{schema}"."{table}"'},
                )
            ).one()
            input_map[key] = (NpiInputRelation(schema, table, *address_row),)
            await connection.execute(text(f'GRANT SELECT ON "{schema}"."{table}" TO "{native_runtime.builder}"'))
    return input_map


@pytest.mark.asyncio
async def test_native_zip_restore_requires_one_pinned_state_checked_match(native_runtime):
    async with native_runtime.administrator.begin() as connection:
        if await connection.scalar(text("SELECT to_regtype('public.geometry')")) is None:
            pytest.skip("native PostGIS fixture is unavailable")
    output, _owner, _builder = await _builder_output(native_runtime)
    input_map = await _zip_inputs(native_runtime, output)
    async with native_runtime.builders() as session, session.begin():
        await session.execute(text(f'INSERT INTO "{output.schema_name}".npi(npi) VALUES(1000000001)'))
        for checksum, state, country, coordinate in (
            (1, "PA", "US", 0),
            (2, "PA", "US", 2),
            (3, "NJ", "US", 0),
            (4, "PA", "CA", 0),
        ):
            await session.execute(
                text(
                    f"INSERT INTO \"{output.schema_name}\".npi_address(npi,type,checksum,first_line,city_name,state_name,country_code,lat,long) VALUES(1000000001,'primary',:checksum,'108 OAK ST','EXAMPLE',:state,:country,:coordinate,:coordinate)"
                ),
                {"checksum": checksum, "state": state, "country": country, "coordinate": coordinate},
            )
        observed = await session.scalar(text("SELECT transaction_timestamp()"))
        context_map = {
            "context": {
                "_control_attempt_id": "run_example:attempt",
                "_control_attempt_started_at": observed.isoformat(),
            }
        }
        scope = SimpleNamespace(
            run_id="run_example", output_schema=output.schema_name, inputs=input_map, verify=AsyncMock()
        )
        preparation = await addresses.prepare_addresses(
            context_map,
            scope=scope,
            connection=await witnesses.raw_connection(session),
            staging_table="npi_address",
            field_map=addresses._FIELDS,
            output_schema=output.schema_name,
            pinned_inputs=input_map,
            observed_at=observed,
            input_receipt_sha256="a" * 64,
        )
        address_rows = (
            await session.execute(text(f'SELECT checksum,postal_code FROM "{output.schema_name}".npi_address'))
        ).all()
        assert dict(address_rows) == {1: "19000", 2: None, 3: None, 4: None}
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            preparation.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        await witnesses.validate_npi_alias_contribution(session, output.schema_name, cap)


@pytest.mark.asyncio
async def test_native_alias_export_reauthenticates_closed_seven_heap_output(native_runtime, tmp_path):
    runtime = native_runtime
    output, _inputs_value, _s, _t, prepared = await _prepare(runtime, strict_exists=False)
    async with runtime.sessions() as session, session.begin():
        cap = await witnesses.authenticate_npi_alias_witness(
            session,
            prepared.address_witness,
            schema_name=output.schema_name,
            verify_witness=AsyncMock(return_value=True),
        )
        await archive.complete_npi_restore(session, output, npi_alias_authority=cap)
        manifest = await archive.capture_npi_alias_manifest(
            session, output, {witnesses.METADATA_KEY: prepared.address_witness}, npi_alias_authority=cap
        )
    async with runtime.administrator.begin() as connection:
        owner_oid = await connection.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.owner})
        for name, _ in output.relation_oids:
            await connection.execute(text(f'ALTER TABLE "{output.schema_name}"."{name}" OWNER TO "{runtime.owner}"'))
            await connection.execute(text(f'GRANT SELECT ON "{output.schema_name}"."{name}" TO "{runtime.publisher}"'))
        await connection.execute(text(f'REVOKE ALL ON SCHEMA "{output.schema_name}" FROM "{runtime.builder}"'))

    async def verify_custody(session, prepared_source):
        assert prepared_source.ownership == output
        await owned._require_inert_owned_stage(session, output, owner_oid)

    async def authenticate(session, witness):
        assert witness == prepared.address_witness
        return await witnesses.authenticate_npi_alias_witness(
            session, witness, schema_name=output.schema_name, verify_witness=AsyncMock(return_value=True)
        )

    prepared_source = archive.NpiPreparedSource(manifest, output)
    dump_path = tmp_path / "provider-result.dump"
    captures = []

    async def copy_archive(capture):
        captures.append(capture)
        _dump_npi_stage(capture, dump_path, environment=_native_archive_environment(runtime.publisher_url))

    with pytest.raises(RuntimeError):
        await archive.export_prepared_npi_archive(
            runtime.sessions, prepared=prepared_source, archive_copy=copy_archive, verify_custody=verify_custody
        )
    assert not captures
    exported = await archive.export_prepared_npi_archive(
        runtime.sessions,
        prepared=prepared_source,
        archive_copy=copy_archive,
        verify_custody=verify_custody,
        authenticate_npi_alias=authenticate,
    )
    assert exported == manifest and len(captures) == 1 and len(captures[0].manifest.tables) == 7
    assert dump_path.stat().st_size > 0
