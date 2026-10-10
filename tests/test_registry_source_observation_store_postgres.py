# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable bulk source evidence on actual additive PostgreSQL migrations."""

import hashlib
import json
import os
from dataclasses import replace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process.registry_identity_materialization import materialize_registry_source_identities
from process.registry_source_observation_store import (
    RegistryObservationError,
    RegistryObservationLanding,
    persist_registry_source_observations,
    read_registry_issuer_evidence,
)
from tests.test_network_serving_schema_postgres import serving_schema

pytestmark = pytest.mark.asyncio
_RAW_FIELDS = (
    "mr_submission_template_id",
    "business_state",
    "group_affiliation",
    "company_pk",
    "hios_issuer_id",
    "company_name",
    "company_address",
    "domiciliary_state",
    "naic_group_code",
    "naic_company_code",
    "federal_ein",
    "am_best_number",
    "dba_marketing_name",
    "not_for_profit",
    "created_date",
    "merge_markets_ind_small_grp",
    "fit_exempt",
)


def _assertion(source_row=2, *, status="accepted", ein="012345678", group="707", hios="00123", state="CA"):
    raw_fields = dict.fromkeys(_RAW_FIELDS, "")
    raw_fields.update(
        mr_submission_template_id=str(source_row),
        company_name="Example Company",
        business_state=state,
        federal_ein=ein or "",
        naic_group_code=group or "",
        naic_company_code="00456",
    )
    issues = (
        []
        if status == "accepted"
        else [{"field": "federal_ein", "code": "missing_identifier", "rejecting": status == "rejected"}]
    )
    return dict(
        source_row_number=source_row,
        submission_id=str(source_row),
        row_kind="issuer_filing",
        state=state,
        normalized_ein=ein,
        normalized_naic_company="00456",
        normalized_naic_group=group,
        hios=hios,
        company_key=None if ein is None else "cms_mlr:ein:" + ein,
        group_kind=None if group is None else "naic_group",
        status=status,
        issues=issues,
        raw_fields=raw_fields,
    )


async def _landing(connection, control_schema, assertions, *, year=2024):
    snapshot_id = uuid4()
    digest = hashlib.sha256(snapshot_id.bytes).hexdigest()
    await connection.execute(
        f'INSERT INTO "{control_schema}".registry_source_snapshot '
        "(snapshot_id,source_system,source_id,edition_id,source_url,artifact_sha256,input_sha256,parser_version,reporting_year,published_at) "
        "VALUES($1,'cms_mlr','example-source',$2,'https://example.org/source',$3,$3,'fixture-v1',$4,'2025-09-12')",
        snapshot_id,
        f"edition-{snapshot_id.hex}",
        digest,
        year,
    )
    table_name = "source_landing_" + uuid4().hex
    await connection.execute(
        f'CREATE TEMP TABLE "{table_name}" (snapshot_id uuid,source_record_key varchar(128),source_row_number integer,status varchar(16),observation_json jsonb,issues_json jsonb)'
    )
    relation_info = await connection.fetchrow(
        "SELECT c.oid,n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relnamespace=pg_my_temp_schema() AND c.relname=$1",
        table_name,
    )
    landing_rows = [
        (
            snapshot_id,
            f"row:{assertion['source_row_number']}",
            assertion["source_row_number"],
            assertion["status"],
            json.dumps(assertion),
            json.dumps(assertion["issues"]),
        )
        for assertion in assertions
    ]
    await connection.copy_records_to_table(table_name, schema_name=relation_info["nspname"], records=landing_rows)
    return RegistryObservationLanding(
        relation_info["nspname"],
        table_name,
        relation_info["oid"],
        snapshot_id,
        "cms_mlr",
        "example-source",
        f"edition-{snapshot_id.hex}",
        digest,
        "fixture-v1",
        len(assertions),
    )


async def _bindings(connection, schema, snapshot_id):
    company_id, group_id = uuid4(), uuid4()
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_registry(company_id,display_name,roles) VALUES($1,'Manual Company',ARRAY['employer'])",
        company_id,
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_group_registry(group_id,group_kind,display_name) VALUES($1,'naic_group','Manual Group')",
        group_id,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".registry_identifier_binding VALUES '
        "('company','ein','012345678',$1,$3,'review-one'),('group','naic_group','707',$2,$3,'review-two')",
        company_id,
        group_id,
        snapshot_id,
    )
    return company_id, group_id


async def _persist(connection, schema, landing):
    return await persist_registry_source_observations(connection, landing, control_schema=schema)


async def _corroborate_naic(connection, schema, landing, company_id):
    await connection.execute(
        f"INSERT INTO \"{schema}\".registry_identifier_binding VALUES('company','naic_company','00456',$1,$2,'review-naic')",
        company_id,
        landing.snapshot_id,
    )


async def test_existing_bindings_resolve_without_touching_manual_heads(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    company_id, group_id = await _bindings(connection, schema, landing.snapshot_id)
    await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=7,approved_revision=5')
    async with connection.transaction():
        totals = await _persist(connection, schema, landing)
    assert totals == dict(
        observations=1,
        accepted=1,
        unresolved=0,
        rejected=0,
        identifiers=3,
        resolved_identifiers=2,
        issuer_assertions=1,
        resolved_issuers=1,
        conflicting_issuers=0,
        group_assertions=1,
        resolved_groups=1,
        replayed=False,
    )
    assert tuple(
        await connection.fetchrow(
            f'SELECT company_id,group_id,relationship_kind,valid_from FROM "{schema}".registry_company_group_assertion'
        )
    ) == (company_id, group_id, "reported_affiliation", None)
    assert tuple(await connection.fetchrow(f'SELECT display_name,revision,roles FROM "{schema}".company_registry')) == (
        "Manual Company",
        1,
        ["employer"],
    )
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (7, 5)
    evidence = await read_registry_issuer_evidence(connection, 123, control_schema=schema)
    assert len(evidence) == 1 and evidence[0]["issuer_id"] == 123 and evidence[0]["hios_issuer_id"] == "00123"
    assert evidence[0]["company_id"] == company_id and evidence[0]["resolution_status"] == "resolved"
    assert (
        evidence[0]["reporting_year"] == 2024
        and evidence[0]["published_at"].year == 2025
        and evidence[0]["valid_from"] is None
    )
    assert tuple(
        await connection.fetchrow(
            f"SELECT entity_id,resolution_status FROM \"{schema}\".registry_identifier_observation WHERE identifier_system='naic_company'"
        )
    ) == (None, "unresolved")


async def test_naic_company_resolves_only_when_corroborated_with_explicit_ein(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    company_id, _ = await _bindings(connection, schema, landing.snapshot_id)
    await _corroborate_naic(connection, schema, landing, company_id)
    async with connection.transaction():
        totals = await _persist(connection, schema, landing)
    assert totals["resolved_identifiers"] == 3 and totals["resolved_issuers"] == 1
    assert tuple(
        await connection.fetchrow(
            f"SELECT entity_id,resolution_status FROM \"{schema}\".registry_identifier_observation WHERE identifier_system='naic_company'"
        )
    ) == (company_id, "resolved")


async def test_naic_only_binding_never_resolves_a_company_or_issuer(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion(ein=None, status="unresolved")])
    company_id, _ = await _bindings(connection, schema, landing.snapshot_id)
    await _corroborate_naic(connection, schema, landing, company_id)
    async with connection.transaction():
        totals = await _persist(connection, schema, landing)
    assert totals["resolved_identifiers"] == 1 and totals["resolved_issuers"] == 0
    assert tuple(
        await connection.fetchrow(
            f"SELECT entity_id,resolution_status FROM \"{schema}\".registry_identifier_observation WHERE identifier_system='naic_company'"
        )
    ) == (None, "unresolved")


@pytest.mark.parametrize("conflict", ["legal_names", "ein_many_naic", "naic_many_ein", "declared_fact"])
async def test_complete_source_conflicts_remain_raw_and_cannot_resolve_existing_ein(serving_schema, conflict):
    connection, schema, _ = serving_schema
    first, second = _assertion(), _assertion(3, hios="00999")
    if conflict == "legal_names":
        second["raw_fields"]["company_name"] = "Other Source Company"
    elif conflict == "ein_many_naic":
        second["normalized_naic_company"] = "00457"
        second["raw_fields"]["naic_company_code"] = "00457"
    elif conflict == "naic_many_ein":
        second["normalized_ein"] = "023456789"
        second["raw_fields"]["federal_ein"] = "023456789"
    else:
        first["status"] = "unresolved"
        first["issues"] = [{"field": "company_name", "code": "conflicting_company_names_for_ein", "rejecting": False}]
    assertions = [first, second]
    landing = await _landing(connection, schema, assertions)
    company_id, _ = await _bindings(connection, schema, landing.snapshot_id)
    await _corroborate_naic(connection, schema, landing, company_id)
    async with connection.transaction():
        materialized = await materialize_registry_source_identities(connection, landing, control_schema=schema)
        totals = await _persist(connection, schema, landing)
    assert materialized["companies_created"] == 0 and materialized["company_conflicts"] >= 1
    assert totals["resolved_issuers"] == 0 and totals["conflicting_issuers"] == 2 and totals["group_assertions"] == 0
    company_assertions = await connection.fetch(
        f"SELECT entity_id,resolution_status FROM \"{schema}\".registry_identifier_observation WHERE entity_kind='company'"
    )
    assert all(tuple(assertion) == (None, "conflicting") for assertion in company_assertions)
    retained = await connection.fetch(
        f'SELECT observation_json FROM "{schema}".registry_source_observation ORDER BY source_row_number'
    )
    assert [json.loads(entry[0]) for entry in retained] == assertions


async def test_contradictory_ledger_conflict_propagates_to_same_ein_without_naic(serving_schema):
    connection, schema, _ = serving_schema
    first, second = _assertion(), _assertion(3, hios="00999")
    second["normalized_naic_company"] = None
    second["raw_fields"]["naic_company_code"] = ""
    landing = await _landing(connection, schema, [first, second])
    await _bindings(connection, schema, landing.snapshot_id)
    other_company = uuid4()
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_registry(company_id,display_name,roles) VALUES($1,'Other Manual Company',ARRAY['employer'])",
        other_company,
    )
    await _corroborate_naic(connection, schema, landing, other_company)
    async with connection.transaction():
        totals = await _persist(connection, schema, landing)
    assert totals["resolved_issuers"] == 0 and totals["conflicting_issuers"] == 2
    assert not await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM \"{schema}\".registry_identifier_observation WHERE entity_kind='company' AND entity_id IS NOT NULL)"
    )
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_identifier_binding') == 3


async def test_declared_issuer_conflict_preserves_company_identity_but_not_issuer_link(serving_schema):
    connection, schema, _ = serving_schema
    assertion = _assertion(status="unresolved")
    assertion["issues"] = [{"field": "hios_issuer_id", "code": "conflicting_issuer_identity", "rejecting": False}]
    landing = await _landing(connection, schema, [assertion])
    await _bindings(connection, schema, landing.snapshot_id)
    async with connection.transaction():
        totals = await _persist(connection, schema, landing)
    assert totals["resolved_identifiers"] == 2 and totals["conflicting_issuers"] == 1
    assert await connection.fetchval(f'SELECT company_id FROM "{schema}".registry_issuer_company_assertion') is None


async def test_one_hios_with_distinct_eins_keeps_companies_but_conflicts_issuer_links(serving_schema):
    connection, schema, _ = serving_schema
    first, second = _assertion(), _assertion(3, ein="023456789")
    second["normalized_naic_company"] = "00457"
    second["raw_fields"]["naic_company_code"] = "00457"
    landing = await _landing(connection, schema, [first, second])
    async with connection.transaction():
        materialized = await materialize_registry_source_identities(connection, landing, control_schema=schema)
        totals = await _persist(connection, schema, landing)
    assert materialized["companies_created"] == 2 and materialized["company_conflicts"] == 0
    assert totals["resolved_identifiers"] == 6 and totals["conflicting_issuers"] == 2
    assert not await connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{schema}".registry_issuer_company_assertion WHERE company_id IS NOT NULL)'
    )


@pytest.mark.parametrize("invalid_binding", ["dangling_naic", "corporate_group"])
async def test_durable_binding_checks_cover_naic_and_group_kind(serving_schema, invalid_binding):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    _, group_id = await _bindings(connection, schema, landing.snapshot_id)
    if invalid_binding == "dangling_naic":
        await _corroborate_naic(connection, schema, landing, uuid4())
    else:
        await connection.execute(
            f"UPDATE \"{schema}\".company_group_registry SET group_kind='corporate_parent' WHERE group_id=$1", group_id
        )
    async with connection.transaction():
        with pytest.raises(RegistryObservationError, match="missing durable or incompatible"):
            await _persist(connection, schema, landing)
        assert await connection.fetchval("SELECT 1") == 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 0


async def test_missing_identifiers_and_rejected_rows_remain_raw(serving_schema):
    connection, schema, _ = serving_schema
    assertions = [_assertion(ein=None, group=None, status="unresolved"), _assertion(3, ein=None, status="rejected")]
    landing = await _landing(connection, schema, assertions)
    async with connection.transaction():
        totals = await _persist(connection, schema, landing)
    assert totals["observations"] == 2 and totals["unresolved"] == 1 and totals["rejected"] == 1
    assert totals["resolved_identifiers"] == 0 and totals["issuer_assertions"] == 1 and totals["resolved_issuers"] == 0
    assert await connection.fetchval(f'SELECT company_id FROM "{schema}".registry_issuer_company_assertion') is None
    retained = await connection.fetch(
        f'SELECT observation_json FROM "{schema}".registry_source_observation ORDER BY source_row_number'
    )
    assert [json.loads(entry[0]) for entry in retained] == assertions
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 0


@pytest.mark.parametrize("existing_state", [None, "FL"])
async def test_hios_conflicts_never_overwrite_registry_state(serving_schema, existing_state):
    connection, schema, _ = serving_schema
    assertions = [_assertion()]
    if existing_state is None:
        assertions.append(_assertion(3, state="FL"))
    else:
        await connection.execute(
            f"INSERT INTO \"{schema}\".hios_issuer_registry VALUES('00123',$1,now())", existing_state
        )
    landing = await _landing(connection, schema, assertions)
    await _bindings(connection, schema, landing.snapshot_id)
    async with connection.transaction():
        totals = await _persist(connection, schema, landing)
    assert totals["conflicting_issuers"] == len(assertions) and totals["resolved_issuers"] == 0
    assert (
        await connection.fetchval(
            f"SELECT business_state FROM \"{schema}\".hios_issuer_registry WHERE hios_issuer_id='00123'"
        )
        == existing_state
    )
    assert not await connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{schema}".registry_issuer_company_assertion WHERE company_id IS NOT NULL)'
    )


async def test_exact_replay_does_not_re_resolve_after_manual_correction(serving_schema):
    connection, schema, _ = serving_schema
    old_landing = await _landing(connection, schema, [_assertion()])
    async with connection.transaction():
        before = await _persist(connection, schema, old_landing)
    assert before["resolved_issuers"] == 0
    company_id, _ = await _bindings(connection, schema, old_landing.snapshot_id)
    await _corroborate_naic(connection, schema, old_landing, company_id)
    await connection.execute(f"UPDATE \"{schema}\".company_registry SET display_name='Corrected Name',revision=9")
    async with connection.transaction():
        replay = await _persist(connection, schema, old_landing)
    assert replay == before | {"replayed": True}
    new_landing = await _landing(connection, schema, [_assertion()], year=2025)
    async with connection.transaction():
        after = await _persist(connection, schema, new_landing)
    assert after["resolved_issuers"] == 1
    assert after["resolved_identifiers"] == 3 and replay["resolved_identifiers"] == 0
    evidence = await read_registry_issuer_evidence(connection, "00123", control_schema=schema)
    assert [entry["reporting_year"] for entry in evidence] == [2025, 2024]
    assert evidence[0]["company_id"] == company_id and evidence[1]["company_id"] is None
    assert tuple(await connection.fetchrow(f'SELECT display_name,revision FROM "{schema}".company_registry')) == (
        "Corrected Name",
        9,
    )


@pytest.mark.parametrize("change", ["changed", "missing", "extra"])
async def test_replay_changed_missing_or_extra_rows_is_rejected(serving_schema, change):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    async with connection.transaction():
        await _persist(connection, schema, landing)
    if change == "changed":
        await connection.execute(
            f'UPDATE "{landing.schema_name}"."{landing.table_name}" SET observation_json=jsonb_set(observation_json,\'{{raw_fields,company_name}}\',\'"Changed Source Name"\')'
        )
    elif change == "missing":
        await connection.execute(f'DELETE FROM "{landing.schema_name}"."{landing.table_name}"')
        landing = replace(landing, expected_rows=0)
    else:
        extra = _assertion(3)
        await connection.copy_records_to_table(
            landing.table_name,
            schema_name=landing.schema_name,
            records=[
                (landing.snapshot_id, "row:3", 3, extra["status"], json.dumps(extra), json.dumps(extra["issues"]))
            ],
        )
        landing = replace(landing, expected_rows=2)
    async with connection.transaction():
        with pytest.raises(RegistryObservationError, match="differs"):
            await _persist(connection, schema, landing)
        assert await connection.fetchval("SELECT 1") == 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 1


@pytest.mark.parametrize(
    "field,invalid",
    [
        ("normalized_ein", "000000000"),
        ("normalized_naic_company", "123"),
        ("normalized_naic_group", "00707"),
        ("hios", "123"),
        ("state", "ca"),
        ("source_row_number", 4),
        ("status", "unresolved"),
        ("submission_id", "wrong"),
    ],
)
async def test_malformed_content_rejects_entire_landing(serving_schema, field, invalid):
    connection, schema, _ = serving_schema
    assertion = _assertion()
    landing = await _landing(connection, schema, [assertion])
    await connection.execute(
        f'UPDATE "{landing.schema_name}"."{landing.table_name}" SET observation_json=jsonb_set(observation_json,ARRAY[$1::text],$2::jsonb)',
        field,
        json.dumps(invalid),
    )
    async with connection.transaction():
        with pytest.raises(RegistryObservationError, match="content"):
            await _persist(connection, schema, landing)
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 0


async def test_wrong_landing_scope_and_missing_binding_identity_fail_atomically(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    with pytest.raises(RegistryObservationError, match="caller-owned"):
        await _persist(connection, schema, landing)
    async with connection.transaction():
        for altered in [
            replace(landing, relation_oid=1),
            replace(landing, source_id="different-source"),
            replace(landing, snapshot_id=uuid4()),
        ]:
            with pytest.raises(RegistryObservationError):
                await _persist(connection, schema, altered)
        await connection.execute(
            f"INSERT INTO \"{schema}\".registry_identifier_binding VALUES('company','ein','012345678',$1,$2,'review')",
            uuid4(),
            landing.snapshot_id,
        )
        with pytest.raises(RegistryObservationError, match="missing durable"):
            await _persist(connection, schema, landing)
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 0


class CountedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statement_count = 0

    def __getattr__(self, name):
        if name not in ("execute", "fetch", "fetchrow", "fetchval"):
            return getattr(self.connection, name)

        async def counted(*args, **kwargs):
            self.statement_count += 1
            return await getattr(self.connection, name)(*args, **kwargs)

        return counted


async def test_statement_count_is_constant_for_one_and_five_thousand_rows(serving_schema):
    connection, schema, _ = serving_schema
    statement_counts = []
    for count in [1, 5000]:
        landing = await _landing(connection, schema, [_assertion(index + 2) for index in range(count)])
        counted = CountedConnection(connection)
        async with connection.transaction():
            totals = await _persist(counted, schema, landing)
        assert totals["observations"] == count and totals["accepted"] == count
        statement_counts.append(counted.statement_count)
    assert statement_counts[0] == statement_counts[1] and statement_counts[0] <= 16


@pytest.mark.parametrize("issuer", [True, 0, -1, 100000, "123", "00000", "00123x", None])
async def test_evidence_reader_denies_identity_coercion(serving_schema, issuer):
    connection, schema, _ = serving_schema
    with pytest.raises(RegistryObservationError):
        await read_registry_issuer_evidence(connection, issuer, control_schema=schema)


async def test_other_session_cannot_use_a_native_landing_oid(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    other_connection = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        async with other_connection.transaction():
            with pytest.raises(RegistryObservationError, match="session-owned"):
                await _persist(other_connection, schema, landing)
    finally:
        await other_connection.close()
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 0


async def test_outer_rollback_removes_every_source_derived_write(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    await _bindings(connection, schema, landing.snapshot_id)
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with connection.transaction():
            await _persist(connection, schema, landing)
            raise RuntimeError("caller rollback")
    for table_name in (
        "registry_source_observation",
        "registry_identifier_observation",
        "registry_issuer_company_assertion",
        "registry_company_group_assertion",
        "hios_issuer_registry",
    ):
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"') == 0
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 1


async def test_submission_metadata_uses_the_native_unicode_trim_contract(serving_schema):
    connection, schema, _ = serving_schema
    assertion = _assertion()
    assertion["raw_fields"]["mr_submission_template_id"] = "\u2003\t2\u00a0"
    landing = await _landing(connection, schema, [assertion])
    async with connection.transaction():
        assert (await _persist(connection, schema, landing))["observations"] == 1


async def test_null_duplicate_and_wrong_snapshot_rows_cannot_be_admitted(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    async with connection.transaction():
        for modification in (
            "snapshot_id=NULL",
            "snapshot_id='00000000-0000-0000-0000-000000000000'::uuid",
            "source_record_key=NULL",
            "issues_json='{}'::jsonb",
        ):
            async with connection.transaction():
                await connection.execute(f'UPDATE "{landing.schema_name}"."{landing.table_name}" SET {modification}')
                with pytest.raises(RegistryObservationError, match="content"):
                    await _persist(connection, schema, landing)
                assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 0
                await connection.execute(f'DELETE FROM "{landing.schema_name}"."{landing.table_name}"')
                assertion = _assertion()
                await connection.copy_records_to_table(
                    landing.table_name,
                    schema_name=landing.schema_name,
                    records=[(landing.snapshot_id, "row:2", 2, "accepted", json.dumps(assertion), "[]")],
                )
        await connection.execute(
            f'INSERT INTO "{landing.schema_name}"."{landing.table_name}" SELECT * FROM "{landing.schema_name}"."{landing.table_name}"'
        )
        with pytest.raises(RegistryObservationError, match="content"):
            await _persist(connection, schema, replace(landing, expected_rows=2))


@pytest.mark.parametrize(
    "invalid_context",
    [
        {"snapshot_id": UUID(int=0)},
        {"relation_oid": 0},
        {"relation_oid": True},
        {"expected_rows": 5001},
        {"expected_rows": True},
        {"schema_name": "unsafe;schema"},
        {"source_system": ""},
    ],
)
async def test_landing_context_rejects_nil_and_unbounded_values(serving_schema, invalid_context):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    with pytest.raises(RegistryObservationError):
        replace(landing, **invalid_context)
