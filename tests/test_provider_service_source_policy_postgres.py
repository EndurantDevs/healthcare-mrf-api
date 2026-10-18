# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission SQL on a genuine retained catalog and ordinary Reader login."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from api import provider_service_source_policy as policy
from api.custom_import_provider_detail import parse_managed_provider_policy
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding
from api.provider_detail_network_read import managed_provider_network_read_scope
from db.connection import current_session
from process.network_approved_membership_source import pin_approved_membership_source
from process.ptg_parts.result_archive_published_authority import PtgPublishedResultSourceAuthority
from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256
from process.registry_ptg_published_plan_scope import _digest, _document, _parameters
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_profile_network_facts_postgres import (
    _compose_profile_manifest,
)
from tests.test_provider_profile_network_facts_postgres import (
    published_profile_source as published_profile_source,
)
from tests.test_registry_candidate_composition_postgres import _remove_candidates
from tests.test_registry_provider_profile_transport_postgres import profile_transport as profile_transport
from tests.test_registry_published_plan_binding_refs import reviewed_binding

pytestmark = pytest.mark.asyncio
_ALLOWED = 9001
_EXCLUDED = 9002
_ARCHIVED = 9003


def _review(schema, name, company, network, revision, *, archived=False):
    mapping, entry = reviewed_binding()
    command = entry["document"]["command"]
    manifest_by_field = {"serving_index": {"source_key": "source_" + name}, "activation": {"state": "activated"}}
    snapshot_id = "snapshot_" + name
    digest = result_archive_manifest_sha256(manifest_by_field)
    command.update(
        scope_id=str(uuid4()),
        statement_id=str(uuid4()),
        idempotency_key=uuid4().hex,
        legal_company_id=company,
        network_id=network,
        approved_revision=revision,
        plan_id="plan_" + name,
    )
    command["published_identity"].update(
        snapshot_id=snapshot_id,
        source_key="source_" + name,
        snapshot_manifest_sha256=digest,
        plan_id=command["plan_id"],
    )
    command["ownership"].update(snapshot_id=snapshot_id, source_key="source_" + name)
    command["source"].update(snapshot_id=snapshot_id, binding_source_key="source_" + name, ptg_schema_name=schema)
    command["coordinates"].update(
        source_id="source_" + name, dataset_schema=schema, dataset_id=snapshot_id, edition_id=digest
    )
    evidence = entry["document"]["evidence"]
    evidence["source_authority"] = PtgPublishedResultSourceAuthority(
        "native_review_" + uuid4().hex, command["published_identity"]
    ).as_dict()
    document = _document(command, entry["document"]["actor"], evidence)
    approval_sha = _digest(document)
    mapping.update(
        command["coordinates"],
        source_key="source_" + name,
        network_id=network,
        archived=archived,
        binding_id=str(uuid4()),
        revision=1,
        evidence_id="published-plan-review:" + command["scope_id"],
        evidence_sha256=approval_sha,
    )
    mapping["source_scope_json"].update(
        scope_id=command["scope_id"], approval_sha256=approval_sha, snapshot_id=snapshot_id, plan_id=command["plan_id"]
    )
    mapping["binding_key"] = _digest(mapping)
    return SimpleNamespace(mapping=mapping, document=document, manifest=manifest_by_field, name=name)


async def _approved_row(held, revision, kind, key, document):
    await held.fixture.connection.execute(
        f'INSERT INTO "{held.fixture.control_schema}".registry_approved_record '
        "(approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) "
        "VALUES($1,$2,$3,1,1,$4::jsonb)",
        revision,
        kind,
        str(key),
        json.dumps(document),
    )


async def _store_review(held, admission, *, missing=False, malformed=False):
    parameters = _parameters(admission.document)
    if malformed:
        parameters["approval_sha256"] = "0" * 64
        admission.mapping["source_scope_json"]["approval_sha256"] = "0" * 64
        admission.mapping["evidence_sha256"] = "0" * 64
    if not missing:
        await held.fixture.connection.execute(
            f'INSERT INTO "{held.fixture.control_schema}".registry_ptg_published_plan_scope '
            "(scope_id,scope_key,actor_key,idempotency_key,approval_sha256,approval_json) "
            "VALUES($1::uuid,$2,$3,$4,$5,$6::jsonb)",
            admission.document["scope_id"],
            parameters["scope_key"],
            parameters["actor_key"],
            parameters["idempotency_key"],
            parameters["approval_sha256"],
            parameters["document"],
        )


async def _seed_companies(held, revision, companies):
    for index, company in enumerate(companies):
        if index != 1:
            await _approved_row(
                held,
                revision,
                "company",
                company,
                {"company_id": company, "display_name": "Synthetic Company", "archived": index == 3},
            )
        if index != 2:
            await _approved_row(
                held,
                revision,
                "company_links",
                company,
                {"company_id": company, "network_ids": [_ALLOWED, _EXCLUDED, _ARCHIVED], "archived": False},
            )
    for network in (_ALLOWED, _EXCLUDED, _ARCHIVED):
        await _approved_row(
            held,
            revision,
            "network",
            network,
            {"network_id": network, "display_name": "Synthetic Network", "archived": network == _ARCHIVED},
        )


async def _seed_sources(held, schema, revision, companies):
    definitions = [
        ("allowed", 0, _ALLOWED),
        ("excluded", 0, _EXCLUDED),
        ("archived", 0, _ALLOWED),
        ("network_archived", 0, _ARCHIVED),
        ("company_missing", 1, _ALLOWED),
        ("link_missing", 2, _ALLOWED),
        ("company_archived", 3, _ALLOWED),
        ("mixed", 0, _ALLOWED),
        ("public", 0, _ALLOWED),
        ("draft", 0, _ALLOWED),
        ("bad_hash", 0, _ALLOWED),
        ("missing_review", 0, _ALLOWED),
        ("manifest_changed", 0, _ALLOWED),
    ]
    reviews_by_name = {}
    for name, company_index, network in definitions:
        admission = _review(schema, name, companies[company_index], network, revision, archived=name == "archived")
        reviews_by_name[name] = admission
        await held.fixture.connection.execute(
            f"INSERT INTO \"{schema}\".ptg2_snapshot VALUES($1,'published',$2::jsonb)",
            admission.mapping["dataset_id"],
            json.dumps(admission.manifest),
        )
        if name != "public":
            await _store_review(
                held, admission, missing=name == "missing_review", malformed=name in {"bad_hash", "draft"}
            )
        if name not in {"public", "draft"}:
            await _approved_row(held, revision, "network_binding", admission.mapping["binding_id"], admission.mapping)
    second = _review(schema, "mixed", companies[0], _EXCLUDED, revision)
    await _store_review(held, second)
    await _approved_row(held, revision, "network_binding", second.mapping["binding_id"], second.mapping)
    return reviews_by_name


@pytest.fixture
async def source_admission(profile_transport, monkeypatch):
    held = profile_transport
    schema = "source_admission_" + uuid4().hex
    candidates = []
    revision = held.manifest.approved_custom_revision + 1
    companies = [str(uuid4()) for _ in range(4)]
    try:
        await held.fixture.connection.execute(f'CREATE SCHEMA "{schema}"')
        await held.fixture.connection.execute(
            f'CREATE TABLE "{schema}".ptg2_snapshot(snapshot_id text PRIMARY KEY,status text,manifest jsonb)'
        )
        await held.fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{held.role}"')
        await held.fixture.connection.execute(f'GRANT SELECT ON "{schema}".ptg2_snapshot TO "{held.role}"')
        await held.fixture.connection.execute(
            f'INSERT INTO "{held.fixture.control_schema}".registry_approved_record '
            f"SELECT $1,record_kind,record_key,record_revision,custom_revision,record_json "
            f'FROM "{held.fixture.control_schema}".registry_approved_record WHERE approved_revision=$2',
            revision,
            held.manifest.approved_custom_revision,
        )
        await _seed_companies(held, revision, companies)
        reviews_by_name = await _seed_sources(held, schema, revision, companies)
        await held.fixture.connection.execute(
            f'UPDATE "{held.fixture.control_schema}".registry_revision_control '
            "SET approved_revision=$1,draft_revision=$1 WHERE id=1",
            revision,
        )
        async with held.fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            approved = await pin_approved_membership_source(
                held.fixture.connection, approved_revision=revision, control_schema=held.fixture.control_schema
            )
        manifest = await _compose_profile_manifest(held.fixture, held.manifest, approved, candidates)
        monkeypatch.setattr(policy, "PTG2_SCHEMA", schema)
        yield SimpleNamespace(
            held=held, schema=schema, reviews_by_name=reviews_by_name, manifest=manifest, companies=companies
        )
    finally:
        await _remove_candidates(held.fixture, candidates, uuid4())
        await held.fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _selection(reviewed_sources):
    bindings = tuple(
        PlanReleaseSnapshotBinding(
            index * 7 + 3,
            admission.mapping["dataset_id"],
            admission.mapping["source_key"],
            admission.mapping["source_scope_json"]["plan_id"],
            "group",
            "in_network",
            True,
        )
        for index, admission in enumerate(reviewed_sources)
    )
    return PlanReleaseServingSelection(
        "native-serving", "native-release", "native-plan", None, "2026-01", "published", "a" * 64, bindings
    )


async def _reader_identity(driver, held):
    identity = await driver.fetchrow(
        "SELECT session_user,current_user,current_setting('transaction_read_only'),"
        "current_setting('transaction_isolation'),pg_backend_pid(),"
        "(SELECT rolcanlogin FROM pg_roles WHERE rolname=current_user),"
        "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),pg_has_role(current_user,$1,'SET'),"
        "has_table_privilege(current_user,$2,'INSERT,UPDATE,DELETE,TRUNCATE'),"
        "has_table_privilege(current_user,'mrf.npi','INSERT,UPDATE,DELETE,TRUNCATE')",
        held.fixture.roles["owner"],
        held.fixture.control_schema + ".registry_approved_record",
    )
    assert tuple(identity)[:4] == (held.role, held.role, "on", "repeatable read")
    assert tuple(identity)[5:] == (True, False, False, False, False)
    return identity[4]


async def _admit(admission, selection, *, excluded=(_EXCLUDED,)):
    held = admission.held
    document = v2_envelope({}, excluded_network_ids=list(excluded))
    _, exclusions, digest, organization = parse_managed_provider_policy(document)
    queries = []
    held.writer_statements.clear()
    async with held.database.reader_session() as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        driver.add_query_logger(queries.append)
        try:
            pid = await _reader_identity(driver, held)
            assert current_session() is session and session.info["api_reader_verified"]
            async with managed_provider_network_read_scope(
                session,
                selector=None,
                network_generation=admission.manifest.generation_id,
                excluded_network_ids=exclusions,
                access_scope_sha256=digest,
                organization_policy=organization,
            ) as manifest:
                assert manifest == admission.manifest
                admitted = await policy.admitted_plan_release_bindings(session, selection)
                assert await driver.fetchval("SELECT pg_backend_pid()") == pid
                assert not await driver.fetchval(
                    "SELECT has_table_privilege(current_user,$1,'INSERT,UPDATE,DELETE,TRUNCATE')",
                    admission.schema + ".ptg2_snapshot",
                )
        finally:
            await asyncio.sleep(0)
            driver.remove_query_logger(queries.append)
    assert sum("binding_matches AS MATERIALIZED" in query.query for query in queries) == 1
    assert sum("validation_json::text AS validation_json" in query.query for query in queries) == 1
    batches = [query for query in queries if query.query.startswith("WITH source_rows AS MATERIALIZED")]
    assert len(batches) == 3 and all(query.exception is None for query in batches)
    assert batches[1].args[1] == batches[2].args[1] == admission.manifest.approved_custom_revision
    assert not held.database.has_reader_session() and held.database.engine is None
    assert not held.writer_statements
    return admitted


async def test_native_source_admission_policy_retained_revision_and_bulk(source_admission):
    admission = source_admission
    names = [
        "allowed",
        "excluded",
        "archived",
        "network_archived",
        "company_missing",
        "link_missing",
        "company_archived",
        "mixed",
        "public",
        "draft",
    ]
    selection = _selection([admission.reviews_by_name[name] for name in names])
    original = selection.bindings
    admitted = await _admit(admission, selection)
    assert admitted == tuple(original[index] for index in (0, 7, 8, 9)) and selection.bindings is original
    assert all(binding is original[index] for binding, index in zip(admitted, (0, 7, 8, 9), strict=True))
    assert (
        await _admit(admission, _selection([admission.reviews_by_name["allowed"]]), excluded=(_ALLOWED, _EXCLUDED))
    ) == ()
    await _advance_approval(admission)
    assert await _admit(admission, selection) == admitted
    public = _selection([admission.reviews_by_name["public"]]).bindings[0]
    bulk = replace(
        selection,
        bindings=tuple(replace(public, binding_ordinal=index, plan_id="public_" + str(index)) for index in range(50)),
    )
    assert await _admit(admission, bulk) == bulk.bindings


async def _advance_approval(admission):
    held = admission.held
    future = admission.manifest.approved_custom_revision + 1
    await held.fixture.connection.execute(
        f'INSERT INTO "{held.fixture.control_schema}".registry_approved_record '
        f"SELECT $1,record_kind,record_key,record_revision,custom_revision,record_json "
        f'FROM "{held.fixture.control_schema}".registry_approved_record WHERE approved_revision=$2',
        future,
        admission.manifest.approved_custom_revision,
    )
    await held.fixture.connection.execute(
        f'UPDATE "{held.fixture.control_schema}".registry_approved_record '
        "SET record_json=record_json||'{\"archived\":true}'::jsonb WHERE approved_revision=$1 AND record_kind='company' AND record_key=$2",
        future,
        admission.companies[0],
    )
    await _approved_row(
        held,
        future,
        "network_binding",
        admission.reviews_by_name["draft"].mapping["binding_id"],
        admission.reviews_by_name["draft"].mapping,
    )
    await held.fixture.connection.execute(
        f'UPDATE "{held.fixture.control_schema}".registry_revision_control '
        "SET approved_revision=$1,draft_revision=$1 WHERE id=1",
        future,
    )
    assert (
        await held.fixture.connection.fetchval(
            f'SELECT approved_revision FROM "{held.fixture.control_schema}".registry_revision_control'
        )
        > admission.manifest.approved_custom_revision
    )


async def test_native_source_admission_rejects_anchored_damage(source_admission):
    admission = source_admission
    for name in ("bad_hash", "missing_review"):
        with pytest.raises(PlanPricingProjectionUnavailable, match="^managed plan source admission unavailable$"):
            await _admit(admission, _selection([admission.reviews_by_name[name]]))
    target = admission.reviews_by_name["manifest_changed"]
    await admission.held.fixture.connection.execute(
        f'UPDATE "{admission.schema}".ptg2_snapshot '
        'SET manifest=manifest||\'{"activation":{"retained":true}}\'::jsonb WHERE snapshot_id=$1',
        target.mapping["dataset_id"],
    )
    assert await _admit(admission, _selection([target]))
    await admission.held.fixture.connection.execute(
        f'UPDATE "{admission.schema}".ptg2_snapshot '
        "SET manifest=manifest||'{\"immutable_changed\":true}'::jsonb WHERE snapshot_id=$1",
        target.mapping["dataset_id"],
    )
    with pytest.raises(PlanPricingProjectionUnavailable, match="^managed plan source admission unavailable$"):
        await _admit(admission, _selection([target]))


@pytest.fixture
def contributor_company_inputs(monkeypatch):
    from tests.provider_contributor_authority_postgres_support import contributor_company_inputs as prepare_inputs

    return prepare_inputs.__wrapped__(monkeypatch)


@pytest.fixture
def billing_office_inputs(monkeypatch):
    from tests.managed_billing_sql_postgres_support import billing_office_inputs as prepare_inputs

    return prepare_inputs.__wrapped__(monkeypatch)


@pytest.mark.usefixtures("contributor_company_inputs")
async def test_native_hidden_company_source_contributors(source_admission):
    from tests.provider_contributor_authority_postgres_support import (
        test_native_hidden_company_sources_cannot_use_public_shared_office,
    )

    await test_native_hidden_company_sources_cannot_use_public_shared_office(source_admission)


@pytest.mark.usefixtures("billing_office_inputs", "contributor_company_inputs")
async def test_native_selected_company_source_exact_office(source_admission, monkeypatch):
    from tests.provider_contributor_authority_postgres_support import (
        test_native_selected_reviewed_company_source_keeps_exact_retained_office,
    )

    await test_native_selected_reviewed_company_source_keeps_exact_retained_office(source_admission, monkeypatch)


@pytest.fixture
def typed_plan_inputs(monkeypatch):
    from tests.provider_service_legacy_postgres_support import typed_plan_inputs as prepare_inputs

    return prepare_inputs.__wrapped__(monkeypatch)


@pytest.mark.parametrize("selected", [False, True])
async def test_native_legacy_release_preserves_originals_and_admits_before_readiness(
    source_admission, monkeypatch, selected
):
    from tests.provider_service_legacy_postgres_support import (
        test_native_legacy_release_preserves_originals_and_admits_before_readiness as run_case,
    )

    await run_case(source_admission, monkeypatch, selected)


async def test_native_legacy_ambiguous_releases_refuse_before_source_probe(source_admission, monkeypatch):
    from tests.provider_service_legacy_postgres_support import (
        test_native_legacy_ambiguous_releases_refuse_before_source_probe as run_case,
    )

    await run_case(source_admission, monkeypatch)


@pytest.mark.usefixtures("typed_plan_inputs")
@pytest.mark.parametrize("kind,ordinal", [("ein", 3), ("hios", 10)])
@pytest.mark.parametrize("selected", [False, True])
async def test_native_typed_legacy_uses_commitment_then_exact_retained_rows(
    source_admission, monkeypatch, kind, ordinal, selected
):
    from tests.provider_service_legacy_postgres_support import (
        test_native_typed_legacy_uses_commitment_then_exact_retained_rows as run_case,
    )

    await run_case(source_admission, monkeypatch, kind, ordinal, selected)


@pytest.mark.usefixtures("typed_plan_inputs")
@pytest.mark.parametrize("damage", ["absent", "type", "payload", "source", "commitment", "conflict"])
async def test_native_typed_legacy_rejects_changed_or_missing_evidence(source_admission, monkeypatch, damage):
    from tests.provider_service_legacy_postgres_support import (
        test_native_typed_legacy_rejects_changed_or_missing_evidence as run_case,
    )

    await run_case(source_admission, monkeypatch, damage)


@pytest.mark.usefixtures("typed_plan_inputs")
async def test_native_typed_allowed_original_role_and_blank_market_are_exact(source_admission, monkeypatch):
    from tests.provider_service_legacy_postgres_support import (
        test_native_typed_allowed_original_role_and_blank_market_are_exact as run_case,
    )

    await run_case(source_admission, monkeypatch)


@pytest.mark.usefixtures("typed_plan_inputs")
async def test_native_typed_legacy_ambiguous_commitments_refuse(source_admission, monkeypatch):
    from tests.provider_service_legacy_postgres_support import (
        test_native_typed_legacy_ambiguous_commitments_refuse as run_case,
    )

    await run_case(source_admission, monkeypatch)


@pytest.mark.usefixtures("typed_plan_inputs")
async def test_native_explicit_type_disambiguates_distinct_published_release_namespaces(source_admission, monkeypatch):
    from tests.provider_service_legacy_postgres_support import (
        test_native_explicit_type_disambiguates_distinct_published_release_namespaces as run_case,
    )

    await run_case(source_admission, monkeypatch)
