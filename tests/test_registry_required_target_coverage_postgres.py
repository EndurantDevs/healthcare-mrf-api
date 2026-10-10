# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete retained-target coverage through real approved exact-office composition."""

import csv
import io
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import network_approved_catalog_evidence as catalog_evidence
from process import network_approved_membership_source as membership_source
from process.registry_required_target_coverage import (
    RegistryRequiredTargetCoverageError,
    RegistryRequiredTargetCoverageUnavailable,
    read_registry_required_target_coverage,
)
from process.registry_required_target_store import admit_registry_required_targets
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_provider_routes_postgres import _composed_candidate, _durable_state
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _approve, _command, _create
from tests.test_registry_candidate_composition_postgres import _imported_candidate, _remove_candidates
from tests.test_registry_company_links_postgres import _assertion, _links
from tests.test_registry_network_binding_approval_postgres import _bind, _binding
from tests.test_registry_required_target_review_store_postgres import _SOURCE_FIELDS, _review
from tests.test_registry_required_target_store_postgres import _HEADERS, _edition

pytestmark = pytest.mark.asyncio


def _input(extra=False):
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(_HEADERS)
    for source_id, ribbon in [("123", "12345678-1234-5678-8123-123456789abc"), ("456", ""), ("", "")]:
        writer.writerow(
            (
                "Example",
                "Example Company",
                "Example Plan",
                "PPO",
                "Example Carrier",
                "Example Network",
                source_id,
                json.dumps([ribbon]) if ribbon else "",
            )
        )
    if extra:
        writer.writerow(("Other", "Other Company", "Other Plan", "HMO", "Other Carrier", "Other Network", "789", ""))
    return output.getvalue().encode()


async def _ledger(fixture, *, extra=False):
    input_bytes = _input(extra)
    edition = _edition(input_bytes)
    async with fixture.connection.transaction():
        receipt = await admit_registry_required_targets(
            fixture.connection, input_bytes, edition, control_schema=fixture.control_schema
        )
    targets = json.loads(
        await fixture.connection.fetchval(
            f"SELECT observation_json#>'{{ledger,targets}}' FROM \"{fixture.control_schema}\".registry_source_observation WHERE snapshot_id=$1",
            edition.snapshot_id,
        )
    )
    return edition, receipt, targets


def _decision(key, row):
    return {
        "target_key": key,
        "resolution_status": "resolved",
        "network_id": row["network_id"],
        "source_binding": {field: row[field] for field in _SOURCE_FIELDS},
        "evidence_reference": "https://example.test/network-review",
        "evidence_sha256": "b" * 64,
        "reason": "Verified source identity",
    }


async def _approved_conflict_review(fixture, seed, edition, document, required_targets, scenario):
    scenario_context_dict = {}
    selected_records = []
    if scenario == "different_networks":
        other_network = await _draft(fixture, _create("network"), seed.actor)
        selected_records.append(other_network)
        scenario_context_dict["conflicting_network_ids"] = {seed.network["record_id"], other_network["record_id"]}
        other_row = _binding(other_network["record_id"])
        other_row["source_key"] = "additional-reviewed-source"
        decisions = [_decision(required_targets[0]["target_key"], other_row)]
    else:
        other_row = _binding(seed.network["record_id"])
        other_row["source_key"] = "additional-reviewed-source"
        decisions = [
            _decision(required_targets[1]["target_key"], other_row),
            {
                **document["decisions"][0],
                "resolution_status": "conflicting",
                "network_id": None,
                "source_binding": None,
                "reason": "Conflicting source evidence remains unresolved",
            },
        ]
    other_edition, other_review = await _review(
        (fixture.connection, fixture.control_schema, fixture.engine),
        edition,
        {**document, "decisions": decisions},
    )
    other_row.update(evidence_id=other_review["evidence_id"], evidence_sha256=other_review["artifact_sha256"])
    other_binding = (await _bind(fixture.connection, fixture.control_schema, other_row, seed.actor))["records"][0]
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *selected_records, other_binding),
        seed.actor,
    )
    seed.revision = approval["approved_revision"]
    scenario_context_dict.update(other_review=other_review, other_review_edition=other_edition)
    return scenario_context_dict


async def _approved_company_link_with_pending_clear(fixture, seed):
    company = await _draft(fixture, _create("company"), seed.actor)
    group = await _draft(fixture, _create("group"), seed.actor)
    grouped_company = await _draft(fixture, _create("company"), seed.actor)
    link_command = _links(company, [seed.network], group)
    link_command = replace(
        link_command,
        fields={**link_command.fields, "network_assertions": [_assertion(company, seed.network)]},
    )
    link = await _draft(fixture, link_command, seed.actor)
    group_only_link = await _draft(fixture, _links(grouped_company, group=group), seed.actor)
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(
            fixture.connection, fixture.control_schema, company, group, grouped_company, link, group_only_link
        ),
        seed.actor,
    )
    seed.revision = approval["approved_revision"]
    pending_link = await _draft(
        fixture,
        replace(
            link_command,
            operation="correct",
            expected_revision=1,
            fields={"network_ids": [], "group_id": None, "network_assertions": []},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    return {"company": company, "grouped_company": grouped_company, "link": link, "pending_link": pending_link}


@pytest.fixture
async def coverage_db(custom_db, request):
    fixture = custom_db
    copy_targets = []
    request_id = uuid4()
    try:
        source_manifest = await _imported_candidate(fixture, copy_targets)
        seed = await _seed(fixture)
        edition, ledger, required_targets = await _ledger(fixture)
        reviewed_binding = _binding(seed.network["record_id"])
        review_document_dict = {
            "ledger_snapshot_id": str(edition.snapshot_id),
            "ledger_artifact_sha256": ledger["artifact_sha256"],
            "decisions": [
                _decision(required_target["target_key"], reviewed_binding) for required_target in required_targets[:2]
            ],
        }
        review_edition, review = await _review(
            (fixture.connection, fixture.control_schema, fixture.engine), edition, review_document_dict
        )
        reviewed_binding.update(evidence_id=review["evidence_id"], evidence_sha256=review["artifact_sha256"])
        binding = (await _bind(fixture.connection, fixture.control_schema, reviewed_binding, seed.actor))["records"][0]
        approval = await _approve(
            fixture.connection,
            fixture.control_schema,
            await _command(fixture.connection, fixture.control_schema, binding),
            seed.actor,
        )
        seed.revision = approval["approved_revision"]
        scenario = getattr(request, "param", None)
        scenario_context_dict = {}
        if scenario in ("different_networks", "explicit_conflict"):
            scenario_context_dict = await _approved_conflict_review(
                fixture, seed, edition, review_document_dict, required_targets, scenario
            )
        elif scenario == "company_link":
            scenario_context_dict = await _approved_company_link_with_pending_clear(fixture, seed)
        await _composed_candidate(fixture, seed, source_manifest, request_id, copy_targets)
        yield SimpleNamespace(
            fixture=fixture,
            seed=seed,
            edition=edition,
            ledger=ledger,
            targets=required_targets,
            row=reviewed_binding,
            review=review,
            review_edition=review_edition,
            document=review_document_dict,
            schema=copy_targets[-1].schema_name,
            **scenario_context_dict,
        )
    finally:
        await _remove_candidates(fixture, copy_targets, request_id)


async def _read(context, *, edition=None, generation_id=None):
    fixture = context.fixture
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        return await read_registry_required_target_coverage(
            fixture.connection,
            (edition or context.edition).snapshot_id,
            generation_id=generation_id,
            control_schema=fixture.control_schema,
        )


async def test_all_targets_exact_office_shared_network_and_unknown_pricing(coverage_db):
    context = coverage_db
    report = await _read(context)
    assert len(report["targets"]) == report["totals"]["ledger_targets"] == 3
    mapped_targets = [target for target in report["targets"] if target["mapping_status"] == "resolved"]
    assert len(mapped_targets) == report["totals"]["mapped_targets"] == report["totals"]["directory_available"] == 2
    assert report["totals"]["exact_membership_count"] == 1
    assert report["totals"]["pricing_evidence_count"] is None
    assert report["totals"]["priceable"] == 0
    assert all(target["priceable"] is None and "pricing_not_assessed" in target["gaps"] for target in mapped_targets)
    assert report["totals"]["unresolved_location_count"] == 0
    assert report["provenance"]["approved_map_pin"] == "manifest"
    assert report["provenance"]["reviews"] == [
        {"snapshot_id": str(context.review_edition.snapshot_id), "artifact_sha256": context.review["artifact_sha256"]}
    ]
    assert report["assessment"]["source_status"] == "selected_serving_generation_lineage"
    assert context.seed.second["record_id"] not in json.dumps(report["provenance"]["target_evidence"])
    assert (
        await context.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{context.schema}".network_membership WHERE network_id=$1',
            context.seed.network["record_id"],
        )
        == 1
    )


@pytest.mark.parametrize("coverage_db", ["different_networks"], indirect=True)
async def test_two_approved_reviewed_canonical_ids_leave_target_conflicting_without_a_winner(coverage_db):
    context = coverage_db
    report = await _read(context)
    target = next(target for target in report["targets"] if target["target_key"] == context.targets[0]["target_key"])
    assert target["mapping_status"] == "conflicting" and target["network_id"] is None
    assert not target["directory_available"] and not target["company_link_verified"] and target["priceable"] is None
    assert "mapping_conflicting" in target["gaps"]
    assert report["totals"]["mapped_targets"] == report["totals"]["directory_available"] == 1
    evidence = next(
        entry for entry in report["provenance"]["target_evidence"] if entry["target_key"] == target["target_key"]
    )
    assert evidence["agreed_id_count"] == 2 and evidence["agreed_network_id"] is None
    assert evidence["network_id"] is None
    assert {
        review["decision"]["network_id"] for review in evidence["review_provenance"]
    } == context.conflicting_network_ids
    assert all(review["approved_exact_binding"] for review in evidence["review_provenance"])
    assert len(report["provenance"]["reviews"]) == 2


@pytest.mark.parametrize("coverage_db", ["explicit_conflict"], indirect=True)
async def test_explicit_conflict_in_approved_review_is_kept_with_its_resolved_anchor(coverage_db):
    context = coverage_db
    report = await _read(context)
    coverage_by_target = {target["target_key"]: target for target in report["targets"]}
    conflicted = coverage_by_target[context.targets[0]["target_key"]]
    assert conflicted["mapping_status"] == "conflicting" and conflicted["network_id"] is None
    assert not conflicted["directory_available"] and "mapping_conflicting" in conflicted["gaps"]
    anchor = coverage_by_target[context.targets[1]["target_key"]]
    assert anchor["mapping_status"] == "resolved" and anchor["network_id"] == context.seed.network["record_id"]
    assert anchor["directory_available"]
    evidence_by_target = {target["target_key"]: target for target in report["provenance"]["target_evidence"]}
    review_id = str(context.other_review_edition.snapshot_id)
    conflict = next(
        review
        for review in evidence_by_target[conflicted["target_key"]]["review_provenance"]
        if review["review_snapshot_id"] == review_id
    )
    assert conflict["decision"]["resolution_status"] == "conflicting" and not conflict["approved_exact_binding"]
    assert any(
        review["review_snapshot_id"] == review_id and review["approved_exact_binding"]
        for review in evidence_by_target[anchor["target_key"]]["review_provenance"]
    )
    assert {"snapshot_id": review_id, "artifact_sha256": context.other_review["artifact_sha256"]} in report[
        "provenance"
    ]["reviews"]


@pytest.mark.parametrize("coverage_db", ["company_link"], indirect=True)
async def test_active_approved_company_link_excludes_pending_clear_and_group_inference(coverage_db):
    context = coverage_db
    report = await _read(context)
    mapped_targets = [target for target in report["targets"] if target["mapping_status"] == "resolved"]
    assert len(mapped_targets) == report["totals"]["company_link_verified"] == 2
    assert all(
        target["company_link_verified"] and "company_link_unresolved" not in target["gaps"] for target in mapped_targets
    )
    assert context.pending_link["revision"] == 2 and context.pending_link["record"]["network_ids"] == []
    for evidence in report["provenance"]["target_evidence"]:
        if evidence["mapping_status"] != "resolved":
            continue
        links = evidence["company_link_provenance"]
        assert len(links) == 1 and links[0]["company_id"] == context.company["record_id"]
        assert links[0]["network_id"] == context.seed.network["record_id"] and links[0]["record_revision"] == 1
        assert links[0]["explicit_assertions"] == context.link["record"]["network_assertions"]
        assert context.grouped_company["record_id"] not in json.dumps(links)
    approved = await context.fixture.connection.fetch(
        f'SELECT record_kind,record_json FROM "{context.fixture.control_schema}".registry_approved_record '
        "WHERE approved_revision=$1 AND ((record_kind='company' AND record_key=$2) "
        "OR (record_kind='network' AND record_key=$3))",
        context.seed.revision,
        context.company["record_id"],
        str(context.seed.network["record_id"]),
    )
    assert {record["record_kind"] for record in approved} == {"company", "network"}
    assert all(json.loads(record["record_json"])["archived"] is False for record in approved)


async def test_read_only_reader_without_owner_uses_nine_set_queries(coverage_db):
    context = coverage_db
    fixture = context.fixture
    expected = await _read(context)
    before = await _durable_state(fixture)
    sequences_sql = "SELECT sequencename,last_value FROM pg_sequences WHERE schemaname=$1 ORDER BY sequencename"
    sequences_before = await fixture.connection.fetch(sequences_sql, fixture.control_schema)
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"')
    await fixture.connection.execute(
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
    )
    statements = []

    class Reader:
        def is_in_transaction(self):
            return fixture.connection.is_in_transaction()

        async def fetchrow(self, statement, *arguments):
            assert statement.lstrip().startswith(("SELECT", "WITH"))
            assert "FOR SHARE" not in statement.upper() and "FOR UPDATE" not in statement.upper()
            statements.append(statement)
            return await fixture.connection.fetchrow(statement, *arguments)

    try:
        await fixture.connection.execute(f'SET ROLE "{fixture.roles["reader"]}"')
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            settings = await fixture.connection.fetchrow(
                "SELECT current_user,current_setting('transaction_isolation') AS isolation,"
                "current_setting('transaction_read_only') AS readonly,pg_has_role(current_user,$1,'SET') AS can_set_owner",
                fixture.roles["owner"],
            )
            assert tuple(settings) == (fixture.roles["reader"], "repeatable read", "on", False)
            assert (
                await read_registry_required_target_coverage(
                    Reader(), context.edition.snapshot_id, control_schema=fixture.control_schema
                )
                == expected
            )
    finally:
        await fixture.connection.execute("RESET ROLE")
    assert len(statements) == 9
    namespace = f'"{fixture.control_schema}"'
    assert statements[-2] == membership_source._SCOPE_SQL.format(
        namespace=namespace, control_source="(SELECT $1::bigint AS approved_revision,1 AS id)"
    )
    assert statements[-1] == catalog_evidence._PAGE_SQL.format(namespace=namespace)
    assert await _durable_state(fixture) == before
    assert await fixture.connection.fetch(sequences_sql, fixture.control_schema) == sequences_before


async def test_unreviewed_ledger_is_native_verified_and_keeps_every_target(coverage_db):
    context = coverage_db
    edition, receipt, _ = await _ledger(context.fixture, extra=True)
    report = await _read(context, edition=edition)
    assert report["totals"]["ledger_targets"] == 4
    assert report["totals"]["mapped_targets"] == report["totals"]["directory_available"] == 0
    assert all(target["mapping_status"] == "missing" and target["priceable"] is None for target in report["targets"])
    assert report["provenance"]["ledger"]["artifact_sha256"] == receipt["artifact_sha256"]
    assert report["provenance"]["ledger"]["source_rows"] == 4


async def test_new_unapproved_review_and_draft_do_not_change_pinned_report(coverage_db):
    context = coverage_db
    before = await _read(context)
    pending_review_dict = {
        **context.document,
        "decisions": [{**context.document["decisions"][0], "reason": "Pending second review"}],
    }
    await _review(
        (context.fixture.connection, context.fixture.control_schema, context.fixture.engine),
        context.edition,
        pending_review_dict,
    )
    pending_binding_dict = {
        **context.row,
        "binding_id": str(uuid4()),
        "source_key": "pending-source",
        "evidence_id": "pending-review",
        "evidence_sha256": "c" * 64,
    }
    await _bind(context.fixture.connection, context.fixture.control_schema, pending_binding_dict, context.seed.actor)
    assert await _read(context) == before


async def test_approval_head_advance_preserves_old_manifest_mapping(coverage_db):
    context = coverage_db
    before = await _read(context)
    approved_binding_dict = {
        **context.row,
        "binding_id": str(uuid4()),
        "source_key": "new-approved-source",
        "evidence_id": "separate-approved-evidence",
        "evidence_sha256": "c" * 64,
    }
    binding = (
        await _bind(
            context.fixture.connection, context.fixture.control_schema, approved_binding_dict, context.seed.actor
        )
    )["records"][0]
    approval = await _approve(
        context.fixture.connection,
        context.fixture.control_schema,
        await _command(context.fixture.connection, context.fixture.control_schema, binding),
        context.seed.actor,
    )
    assert approval["approved_revision"] > before["provenance"]["serving"]["approved_custom_revision"]
    assert await _read(context, generation_id=before["provenance"]["serving"]["generation_id"]) == before


@pytest.mark.parametrize("changed", ["review", "ledger"])
async def test_retained_body_drift_is_refused_even_when_metadata_digest_is_unchanged(coverage_db, changed):
    context = coverage_db
    snapshot_id = context.review_edition.snapshot_id if changed == "review" else context.edition.snapshot_id
    path = ["decisions", "0", "reason"] if changed == "review" else ["ledger", "observations", "0", "raw_cells", "0"]
    await context.fixture.connection.execute(
        f'UPDATE "{context.fixture.control_schema}".registry_source_observation SET observation_json=jsonb_set(observation_json,$2,\'"changed"\'::jsonb) WHERE snapshot_id=$1',
        snapshot_id,
        path,
    )
    with pytest.raises(RegistryRequiredTargetCoverageUnavailable):
        await _read(context)


@pytest.mark.parametrize("changed", ["address", "reverse_binding"])
async def test_exact_serving_integrity_damage_never_becomes_positive_directory_evidence(coverage_db, changed):
    context = coverage_db
    if changed == "address":
        await context.fixture.connection.execute(
            f"DELETE FROM \"{context.schema}\".entity_address_unified WHERE entity_type='manual'"
        )
    else:
        await context.fixture.connection.execute(
            f"INSERT INTO \"{context.schema}\".provider_location_binding SELECT provider_system,provider_id,$1,location_key,entity_type,'different-entity' FROM \"{context.schema}\".provider_location_binding WHERE provider_system='manual'",
            uuid4(),
        )
    with pytest.raises(RegistryRequiredTargetCoverageUnavailable):
        await _read(context)


async def test_duplicate_raw_membership_is_counted_as_one_exact_site(coverage_db):
    context = coverage_db
    await context.fixture.connection.execute(
        f'INSERT INTO "{context.schema}".network_membership SELECT * FROM "{context.schema}".network_membership WHERE network_id=$1',
        context.seed.network["record_id"],
    )
    report = await _read(context)
    assert report["totals"]["exact_membership_count"] == 1
    assert report["totals"]["directory_available"] == 2


async def test_read_only_repeatable_transaction_and_selectors_are_required(coverage_db):
    fixture = coverage_db.fixture
    with pytest.raises(RegistryRequiredTargetCoverageError):
        await read_registry_required_target_coverage(
            fixture.connection, coverage_db.edition.snapshot_id, control_schema=fixture.control_schema
        )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(RegistryRequiredTargetCoverageError):
            await read_registry_required_target_coverage(
                fixture.connection, coverage_db.edition.snapshot_id, control_schema=fixture.control_schema
            )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        for selector in ("not-a-uuid", uuid4().hex, "00000000-0000-0000-0000-000000000000", True):
            with pytest.raises(RegistryRequiredTargetCoverageError):
                await read_registry_required_target_coverage(
                    fixture.connection, selector, control_schema=fixture.control_schema
                )
