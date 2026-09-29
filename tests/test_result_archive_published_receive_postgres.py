# See LICENSE.
"""Native proof of result-authorized local initialization and fresh audit handoff."""

from __future__ import annotations

import datetime
import json
from dataclasses import replace

import pytest
from sqlalchemy import text

from db.connection import db
from process.ptg_candidate_audit import load_candidate_audit_target
from process.ptg_parts import ptg2_candidate_attestation, source_pointers
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_preparation as preparation
from process.ptg_parts import result_archive_candidate_validation as validation
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportInput,
    BatchAuditReportTarget,
    build_batch_audit_report,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchWitnessBinding,
    build_audit_batch_request,
    matched_audit_batch_digest,
    parse_audit_batch_response,
)
from process.ptg_parts.ptg2_candidate_audit_contract import FastAuditHttpConfig
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
    invalid_price_exclusion_evidence,
    invalid_price_exclusion_policy,
    invalid_price_exclusion_source,
    invalid_price_value_sha256,
)
from process.ptg_parts.ptg2_shared_source_set import shared_source_set_metadata
from process.ptg_parts.result_archive_adoption import RESULT_ARCHIVE_ADOPTION_CONTRACT, PreparedResultArchiveLayout
from process.ptg_parts.result_archive_published_authority import _authority_from_identity
from process.ptg_parts.result_archive_published_identity import load_published_result_identity
from process.ptg_parts.result_archive_receive_binding import authenticate_published_result_stage
from tests.test_ptg2_batch_candidate_audit import _response_payload
from tests.test_result_archive_candidate_initialization_postgres import _caller_transaction, native_candidate
from tests.test_result_archive_candidate_validation_postgres import (
    _install_validation_tables,
    _seed_incumbent,
    _seed_local_layout,
)
from tests.test_result_archive_published_identity import _audit_sample, _published_row, _source_witness


def _published_stage_metadata(fixture, *, single_source=False):
    """Build synthetic serving evidence and its result-only publication manifest."""
    snapshot_by_field = _published_row()
    serving = snapshot_by_field["manifest"]["serving_index"]
    hashes = [descriptor["raw_sha256"] for descriptor in fixture.frozen_params["frozen_rate_files"]]
    if single_source:
        hashes = hashes[:1]
    source_set = shared_source_set_metadata(hashes)
    serving.update(
        shared_snapshot_key=1701,
        coverage_scope_id=(b"c" * 32).hex(),
        source_set=source_set,
        source_count=len(hashes),
        source_witness=_source_witness(source_set),
        audit_sample={**_audit_sample("ab" * 32), "source_count": len(hashes)},
        snapshot_map={"map_digest": "ab" * 32},
    )
    manifest_by_name = {
        "serving_index": serving,
        "activation": {
            "contract": "ptg2_candidate_activation_v1",
            "state": "activated",
            "source_key": "source_a",
        },
    }
    return snapshot_by_field, serving, manifest_by_name


async def _seed_published_finalizer(session, schema, snapshot_by_field):
    """Seed only the finalizer metadata consumed by this receiver proof."""
    # Finalizer shape belongs to the closure proof; only metadata is consumed here.
    await session.execute(
        text(
            f"CREATE TABLE {schema}.ptg2_v4_finalizer_map_root (snapshot_key bigint PRIMARY KEY,state text,contract text,map_format text,map_digest bytea)"
        )
    )
    await session.execute(
        text(f"INSERT INTO {schema}.ptg2_v4_finalizer_map_root VALUES (1701,'complete',:contract,:format,:digest)"),
        {
            "contract": snapshot_by_field["finalizer_contract"],
            "format": snapshot_by_field["finalizer_map_format"],
            "digest": b"f" * 32,
        },
    )


async def _seed_published_run(session, stage_fixture, serving, manifest_by_name, policy):
    """Seed the source run, snapshot, and layout in the caller's transaction."""
    schema = f'"{stage_fixture.destination_schema}"'
    await session.execute(text(f"CREATE TABLE {schema}.ptg2_artifact_manifest (snapshot_id text)"))
    await _seed_local_layout(session, stage_fixture)
    await session.execute(
        text(
            f"UPDATE {schema}.ptg2_v3_snapshot_layout SET layout_manifest=CAST(:manifest AS jsonb) WHERE snapshot_key=1701"
        ),
        {"manifest": json.dumps({"serving_index": serving})},
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.ptg2_import_run (import_run_id,import_month,status,started_at,heartbeat_at,options,report) VALUES ('legacy-run', DATE '2026-09-01','published',now(),now(),CAST(:options AS jsonb),'{{}}'::jsonb)"
        ),
        {
            "options": json.dumps(
                {"source_key": "source_a"}
                | ({INVALID_PRICE_EXCLUSION_POLICY_FIELD: policy} if policy is not None else {})
            )
        },
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.ptg2_snapshot (snapshot_id,import_run_id,import_month,status,created_at,published_at,manifest) VALUES ('legacy-source','legacy-run',DATE '2026-09-01','published',now(),now(),CAST(:manifest AS jsonb))"
        ),
        {"manifest": json.dumps(manifest_by_name)},
    )
    await session.execute(
        text(
            f"UPDATE {schema}.ptg2_v3_snapshot_binding SET snapshot_id='legacy-source' WHERE snapshot_id='local-candidate'"
        )
    )


async def _seed_published_scopes(session, fixture, snapshot_by_field, *, extra_plan_scope, price_policy):
    """Seed the source's plan scopes, source assignment, and finalizer."""
    schema = f'"{fixture.stage_schema}"'
    await session.execute(
        text(
            f"INSERT INTO {schema}.ptg2_v3_snapshot_scope SELECT 'legacy-source',plan_id,plan_market_type,coverage_scope_id FROM {schema}.ptg2_v3_snapshot_scope WHERE snapshot_id='source-snapshot'"
        )
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope SELECT 'legacy-source',plan_id,plan_market_type FROM {schema}.ptg2_v3_snapshot_plan_scope WHERE snapshot_id='source-snapshot'"
        )
    )
    if extra_plan_scope:
        await session.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope (snapshot_id,plan_id,plan_market_type) VALUES ('legacy-source','plan-transplant','market-a')"
            )
        )
    await session.execute(
        text(
            f"INSERT INTO {schema}.ptg2_v3_snapshot_source SELECT 'legacy-source',source_key,source_type,identity_kind,identity_sha256,raw_container_sha256,logical_json_sha256,logical_hash_deferred,source_trace_set_hash FROM {schema}.ptg2_v3_snapshot_source WHERE snapshot_id='source-snapshot'"
        )
    )
    if price_policy:
        await session.execute(
            text(
                f"DELETE FROM {schema}.ptg2_v3_snapshot_source WHERE snapshot_id='legacy-source' "
                "AND raw_container_sha256 <> :digest"
            ),
            {"digest": fixture.frozen_params["frozen_rate_files"][0]["raw_sha256"]},
        )
    await _seed_published_finalizer(session, schema, snapshot_by_field)


async def _published_stage(fixture, *, extra_plan_scope=False, price_policy=False):
    """Seed and authenticate one synthetic published stage in a caller transaction."""
    stage_fixture = replace(fixture, destination_schema=fixture.stage_schema)
    await _install_validation_tables(stage_fixture)
    await _install_validation_tables(fixture)
    snapshot_by_field, serving, manifest_by_name = _published_stage_metadata(fixture, single_source=price_policy)
    policy = None
    if price_policy:
        policy = invalid_price_exclusion_policy(
            [
                invalid_price_exclusion_source(
                    raw_source_sha256=fixture.frozen_params["frozen_rate_files"][0]["raw_sha256"],
                    entries=[
                        {
                            "object_ordinal": 0,
                            "rate_ordinal": 0,
                            "price_ordinal": 0,
                            "invalid_value_sha256": invalid_price_value_sha256("2027-02-30"),
                        }
                    ],
                    emptied_rate_count=0,
                )
            ]
        )
        serving["invalid_price_exclusion"] = invalid_price_exclusion_evidence(policy)
    async with _caller_transaction() as session:
        await _seed_published_run(session, stage_fixture, serving, manifest_by_name, policy)
        await _seed_published_scopes(
            session, fixture, snapshot_by_field, extra_plan_scope=extra_plan_scope, price_policy=price_policy
        )
        identity = await load_published_result_identity(
            session, schema_name=fixture.stage_schema, snapshot_id="legacy-source"
        )
    return _authority_from_identity("published-receive", identity).as_dict(), serving


async def _initialize(session, fixture, receipt, *, source_key="source_a"):
    return await initialization.initialize_result_archive_candidate(
        session,
        schema_name=fixture.destination_schema,
        staging_schema_name=fixture.stage_schema,
        source_snapshot_key=1701,
        destination_snapshot_id="local-candidate",
        frozen_binding_params={},
        authenticated_source_archive_metadata=receipt,
        reviewed_source_key=source_key,
    )


async def _assert_no_local_publication_authority(session, fixture):
    """Require preparation to create neither frozen provenance nor a serving pointer."""
    assert (
        await session.execute(
            text(f'SELECT count(*) FROM "{fixture.destination_schema}".ptg2_frozen_source_file_binding')
        )
    ).scalar_one() == 0
    assert (
        await session.execute(text(f'SELECT count(*) FROM "{fixture.destination_schema}".ptg2_current_source_snapshot'))
    ).scalar_one() == 0


async def _assert_persisted_audit_target(import_run_id):
    """Verify the persisted destination candidate is available to the native audit importer."""
    audit_target = await load_candidate_audit_target(candidate_run_id=import_run_id, snapshot_id="local-candidate")
    assert audit_target.snapshot_id == "local-candidate"
    assert audit_target.source_key == "source_a"


async def _install_activation_relations(fixture):
    """Install pointer relations; the separate guard contract has its own tests."""
    schema = f'"{fixture.destination_schema}"'
    for statement in (
        f"""CREATE TABLE {schema}.ptg2_current_plan_source (
            plan_source_key text PRIMARY KEY, plan_id text, plan_market_type text,
            import_month date, source_key text, snapshot_id text,
            previous_snapshot_id text, updated_at timestamp)""",
        f"""CREATE TABLE {schema}.ptg2_snapshot_pin (
            owner_type text, owner_id text, snapshot_id text, reason text,
            created_at timestamp, PRIMARY KEY (owner_type, owner_id, snapshot_id))""",
        f"""CREATE TABLE {schema}.ptg2_legacy_global_pointer_projection_queue (
            source_key text PRIMARY KEY, requested_generation bigint,
            applied_generation bigint, available_at timestamp,
            created_at timestamp, updated_at timestamp)""",
        f"""CREATE FUNCTION {schema}.guard_ptg2_v4_attempt(
            snapshot_id text, internal_run_id text, allow_reconciled boolean)
            RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$""",
    ):
        await db.execute_ddl(statement)


def _synthetic_response_payload(request, witness, audit_sample):
    response_fields = _response_payload(request.request_digest)
    challenge_count = request.challenge_count
    sample_count = audit_sample["sample_count"]
    response_fields.update(
        challenge_count=challenge_count,
        unique_challenge_count=challenge_count,
        matched_challenge_count=challenge_count,
        persisted_audit_occurrence_count=sample_count,
        validated_persisted_audit_occurrence_count=sample_count,
        matched_challenge_digest=matched_audit_batch_digest(request.request_digest, challenge_count),
    )
    response_fields["witness_io"].update(
        record_decodes=witness["record_count"],
        unique_evidence_entries=witness["evidence_dictionary_count"],
        evidence_decompressions=witness["evidence_dictionary_count"],
        evidence_sha256_hashes=witness["evidence_dictionary_count"],
        evidence_json_parses=witness["evidence_dictionary_count"],
        evidence_reuse_deliveries=witness["record_count"] - witness["evidence_dictionary_count"],
    )
    response_fields["candidate_processing_io"].update(
        candidate_occurrence_deliveries=challenge_count,
        unique_candidate_projections=challenge_count,
        candidate_projection_builds=challenge_count,
        candidate_projection_reuse_deliveries=0,
        availability_condition_count=challenge_count,
        duplicate_availability_deliveries=0,
    )
    return response_fields


def _synthetic_audit_report(fixture, serving):
    """Make a valid synthetic report, without claiming that the API audit ran."""
    raw_hashes = tuple(source_file["raw_sha256"] for source_file in fixture.frozen_params["frozen_rate_files"])
    audit_target = BatchAuditReportTarget(
        snapshot_id="local-candidate",
        source_key="source_a",
        plan_id="plan-a",
        plan_market_type="market-a",
        raw_container_sha256=raw_hashes,
        source_witness=serving["source_witness"],
        audit_sample=serving["audit_sample"],
        provider_identifier_quarantine=serving["provider_identifier_quarantine"],
        storage_generation="shared_blocks_v4",
    )
    witness = audit_target.source_witness
    request = build_audit_batch_request(
        snapshot_id=audit_target.snapshot_id,
        source_key=audit_target.source_key,
        plan_id=audit_target.plan_id,
        plan_market_type=audit_target.plan_market_type,
        witness_binding=AuditBatchWitnessBinding(
            audit_sample_digest=audit_target.audit_sample["sample_digest"],
            source_witness_sample_digest=witness["sample_digest"],
            source_witness_payload_sha256=witness["payload_sha256"],
            raw_container_sha256=raw_hashes,
            source_witness_occurrence_count=witness["occurrence_witness_count"],
        ),
    )
    response = parse_audit_batch_response(
        _synthetic_response_payload(request, witness, audit_target.audit_sample),
        request=request,
        expected_source_witness=witness,
        expected_audit_sample=audit_target.audit_sample,
    )
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    return build_batch_audit_report(
        BatchAuditReportInput(
            target=audit_target,
            request=request,
            response=response,
            http_config=FastAuditHttpConfig(
                api_base_url="https://candidate-api.internal.example",
                headers={},
                verify_tls=True,
                transport_contract="verified_https_v1",
            ),
            event_loop_contract="uvloop",
            started_at=completed_at - datetime.timedelta(seconds=1),
            completed_at=completed_at,
        )
    )


async def _seed_published_candidate_for_validation(session, fixture, receipt):
    prepared = await preparation.prepare_result_archive_candidate_evidence(
        session,
        schema_name=fixture.destination_schema,
        staging_schema_name=fixture.stage_schema,
        source_snapshot_key=1701,
        destination_snapshot_id="local-candidate",
        frozen_binding_params={},
        published_result_receipt=receipt,
    )
    await _seed_local_layout(session, fixture)
    await _seed_published_finalizer(session, f'"{fixture.destination_schema}"', _published_row())
    layout = PreparedResultArchiveLayout(
        contract=RESULT_ARCHIVE_ADOPTION_CONTRACT,
        source_snapshot_key=1701,
        destination_snapshot_id="local-candidate",
        destination_snapshot_key=1701,
        mapping_digest=bytes.fromhex("ab" * 32),
    )
    return prepared, layout


async def _prepare_published_candidate(session, fixture, receipt, serving, *, initialized=None):
    if initialized is None:
        initialized = await _initialize(session, fixture, receipt)
    prepared, layout = await _seed_published_candidate_for_validation(session, fixture, receipt)
    await session.execute(
        text(
            f'UPDATE "{fixture.destination_schema}".ptg2_v3_snapshot_layout SET layout_manifest=CAST(:manifest AS jsonb) WHERE snapshot_key=1701'
        ),
        {"manifest": json.dumps({"serving_index": serving})},
    )
    handoff = await validation.validate_result_archive_candidate_for_audit(
        session,
        schema_name=fixture.destination_schema,
        prepared_candidate=prepared,
        prepared_layout=layout,
    )
    return initialized, prepared, layout, handoff


@pytest.mark.asyncio
async def test_native_published_receive_replays_without_frozen_provenance_and_requires_fresh_audit(native_candidate):
    """Replay local preparation and retain only the normal fresh-audit handoff."""
    fixture = native_candidate
    receipt, serving = await _published_stage(fixture)
    async with _caller_transaction() as session:
        initialized = await _initialize(session, fixture, receipt)
        replay = await _initialize(session, fixture, receipt)
        assert initialized.reused is False and replay.reused is True
        initialized, prepared, layout, handoff = await _prepare_published_candidate(
            session, fixture, receipt, serving, initialized=initialized
        )
        assert prepared.authority_sha256 == initialized.authority_sha256
        assert prepared.frozen_binding_sha256 is initialized.frozen_binding_sha256 is None
        assert handoff.next_parameters == {
            "candidate_run_id": initialized.destination_import_run_id,
            "snapshot_id": "local-candidate",
            "candidate_audit_mode": "audit_only",
        }
        await _assert_no_local_publication_authority(session, fixture)
    async with _caller_transaction() as session:
        repeated = await validation.validate_result_archive_candidate_for_audit(
            session,
            schema_name=fixture.destination_schema,
            prepared_candidate=prepared,
            prepared_layout=layout,
        )
        assert repeated == handoff
    await _assert_persisted_audit_target(initialized.destination_import_run_id)
    async with _caller_transaction() as session:
        identity = await ptg2_candidate_attestation._locked_candidate_identity(
            session,
            schema_name=fixture.destination_schema,
            snapshot_id="local-candidate",
        )
        assert identity["source_key"] == "source_a"
        assert identity["storage_generation"] == "shared_blocks_v4"
    async with _caller_transaction() as session:
        with pytest.raises(validation.ResultArchiveCandidateValidationError, match="authority is invalid"):
            await validation.validate_result_archive_candidate_for_audit(
                session,
                schema_name=fixture.destination_schema,
                prepared_candidate=replace(prepared, authority_contract="invalid"),
                prepared_layout=layout,
            )
        await session.execute(
            text(
                f'UPDATE "{fixture.destination_schema}".ptg2_snapshot '
                "SET manifest = (manifest::jsonb - 'result_archive_source')::json "
                "WHERE snapshot_id='local-candidate'"
            )
        )
        with pytest.raises(validation.ResultArchiveCandidateValidationError, match="local authority differs"):
            await validation.validate_result_archive_candidate_for_audit(
                session,
                schema_name=fixture.destination_schema,
                prepared_candidate=prepared,
                prepared_layout=layout,
            )


@pytest.mark.asyncio
async def test_native_published_receive_normalizes_mixed_case_scope_order(native_candidate):
    fixture = native_candidate
    await _published_stage(fixture)
    async with _caller_transaction() as session:
        await session.execute(
            text(
                f'UPDATE "{fixture.stage_schema}".ptg2_v3_snapshot_plan_scope '
                "SET plan_market_type=upper(plan_market_type) WHERE snapshot_id='legacy-source'"
            )
        )
        await session.execute(
            text(
                f'INSERT INTO "{fixture.stage_schema}".ptg2_v3_snapshot_plan_scope '
                "(snapshot_id,plan_id,plan_market_type) "
                f"SELECT snapshot_id,plan_id,'aa-market' FROM \"{fixture.stage_schema}\".ptg2_v3_snapshot_plan_scope "
                "WHERE snapshot_id='legacy-source'"
            )
        )
        identity = await load_published_result_identity(
            session, schema_name=fixture.stage_schema, snapshot_id="legacy-source"
        )
        receipt = _authority_from_identity("published-receive", identity).as_dict()
        source_scopes = await initialization._staged_plan_scopes(
            session, staging_schema=fixture.stage_schema, source_snapshot_id="legacy-source"
        )
        assert tuple(market for _, market in source_scopes) == ("aa-market", "market-a")
        initialized = await _initialize(session, fixture, receipt)
        local_scopes = await initialization._staged_plan_scopes(
            session, staging_schema=fixture.destination_schema, source_snapshot_id="local-candidate"
        )
        assert local_scopes == source_scopes
        assert initialized.reused is False
        assert (await _initialize(session, fixture, receipt)).reused is True


@pytest.mark.asyncio
async def test_native_published_receive_rejects_mixed_inputs_and_changed_stage(native_candidate):
    fixture = native_candidate
    receipt, _ = await _published_stage(fixture)
    async with _caller_transaction() as session:
        with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="cannot carry frozen"):
            await preparation.prepare_result_archive_candidate_evidence(
                session,
                schema_name=fixture.destination_schema,
                staging_schema_name=fixture.stage_schema,
                source_snapshot_key=1701,
                destination_snapshot_id="local-candidate",
                frozen_binding_params={"unexpected": True},
                published_result_receipt=receipt,
            )
        with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="remapped destination"):
            await preparation.prepare_result_archive_candidate_evidence(
                session,
                schema_name=fixture.destination_schema,
                staging_schema_name=fixture.stage_schema,
                source_snapshot_key=1701,
                destination_snapshot_id=receipt["snapshot_id"],
                frozen_binding_params={},
                published_result_receipt=receipt,
            )
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="source layout differs"):
            await authenticate_published_result_stage(
                session, staging_schema_name=fixture.stage_schema, source_snapshot_key=1702, receipt=receipt
            )
        await session.execute(
            text(
                f'UPDATE "{fixture.stage_schema}".ptg2_snapshot '
                "SET manifest = (manifest::jsonb || '{\"changed\": true}'::jsonb)::json "
                "WHERE snapshot_id='legacy-source'"
            )
        )
        with pytest.raises(
            initialization.ResultArchiveCandidateInitializationError, match="restored authority differs"
        ):
            await authenticate_published_result_stage(
                session, staging_schema_name=fixture.stage_schema, source_snapshot_key=1701, receipt=receipt
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changed_part", "error"),
    [
        ("source_key", "layout differs"),
        ("map_digest", "layout differs"),
        ("finalizer_digest", "finalizer differs"),
    ],
)
async def test_native_published_receive_rejects_layout_from_another_result(native_candidate, changed_part, error):
    """A valid logical receipt cannot authorize a different sealed layout."""

    fixture = native_candidate
    receipt, serving_index = await _published_stage(fixture)
    schema = f'"{fixture.destination_schema}"'
    async with _caller_transaction() as session:
        await _initialize(session, fixture, receipt)
        prepared, layout = await _seed_published_candidate_for_validation(session, fixture, receipt)
        if changed_part == "source_key":
            layout = replace(layout, source_snapshot_key=1702)
        elif changed_part == "map_digest":
            other_digest = bytes.fromhex("cd" * 32)
            layout = replace(layout, mapping_digest=other_digest)
            serving_index = {
                **serving_index,
                "snapshot_map": {**serving_index["snapshot_map"], "map_digest": other_digest.hex()},
            }
            await session.execute(
                text(f"UPDATE {schema}.ptg2_v3_snapshot_layout SET mapping_digest=:digest WHERE snapshot_key=1701"),
                {"digest": other_digest},
            )
            await session.execute(
                text(f"UPDATE {schema}.ptg2_v4_snapshot_map_root SET map_digest=:digest WHERE snapshot_key=1701"),
                {"digest": other_digest},
            )
        else:
            await session.execute(
                text(f"UPDATE {schema}.ptg2_v4_finalizer_map_root SET map_digest=:digest WHERE snapshot_key=1701"),
                {"digest": b"z" * 32},
            )
        await session.execute(
            text(
                f"UPDATE {schema}.ptg2_v3_snapshot_layout SET layout_manifest=CAST(:manifest AS jsonb) WHERE snapshot_key=1701"
            ),
            {"manifest": json.dumps({"serving_index": serving_index})},
        )
        with pytest.raises(validation.ResultArchiveCandidateValidationError, match=error):
            await validation.validate_result_archive_candidate_for_audit(
                session,
                schema_name=fixture.destination_schema,
                prepared_candidate=prepared,
                prepared_layout=layout,
            )
        await _assert_no_local_publication_authority(session, fixture)


@pytest.mark.asyncio
async def test_native_published_receive_retains_singleton_invalid_price_policy(native_candidate):
    fixture = native_candidate
    receipt, serving = await _published_stage(fixture, price_policy=True)
    async with _caller_transaction() as session:
        initialized = await _initialize(session, fixture, receipt)
        assert initialized.source_count == 1
        result = await session.execute(
            text(f'SELECT options FROM "{fixture.destination_schema}".ptg2_import_run WHERE import_run_id=:run_id'),
            {"run_id": initialized.destination_import_run_id},
        )
        options = result.scalar_one()
        assert options[INVALID_PRICE_EXCLUSION_POLICY_FIELD]["source_count"] == 1
        assert options[INVALID_PRICE_EXCLUSION_POLICY_FIELD]["sha256"] == serving["invalid_price_exclusion"]["sha256"]
        await _prepare_published_candidate(session, fixture, receipt, serving, initialized=initialized)
    await _assert_persisted_audit_target(initialized.destination_import_run_id)


@pytest.mark.asyncio
async def test_native_published_receive_rejects_wrong_reviewed_scope_and_local_tamper(native_candidate):
    fixture = native_candidate
    receipt, _ = await _published_stage(fixture)
    with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="reviewed"):
        async with _caller_transaction() as session:
            await _initialize(session, fixture, receipt, source_key="other")
    async with _caller_transaction() as session:
        await _initialize(session, fixture, receipt)
        await session.execute(
            text(
                f"UPDATE \"{fixture.destination_schema}\".ptg2_v3_snapshot_source SET source_trace_set_hash=:hash WHERE snapshot_id='local-candidate'"
            ),
            {"hash": "ef" * 32},
        )
    with pytest.raises(preparation.ResultArchiveCandidatePreparationError, match="evidence differs"):
        async with _caller_transaction() as session:
            await preparation.prepare_result_archive_candidate_evidence(
                session,
                schema_name=fixture.destination_schema,
                staging_schema_name=fixture.stage_schema,
                source_snapshot_key=1701,
                destination_snapshot_id="local-candidate",
                frozen_binding_params={},
                published_result_receipt=receipt,
            )


async def _activate_published_candidate(session, fixture, digest, *, predecessor="destination-incumbent"):
    return await source_pointers.activate_ptg2_candidate_in_transaction(
        session,
        schema_name=fixture.destination_schema,
        source_key="source_a",
        snapshot_id="local-candidate",
        expected_current_snapshot_id=predecessor,
        expected_audit_only_attestation_digest=digest,
        rollback_owner_id="published-receive-test",
    )


async def _assert_source_tamper_refused(fixture, digest):
    schema = f'"{fixture.destination_schema}"'
    source_query = (
        f"SELECT raw_container_sha256 FROM {schema}.ptg2_v3_snapshot_source "
        "WHERE snapshot_id='local-candidate' AND source_key=0"
    )
    change_query = (
        f"UPDATE {schema}.ptg2_v3_snapshot_source SET raw_container_sha256=:digest "
        "WHERE snapshot_id='local-candidate' AND source_key=0"
    )
    original_digest = await db.scalar(source_query)
    await db.status(change_query, digest=(b"z" * 32).hex())
    with pytest.raises(ValueError, match="candidate manifest disagrees"):
        async with _caller_transaction() as session:
            await _activate_published_candidate(session, fixture, digest)
    await db.status(change_query, digest=str(original_digest))
    assert await db.scalar(source_query) == original_digest


async def _assert_wrong_approval_refused(fixture, digest):
    schema = f'"{fixture.destination_schema}"'
    with pytest.raises(source_pointers.PTG2SourcePointerConflict, match="predecessor"):
        async with _caller_transaction() as session:
            await _activate_published_candidate(session, fixture, digest, predecessor="wrong-predecessor")
    with pytest.raises(ptg2_candidate_attestation.CandidateAttestationApprovalConflict, match="approval digest"):
        async with _caller_transaction() as session:
            await _activate_published_candidate(session, fixture, b"x" * 32)
    assert (
        await db.scalar(f"SELECT snapshot_id FROM {schema}.ptg2_current_source_snapshot WHERE source_key='source_a'")
        == "destination-incumbent"
    )
    assert await db.scalar(f"SELECT count(*) FROM {schema}.ptg2_snapshot_pin") == 0


async def _assert_caller_rollback(fixture, digest):
    schema = f'"{fixture.destination_schema}"'

    class CallerRollback(Exception):
        pass

    with pytest.raises(CallerRollback):
        async with _caller_transaction() as session:
            activation_result = await _activate_published_candidate(session, fixture, digest)
            assert activation_result["status"] == "promoted"
            raise CallerRollback()
    assert (
        await db.scalar(f"SELECT status FROM {schema}.ptg2_snapshot WHERE snapshot_id='local-candidate'") == "validated"
    )
    assert (
        await db.scalar(f"SELECT snapshot_id FROM {schema}.ptg2_current_source_snapshot WHERE source_key='source_a'")
        == "destination-incumbent"
    )
    assert await db.scalar(f"SELECT count(*) FROM {schema}.ptg2_current_plan_source") == 0
    assert await db.scalar(f"SELECT count(*) FROM {schema}.ptg2_snapshot_pin") == 0
    assert (
        await db.scalar(
            f"SELECT count(*) FROM {schema}.ptg2_v3_candidate_audit_attestation WHERE activated_at IS NOT NULL"
        )
        == 0
    )


async def _assert_atomic_publication(fixture, digest):
    schema = f'"{fixture.destination_schema}"'
    async with _caller_transaction() as session:
        activation_result = await _activate_published_candidate(session, fixture, digest)
        assert activation_result["status"] == "promoted"
        assert activation_result["plan_source_count"] == 2
    assert (
        await db.scalar(f"SELECT status FROM {schema}.ptg2_snapshot WHERE snapshot_id='local-candidate'") == "published"
    )
    assert (
        await db.scalar(
            f"SELECT count(*) FROM {schema}.ptg2_current_source_snapshot WHERE snapshot_id='local-candidate'"
        )
        == 1
    )
    assert (
        await db.scalar(f"SELECT count(*) FROM {schema}.ptg2_current_plan_source WHERE snapshot_id='local-candidate'")
        == 2
    )
    assert (
        await db.scalar(
            f"SELECT count(*) FROM {schema}.ptg2_snapshot_pin WHERE snapshot_id='destination-incumbent' AND owner_id='published-receive-test'"
        )
        == 1
    )
    assert (
        await db.scalar(
            f"SELECT count(*) FROM {schema}.ptg2_v3_candidate_audit_attestation WHERE activated_at IS NOT NULL"
        )
        == 1
    )
    async with _caller_transaction() as session:
        replay = await _activate_published_candidate(session, fixture, digest)
    assert replay["status"] == "already_promoted"


@pytest.mark.asyncio
async def test_native_published_receive_attests_synthetic_report_and_activates_atomically(native_candidate):
    """Verify synthetic report attestation and atomic publication in PostgreSQL."""
    fixture = native_candidate
    receipt, serving = await _published_stage(fixture, extra_plan_scope=True)
    await _install_activation_relations(fixture)
    await db.status(
        f'INSERT INTO "{fixture.destination_schema}".ptg2_snapshot '
        "(snapshot_id,import_run_id,import_month,status,created_at,published_at,manifest) "
        "VALUES ('destination-incumbent','incumbent-run',DATE '2026-08-01','published',now(),now(),'{}')"
    )
    await _seed_incumbent(fixture)
    async with _caller_transaction() as session:
        await _prepare_published_candidate(session, fixture, receipt, serving)
    attestation = await ptg2_candidate_attestation.record_candidate_audit_attestation(
        snapshot_id="local-candidate",
        source_key="source_a",
        plan_id="plan-a",
        plan_market_type="market-a",
        report=_synthetic_audit_report(fixture, serving),
        storage_generation="shared_blocks_v4",
        activation_intent="audit_only",
    )
    assert attestation["status"] == "attested"
    assert attestation["activation_intent"] == "audit_only"
    digest = bytes.fromhex(attestation["attestation_digest"])
    await _assert_source_tamper_refused(fixture, digest)
    await _assert_wrong_approval_refused(fixture, digest)
    await _assert_caller_rollback(fixture, digest)
    await _assert_atomic_publication(fixture, digest)
