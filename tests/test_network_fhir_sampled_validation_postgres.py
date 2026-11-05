# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original Source subsets detect producer faults without upgrading sampled read authority."""

from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import pytest

from process import network_fhir_sampled_validation as sampled
from process import network_fhir_typed_capture as custody
from process import network_fhir_typed_source as typed
from process.network_address_projection import _identifier
from process.network_fhir_source_witness_contract import canonical
from process.network_fhir_typed_contract import PAIR_COLUMNS, TypedFHIRGraphError
from tests.source_witness_capture_postgres_support import (
    _observed_source_descriptor,
    source_modules,
    source_witness_case,
)
from tests.test_network_fhir_typed_source_postgres import _produce_recipe_capture

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _reader_replay(case):
    """Precreate only owned scratch tables before the genuine read-only Reader transaction."""
    reader, created_names = case.reader, []
    names_by_kind = {kind: "sample_replay_" + kind + "_" + uuid4().hex for kind in typed._DDL}
    try:
        for kind, definition in typed._DDL.items():
            await reader.execute("CREATE TEMP TABLE " + _identifier(names_by_kind[kind]) + "(" + definition + ")")
            created_names.append(names_by_kind[kind])
        await reader.execute("CREATE INDEX ON " + _identifier(names_by_kind["addresses"]) + "(address_key)")
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await reader.fetchval("SELECT session_user::text") == case.fixture.component.reader
            assert await reader.fetchval("SHOW transaction_isolation") == "repeatable read"
            assert await reader.fetchval("SHOW transaction_read_only") == "on"
            standing_limit = await reader.fetchval(
                "SELECT CASE WHEN current_setting('temp_file_limit')='-1' THEN NULL "
                "ELSE pg_size_bytes(current_setting('temp_file_limit')) END"
            )
            assert type(standing_limit) is int and 0 < standing_limit <= 256 * 1024**2
            work_mem = await reader.fetchval("SHOW work_mem")
            replay = await typed.bind_typed_graph_stage(reader, schema_name="pg_temp", table_names=names_by_kind)
            try:
                yield replay
            finally:
                assert await reader.fetchval("SHOW work_mem") == work_mem
                for kind in names_by_kind:
                    assert await reader.fetchval("SELECT NOT EXISTS(SELECT FROM " + replay.relation(kind) + ")")
    finally:
        for name in reversed(created_names):
            await reader.execute("DROP TABLE pg_temp." + _identifier(name))
            assert await reader.fetchval("SELECT to_regclass($1)", "pg_temp." + name) is None


async def _immutable_publication(case, capture):
    """Compare native Source header and candidate seal around validation, without replaying Source payloads."""
    header = await case.fixture.admin.fetchrow(
        "SELECT publication.tableoid::oid::bigint,row_to_json(publication)::text FROM "
        + _identifier(case.origin.schema_name)
        + ".provider_directory_endpoint_dataset publication WHERE dataset_id=$1",
        case.origin.dataset_id,
    )
    seal = await case.fixture.admin.fetchval(
        "SELECT obj_description($1::oid,'pg_namespace')", capture["catalog"]["schema_oid"]
    )
    assert header is not None and seal == custody.COMMENT + canonical(capture)
    return tuple(header), seal, canonical(capture)


def _producer_fault(monkeypatch, fault):
    """Inject only before the producer closes native catalog and content commitments."""
    project_page, produce_pairs = typed._project_page, typed._pairs

    async def omit_role(connection, stage, rows):
        remaining_rows = [
            row for row in rows if (row["resource_type"], row["resource_id"]) != ("PractitionerRole", "role-office")
        ]
        await project_page(connection, stage, remaining_rows)

    async def invent_pair(connection, stage, origin, as_of, *, checker=False):
        await produce_pairs(connection, stage, origin, as_of, checker=checker)
        if not checker:
            replacement_by_field = {"provider_id": "'1000000004'", "resource_id": "'role-invented'"}
            inserted = await connection.execute(
                "INSERT INTO "
                + stage.relation("pairs")
                + "("
                + ",".join(PAIR_COLUMNS)
                + ") SELECT "
                + ",".join(replacement_by_field.get(field, field) for field in PAIR_COLUMNS)
                + " FROM "
                + stage.relation("pairs")
                + " WHERE resource_type='PractitionerRole' AND resource_id='role-office'"
            )
            assert inserted == "INSERT 0 1"

    if fault == "omitted-role":
        monkeypatch.setattr(typed, "_project_page", omit_role)
    elif fault == "invented-pair":
        monkeypatch.setattr(typed, "_pairs", invent_pair)
    else:
        assert fault is None


async def _capture_candidate(case, monkeypatch, fault):
    catalog, _ = source_modules()
    descriptor, _ = await _observed_source_descriptor(
        case.source_runtime, case.source_profile, case.fixture.candidates[-1], catalog
    )
    originals = typed._project_page, typed._pairs
    with monkeypatch.context() as producer_patch:
        _producer_fault(producer_patch, fault)
        capture, anchor = await _produce_recipe_capture(case, descriptor, (case.fixture.component.reader,))
    assert (typed._project_page, typed._pairs) == originals
    return capture, anchor


async def _verify_candidate(case, capture, anchor, replay, failed_direction):
    arguments_by_name = {
        "scope": sampled.SampledCaptureScope(case.source_profile, case.origin, anchor, ("network-one",), replay),
        "policy": sampled.SemanticSamplingPolicy("1" * 64),
    }
    if failed_direction is None:
        result = await sampled.verify_sampled_typed_capture(
            case.source_export, case.reader, capture, **arguments_by_name
        )
        assert type(result) is sampled.SampledTypedGraphRead
        assert result.sealed_read.stage.checked is False
        await result.verify(case.reader)
        with pytest.raises(TypedFHIRGraphError, match="registry_fhir_typed_source_incomplete"):
            await result.sealed_read.stage.verify(case.reader)
        return result.receipt
    with pytest.raises(TypedFHIRGraphError, match="^registry_fhir_typed_sample_failed$") as failure:
        await sampled.verify_sampled_typed_capture(case.source_export, case.reader, capture, **arguments_by_name)
    receipt = failure.value.receipt
    assert receipt["networks"][0][failed_direction]["failures"] == 1
    return receipt


@pytest.mark.parametrize(
    ("fault", "failed_direction", "pair_rows"),
    [(None, None, 1), ("omitted-role", "source_to_candidate", 0), ("invented-pair", "candidate_to_source", 2)],
    ids=("valid-sampled-read", "missing-source-membership", "invented-candidate-membership"),
)
async def test_sampled_capture_preserves_native_custody(monkeypatch, fault, failed_direction, pair_rows):
    async with source_witness_case(monkeypatch) as case:
        capture, anchor = await _capture_candidate(case, monkeypatch, fault)
        assert capture["graph"]["counts"]["pair_rows"] == pair_rows
        before = await _immutable_publication(case, capture)
        async with _reader_replay(case) as replay:
            receipt = await _verify_candidate(case, capture, anchor, replay, failed_direction)
            assert sampled.validate_sampled_semantic_receipt(receipt) is receipt
            assert receipt["mode"] == "sampled" and receipt["capture_sha256"] == capture["capture_sha256"]
            assert receipt["series_count"] == 10 and receipt["items_per_series"] == 100
            network = receipt["networks"][0]
            assert network["network_source_key"] == "network-one"
            assert network["source_to_candidate"]["selected"] == network["source_to_candidate"]["population"] > 0
            assert (
                network["candidate_to_source"]["selected"] == network["candidate_to_source"]["population"] == pair_rows
            )
            for direction in ("source_to_candidate", "candidate_to_source"):
                assert network[direction]["failures"] == int(direction == failed_direction)
            sealed = await custody.load_typed_graph_capture(case.reader, capture, expected_anchor=anchor)
            assert sealed.stage.checked is False
            await sealed.verify(case.reader)
        assert await _immutable_publication(case, capture) == before


async def test_candidate_budget_splits_after_source_eof_without_losing_provenance(monkeypatch):
    """Retry bounded scratch work while preserving the original Source and selected pair."""
    from tests import generic_source_successor_postgres_support as source

    edition_rows = source._edition_rows

    def additional_role(edition, dataset):
        resource_rows = edition_rows(edition, dataset)
        original = next(row for row in resource_rows if row["resource_id"] == "role-office")
        payload = {**original["payload_json"], "resource_id": "role-office-alias"}
        resource_rows.append(
            {
                **original,
                "resource_id": payload["resource_id"],
                "payload_json": payload,
                "payload_hash": source.directory.resource_payload_sha256_for_contract(
                    payload,
                    source.directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
                    resource_type="PractitionerRole",
                ),
            }
        )
        return resource_rows

    replay_roles, candidate_batches = sampled._replay_roles, []

    async def observe(*arguments, **options):
        result = await replay_roles(*arguments, **options)
        if options.get("bound_fanout"):
            candidate_batches.append((len(arguments[3]), result is False))
        return result

    monkeypatch.setattr(source, "_edition_rows", additional_role)
    monkeypatch.setattr(sampled, "_replay_roles", observe)
    monkeypatch.setattr(sampled, "_CANDIDATE_REPLAY_BUDGET", 4)
    async with source_witness_case(monkeypatch) as case:
        capture, anchor = await _capture_candidate(case, monkeypatch, None)
        before = await _immutable_publication(case, capture)
        async with _reader_replay(case) as replay:
            receipt = await _verify_candidate(case, capture, anchor, replay, None)
            direction = receipt["networks"][0]["candidate_to_source"]
            assert (direction["selected"], direction["provenance_rows"], direction["witness_rows"]) == (1, 2, 2)
            assert direction["failures"] == 0
        assert candidate_batches == [(2, True), (1, False), (1, False)]
        assert await _immutable_publication(case, capture) == before


async def test_sampled_publication_keeps_proof_mode_on_retained_successor(monkeypatch):
    """Publish through original actors and retain sampled provenance without replay or promotion."""
    from process import network_candidate_custody
    from process.network_sampled_custody import validate_sampled_custody
    from tests import test_network_fhir_source_witness_publication_postgres as publication
    from tests import test_network_fhir_typed_source_postgres as typed_fixture

    original_capture = typed_fixture._typed_worker_capture
    original_verification = sampled.verify_sampled_typed_capture
    checked_actors = []

    async def capture_with_policy(*arguments):
        context = await original_capture(*arguments)
        context.runtime = replace(context.runtime, semantic_policy=sampled.SemanticSamplingPolicy("1" * 64))
        return context

    async def observe(factory, connection, capture, *, scope, policy):
        result = await original_verification(factory, connection, capture, scope=scope, policy=policy)
        checked_actors.append(await connection.fetchval("SELECT session_user"))
        return result

    monkeypatch.setattr(publication, "_source_capture_for_workers", capture_with_policy)
    monkeypatch.setattr(sampled, "verify_sampled_typed_capture", observe)
    async with publication._publication_case(monkeypatch) as context:
        first = await publication._publish_once(context)
        second = await publication._publish_once(context, head=first.generation_id)
        assert second.generation_id > first.generation_id
        assert checked_actors == [await context.operator.connection.fetchval("SELECT session_user")]
        receipts = []
        async with (
            publication._login(context.worker, "api") as reader,
            reader.transaction(isolation="repeatable_read", readonly=True),
        ):
            assert await publication._membership_snapshot(reader, first) == await publication._membership_snapshot(
                reader, second
            )
            for manifest in (first, second):
                seal = await reader.fetchrow(
                    "SELECT receipt_json::text,seal_sha256 FROM "
                    + _identifier(context.worker.fixture.control_schema)
                    + ".network_candidate_custody WHERE candidate_id=$1",
                    manifest.candidate_id,
                )
                receipt = network_candidate_custody._sealed_receipt(seal)
                assert receipt["revision"] == 5
                semantic = validate_sampled_custody(
                    receipt["semantic_validation"], candidate_id=str(manifest.candidate_id)
                )
                receipts.append(semantic)
        assert receipts[0]["sources"][0]["method"] == "sampled"
        assert receipts[1]["sources"][0]["method"] == "sampled-retained"
        assert receipts[1]["sources"][0]["receipt"] == receipts[0]["sources"][0]["receipt"]
