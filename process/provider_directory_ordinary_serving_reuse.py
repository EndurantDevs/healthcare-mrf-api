# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse one fully verified current common cut under the new run's consumed pair."""

from contextlib import asynccontextmanager
from dataclasses import asdict, replace

from sqlalchemy import text

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_common_initial_witness as observations
from process import provider_directory_common_serving_journal as journal
from process import provider_directory_common_serving_postimages as postimages
from process import provider_directory_ordinary_retention_capacity as retention
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_ordinary_address_admission import require_capture


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_ordinary_serving_reuse_changed")


@asynccontextmanager
async def _snapshot(prepared):
    """Use the original Builder's bounded RR without writing any serving state."""
    fhir = prepared.fhir
    _require(fhir.db._transaction_binding() is None)
    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        async with (
            preparation.nonprofile_sql_transaction(fhir, prepared.nonprofile_admission),
            observations._read_transaction(fhir) as context,
        ):
            await receipts.ordinary(fhir.db)
            yield context


def _can_reuse(prepared, document):
    """Unknown inputs and uncovered requested or mandatory Sources require preparation."""
    admission, execution = prepared.nonprofile_admission, prepared.execution
    capture = require_capture(prepared.fhir, admission.plan, admission.captured_inputs)
    semantic = capture.semantic_inputs
    recorded = document["preparation"]
    required = set(capture.affected_scope["source_ids"]) | set(capture.scope_payload["requested_source_ids"])
    result = recorded["profile_result"]
    return (
        semantic["dependency_proofs_complete"] is True
        and recorded["semantic_inputs"] == semantic
        and required <= set(recorded["qualified_source_ids"])
        and result["proof_id"] == execution.attestation.proof_id
        and type(result["generation"]) is int
        and 0 < result["generation"] <= execution.generation
    )


async def _candidate(prepared):
    """Find the exact current receipt before consuming authority for content verification."""
    fhir, schema = prepared.fhir, prepared.fhir._schema()
    capture = require_capture(fhir, prepared.nonprofile_admission.plan, prepared.nonprofile_admission.captured_inputs)
    if capture.predecessor["kind"] != "published" or capture.semantic_inputs["dependency_proofs_complete"] is not True:
        return None
    async with _snapshot(prepared):
        state = await journal.capture_state(fhir)
        await recovery._journal(fhir.db, schema)
        profile, address = state["metadata"]["profile"], state["metadata"]["address"]
        candidate_rows = await fhir.db.all(
            f"SELECT generation::text AS generation FROM {receipts.qualified(schema, receipts.JOURNAL)} "
            "WHERE phase='published' AND document->'preparation'->>'contract_id'=:contract "
            "AND document->'after'->'metadata'->'profile'->>'generation_id'=:profile "
            "AND document->'after'->'metadata'->'address'->>'local_lineage_id'=:lineage "
            "AND document->'after'->'metadata'->'address'->>'local_generation'=:address LIMIT 2",
            contract=journal.documents.CONTRACT,
            profile=profile.get("generation_id"),
            lineage=address["local_lineage_id"],
            address=str(address["local_generation"]),
        )
        if not candidate_rows:
            return None
        _require(len(candidate_rows) == 1)
        generation = fhir._pagination_checkpoint_row_mapping(candidate_rows[0])["generation"]
        history = await recovery._load_history(fhir.db, schema, generation)
        if set(history) != {"published"}:
            return None
        event = history["published"]
        document = event["document"]
        if document["after"] != state or not _can_reuse(prepared, document):
            return None
        return event


async def _lock_relations(context, schema, document):
    """Pin actual root and retained names against DDL while RR pins their row versions."""
    retained = journal.documents.validate_retention(document["preparation"]["retention"])
    names = set(journal.documents.FAMILIES.values())
    names.update(entry["name"] for entry in (*retained["scopes"], *retained["families"]))
    await context.driver.execute(
        "LOCK TABLE "
        + ",".join(receipts.qualified(schema, name) for name in sorted(names))
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    return retained


async def _initial_retention(context, schema, document, retained):
    """Keep authentic initial heap custody without inventing an original content hash."""
    original = document["before"]["initial_targets"]
    for entry in retained["families"]:
        if not entry["reused_initial"]:
            continue
        family = entry["family"]
        _require(original is not None and entry["oid"] == original[family + "_target_oid"])
        _require(entry["name"] == receipts.retained_name(entry["oid"]))
        native = (await receipts.catalog(context.fhir.db, schema, (entry["name"],)))[entry["name"]]
        _require(all(index["name"] == receipts.retained_name(index["oid"], index=True) for index in native["indexes"]))
        receipts.require_relation(
            {"native": document["before"]["roots"][journal.documents.FAMILIES[family]]},
            {"native": native},
            renamed=True,
            content=False,
        )
        relation = preparation.OwnedRelation(schema, entry["name"], entry["oid"], 0, "p")
        catalog, _toast = await retention._catalog(context.fhir, relation, initial_family=family)
        _require(retention._layout_digest(catalog) == entry["layout_sha256"])
        _require(
            await context.driver.fetchval(f"SELECT count(*) FROM {receipts.qualified(schema, entry['name'])}")
            == entry["rows"]
            == original[family + "_rows"]
        )


async def _prove_current(prepared, candidate):
    """Verify all current metadata, native identities and scoped rows in one original snapshot."""
    from process.provider_directory_ordinary_serving_publication import _historical_profile_result

    fhir, schema = prepared.fhir, prepared.fhir._schema()
    document = candidate["document"]
    async with _snapshot(prepared) as context:
        retained = await _lock_relations(context, schema, document)
        history = await recovery._load_history(fhir.db, schema, document["generation"])
        _require(set(history) == {"published"} and history["published"] == candidate)
        await prepared.nonprofile_admission.captured_inputs.recheck(fhir)
        state = await journal.capture_state(fhir)
        _require(state == document["after"] and _can_reuse(prepared, document))
        initial = document["preparation"]["initial_receipt"]
        if initial is not None:
            _require(
                await recovery.assert_initial_common_receipt(
                    fhir, schema, initial["build_id"], initial["generation_id"], document["writer"]["xid"]
                )
            )
        _require(retained["creator"]["owner"] == context.actor["owner"])
        await observations._retained(context, schema, document)
        await _initial_retention(context, schema, document, retained)
        queries = postimages.current_postimage_queries(
            schema, retained, document["preparation"]["qualified_source_ids"]
        )
        _require(
            await postimages.capture_postimages(context.session, queries)
            == document["preparation"]["expected_postimages"]
        )
        serving = fhir._profile_serving_state_from_row(state["metadata"]["profile"])
        expected = fhir.profile_selection_result(
            replace(prepared.execution, generation=serving.control_generation),
            profile_generation_id=serving.generation_id,
            profile_as_of=serving.profile_as_of,
            profile_rows=serving.profile_rows,
            profile_source_evidence_rows=serving.evidence_rows,
        )
        _require(expected == document["preparation"]["profile_result"])
        await _historical_profile_result(fhir, expected)
        return asdict(serving)


async def reuse_current_ordinary_serving(prepared, snapshot, *, run_id, control_run_id):
    """Consume both exact new leases before proving and returning the old common result."""
    from process.provider_directory_ordinary_serving_publication import common_result_metrics

    candidate = await _candidate(prepared)
    if candidate is None:
        return None
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    types = fhir._provider_directory_artifact_resource_types({"address_overlay"}, publish_corroboration=False)
    await admission.before_scratch(
        prepared.execution, prepared.fence, snapshot.artifact_projection, {"address_overlay"}, types
    )
    await preparation._admit_profile_before_full_build(
        fhir, prepared.execution, prepared.fence, run_id, control_run_id, admission
    )
    await admission.assert_ready(fhir, fhir._schema())
    serving = await _prove_current(prepared, candidate)
    await admission.assert_ready(fhir, fhir._schema())
    prepared.metrics["serving_recovery"] = common_result_metrics(candidate, serving, outcome="reused")
    prepared.metrics[fhir.PROFILE_SELECTION_RESULT_METRIC] = candidate["document"]["preparation"]["profile_result"]
    return prepared.metrics
