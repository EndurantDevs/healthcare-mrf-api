# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify an existing common inverse on its original locked Builder transaction.

These observations and SQL plans grant no rollback spending authority. A fresh
inverse-purpose admission must exist before a coordinator executes any plan.
"""

import asyncio
from dataclasses import dataclass

from process import provider_directory_common_serving_inverse as inverse
from process import provider_directory_common_serving_journal as journal
from process import provider_directory_common_serving_postimages as postimages
from process import provider_directory_common_serving_recovery_metadata as metadata
from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_output_snapshot_catalog import decoded, encoded


@dataclass(frozen=True)
class CommonInverseObservation:
    """Content evidence remains tied to one real task/session/backend/XID; never a lease."""

    fhir: object
    session: object
    task: object
    actor_json: str
    generation: str
    event_sha256: str
    document_json: str
    row_restores: tuple
    initial_restores: tuple
    initial_proofs: tuple

    @property
    def document(self):
        """Return detached immutable evidence, never an execution capability."""
        return decoded(self.document_json)


def _require(condition):
    receipts.require(condition, "common_inverse_observation_changed")


async def _published(fhir, generation, source_id):
    history = await recovery._load_history(fhir.db, fhir._schema(), generation, for_update=True)
    _require(set(history) == {"published"})
    event = history["published"]
    _require(journal.is_common(event["document"]["preparation"]))
    document = recovery._document(event["document"], "published", generation, source_id)
    return event, document


async def _frozen_roots(driver, document):
    for native in document["after"]["roots"].values():
        await receipts._require_freeze(driver, native["oid"])


async def _current(fhir, document):
    actual = await journal.capture_state(fhir, initial_targets=document["after"]["initial_targets"])
    _require(actual == document["after"])


async def _initial_witness(fhir, document):
    """Authenticate old original targets through immutable receipt/consumption, not its expired budget."""
    if document["before"]["initial_targets"] is None:
        _require(document["preparation"]["initial_receipt"] is None)
        return
    from process import provider_directory_common_initial_witness as witness

    reference = document["preparation"]["initial_receipt"]
    _require(reference is not None)
    async with witness._read_transaction(fhir) as context:
        receipt = await witness._receipt(context, fhir._schema(), reference["build_id"])
        _require(receipt is not None and all(receipt[key] == value for key, value in reference.items()))
        _require(
            await witness._admitted_targets(context, fhir._schema(), receipt) == document["before"]["initial_targets"]
        )
        history = await witness._history(context, fhir._schema(), receipt)
        _require(history.get("published", {}).get("document") == document)


async def observe_locked_inverse(fhir, generation, *, source_id):
    """Hash exact scoped forward rows and retained inputs before the caller's live cut window."""
    generation = recovery.canonical_generation(generation)
    session, driver, actor = await receipts.ordinary(fhir.db)
    event, document = await _published(fhir, generation, source_id)
    await _frozen_roots(driver, document)
    await _current(fhir, document)
    preparation = document["preparation"]
    await metadata.assert_common_ancestry(
        driver, fhir._schema(), document["before"]["metadata"], document["after"]["metadata"]
    )
    await inverse.assert_retained_rows(session, fhir._schema(), preparation["retention"])
    await _initial_witness(fhir, document)
    queries = postimages.current_postimage_queries(
        fhir._schema(), preparation["retention"], preparation["qualified_source_ids"]
    )
    _require(await postimages.capture_postimages(session, queries) == preparation["expected_postimages"])
    plans = inverse.initial_restore_plan(document["before"], preparation["retention"])
    initial_proofs = []
    for plan in plans:
        proof = await receipts.prepare_relation(fhir.db, fhir._schema(), plan.retained_relation)
        receipts.require_relation(
            {"native": document["before"]["roots"][plan.root]}, decoded(proof.receipt_json), renamed=True, content=False
        )
        original = document["before"]["initial_targets"]
        _require(decoded(proof.receipt_json)["rows"] == original[plan.family + "_rows"])
        initial_proofs.append((plan.family, proof))
    await _current(fhir, document)
    final_session, final_driver, final_actor = await receipts.ordinary(fhir.db)
    _require(final_session is session and final_driver is driver and final_actor == actor)
    return CommonInverseObservation(
        fhir,
        session,
        asyncio.current_task(),
        encoded(actor),
        generation,
        event["sha256"],
        encoded(document),
        inverse.restore_rows_sql(fhir._schema(), preparation["retention"], preparation["qualified_source_ids"]),
        plans,
        tuple(initial_proofs),
    )


async def recheck_locked_inverse(observed):
    """Repeat CAS without bulk hashing after the same transaction retains every freeze lock."""
    _require(type(observed) is CommonInverseObservation and observed.task is asyncio.current_task())
    session, driver, actor = await receipts.ordinary(observed.fhir.db)
    _require(session is observed.session and encoded(actor) == observed.actor_json)
    document = observed.document
    event, current = await _published(observed.fhir, observed.generation, document["source"]["source_id"])
    _require(event["sha256"] == observed.event_sha256 and current == document)
    await _frozen_roots(driver, document)
    for entry in (*document["preparation"]["retention"]["scopes"], *document["preparation"]["retention"]["families"]):
        await receipts._require_freeze(driver, entry["oid"])
    await _current(observed.fhir, document)
    return observed
