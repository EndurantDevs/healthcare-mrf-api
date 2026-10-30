# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify a complete common cut in the existing immutable recovery journal."""

from process import provider_directory_common_serving_recovery_document as documents
from process import provider_directory_serving_recovery_receipts as receipts


async def capture_state(fhir, *, initial_targets=None):
    """Read actual native roots and metadata on the already locked publication owner."""
    return documents.validate_state(
        {
            "metadata": await receipts.metadata(fhir.db, fhir._schema()),
            "roots": await receipts.catalog(fhir.db, fhir._schema(), tuple(documents.FAMILIES.values())),
            "initial_targets": initial_targets,
        }
    )


async def committed_event(fhir, generation, expected_sha256):
    """Read immutable commit evidence without requiring it to remain the latest serving cut."""
    from sqlalchemy import text

    from process import provider_directory_serving_recovery as recovery

    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        history = await recovery._load_history(fhir.db, fhir._schema(), generation)
        event = history.get("published")
        if event is None or event["sha256"] != expected_sha256 or not is_common(event["document"]["preparation"]):
            return None
        return event


def is_common(preparation):
    """Select the closed common contract without changing legacy journal documents."""
    return type(preparation) is dict and preparation.get("contract_id") == documents.CONTRACT


def validate_history(events):
    """Require one complete cut followed only by its exact scoped inverse."""
    receipts.require(set(events) in ({"published"}, {"published", "recovered"}), "common_history_changed")
    published = events["published"]["document"]
    receipts.require(is_common(published["preparation"]), "common_history_changed")
    if "recovered" not in events:
        return events
    recovered = events["recovered"]["document"]
    receipts.require(
        recovered["predecessor"] == events["published"]["sha256"]
        and recovered["source"] == published["source"]
        and recovered["preparation"] == published["preparation"]
        and recovered["before"] == published["after"]
        and recovered["after"]["roots"] == published["before"]["roots"]
        and recovered["after"]["initial_targets"] == published["before"]["initial_targets"],
        "common_inverse_changed",
    )
    baseline, restored = published["before"]["metadata"], recovered["after"]["metadata"]
    receipts.require(
        all(
            restored[key] == baseline[key]
            for key in ("alias", "overlay_alias", "profile", "doctors", "geo", "relations")
        ),
        "common_inverse_changed",
    )
    return events


def append_predecessor(history, phase, preparation):
    """A common cut has no independently publishable Overlay phase."""
    receipts.require(phase in {"published", "recovered"}, "common_phase_invalid")
    if phase == "published":
        receipts.require(not history, "common_history_changed")
        return None
    receipts.require(set(history) == {"published"}, "common_history_changed")
    validate_history(history)
    receipts.require(history["published"]["document"]["preparation"] == preparation, "common_inverse_changed")
    return history["published"]["sha256"]
