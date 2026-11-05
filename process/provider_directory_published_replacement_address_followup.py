# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Replay a completed replacement for the unchanged separate Address child request."""

import asyncio
import importlib

_PREFIX = "provider_directory_dataset_followup_"
_PIN_FIELDS = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "parent_run_id")
_MARKER_FIELDS = (*_PIN_FIELDS, "version", "stage", "attempt", "overlay_run_id")


def _exact_text(value):
    return type(value) is str and bool(value) and value == value.strip()


def _request(task):
    """Decode only the existing closed V1 partial Address wire contract."""
    from process import provider_directory_serving_recovery as recovery

    receipts = recovery.receipts
    required_by_field = {
        "refresh_mode": "provider-directory-partial",
        "provider_directory_partial_scope": "latest-run",
        "serving_only_refresh": True,
        "publish": True,
    }
    expected_fields = {
        *required_by_field,
        "provider_directory_source_ids",
        "provider_directory_run_id",
        "provider_directory_dataset_id",
        *(_PREFIX + field for field in _MARKER_FIELDS),
    }
    pin_by_field = {name: task.get(_PREFIX + name) for name in _PIN_FIELDS}
    receipts.require(
        set(task) - {"test_mode", "run_id", recovery.GENERATION_PARAM} == expected_fields
        and task.get("test_mode", False) is False
        and ("run_id" not in task or _exact_text(task["run_id"]))
        and all(task.get(name) == required_value for name, required_value in required_by_field.items())
        and task.get("serving_only_refresh") is True
        and task.get("publish") is True
        and type(task.get(_PREFIX + "version")) is int
        and task[_PREFIX + "version"] == 1
        and task.get(_PREFIX + "stage") == "entity_address_unified"
        and type(task.get(_PREFIX + "attempt")) is int
        and task[_PREFIX + "attempt"] > 0
        and _exact_text(task.get(_PREFIX + "overlay_run_id"))
        and task.get("provider_directory_source_ids") == [pin_by_field["source_id"]]
        and task.get("provider_directory_run_id") == pin_by_field["parent_run_id"]
        and task.get("provider_directory_dataset_id") == pin_by_field["dataset_id"],
        "replacement_address_request_invalid",
    )
    ordinary_generation = recovery.publication_generation(pin_by_field)
    if recovery.GENERATION_PARAM in task:
        receipts.require(
            recovery.canonical_generation(task[recovery.GENERATION_PARAM]) == ordinary_generation,
            "replacement_address_generation_changed",
        )
    return pin_by_field


async def resume_address(database, ctx, task):
    """Read native journal/header/output proofs before skipping an already completed build."""
    if type(task) is not dict or not any(type(name) is str and name.startswith(_PREFIX) for name in task):
        return None
    from process import provider_directory_generic_source_policy as source
    from process import provider_directory_published_replacement_inputs as serving_inputs
    from process import provider_directory_published_replacement_journal as journal
    from process import provider_directory_serving_recovery as recovery

    if source.is_source_storage_required():
        return None
    pin = _request(task)
    receipts = recovery.receipts
    receipts.require(source._FACTORY.get() is None, "replacement_address_ordinary_required")
    directory = importlib.import_module("process.provider_directory_fhir")
    receipts.require(directory.db is database, "database_changed")
    generation = journal.generation(pin)
    async with asyncio.timeout(receipts.PREPARATION_SECONDS), database.transaction():
        await database.status("SET LOCAL lock_timeout='500ms'")
        await receipts.ordinary(database)
        publication = await serving_inputs._publication(directory, pin)
        receipts.require(
            serving_inputs._pin(publication) == pin,
            "replacement_address_publication_changed",
        )
        if not await recovery.replacement_journal_present(database, directory._schema()):
            return None
        latest = await recovery._resume_state(directory, generation)
    if latest is None:
        return None
    receipts.require(
        latest["phase"] == "published" and latest["source"] == pin, "replacement_address_publication_changed"
    )
    ctx.setdefault("context", {})["run"] = 0
    return {
        "refresh_mode": "provider-directory-partial",
        "partial_provider_directory_scope": "latest-run",
        "partial_provider_directory_run_id": pin["parent_run_id"],
        "partial_provider_directory_dataset_id": pin["dataset_id"],
        "partial_provider_directory_source_count": 1,
        "serving_only_refresh": True,
        "rows": latest["after"]["roots"][receipts.ROOTS[1]]["rows"],
        "serving_recovery": {"generation": generation, "phase": "published"},
    }
