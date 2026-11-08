# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic ordinary serving retention and exact, forward-only recovery history."""

import asyncio
import contextvars
import importlib
import time
from contextlib import asynccontextmanager
from uuid import NAMESPACE_URL, UUID, uuid5

from process import live_progress
from process import provider_directory_published_replacement_journal as replacement_journal
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_output_snapshot_catalog import decoded, encoded

GENERATION_PARAM = "provider_directory_serving_generation"
OPERATION_PARAM = "provider_directory_serving_operation"
DESCRIPTOR_FIELD = "serving_recovery_generation"
_PIN_FIELDS = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "parent_run_id")
_UUID_DOMAIN = "provider-directory-serving-recovery/v1:"
_COUPLED_ADDRESS = contextvars.ContextVar("provider_directory_coupled_address", default=None)
_JOURNAL_CHECKS = {
    "pd_serving_recovery_phase": "CHECK ((phase = ANY (ARRAY['overlay'::text, 'published'::text, 'recovered'::text])))",
    "pd_serving_recovery_document": "CHECK (((jsonb_typeof(document) = 'object'::text) AND ((document ->> 'version'::text) = '1'::text) AND (octet_length((document)::text) <= 2097152)))",
    "pd_serving_recovery_hash": "CHECK ((document_sha256 = encode(sha256(convert_to((document)::text, 'UTF8'::name)), 'hex'::text)))",
}


def canonical_generation(value):
    """Accept only one canonical, non-nil opaque generation identifier."""
    try:
        result = UUID(value) if type(value) is str else None
    except ValueError, AttributeError:
        result = None
    receipts.require(result is not None and result.int != 0 and str(result) == value, "generation_invalid")
    return value


def publication_generation(publication):
    """Derive one stable UUID from the exact immutable acquired publication pin."""
    pin_by_field = {field: publication.get(field) for field in _PIN_FIELDS}
    receipts.require(
        all(type(value) is str and value and value == value.strip() for value in pin_by_field.values())
        and len(pin_by_field["dataset_hash"]) == 64
        and set(pin_by_field["dataset_hash"]) <= set("0123456789abcdef"),
        "publication_invalid",
    )
    return str(uuid5(NAMESPACE_URL, _UUID_DOMAIN + encoded(pin_by_field)))


def publication_descriptor(task, descriptor):
    """Add recovery lineage only to the genuine explicit Source-only outcome."""
    if descriptor is not None and task.get("publish_source_dataset_only") is True:
        return {**descriptor, DESCRIPTOR_FIELD: publication_generation(descriptor)}
    return descriptor


def is_recovery_request(importer, params):
    """Keep recovery outside acquisition, Profile and caller-selected object paths."""
    if OPERATION_PARAM not in params:
        if GENERATION_PARAM in params:
            canonical_generation(params[GENERATION_PARAM])
            receipts.require(
                importer == "provider-directory-fhir"
                and params.get("publish_artifacts_only") is True
                and params.get("publish_artifacts_targets") == "address_overlay"
                and params.get("publish_corroboration") is False
                and not params.get("import_resources")
                and type(params.get("source_ids")) is list
                and len(params["source_ids"]) == 1
                or importer == "entity-address-unified"
                and params.get("refresh_mode") == "provider-directory-partial"
                and params.get("serving_only_refresh") is True
                and params.get("publish") is True,
                "marker_scope_invalid",
            )
        return False
    receipts.require(
        importer == "provider-directory-fhir"
        and params[OPERATION_PARAM] == "recover"
        and set(params) <= {OPERATION_PARAM, GENERATION_PARAM, "source_ids", "run_id"}
        and type(params.get("source_ids")) is list
        and len(params["source_ids"]) == 1
        and type(params["source_ids"][0]) is str
        and bool(params["source_ids"][0].strip())
        and params["source_ids"][0] == params["source_ids"][0].strip(),
        "request_invalid",
    )
    canonical_generation(params.get(GENERATION_PARAM))
    return True


async def replacement_journal_present(database, schema):
    """Keep optional replacement replay compatible with unmigrated ordinary callers."""
    return await database.scalar(
        "SELECT to_regclass(:relation) IS NOT NULL", relation=receipts.qualified(schema, receipts.JOURNAL)
    )


async def _journal(database, schema):
    """Require the migration's fixed ordinary table before reading any authority."""
    native = (await receipts.catalog(database, schema, (receipts.JOURNAL,)))[receipts.JOURNAL]
    receipts.require(
        [(column["name"], column["type"]) for column in native["columns"]]
        == [
            ("generation", "uuid"),
            ("phase", "text"),
            ("source_id", "text"),
            ("document", "jsonb"),
            ("document_sha256", "text"),
        ]
        and all(
            column["required"] and column["default"] is None and not column["generated"] and not column["identity"]
            for column in native["columns"]
        )
        and {constraint["name"] for constraint in native["constraints"] if constraint["type"] != "n"}
        == {
            "pd_serving_recovery_key",
            "pd_serving_recovery_phase",
            "pd_serving_recovery_document",
            "pd_serving_recovery_hash",
        }
        and len(native["indexes"]) == 1
        and native["indexes"][0]["primary"]
        and native["indexes"][0]["keys"] == ["generation", "phase"],
        "journal_layout_changed",
    )
    receipts.require(
        {
            constraint["name"]: constraint["definition"]
            for constraint in native["constraints"]
            if constraint["type"] == "c"
        }
        == _JOURNAL_CHECKS
        and all(not constraint["deferrable"] and not constraint["deferred"] for constraint in native["constraints"]),
        "journal_constraints_changed",
    )
    return native


def _document(document_by_field, phase, generation, source_id):
    document_by_field = decoded(document_by_field)
    receipts.require(
        type(document_by_field) is dict
        and set(document_by_field)
        == {
            "version",
            "generation",
            "phase",
            "source",
            "before",
            "after",
            "writer",
            "predecessor",
            "journal",
            "preparation",
        }
        and document_by_field["version"] == 1
        and type(document_by_field["version"]) is int
        and document_by_field["generation"] == generation
        and document_by_field["phase"] == phase
        and type(document_by_field["source"]) is dict
        and set(document_by_field["source"]) == set(_PIN_FIELDS)
        and document_by_field["source"]["source_id"] == source_id
        and type(document_by_field["preparation"]) is dict,
        "journal_document_changed",
    )
    _require_transition(phase, document_by_field["before"], document_by_field["after"])
    receipts.require(type(document_by_field["preparation"]) is dict, "journal_preparation_changed")
    receipts.require(
        replacement_journal.FIELD not in document_by_field["preparation"]
        or document_by_field["preparation"][replacement_journal.FIELD] is not None,
        "replacement_binding_invalid",
    )
    replacement_journal.validate(
        document_by_field["preparation"].get(replacement_journal.FIELD),
        document_by_field["source"],
        before=document_by_field["before"] if phase == "overlay" else None,
    )
    generation_for_source = (
        replacement_journal.generation(document_by_field["source"])
        if replacement_journal.FIELD in document_by_field["preparation"]
        else publication_generation(document_by_field["source"])
    )
    receipts.require(generation_for_source == generation, "journal_generation_changed")
    receipts.require(
        type(document_by_field["writer"]) is dict
        and set(document_by_field["writer"]) == {"session", "current", "owner", "backend", "xid", "database_oid"}
        and document_by_field["writer"]["session"] == document_by_field["writer"]["current"],
        "journal_writer_changed",
    )
    return document_by_field


def _require_transition(phase, before, after):
    """Attest the fixed native delta instead of accepting an arbitrary stored snapshot."""
    receipts.validate_state(before)
    receipts.validate_state(after)
    old, new = before["metadata"], after["metadata"]
    stable_keys = {"alias", "profile", "doctors"}
    if phase == "overlay":
        stable_keys |= {"address", "geo", "relations", "common_receipt"}
        receipts.require(before["roots"][receipts.ROOTS[1]] == after["roots"][receipts.ROOTS[1]], "transition_changed")
    else:
        receipts.require(
            new["address"]["local_lineage_id"] == old["address"]["local_lineage_id"]
            and new["address"]["local_generation"] == old["address"]["local_generation"] + 1
            and new["relations"][1:] == old["relations"][1:],
            "generation_not_advanced",
        )
        if phase == "published":
            stable_keys.add("overlay_alias")
            receipts.require(
                before["roots"][receipts.ROOTS[0]] == after["roots"][receipts.ROOTS[0]], "transition_changed"
            )
        if old["common_receipt"] is None:
            receipts.require(new["common_receipt"] is None, "common_history_changed")
        else:
            receipts.require(
                new["common_receipt"] is not None
                and new["common_receipt"]["payload"]["predecessor_receipt_id"] == old["common_receipt"]["receipt_id"],
                "common_history_changed",
            )
    receipts.require(all(old[key] == new[key] for key in stable_keys), "unrelated_state_changed")


async def _load_history(database, schema, generation, *, for_update=False, _native=None):
    native = await _journal(database, schema) if _native is None else _native
    _session, driver, _actor = await receipts.ordinary(database)
    event_rows = await driver.fetch(
        "SELECT phase,source_id,document,document_sha256,document_sha256="
        "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(document::text,'UTF8')),'hex') AS valid_hash "
        f"FROM {receipts.qualified(schema, receipts.JOURNAL)} "
        "WHERE generation=$1::uuid ORDER BY phase" + (" FOR UPDATE" if for_update else ""),
        UUID(generation),
    )
    events_by_phase = {}
    for event in event_rows:
        receipts.require(event["valid_hash"] is True, "journal_hash_changed")
        phase = event["phase"]
        receipts.require(phase in {"overlay", "published", "recovered"}, "journal_phase_changed")
        document = _document(event["document"], phase, generation, event["source_id"])
        receipts.require(document["journal"] == native, "journal_catalog_changed")
        events_by_phase[phase] = {"document": document, "sha256": event["document_sha256"]}
    for phase, prior in (
        ("published", "overlay"),
        ("recovered", "published" if "published" in events_by_phase else "overlay"),
    ):
        if phase in events_by_phase:
            receipts.require(
                prior in events_by_phase
                and events_by_phase[phase]["document"]["predecessor"] == events_by_phase[prior]["sha256"],
                "journal_history_changed",
            )
            receipts.require(
                events_by_phase[phase]["document"]["before"] == events_by_phase[prior]["document"]["after"],
                "journal_history_changed",
            )
    if "recovered" in events_by_phase:
        receipts.require(
            events_by_phase["recovered"]["document"]["after"]["roots"]
            == events_by_phase["overlay"]["document"]["before"]["roots"],
            "journal_inverse_changed",
        )
    if "overlay" in events_by_phase:
        original = events_by_phase["overlay"]["document"]
        source_binding = original["preparation"].get(replacement_journal.FIELD)
        replacement_journal.validate(source_binding, original["source"], before=original["before"])
        receipts.require(
            all(
                event["document"]["preparation"].get(replacement_journal.FIELD) == source_binding
                and event["document"]["source"] == original["source"]
                for event in events_by_phase.values()
            ),
            "replacement_history_changed",
        )
    return events_by_phase


async def _append(database, schema, generation, phase, publication_pin, before, after, *, preparation=None):
    native = await _journal(database, schema)
    history = await _load_history(database, schema, generation, for_update=True, _native=native)
    receipts.require(phase not in history, "phase_already_recorded")
    predecessor = None
    if phase != "overlay":
        previous = "published" if phase == "recovered" and "published" in history else "overlay"
        receipts.require(previous in history, "journal_history_changed")
        predecessor = history[previous]["sha256"]
    _session, driver, actor = await receipts.ordinary(database)
    preparation_by_field = dict(preparation or {})
    if phase != "overlay":
        original_binding = history["overlay"]["document"]["preparation"].get(replacement_journal.FIELD)
        receipts.require(
            replacement_journal.FIELD not in preparation_by_field
            or preparation_by_field[replacement_journal.FIELD] == original_binding,
            "replacement_history_changed",
        )
        if original_binding is not None:
            preparation_by_field[replacement_journal.FIELD] = original_binding
    document_by_field = dict(
        version=1,
        generation=generation,
        phase=phase,
        source=publication_pin,
        before=before,
        after=after,
        writer=actor,
        predecessor=predecessor,
        journal=native,
        preparation=preparation_by_field,
    )
    _document(document_by_field, phase, generation, publication_pin["source_id"])
    table = receipts.qualified(schema, receipts.JOURNAL)
    document = encoded(document_by_field)
    await driver.execute(
        f"INSERT INTO {table}(generation,phase,source_id,document,document_sha256) "
        "SELECT $1::uuid,$2,$3,value,encode(sha256(convert_to(value::text,'UTF8')),'hex') "
        "FROM (SELECT $4::jsonb AS value) document",
        UUID(generation),
        phase,
        publication_pin["source_id"],
        document,
    )
    return document


def overlay_request(fence, request):
    """Authenticate an optional marker against the actual acquired Source pin."""
    generation = request.metrics.get(GENERATION_PARAM)
    if generation is None:
        return None
    canonical_generation(generation)
    receipts.require(
        len(fence.datasets) == 1 and request.publish_artifacts_targets == {"address_overlay"}, "overlay_scope_invalid"
    )
    dataset = fence.datasets[0]
    pin_by_field = dict(
        source_id=dataset.source_id,
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        parent_run_id=dataset.evidence_run_id,
    )
    receipts.require(publication_generation(pin_by_field) == generation, "publication_changed")
    return {"generation": generation, "source": pin_by_field, "run_id": request.run_id}


def _progress(run_id, label):
    """Publish an existing live phase without borrowing the database writer."""
    if run_id:
        live_progress.enqueue_live_progress(
            run_id=run_id,
            status="running",
            phase="preparing serving recovery receipts",
            message=label,
            basis="semantic_work",
            denominator_state="unknown",
        )


def attach_overlay_stage(directory, bundle):
    """Select only the actual fully prepared stage supplied by the genuine bundle."""
    from process.provider_directory_generic_source_artifacts import current

    recovery = getattr(current(directory), "recovery", None)
    if recovery is None:
        return
    receipts.require(
        len(bundle.stages) == 1 and bundle.profile_delta is None and bundle.archive_delta is None,
        "prepared_bundle_changed",
    )
    stage = bundle.stages[0]
    receipts.require(
        type(stage) is directory.ProviderDirectoryPreparedArtifactStage
        and stage.target_relation == receipts.ROOTS[0]
        and stage.schema == directory._schema(),
        "prepared_stage_changed",
    )
    recovery["stage"] = stage
    recovery["bundle"] = bundle


def preparation_timeout(directory, ordinary_timeout):
    """Allow bounded reader-compatible preparation, retaining the existing live cut wall."""
    from process.provider_directory_generic_source_artifacts import current

    recovery = getattr(current(directory), "recovery", None)
    if recovery is None:
        return ordinary_timeout
    receipts.require("stage" in recovery, "prepared_stage_required")
    return float(receipts.PREPARATION_SECONDS)


def reported_metrics(context):
    """Expose only a serializable serving outcome in the existing run metrics."""
    reported = context.get("serving_recovery")
    return {} if reported is None else {"serving_recovery": reported}


async def begin_overlay(directory, recovery):
    """Capture the incumbent in the original ordinary overlay transaction."""
    if recovery is None:
        return
    receipts.require(
        not await _load_history(directory.db, directory._schema(), recovery["generation"], for_update=True),
        "operation_already_recorded",
    )
    started = time.monotonic()
    stage = recovery["stage"]
    await receipts.lock(directory.db, directory._schema(), (*receipts.ROOTS, stage.stage_table), mode="SHARE")
    proofs_by_root = {}
    for root in receipts.ROOTS:
        _progress(recovery["run_id"], "Preparing previous " + root.replace("_", " "))
        proofs_by_root[root] = await receipts.prepare_relation(directory.db, directory._schema(), root)
    recovery["before"] = await receipts.prepared_state(directory.db, directory._schema(), proofs_by_root)
    from process import provider_directory_generic_source_artifacts as artifacts

    admission = artifacts.current(directory)
    recovery[replacement_journal.FIELD] = (
        replacement_journal.capture(directory, admission.fence, recovery["before"]) if admission is not None else None
    )
    recovery["proofs"] = proofs_by_root
    _progress(recovery["run_id"], "Preparing replacement provider address overlay")
    from process.provider_directory_artifact_read_access import prepare_ordinary_artifact_reads

    recovery["read_access"] = await prepare_ordinary_artifact_reads(directory.db, stage.schema, (stage,))
    recovery["successor"] = await receipts.prepare_relation(directory.db, directory._schema(), stage.stage_table)
    recovery["preparation_seconds"] = time.monotonic() - started
    recovery["actor"] = (await receipts.ordinary(directory.db))[2]


async def before_overlay(directory):
    """Freeze the fixed metadata and recheck the original state before live locks."""
    from process.provider_directory_generic_source_artifacts import current

    recovery = getattr(current(directory), "recovery", None)
    if recovery is None:
        return
    await receipts.lock_metadata(directory.db, directory._schema())
    state = await receipts.prepared_state(directory.db, directory._schema(), recovery["proofs"])
    receipts.require(state == recovery["before"], "current_serving_changed")


async def retain_overlay(directory, stage):
    """Preserve every ordinary overlay's reads and retain only admitted predecessors."""
    from process import provider_directory_generic_source_artifacts as artifacts
    from process import provider_directory_generic_source_policy as source
    from process import provider_directory_output_snapshot_custody as outputs
    from process.provider_directory_artifact_read_access import (
        prepare_ordinary_artifact_reads,
        recheck_ordinary_read_access,
    )

    if stage.target_relation != receipts.ROOTS[0]:
        return
    capability = outputs._OUTPUT.get()
    if capability is not None:
        await outputs._require_capability(capability)
        return
    if source._PUBLISHER.get() is not None or source._OWNER.get() is not None:
        publisher = await source.require_source_publication_transaction(directory.db)
        receipts.require(publisher.policy.schema == stage.schema == directory._schema(), "overlay_scope_changed")
        return
    admission = artifacts.current(directory)
    recovery = getattr(admission, "recovery", None)
    if admission is not None:
        receipts.require(stage.schema == directory._schema(), "overlay_scope_changed")
    if recovery is None:
        proofs = await prepare_ordinary_artifact_reads(directory.db, stage.schema, (stage,))
    else:
        proofs = recovery.get("read_access")
        receipts.require(proofs is not None and len(proofs) == 1, "overlay_read_access_required")
    await recheck_ordinary_read_access(directory.db, proofs)
    if recovery is None:
        return
    receipts.require((await receipts.ordinary(directory.db))[2] == recovery["actor"], "writer_changed")
    await receipts.retain(directory.db, stage.schema, recovery["before"]["roots"][stage.target_relation])


async def finish_overlay(directory, recovery):
    """Append the installed overlay proof before its original writer commits."""
    if recovery is None:
        return
    receipts.require((await receipts.ordinary(directory.db))[2] == recovery["actor"], "writer_changed")
    proofs_by_root = {**recovery["proofs"], receipts.ROOTS[0]: recovery["successor"]}
    after = await receipts.prepared_state(directory.db, directory._schema(), proofs_by_root)
    return await _append(
        directory.db,
        directory._schema(),
        recovery["generation"],
        "overlay",
        recovery["source"],
        recovery["before"],
        after,
        preparation={
            **(
                {replacement_journal.FIELD: recovery[replacement_journal.FIELD]}
                if recovery.get(replacement_journal.FIELD) is not None
                else {}
            ),
            "elapsed_seconds": recovery["preparation_seconds"],
            "digests": {
                **{root: receipts.preparation_metrics(proof) for root, proof in recovery["proofs"].items()},
                "replacement_overlay": receipts.preparation_metrics(recovery["successor"]),
            },
        },
    )


async def _resume_state(directory, generation):
    """Read a committed event only while its raw and serving identities remain pinned."""
    database, schema = directory.db, directory._schema()
    history = await _load_history(database, schema, generation)
    if not history:
        return None
    receipts.require("recovered" not in history, "operation_recovered")
    source = history["overlay"]["document"]["source"]
    is_replacement = _replacement_binding(history) is not None
    if not is_replacement:
        await _source_fence(directory, source, history=history)
    await receipts.lock(database, schema, receipts.ROOTS, mode="SHARE")
    history = await _load_history(database, schema, generation, for_update=True)
    latest = history.get("published", history["overlay"])["document"]
    current = await receipts.state(database, schema)
    if is_replacement:
        await _finish_replacement_read(directory, source, history, current)
    receipts.require(current == latest["after"], "current_serving_changed")
    return latest


async def resume_overlay(directory, fence, request):
    """Resolve a committed overlay retry without rebuilding or publishing it again."""
    recovery = overlay_request(fence, request)
    if recovery is None:
        return None
    async with asyncio.timeout(receipts.PREPARATION_SECONDS), directory.db.transaction():
        latest = await _resume_state(directory, recovery["generation"])
    if latest is None:
        return None
    receipts.require(latest["source"] == recovery["source"], "publication_changed")
    pin = latest["source"]
    return {
        **request.metrics,
        "address_overlay": {
            "published": True,
            "rows": latest["after"]["roots"][receipts.ROOTS[0]]["rows"],
            "source_ids": [pin["source_id"]],
        },
        "artifact_dataset_ids": [pin["dataset_id"]],
        "artifact_dataset_evidence_run_ids": [pin["parent_run_id"]],
        "serving_recovery": {"generation": recovery["generation"], "phase": latest["phase"]},
    }


def address_request(context, task):
    """Carry an opaque marker into the unchanged separate ordinary Address writer."""
    generation = task.get(GENERATION_PARAM)
    if generation is not None:
        canonical_generation(generation)
        receipts.require(
            task.get("refresh_mode") == "provider-directory-partial"
            and task.get("serving_only_refresh") is True
            and task.get("publish") is True
            and task.get("provider_directory_partial_scope") == "latest-run",
            "address_scope_invalid",
        )
        context[GENERATION_PARAM] = generation


async def resume_address(database, ctx, task):
    """Keep ordinary task context and resolve only an already committed Address cut."""
    from process.provider_directory_published_replacement_address_followup import resume_address as resume_replacement

    if (replacement_result := await resume_replacement(database, ctx, task)) is not None:
        return replacement_result
    address_request(ctx.setdefault("context", {}), task)
    if task.get(GENERATION_PARAM) is None:
        return None
    ctx["context"]["serving_recovery_run_id"] = ctx.get("control_run_id") or task.get("run_id")
    directory = importlib.import_module("process.provider_directory_fhir")
    receipts.require(directory.db is database, "database_changed")
    async with asyncio.timeout(receipts.PREPARATION_SECONDS), database.transaction():
        latest = await _resume_state(directory, task[GENERATION_PARAM])
    if latest is None or latest["phase"] != "published":
        return None
    pin = latest["source"]
    receipts.require(
        task.get("provider_directory_source_ids") == [pin["source_id"]]
        and task.get("provider_directory_dataset_id") == pin["dataset_id"]
        and task.get("provider_directory_run_id") == pin["parent_run_id"],
        "address_publication_changed",
    )
    ctx["context"]["run"] = 0
    return {
        "refresh_mode": "provider-directory-partial",
        "partial_provider_directory_scope": "latest-run",
        "partial_provider_directory_run_id": pin["parent_run_id"],
        "partial_provider_directory_dataset_id": pin["dataset_id"],
        "partial_provider_directory_source_count": 1,
        "serving_only_refresh": True,
        "rows": latest["after"]["roots"][receipts.ROOTS[1]]["rows"],
        "serving_recovery": {"generation": task[GENERATION_PARAM], "phase": "published"},
    }


@asynccontextmanager
async def address_transaction(database, lock_timeout, quote_literal, context):
    """Keep preparation reader-compatible and the marked live cut through commit bounded."""
    from process.entity_address_cutover_contract import entity_address_cutover_transaction

    if context.get(GENERATION_PARAM) is None:
        async with entity_address_cutover_transaction(database, lock_timeout, quote_literal):
            yield
        return
    has_preparation = False
    try:
        async with (
            asyncio.timeout(receipts.PREPARATION_SECONDS) as wall,
            entity_address_cutover_transaction(database, lock_timeout, quote_literal),
        ):
            context["serving_recovery_wall"] = wall
            try:
                yield
            finally:
                has_preparation = context.pop("serving_recovery_preparation_started", False)
                for key in (
                    "serving_recovery_wall",
                    "serving_recovery_before",
                    "serving_recovery_writer",
                    "serving_recovery_proofs",
                    "serving_recovery_stage",
                    "serving_recovery_read_access",
                    "serving_recovery_prepared_address",
                ):
                    context.pop(key, None)
    except Exception as error:
        address = importlib.import_module("process.entity_address_unified")
        if has_preparation and address._is_retryable_cutover_lock_error(error):
            raise RuntimeError(
                "provider_directory_serving_recovery_prepared_cutover_lock_unavailable: retry the operation explicitly"
            ) from None
        raise


def _coupled_context(context):
    """Snapshot only the immutable publication coordinates used by the prepared cut."""
    return encoded(
        {
            key: context.get(key)
            for key in (
                GENERATION_PARAM,
                "partial_provider_directory_source_ids",
                "partial_provider_directory_dataset_id",
                "partial_provider_directory_run_id",
            )
        }
    )


def _coupled_prepared_identity(prepared):
    """Keep the exact original proof objects, rather than admitting a substituted proof set."""
    receipts.require(
        type(prepared) is dict
        and set(prepared) == {"proofs", "read_access", "stage"}
        and type(prepared["proofs"]) is dict
        and set(prepared["proofs"]) == set(receipts.ROOTS)
        and type(prepared["read_access"]) in {tuple, list},
        "coupled_preparation_changed",
    )
    return (
        prepared["proofs"],
        *(prepared["proofs"][root] for root in receipts.ROOTS),
        prepared["read_access"],
        *prepared["read_access"],
        prepared["stage"],
    )


@asynccontextmanager
async def coupled_address_scope(directory, selected, address, document):
    """Reuse only this writer's just-inserted overlay receipt during its prepared Address call."""
    from process import provider_directory_generic_source_artifacts as artifacts

    receipts.require(type(document) is str, "coupled_preparation_changed")
    admission = artifacts.current(directory)
    session, _driver, actor = await receipts.ordinary(directory.db)
    entry = _document(document, "overlay", selected["generation"], selected["source"]["source_id"])
    prepared = address.context.get("serving_recovery_prepared_address")
    receipts.require(
        _COUPLED_ADDRESS.get() is None
        and admission is not None
        and admission.factory is None
        and admission.recovery is selected
        and entry["source"] == selected["source"]
        and entry["before"] == selected["before"]
        and entry["writer"] == selected["actor"] == actor
        and entry["preparation"].get(replacement_journal.FIELD) == selected[replacement_journal.FIELD]
        and address.db_schema == directory._schema()
        and len(address.swaps) == 1
        and address.swaps[0].live_cls.__main_table__ == receipts.ROOTS[1]
        and address.context.get(GENERATION_PARAM) == selected["generation"]
        and address.context.get("partial_provider_directory_source_ids") == [entry["source"]["source_id"]]
        and address.context.get("partial_provider_directory_dataset_id") == entry["source"]["dataset_id"]
        and address.context.get("partial_provider_directory_run_id") == entry["source"]["parent_run_id"],
        "coupled_preparation_changed",
    )
    state_by_field = dict(
        directory=directory,
        admission=admission,
        recovery=selected,
        address=address,
        context=address.context,
        swaps=address.swaps,
        preparation_input=address.preparation_input,
        prepared=prepared,
        prepared_identity=_coupled_prepared_identity(prepared),
        session=session,
        actor=encoded(actor),
        task=asyncio.current_task(),
        context_pin=_coupled_context(address.context),
        document=document,
    )
    token = _COUPLED_ADDRESS.set(state_by_field)
    try:
        yield
    finally:
        _COUPLED_ADDRESS.reset(token)


async def _prepare_coupled_address(database, schema, swaps, context):
    """Skip duplicate preparation only inside the original coupled native publication call."""
    state_by_field = _COUPLED_ADDRESS.get()
    if state_by_field is None:
        return None
    from process import provider_directory_generic_source_artifacts as artifacts

    started = time.monotonic()
    directory, address = state_by_field["directory"], state_by_field["address"]
    proof_parts = _coupled_prepared_identity(state_by_field["prepared"])
    original_parts = state_by_field["prepared_identity"]
    receipts.require(
        state_by_field["task"] is asyncio.current_task()
        and database is directory.db
        and schema == directory._schema() == address.db_schema
        and context is state_by_field["context"] is address.context
        and swaps is state_by_field["swaps"] is address.swaps
        and address.preparation_input is state_by_field["preparation_input"]
        and artifacts.current(directory) is state_by_field["admission"]
        and state_by_field["admission"].recovery is state_by_field["recovery"]
        and _coupled_context(context) == state_by_field["context_pin"]
        and context.get("serving_recovery_prepared_address") is state_by_field["prepared"]
        and len(proof_parts) == len(original_parts)
        and all(actual is original for actual, original in zip(proof_parts, original_parts, strict=True)),
        "coupled_preparation_changed",
    )
    session, _driver, actor = await receipts.ordinary(database)
    entry = decoded(state_by_field["document"])
    receipts.require(
        session is state_by_field["session"]
        and encoded(actor) == state_by_field["actor"]
        and state_by_field["recovery"]["source"] == entry["source"]
        and state_by_field["recovery"]["generation"] == entry["generation"],
        "coupled_preparation_changed",
    )
    prepared = state_by_field["prepared"]
    await receipts.recheck_prepared(database, prepared["stage"], swaps[0].stage_cls.__tablename__)
    context.update(
        serving_recovery_preparation_started=True,
        serving_recovery_before=entry["after"],
        serving_recovery_writer=actor,
        serving_recovery_proofs=prepared["proofs"],
        serving_recovery_read_access=prepared["read_access"],
        serving_recovery_stage=prepared["stage"],
        serving_recovery_preparation_seconds=time.monotonic() - started,
    )
    return prepared


async def prepare_address(database, schema, swaps, context):
    """Freeze and hash actual old/new heaps before any live AccessExclusive lock."""
    generation = context.get(GENERATION_PARAM)
    if await _prepare_coupled_address(database, schema, swaps, context) is not None or generation is None:
        return
    started = time.monotonic()
    history = await _load_history(database, schema, generation, for_update=True)
    receipts.require(
        set(history) == {"overlay"} and len(swaps) == 1 and swaps[0].live_cls.__main_table__ == receipts.ROOTS[1],
        "address_phase_changed",
    )
    entry = history["overlay"]["document"]
    pin = entry["source"]
    receipts.require(
        context.get("partial_provider_directory_source_ids") == [pin["source_id"]]
        and context.get("partial_provider_directory_dataset_id") == pin["dataset_id"]
        and context.get("partial_provider_directory_run_id") == pin["parent_run_id"],
        "address_publication_changed",
    )
    directory = importlib.import_module("process.provider_directory_fhir")
    receipts.require(directory.db is database, "database_changed")
    await _source_fence(directory, pin, history=history)
    stage_name = swaps[0].stage_cls.__tablename__
    await receipts.lock(database, schema, (*receipts.ROOTS, stage_name), mode="SHARE")
    context["serving_recovery_preparation_started"] = True
    prepared_address = context.get("serving_recovery_prepared_address")
    proofs_by_root = {} if prepared_address is None else prepared_address["proofs"]
    if prepared_address is None:
        for root in receipts.ROOTS:
            _progress(context.get("serving_recovery_run_id"), "Preparing previous " + root.replace("_", " "))
            proofs_by_root[root] = await receipts.prepare_relation(database, schema, root)
    current = await receipts.prepared_state(database, schema, proofs_by_root)
    receipts.require(current == entry["after"], "current_serving_changed")
    context["serving_recovery_before"] = current
    context["serving_recovery_writer"] = (await receipts.ordinary(database))[2]
    context["serving_recovery_proofs"] = proofs_by_root
    _progress(context.get("serving_recovery_run_id"), "Preparing replacement provider addresses")
    from process.provider_directory_artifact_read_access import prepare_ordinary_read_access

    context["serving_recovery_read_access"] = (
        await prepare_ordinary_read_access(database, schema, swaps)
        if prepared_address is None
        else prepared_address["read_access"]
    )
    context["serving_recovery_stage"] = (
        await receipts.prepare_relation(database, schema, stage_name)
        if prepared_address is None
        else prepared_address["stage"]
    )
    if prepared_address is not None:
        await receipts.recheck_prepared(database, prepared_address["stage"], stage_name)
    context["serving_recovery_preparation_seconds"] = time.monotonic() - started
    await database.status("SET LOCAL statement_timeout='1000ms'")
    deadline = asyncio.get_running_loop().time() + 2.0
    context["serving_recovery_wall"].reschedule(min(context["serving_recovery_wall"].when(), deadline))
    await receipts.lock_metadata(database, schema)
    receipts.require(
        await receipts.prepared_state(database, schema, proofs_by_root) == current, "current_serving_changed"
    )


async def prepare_replacement_address(database, schema, address, context, selected_recovery):
    """Hash the private Address stage while the original serving heaps still admit readers."""
    from process.provider_directory_artifact_read_access import prepare_ordinary_read_access

    stage_name = address.swaps[0].stage_cls.__tablename__
    await receipts.lock(database, schema, (stage_name,), mode="SHARE")
    context["serving_recovery_prepared_address"] = dict(
        proofs={**selected_recovery["proofs"], receipts.ROOTS[0]: selected_recovery["successor"]},
        read_access=await prepare_ordinary_read_access(database, schema, address.swaps),
        stage=await receipts.prepare_relation(database, schema, stage_name),
    )


async def before_address(database, schema, swaps, context):
    """Preserve ordinary reads and retain marked main inside the short live cut."""
    from process.provider_directory_artifact_read_access import (
        prepare_ordinary_read_access,
        recheck_ordinary_read_access,
    )

    if context.get("snapshot_contract") == "entity_address_unified.postgres.v2":
        receipts.require(context.get(GENERATION_PARAM) is None, "address_scope_invalid")
        return
    from process import provider_directory_generic_source_policy as source

    if source._PUBLISHER.get() is not None or source._OWNER.get() is not None:
        from process.native_publication_read_access import preserve_publication_reads

        receipts.require(GENERATION_PARAM not in context, "address_scope_invalid")
        publisher = await source.require_source_publication_transaction(database)
        receipts.require(publisher.policy.schema == schema, "address_scope_changed")
        for swap in swaps:
            await preserve_publication_reads(
                publisher.connection,
                schema=schema,
                live_table=swap.live_cls.__main_table__,
                stage_table=swap.stage_cls.__tablename__,
            )
        receipts.require(await source.require_source_publication_transaction(database) is publisher, "writer_changed")
        return
    proofs = context.get("serving_recovery_read_access")
    if context.get(GENERATION_PARAM) is None:
        proofs = await prepare_ordinary_read_access(database, schema, swaps)
    receipts.require(proofs is not None and len(proofs) == len(swaps), "address_read_access_required")
    await recheck_ordinary_read_access(database, proofs)
    if context.get(GENERATION_PARAM) is None:
        return
    current = await receipts.prepared_state(database, schema, context["serving_recovery_proofs"])
    receipts.require(current == context["serving_recovery_before"], "current_serving_changed")
    await receipts.retain(database, schema, current["roots"][receipts.ROOTS[1]])


async def after_address(database, schema, context):
    """Commit the complete serving proof in the same Address publication transaction."""
    generation = context.get(GENERATION_PARAM)
    if generation is None:
        return
    history = await _load_history(database, schema, generation, for_update=True)
    entry = history["overlay"]
    receipts.require((await receipts.ordinary(database))[2] == context.pop("serving_recovery_writer"), "writer_changed")
    before = context.pop("serving_recovery_before")
    proofs_by_root = {**context["serving_recovery_proofs"], receipts.ROOTS[1]: context["serving_recovery_stage"]}
    after = await receipts.prepared_state(database, schema, proofs_by_root)
    await _append(
        database,
        schema,
        generation,
        "published",
        entry["document"]["source"],
        before,
        after,
        preparation={
            "elapsed_seconds": context["serving_recovery_preparation_seconds"],
            "digests": {
                **{
                    root: receipts.preparation_metrics(proof)
                    for root, proof in context["serving_recovery_proofs"].items()
                },
                "replacement_address": receipts.preparation_metrics(context["serving_recovery_stage"]),
            },
        },
    )
    context["serving_recovery"] = {
        "generation": generation,
        "phase": "published",
        "preparation_seconds": context["serving_recovery_preparation_seconds"],
    }


async def _source_fence(directory, source, *, history=None):
    """Fence only the recorded current raw tuple using the ordinary committing session."""
    from process.provider_directory_generic_source_artifacts import lock_address_source_fence

    if history is not None:
        binding = history["overlay"]["document"]["preparation"].get(replacement_journal.FIELD)
        if binding is not None:
            await replacement_journal.check(directory, source, binding, restored="recovered" in history)
            return
    fence_context_by_field = dict(
        partial_provider_directory_dataset_id=source["dataset_id"],
        partial_provider_directory_source_ids=[source["source_id"]],
        partial_provider_directory_run_id=source["parent_run_id"],
    )
    await lock_address_source_fence(directory, directory.db, directory._schema(), fence_context_by_field)
    _session, driver, _actor = await receipts.ordinary(directory.db)
    actual = decoded(
        await driver.fetchval(
            "SELECT jsonb_build_object('source_id',s.source_id,'endpoint_id',d.endpoint_id,"
            "'dataset_id',d.dataset_id,'dataset_hash',d.dataset_hash,'parent_run_id',d.acquisition_root_run_id) "
            f"FROM {receipts.qualified(directory._schema(), 'provider_directory_source')} s "
            f"JOIN {receipts.qualified(directory._schema(), 'provider_directory_endpoint_dataset')} d ON d.endpoint_id=s.endpoint_id "
            "WHERE s.source_id=$1 AND d.is_current AND d.status='published'",
            source["source_id"],
        )
    )
    receipts.require(actual == source, "current_source_changed")


def _replacement_binding(history):
    return None if history is None else history["overlay"]["document"]["preparation"].get(replacement_journal.FIELD)


async def _finish_replacement_read(directory, source, history, current):
    """Join hashed heaps with simultaneously current Source and fixed metadata, without rescanning."""
    async with asyncio.timeout(2.0):
        await directory.db.status("SET LOCAL statement_timeout='1000ms'")
        await _source_fence(directory, source, history=history)
        await receipts.lock_metadata(directory.db, directory._schema())
        receipts.require(
            await receipts.metadata(directory.db, directory._schema()) == current["metadata"], "current_serving_changed"
        )


async def _lock_recovery(directory, source, baseline, forward, *, mode="SHARE", history=None):
    """Use ordinary Source/alias order, then pin every native serving dependency."""
    from process import provider_directory_cms_serving_receipt as common
    from process.ext import address_alias_sql

    database, schema = directory.db, directory._schema()
    if _replacement_binding(history) is None:
        await _source_fence(directory, source, history=history)
        await database.scalar(address_alias_sql.alias_advisory_xact_lock_sql())
    _session, driver, _actor = await receipts.ordinary(database)
    dependencies = set(common._NATIVE_RELATIONS) - set(receipts.ROOTS)
    existing = await driver.fetch(
        "SELECT name FROM unnest($1::text[]) name WHERE to_regclass(name) IS NOT NULL",
        [receipts.qualified(schema, name) for name in sorted(dependencies)],
    )
    if existing:
        await driver.execute("LOCK TABLE " + ",".join(row["name"] for row in existing) + " IN ACCESS SHARE MODE NOWAIT")
    retained_names = [
        receipts.retained_name(baseline["roots"][root]["native"]["oid"])
        for root in receipts.ROOTS
        if baseline["roots"][root]["native"]["oid"] != forward["roots"][root]["native"]["oid"]
    ]
    names = (*receipts.ROOTS, *retained_names)
    await receipts.lock(database, schema, names, mode=mode)
    return names


async def _restore_geo(database, schema, baseline):
    """Restore active local geo metadata only while its exact selected inputs still match."""
    from api import ptg2_geo_projection as projection

    geo = baseline["metadata"]["geo"]
    if geo is None:
        return
    signature, bindings_match = projection.projection_stored_bindings_sql(schema, "active_dependency_bindings")
    _session, driver, _actor = await receipts.ordinary(database)
    table = receipts.qualified(schema, projection.GEO_ASSURANCE_STATE_TABLE)
    await driver.execute(
        f"UPDATE {table} SET active_geo_assurance_version=$1,active_table_oid=$2::oid,"
        "active_relation_signature=$3::jsonb,active_dependency_bindings=$4::jsonb WHERE singleton",
        geo["version"],
        geo["table_oid"],
        encoded(geo["signature"]),
        encoded(geo["bindings"]) if geo["bindings"] is not None else None,
    )
    valid = await driver.fetchval(
        f"SELECT active_relation_signature=({signature}) AND ({bindings_match}) FROM {table} WHERE singleton"
    )
    receipts.require(valid is True or geo["version"] is None and geo["table_oid"] is None, "geo_dependencies_changed")


async def _inverse(directory, history, generation, current, proofs_by_root, retained_by_root, preparation):
    """Restore exact retained data while advancing existing native authority and receipts."""
    from process import entity_address_result_generation as generations
    from process.entity_address_serving_receipt import ordinary_address_receipt_callbacks

    database, schema = directory.db, directory._schema()
    baseline = history["overlay"]["document"]["before"]
    forward = history.get("published", history["overlay"])
    expected = forward["document"]["after"]
    receipts.require(current == expected, "current_serving_changed")
    for root in receipts.ROOTS:
        if current["roots"][root]["native"]["oid"] != baseline["roots"][root]["native"]["oid"]:
            await receipts.retain(database, schema, current["roots"][root])
            await receipts.restore(database, schema, baseline["roots"][root], prepared=retained_by_root[root])
        if root == receipts.ROOTS[0]:
            await database.status(
                f"UPDATE {receipts.qualified(schema, 'address_alias_artifact_state_v1')} SET updated_at=clock_timestamp() WHERE artifact_name='provider_directory_address_overlay'"
            )
            quote_literal = importlib.import_module("process.entity_address_unified")._sql_literal
            callbacks = ordinary_address_receipt_callbacks(database, schema, quote_literal)
            if callbacks.before_cutover is not None:
                await callbacks.before_cutover()
    await _restore_geo(database, schema, baseline)
    await generations.publish_local_entity_address_generation(database, schema_name=schema)
    if callbacks.after_publish is not None:
        await callbacks.after_publish()
    restored_by_root = {root: retained_by_root.get(root, proofs_by_root[root]) for root in receipts.ROOTS}
    after = await receipts.prepared_state(database, schema, restored_by_root)
    for root in receipts.ROOTS:
        receipts.require_relation(baseline["roots"][root], after["roots"][root])
    await replacement_journal.restore(
        directory,
        forward["document"]["source"],
        history["overlay"]["document"]["preparation"].get(replacement_journal.FIELD),
    )
    await _append(
        database,
        schema,
        generation,
        "recovered",
        forward["document"]["source"],
        current,
        after,
        preparation=preparation,
    )
    return after


def _outcome(generation, source, state):
    return {
        "serving_recovery": {
            "generation": generation,
            "phase": "recovered",
            "current_acquired": source,
            "restored_serving_generation": state["metadata"]["address"],
        }
    }


async def _prepare_inverse_receipts(directory, baseline, forward, run_id):
    """Hash current and retained heaps before any short final Source freeze."""
    database, schema = directory.db, directory._schema()
    started = time.monotonic()
    proofs_by_root, retained_by_root = {}, {}
    for root in receipts.ROOTS:
        _progress(run_id, "Verifying current " + root.replace("_", " "))
        proofs_by_root[root] = await receipts.prepare_relation(database, schema, root)
        previous_oid = baseline["roots"][root]["native"]["oid"]
        if previous_oid != forward["roots"][root]["native"]["oid"]:
            _progress(run_id, "Verifying retained " + root.replace("_", " "))
            retained_by_root[root] = await receipts.prepare_relation(
                database, schema, receipts.retained_name(previous_oid)
            )
            old = decoded(retained_by_root[root].receipt_json)
            receipts.require_relation(baseline["roots"][root], old, renamed=True)
    preparation_by_field = {
        "elapsed_seconds": time.monotonic() - started,
        "digests": {
            **{root: receipts.preparation_metrics(proof) for root, proof in proofs_by_root.items()},
            **{"retained_" + root: receipts.preparation_metrics(proof) for root, proof in retained_by_root.items()},
        },
    }
    return proofs_by_root, retained_by_root, preparation_by_field


async def _recover_once(directory, task):
    """Keep the journal, exact inverse and native receipt in one ordinary transaction."""
    from process import provider_directory_generic_source_policy as source_policy

    receipts.require(source_policy._FACTORY.get() is None, "ordinary_context_required")
    database, schema, generation = directory.db, directory._schema(), task[GENERATION_PARAM]
    async with asyncio.timeout(receipts.PREPARATION_SECONDS) as wall, database.transaction():
        await database.status("SET LOCAL lock_timeout='500ms'")
        history = await _load_history(database, schema, generation)
        receipts.require("overlay" in history, "operation_missing")
        publication_pin = history["overlay"]["document"]["source"]
        receipts.require(task["source_ids"] == [publication_pin["source_id"]], "source_changed")
        baseline = history["overlay"]["document"]["before"]
        forward = history.get("recovered", history.get("published", history["overlay"]))["document"]["after"]
        names = await _lock_recovery(directory, publication_pin, baseline, forward, history=history)
        history = await _load_history(database, schema, generation, for_update=True)
        if "recovered" in history:
            state = await receipts.state(database, schema)
            if _replacement_binding(history) is not None:
                await _finish_replacement_read(directory, publication_pin, history, state)
            receipts.require(state == history["recovered"]["document"]["after"], "current_serving_changed")
        else:
            proofs_by_root, retained_by_root, preparation_by_field = await _prepare_inverse_receipts(
                directory, baseline, forward, task.get("run_id")
            )
            from process.entity_address_cutover_contract import lock_live_serving_relations

            wall.reschedule(asyncio.get_running_loop().time() + 2.0)
            await database.status("SET LOCAL statement_timeout='1000ms'")
            if _replacement_binding(history) is not None:
                from process.ext import address_alias_sql

                await _source_fence(directory, publication_pin, history=history)
                await database.scalar(address_alias_sql.alias_advisory_xact_lock_sql())
            await receipts.lock_metadata(database, schema)
            current = await receipts.prepared_state(database, schema, proofs_by_root)
            receipts.require(current == forward, "current_serving_changed")
            await lock_live_serving_relations(database.status, schema, receipts.ROOTS)
            if retained_names := set(names) - set(receipts.ROOTS):
                await receipts.lock(database, schema, retained_names)
            state = await _inverse(
                directory, history, generation, current, proofs_by_root, retained_by_root, preparation_by_field
            )
        outcome = _outcome(generation, publication_pin, state)
    return outcome


async def _verify_recovered(directory, task):
    """Resolve an uncertain acknowledgment by observation alone, never a second inverse."""
    database, schema, generation = directory.db, directory._schema(), task[GENERATION_PARAM]
    async with asyncio.timeout(receipts.PREPARATION_SECONDS), database.transaction():
        await database.status("SET LOCAL lock_timeout='500ms'")
        history = await _load_history(database, schema, generation)
        if "recovered" not in history:
            return None
        source = history["recovered"]["document"]["source"]
        receipts.require(task["source_ids"] == [source["source_id"]], "source_changed")
        is_replacement = _replacement_binding(history) is not None
        if not is_replacement:
            await _source_fence(directory, source, history=history)
        await receipts.lock(database, schema, receipts.ROOTS, mode="SHARE")
        history = await _load_history(database, schema, generation, for_update=True)
        state = await receipts.state(database, schema)
        if is_replacement:
            await _finish_replacement_read(directory, source, history, state)
        receipts.require(state == history["recovered"]["document"]["after"], "current_serving_changed")
        return _outcome(generation, source, state)


async def _recover(directory, task):
    """Preserve a proven committed result when only the acknowledgment was lost."""
    try:
        return await _recover_once(directory, task)
    except Exception:
        try:
            committed = await _verify_recovered(directory, task)
        except Exception:
            committed = None
        if committed is not None:
            return committed
        raise


async def maybe_recover(directory, ctx, task):
    """Dispatch the fixed recovery action in a clean ordinary context only."""
    if not is_recovery_request("provider-directory-fhir", task):
        return None
    receipts.require(directory.db._transaction_binding() is None, "ordinary_binding_required")
    await directory.ensure_database(bool(ctx.get("context", {}).get("test_mode")))
    result = await asyncio.create_task(_recover(directory, task), context=contextvars.Context())
    ctx.setdefault("context", {})["audit"] = result
    return result
