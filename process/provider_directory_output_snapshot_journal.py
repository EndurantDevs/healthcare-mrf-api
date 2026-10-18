# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed maintenance history in the existing protected dataset migration plan."""

from copy import deepcopy
from dataclasses import replace

import asyncpg

from db.migration_provider_directory_dataset_candidates import TABLES, candidate_functions
from db.plan_release_storage import storage_relation
from process import provider_directory_output_snapshot_custody as custody
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    OUTPUT_ROOTS,
    decoded,
    encoded,
    require_output,
)
from process.provider_directory_output_snapshot_retention import RetainedOutputReceipt
from process.provider_directory_output_snapshot_stage import OutputStage, _stage_row

PLAN_ROOT = "pd_dataset_migration_plan"
_KEY = "output_maintenance"
_KINDS = {
    "source_projection",
    "contact_backfill",
    "coordinate_backfill",
    "npi_backfill",
    "address_key_backfill",
    "canonical_backfill",
    "stale_filter",
    "source_catalog_filter",
}
_ENTRY_FIELDS = {"operation", "targets", "phase", "stages", "prepared", "history"}
_HISTORY_FIELDS = {"generation", "operation", "operation_digest", "reference", "receipts"}


def _require(condition, code):
    require_output(condition, "journal_" + code)


def _targets(values):
    _require(
        type(values) is list
        and bool(values)
        and all(type(value) is str for value in values)
        and values == sorted(set(values))
        and set(values) <= set(OUTPUT_ROOTS),
        "targets_invalid",
    )
    return tuple(values)


def _receipt_set(values, targets, decoder):
    _require(type(values) is list and len(values) == len(targets), "receipt_set_changed")
    receipts = tuple(decoder(value) for value in values)
    _require(tuple(receipt.incumbent.target for receipt in receipts) == targets, "receipt_set_changed")
    return receipts


def _entry(digest, entry_by_field):
    _require(type(entry_by_field) is dict and set(entry_by_field) == _ENTRY_FIELDS, "entry_invalid")
    target_names = _targets(entry_by_field["targets"])
    _require(
        custody._operation_digest(entry_by_field["operation"], target_names) == digest
        and entry_by_field["operation"]["kind"] in _KINDS
        and entry_by_field["phase"] in {"preparing", "prepared", "published"}
        and type(entry_by_field["stages"]) is dict
        and set(entry_by_field["stages"]) <= set(target_names)
        and type(entry_by_field["prepared"]) is list
        and type(entry_by_field["history"]) is list,
        "entry_invalid",
    )
    for target_name, progress in entry_by_field["stages"].items():
        _require(type(progress) is dict and set(progress) == {"stage", "copied_rows"}, "progress_invalid")
        stage = OutputStage.from_payload(progress["stage"])
        _require(
            stage.target == target_name
            and stage.operation_digest == digest
            and type(progress["copied_rows"]) is int
            and 0 <= progress["copied_rows"] < 2**63,
            "progress_invalid",
        )
    if entry_by_field["phase"] == "preparing":
        _require(not entry_by_field["prepared"] and not entry_by_field["history"], "phase_invalid")
    else:
        prepared = _receipt_set(entry_by_field["prepared"], target_names, custody.PreparedOutputReceipt.from_payload)
        _require(
            set(entry_by_field["stages"]) == set(target_names)
            and all(receipt.operation_digest == digest for receipt in prepared),
            "prepared_changed",
        )
        for receipt in prepared:
            stage = OutputStage.from_payload(entry_by_field["stages"][receipt.incumbent.target]["stage"])
            _require(
                stage.incumbent == receipt.incumbent
                and stage.oid == receipt.successor.oid
                and stage.relation == receipt.successor.relation,
                "prepared_changed",
            )
        _require(bool(entry_by_field["history"]) == (entry_by_field["phase"] == "published"), "phase_invalid")
    _require_history(digest, entry_by_field, target_names)
    return entry_by_field


def _require_history(digest, entry_by_field, target_names):
    """Every inverse generation preserves the complete prior native proof pair."""
    previous, previous_receipts = None, None
    for generation, history in enumerate(entry_by_field["history"], 1):
        _require(type(history) is dict and set(history) == _HISTORY_FIELDS, "history_invalid")
        expected_digest = custody._operation_digest(history["operation"], target_names)
        _require(
            type(history["generation"]) is int
            and history["generation"] == generation
            and history["operation_digest"] == expected_digest
            and history["reference"] == previous
            and (
                history["operation"] == entry_by_field["operation"] and expected_digest == digest
                if previous is None
                else history["operation"]["kind"] == "rollback" and history["operation"]["rollback_of"] == previous
            ),
            "history_invalid",
        )
        receipts = _receipt_set(history["receipts"], target_names, RetainedOutputReceipt.from_payload)
        _require(all(receipt.operation_digest == expected_digest for receipt in receipts), "history_invalid")
        if previous_receipts is not None:
            _require(
                all(
                    current.incumbent == replace(prior.successor, relation=current.retained_name)
                    and current.successor == replace(prior.incumbent, relation=current.target)
                    for prior, current in zip(previous_receipts, receipts, strict=True)
                ),
                "history_continuity_changed",
            )
        previous_receipts = receipts
        previous = expected_digest


def _journal(plan):
    _require(
        type(plan) is dict
        and plan.get("complete") is True
        and type(plan.get("tables")) is dict
        and set(plan["tables"]) == set(TABLES),
        "plan_changed",
    )
    journal = plan.get(_KEY, {"protocol": 1, "operations": {}})
    _require(
        type(journal) is dict
        and set(journal) == {"protocol", "operations"}
        and type(journal["protocol"]) is int
        and journal["protocol"] == 1
        and type(journal["operations"]) is dict,
        "invalid",
    )
    for digest, value in journal["operations"].items():
        _entry(digest, value)
    return journal


def _require_plan_layout(catalog_entry, owner):
    topology, columns = decoded(catalog_entry["topology"]), decoded(catalog_entry["columns"])
    _require(
        catalog_entry["relname"] == PLAN_ROOT
        and catalog_entry["relowner"] == owner
        and catalog_entry["acl_closed"]
        and catalog_entry["readers_closed"]
        and catalog_entry["readers_select"]
        and not decoded(catalog_entry["acl"])["columns"]
        and topology["kind"] == "r"
        and topology["persistence"] == "p"
        and not topology["partition"]
        and not topology["rls"]
        and not topology["force_rls"]
        and not topology["parents"]
        and not topology["children"]
        and topology["hooks"] == 1
        and topology["rules"] == 0,
        "custody_changed",
    )
    _require(
        [(column["name"], column["type"], column["type_oid"]) for column in columns]
        == [("singleton", "boolean", 16), ("plan", "jsonb", 3802)]
        and all(
            column["required"]
            and column["default"] is None
            and not column["identity"]
            and not column["generated"]
            and column["native_collation"]
            for column in columns
        ),
        "layout_changed",
    )
    constraints = decoded(catalog_entry["constraints"])
    structural_constraints = [constraint for constraint in constraints if constraint["type"] != "n"]
    _require(
        len(structural_constraints) == 2
        and {constraint["type"] for constraint in structural_constraints} == {"p", "c"}
        and all(
            constraint["validated"] and not constraint["deferrable"] and not constraint["deferred"]
            for constraint in constraints
        )
        and next(constraint for constraint in structural_constraints if constraint["type"] == "p")["keys"] == [1]
        and next(constraint for constraint in structural_constraints if constraint["type"] == "c")["definition"]
        == "CHECK (singleton)",
        "layout_changed",
    )
    _require_plan_indexes(catalog_entry, owner)


def _require_plan_indexes(catalog_entry, owner):
    """The singleton keeps its one essential native Boolean primary key."""
    indexes = decoded(catalog_entry["indexes"])
    _require(
        len(indexes) == 1
        and all(
            index["owner"] == owner
            and index["method"] == "btree"
            and index["primary"]
            and index["unique"]
            and index["valid"]
            and index["ready"]
            and index["live"]
            and index["immediate"]
            and index["native"]
            and not index["exclusion"]
            and index["keys_count"] == index["total_count"] == 1
            and index["keys"] == ["singleton"]
            and index["predicate"] is None
            and index["expression"] is None
            for index in indexes
        ),
        "layout_changed",
    )


async def _plan(capability):
    await custody._require_capability(capability)
    _require(
        custody._operation_digest(capability.operation, capability.targets) == capability.operation_digest,
        "operation_changed",
    )
    owner, readers = await storage_roles(
        capability.connection, capability.policy.schema, **capability.policy.role_arguments()
    )
    catalog_entries = await capability.connection.fetch(
        OUTPUT_CATALOG_SQL, capability.policy.schema, [PLAN_ROOT], owner, readers
    )
    _require(len(catalog_entries) == 1, "missing")
    _require_plan_layout(catalog_entries[0], owner)
    hooks = await capability.connection.fetch(
        "SELECT t.tgname,t.tgtype,t.tgenabled::text,t.tgisinternal,t.tgconstraint,t.tgargs,t.tgqual::text,"
        "t.tgoldtable,t.tgnewtable,p.proname,n.nspname,p.prosrc,p.prosecdef,p.proconfig,p.pronargs,"
        "p.prorettype='pg_catalog.trigger'::regtype AS trigger_result,l.lanname,"
        "r.rolname AS function_owner,(SELECT amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam "
        "WHERE c.oid=t.tgrelid) AS table_method FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
        "JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles r ON r.oid=p.proowner "
        "JOIN pg_language l ON l.oid=p.prolang WHERE t.tgrelid=$1::oid",
        catalog_entries[0]["oid"],
    )
    expected_body = candidate_functions(capability.policy.schema).split("$fn$", 2)[1]
    _require(
        len(hooks) == 1
        and all(
            hook["tgname"] == "pd_dataset_plan_owner"
            and hook["tgtype"] == 62
            and hook["tgenabled"] == "A"
            and not hook["tgisinternal"]
            and hook["tgconstraint"] == 0
            and not hook["tgargs"]
            and hook["tgqual"] is None
            and hook["tgoldtable"] is None
            and hook["tgnewtable"] is None
            and hook["proname"] == "guard_pd_dataset_migration_owner"
            and hook["nspname"] == capability.policy.schema
            and hook["prosrc"] == expected_body
            and not hook["prosecdef"]
            and hook["proconfig"] == ["search_path=pg_catalog"]
            and hook["pronargs"] == 0
            and hook["trigger_result"]
            and hook["lanname"] == "plpgsql"
            and hook["table_method"] == "heap"
            and hook["function_owner"] in (capability.policy.owner_role, *capability.policy.control_runtime_roles)
            for hook in hooks
        ),
        "owner_hook_changed",
    )
    plan = decoded(
        await capability.connection.fetchval(
            f"SELECT plan FROM {storage_relation(capability.policy.schema, PLAN_ROOT)} WHERE singleton"
        )
    )
    _journal(plan)
    return plan


async def _cas(capability, prior, updated):
    """This must be the caller's LAST mutation before transaction completion."""
    await custody._require_capability(capability)
    _require(
        {key: value for key, value in prior.items() if key != _KEY}
        == {key: value for key, value in updated.items() if key != _KEY},
        "unrelated_plan_changed",
    )
    _journal(updated)
    try:
        changed = await capability.connection.fetchval(
            f"UPDATE {storage_relation(capability.policy.schema, PLAN_ROOT)} SET plan=$1::jsonb "
            "WHERE singleton AND plan=$2::jsonb RETURNING singleton",
            encoded(updated),
            encoded(prior),
        )
    except asyncpg.SerializationError as error:
        raise RuntimeError("provider_directory_output_snapshot_journal_conflict") from error
    _require(changed is True, "conflict")


def _forward_entry(capability, updated):
    _require(capability.operation["kind"] in _KINDS and capability.reference is None, "maintenance_required")
    journal = deepcopy(_journal(updated))
    updated[_KEY] = journal
    entry = journal["operations"].setdefault(
        capability.operation_digest,
        {
            "operation": capability.operation,
            "targets": list(capability.targets),
            "phase": "preparing",
            "stages": {},
            "prepared": [],
            "history": [],
        },
    )
    _entry(capability.operation_digest, entry)
    return entry


async def record_output_progress(capability, stage, copied_rows):
    """Record one bounded COPY batch only after its last payload mutation."""
    await _stage_row(capability, stage, phases={"loading"})
    _require(type(copied_rows) is int and 0 <= copied_rows < 2**63, "progress_invalid")
    prior = await _plan(capability)
    updated = deepcopy(prior)
    entry = _forward_entry(capability, updated)
    _require(entry["phase"] == "preparing", "phase_invalid")
    previous = entry["stages"].get(stage.target)
    progress_by_field = {"stage": stage.to_payload(), "copied_rows": copied_rows}
    _require(
        previous is None or previous["stage"] == progress_by_field["stage"] and previous["copied_rows"] <= copied_rows,
        "progress_changed",
    )
    entry["stages"][stage.target] = progress_by_field
    await _cas(capability, prior, updated)


async def record_output_prepared(capability, receipts):
    """Seal the exact complete prepared set after all indexed semantic checks."""
    prior = await _plan(capability)
    updated = deepcopy(prior)
    entry = _forward_entry(capability, updated)
    receipts = tuple(receipts)
    _require(all(type(receipt) is custody.PreparedOutputReceipt for receipt in receipts), "receipt_set_changed")
    values = [receipt.to_payload() for receipt in receipts]
    parsed = _receipt_set(values, capability.targets, custody.PreparedOutputReceipt.from_payload)
    _require(
        entry["phase"] in {"preparing", "prepared"} and set(entry["stages"]) == set(capability.targets),
        "prepared_changed",
    )
    for receipt in parsed:
        actual = await custody.capture_prepared_output(
            capability, receipt.incumbent.target, receipt.successor.relation, expected_incumbent=receipt.incumbent
        )
        _require(actual == receipt, "prepared_changed")
    _require(not entry["prepared"] or entry["prepared"] == values, "prepared_changed")
    entry["phase"], entry["prepared"] = "prepared", values
    _entry(capability.operation_digest, entry)
    await _cas(capability, prior, updated)


async def read_output_history(capability, forward_digest):
    """Read protected history; the caller still authenticates the full publication intent."""
    plan = await _plan(capability)
    entry = _journal(plan)["operations"].get(forward_digest)
    _require(
        entry is not None and entry["phase"] == "published" and tuple(entry["targets"]) == capability.targets,
        "history_missing",
    )
    last = entry["history"][-1]
    _require(
        capability.reference == last["operation_digest"]
        or capability.reference is None
        and capability.operation_digest == forward_digest,
        "history_reference_changed",
    )
    return _receipt_set(last["receipts"], capability.targets, RetainedOutputReceipt.from_payload)


async def append_output_history(capability, prepared_receipts, retained_receipts):
    """Append the complete actual forward/inverse pair as the cut's LAST mutation."""
    prepared_receipts, retained_receipts = tuple(prepared_receipts), tuple(retained_receipts)
    _require(
        all(type(receipt) is custody.PreparedOutputReceipt for receipt in prepared_receipts)
        and all(type(receipt) is RetainedOutputReceipt for receipt in retained_receipts),
        "receipt_set_changed",
    )
    prior = await _plan(capability)
    updated = deepcopy(prior)
    journal = deepcopy(_journal(updated))
    updated[_KEY] = journal
    if capability.reference is None:
        digest = capability.operation_digest
        entry = journal["operations"].get(digest)
        _require(entry is not None and entry["phase"] == "prepared", "prepared_changed")
        expected_preparations = entry["prepared"]
    else:
        matches = [
            (key, entry_by_field)
            for key, entry_by_field in journal["operations"].items()
            if entry_by_field["history"] and entry_by_field["history"][-1]["operation_digest"] == capability.reference
        ]
        _require(capability.operation["kind"] == "rollback" and len(matches) == 1, "history_reference_changed")
        digest, entry = matches[0]
        previous = entry["history"][-1]
        expected_preparations = [
            {key: receipt_by_field[key] for key in ("protocol", "operation_digest", "incumbent", "successor")}
            for receipt_by_field in previous["receipts"]
        ]
    _require(
        tuple(entry["targets"]) == capability.targets
        and [receipt.to_payload() for receipt in prepared_receipts] == expected_preparations,
        "prepared_changed",
    )
    receipt_payloads = [receipt.to_payload() for receipt in retained_receipts]
    parsed = _receipt_set(receipt_payloads, capability.targets, RetainedOutputReceipt.from_payload)
    for preparation, receipt in zip(prepared_receipts, parsed, strict=True):
        _require(
            receipt.operation_digest == capability.operation_digest and receipt.schema == capability.policy.schema,
            "history_changed",
        )
        capture = custody.capture_retained_output if capability.reference is None else custody.capture_restored_output
        incumbent, successor = await capture(capability, preparation, receipt.retained_name)
        _require(incumbent == receipt.incumbent and successor == receipt.successor, "history_changed")
    entry["history"].append(
        {
            "generation": len(entry["history"]) + 1,
            "operation": capability.operation,
            "operation_digest": capability.operation_digest,
            "reference": capability.reference,
            "receipts": receipt_payloads,
        }
    )
    entry["phase"] = "published"
    _entry(digest, entry)
    await _cas(capability, prior, updated)
