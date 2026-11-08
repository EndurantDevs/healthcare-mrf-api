# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed retained-output protocol inside existing resource checkpoint evidence."""

from copy import deepcopy

from process.provider_directory_dataset_rehydrate_pages import _key_columns
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError
from process.provider_directory_output_snapshot_catalog import output_models
from process.provider_directory_output_snapshot_custody import PreparedOutputReceipt, _operation_digest
from process.provider_directory_output_snapshot_retention import RetainedOutputReceipt
from process.provider_directory_output_snapshot_stage import OutputStage


def _require(condition, code):
    if not condition:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_journal_" + code)


def _members(members):
    _require(
        type(members) in {list, tuple}
        and 1 <= len(members) <= 8
        and all(type(member) is str and member for member in members)
        and list(members) == sorted(set(members)),
        "members_invalid",
    )
    return list(members)


def _intent(value):
    _require(type(value) is dict and {"operation", "targets", "members"} <= set(value), "shape_invalid")
    _members(value["members"])
    _require(
        type(value["targets"]) is list
        and bool(value["targets"])
        and len(value["targets"]) == len(set(value["targets"]))
        and sorted(value["operation"]["resource_types"]) == value["members"],
        "intent_invalid",
    )
    return _operation_digest(value["operation"], tuple(value["targets"]))


def _stage(entry, digest, target):
    _require(type(entry) is dict and set(entry) == {"stage", "cursor", "rows", "receipt"}, "stage_invalid")
    stage = OutputStage.from_payload(entry["stage"])
    _require(stage.operation_digest == digest and stage.target == target, "stage_changed")
    _require(type(entry["rows"]) is int and entry["rows"] >= 0, "count_invalid")
    cursor = entry["cursor"]
    _require(
        cursor is None
        or type(cursor) is list
        and len(cursor) == len(_key_columns(output_models()[target]))
        and all(type(cell) is str for cell in cursor),
        "cursor_invalid",
    )
    _require((entry["rows"] == 0) == (cursor is None), "cursor_invalid")
    if entry["receipt"] is not None:
        receipt = PreparedOutputReceipt.from_payload(entry["receipt"])
        _require(
            receipt.operation_digest == digest
            and receipt.incumbent == stage.incumbent
            and receipt.successor.oid == stage.oid
            and receipt.successor.relation == stage.relation
            and receipt.successor.target == target,
            "stage_receipt_changed",
        )


def _generation(generation_by_field, digest):
    _require(
        type(generation_by_field) is dict
        and set(generation_by_field) == {"operation", "targets", "members", "prepared", "published", "proofs"}
        and _intent(generation_by_field) == digest,
        "generation_invalid",
    )
    prepared = generation_by_field["prepared"]
    _require(type(prepared) is list, "generation_invalid")
    if generation_by_field["operation"]["kind"] != "rollback":
        receipts = [PreparedOutputReceipt.from_payload(receipt_payload) for receipt_payload in prepared]
        _require(
            [receipt.incumbent.target for receipt in receipts] == generation_by_field["targets"]
            and all(receipt.operation_digest == digest for receipt in receipts),
            "generation_invalid",
        )
    else:
        _require(prepared == [], "generation_invalid")
    published = generation_by_field["published"]
    if published is not None:
        _require(type(published) is list, "generation_invalid")
        receipts = [RetainedOutputReceipt.from_payload(receipt_payload) for receipt_payload in published]
        _require(
            [receipt.target for receipt in receipts] == generation_by_field["targets"]
            and all(receipt.operation_digest == digest for receipt in receipts)
            and type(generation_by_field["proofs"]) is dict
            and set(generation_by_field["proofs"]) == set(generation_by_field["members"]),
            "generation_invalid",
        )
        if prepared:
            _require(
                all(
                    before["incumbent"]["oid"] == after.old_oid and before["successor"]["oid"] == after.new_oid
                    for before, after in zip(prepared, receipts, strict=True)
                ),
                "generation_invalid",
            )
    else:
        _require(generation_by_field["proofs"] is None, "generation_invalid")


def output_journal(evidence):
    """Validate stored structure; native custody remains a separate mandatory check."""
    value = deepcopy(evidence.get("output_snapshots", {"protocol": 1, "active": None, "generations": {}}))
    _require(
        type(value) is dict
        and set(value) == {"protocol", "active", "generations"}
        and type(value["protocol"]) is int
        and value["protocol"] == 1
        and type(value["generations"]) is dict,
        "shape_invalid",
    )
    for digest, generation in value["generations"].items():
        _generation(generation, digest)
    active = value["active"]
    if active is not None:
        _require(
            type(active) is dict
            and set(active) == {"operation_digest", "operation", "targets", "members", "stages"}
            and _intent(active) == active["operation_digest"]
            and type(active["stages"]) is dict
            and set(active["stages"]) <= set(active["targets"]),
            "active_invalid",
        )
        for target, entry in active["stages"].items():
            _stage(entry, active["operation_digest"], target)
    return value


def merge_completed_histories(journals):
    """Combine only immutable completed histories; active members must remain identical."""
    _require(bool(journals), "members_invalid")
    values = [output_journal({"output_snapshots": journal}) for journal in journals]
    result = values[0]
    if all(value == result for value in values):
        return result
    _require(all(value["active"] is None for value in values), "member_receipts_disagree")
    for value in values:
        for digest, generation in value["generations"].items():
            _require(generation["published"] is not None, "member_receipts_disagree")
            prior = result["generations"].get(digest)
            _require(prior is None or prior == generation, "member_receipts_disagree")
            result["generations"][digest] = deepcopy(generation)
    return result


def begin_preparation(journal, operation, targets, members):
    """A changed intent cannot adopt another attempt's private stages."""
    result = output_journal({"output_snapshots": journal})
    intent_by_field = dict(operation=deepcopy(operation), targets=list(targets), members=_members(members))
    digest = _intent(intent_by_field)
    active = result["active"]
    if active is not None:
        _require({key: active[key] for key in intent_by_field} == intent_by_field, "active_conflict")
    else:
        _require(digest not in result["generations"], "generation_exists")
        result["active"] = dict(operation_digest=digest, **intent_by_field, stages={})
    return result


def record_stage(journal, stage, *, cursor, rows, receipt=None):
    """COPY and its exact stage/cursor/count witness commit together."""
    result = output_journal({"output_snapshots": journal})
    active = result["active"]
    _require(active is not None and stage.operation_digest == active["operation_digest"], "active_conflict")
    _require(stage.target in active["targets"], "stage_changed")
    entry_by_field = dict(
        stage=stage.to_payload(),
        cursor=list(cursor) if cursor is not None else None,
        rows=rows,
        receipt=receipt.to_payload() if receipt is not None else None,
    )
    _stage(entry_by_field, stage.operation_digest, stage.target)
    old = active["stages"].get(stage.target)
    if old is not None:
        _require(old["stage"] == entry_by_field["stage"] and rows >= old["rows"], "stage_changed")
        _require(old["receipt"] is None or old == entry_by_field, "sealed_stage_changed")
        _require(rows > old["rows"] or old["cursor"] == entry_by_field["cursor"], "cursor_changed")
    active["stages"][stage.target] = entry_by_field
    return result


def seal_preparation(journal):
    """Append immutable prepared receipts before the preparation transaction commits."""
    result = output_journal({"output_snapshots": journal})
    active = result["active"]
    _require(active is not None and set(active["stages"]) == set(active["targets"]), "preparation_incomplete")
    prepared_receipts = [active["stages"][target]["receipt"] for target in active["targets"]]
    _require(all(receipt is not None for receipt in prepared_receipts), "preparation_incomplete")
    generation_by_field = {key: deepcopy(active[key]) for key in ("operation", "targets", "members")}
    generation_by_field.update(prepared=prepared_receipts, published=None, proofs=None)
    digest = active["operation_digest"]
    _generation(generation_by_field, digest)
    _require(result["generations"].get(digest, generation_by_field) == generation_by_field, "generation_changed")
    result["generations"][digest] = generation_by_field
    return result


def publish_generation(journal, digest, retained, proofs):
    """The same physical cut transaction appends receipts and clears its active intent."""
    result = output_journal({"output_snapshots": journal})
    active = result["active"]
    _require(active is not None and active["operation_digest"] == digest, "active_conflict")
    generation = result["generations"].get(digest)
    _require(generation is not None, "preparation_incomplete")
    published_by_field = {
        **generation,
        "published": [receipt.to_payload() for receipt in retained],
        "proofs": deepcopy(proofs),
    }
    _generation(published_by_field, digest)
    _require(generation["published"] is None or generation == published_by_field, "generation_changed")
    result["generations"][digest] = published_by_field
    result["active"] = None
    return result


def record_rollback(journal, operation, targets, members, retained, proofs):
    """An inverse cut adds a new generation without rewriting its predecessor."""
    result = output_journal({"output_snapshots": journal})
    _require(result["active"] is None and operation.get("kind") == "rollback", "active_conflict")
    prior = result["generations"].get(operation.get("rollback_of"))
    _require(prior is not None and prior["published"] is not None, "rollback_unknown")
    generation_by_field = dict(
        operation=deepcopy(operation),
        targets=list(targets),
        members=_members(members),
        prepared=[],
        published=[receipt.to_payload() for receipt in retained],
        proofs=deepcopy(proofs),
    )
    digest = _intent(generation_by_field)
    _generation(generation_by_field, digest)
    _require(
        prior["targets"] == generation_by_field["targets"]
        and prior["members"] == generation_by_field["members"]
        and all(
            old["incumbent"]["oid"] == new.new_oid and old["successor"]["oid"] == new.old_oid
            for old, new in zip(prior["published"], retained, strict=True)
        ),
        "rollback_changed",
    )
    _require(result["generations"].get(digest, generation_by_field) == generation_by_field, "generation_changed")
    result["generations"][digest] = generation_by_field
    return result
