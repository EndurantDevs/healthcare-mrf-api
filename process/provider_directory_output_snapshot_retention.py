# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rotate verified output heaps without deleting their readable predecessors."""

from copy import deepcopy
from dataclasses import dataclass, field

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_output_snapshot_custody as custody
from process.entity_address_cutover_contract import serving_relation_lock_window
from process.provider_directory_generic_source_policy import _savepoint


def _require(condition, code):
    if not condition:
        raise ValueError("provider_directory_output_retention_" + code)


def retained_name(operation, target=None):
    """Name new retained generations by logical operation; old numeric names remain a codec only."""
    if target is None:
        _require(type(operation) is int and 0 < operation < 2**32, "oid_invalid")
        return f"pdout_retained_{operation:x}"
    _require(
        type(operation) is str
        and len(operation) == 64
        and set(operation) <= set("0123456789abcdef")
        and target in custody.OUTPUT_ROOTS,
        "operation_invalid",
    )
    return "pdout_retained_" + custody._digest({"operation_digest": operation, "target": target})[:32]


@dataclass(frozen=True, eq=False)
class RetainedOutputReceipt:
    """Explicit readable predecessor and successor names with logical layout/generation metadata."""

    protocol: int
    operation_digest: str
    schema: str
    target: str
    retained_name: str
    incumbent: custody.NativeOutputRelation
    successor: custody.NativeOutputRelation
    legacy_payload: dict | None = field(default=None, compare=False, repr=False)

    @property
    def old_oid(self):
        """Expose only saved compatibility data; current callers must use declared names."""
        return self.incumbent.oid

    @property
    def new_oid(self):
        """Expose only saved compatibility data; current callers must use declared names."""
        return self.successor.oid

    def named_payload(self):
        """Serialize portable rollback coordinates and complete logical output metadata."""
        return {
            "protocol": 2,
            "operation_digest": self.operation_digest,
            "schema": self.schema,
            "target": self.target,
            "retained_name": self.retained_name,
            "incumbent": self.incumbent.named_payload(),
            "successor": self.successor.named_payload(),
        }

    def to_payload(self):
        """Preserve original retained records; new publication stores only portable metadata."""
        if self.legacy_payload is not None:
            return deepcopy(self.legacy_payload)
        if self.protocol == 2:
            return self.named_payload()
        return {
            **self.named_payload(),
            "protocol": 1,
            "incumbent": self.incumbent.to_payload(),
            "successor": self.successor.to_payload(),
        }

    def __eq__(self, other):
        return isinstance(other, RetainedOutputReceipt) and self.named_payload() == other.named_payload()

    @classmethod
    def from_payload(cls, payload_by_field):
        """Decode the declared coordinates, never recomputing old locations from physical tokens."""
        _require(
            isinstance(payload_by_field, dict)
            and set(payload_by_field)
            == {"protocol", "operation_digest", "schema", "target", "retained_name", "incumbent", "successor"},
            "receipt_invalid",
        )
        prepared = custody.PreparedOutputReceipt.from_payload(
            {key: payload_by_field[key] for key in ("protocol", "operation_digest", "incumbent", "successor")}
        )
        old, new = prepared.incumbent, prepared.successor
        storage_identifier(payload_by_field["retained_name"])
        _require(
            payload_by_field["schema"] == old.schema == new.schema
            and payload_by_field["target"] == old.target == new.target == new.relation
            and payload_by_field["retained_name"] == old.relation
            and old.relation != new.relation
            and payload_by_field["retained_name"] not in custody.OUTPUT_ROOTS,
            "receipt_invalid",
        )
        return cls(
            payload_by_field["protocol"],
            prepared.operation_digest,
            old.schema,
            old.target,
            old.relation,
            old,
            new,
            deepcopy(payload_by_field) if payload_by_field["protocol"] == 1 else None,
        )


async def _require_locks(capability, preparations):
    """Pin every named heap; actual ALTER takes its bounded exclusive native DDL lock."""
    names = sorted({proof.relation for receipt in preparations for proof in (receipt.incumbent, receipt.successor)})
    await capability.connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(capability.policy.schema, name) for name in names)
        + " IN ACCESS SHARE MODE NOWAIT"
    )


async def _require_vacant(capability, names):
    occupied = await capability.connection.fetch(
        "SELECT name FROM unnest($1::text[]) AS candidate(name) WHERE to_regclass(name) IS NOT NULL",
        [storage_relation(capability.policy.schema, name) for name in names],
    )
    _require(not occupied, "retained_name_occupied")


async def _rename(capability, original, replacement):
    await capability.connection.execute(
        f"ALTER TABLE {storage_relation(capability.policy.schema, original)} RENAME TO {storage_identifier(replacement)}"
    )


def _receipt(capability, name, incumbent, successor):
    receipt = RetainedOutputReceipt(
        2, capability.operation_digest, incumbent.schema, incumbent.target, name, incumbent, successor
    )
    # The decoder also checks that post-cut coordinates match both native proofs.
    return RetainedOutputReceipt.from_payload(receipt.to_payload())


async def retain_prepared_outputs(capability, prepared_receipts):
    """Publish the complete verified set inside the caller's original transaction."""
    preparations = tuple(prepared_receipts)
    await custody.recheck_output_cut(capability, preparations)
    await _require_locks(capability, preparations)
    names = tuple(retained_name(capability.operation_digest, receipt.incumbent.target) for receipt in preparations)
    await _require_vacant(capability, names)
    async with _savepoint(capability.connection):
        async with serving_relation_lock_window():
            for receipt, name in zip(preparations, names, strict=True):
                await _rename(capability, receipt.incumbent.relation, name)
            for receipt in preparations:
                await _rename(capability, receipt.successor.relation, receipt.successor.target)
        results = []
        for receipt, name in zip(preparations, names, strict=True):
            await custody.publish_output_read_access(capability, receipt)
            incumbent, successor = await custody.capture_retained_output(capability, receipt, name)
            results.append(_receipt(capability, name, incumbent, successor))
        return tuple(results)


async def restore_retained_outputs(capability, retained_receipts):
    """Reverse an authenticated stored cut while retaining its replaced successor."""
    retained_outputs = tuple(retained_receipts)
    _require(all(type(receipt) is RetainedOutputReceipt for receipt in retained_outputs), "receipt_invalid")
    preparations = tuple(
        custody.PreparedOutputReceipt(2, receipt.operation_digest, receipt.incumbent, receipt.successor)
        for receipt in retained_outputs
    )
    await custody.recheck_output_restore(
        capability, preparations, {receipt.target: receipt.retained_name for receipt in retained_outputs}
    )
    await _require_locks(capability, preparations)
    names = tuple(retained_name(capability.operation_digest, receipt.target) for receipt in retained_outputs)
    await _require_vacant(capability, names)
    async with _savepoint(capability.connection):
        async with serving_relation_lock_window():
            for receipt, name in zip(retained_outputs, names, strict=True):
                await _rename(capability, receipt.successor.relation, name)
            for receipt in retained_outputs:
                await _rename(capability, receipt.incumbent.relation, receipt.target)
        results = []
        for receipt, name in zip(preparations, names, strict=True):
            incumbent, successor = await custody.capture_restored_output(capability, receipt, name)
            results.append(_receipt(capability, name, incumbent, successor))
        return tuple(results)
