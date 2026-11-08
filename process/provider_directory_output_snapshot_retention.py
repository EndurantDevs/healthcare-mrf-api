# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rotate verified output heaps without deleting their readable predecessors."""

from dataclasses import dataclass

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_output_snapshot_custody as custody
from process.provider_directory_generic_source_policy import _savepoint


def _require(condition, code):
    if not condition:
        raise ValueError("provider_directory_output_retention_" + code)


def retained_name(oid):
    """A native table OID gives each retained predecessor a unique name."""
    _require(type(oid) is int and 0 < oid < 2**32, "oid_invalid")
    return f"pdout_retained_{oid:x}"


@dataclass(frozen=True)
class RetainedOutputReceipt:
    """Actual post-cut native proofs; persistence alone grants no authority."""

    protocol: int
    operation_digest: str
    schema: str
    target: str
    retained_name: str
    incumbent: custody.NativeOutputRelation
    successor: custody.NativeOutputRelation

    @property
    def old_oid(self):
        """Return the preserved predecessor table identity."""
        return self.incumbent.oid

    @property
    def new_oid(self):
        """Return the installed successor table identity."""
        return self.successor.oid

    def to_payload(self):
        """Store exact native receipts in the existing protected publication record."""
        return {
            "protocol": self.protocol,
            "operation_digest": self.operation_digest,
            "schema": self.schema,
            "target": self.target,
            "retained_name": self.retained_name,
            "incumbent": self.incumbent.to_payload(),
            "successor": self.successor.to_payload(),
        }

    @classmethod
    def from_payload(cls, payload):
        """Decode structure only; a fresh native capability must recheck the cut."""
        _require(
            type(payload) is dict
            and set(payload)
            == {"protocol", "operation_digest", "schema", "target", "retained_name", "incumbent", "successor"},
            "receipt_invalid",
        )
        prepared = custody.PreparedOutputReceipt.from_payload(
            {key: payload[key] for key in ("protocol", "operation_digest", "incumbent", "successor")}
        )
        incumbent, successor = prepared.incumbent, prepared.successor
        _require(
            payload["schema"] == incumbent.schema == successor.schema
            and payload["target"] == incumbent.target == successor.target == successor.relation
            and payload["retained_name"] == incumbent.relation == retained_name(incumbent.oid)
            and incumbent.schema_oid == successor.schema_oid
            and incumbent.oid != successor.oid,
            "receipt_invalid",
        )
        return cls(
            1,
            prepared.operation_digest,
            incumbent.schema,
            incumbent.target,
            incumbent.relation,
            incumbent,
            successor,
        )


async def _require_locks(capability, preparations):
    required_oids = {proof.oid for receipt in preparations for proof in (receipt.incumbent, receipt.successor)}
    locks = await capability.connection.fetch(
        "SELECT relation::bigint AS oid FROM pg_locks WHERE pid=pg_backend_pid() "
        "AND locktype='relation' AND mode='AccessExclusiveLock' AND granted AND relation=ANY($1::oid[])",
        sorted(required_oids),
    )
    _require({row["oid"] for row in locks} == required_oids, "complete_live_locks_required")


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
        1, capability.operation_digest, incumbent.schema, incumbent.target, name, incumbent, successor
    )
    # The decoder also checks that post-cut coordinates match both native proofs.
    return RetainedOutputReceipt.from_payload(receipt.to_payload())


async def retain_prepared_outputs(capability, prepared_receipts):
    """Publish the complete verified set inside the caller's original transaction."""
    preparations = tuple(prepared_receipts)
    await custody.recheck_output_cut(capability, preparations)
    await _require_locks(capability, preparations)
    names = tuple(retained_name(receipt.incumbent.oid) for receipt in preparations)
    await _require_vacant(capability, names)
    async with _savepoint(capability.connection):
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
        custody.PreparedOutputReceipt(1, receipt.operation_digest, receipt.incumbent, receipt.successor)
        for receipt in retained_outputs
    )
    await custody.recheck_output_restore(
        capability, preparations, {receipt.target: receipt.retained_name for receipt in retained_outputs}
    )
    await _require_locks(capability, preparations)
    names = tuple(retained_name(receipt.successor.oid) for receipt in retained_outputs)
    await _require_vacant(capability, names)
    async with _savepoint(capability.connection):
        for receipt, name in zip(retained_outputs, names, strict=True):
            await _rename(capability, receipt.successor.relation, name)
        for receipt in retained_outputs:
            await _rename(capability, receipt.incumbent.relation, receipt.target)
        results = []
        for receipt, name in zip(preparations, names, strict=True):
            incumbent, successor = await custody.capture_restored_output(capability, receipt, name)
            results.append(_receipt(capability, name, incumbent, successor))
        return tuple(results)
