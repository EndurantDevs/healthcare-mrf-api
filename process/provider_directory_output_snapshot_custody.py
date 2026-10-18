# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original-session authority and immutable native receipts for fixed outputs."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, fields, replace

from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_copy_session,
    source_transaction,
)
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    OUTPUT_ROOTS,
    decoded,
    encoded,
    require_output,
    require_output_shape,
)
from process.provider_directory_projection_stage import _copy_driver

_OPERATION_FIELDS = {"kind", "run_id", "producer", "source_pins", "resource_types", "source_fence_digest"}
_KINDS = {
    "source_projection",
    "rehydrate",
    "contact_backfill",
    "coordinate_backfill",
    "npi_backfill",
    "address_key_backfill",
    "canonical_backfill",
    "stale_filter",
    "source_catalog_filter",
}
_OUTPUT = ContextVar("provider_directory_output_snapshot", default=None)
_PIN_FIELDS = {
    "source_id",
    "endpoint_id",
    "dataset_id",
    "acquisition_root_run_id",
    "dataset_hash",
    "resource_hash_contract",
    "semantic_projection_as_of",
    "publication_metadata_sha256",
}


def _digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def _operation_digest(operation, target_names):
    require_output(
        type(target_names) is tuple
        and bool(target_names)
        and len(target_names) == len(set(target_names))
        and set(target_names) <= set(OUTPUT_ROOTS),
        "targets_invalid",
    )
    require_output(type(operation) is dict, "operation_invalid")
    is_rollback = operation.get("kind") == "rollback"
    require_output(
        set(operation) == _OPERATION_FIELDS | ({"rollback_of"} if is_rollback else set()), "operation_invalid"
    )
    require_output(is_rollback or operation["kind"] in _KINDS, "operation_invalid")
    if is_rollback:
        require_output(
            type(operation["rollback_of"]) is str
            and len(operation["rollback_of"]) == 64
            and set(operation["rollback_of"]) <= set("0123456789abcdef"),
            "operation_invalid",
        )
    require_output(
        all(type(operation[key]) is str and operation[key] for key in ("run_id", "producer", "source_fence_digest"))
        and len(operation["source_fence_digest"]) == 64
        and set(operation["source_fence_digest"]) <= set("0123456789abcdef"),
        "operation_invalid",
    )
    resources, pins = operation["resource_types"], operation["source_pins"]
    require_output(
        type(resources) in {tuple, list}
        and len(resources) == len(set(resources))
        and all(type(resource) is str and resource for resource in resources)
        and type(pins) in {tuple, list},
        "operation_invalid",
    )
    for pin in pins:
        require_output(
            type(pin) is dict
            and set(pin) == _PIN_FIELDS
            and all(type(pin[key]) is str and pin[key] for key in _PIN_FIELDS - {"semantic_projection_as_of"})
            and (pin["semantic_projection_as_of"] is None or type(pin["semantic_projection_as_of"]) is str),
            "source_identity_invalid",
        )
    require_output(operation["kind"] not in {"rehydrate", "source_projection"} or bool(pins), "source_identity_invalid")
    return _digest({"protocol": 1, "operation": operation, "targets": target_names})


@dataclass(frozen=True)
class NativeOutputRelation:
    """Canonical JSON atoms keep every nested native receipt immutable."""

    schema: str
    schema_oid: int
    target: str
    relation: str
    oid: int
    owner_oid: int
    columns: tuple[str, ...]
    constraints: tuple[str, ...]
    indexes: tuple[str, ...]
    table: str
    acl: str
    descriptor: str | None

    def to_payload(self):
        """Return a detached JSON representation of the immutable native proof."""
        payload_by_field = {field.name: getattr(self, field.name) for field in fields(self)}
        for name in ("columns", "constraints", "indexes"):
            payload_by_field[name] = [decoded(value) for value in payload_by_field[name]]
        for name in ("table", "acl", "descriptor"):
            payload_by_field[name] = decoded(payload_by_field[name]) if payload_by_field[name] is not None else None
        return payload_by_field

    @classmethod
    def from_payload(cls, receipt_by_field):
        """Decode structure only; a fresh original-session native gate grants authority."""
        require_output(
            type(receipt_by_field) is dict and set(receipt_by_field) == {field.name for field in fields(cls)},
            "receipt_invalid",
        )
        payload_by_field = dict(receipt_by_field)
        for name in ("columns", "constraints", "indexes"):
            require_output(
                type(payload_by_field[name]) is list
                and all(type(atom_by_field) is dict for atom_by_field in payload_by_field[name]),
                "receipt_invalid",
            )
            payload_by_field[name] = tuple(encoded(atom_by_field) for atom_by_field in payload_by_field[name])
        for name in ("table", "acl", "descriptor"):
            require_output(
                type(payload_by_field[name]) is dict or name == "descriptor" and payload_by_field[name] is None,
                "receipt_invalid",
            )
            payload_by_field[name] = encoded(payload_by_field[name]) if payload_by_field[name] is not None else None
        require_output(
            all(
                type(payload_by_field[key]) is int and 0 < payload_by_field[key] < 2**32
                for key in ("schema_oid", "oid", "owner_oid")
            )
            and all(
                type(payload_by_field[key]) is str and payload_by_field[key] for key in ("schema", "target", "relation")
            )
            and payload_by_field["target"] in OUTPUT_ROOTS,
            "receipt_invalid",
        )
        return cls(**payload_by_field)


@dataclass(frozen=True)
class PreparedOutputReceipt:
    protocol: int
    operation_digest: str
    incumbent: NativeOutputRelation
    successor: NativeOutputRelation

    def to_payload(self):
        """Serialize both exact native proofs and their admitted operation digest."""
        return {
            "protocol": self.protocol,
            "operation_digest": self.operation_digest,
            "incumbent": self.incumbent.to_payload(),
            "successor": self.successor.to_payload(),
        }

    @classmethod
    def from_payload(cls, payload):
        """Decode the closed receipt shape without accepting publication authority."""
        require_output(
            type(payload) is dict
            and set(payload) == {"protocol", "operation_digest", "incumbent", "successor"}
            and type(payload["protocol"]) is int
            and payload["protocol"] == 1
            and type(payload["operation_digest"]) is str
            and len(payload["operation_digest"]) == 64
            and set(payload["operation_digest"]) <= set("0123456789abcdef"),
            "receipt_invalid",
        )
        return cls(
            1,
            payload["operation_digest"],
            NativeOutputRelation.from_payload(payload["incumbent"]),
            NativeOutputRelation.from_payload(payload["successor"]),
        )


@dataclass(frozen=True)
class _OutputCapability:
    session: object
    connection: object
    policy: object
    operation_digest: str
    targets: tuple[str, ...]
    task: object
    backend: int
    xid: str
    publisher: object
    reference: str | None
    operation_json: str

    @property
    def operation(self):
        """Return a copy of the admitted intent, never its mutable caller input."""
        return decoded(self.operation_json)


async def _actor(connection):
    require_output(connection.is_in_transaction() is True, "transaction_required")
    return dict(
        await connection.fetchrow(
            "SELECT session_user::text AS session,current_user::text AS current,"
            "pg_backend_pid() AS backend,pg_current_xact_id()::text AS xid"
        )
    )


async def _require_capability(capability):
    require_output(
        type(capability) is _OutputCapability
        and _OUTPUT.get() is capability
        and capability.task is asyncio.current_task(),
        "capability_required",
    )
    publisher = bound_publisher()
    require_output(
        publisher is capability.publisher
        and publisher.policy == capability.policy
        and publisher.connection is capability.connection
        and source_copy_session() is capability.session
        and await _copy_driver(capability.session) is capability.connection,
        "original_session_changed",
    )
    actor = await _actor(capability.connection)
    require_output(
        actor
        == {
            "session": capability.policy.publisher_role,
            "current": capability.policy.owner_role,
            "backend": capability.backend,
            "xid": capability.xid,
        },
        "original_transaction_changed",
    )


async def _capture(capability, targets_by_name, *, private=False):
    await _require_capability(capability)
    owner, readers = await storage_roles(
        capability.connection, capability.policy.schema, **capability.policy.role_arguments()
    )
    allowed_readers = [] if private is True else readers
    catalog_entries = await capability.connection.fetch(
        OUTPUT_CATALOG_SQL,
        capability.policy.schema,
        list(targets_by_name),
        owner,
        allowed_readers,
    )
    require_output(len(catalog_entries) == len(targets_by_name), "storage_missing")
    from process.mrf_address_publication import require_native_read_catalog

    await require_native_read_catalog(
        capability.session,
        tuple(catalog_entry["oid"] for catalog_entry in catalog_entries),
        executable_roles=(capability.policy.owner_role, capability.policy.publisher_role),
    )
    proof_by_name = {}
    for catalog_entry in catalog_entries:
        name = catalog_entry["relname"]
        require_output(name in targets_by_name, "storage_changed")
        require_output(
            catalog_entry["relowner"] == owner
            and catalog_entry["acl_closed"]
            and catalog_entry["readers_closed"]
            and (private is not False or catalog_entry["readers_select"]),
            "custody_changed",
        )
        target_name = targets_by_name[name]
        require_output_shape(catalog_entry, target_name)
        proof_by_name[name] = _native_receipt(capability.policy, catalog_entry, target_name)
    return proof_by_name


def _native_receipt(policy, row, target):
    """Construct only after the calling native custody verifier has admitted the row."""
    descriptor = row["descriptor"]
    if descriptor is not None:
        try:
            descriptor = decoded(descriptor)
        except (ValueError, TypeError) as error:
            raise RuntimeError("provider_directory_output_snapshot_descriptor_changed") from error
        require_output(type(descriptor) is dict, "descriptor_changed")
    return NativeOutputRelation(
        policy.schema,
        row["schema_oid"],
        target,
        row["relname"],
        row["oid"],
        row["relowner"],
        *(tuple(encoded(value) for value in decoded(row[key])) for key in ("columns", "constraints", "indexes")),
        encoded(decoded(row["topology"])),
        encoded(decoded(row["acl"])),
        encoded(descriptor) if descriptor is not None else None,
    )


async def _issue_output_capability(directory, session, operation, targets, operation_digest):
    """Bind the admitted intent to the genuine original native transaction."""
    publisher = bound_publisher()
    require_output(directory._schema() == publisher.policy.schema, "schema_changed")
    actor = await _actor(publisher.connection)
    return _OutputCapability(
        session,
        publisher.connection,
        publisher.policy,
        operation_digest,
        targets,
        asyncio.current_task(),
        actor["backend"],
        actor["xid"],
        publisher,
        operation.get("rollback_of"),
        encoded(operation),
    )


def _require_original_output_session(directory):
    """Refuse replacing another publisher or task's already-bound session."""
    if directory.db._transaction_binding() is not None:
        # A bound CMS/other publisher session cannot be replaced by the Factory.
        require_output(source_copy_session() is directory.db._transaction_binding().session, "original_session_changed")


@asynccontextmanager
async def output_snapshot_transaction(directory, *, operation, targets, isolation="repeatable read"):
    """Admit fixed output work on one existing genuine Publisher owner transaction."""
    operation_digest = _operation_digest(operation, targets)
    require_output(_OUTPUT.get() is None, "capability_already_bound")
    _require_original_output_session(directory)
    async with source_transaction(directory.db, isolation=isolation) as session:
        capability = await _issue_output_capability(directory, session, operation, targets, operation_digest)
        token = _OUTPUT.set(capability)
        try:
            # Whole-family closure establishes immutability before preservation reads.
            await _capture(capability, {name: name for name in OUTPUT_ROOTS})
            yield capability
            await _require_capability(capability)
        finally:
            _OUTPUT.reset(token)


def _require_stage_descriptor(capability, successor, incumbent):
    descriptor = decoded(successor.descriptor)
    require_output(
        descriptor
        == {
            "protocol": 1,
            "operation_digest": capability.operation_digest,
            "target": successor.target,
            "phase": "sealed",
            "incumbent_oid": incumbent.oid,
            "incumbent_digest": _digest(incumbent.to_payload()),
        },
        "stage_unsealed",
    )


async def capture_prepared_output(capability, target, stage, *, expected_incumbent):
    """Capture only a finished fixed stage and its immutable expected incumbent."""
    require_output(
        capability.reference is None and target in capability.targets and type(stage) is str and stage != target,
        "stage_invalid",
    )
    from db.plan_release_storage import storage_identifier

    storage_identifier(stage)
    incumbent = (await _capture(capability, {target: target}))[target]
    require_output(
        type(expected_incumbent) is NativeOutputRelation and incumbent == expected_incumbent,
        "incumbent_changed",
    )
    successor = (await _capture(capability, {stage: target}, private=True))[stage]
    _require_stage_descriptor(capability, successor, incumbent)
    return PreparedOutputReceipt(1, capability.operation_digest, incumbent, successor)


async def recheck_output_cut(capability, receipts):
    """Recheck exact native proofs after the caller locks the complete live set."""
    await _require_capability(capability)
    require_output(capability.reference is None, "forward_operation_required")
    require_output(
        type(receipts) is tuple
        and len(receipts) == len(capability.targets)
        and all(type(receipt) is PreparedOutputReceipt for receipt in receipts)
        and {receipt.incumbent.target for receipt in receipts} == set(capability.targets),
        "receipt_set_changed",
    )
    for receipt in receipts:
        require_output(
            receipt.protocol == 1
            and receipt.operation_digest == capability.operation_digest
            and receipt.incumbent.schema == receipt.successor.schema == capability.policy.schema
            and receipt.incumbent.target == receipt.successor.target
            and receipt.incumbent.oid != receipt.successor.oid,
            "receipt_identity_changed",
        )
        _require_stage_descriptor(capability, receipt.successor, receipt.incumbent)
    # Capture private and serving proofs separately: stages have no Reader grants.
    serving = await _capture(capability, {receipt.incumbent.relation: receipt.incumbent.target for receipt in receipts})
    private = await _capture(
        capability, {receipt.successor.relation: receipt.successor.target for receipt in receipts}, private=True
    )
    require_output(
        all(
            serving[receipt.incumbent.relation] == receipt.incumbent
            and private[receipt.successor.relation] == receipt.successor
            for receipt in receipts
        ),
        "native_receipt_changed",
    )


async def _published_successor(capability, prepared_receipt):
    """Derive exactly the permitted Reader SELECT ACL change, not a new baseline."""
    owner, readers = await storage_roles(
        capability.connection, capability.policy.schema, **capability.policy.role_arguments()
    )
    acl = decoded(prepared_receipt.successor.acl)
    acl["table"] = sorted(acl["table"] + [[reader, owner, "SELECT", False] for reader in readers])
    return replace(prepared_receipt.successor, relation=prepared_receipt.successor.target, acl=encoded(acl))


async def publish_output_read_access(capability, prepared_receipt):
    """Grant only the configured Readers SELECT on the exact installed successor."""
    from db.plan_release_storage import storage_identifier, storage_relation

    await _require_capability(capability)
    require_output(
        capability.reference is None
        and type(prepared_receipt) is PreparedOutputReceipt
        and prepared_receipt.operation_digest == capability.operation_digest
        and prepared_receipt.successor.target in capability.targets,
        "receipt_identity_changed",
    )
    _require_stage_descriptor(capability, prepared_receipt.successor, prepared_receipt.incumbent)
    target = prepared_receipt.successor.target
    current = (await _capture(capability, {target: target}, private=True))[target]
    require_output(current == replace(prepared_receipt.successor, relation=target), "native_receipt_changed")
    readers = ",".join(storage_identifier(role) for role in capability.policy.runtime_roles)
    await capability.connection.execute(
        f"GRANT SELECT ON TABLE {storage_relation(capability.policy.schema, target)} TO {readers}"
    )
    current = (await _capture(capability, {target: target}))[target]
    require_output(current == await _published_successor(capability, prepared_receipt), "native_receipt_changed")


async def capture_retained_output(capability, prepared_receipt, retained_name):
    """Authenticate only the forward renames and fixed Reader grant delta."""
    from db.plan_release_storage import storage_identifier

    await _require_capability(capability)
    require_output(
        capability.reference is None
        and type(prepared_receipt) is PreparedOutputReceipt
        and prepared_receipt.operation_digest == capability.operation_digest
        and prepared_receipt.incumbent.target == prepared_receipt.successor.target
        and prepared_receipt.incumbent.target in capability.targets,
        "receipt_identity_changed",
    )
    storage_identifier(retained_name)
    _require_stage_descriptor(capability, prepared_receipt.successor, prepared_receipt.incumbent)
    target = prepared_receipt.incumbent.target
    require_output(
        retained_name not in OUTPUT_ROOTS and retained_name != prepared_receipt.successor.relation,
        "retained_name_invalid",
    )
    actual = await _capture(capability, {target: target, retained_name: target})
    old, new = actual[retained_name], actual[target]
    require_output(
        old == replace(prepared_receipt.incumbent, relation=retained_name)
        and new == await _published_successor(capability, prepared_receipt),
        "native_receipt_changed",
    )
    return old, new


async def recheck_output_restore(capability, prepared_receipts, retained_names):
    """Authorize exact recorded post-cut predecessors under a fresh rollback intent."""
    from db.plan_release_storage import storage_identifier

    await _require_capability(capability)
    require_output(
        capability.reference is not None
        and type(prepared_receipts) is tuple
        and len(prepared_receipts) == len(capability.targets)
        and type(retained_names) is dict
        and set(retained_names) == set(capability.targets)
        and all(type(receipt) is PreparedOutputReceipt for receipt in prepared_receipts)
        and {receipt.incumbent.target for receipt in prepared_receipts} == set(capability.targets),
        "restore_receipt_set_changed",
    )
    relation_by_name = {}
    for receipt in prepared_receipts:
        target_name = receipt.incumbent.target
        name = retained_names[target_name]
        storage_identifier(name)
        require_output(
            receipt.protocol == 1
            and receipt.operation_digest == capability.reference
            and receipt.incumbent.schema == receipt.successor.schema == capability.policy.schema
            and receipt.incumbent.target == receipt.successor.target
            and receipt.incumbent.relation == name
            and receipt.successor.relation == target_name
            and name not in OUTPUT_ROOTS
            and receipt.incumbent.oid != receipt.successor.oid,
            "restore_receipt_identity_changed",
        )
        relation_by_name[name] = target_name
        relation_by_name[target_name] = target_name
    require_output(len(relation_by_name) == 2 * len(capability.targets), "restore_receipt_identity_changed")
    actual = await _capture(capability, relation_by_name)
    require_output(
        all(
            actual[receipt.incumbent.relation] == receipt.incumbent
            and actual[receipt.successor.relation] == receipt.successor
            for receipt in prepared_receipts
        ),
        "native_restore_receipt_changed",
    )


async def capture_restored_output(capability, stored_forward_prepared_receipt, failed_retained_name):
    """Authenticate name reversal only; historical descriptors and ACLs stay exact."""
    from db.plan_release_storage import storage_identifier

    await _require_capability(capability)
    receipt = stored_forward_prepared_receipt
    require_output(
        capability.reference is not None
        and type(receipt) is PreparedOutputReceipt
        and receipt.protocol == 1
        and receipt.operation_digest == capability.reference
        and receipt.incumbent.target == receipt.successor.target
        and receipt.incumbent.target in capability.targets,
        "restore_receipt_identity_changed",
    )
    storage_identifier(failed_retained_name)
    target = receipt.incumbent.target
    require_output(failed_retained_name not in OUTPUT_ROOTS, "retained_name_invalid")
    actual = await _capture(capability, {target: target, failed_retained_name: target})
    failed, restored = actual[failed_retained_name], actual[target]
    require_output(
        failed == replace(receipt.successor, relation=failed_retained_name)
        and restored == replace(receipt.incumbent, relation=target),
        "native_restore_receipt_changed",
    )
    return failed, restored
