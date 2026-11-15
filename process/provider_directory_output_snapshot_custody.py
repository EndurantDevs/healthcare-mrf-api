# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable named receipts and logical operation state for fixed output snapshots."""

import hashlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field, replace

from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_transaction,
)
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    OUTPUT_ROOTS,
    decoded,
    encoded,
    portable_catalog_payload,
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


def logical_output_descriptor(value):
    """Project only logical stage metadata; old physical hashes and IDs remain opaque."""
    if value is None:
        return None
    descriptor = decoded(value)
    require_output(isinstance(descriptor, dict), "descriptor_changed")
    if descriptor.get("protocol") not in (1, 2):
        return None
    require_output(all(key in descriptor for key in ("operation_digest", "target", "phase")), "descriptor_changed")
    return {
        "protocol": 2,
        "operation_digest": descriptor["operation_digest"],
        "target": descriptor["target"],
        "phase": descriptor["phase"],
        "incumbent_digest": descriptor.get("incumbent_digest") if descriptor["protocol"] == 2 else None,
    }


@dataclass(frozen=True, eq=False)
class NativeOutputRelation:
    """Named logical output metadata, retaining old raw catalog fields only for decoding."""

    schema: str
    schema_oid: object = field(default=None, compare=False)
    target: str = ""
    relation: str = ""
    oid: object = field(default=None, compare=False)
    owner_oid: object = field(default=None, compare=False)
    columns: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    indexes: tuple[str, ...] = ()
    table: str = "{}"
    acl: str | None = field(default=None, compare=False)
    descriptor: str | None = None
    legacy_payload: dict | None = field(default=None, compare=False, repr=False)

    def named_payload(self):
        """Return the portable current edition, excluding all physical compatibility data."""
        layout = portable_catalog_payload(
            {
                "relation": self.relation,
                "table": self.table,
                "columns": self.columns,
                "constraints": self.constraints,
                "indexes": self.indexes,
            }
        )
        return {
            "protocol": 2,
            "schema": self.schema,
            "target": self.target,
            "relation": self.relation,
            "columns": layout["columns"],
            "constraints": layout["constraints"],
            "indexes": layout["indexes"],
            "table": layout["topology"],
            "descriptor": logical_output_descriptor(self.descriptor),
        }

    def to_payload(self):
        """Preserve old saved bytes semantically; fresh output always uses the named edition."""
        if self.legacy_payload is not None:
            return deepcopy(self.legacy_payload)
        if any(token is not None for token in (self.schema_oid, self.oid, self.owner_oid)):
            return {
                "schema": self.schema,
                "schema_oid": self.schema_oid,
                "target": self.target,
                "relation": self.relation,
                "oid": self.oid,
                "owner_oid": self.owner_oid,
                **{
                    key: [decoded(atom) for atom in getattr(self, key)] for key in ("columns", "constraints", "indexes")
                },
                "table": decoded(self.table),
                "acl": decoded(self.acl),
                "descriptor": None if self.descriptor is None else decoded(self.descriptor),
            }
        return self.named_payload()

    def __eq__(self, other):
        return isinstance(other, NativeOutputRelation) and self.named_payload() == other.named_payload()

    @classmethod
    def from_payload(cls, payload_by_field):
        """Decode either closed edition without resolving or validating old physical tokens."""
        require_output(isinstance(payload_by_field, dict), "receipt_invalid")
        named_fields = {
            "protocol",
            "schema",
            "target",
            "relation",
            "columns",
            "constraints",
            "indexes",
            "table",
            "descriptor",
        }
        legacy_fields = (named_fields - {"protocol"}) | {"schema_oid", "oid", "owner_oid", "acl"}
        is_named = payload_by_field.get("protocol") == 2
        require_output(set(payload_by_field) == (named_fields if is_named else legacy_fields), "receipt_invalid")
        from db.plan_release_storage import storage_identifier

        for key in ("schema", "target", "relation"):
            require_output(type(payload_by_field[key]) is str, "receipt_invalid")
            storage_identifier(payload_by_field[key])
        require_output(payload_by_field["target"] in OUTPUT_ROOTS, "receipt_invalid")
        for key in ("columns", "constraints", "indexes"):
            require_output(
                isinstance(payload_by_field[key], list)
                and all(isinstance(atom, dict) for atom in payload_by_field[key]),
                "receipt_invalid",
            )
        require_output(
            isinstance(payload_by_field["table"], dict)
            and (payload_by_field["descriptor"] is None or isinstance(payload_by_field["descriptor"], dict)),
            "receipt_invalid",
        )
        relation = cls(
            schema=payload_by_field["schema"],
            target=payload_by_field["target"],
            relation=payload_by_field["relation"],
            **{
                key: tuple(encoded(atom) for atom in payload_by_field[key])
                for key in ("columns", "constraints", "indexes")
            },
            table=encoded(payload_by_field["table"]),
            descriptor=None if payload_by_field["descriptor"] is None else encoded(payload_by_field["descriptor"]),
            **({} if is_named else {key: payload_by_field[key] for key in ("schema_oid", "oid", "owner_oid")}),
            acl=None if is_named else encoded(payload_by_field["acl"]),
            legacy_payload=None if is_named else deepcopy(payload_by_field),
        )
        if is_named:
            require_output(relation.named_payload() == payload_by_field, "receipt_invalid")
        return relation


@dataclass(frozen=True, eq=False)
class PreparedOutputReceipt:
    protocol: int
    operation_digest: str
    incumbent: NativeOutputRelation
    successor: NativeOutputRelation
    legacy_payload: dict | None = field(default=None, compare=False, repr=False)

    def named_payload(self):
        """Normalize logical current receipts without changing saved legacy payloads."""
        return {
            "protocol": 2,
            "operation_digest": self.operation_digest,
            "incumbent": self.incumbent.named_payload(),
            "successor": self.successor.named_payload(),
        }

    def to_payload(self):
        """Fresh prepared records contain only logical names/layout and operation lineage."""
        if self.legacy_payload is not None:
            return deepcopy(self.legacy_payload)
        if self.protocol == 2:
            return self.named_payload()
        return {
            "protocol": self.protocol,
            "operation_digest": self.operation_digest,
            "incumbent": self.incumbent.to_payload(),
            "successor": self.successor.to_payload(),
        }

    def __eq__(self, other):
        return isinstance(other, PreparedOutputReceipt) and (self.operation_digest, self.incumbent, self.successor) == (
            other.operation_digest,
            other.incumbent,
            other.successor,
        )

    @classmethod
    def from_payload(cls, value):
        """Keep retained legacy metadata intact before logical current comparisons."""
        require_output(
            isinstance(value, dict)
            and set(value) == {"protocol", "operation_digest", "incumbent", "successor"}
            and type(value["protocol"]) is int
            and value["protocol"] in (1, 2)
            and type(value["operation_digest"]) is str
            and len(value["operation_digest"]) == 64
            and set(value["operation_digest"]) <= set("0123456789abcdef"),
            "receipt_invalid",
        )
        return cls(
            value["protocol"],
            value["operation_digest"],
            NativeOutputRelation.from_payload(value["incumbent"]),
            NativeOutputRelation.from_payload(value["successor"]),
            deepcopy(value) if value["protocol"] == 1 else None,
        )


@dataclass(frozen=True)
class _OutputCapability:
    session: object
    connection: object
    policy: object
    operation_digest: str
    targets: tuple[str, ...]
    reference: str | None
    operation_json: str

    @property
    def operation(self):
        """Return the existing admitted operation data without an identity attestation."""
        return decoded(self.operation_json)


async def _require_capability(capability):
    """Use native transaction availability and the declared logical operation scope."""
    require_output(
        isinstance(capability, _OutputCapability)
        and capability.session.in_transaction()
        and capability.connection.is_in_transaction(),
        "transaction_required",
    )
    require_output(
        _operation_digest(capability.operation, capability.targets) == capability.operation_digest, "operation_changed"
    )


async def _capture(capability, targets_by_name, *, private=False):
    await _require_capability(capability)
    return await read_output_relations(capability.connection, capability.policy, targets_by_name)


async def read_output_relations(connection, policy, targets_by_name):
    """Read finite named model layouts through either native publisher or read-only access."""
    entries = await connection.fetch(OUTPUT_CATALOG_SQL, policy.schema, list(targets_by_name))
    require_output(
        len(entries) == len(targets_by_name) and {row["relname"] for row in entries} == set(targets_by_name),
        "storage_missing",
    )
    captured_by_name = {}
    for row in entries:
        target = targets_by_name[row["relname"]]
        require_output_shape(row, target)
        captured_by_name[row["relname"]] = _native_receipt(policy, row, target)
    return captured_by_name


def _native_receipt(policy, row, target):
    """Capture actual logical layout; native permissions govern access independently."""
    layout = portable_catalog_payload(row)
    descriptor = logical_output_descriptor(row["descriptor"])
    return NativeOutputRelation(
        schema=policy.schema,
        target=target,
        relation=row["relname"],
        **{key: tuple(encoded(atom) for atom in layout[key]) for key in ("columns", "constraints", "indexes")},
        table=encoded(layout["topology"]),
        descriptor=None if descriptor is None else encoded(descriptor),
    )


async def _issue_output_capability(directory, session, operation, targets, operation_digest):
    publisher = bound_publisher()
    require_output(directory._schema() == publisher.policy.schema, "schema_changed")
    return _OutputCapability(
        session,
        publisher.connection,
        publisher.policy,
        operation_digest,
        targets,
        operation.get("rollback_of"),
        encoded(operation),
    )


def _require_original_output_session(directory):
    """An already open transaction remains application-owned; PostgreSQL supplies its access."""
    binding = directory.db._transaction_binding()
    require_output(binding is None or binding.session.in_transaction(), "transaction_required")


@asynccontextmanager
async def output_snapshot_transaction(directory, *, operation, targets, isolation="repeatable read"):
    """Use the existing native publisher transaction for one admitted logical output operation."""
    operation_digest = _operation_digest(operation, targets)
    require_output(_OUTPUT.get() is None, "operation_already_bound")
    _require_original_output_session(directory)
    async with source_transaction(directory.db, isolation=isolation) as session:
        capability = await _issue_output_capability(directory, session, operation, targets, operation_digest)
        token = _OUTPUT.set(capability)
        try:
            await _capture(capability, {name: name for name in OUTPUT_ROOTS})
            yield capability
            await _require_capability(capability)
        finally:
            _OUTPUT.reset(token)


def _require_stage_descriptor(capability, successor, incumbent):
    descriptor = logical_output_descriptor(successor.descriptor)
    require_output(
        descriptor is not None
        and descriptor["operation_digest"] == capability.operation_digest
        and descriptor["target"] == successor.target
        and descriptor["phase"] == "sealed",
        "stage_unsealed",
    )
    if descriptor["incumbent_digest"] is not None:
        require_output(descriptor["incumbent_digest"] == _digest(incumbent.named_payload()), "incumbent_changed")


async def capture_prepared_output(capability, target, stage, *, expected_incumbent):
    """Capture a complete validated candidate against the same logical incumbent generation."""
    require_output(
        capability.reference is None and target in capability.targets and type(stage) is str and stage != target,
        "stage_invalid",
    )
    from db.plan_release_storage import storage_identifier

    storage_identifier(stage)
    incumbent = (await _capture(capability, {target: target}))[target]
    require_output(
        isinstance(expected_incumbent, NativeOutputRelation) and incumbent == expected_incumbent, "incumbent_changed"
    )
    successor = (await _capture(capability, {stage: target}, private=True))[stage]
    _require_stage_descriptor(capability, successor, incumbent)
    return PreparedOutputReceipt(2, capability.operation_digest, incumbent, successor)


def _require_output_set(capability, receipts, *, restore=False):
    require_output(
        isinstance(receipts, tuple)
        and len(receipts) == len(capability.targets)
        and all(isinstance(receipt, PreparedOutputReceipt) for receipt in receipts)
        and {receipt.incumbent.target for receipt in receipts} == set(capability.targets),
        "receipt_set_changed",
    )
    for receipt in receipts:
        require_output(
            receipt.protocol in (1, 2)
            and receipt.operation_digest == (capability.reference if restore else capability.operation_digest)
            and receipt.incumbent.schema == receipt.successor.schema == capability.policy.schema
            and receipt.incumbent.target == receipt.successor.target
            and receipt.incumbent.relation != receipt.successor.relation,
            "receipt_identity_changed",
        )


async def recheck_output_cut(capability, receipts):
    """Recheck operation scope, complete declared names and logical generation/layout before rotation."""
    await _require_capability(capability)
    require_output(capability.reference is None, "forward_operation_required")
    _require_output_set(capability, receipts)
    for receipt in receipts:
        _require_stage_descriptor(capability, receipt.successor, receipt.incumbent)
    serving = await _capture(capability, {receipt.incumbent.relation: receipt.incumbent.target for receipt in receipts})
    private = await _capture(capability, {receipt.successor.relation: receipt.successor.target for receipt in receipts})
    require_output(
        all(
            serving[receipt.incumbent.relation] == receipt.incumbent
            and private[receipt.successor.relation] == receipt.successor
            for receipt in receipts
        ),
        "logical_receipt_changed",
    )


async def publish_output_read_access(capability, prepared_receipt):
    """Grant configured native Readers SELECT on the installed immutable snapshot name."""
    from db.plan_release_storage import storage_identifier, storage_relation

    await _require_capability(capability)
    require_output(
        prepared_receipt.operation_digest == capability.operation_digest
        and prepared_receipt.successor.target in capability.targets,
        "receipt_identity_changed",
    )
    _require_stage_descriptor(capability, prepared_receipt.successor, prepared_receipt.incumbent)
    target = prepared_receipt.successor.target
    current = (await _capture(capability, {target: target}))[target]
    require_output(current == replace(prepared_receipt.successor, relation=target), "logical_receipt_changed")
    readers = ",".join(storage_identifier(role) for role in capability.policy.runtime_roles)
    if readers:
        await capability.connection.execute(
            f"GRANT SELECT ON TABLE {storage_relation(capability.policy.schema, target)} TO {readers}"
        )


async def capture_retained_output(capability, prepared_receipt, retained_name):
    """Read both explicit post-cut names and preserve their logical data/layout lineage."""
    await _require_capability(capability)
    require_output(prepared_receipt.operation_digest == capability.operation_digest, "receipt_identity_changed")
    target = prepared_receipt.incumbent.target
    from db.plan_release_storage import storage_identifier

    storage_identifier(retained_name)
    require_output(
        retained_name not in OUTPUT_ROOTS and retained_name != prepared_receipt.successor.relation,
        "retained_name_invalid",
    )
    actual = await _capture(capability, {target: target, retained_name: target})
    old, new = actual[retained_name], actual[target]
    require_output(
        old == replace(prepared_receipt.incumbent, relation=retained_name)
        and new == replace(prepared_receipt.successor, relation=target),
        "logical_receipt_changed",
    )
    return old, new


async def recheck_output_restore(capability, prepared_receipts, retained_names):
    """Recheck the recorded inverse names and logical generations before native rollback DDL."""
    await _require_capability(capability)
    require_output(
        capability.reference is not None
        and isinstance(retained_names, dict)
        and set(retained_names) == set(capability.targets),
        "restore_receipt_set_changed",
    )
    _require_output_set(capability, prepared_receipts, restore=True)
    names_by_relation = {}
    from db.plan_release_storage import storage_identifier

    for receipt in prepared_receipts:
        target_name = receipt.incumbent.target
        name = retained_names[target_name]
        storage_identifier(name)
        require_output(
            name == receipt.incumbent.relation
            and receipt.successor.relation == target_name
            and name not in OUTPUT_ROOTS,
            "restore_receipt_identity_changed",
        )
        names_by_relation[name] = target_name
        names_by_relation[target_name] = target_name
    require_output(len(names_by_relation) == 2 * len(capability.targets), "restore_receipt_identity_changed")
    actual = await _capture(capability, names_by_relation)
    require_output(
        all(
            actual[receipt.incumbent.relation] == receipt.incumbent
            and actual[receipt.successor.relation] == receipt.successor
            for receipt in prepared_receipts
        ),
        "logical_restore_receipt_changed",
    )


async def capture_restored_output(capability, stored_forward_prepared_receipt, failed_retained_name):
    """Check named inverse results while preserving both immutable generations."""
    await _require_capability(capability)
    receipt = stored_forward_prepared_receipt
    require_output(
        capability.reference is not None
        and receipt.operation_digest == capability.reference
        and receipt.incumbent.target in capability.targets,
        "restore_receipt_identity_changed",
    )
    from db.plan_release_storage import storage_identifier

    storage_identifier(failed_retained_name)
    require_output(failed_retained_name not in OUTPUT_ROOTS, "retained_name_invalid")
    target = receipt.incumbent.target
    actual = await _capture(capability, {target: target, failed_retained_name: target})
    failed, restored = actual[failed_retained_name], actual[target]
    require_output(
        failed == replace(receipt.successor, relation=failed_retained_name)
        and restored == replace(receipt.incumbent, relation=target),
        "logical_restore_receipt_changed",
    )
    return failed, restored
