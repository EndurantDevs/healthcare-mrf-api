# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete selected retained inputs under the caller's live native base pin.

No destination IDs, phase, verification or publication authority is produced.
Closed replay remains owner-only; native origin is checked before any descriptor
is returned and every reader must reach the exact complete file EOF.
"""

import hashlib
import os
import shutil
import stat
import struct
from contextlib import ExitStack, closing
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import msgpack

from process.custom_import import family_shard_base_values_sql as sql
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit, _read_object
from process.custom_import.family_block_materialization import _verify_group
from process.custom_import.family_block_records import record_from_wire, record_parts
from process.custom_import.family_shard_base import PinnedBase, _require_deadline
from process.custom_import.family_shard_facts import SourceFactLimits, _Replay, _sorted
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import FinalPlanDescriptor, PlanWork, _cpu, _Scratch, read_family_plan
from process.custom_import.family_shard_retained import restore_legacy_record
from process.custom_import.materialization import TypedScalar, _validate_typed_scalar
from process.custom_import.runner_codec import digest_text, root_key_contract_hash, root_key_document

_CONTRACT = "custom-import/retained-input/v1"
_FILE = "retained-input.mpk"
_LENGTH = struct.Struct("!Q")
_LANES = ("string_value", "integer_value", "decimal_value", "boolean_value", "date_value", "timestamp_value")
_SCOPE = ("dataset_id", "definition_revision_id", "schema_revision_id")


@dataclass(frozen=True)
class RetainedWork:
    root: object
    limits: SourceFactLimits
    register_cleanup: object
    check: object = None

    def __post_init__(self):
        PlanWork(self.root, self.limits, self.register_cleanup, self.check)


@dataclass(frozen=True)
class RetainedInputDescriptor:
    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    plan_sha256: bytes
    base_identity: tuple | None
    definition_canonical: str
    collection_slots: tuple
    current_definition_sha256: str
    origin_scope: tuple | None
    counts: tuple
    max_frame_bytes: int

    @property
    def counts_by_role(self):
        """Original input counts, never destination/publication acceptance."""
        return dict(self.counts)


def _scope(base, plan, work):
    if type(plan) is not FinalPlanDescriptor or type(work) is not RetainedWork:
        raise ValueError("retained inputs require completed typed plan and explicit work")
    if base is None:
        if plan.base_scope is not None or plan.counts_by_kind["retained"] or plan.request_scope[6:8] != (None, 0):
            raise FamilyBlockUnavailable("retained input requires its actual live base")
        return None
    if type(base) is not PinnedBase or not base.complete or base.scope != plan.base_scope:
        raise FamilyBlockUnavailable("retained input base header EOF/scope differs")
    request = base.request
    expected = (
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        request.fence,
    )
    if plan.request_scope[:6] != (*expected, producer_token_sha256(request)) or plan.request_scope[6:] != (
        request.expected_base_generation_id,
        request.expected_pointer_version,
        request.definition.refresh_mode,
        request.complete_scope,
    ):
        raise FamilyBlockUnavailable("retained input plan request scope differs")
    if plan.counts_by_kind["retained"] and (
        base.definition.schema_digest != request.definition.schema_digest
        or root_key_contract_hash(base.definition) != root_key_contract_hash(request.definition)
    ):
        raise FamilyBlockUnavailable("selected retained schema/root-key contract is incompatible")
    generation = base.generation
    return (
        base.scope,
        base.definition.digest,
        generation.execution_id,
        generation.producing_fence,
        bytes(generation.producing_token_sha256),
        bytes(base.seal.materialization_sha256),
    )


def _selected(work, plan):
    count = 0
    with closing(read_family_plan(plan, check=work.current)) as families:
        for family in families:
            if family.selection_kind != "retained":
                continue
            header = family.base_header
            if header is None or header.typed_key != family.typed_key:
                raise FamilyBlockUnavailable("retained plan omits original base family")
            count += 1
            yield header.family_revision_id, tuple(vars(header).values())
    if count != plan.counts_by_kind["retained"]:
        raise FamilyBlockUnavailable("retained plan complete selected census differs")


def _selection_file(work, plan):
    with closing(_sorted(work, _selected(work, plan), lambda entry: entry[0])) as selected:
        previous = None
        path = work.path()
        with path.open("xb") as target:
            for entry in selected:
                if previous is not None and entry[0] <= previous:
                    raise FamilyBlockUnavailable("retained plan repeats a base family")
                work.frame(target, entry, path)
                previous = entry[0]
        return path


def _batches(work, selected):
    family_batch, charged_bytes = [], 0
    with closing(work.rows(selected)) as families:
        for entry in families:
            work.current()
            charge = 1024 + max(4096, 8 * len(_pack_row(entry)))
            if charge > work.limits.memory_bytes:
                raise FamilyBlockResourceLimit("retained selected header exceeds its owner buffer budget")
            if family_batch and (len(family_batch) == 4096 or charged_bytes + charge > work.limits.memory_bytes):
                yield tuple(family_batch)
                family_batch, charged_bytes = [], 0
            family_batch.append(entry)
            charged_bytes += charge
        if family_batch:
            yield tuple(family_batch)


def _plain(native):
    return {
        name: bytes(value) if isinstance(value, bytearray | memoryview) else value for name, value in native.items()
    }


async def _stage(base, work, statement, ids):
    path = work.path()
    with path.open("xb") as output:
        async with sql.rows(base, statement, ids, work.limits) as natives:
            async for native in natives:
                work.current()
                _require_deadline(base._deadline)
                work.frame(output, _pack_row(_plain(native)), path)
    return path


class _Cursor:
    def __init__(self, work, path, key):
        self.rows = work.rows(path)
        self.key = key
        self.current = self._next()

    def _next(self):
        packed = next(self.rows, None)
        return None if packed is None else _unpack_row(packed)

    def take(self, key):
        """Consume exactly one ordered native group without collecting children."""
        if self.current is not None and self.key(self.current) < key:
            raise FamilyBlockUnavailable("retained native cursor has an unowned prefix")
        while self.current is not None and self.key(self.current) == key:
            current, self.current = self.current, self._next()
            yield current

    def finish(self):
        """A selected role stream must reach its complete EOF in both directions."""
        if self.current is not None:
            raise FamilyBlockUnavailable("retained native cursor has extra rows")

    def close(self):
        """Release the exact borrowed file iterator."""
        self.rows.close()


def _one(rows, label):
    with closing(rows):
        first = next(rows, None)
        if first is None or next(rows, None) is not None:
            raise FamilyBlockUnavailable("retained " + label + " census differs")
        return first


def _prefix(native, prefix):
    return {name[len(prefix) :]: value for name, value in native.items() if name.startswith(prefix)}


def _family(base, cells, native):
    family, member = _prefix(native, "family_"), _prefix(native, "member_")
    root_key, root, family_id, revision, entity, digest, children, generation, snapshot = cells
    if (generation, snapshot) != base.scope or (
        family["family_revision_id"],
        family["root_record_id"],
        family["root_revision_id"],
        family["entity_binding_id"],
        bytes(family["family_sha256"]),
        family["child_count"],
    ) != (family_id, root, revision, entity, digest, children):
        raise FamilyBlockUnavailable("retained original family/header identity differs")
    if tuple(member[name] for name in ("generation_id", *_SCOPE, "root_record_id", "family_revision_id")) != (
        base.generation.generation_id,
        base.generation.dataset_id,
        base.generation.definition_revision_id,
        base.generation.schema_revision_id,
        root,
        family_id,
    ):
        raise FamilyBlockUnavailable("retained original membership scope differs")
    if base.binding is None and tuple(
        family[name]
        for name in (
            "dataset_id",
            "schema_revision_id",
            "producing_execution_id",
            "producing_fence",
            "producing_token_sha256",
        )
    ) != (
        base.generation.dataset_id,
        base.generation.schema_revision_id,
        base.generation.execution_id,
        base.generation.producing_fence,
        bytes(base.generation.producing_token_sha256),
    ):
        raise FamilyBlockUnavailable("retained original family producer differs")
    return family, member


def _field_by_slot(base):
    return {field.field_slot: field for field in base.definition.fields}


def _decode_scalar_row(base, scalar, collection, root_id, revision_id, fields_by_slot):
    field = fields_by_slot.get(scalar["field_slot"])
    slot = 0 if collection is None else base.registry.child_collection_slots[collection]
    kind = "root" if collection is None else "child"
    if (
        field is None
        or field.collection != collection
        or field.projection_slot is None
        or (
            scalar["dataset_id"],
            scalar["schema_revision_id"],
            scalar["root_record_id"],
            scalar[kind + "_revision_id"],
            scalar["field_collection_slot"],
            scalar["field_type"],
            scalar["projection_slot"],
        )
        != (
            base.generation.dataset_id,
            base.generation.schema_revision_id,
            root_id,
            revision_id,
            slot,
            field.value_type,
            field.projection_slot,
        )
        or (collection is not None and scalar["collection_slot"] != slot)
    ):
        raise FamilyBlockUnavailable("retained scalar field/projection/collection/type tuple differs")
    typed = TypedScalar(scalar["field_type"], scalar["value_state"], **{name: scalar[name] for name in _LANES})
    _validate_typed_scalar(typed)
    return field.field_id, typed, (field.field_slot, field.projection_slot, slot, scalar["field_type"])


def _record_scalars(base, cursor, family_id, revision, collection, fields_by_slot):
    kind = "root" if collection is None else "child"
    revision_id = revision[kind + "_revision_id"]
    scalars_by_name, declarations = {}, []
    with closing(cursor.take((family_id, revision_id))) as rows:
        for native in rows:
            name, typed, declaration = _decode_scalar_row(
                base, _prefix(native, "scalar_"), collection, revision["root_record_id"], revision_id, fields_by_slot
            )
            if name in scalars_by_name:
                raise FamilyBlockUnavailable("retained scalar field repeats")
            scalars_by_name[name] = typed
            declarations.append(declaration)
    return scalars_by_name, tuple(declarations)


def _record_scope(base, family, revision, collection, edge=None):
    if tuple(revision[name] for name in _SCOPE) != tuple(getattr(base.generation, name) for name in _SCOPE) or (
        revision["root_record_id"] != family["root_record_id"]
    ):
        raise FamilyBlockUnavailable("retained original revision scope differs")
    if collection is not None:
        slot = base.registry.child_collection_slots[collection]
        if (
            edge["dataset_id"],
            edge["schema_revision_id"],
            edge["root_record_id"],
            edge["collection_slot"],
            edge["child_revision_id"],
            revision["collection_slot"],
        ) != (
            base.generation.dataset_id,
            base.generation.schema_revision_id,
            family["root_record_id"],
            slot,
            revision["child_revision_id"],
            slot,
        ):
            raise FamilyBlockUnavailable("retained original edge/revision bijection differs")


def _record_order(definition, collection, record, typed_key):
    wire, parts = record_parts(definition, collection, record, retained=True)
    if collection is None:
        parent = parts[1], digest_text("root-key", parts[1])
    elif record.sparse is not None:
        parent = record.sparse.parent_key_text, record.sparse.parent_key_sha256
    else:
        relation = definition.collections_by_name[collection]
        parent_text = root_key_document(
            definition, {part.root_field: record.values_by_field[part.child_field] for part in relation.parent_key}
        )
        parent = parent_text, digest_text("root-key", parent_text)
    if parent != typed_key:
        raise FamilyBlockUnavailable("retained record original root/parent key differs")
    return (0, "", b"", "", "") if collection is None else (1, collection, *parts), msgpack.packb(
        wire, use_bin_type=True
    )


def _append_record(work, output_by_role, base, original, collection, original_record, lineage_evidence, fields_by_name):
    family, typed_key = original
    work.current()
    if collection is None and original_record.revision_id != family["root_revision_id"]:
        raise FamilyBlockUnavailable("retained root revision differs")
    declarations = []
    for name, scalar in original_record.scalars.items():
        field = fields_by_name[name]
        if field.collection != collection or field.projection_slot is None or scalar.field_type != field.value_type:
            raise FamilyBlockUnavailable("retained block scalar declaration tuple differs")
        _validate_typed_scalar(scalar)
        declarations.append(
            (
                field.field_slot,
                field.projection_slot,
                0 if collection is None else base.registry.child_collection_slots[collection],
                scalar.field_type,
            )
        )
    order, wire = _record_order(base.definition, collection, original_record, typed_key)
    slot = 0 if collection is None else base.registry.child_collection_slots[collection]
    family_id = family["family_revision_id"]
    work.frame(
        output_by_role["record"][1],
        (family_id, order, slot, wire, _pack_row((lineage_evidence[0], lineage_evidence[1] or tuple(declarations)))),
        output_by_role["record"][0],
    )
    work.frame(output_by_role["reference"][1], (original_record.pack_id, slot), output_by_role["reference"][0])
    work.frame(
        output_by_role["identity"][1],
        (0 if collection is None else 1, original_record.revision_id, family_id),
        output_by_role["identity"][0],
    )
    return len(declarations)


def _legacy_records(work, output_by_role, base, cells, family, cursor_by_role, schema):
    family_id = family["family_revision_id"]
    native_root = _one(cursor_by_role["root"].take(family_id), "root")
    root = _prefix(native_root, "record_")
    _record_scope(base, family, root, None)
    scalars, declarations = _record_scalars(base, cursor_by_role["root_scalar"], family_id, root, None, schema[0])
    original_record = restore_legacy_record(base.definition, None, root, key=cells[0], scalars=scalars)
    _append_record(
        work, output_by_role, base, (family, cells[0]), None, original_record, (_lineage(root), declarations), schema[1]
    )
    count = 0
    names_by_slot = {slot: name for name, slot in base.registry.child_collection_slots.items()}
    for native in cursor_by_role["child"].take(family_id):
        revision, edge = _prefix(native, "record_"), _prefix(native, "edge_")
        collection = names_by_slot.get(edge["collection_slot"])
        if collection is None:
            raise FamilyBlockUnavailable("retained child collection declaration is absent")
        _record_scope(base, family, revision, collection, edge)
        if (revision["canonical_parent_key"], bytes(revision["parent_key_sha256"])) != cells[0]:
            raise FamilyBlockUnavailable("retained child parent differs from original root")
        scalars, declarations = _record_scalars(
            base, cursor_by_role["child_scalar"], family_id, revision, collection, schema[0]
        )
        original_record = restore_legacy_record(
            base.definition,
            collection,
            revision,
            key=(revision["canonical_child_key"], bytes(revision["child_key_sha256"])),
            scalars=scalars,
        )
        _append_record(
            work,
            output_by_role,
            base,
            (family, cells[0]),
            collection,
            original_record,
            (_lineage(revision), declarations),
            schema[1],
        )
        count += 1
    if count != family["child_count"]:
        raise FamilyBlockUnavailable("retained complete original child census differs")


def _lineage(revision):
    excluded_names = {
        "canonical_payload",
        "payload_sha256",
        "canonical_parent_key",
        "parent_key_sha256",
        "canonical_child_key",
        "child_key_sha256",
    }
    return {name: value for name, value in revision.items() if name not in excluded_names}


def _verified_records(verified):
    source = verified._file
    source.seek(0)
    _read_object(source, verified.limits)
    yield (
        None,
        record_from_wire(
            verified.definition, None, _read_object(source, verified.limits), retained=verified.manifest.retained
        )[0],
    )
    for collection in sorted(verified.definition.collections_by_name):
        if _read_object(source, verified.limits) != ["collection", collection]:
            raise FamilyBlockUnavailable("verified retained collection frame differs")
        while True:
            cell = _read_object(source, verified.limits)
            if cell[0] == "end":
                break
            yield (
                collection,
                record_from_wire(verified.definition, collection, cell[1], retained=verified.manifest.retained)[0],
            )


def _block_records(work, output_by_role, base, cells, family, cursor_by_role, schema):
    with _Replay(work) as fragments, _Replay(work) as memberships:
        for native in cursor_by_role["fragment"].take(family["family_revision_id"]):
            fragments.append(tuple(native.values()))
        for native in cursor_by_role["revision_run"].take(family["family_revision_id"]):
            memberships.append(tuple(native.values()))
        sections = SimpleNamespace(binding=base.binding, work=work)
        with _verify_group(sections, family, fragments, memberships, work.current) as verified:
            work.charge(verified.manifest.raw_bytes)
            try:
                for collection, original_record in _verified_records(verified):
                    lineage_by_name = dict(
                        zip(_SCOPE, (getattr(base.generation, name) for name in _SCOPE), strict=True)
                    )
                    lineage_by_name.update(
                        root_record_id=family["root_record_id"],
                        pack_id=original_record.pack_id,
                        source_ordinal=original_record.source_ordinal,
                        original_revision_id=original_record.revision_id,
                    )
                    _append_record(
                        work,
                        output_by_role,
                        base,
                        (family, cells[0]),
                        collection,
                        original_record,
                        (lineage_by_name, ()),
                        schema[1],
                    )
            finally:
                work.used -= verified.manifest.raw_bytes


def _cursor_key(role):
    if role.endswith("_scalar"):
        return lambda native: (native["family_revision_id"], native["scalar_" + role[:-7] + "_revision_id"])
    if role == "family":
        return lambda native: native["family_family_revision_id"]
    return lambda native: native["family_revision_id"]


def _process_batch(work, output_by_role, base, batch, path_by_role):
    schema = _field_by_slot(base), base.definition.fields_by_id
    with ExitStack() as stack:
        cursor_by_role = {
            role: stack.enter_context(closing(_Cursor(work, path, _cursor_key(role))))
            for role, path in path_by_role.items()
        }
        for identity, cells in batch:
            work.current()
            family, member = _family(base, cells, _one(cursor_by_role["family"].take(identity), "family"))
            work.frame(
                output_by_role["family"][1], (identity, cells, _pack_row((family, member))), output_by_role["family"][0]
            )
            if base.binding is None:
                _legacy_records(work, output_by_role, base, cells, family, cursor_by_role, schema)
            else:
                _block_records(work, output_by_role, base, cells, family, cursor_by_role, schema)
        for cursor in cursor_by_role.values():
            cursor.finish()
    for path in path_by_role.values():
        work.remove(path)


async def _read_batch(base, work, output_by_role, batch, native, origin):
    ids = [entry[0] for entry in batch]
    statement_by_role = {"family": sql.family_statement(base, native)}
    statement_by_role |= sql.legacy_statements(base, native) if base.binding is None else sql.block_statements(native)
    path_by_role = {role: await _stage(base, work, statement, ids) for role, statement in statement_by_role.items()}
    await _cpu(work, _process_batch, work, output_by_role, base, batch, path_by_role)
    if origin is not None:
        statement = sql.origin_statement(origin, check=True)
        violation = (
            await base.session.execute(statement, {"retained_ids": ids, "p_9": base.generation.base_generation_id})
        ).first()
        if violation is not None:
            raise FamilyBlockUnavailable("retained original legacy-copy origin join differs")
        async with sql.rows(base, sql.origin_statement(origin), ids, work.limits) as natives:
            async for native_row in natives:
                work.frame(output_by_role["origin"][1], _pack_row(_plain(native_row)), output_by_role["origin"][0])
    await base.check()


def _unique_references(work, path):
    previous = None
    with closing(_sorted(work, work.rows(path), lambda entry: entry)) as references:
        for entry in references:
            if entry != previous:
                yield entry
                previous = entry


def _reference_batches(work, path):
    batch, slots_by_pack = [], {}
    with closing(_unique_references(work, path)) as references:
        for identity, slot in references:
            if identity not in slots_by_pack and len(batch) >= min(4096, max(1, work.limits.memory_bytes // 4096)):
                yield tuple(batch), slots_by_pack
                batch, slots_by_pack = [], {}
            if identity not in slots_by_pack:
                batch.append(identity)
                slots_by_pack[identity] = []
            if slots_by_pack[identity] and slots_by_pack[identity] != [slot]:
                raise FamilyBlockUnavailable("retained original pack crosses declared collections")
            slots_by_pack[identity].append(slot)
        if batch:
            yield tuple(batch), slots_by_pack


def _pack_scope(base, native, expected_slots):
    pack = _prefix(native, "pack_")
    owner_values = tuple(pack[name] for name in _SCOPE)
    expected_owner_values = tuple(getattr(base.generation, name) for name in _SCOPE)
    if owner_values != expected_owner_values or (
        pack["execution_id"],
        pack["producing_fence"],
        pack["producing_token_sha256"],
    ) != (
        base.generation.execution_id,
        base.generation.producing_fence,
        bytes(base.generation.producing_token_sha256),
    ):
        raise FamilyBlockUnavailable("retained original pack/family producer differs")
    if tuple(native["execution_" + name] for name in ("execution_id", *_SCOPE, "capture_bundle_id")) != (
        pack["execution_id"],
        *owner_values,
        pack["capture_bundle_id"],
    ):
        raise FamilyBlockUnavailable("retained pack execution source binding differs")
    if tuple(native["capture_" + name] for name in ("capture_bundle_id", *_SCOPE, "stream_slot", "capture_state")) != (
        pack["capture_bundle_id"],
        *owner_values,
        pack["stream_slot"],
        "sealed",
    ):
        raise FamilyBlockUnavailable("retained pack capture is not its original sealed source")
    if tuple(native["stream_" + name] for name in (*_SCOPE, "stream_slot")) != (*owner_values, pack["stream_slot"]):
        raise FamilyBlockUnavailable("retained pack source-stream declaration differs")
    slot = 0 if native["stream_record_kind"] == "root" else native["stream_collection_slot"]
    if native["stream_record_kind"] not in ("root", "child") or expected_slots != [slot]:
        raise FamilyBlockUnavailable("retained pack record-kind/collection differs")
    return pack


async def _read_packs(base, work, output_by_role, native):
    for ids, expected_slots in _reference_batches(work, output_by_role["reference"][0]):
        observed = 0
        async with sql.rows(base, sql.pack_statement(native), ids, work.limits) as natives:
            async for row in natives:
                work.current()
                if row["pack_pack_id"] != ids[observed]:
                    raise FamilyBlockUnavailable("retained original pack bijection differs")
                _pack_scope(base, row, expected_slots[row["pack_pack_id"]])
                work.frame(output_by_role["pack"][1], _pack_row(_plain(row)), output_by_role["pack"][0])
                observed += 1
        if observed != len(ids):
            raise FamilyBlockUnavailable("retained original referenced pack is missing")


def _identity_guard(work, path):
    previous = None
    with closing(_sorted(work, work.rows(path), lambda entry: entry)) as identities:
        for entry in identities:
            if previous is not None and entry[:2] == previous[:2]:
                raise FamilyBlockUnavailable("retained original revision identity repeats")
            previous = entry


def _finish(work, path_by_role, plan, identity, original, current, origin_scope, collection_slots):
    _identity_guard(work, path_by_role["identity"])
    output = work.path()
    counts_by_role = dict(family=0, root=0, child=0, scalar=0, pack=0, origin=0)
    with output.open("xb") as output_file:
        work.frame(
            output_file,
            ("header", _CONTRACT, plan.sha256, identity, original, collection_slots, current, origin_scope),
            output,
        )
        for cells in work.rows(path_by_role["family"]):
            work.frame(output_file, ("family", cells), output)
            counts_by_role["family"] += 1
        with closing(_sorted(work, work.rows(path_by_role["record"]), lambda row: (row[0], row[1]))) as ordered_records:
            for family_id, _order, slot, wire, lineage_by_name in ordered_records:
                work.frame(output_file, ("record", family_id, slot, wire, lineage_by_name), output)
                counts_by_role["root" if slot == 0 else "child"] += 1
                counts_by_role["scalar"] += len(msgpack.unpackb(wire, raw=False)[4])
        for role in ("pack", "origin"):
            for cells in work.rows(path_by_role[role]):
                work.frame(output_file, (role, cells), output)
                counts_by_role[role] += 1
        if (
            counts_by_role["family"] != plan.counts_by_kind["retained"]
            or counts_by_role["root"] != counts_by_role["family"]
        ):
            raise FamilyBlockUnavailable("retained complete selected family/root census differs")
        counts = tuple(counts_by_role.items())
        work.frame(output_file, ("complete", counts), output)
    for temporary in tuple(work.paths):
        if temporary != output:
            work.remove(temporary)
    ready, digest = _seal(work, output)
    info = ready.stat()
    return RetainedInputDescriptor(
        str(work.root),
        work.identity.st_dev,
        work.identity.st_ino,
        info.st_dev,
        info.st_ino,
        info.st_size,
        digest,
        plan.sha256,
        identity,
        original,
        collection_slots,
        current,
        origin_scope,
        counts,
        work.limits.max_record_bytes,
    )


def _seal(work, path):
    os.chmod(path, 0o400)
    with path.open("rb") as source:
        os.fsync(source.fileno())
        digest = hashlib.sha256()
        while chunk := source.read(65536):
            work.current()
            digest.update(chunk)
    ready = work.root / _FILE
    os.link(path, ready, follow_symlinks=False)
    path.unlink()
    work.paths[ready] = work.paths.pop(path)
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return ready, digest.digest()


async def build_retained_input(base, plan, *, work):
    """Read only selected complete original families under the live native context.

    Keep that context alive through later destination allocation/copy and fresh
    owner validation. Completed output is replayable evidence, not a pin grant.
    """
    identity = _scope(base, plan, work)
    scratch = _Scratch(PlanWork(work.root, work.limits, work.register_cleanup, work.check))
    try:
        selected = await _cpu(scratch, _selection_file, scratch, plan)
        origin = None if base is None else await sql.origin_context(base)
        native = None if base is None else sql.tables(base)
        with ExitStack() as stack:
            output_by_role = {
                role: (path := scratch.path(), stack.enter_context(path.open("xb")))
                for role in ("family", "record", "reference", "identity", "pack", "origin")
            }
            if base is not None:
                await base.check()
                for batch in _batches(scratch, selected):
                    await _read_batch(base, scratch, output_by_role, batch, native, origin)
                output_by_role["reference"][1].flush()
                await _read_packs(base, scratch, output_by_role, native)
                await base.check()
        original = "" if base is None else base.definition.canonical
        current = "" if base is None else base.request.definition.digest
        scope = None if origin is None else origin.ancestor_scope
        descriptor = await _cpu(
            scratch,
            _finish,
            scratch,
            {role: output_entry[0] for role, output_entry in output_by_role.items()},
            plan,
            identity,
            original,
            current,
            scope,
            () if base is None else tuple(sorted(base.registry.child_collection_slots.items())),
        )
        if base is not None:
            await base.check()
        return descriptor
    except BaseException:
        scratch.close()
        raise


def _owned(descriptor):
    if type(descriptor) is not RetainedInputDescriptor:
        raise ValueError("retained input descriptor differs")
    root, path = Path(descriptor.root), Path(descriptor.root) / _FILE
    directory, observed = root.lstat(), path.lstat()
    if not stat.S_ISDIR(directory.st_mode) or (directory.st_dev, directory.st_ino) != (
        descriptor.device,
        descriptor.inode,
    ):
        raise FamilyBlockUnavailable("retained input root custody differs")
    if (
        not stat.S_ISREG(observed.st_mode)
        or (observed.st_dev, observed.st_ino, observed.st_size)
        != (descriptor.file_device, descriptor.file_inode, descriptor.file_bytes)
        or {entry.name for entry in root.iterdir()} != {_FILE}
    ):
        raise FamilyBlockUnavailable("retained input file custody differs")
    return path


def _entries(descriptor, check):
    path = _owned(descriptor)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        observed = os.fstat(source.fileno())
        if (observed.st_dev, observed.st_ino, observed.st_size) != (
            descriptor.file_device,
            descriptor.file_inode,
            descriptor.file_bytes,
        ):
            raise FamilyBlockUnavailable("retained input open custody differs")
        digest = hashlib.sha256()
        while prefix := source.read(8):
            if check is not None:
                check()
            if len(prefix) != 8 or not 0 < (length := _LENGTH.unpack(prefix)[0]) <= descriptor.max_frame_bytes:
                raise FamilyBlockUnavailable("retained input frame budget/length differs")
            packed = source.read(length)
            if len(packed) != length:
                raise FamilyBlockUnavailable("retained input frame is truncated")
            digest.update(prefix + packed)
            yield _unpack_row(packed)
        if digest.digest() != descriptor.sha256:
            raise FamilyBlockUnavailable("retained input complete EOF bytes differ")


def _has_complete_frame(entry, descriptor, counts_by_role):
    if entry[0] == "complete":
        if entry[1] != descriptor.counts or dict(entry[1]) != counts_by_role:
            raise FamilyBlockUnavailable("retained input complete census differs")
        return True
    if entry[0] == "record":
        counts_by_role["root" if entry[2] == 0 else "child"] += 1
        counts_by_role["scalar"] += len(msgpack.unpackb(entry[3], raw=False)[4])
    elif entry[0] in ("family", "pack", "origin"):
        counts_by_role[entry[0]] += 1
    else:
        raise FamilyBlockUnavailable("retained input frame kind differs")
    return False


def read_retained_input(descriptor, *, check=None):
    """Owner-only typed frames; no prefix authorizes COPY commit, proof or seal."""
    expected_header = (
        "header",
        _CONTRACT,
        descriptor.plan_sha256,
        descriptor.base_identity,
        descriptor.definition_canonical,
        descriptor.collection_slots,
        descriptor.current_definition_sha256,
        descriptor.origin_scope,
    )
    counts_by_role = dict(family=0, root=0, child=0, scalar=0, pack=0, origin=0)
    is_complete = False
    with closing(_entries(descriptor, check)) as entries:
        if next(entries, None) != expected_header:
            raise FamilyBlockUnavailable("retained input header scope differs")
        for entry in entries:
            if is_complete:
                raise FamilyBlockUnavailable("retained input has trailing frames")
            is_complete = _has_complete_frame(entry, descriptor, counts_by_role)
            yield entry
    if not is_complete:
        raise FamilyBlockUnavailable("retained input complete EOF is missing")


@dataclass(frozen=True)
class RetainedOriginalRecord:
    """Original proven identity, independent values and lineage; no new IDs."""

    family_revision_id: int
    collection_slot: int
    record: object
    lineage: object
    scalar_declarations: tuple


def read_retained_records(descriptor, *, check=None):
    """Replay original typed records while requiring every role's complete EOF."""
    definition = (
        None
        if not descriptor.definition_canonical
        else CustomImportDefinition.from_json(descriptor.definition_canonical)
    )
    names_by_slot = {0: None} | {slot: name for name, slot in descriptor.collection_slots}
    with closing(read_retained_input(descriptor, check=check)) as entries:
        for entry in entries:
            if entry[0] != "record":
                continue
            if definition is None or entry[2] not in names_by_slot:
                raise FamilyBlockUnavailable("retained original record collection is unresolved")
            wire = msgpack.unpackb(entry[3], raw=False, strict_map_key=True)
            record = record_from_wire(definition, names_by_slot[entry[2]], wire, retained=True)[0]
            lineage, declarations = _unpack_row(entry[4])
            yield RetainedOriginalRecord(entry[1], entry[2], record, lineage, declarations)


def cleanup_retained_input(descriptor):
    """Delete only this exact completed task-owned root/file census."""
    _owned(descriptor)
    shutil.rmtree(descriptor.root)
