# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Optional bounded native encoding after frozen scalar verification."""

from __future__ import annotations

import importlib
from contextlib import closing
from functools import lru_cache

from db.models.custom_import import CustomImportChildScalar, CustomImportRootScalar
from process.custom_import import materialization, publication
from process.custom_import.runner_codec import fields_by_collection, payload_values
from process.custom_import.runner_types import CandidateRunnerError

BATCH_ROWS = 256
MAX_FRAME_BYTES = 16_384
# Includes copied Python tuples/strings, Rust-owned inputs, reusable row JSON,
# the full Rust frame allocation and the simultaneously copied Python bytes.
RESERVE_BYTES = 16 * 1024 * 1024
VERIFY_BATCH_ROWS = 128
VERIFY_BATCH_REVISIONS = 16
VERIFY_PENDING_ROWS = 20
VERIFY_RESERVED_ROWS = 5 * VERIFY_BATCH_ROWS + 2 * VERIFY_PENDING_ROWS
# Two full frame buffers (Rust/Python), retained Python cells including one
# pending revision, Rust-owned comparison/wire strings and bounded container
# overhead. 32 KiB/cell covers both retained Python strings (including cached
# UTF-8), typed objects, bindings and tuples; 8 KiB/cell covers Rust-owned
# expected/actual values and frame-row metadata. The fixed MiB covers layouts,
# one frame body and sequential Decimal conversion scratch. Payload parsing
# stays in the existing physical-page envelope; no payload/model is buffered.
VERIFY_BUFFER_BYTES = (
    2 * VERIFY_BATCH_ROWS * MAX_FRAME_BYTES
    + (VERIFY_BATCH_ROWS + VERIFY_PENDING_ROWS) * 32_768
    + VERIFY_BATCH_ROWS * 8_192
    + 1024 * 1024
)
SCALAR_COLUMNS = frozenset(
    {
        "boolean_value",
        "date_value",
        "decimal_value",
        "field_collection_slot",
        "field_slot",
        "field_type",
        "integer_value",
        "projection_slot",
        "string_value",
        "timestamp_value",
        "value_state",
    }
)


@lru_cache(maxsize=1)
def native_encoder():
    """Use only a compatible capability; absence selects Python before work."""
    omitted = publication._MATERIALIZATION_IDENTITY_COLUMNS | publication._MATERIALIZATION_VOLATILE_COLUMNS
    for model, columns in (
        (CustomImportRootScalar, SCALAR_COLUMNS),
        (CustomImportChildScalar, SCALAR_COLUMNS | {"collection_slot"}),
    ):
        if set(model.__table__.columns.keys()) - omitted != columns:
            return None
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    encoder = getattr(module, "custom_import_scalar_frames_v1", None)
    return encoder if callable(encoder) else None


@lru_cache(maxsize=1)
def native_verifier():
    """Select only the exact all-column ABI inside the reserved typed envelope."""
    identities = {"dataset_id", "schema_revision_id", "root_record_id"}
    if VERIFY_BUFFER_BYTES > RESERVE_BYTES:
        return None
    for model, columns in (
        (CustomImportRootScalar, SCALAR_COLUMNS | identities | {"root_revision_id"}),
        (CustomImportChildScalar, SCALAR_COLUMNS | identities | {"child_revision_id", "collection_slot"}),
    ):
        if set(model.__table__.columns.keys()) != columns:
            return None
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    verifier = getattr(module, "custom_import_verified_scalar_frames_v1", None)
    return verifier if callable(verifier) else None


def verification_layouts(definition, collection_slots, *, child):
    """Freeze complete collection descriptors once, retaining empty hot layouts."""
    fields = fields_by_collection(definition)
    scoped_fields = (
        {slot: fields[name] for name, slot in collection_slots.items()} if child else {0: definition.root_fields}
    )
    contexts_by_slot = {
        slot: (
            tuple(values),
            tuple(
                sorted(
                    (field for field in values if field.projection_slot is not None), key=lambda field: field.field_slot
                )
            ),
        )
        for slot, values in scoped_fields.items()
    }
    layouts = [
        (
            slot,
            tuple((field.field_slot, field.projection_slot, field.value_type, field.nullable) for field in hot_fields),
        )
        for slot, (_fields, hot_fields) in contexts_by_slot.items()
    ]
    return layouts, contexts_by_slot


def _prepare_verification_revision(revision, root_key, contexts_by_slot, *, child):
    """Decode every field once, then retain only bounded normalized hot cells."""
    slot = revision.collection_slot if child else 0
    fields, hot_fields = contexts_by_slot[slot]
    values = payload_values(fields, revision.canonical_payload, label="frozen scalar payload")
    expected_cells = []
    for field in hot_fields:
        if field.field_id not in values:
            expected_cells.append(("missing", None))
        elif values[field.field_id] is None:
            expected_cells.append(("null", None))
        else:
            _kind, value = materialization._normalized_scalar_value(
                field.value_type, values[field.field_id], field.field_id
            )
            expected_cells.append(("value", value))
    identity = revision.child_revision_id if child else revision.root_revision_id
    keys = (bytes(root_key).hex(), bytes(revision.child_key_sha256).hex() if child else None)
    return ((revision.root_record_id, identity, slot), keys, tuple(expected_cells), [])


def _verification_row(scalar, *, child):
    """Retain every model column, without converting actual stored values."""
    for name, limit in (("string_value", 2048), ("field_type", 16), ("value_state", 8)):
        column_value = getattr(scalar, name)
        if column_value is not None and (type(column_value) is not str or len(column_value) > limit):
            raise CandidateRunnerError("typed scalar projection differs from the frozen payload")
    return (
        (
            scalar.dataset_id,
            scalar.schema_revision_id,
            scalar.root_record_id,
            scalar.child_revision_id if child else scalar.root_revision_id,
        ),
        (
            scalar.field_slot,
            scalar.field_collection_slot,
            scalar.projection_slot,
            scalar.collection_slot if child else None,
            scalar.field_type,
            scalar.value_state,
        ),
        (
            scalar.string_value,
            scalar.integer_value,
            scalar.decimal_value,
            scalar.boolean_value,
            scalar.date_value,
            scalar.timestamp_value,
        ),
    )


def verification_revisions(projection_records, contexts_by_slot, *, child):
    """Close groups only on revision transition or real upstream exhaustion."""
    current, previous_id, has_empty_row = None, None, False
    with closing(projection_records):
        for scalar, revision, root_key in projection_records:
            identity = revision.child_revision_id if child else revision.root_revision_id
            if identity != previous_id:
                if current is not None:
                    yield current
                current = _prepare_verification_revision(revision, root_key, contexts_by_slot, child=child)
                previous_id, has_empty_row = identity, False
            if scalar is None:
                if current[3] or has_empty_row:
                    raise CandidateRunnerError("typed scalar projection differs from the frozen payload")
                has_empty_row = True
            else:
                if has_empty_row or len(current[3]) == VERIFY_PENDING_ROWS:
                    raise CandidateRunnerError("typed scalar projection differs from the frozen payload")
                current[3].append(_verification_row(scalar, child=child))
            del scalar, revision, root_key
        if current is not None:
            yield current


def verified_material(verifier, revisions, owner, layouts, digests, *, child, check_budget):
    """Verify bounded complete groups before hashing; never retry rejected data."""
    batch, batch_cell_count, batch_row_count, scalar_count = [], 0, 0, 0

    def flush():
        """Hash a completely verified batch inside its live budget."""
        check_budget()
        try:
            frames = verifier(child, owner, layouts, batch)
        except ValueError, TypeError, OverflowError:
            raise CandidateRunnerError("native scalar verification failed") from None
        check_budget()
        if type(frames) is not bytes or len(frames) > batch_row_count * MAX_FRAME_BYTES:
            raise CandidateRunnerError("native scalar verification output exceeds its admitted frame bound")
        for digest in digests:
            digest.update(frames)
        batch.clear()

    try:
        with closing(revisions):
            for revision in revisions:
                expected_count, actual_count = len(revision[2]), len(revision[3])
                if batch and (
                    len(batch) == VERIFY_BATCH_REVISIONS
                    or batch_cell_count + expected_count > VERIFY_BATCH_ROWS
                    or batch_row_count + actual_count > VERIFY_BATCH_ROWS
                ):
                    flush()
                    batch_cell_count, batch_row_count = 0, 0
                batch.append(revision)
                batch_cell_count += expected_count
                batch_row_count += actual_count
                scalar_count += actual_count
                del revision
            if batch:
                flush()
    finally:
        batch.clear()
    return scalar_count


def _scalar_tuple(scalar, revision, root_key, *, child):
    return (
        bytes(root_key).hex(),
        bytes(revision.child_key_sha256).hex() if child else None,
        (
            scalar.field_slot,
            scalar.field_collection_slot,
            scalar.projection_slot,
            scalar.collection_slot if child else None,
            scalar.field_type,
            scalar.value_state,
        ),
        (
            scalar.string_value,
            scalar.integer_value,
            publication._json_value(scalar.decimal_value),
            scalar.boolean_value,
            publication._json_value(scalar.date_value),
            publication._json_value(scalar.timestamp_value),
        ),
    )


def _flush(encoder, prepared_rows, digests, child, check_budget):
    check_budget()
    try:
        frames = encoder(child, prepared_rows)
    except ValueError, TypeError, OverflowError:
        raise CandidateRunnerError("native scalar digest validation failed") from None
    check_budget()
    if type(frames) is not bytes or len(frames) > len(prepared_rows) * MAX_FRAME_BYTES:
        raise CandidateRunnerError("native scalar digest output exceeds its admitted frame bound")
    for digest in digests:
        digest.update(frames)
    prepared_rows.clear()


def scalar_material(encoder, projection_records, digests, *, child, check_budget):
    """Encode verified primitives once per bounded batch and close on failure."""
    prepared_rows, scalar_count = [], 0
    try:
        with closing(projection_records):
            for scalar, revision, root_key in projection_records:
                prepared_rows.append(_scalar_tuple(scalar, revision, root_key, child=child))
                scalar_count += 1
                del scalar, revision, root_key
                if len(prepared_rows) == BATCH_ROWS:
                    _flush(encoder, prepared_rows, digests, child, check_budget)
            if prepared_rows:
                _flush(encoder, prepared_rows, digests, child, check_budget)
    finally:
        prepared_rows.clear()
    return scalar_count
