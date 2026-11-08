# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original canonical material over fully verified family-block records.

Reconstructed sections are provisional data, never structural acceptance. The
owner must independently complete the source/admission/retained/context joins
and recheck immutable closure before recording the existing automated proof.
"""

import hashlib
import io
import itertools
import threading
import time
from contextlib import AsyncExitStack, aclosing, asynccontextmanager, closing, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import and_, select, text
from sqlalchemy.orm import aliased, undefer

from db.models import custom_import as models
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from process.custom_import import family_shard_semantic_audit as source_identity
from process.custom_import import publication, runner_codec
from process.custom_import.definition import CustomImportDefinition, DefinitionError
from process.custom_import.family_block_binding import (
    FamilyBlockBinding,
    FamilyBlockUnavailable,
    resolve_family_block_binding,
)
from process.custom_import.family_block_codec import FamilyBlockLimits, VerifiedFamily
from process.custom_import.family_block_directory import RevisionMembership
from process.custom_import.family_block_read import present_scalar
from process.custom_import.family_block_records import FamilyBlockError, FamilyRecord
from process.custom_import.family_block_storage_codec import verify_stored_family
from process.custom_import.family_shard_facts import SourceFactLimits, _pack, _Replay, _sorted, _Work
from process.custom_import.family_shard_plan import _cpu
from process.custom_import.query_projection_binding import QueryProjectionUnavailable
from process.custom_import.runner_types import CancellationRequested

_WORK_KEY = "custom_import_family_block_materialization_work"
_COUNTS = (
    "root_count",
    "family_count",
    "generation_family_count",
    "family_child_count",
    "winner_count",
    "profile_count",
    "root_scalar_count",
    "child_scalar_count",
)
_SECTIONS = ("family", "child", "root_scalar", "child_scalar", "winner")
_ROOT_COLUMNS = ("root_record_id", "dataset_id", "key_contract_sha256", "canonical_logical_key", "logical_key_sha256")
_ENTITY_COLUMNS = ("entity_binding_id", "dataset_id", "adapter_id", "canonical_value", "value_sha256")
_MEMBER_COLUMNS = (
    "generation_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "root_record_id",
    "family_revision_id",
)
_FAMILY_COLUMNS = (
    "family_revision_id",
    "root_record_id",
    "root_revision_id",
    "entity_binding_id",
    "family_sha256",
    "storage_sha256",
    "retained",
    "child_count",
    "fragment_count",
    "raw_bytes",
    "stored_bytes",
    "membership_count",
)
_PRODUCER_COLUMNS = (
    "generation_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "execution_id",
    "capture_bundle_id",
    "producing_fence",
    "producing_token_sha256",
)


@dataclass(frozen=True)
class MaterializationWork:
    root: Path
    limits: SourceFactLimits
    register_cleanup: object
    transaction: object
    check: object | None = None


class BlockMaterializationModels(dict):
    """Common native models plus the exact binding already pinned for finality."""

    def __init__(self, binding, closed_at):
        super().__init__(
            {
                model: aliased(model, binding.tables_by_role[role], adapt_on_names=True)
                for model, role in (
                    (models.CustomImportRootRecord, "root_record"),
                    (models.CustomImportEntityBinding, "entity_binding"),
                    (models.CustomImportPack, "pack"),
                    (models.CustomImportRejection, "rejection"),
                    (models.CustomImportGenerationFamily, "generation_family"),
                    (models.CustomImportWinner, "winner"),
                )
            }
        )
        self.family_block_binding, self.closed_at = binding, closed_at


async def _registered_slots(session, family_id):
    connection = await session.connection()
    declared = models.CustomImportGeneration.__table__.schema
    translated = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = translated.get(declared, declared)
    if not schema:
        raise publication.PublicationConflict("block finality requires its explicit control schema")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    await publication._prepare_bounded_materialization_statement(session, reserve_for_authority=True)
    return (
        await session.execute(
            text(f"SELECT * FROM {quoted}.resolve_custom_import_snapshot_relations(CAST(:family_id AS bigint))"),
            {"family_id": family_id},
        )
    ).all()


async def resolve_models(session, generation, family_id):
    """Resolve only after the original protected generation/finality resolver."""
    registration = await _registered_slots(session, family_id)
    if len(registration) == 15 and {slot_record[0] for slot_record in registration} == set(range(1, 16)):
        return None
    definition_row = await session.get(models.CustomImportDefinitionRevision, generation.definition_revision_id)
    if definition_row is None or definition_row.dataset_id != generation.dataset_id:
        raise publication.PublicationConflict("block finality definition identity is missing")
    try:
        definition = CustomImportDefinition.from_json(definition_row.canonical_definition)
    except DefinitionError as error:
        raise publication.PublicationConflict(str(error)) from error
    collections = (
        await session.execute(
            select(
                models.CustomImportChildCollection.collection_name, models.CustomImportChildCollection.collection_slot
            ).where(
                models.CustomImportChildCollection.dataset_id == generation.dataset_id,
                models.CustomImportChildCollection.schema_revision_id == generation.schema_revision_id,
            )
        )
    ).all()
    try:
        binding = await resolve_family_block_binding(
            session,
            family_id,
            definition,
            dict(collections),
            prepare=lambda current: publication._prepare_bounded_materialization_statement(
                current, reserve_for_authority=True
            ),
        )
    except FamilyBlockUnavailable as error:
        raise publication.PublicationConflict(str(error)) from error
    if binding is None:
        raise publication.PublicationConflict("block finality role binding changed")
    descriptor = await session.get(CustomImportFamilyBlockLayout, family_id, populate_existing=True)
    if descriptor is None or descriptor.phase != "logged" or descriptor.closed_at is None:
        raise publication.PublicationConflict("block finality has no durable closed candidate")
    return BlockMaterializationModels(binding, descriptor.closed_at)


async def automated_proof(session, generation):
    """Require the real immutable automated proof, never a page-count placeholder."""
    from process.custom_import.build_output import _proof_matches
    from process.custom_import.runner_types import CandidateRunnerError

    build, proof = models.CustomImportBuildAttempt, models.CustomImportBuildVerification
    result = (
        await session.execute(
            select(build, proof)
            .join(proof, proof.build_id == build.build_id)
            .where(proof.generation_id == generation.generation_id)
            .options(undefer(proof.manual_acceptance))
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if result is None:
        raise publication.PublicationConflict("block finality requires complete automated owner validation")
    try:
        _proof_matches(result[0], generation, result[1])
    except CandidateRunnerError as error:
        raise publication.PublicationConflict(str(error)) from error
    if result[1].verification_contract != "custom-import/build-structure/v1" or result[1].manual_acceptance is not None:
        raise publication.PublicationConflict("block finality requires its automated structural proof")
    return result[1]


@contextmanager
def materialization_work(session, root, limits, register_cleanup, *, check=None):
    """Attach caller-accounted scratch to exactly one existing owner transaction."""
    transaction = session.get_transaction()
    if transaction is None or not transaction.is_active or type(limits) is not SourceFactLimits:
        raise publication.PublicationConflict("block materialization requires its bounded owner transaction")
    previous = session.info.get(_WORK_KEY)
    plan = MaterializationWork(Path(root), limits, register_cleanup, transaction, check)
    session.info[_WORK_KEY] = plan
    try:
        yield plan
    finally:
        if previous is None:
            session.info.pop(_WORK_KEY, None)
        else:
            session.info[_WORK_KEY] = previous


def _work_plan(session):
    plan = session.info.get(_WORK_KEY)
    if (
        type(plan) is not MaterializationWork
        or plan.transaction is not session.get_transaction()
        or not plan.transaction.is_active
    ):
        raise publication.PublicationConflict("block materialization has no current accounted work context")
    return plan


def _scope(generation):
    return {name: getattr(generation, name) for name in ("dataset_id", "definition_revision_id", "schema_revision_id")}


def _canonical_parts(definition, collection, record):
    if type(record) is not FamilyRecord:
        raise publication.PublicationConflict("block materialization record type differs")
    if record.sparse is not None:
        return (
            record.sparse.payload_text,
            record.sparse.payload_sha256,
            record.sparse.key_text,
            record.sparse.key_sha256,
        )
    fields = definition.root_fields if collection is None else runner_codec.fields_by_collection(definition)[collection]
    payload = runner_codec.record_payload(fields, record.values_by_field)
    key = (
        runner_codec.root_key_document(definition, record.values_by_field)
        if collection is None
        else runner_codec.child_key_document(definition, collection, record.values_by_field)
    )
    kind = "root" if collection is None else "child"
    return (
        payload,
        runner_codec.digest_text(kind + "-payload", payload),
        key,
        runner_codec.digest_text(kind + "-key", key),
    )


def logical_root(binding, generation, root_identity, record):
    """Reconstruct the original root row, including exact retained lexical cells."""
    payload, payload_hash, key, key_hash = _canonical_parts(binding.layout.definition, None, record)
    if (
        root_identity.canonical_logical_key != key
        or bytes(root_identity.logical_key_sha256) != key_hash
        or bytes(root_identity.key_contract_sha256) != runner_codec.root_key_contract_hash(binding.layout.definition)
    ):
        raise publication.PublicationConflict("block root differs from its original dictionary identity")
    return models.CustomImportRootRevision(
        **_scope(generation),
        root_record_id=root_identity.root_record_id,
        root_revision_id=record.revision_id,
        pack_id=record.pack_id,
        source_ordinal=record.source_ordinal,
        canonical_payload=payload,
        payload_sha256=payload_hash,
    )


def logical_child(binding, generation, root_identity, collection, record):
    """Regenerate only canonical hashes; retained sparse text/hashes remain exact."""
    payload, payload_hash, key, key_hash = _canonical_parts(binding.layout.definition, collection, record)
    if record.sparse is not None:
        parent, parent_hash = record.sparse.parent_key_text, record.sparse.parent_key_sha256
    else:
        relation = binding.layout.definition.collections_by_name[collection]
        values_by_field = {part.root_field: record.values_by_field[part.child_field] for part in relation.parent_key}
        parent = runner_codec.root_key_document(binding.layout.definition, values_by_field)
        parent_hash = runner_codec.digest_text("root-key", parent)
    if parent != root_identity.canonical_logical_key or parent_hash != bytes(root_identity.logical_key_sha256):
        raise publication.PublicationConflict("child parent identity does not match its selected root record")
    return models.CustomImportChildRevision(
        **_scope(generation),
        root_record_id=root_identity.root_record_id,
        child_revision_id=record.revision_id,
        pack_id=record.pack_id,
        source_ordinal=record.source_ordinal,
        collection_slot=binding.layout.collection_slots[collection],
        canonical_parent_key=parent,
        parent_key_sha256=parent_hash,
        canonical_child_key=key,
        child_key_sha256=key_hash,
        canonical_payload=payload,
        payload_sha256=payload_hash,
    )


def logical_scalars(binding, generation, root_id, collection, record):
    """Preserve independent scalar presence, actual type and native representation."""
    field_slots = set()
    for name, scalar in record.scalars.items():
        field = binding.layout.definition.fields_by_id[name]
        if field.collection != collection or field.projection_slot is None or field.field_slot in field_slots:
            raise publication.PublicationConflict("block scalar declaration differs")
        field_slots.add(field.field_slot)
        slot = 0 if collection is None else binding.layout.collection_slots[collection]
        model = models.CustomImportRootScalar if collection is None else models.CustomImportChildScalar
        identity_by_name = {"root_revision_id" if collection is None else "child_revision_id": record.revision_id}
        if slot:
            identity_by_name["collection_slot"] = slot
        yield model(
            dataset_id=generation.dataset_id,
            schema_revision_id=generation.schema_revision_id,
            root_record_id=root_id,
            field_slot=field.field_slot,
            field_collection_slot=slot,
            projection_slot=field.projection_slot,
            **identity_by_name,
            **vars(present_scalar(scalar)),
        )


def _order_cell(value):
    if value is None:
        return 1, ()
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        digits = list(digits)
        while digits and digits[-1] == 0:
            digits.pop()
            exponent += 1
        if not digits:
            return 0, (1, 0, b"")
        adjusted = len(digits) + exponent - 1
        encoded = bytes(9 - digit if sign else digit for digit in digits)
        return 0, (0 if sign else 2, -adjusted if sign else adjusted, encoded + (b"\xff" if sign else b""))
    if isinstance(value, datetime):
        value = value.astimezone(timezone.utc)
        return 0, (value.toordinal(), value.hour, value.minute, value.second, value.microsecond)
    if isinstance(value, date):
        return 0, value.toordinal()
    return 0, value.encode("utf-8") if isinstance(value, str) else value


def _ordered(*values):
    return tuple(_order_cell(value) for value in values)


class _CapturedDigest:
    def __init__(self):
        self.buffer = io.BytesIO()

    def update(self, value):
        """Accept the exact bytes emitted by an existing publication helper."""
        self.buffer.write(value)

    def bytes(self):
        """Return the exact old helper's framed hash input, without a new hash domain."""
        return self.buffer.getvalue()


class _CheckedWork(_Work):
    def __init__(self, plan, check):
        self.check, self.stop = check, threading.Event()
        super().__init__(plan.root, plan.limits, plan.register_cleanup)

    def current(self):
        """Stop CPU work cooperatively before the owner closes its scratch or cursors."""
        if self.stop.is_set():
            raise CancellationRequested("block materialization task was cancelled")
        self.check()

    def frame(self, target, value, path=None):
        """Apply the existing finality budget before and after every scratch write."""
        self.current()
        size = super().frame(target, value, path)
        self.current()
        return size

    def read(self, source):
        """Keep external-sort merges and final replay inside the same deadline."""
        for value in super().read(source):
            self.current()
            yield value
            self.current()
        self.current()


def _producer(generation):
    return tuple(getattr(generation, name) for name in _PRODUCER_COLUMNS)


def _binding_identity(binding):
    return (
        binding.family_id,
        binding.owner_oid,
        binding.layout.digest,
        tuple(sorted(binding.oids_by_role.items())),
        None if binding.columns_sha256_by_role is None else tuple(sorted(binding.columns_sha256_by_role.items())),
        tuple(sorted((role, id(table)) for role, table in binding.tables_by_role.items())),
    )


def _deadline_check(session, check):
    """Capture immutable owner observations; CPU callbacks never access the session."""
    window = publication._current_finality_scan_window(session)
    deadline = None if window is None else window.monotonic_deadline

    def current():
        """Preserve the original lease horizon and the caller's CPU-safe stop check."""
        if deadline is not None and time.monotonic() >= deadline:
            raise publication.PublicationConflict(
                "generation sealing exceeded its lease-bounded materialization window"
            )
        if check is not None:
            check()

    return current


def _check_owner(session, generation, sections):
    publication._require_materialization_budget(session)
    sections.work.current()
    if (
        sections.transaction is not session.get_transaction()
        or not sections.transaction.is_active
        or sections.producer != _producer(generation)
        or sections.binding_identity != _binding_identity(sections.binding)
    ):
        raise publication.PublicationConflict("block materialization owner transaction or binding changed")


async def _cpu_sections(session, generation, sections, operation, *arguments):
    _check_owner(session, generation, sections)
    result = await _cpu(sections.work, operation, *arguments)
    _check_owner(session, generation, sections)
    return result


def family_frames(family_tuple):
    """Serialize the unchanged five root-family sections for both original domains."""
    frames = []
    for effective in (False, True):
        captured = _CapturedDigest()
        publication._add_family_revision_material(captured, family_tuple, {}, effective_output=effective)
        frames.append(captured.bytes())
    return tuple(frames)


def child_frames(child_tuple, expected_count):
    """Serialize one already streamed-unique child with original parent checks."""
    frames = []
    family_id = child_tuple[0].family_revision_id
    for effective in (False, True):
        captured = _CapturedDigest()
        publication._validate_and_add_family_child(
            captured, child_tuple, {family_id: expected_count}, {}, set(), effective_output=effective
        )
        frames.append(captured.bytes())
    return tuple(frames)


def _root_order(root):
    return root.key_contract_sha256, root.logical_key_sha256, root.canonical_logical_key


def _family_order(root):
    # Hash uniqueness and one selected family per root are proved before hashing.
    return _ordered(*_root_order(root))


def _child_order(family, child, root):
    # The remaining original suffix is constant for a unique collection/key hash.
    return _ordered(
        *_root_order(root), family.family_sha256, family.child_count, child.collection_slot, child.child_key_sha256
    )


def _scalar_order(root, revision, scalar, family=None):
    prefix = _root_order(root)
    if family is not None:
        prefix += (family.family_sha256, revision.collection_slot, revision.child_key_sha256)
    return _ordered(*prefix, scalar.field_collection_slot, scalar.field_slot)


def _record_frame(section, document_by_name):
    captured = _CapturedDigest()
    publication._add_digest_record(captured, section, document_by_name)
    return captured.bytes()


class ReconstructedSections:
    """Replayable canonical data; counts do not mint acceptance or database authority."""

    def __init__(self, binding, generation, plan, closed_at, check=lambda: None, *, source_record_claims=None):
        self.binding = binding
        self.producer, self.binding_identity = _producer(generation), _binding_identity(binding)
        self.work = _CheckedWork(plan, check)
        self.closed_at = closed_at
        self.transaction = plan.transaction
        self.source_record_claims, self.source_record_claims_sha256 = source_record_claims, None
        self.rows_by_section = {}
        try:
            for name in (*_SECTIONS, "pack", "rejection", "pack_id", "pack_ref", "child_id", "winner_ref", "root_key"):
                self.rows_by_section[name] = _Replay(self.work)
            if source_record_claims is not None:
                self.rows_by_section["source_record_claim"] = _Replay(self.work)
        except BaseException:
            self.close()
            raise
        self.counts = dict.fromkeys(_COUNTS, 0)
        self.digest_by_section = {name: hashlib.sha256() for name in self.rows_by_section}
        self.count_by_section = dict.fromkeys(self.rows_by_section, 0)
        self.completed_counts = None
        self.root_uniqueness_count = None

    def append(self, section, order, frames):
        """Retain one exact original framed input and its total semantic order."""
        if self.completed_counts is not None:
            raise publication.PublicationConflict("canonical materialization data is already complete")
        self.rows_by_section[section].append((order, frames))
        payload = _pack((order, frames))
        self.digest_by_section[section].update(len(payload).to_bytes(8, "big") + payload)
        self.count_by_section[section] += 1

    def _checked_rows(self, section):
        digest, count = hashlib.sha256(), 0
        for order, frames in self.rows_by_section[section].rows():
            payload = _pack((order, frames))
            digest.update(len(payload).to_bytes(8, "big") + payload)
            count += 1
            yield order, frames
        if count != self.count_by_section[section] or digest.digest() != self.digest_by_section[section].digest():
            raise publication.PublicationConflict("canonical materialization spool integrity differs")

    def ordered(self, section):
        """Stream one bounded external sort; close its runs on early exit/failure."""
        ordered = _sorted(self.work, self._checked_rows(section), lambda row: row[0])
        try:
            yield from ordered
        finally:
            ordered.close()

    def hash_into(self, digests):
        """Replay the original section sequence into unchanged hash domains."""
        if (
            self.completed_counts != tuple(self.counts[name] for name in _COUNTS)
            or self.root_uniqueness_count != self.counts["root_count"]
        ):
            raise publication.PublicationConflict("canonical materialization data is incomplete or changed")
        for section in _SECTIONS:
            for _order, frames in self.ordered(section):
                for index, digest in enumerate(digests):
                    digest.update(frames[index if len(frames) == 2 else 0])

    def close(self):
        """Release every bounded spool and only the exact registered work directory."""
        for rows in self.rows_by_section.values():
            rows.close()
        self.work.close()


def _family_model(generation, family_by_name):
    return models.CustomImportFamilyRevision(
        dataset_id=generation.dataset_id,
        schema_revision_id=generation.schema_revision_id,
        **{
            name: family_by_name[name]
            for name in (
                "family_revision_id",
                "root_record_id",
                "root_revision_id",
                "entity_binding_id",
                "family_sha256",
                "child_count",
            )
        },
        producing_execution_id=generation.execution_id,
        producing_fence=generation.producing_fence,
        producing_token_sha256=generation.producing_token_sha256,
    )


def add_verified_family(sections, generation, family_by_name, root_identity, entity, member, verified, check):
    """Reconstruct all root/child/scalar sections from one completed family proof."""
    binding = sections.binding
    if type(verified) is not VerifiedFamily or getattr(verified, "stored_manifest", None) != binding.manifest(
        family_by_name
    ):
        raise publication.PublicationConflict("block materialization requires its full stored family proof")
    family = _family_model(generation, family_by_name)
    root_record = next(verified.records())
    root_revision = logical_root(binding, generation, root_identity, root_record)
    if root_revision.root_revision_id != family.root_revision_id:
        raise publication.PublicationConflict("block family root revision differs")
    sections.append(
        "family",
        _family_order(root_identity),
        family_frames((member, family, root_revision, root_identity, entity)),
    )
    sections.append(
        "root_key",
        (bytes(root_identity.key_contract_sha256), bytes(root_identity.logical_key_sha256)),
        (root_identity.canonical_logical_key, family.root_record_id, family.family_revision_id),
    )
    for name in ("root_count", "family_count", "generation_family_count"):
        sections.counts[name] += 1
    _add_scalar_sections(sections, generation, family, root_identity, root_revision, None, root_record)
    _add_source_claim(sections, family_by_name, root_identity, root_revision, None, root_record, 0)
    sections.append("pack_ref", (root_revision.pack_id,), (root_revision.pack_id,))
    children = 0
    for collection in sorted(binding.layout.collection_slots):
        previous = None
        for child_record in verified.records(collection):
            check()
            child = logical_child(binding, generation, root_identity, collection, child_record)
            if previous == child.child_key_sha256:
                raise publication.PublicationConflict("family has duplicate logical child key")
            previous = child.child_key_sha256
            _add_child_sections(sections, generation, family, root_identity, member, child, collection, child_record)
            children += 1
            _add_source_claim(sections, family_by_name, root_identity, child, collection, child_record, children)
    if children != family.child_count:
        raise publication.PublicationConflict("family child count does not match exact membership")
    sections.counts["family_child_count"] += children


def _add_source_claim(sections, family, root, revision, collection, family_record, ordinal):
    """Observe already reconstructed hashes/coordinates; never render payload twice."""
    if sections.source_record_claims is None:
        return
    if type(family["retained"]) is not bool:
        raise publication.PublicationConflict("SOURCE record claim retained declaration differs")
    if family["retained"]:
        return
    definition = sections.binding.layout.definition
    slot = 0 if collection is None else sections.binding.layout.collection_slots[collection]
    declarations = []
    for field in definition.fields:
        if field.collection != collection or field.projection_slot is None:
            continue
        scalar = family_record.scalars.get(field.field_id)
        if scalar is not None and scalar.value_state not in ("value", "null"):
            raise publication.PublicationConflict("SOURCE scalar claim presence declaration differs")
        declarations.append(
            (
                field.field_id,
                field.field_slot,
                field.projection_slot,
                slot,
                field.value_type if scalar is None else scalar.field_type,
                scalar is not None,
                scalar is not None and scalar.value_state == "null",
            )
        )
    names = (
        definition.query.root_fields
        if collection is None
        else (definition.query.child_fields if collection == definition.query.child_collection else ())
    )
    states = bytearray((len(names) + 3) // 4)
    for index, name in enumerate(names):
        scalar = family_record.scalars.get(name)
        if scalar is not None:
            states[index // 4] |= (1 if scalar.value_state == "null" else 2) << (2 * (index % 4))
    claim = source_identity.checked_record_claim(
        (
            family["family_revision_id"],
            slot,
            family_record.revision_id,
            ordinal,
            revision.pack_id,
            revision.source_ordinal,
            bytes(root.logical_key_sha256) if collection is None else bytes(revision.child_key_sha256),
            root.canonical_logical_key if collection is None else revision.canonical_child_key,
            root.root_record_id,
            bytes(states),
            bytes(revision.payload_sha256),
            "root" if collection is None else "child",
            collection or "",
            tuple(sorted(declarations)),
        )
    )
    sections.append("source_record_claim", claim[:3], claim)


def _compare_source_claims(sections):
    """Require exact SOURCE identity/hash/declaration multiplicity and both EOFs."""
    owner = sections.source_record_claims
    if owner is None:
        return
    previous = None
    with closing(sections.ordered("source_record_claim")) as actual, closing(owner.claims()) as expected:
        for native, original in itertools.zip_longest(actual, expected):
            sections.work.current()
            if native is None or original is None:
                raise publication.PublicationConflict("SOURCE complete record claim coverage differs")
            key, row = native
            source_identity.checked_record_claim(row)
            if key != row[:3] or key == previous or _pack(row) != _pack(original):
                raise publication.PublicationConflict("SOURCE original record identity/hash/scalar declarations differ")
            previous = key
    sections.source_record_claims_sha256 = owner.sha256


def _source_claim_owner(binding, generation, claims):
    if claims is None:
        return
    if type(claims) is not source_identity.SourceRecordClaims:
        raise publication.PublicationConflict("SOURCE reconstruction requires genuine original record claims")
    if not claims.is_complete or type(claims.sha256) is not bytes or len(claims.sha256) != 32:
        raise publication.PublicationConflict("SOURCE reconstruction requires complete original record EOF")
    context = claims.context
    request = context.request
    names = ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
    if (
        context.source.snapshot_family_id != binding.family_id
        or context.layout.digest != binding.layout.digest
        or tuple(getattr(generation, name) for name in names) != tuple(getattr(request, name) for name in names)
        or generation.capture_bundle_id != dict(context.source.source.scope[0])["capture_bundle_id"]
        or generation.producing_fence != request.fence
        or bytes(generation.producing_token_sha256) != source_identity.producer_token_sha256(request)
    ):
        raise publication.PublicationConflict("SOURCE reconstruction original producer scope differs")
    claims.check_scope(context, claims.prepared.admission, claims.prepared.final_plan)


def _add_scalar_sections(sections, generation, family, root, revision, collection, record):
    section = "root_scalar" if collection is None else "child_scalar"
    for scalar in logical_scalars(sections.binding, generation, root.root_record_id, collection, record):
        document_by_name = {
            "root_key_sha256": publication._json_value(root.logical_key_sha256),
            "scalar": publication._materialization_document(scalar),
        }
        if collection is not None:
            document_by_name["child_key_sha256"] = publication._json_value(revision.child_key_sha256)
        order = _scalar_order(root, revision, scalar, None if collection is None else family)
        sections.append(section, order, (_record_frame(section, document_by_name),))
        sections.counts[section + "_count"] += 1


def _add_child_sections(sections, generation, family, root, member, child, collection, record):
    edge = models.CustomImportFamilyChild(
        dataset_id=generation.dataset_id,
        schema_revision_id=generation.schema_revision_id,
        root_record_id=root.root_record_id,
        family_revision_id=family.family_revision_id,
        collection_slot=child.collection_slot,
        child_revision_id=child.child_revision_id,
    )
    sections.append(
        "child", _child_order(family, child, root), child_frames((member, edge, child, root), family.child_count)
    )
    _add_scalar_sections(sections, generation, family, root, child, collection, record)
    sections.append("pack_ref", (child.pack_id,), (child.pack_id,))
    sections.append(
        "child_id",
        (family.family_revision_id, child.collection_slot, child.child_revision_id),
        (child.child_key_sha256,),
    )


def _family_statement(bound, generation):
    table = bound.family_block_binding.tables_by_role["family"]
    root = bound[models.CustomImportRootRecord]
    entity = bound[models.CustomImportEntityBinding]
    member = bound[models.CustomImportGenerationFamily]
    return (
        select(root, entity, member, *table.c)
        .select_from(table)
        .outerjoin(root, and_(root.root_record_id == table.c.root_record_id, root.dataset_id == generation.dataset_id))
        .outerjoin(
            entity,
            and_(entity.entity_binding_id == table.c.entity_binding_id, entity.dataset_id == generation.dataset_id),
        )
        .outerjoin(
            member,
            and_(
                member.family_revision_id == table.c.family_revision_id,
                member.root_record_id == table.c.root_record_id,
                member.dataset_id == generation.dataset_id,
                member.generation_id == generation.generation_id,
            ),
        )
        .order_by(table.c.family_revision_id)
    )


@asynccontextmanager
async def _role_rows(session, statement):
    await publication._prepare_bounded_materialization_statement(session)
    result = await session.stream(statement.execution_options(yield_per=64))
    try:
        yield result.mappings().__aiter__()
    finally:
        await result.close()


@asynccontextmanager
async def _materialization_rows(session, statement, *, scalars=False):
    stream = (
        publication._stream_materialization_scalars(session, statement)
        if scalars
        else publication._stream_materialization_records(session, statement)
    )
    async with aclosing(stream):
        yield stream


class _FamilyRoleCursor:
    def __init__(self, session, rows):
        self.session, self.rows = session, rows
        self.current = None
        self.started = False

    async def advance(self):
        """Read one native row under the existing finality deadline."""
        publication._require_materialization_budget(self.session)
        self.current = await anext(self.rows, None)
        self.started = True

    async def group(self, family_id, replay, role):
        """Spill one exact family group, refusing unowned prefix rows."""
        if not self.started:
            await self.advance()
        if self.current is not None and self.current["family_revision_id"] < family_id:
            raise publication.PublicationConflict("block materialization has an orphan native row")
        while self.current is not None and self.current["family_revision_id"] == family_id:
            replay.append((role, tuple(self.current.values())))
            await self.advance()
        replay.append((role, None))

    def require_eof(self):
        """Require true exhaustion after the last fully verified family."""
        if self.current is not None:
            raise publication.PublicationConflict("block materialization has trailing native rows")


def _verify_group(sections, family_by_name, fragments, memberships, check):
    binding, work = sections.binding, sections.work
    tables = binding.tables_by_role
    fragment_names, member_names = tuple(tables["fragment"].c.keys()), tuple(tables["revision_run"].c.keys())
    fragment_fields = (
        "fragment_ordinal",
        "raw_bytes",
        "first_record_ordinal",
        "starting_record_count",
        "first_record_offset",
        "continuation_bytes",
    )
    member_fields = (
        "collection_slot",
        "first_revision_id",
        "last_revision_id",
        "record_count",
        "first_record_ordinal",
        "encoded_membership",
    )

    def fragment_rows():
        """Recover the typed directory in its native column order."""
        for values in fragments.rows() if isinstance(fragments, _Replay) else fragments:
            check()
            native_by_name = dict(zip(fragment_names, values, strict=True))
            yield tuple(native_by_name[name] for name in fragment_fields), native_by_name["encoded_fragment"]

    def member_rows():
        """Recover actual-ID membership without inferring sequence contiguity."""
        for values in memberships.rows() if isinstance(memberships, _Replay) else memberships:
            check()
            native_by_name = dict(zip(member_names, values, strict=True))
            yield RevisionMembership(*(native_by_name[name] for name in member_fields))

    if not getattr(work, "_codec_entries_reserved", False):
        for _ in range(5):
            work._reserve_entry()
        work._codec_entries_reserved = True
    available = work.remaining_bytes
    limits = FamilyBlockLimits(
        available,
        min(work.limits.max_record_bytes, available),
        memory_bytes=min(work.limits.memory_bytes, available),
        spool_directory=str(work.root),
    )
    return verify_stored_family(
        binding.layout.definition,
        binding.layout.collection_slots,
        fragment_rows(),
        member_rows(),
        binding.manifest(family_by_name),
        limits=limits,
        check=check,
    )


async def _reconstruct_families(session, generation, bound, sections):
    tables = sections.binding.tables_by_role
    async with AsyncExitStack() as stack:
        cursor_by_role = {}
        for role in ("fragment", "revision_run"):
            table = tables[role]
            role_rows = await stack.enter_async_context(
                _role_rows(session, select(table).order_by(*table.primary_key.columns))
            )
            cursor_by_role[role] = _FamilyRoleCursor(session, role_rows)
        async with _materialization_rows(session, _family_statement(bound, generation)) as family_rows:
            await _family_batches(session, generation, sections, family_rows, cursor_by_role)
        for cursor in cursor_by_role.values():
            if not cursor.started:
                await cursor.advance()
            cursor.require_eof()


async def _family_batches(session, generation, sections, family_rows, cursor_by_role):
    batch, count = _Replay(sections.work), 0
    try:
        async for native_row in family_rows:
            await _stage_family(sections, native_row, cursor_by_role, batch)
            count += 1
            if (
                count >= publication._MATERIALIZATION_STREAM_CHUNK_SIZE
                or batch.size >= sections.work.limits.memory_bytes
            ):
                await _cpu_sections(session, generation, sections, _verify_family_batch, sections, batch, count)
                batch.close()
                batch, count = _Replay(sections.work), 0
        if count:
            await _cpu_sections(session, generation, sections, _verify_family_batch, sections, batch, count)
    finally:
        batch.close()


async def _stage_family(sections, native_row, cursor_by_role, batch):
    root, entity, member = native_row[:3]
    family_by_name = dict(zip(sections.binding.tables_by_role["family"].c.keys(), native_row[3:], strict=True))
    if root is None or entity is None or member is None:
        raise publication.PublicationConflict("block materialization family reference differs")
    batch.append(
        (
            "family",
            (
                {name: family_by_name[name] for name in _FAMILY_COLUMNS},
                tuple(getattr(root, name) for name in _ROOT_COLUMNS),
                tuple(getattr(entity, name) for name in _ENTITY_COLUMNS),
                tuple(getattr(member, name) for name in _MEMBER_COLUMNS),
            ),
        )
    )
    for role in ("fragment", "revision_run"):
        await cursor_by_role[role].group(family_by_name["family_revision_id"], batch, role)


def _batch_group(rows, role, check):
    for observed, values in rows:
        check()
        if observed != role:
            raise publication.PublicationConflict("block materialization batch group differs")
        if values is None:
            return
        yield values
    raise publication.PublicationConflict("block materialization batch group is truncated")


def _verify_family_batch(sections, batch, expected_count):
    """Consume one byte/row-bounded native batch; only one verified family is live."""
    generation = SimpleNamespace(**dict(zip(_PRODUCER_COLUMNS, sections.producer, strict=True)))
    count = 0
    rows = batch.rows()
    try:
        for role, primitive in rows:
            if role != "family":
                raise publication.PublicationConflict("block materialization batch family differs")
            family, root, entity, member = primitive
            root = models.CustomImportRootRecord(**dict(zip(_ROOT_COLUMNS, root, strict=True)))
            entity = models.CustomImportEntityBinding(**dict(zip(_ENTITY_COLUMNS, entity, strict=True)))
            member = models.CustomImportGenerationFamily(**dict(zip(_MEMBER_COLUMNS, member, strict=True)))
            _verify_batch_family(sections, generation, family, root, entity, member, rows)
            count += 1
        if count != expected_count:
            raise publication.PublicationConflict("block materialization batch census differs")
    finally:
        rows.close()


def _verify_batch_family(sections, generation, family, root, entity, member, rows):
    fragments = _batch_group(rows, "fragment", sections.work.current)
    memberships = _batch_group(rows, "revision_run", sections.work.current)
    try:
        with _verify_group(sections, family, fragments, memberships, sections.work.current) as verified:
            sections.work.charge(verified.manifest.raw_bytes)
            try:
                add_verified_family(sections, generation, family, root, entity, member, verified, sections.work.current)
            finally:
                sections.work.used -= verified.manifest.raw_bytes
    finally:
        fragments.close()
        memberships.close()


async def _execution_sections(session, generation, bound, sections):
    pack, rejection = bound[models.CustomImportPack], bound[models.CustomImportRejection]
    pack_statement = (
        select(pack)
        .where(
            pack.dataset_id == generation.dataset_id,
            pack.definition_revision_id == generation.definition_revision_id,
            pack.schema_revision_id == generation.schema_revision_id,
            pack.capture_bundle_id == generation.capture_bundle_id,
            *publication._pack_attempt_conditions(generation, models=bound),
        )
        .order_by(pack.pack_id)
    )
    async with _materialization_rows(session, pack_statement, scalars=True) as scalar_rows:
        async for native in scalar_rows:
            sections.append(
                "pack",
                _ordered(native.stream_slot, native.pack_ordinal),
                (_record_frame("pack", publication._materialization_document(native)),),
            )
            sections.append("pack_id", (native.pack_id,), (native.pack_id,))
    rejection_statement = (
        select(rejection)
        .where(
            rejection.dataset_id == generation.dataset_id,
            rejection.execution_id == generation.execution_id,
            rejection.definition_revision_id == generation.definition_revision_id,
            rejection.schema_revision_id == generation.schema_revision_id,
            rejection.producing_fence == generation.producing_fence,
            rejection.producing_token_sha256 == generation.producing_token_sha256,
        )
        .order_by(rejection.rejection_ordinal)
    )
    async with _materialization_rows(session, rejection_statement, scalars=True) as scalar_rows:
        async for native in scalar_rows:
            sections.append(
                "rejection",
                _ordered(native.rejection_ordinal),
                (_record_frame("rejection", publication._materialization_document(native)),),
            )


def _check_pack_references(sections):
    packs, references = sections.ordered("pack_id"), sections.ordered("pack_ref")
    current = next(packs, None)
    try:
        for key, _reference in references:
            while current is not None and current[0] < key:
                current = next(packs, None)
            if current is None or current[0] != key:
                raise publication.PublicationConflict("block revision pack does not share its producing attempt")
    finally:
        packs.close()
        references.close()


def _winner_statement(bound, generation):
    winner = bound[models.CustomImportWinner]
    entity = bound[models.CustomImportEntityBinding]
    root = bound[models.CustomImportRootRecord]
    member = bound[models.CustomImportGenerationFamily]
    family = bound.family_block_binding.tables_by_role["family"]
    profile = models.CustomImportSelectionProfile
    return (
        select(winner, entity, root, profile, member, *family.c)
        .select_from(winner)
        .outerjoin(
            family,
            and_(
                family.c.family_revision_id == winner.family_revision_id,
                family.c.entity_binding_id == winner.entity_binding_id,
            ),
        )
        .outerjoin(
            entity, and_(entity.entity_binding_id == winner.entity_binding_id, entity.dataset_id == winner.dataset_id)
        )
        .outerjoin(root, and_(root.root_record_id == family.c.root_record_id, root.dataset_id == winner.dataset_id))
        .outerjoin(
            member,
            and_(
                member.generation_id == winner.generation_id,
                member.dataset_id == winner.dataset_id,
                member.family_revision_id == winner.family_revision_id,
                member.root_record_id == family.c.root_record_id,
            ),
        )
        .outerjoin(
            profile,
            and_(
                profile.definition_revision_id == winner.definition_revision_id,
                profile.schema_revision_id == winner.schema_revision_id,
                profile.dataset_id == winner.dataset_id,
                profile.profile_slot == winner.profile_slot,
            ),
        )
        .where(
            winner.generation_id == generation.generation_id,
            winner.dataset_id == generation.dataset_id,
            winner.definition_revision_id == generation.definition_revision_id,
            winner.schema_revision_id == generation.schema_revision_id,
        )
        .order_by(winner.profile_slot, winner.entity_binding_id, winner.context_key_sha256)
    )


def _winner_frame(sections, prefix, document_by_name, entity_document, family_hash, root_hash, child):
    if child is None:
        child_order = (None,)
        child_hash = None
    else:
        child_hash = child[0]
        child_order = (child_hash,)
    document_by_name = dict(document_by_name) | {
        "context_child_key_sha256": publication._json_value(child_hash),
        "family_sha256": publication._json_value(family_hash),
        "root_key_sha256": publication._json_value(root_hash),
    }
    order = (
        *prefix,
        *_ordered(
            *child_order,
            family_hash,
            document_by_name["context_collection_slot"],
            bytes.fromhex(document_by_name["context_key_sha256"]),
        ),
    )
    framed = _record_frame("winner", document_by_name) + _record_frame("winner_binding", entity_document)
    sections.append("winner", order, (framed,))
    sections.counts["winner_count"] += 1


async def _winner_sections(session, generation, bound, sections):
    family_names = tuple(sections.binding.tables_by_role["family"].c.keys())
    async with _materialization_rows(session, _winner_statement(bound, generation), scalars=False) as native_rows:
        async for native in native_rows:
            winner, entity, root, profile, member = native[:5]
            family_by_name = dict(zip(family_names, native[5:], strict=True))
            if (
                entity is None
                or root is None
                or profile is None
                or member is None
                or family_by_name["family_revision_id"] is None
            ):
                raise publication.PublicationConflict("block winner reference differs")
            if winner.context_collection_slot != (profile.context_collection_slot or 0):
                raise publication.PublicationConflict("winner context collection does not match its selection profile")
            prefix = _ordered(
                winner.profile_slot, entity.adapter_id, entity.value_sha256, entity.canonical_value, *_root_order(root)
            )
            details = (
                prefix,
                publication._materialization_document(winner),
                publication._materialization_document(entity),
                family_by_name["family_sha256"],
                root.logical_key_sha256,
            )
            if winner.context_child_revision_id is None:
                if winner.context_collection_slot:
                    raise publication.PublicationConflict("block winner context child is missing")
                _winner_frame(sections, *details, None)
            else:
                sections.append(
                    "winner_ref",
                    (winner.family_revision_id, winner.context_collection_slot, winner.context_child_revision_id),
                    details,
                )
    await _cpu_sections(session, generation, sections, _winner_references, sections)


def _winner_references(sections):
    """Join the fully staged native winners without involving their database cursor."""
    children, requested = sections.ordered("child_id"), sections.ordered("winner_ref")
    child = next(children, None)
    try:
        for key, details in requested:
            sections.work.current()
            while child is not None and child[0] < key:
                child = next(children, None)
            if child is None or child[0] != key:
                raise publication.PublicationConflict("block winner context does not belong to its selected family")
            _winner_frame(sections, *details, child[1])
    finally:
        children.close()
        requested.close()


@asynccontextmanager
async def reconstruct_sections(session, binding, generation, work=None, *, source_record_claims=None):
    """Yield complete provisional canonical data/counts without minting core proof."""
    plan = _work_plan(session) if work is None else work
    if (
        type(binding) is not FamilyBlockBinding
        or type(plan) is not MaterializationWork
        or plan.transaction is not session.get_transaction()
        or not plan.transaction.is_active
    ):
        raise publication.PublicationConflict("block reconstruction requires checked binding and accounted work")
    descriptor = await session.get(CustomImportFamilyBlockLayout, binding.family_id, populate_existing=True)
    if descriptor is None or descriptor.phase not in ("closed", "verified", "logged") or descriptor.closed_at is None:
        raise publication.PublicationConflict("block reconstruction requires completed write closure")

    check = _deadline_check(session, plan.check)
    _source_claim_owner(binding, generation, source_record_claims)
    sections = ReconstructedSections(
        binding, generation, plan, descriptor.closed_at, check, source_record_claims=source_record_claims
    )
    bound = BlockMaterializationModels(binding, descriptor.closed_at)
    try:
        await _execution_sections(session, generation, bound, sections)
        await _reconstruct_families(session, generation, bound, sections)
        await _cpu_sections(session, generation, sections, _check_section_references, sections)
        await _winner_sections(session, generation, bound, sections)
        profile = models.CustomImportSelectionProfile
        profile_count = 0
        async with _materialization_rows(
            session,
            select(profile)
            .where(
                profile.dataset_id == generation.dataset_id,
                profile.definition_revision_id == generation.definition_revision_id,
                profile.schema_revision_id == generation.schema_revision_id,
            )
            .order_by(profile.profile_slot),
            scalars=True,
        ) as scalar_rows:
            async for _profile in scalar_rows:
                profile_count += 1
        sections.counts["profile_count"] = profile_count
        _complete_reconstruction(sections)
        await _cpu_sections(session, generation, sections, _compare_source_claims, sections)
        _source_claim_owner(binding, generation, source_record_claims)
        _check_owner(session, generation, sections)
        yield sections
    finally:
        sections.close()


async def _effective_prefix(session, generation, digest):
    for section, model, revision_id in (
        ("definition", models.CustomImportDefinitionRevision, generation.definition_revision_id),
        ("schema", models.CustomImportSchemaRevision, generation.schema_revision_id),
    ):
        native = await session.get(model, revision_id)
        if native is None or native.dataset_id != generation.dataset_id:
            raise publication.PublicationConflict("generation definition or schema identity is missing")
        publication._add_digest_record(digest, section, publication._materialization_document(native))
    return await publication._add_definition_shape_material(session, digest, generation)


async def finalize_sections(session, generation, sections):
    """Require fresh finality pin/proof before consuming reusable canonical data."""
    if sections.transaction is not session.get_transaction() or not sections.transaction.is_active:
        raise publication.PublicationConflict("block materialization work transaction changed")
    bound = await publication._finality_materialization_models(session, generation)
    if type(bound) is not BlockMaterializationModels:
        raise publication.PublicationConflict("block materialization no longer has its protected native binding")
    binding = bound.family_block_binding
    producer_values = _producer(generation)
    if (
        sections.producer != producer_values
        or sections.closed_at != bound.closed_at
        or binding.family_id != sections.binding.family_id
        or dict(binding.oids_by_role) != dict(sections.binding.oids_by_role)
        or binding.owner_oid != sections.binding.owner_oid
        or binding.layout.digest != sections.binding.layout.digest
    ):
        raise publication.PublicationConflict("block materialization closure or producer changed")
    if sections.completed_counts != tuple(sections.counts[name] for name in _COUNTS):
        raise publication.PublicationConflict("block materialization data is not complete")
    proof = await automated_proof(session, generation)
    if any(getattr(proof, name) != sections.counts[name] for name in _COUNTS):
        raise publication.PublicationConflict("block materialization differs from complete structural counts")
    with _finality_work_budget(session, sections):
        return await _hash_sections(session, generation, binding, sections)


@contextmanager
def _finality_work_budget(session, sections):
    previous_check = sections.work.check
    sections.work.check = _deadline_check(session, previous_check)
    try:
        sections.work.current()
        yield
    finally:
        sections.work.check = previous_check


async def _hash_sections(session, generation, binding, sections):
    source_bundle_digest = await publication._validated_source_bundle_digest(session, generation)
    digests = (
        publication._new_digest(publication._MATERIALIZATION_DOMAIN),
        publication._new_digest(publication._EFFECTIVE_OUTPUT_DOMAIN),
    )
    await publication._add_generation_identity_material(session, digests[0], generation)
    await publication._add_capture_material(session, digests[0], generation)
    profiles = await publication._add_definition_shape_material(session, digests[0], generation)
    if (
        profiles != sections.counts["profile_count"]
        or await _effective_prefix(session, generation, digests[1]) != profiles
    ):
        raise publication.PublicationConflict("block materialization profile count changed")
    await _cpu_sections(session, generation, sections, _hash_canonical_sections, sections, digests)
    materialization_result = publication._Materialization(
        source_bundle_digest, *(digest.digest() for digest in digests), *(sections.counts[name] for name in _COUNTS)
    )
    from process.custom_import.query_projection_finality import bind_generation_materialization

    try:
        return await bind_generation_materialization(
            session, generation, materialization_result, binding.layout.definition, family_block_binding=binding
        )
    except QueryProjectionUnavailable as error:
        raise publication.PublicationConflict(str(error)) from error


def _hash_canonical_sections(sections, digests):
    for section in ("pack", "rejection"):
        for _key, frames in sections.ordered(section):
            sections.work.current()
            digests[0].update(frames[0])
    sections.hash_into(digests)
    sections.work.current()


async def materialization(session, generation, bound):
    """Recompute both unchanged canonical domains after real automated acceptance."""
    try:
        await automated_proof(session, generation)
        async with reconstruct_sections(session, bound.family_block_binding, generation) as sections:
            return await finalize_sections(session, generation, sections)
    except FamilyBlockError as error:
        raise publication.PublicationConflict(str(error)) from error


def _check_root_uniqueness(sections):
    """Refuse duplicate/colliding root prefixes before compact section keys are used."""
    previous = None
    count = 0
    ordered = sections.ordered("root_key")
    try:
        for key, _identity in ordered:
            if key == previous:
                raise publication.PublicationConflict("block materialization root hash or selected family repeats")
            previous = key
            count += 1
    finally:
        ordered.close()
    if count != sections.counts["root_count"]:
        raise publication.PublicationConflict("block materialization root uniqueness coverage is incomplete")
    sections.root_uniqueness_count = count


def _check_section_references(sections):
    _check_root_uniqueness(sections)
    _check_pack_references(sections)


def _complete_reconstruction(sections):
    expected_by_section = {
        "family": sections.counts["family_count"],
        "root_key": sections.counts["root_count"],
        "child": sections.counts["family_child_count"],
        "child_id": sections.counts["family_child_count"],
        "root_scalar": sections.counts["root_scalar_count"],
        "child_scalar": sections.counts["child_scalar_count"],
        "winner": sections.counts["winner_count"],
        "pack_ref": sections.counts["root_count"] + sections.counts["family_child_count"],
    }
    if (
        sections.root_uniqueness_count != sections.counts["root_count"]
        or not sections.counts["root_count"]
        == sections.counts["family_count"]
        == sections.counts["generation_family_count"]
        or any(sections.count_by_section[name] != count for name, count in expected_by_section.items())
    ):
        raise publication.PublicationConflict("block materialization section coverage is incomplete")
    sections.completed_counts = tuple(sections.counts[name] for name in _COUNTS)
