# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Optional SOURCE semantic selection; exhaustive identity acceptance is separate."""

import hashlib
import json
import os
import stat
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType

from db.models.custom_import import CustomImportBuildAttempt, CustomImportGeneration
from process.custom_import import compact_materialization as compact
from process.custom_import import family_shard_source_values as originals
from process.custom_import.bulk_page_codec import LandingRow
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_codec import _ENVELOPE_DOMAIN, FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_directory import RevisionMembership, storage_envelope_sha256
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_storage_codec import _fragment_header
from process.custom_import.family_block_validate import _equal_native
from process.custom_import.family_shard_facts import _Replay, _sorted
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from process.custom_import.materialization import _normalized_scalar_value
from process.custom_import.snowflake_bundle_replay import _normalized_integer_replay_values, _stream_fields

_COUNTS = ("family_count", "family_child_count", "root_scalar_count", "child_scalar_count")
_CONTRACT = "custom-import/owner-semantic-audit/v1"
_FILE = "owner-semantic-audit.json"
_UNPROVEN = ("hidden_record_lineage", "stored_scalar_declarations", "protected_receipt_archive", "closed_acceptance")


@dataclass(frozen=True, init=False)
class OwnerSemanticIdentity:
    scope: tuple
    seed: bytes


def source_audit_identity(context, generation, build):
    """Snapshot actual protected freezes on the owner task, never accept a caller seed."""
    if type(generation) is not CustomImportGeneration or type(build) is not CustomImportBuildAttempt:
        raise ValueError("semantic audit requires actual frozen owner models")
    request = context.request
    capture = dict(context.source.source.scope[0])["capture_bundle_id"]
    token = producer_token_sha256(request)
    names = ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
    expected_ids = tuple(getattr(request, name) for name in names)
    freezes = tuple(getattr(build, name) for name in ("source_frozen_at", "graph_frozen_at", "output_frozen_at"))
    if (
        tuple(getattr(build, name) for name in names) != expected_ids
        or tuple(getattr(generation, name) for name in names) != expected_ids
        or build.generation_id != generation.generation_id
        or build.phase not in ("verifying", "verified")
        or any(type(stamp) is not datetime or stamp.tzinfo is None for stamp in freezes)
        or build.producing_fence != request.fence
        or build.producing_token_sha256 != token
        or generation.producing_fence != request.fence
        or generation.producing_token_sha256 != token
        or build.capture_bundle_id != capture
        or generation.capture_bundle_id != capture
        or build.base_generation_id != request.expected_base_generation_id
        or build.base_pointer_version != request.expected_pointer_version
    ):
        raise ValueError("semantic audit frozen owner scope differs")
    for identifier in (generation.generation_id, build.build_id):
        _integer(identifier, "semantic audit owner")
    scope = (
        *expected_ids,
        build.build_id,
        generation.generation_id,
        request.fence,
        token.hex(),
        context.source.snapshot_family_id,
        context.source.source.sha256.hex(),
        *(stamp.isoformat() for stamp in freezes),
        request.expected_base_generation_id,
        request.expected_pointer_version,
    )
    identity = object.__new__(OwnerSemanticIdentity)
    object.__setattr__(identity, "scope", scope)
    object.__setattr__(identity, "seed", compact._seed(generation, build))
    return identity


def _scalar_count(selection, occurrence, source_record, collection):
    fields = selection.fields_by_slot[source_record.stream_slot]
    if tuple(name for name, _value in source_record.values) != tuple(field.field_id for field in fields):
        raise ValueError("semantic census source field declarations differ")
    values_by_field = _normalized_integer_replay_values(dict(source_record.values), fields)
    declarations = []
    for declared in fields:
        if declared.projection_slot is None:
            continue
        source_value = values_by_field[declared.field_id]
        if source_value is None:
            if not declared.nullable:
                raise ValueError("semantic census required scalar is null")
        else:
            _normalized_scalar_value(declared.value_type, source_value, declared.field_id)
        declarations.append(
            (
                declared.field_id,
                declared.field_slot,
                declared.projection_slot,
                occurrence.collection_slot,
                declared.value_type,
                True,
                source_value is None,
            )
        )
    exposed = selection.query_names_by_collection.get(collection, ())
    states = bytearray((len(exposed) + 3) // 4)
    for index, name in enumerate(exposed):
        states[index // 4] |= (1 if values_by_field[name] is None else 2) << (2 * (index % 4))
    return len(declarations), bytes(states), tuple(sorted(declarations))


def _has_risk(source_record, occurrence, fields_by_id):
    if source_record.rejection_code is not None or occurrence.decision_by_name["code"] is not None:
        return True
    facts = occurrence.facts_by_name
    if any(
        facts.get(name)
        for name in (
            "parent_raw_differs",
            "payload_differs_from_last",
            "membership_problem",
            "raw_key_collision",
            "child_key_collision",
        )
    ):
        return True
    if facts.get("raw_root_count", 1) != 1 or facts.get("typed_root_count", 1) != 1 or facts.get("child_count", 0) > 1:
        return True
    for name, source_value in source_record.values:
        declared = fields_by_id[name]
        if isinstance(source_value, (date, datetime)):
            return True
        if declared.value_type == "integer" and isinstance(source_value, Decimal):
            return True
        if declared.value_type == "decimal" and source_value is not None:
            try:
                numeric = Decimal(source_value)
                if (
                    (type(source_value) is str and str(numeric) != source_value)
                    or not numeric.is_finite()
                    or numeric.as_tuple().exponent < -12
                ):
                    return True
            except ValueError, TypeError, InvalidOperation:
                return True
    return False


def _summary(plan):
    return {
        "typed_key": plan.typed_key,
        "root_record_id": plan.root_record_id,
        "family_revision_id": plan.source_metadata[0],
        "entity_binding_id": plan.source_metadata[1],
        "root_revision_id": plan.source_root.revision_id,
        "header": dict(plan.source_header),
        "child_count": 0,
        "root_scalar_count": 0,
        "child_scalar_count": 0,
        "risky": False,
        "collections": {},
        "collection_witnesses": {},
    }


def _is_accepted(occurrence, record):
    return occurrence.decision_by_name["code"] is None and not (
        record.collection_slot
        and occurrence.facts_by_name["collapse_identical"]
        and occurrence.facts_by_name["last_child"] != occurrence.coordinate
    )


def _observe_family(selection, plan, occurrences, claims):
    context, summary, roots = selection.context, _summary(plan), 0
    for occurrence in occurrences:
        selection.work.current()
        source_record = occurrence.record
        collection = originals._collection(context, occurrence, source_record)
        if getattr(selection, "observe_risks", True):
            summary["risky"] |= _has_risk(source_record, occurrence, selection.fields_by_id)
        if source_record.rejection_code is None:
            originals._native_ids(occurrence, plan.root_record_id)
        if not _is_accepted(occurrence, source_record):
            continue
        scalars, raw_states, declarations = _scalar_count(selection, occurrence, source_record, collection)
        landing_by_name = dict(zip(LandingRow._fields[1:], occurrence.landing, strict=True))
        if (
            landing_by_name["source_ordinal"] != occurrence.coordinate[1]
            or (landing_by_name["canonical_logical_key"], landing_by_name["logical_key_sha256"]) != plan.typed_key
        ):
            raise ValueError("SOURCE claim original landing scope differs")
        if collection is None:
            if occurrence.coordinate != plan.source_root.coordinate or _pack_row(
                tuple(vars(occurrence).values())
            ) != _pack_row(tuple(vars(plan.source_root).values())):
                raise ValueError("semantic census selected root differs")
            roots += 1
            summary["root_scalar_count"] += scalars
            summary["raw_states"] = raw_states
            key_hash, key_text = landing_by_name["logical_key_sha256"], landing_by_name["canonical_logical_key"]
        else:
            if source_record.typed_parent_key != plan.typed_key or source_record.child_key is None:
                raise ValueError("semantic census accepted child parent/key differs")
            key_hash, key_text = landing_by_name["child_key_sha256"], landing_by_name["canonical_child_key"]
            if key_text != source_record.child_key:
                raise ValueError("SOURCE claim original child key differs")
            summary["child_count"] += 1
            summary["child_scalar_count"] += scalars
            slot = str(occurrence.collection_slot)
            summary["collections"][slot] = summary["collections"].get(slot, 0) + 1
            summary["collection_witnesses"].setdefault(slot, occurrence.revision_id)
        claim = (
            summary["family_revision_id"],
            occurrence.collection_slot,
            occurrence.revision_id,
            occurrence.pack_id,
            occurrence.coordinate[1],
            key_hash,
            key_text,
            collection or "",
            plan.root_record_id,
            raw_states,
            landing_by_name["payload_sha256"],
            "root" if collection is None else "child",
            declarations,
        )
        selection.work.frame(claims, claim, selection.raw_claims)
    if roots != 1:
        raise ValueError("semantic census selected original root coverage differs")
    return summary


def _ordered_claims(selection):
    order = lambda row: (row[0], bool(row[1]), row[7], row[5], row[6])
    previous, ordinal, family = None, 0, None
    with closing(_sorted(selection.work, selection.work.rows(selection.raw_claims), order)) as rows:
        for row in rows:
            if type(row) is not tuple or len(row) != 13:
                raise ValueError("SOURCE raw claim shape differs")
            identity = row[0], row[1], row[5]
            if identity == previous:
                raise ValueError("semantic census repeats a logical child/root hash")
            if row[0] != family:
                family, ordinal = row[0], 0
            elif row[1] == 0:
                raise ValueError("semantic census root repeats")
            else:
                ordinal += 1
            previous = identity
            yield checked_record_claim(
                (row[0], row[1], row[2], ordinal, *row[3:7], row[8], row[9], row[10], row[11], row[7], row[12])
            )


def checked_record_claim(claim_cells):
    """Require the exact complete claim contract; extensions cannot be ignored."""
    if type(claim_cells) is not tuple or len(claim_cells) != 14:
        raise ValueError("SOURCE record claim shape differs")
    for index in (0, 2, 4, 8):
        _integer(claim_cells[index], "SOURCE claim identity")
    for index in (1, 3, 5):
        _integer(claim_cells[index], "SOURCE claim coordinate", zero=True)
    if (
        any(type(claim_cells[index]) is not bytes or len(claim_cells[index]) != 32 for index in (6, 10))
        or type(claim_cells[7]) is not str
        or type(claim_cells[9]) is not bytes
        or claim_cells[11] != ("root" if claim_cells[1] == 0 else "child")
        or type(claim_cells[12]) is not str
        or bool(claim_cells[12]) != bool(claim_cells[1])
        or type(claim_cells[13]) is not tuple
    ):
        raise ValueError("SOURCE record claim declarations differ")
    previous = None
    for declaration in claim_cells[13]:
        if (
            type(declaration) is not tuple
            or len(declaration) != 7
            or type(declaration[0]) is not str
            or type(declaration[3]) is not int
            or declaration[3] != claim_cells[1]
            or declaration[4] not in ("string", "integer", "decimal", "boolean", "date", "timestamp")
            or any(type(declaration[index]) is not bool for index in (5, 6))
            or (declaration[6] and not declaration[5])
            or (previous is not None and declaration[0] <= previous)
        ):
            raise ValueError("SOURCE scalar claim declarations differ")
        for index in (1, 2):
            _integer(declaration[index], "SOURCE scalar declaration")
        previous = declaration[0]
    return claim_cells


def _declarations(owner, definition):
    owner.fields_by_id = definition.fields_by_id
    owner.fields_by_slot = {
        slot: _stream_fields(definition, stream) for slot, stream in enumerate(definition.source_streams, 1)
    }
    owner.query_names_by_collection = {None: definition.query.root_fields}
    if definition.query.child_collection is not None:
        owner.query_names_by_collection[definition.query.child_collection] = definition.query.child_fields


class SourceRecordClaims:
    """Complete original SOURCE claims; no semantic selection or native authority."""

    def __init__(self, context, prepared, *, work):
        if type(prepared) is not originals.PreparedSourceFamilies or type(work) is not PlanWork:
            raise ValueError("SOURCE record claims require genuine prepared original custody")
        prepared.check_scope(context, prepared.admission, prepared.final_plan)
        self.context, self.prepared = context, prepared
        self.work, self.is_closed, self.is_complete = _Scratch(work), False, False
        self.records, self.sha256 = None, None
        self.counts = dict.fromkeys(_COUNTS, 0)
        try:
            self.records = _Replay(self.work)
        except BaseException:
            self.close()
            raise

    def observe(self, plan, occurrence, collection, original_record, ordinal):
        """Use the existing decoded original plus its exact independent landing sidecar."""
        if self.is_closed or self.is_complete:
            raise ValueError("SOURCE complete record claims cannot be rewritten")
        self.work.current()
        claim = _original_record_claim(self.context, plan, occurrence, collection, original_record, ordinal)
        self.records.append(claim)
        prefix = "root" if collection is None else "child"
        self.counts["family_count" if collection is None else "family_child_count"] += 1
        self.counts[prefix + "_scalar_count"] += sum(declaration[5] for declaration in claim[13])

    def seal(self, counts):
        """Publish compact receipt bytes only after complete original/dictionary EOF."""
        if self.is_closed or self.is_complete or dict(self.counts) != dict(counts):
            raise ValueError("SOURCE record claim completion or counts differ")
        if self.counts["family_count"] != self.prepared.final_plan.counts_by_kind["source"]:
            raise ValueError("SOURCE record claim selected family coverage differs")
        self.check_scope(self.context, self.prepared.admission, self.prepared.final_plan)
        with closing(_sorted(self.work, self.records.rows(), lambda claim: claim[:3])) as ordered_claims:
            self.path = self.work.write(_unique_claims(ordered_claims))
        self.records.close()
        self.records = None
        self.path.chmod(0o400)
        self.stamp, self.sha256 = originals._file_stamp(self.path), originals._staged_digest(self.path, self.work)
        self.counts, self.is_complete = MappingProxyType(self.counts), True
        self.check_scope(self.context, self.prepared.admission, self.prepared.final_plan)

    def check_scope(self, context, admission, final_plan):
        """Keep the exact genuine prepared owner and closed file live for every replay."""
        if self.is_closed:
            raise ValueError("SOURCE record claim owner is closed")
        self.work.current()
        self.prepared.check_scope(context, admission, final_plan)
        if self.is_complete and originals._file_stamp(self.path) != self.stamp:
            raise ValueError("SOURCE record claim file custody differs")

    def claims(self):
        """Replay every full typed claim and verify original complete file EOF."""
        if not self.is_complete:
            raise ValueError("SOURCE record claims require complete original EOF")
        self.check_scope(self.context, self.prepared.admission, self.prepared.final_plan)
        with os.fdopen(os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
            if (os.fstat(source.fileno()).st_dev, os.fstat(source.fileno()).st_ino) != self.stamp[:2]:
                raise ValueError("SOURCE record claim opened custody differs")
            checked = originals._DigestInput(source)
            for row in self.work.read(checked):
                yield checked_record_claim(row)
            if checked.digest.digest() != self.sha256:
                raise ValueError("SOURCE record claim complete bytes differ")
        self.check_scope(self.context, self.prepared.admission, self.prepared.final_plan)

    def close(self):
        """Release only accounted temporary claims after all consumers drain."""
        if not self.is_closed:
            self.is_closed = True
            if self.records is not None:
                self.records.close()
            self.work.close()


def _unique_claims(claims):
    previous = None
    for claim in claims:
        checked_record_claim(claim)
        if claim[:3] == previous:
            raise ValueError("SOURCE original record claim multiplicity differs")
        previous = claim[:3]
        yield claim


def _original_record_claim(context, plan, occurrence, collection, original_record, ordinal):
    definition = context.request.definition
    slot = 0 if collection is None else context.layout.collection_slots[collection]
    landing_by_name = dict(zip(LandingRow._fields[1:], occurrence.landing, strict=True))
    if (
        (occurrence.revision_id, occurrence.pack_id, occurrence.coordinate[1])
        != (original_record.revision_id, original_record.pack_id, original_record.source_ordinal)
        or occurrence.root_record_id != plan.root_record_id
        or occurrence.collection_slot != slot
        or landing_by_name["source_ordinal"] != original_record.source_ordinal
        or (landing_by_name["canonical_logical_key"], landing_by_name["logical_key_sha256"]) != plan.typed_key
    ):
        raise ValueError("SOURCE original decoded record and landing identity differ")
    values_by_field = original_record.values_by_field
    declarations = tuple(
        sorted(
            (
                field.field_id,
                field.field_slot,
                field.projection_slot,
                slot,
                field.value_type,
                field.field_id in values_by_field,
                field.field_id in values_by_field and values_by_field[field.field_id] is None,
            )
            for field in definition.fields
            if field.collection == collection and field.projection_slot is not None
        )
    )
    names = (
        definition.query.root_fields
        if collection is None
        else (definition.query.child_fields if collection == definition.query.child_collection else ())
    )
    states = bytearray((len(names) + 3) // 4)
    for index, name in enumerate(names):
        if name in values_by_field:
            states[index // 4] |= (1 if values_by_field[name] is None else 2) << (2 * (index % 4))
    key = "logical" if collection is None else "child"
    return checked_record_claim(
        (
            plan.source_metadata[0],
            slot,
            occurrence.revision_id,
            ordinal,
            occurrence.pack_id,
            occurrence.coordinate[1],
            landing_by_name[key + "_key_sha256"],
            landing_by_name["canonical_" + key + "_key"],
            plan.root_record_id,
            bytes(states),
            landing_by_name["payload_sha256"],
            "root" if collection is None else "child",
            collection or "",
            declarations,
        )
    )


@dataclass(frozen=True)
class OwnerSemanticAudit:
    """Qualified sampled evidence; never a closed identity or acceptance proof."""

    scope: tuple
    counts: tuple
    coverage: tuple
    selected_keys_sha256: bytes
    sha256: bytes
    path: str
    file_identity: tuple
    root_identity: tuple
    unproven_boundaries: tuple = field(default=_UNPROVEN, init=False)

    def check(self):
        """Reopen the retained receipt and require exact immutable file custody."""
        path = Path(self.path)
        observed = path.parent.lstat()
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o700
            or (observed.st_dev, observed.st_ino) != self.root_identity
        ):
            raise ValueError("semantic audit retained directory custody differs")
        if originals._file_stamp(path) != self.file_identity:
            raise ValueError("semantic audit retained receipt custody differs")
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
            if (os.fstat(source.fileno()).st_dev, os.fstat(source.fileno()).st_ino) != self.file_identity[
                :2
            ] or hashlib.file_digest(source, "sha256").digest() != self.sha256:
                raise ValueError("semantic audit retained receipt bytes differ")


class SourceSemanticSelection:
    """One bounded observed census and immutable optional audit selection."""

    def __init__(self, context, prepared, identity, work, receipt_root, register_receipt_cleanup):
        """Reserve only accounted temporary work beside the explicit retained owner."""
        if (
            type(identity) is not OwnerSemanticIdentity
            or type(prepared) is not originals.PreparedSourceFamilies
            or type(work) is not PlanWork
        ):
            raise ValueError("semantic selection requires its typed original owner inputs")
        if (
            type(getattr(identity, "scope", None)) is not tuple
            or type(getattr(identity, "seed", None)) is not bytes
            or len(identity.seed) != 32
        ):
            raise ValueError("semantic selection requires an observed frozen identity")
        _check_identity(context, identity)
        if receipt_root is None or not callable(register_receipt_cleanup):
            raise ValueError("semantic audit requires a registered retained receipt root")
        self.context, self.prepared, self.identity = context, prepared, identity
        _declarations(self, context.request.definition)
        self.receipt_root = Path(receipt_root).absolute()
        for temporary in (
            work.root,
            prepared.work.root,
            context.source.root,
            context.source.source.root,
            prepared.admission.root,
            prepared.final_plan.root,
        ):
            self._retain_outside(temporary)
        register_receipt_cleanup(self.receipt_root)
        prepared.check_scope(context, prepared.admission, prepared.final_plan)
        self.inputs = (
            context.source.sha256,
            prepared.admission.sha256,
            prepared.final_plan.sha256,
            context.layout.digest,
        )
        self.work, self.is_closed = _Scratch(work), False
        self.policy_limits = compact.ROOT_SAMPLE_LIMIT, compact.CHILD_SAMPLE_LIMIT
        if any(type(limit) is not int or limit <= 0 for limit in self.policy_limits):
            self.work.close()
            raise ValueError("semantic audit fixed policy limits differ")
        self.counts = dict.fromkeys(_COUNTS, 0)
        self.risk_family_count = 0
        self.populations, self.root_sample, self.child_samples = (
            {("root", 0): 0, **{("child", slot): 0 for slot in context.layout.collection_slots.values()}},
            compact._Sample(identity.seed, self.policy_limits[0]),
            {},
        )
        try:
            self._prepare()
        except BaseException:
            self.work.close()
            raise

    def _prepare(self):
        self.summaries_path, self.raw_claims = self.work.path(), self.work.path()
        with (
            self.summaries_path.open("xb") as summaries,
            self.raw_claims.open("xb") as claims,
            closing(self.prepared.observations(check=self.work.current)) as families,
        ):
            for plan, occurrences in families:
                summary = _observe_family(self, plan, occurrences, claims)
                self.work.frame(summaries, summary, self.summaries_path)
                for name, count in zip(
                    _COUNTS,
                    (1, summary["child_count"], summary["root_scalar_count"], summary["child_scalar_count"]),
                    strict=True,
                ):
                    self.counts[name] += count
                self._offer(summary)
        if self.counts["family_count"] != self.prepared.final_plan.counts_by_kind["source"]:
            raise ValueError("semantic census selected plan coverage differs")
        with closing(_sorted(self.work, _ordered_claims(self), lambda claim: claim[:3])) as ordered_claims:
            self.claims_path = self.work.write(ordered_claims)
        self.work.remove(self.raw_claims)
        self.normal_keys = frozenset(claim[0] for claim in self.root_sample.values()) | frozenset(
            claim[0] for sample in self.child_samples.values() for claim in sample.values()
        )
        self.staged = {}
        for path in (self.summaries_path, self.claims_path):
            path.chmod(0o400)
            self.staged[path] = originals._file_stamp(path), originals._staged_digest(path, self.work)
        self.counts, self.populations = MappingProxyType(self.counts), MappingProxyType(self.populations)
        self.observed_selection = self._selection_identity()
        self.check_scope(self.context, self.prepared.admission, self.prepared.final_plan, self.prepared)

    def _offer(self, summary):
        self.risk_family_count += int(summary["risky"])
        key = summary["typed_key"]
        value = key, summary["root_record_id"]
        has_change = _has_seed_change(self.root_sample, _pack_row(("root", key)), value)
        self.populations["root", 0] += 1
        for native_slot, count in summary["collections"].items():
            slot = int(native_slot)
            self.populations["child", slot] = self.populations.get(("child", slot), 0) + count
            sample = self.child_samples.setdefault(slot, compact._Sample(self.identity.seed, self.policy_limits[1]))
            has_change |= _has_seed_change(
                sample, _pack_row(("child", slot, key)), (*value, summary["collection_witnesses"][native_slot])
            )
        if not has_change:
            return
        values = (
            *self.root_sample.values(),
            *(row for sample in self.child_samples.values() for row in sample.values()),
        )
        if sum(4096 + 8 * len(_pack_row(row)) for row in values) > self.work.limits.memory_bytes // 2:
            raise FamilyBlockResourceLimit("semantic seed selection exceeds its owner reservation")

    def _selection_identity(self):
        return (
            (self.identity.scope, self.identity.seed),
            self.policy_limits,
            self.risk_family_count,
            tuple(self.counts.items()),
            tuple(sorted(self.populations.items())),
            self.normal_keys,
            tuple(self.root_sample.values()),
            tuple((slot, tuple(sample.values())) for slot, sample in sorted(self.child_samples.items())),
        )

    def check_scope(self, context, admission, final_plan, prepared):
        """Require the same originals and current full file custody on every replay."""
        if self.is_closed or prepared is not self.prepared or context is not self.context:
            raise ValueError("semantic selection owner differs or is closed")
        prepared.check_scope(context, admission, final_plan)
        if self.inputs != (context.source.sha256, admission.sha256, final_plan.sha256, context.layout.digest):
            raise ValueError("semantic selection original scope differs")
        if self.observed_selection != self._selection_identity():
            raise ValueError("semantic selection seed or observed totals changed")
        observed = self.work.root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (
            self.work.identity.st_dev,
            self.work.identity.st_ino,
        ):
            raise ValueError("semantic selection work directory custody differs")
        for path, (stamp, _digest) in self.staged.items():
            if originals._file_stamp(path) != stamp:
                raise ValueError("semantic selection census custody differs")

    def _retain_outside(self, temporary):
        temporary, retained = Path(temporary).resolve(), self.receipt_root.resolve()
        if retained == temporary or temporary in retained.parents:
            raise ValueError("semantic audit receipt cannot use temporary scratch")

    def check_outputs(self, workers):
        """Refuse a receipt location that producer cleanup would silently remove."""
        for worker in workers:
            self._retain_outside(worker._work.root)
            self._retain_outside(worker._output._work.root)

    def _rows(self, path):
        self.check_scope(self.context, self.prepared.admission, self.prepared.final_plan, self.prepared)
        stamp, expected = self.staged[path]
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
            if (os.fstat(source.fileno()).st_dev, os.fstat(source.fileno()).st_ino) != stamp[:2]:
                raise ValueError("semantic selection opened census differs")
            checked = originals._DigestInput(source)
            yield from self.work.read(checked)
            if checked.digest.digest() != expected:
                raise ValueError("semantic selection census complete bytes differ")
        self.check_scope(self.context, self.prepared.admission, self.prepared.final_plan, self.prepared)

    def summaries(self):
        """Replay every original family summary, including every unchosen family."""
        return self._rows(self.summaries_path)

    def claims(self):
        """Replay full original membership/key/lineage claims, never sampled totals."""
        with closing(self._rows(self.claims_path)) as rows:
            for row in rows:
                yield checked_record_claim(row)

    def chosen(self, summary):
        """Select fixed normal seed families and every original risk lane."""
        return summary["risky"] or summary["typed_key"] in self.normal_keys

    def source_decisions(self):
        """Keep filtering ordered and bounded while requiring the complete census."""
        with closing(self.summaries()) as summaries:
            for summary in summaries:
                yield summary["typed_key"], self.chosen(summary)

    def receipt(self, counts, coverage, selected_digest, audited_risk):
        """Persist actual semantic coverage without claiming hidden identity joins."""
        roots = [seed_key[1] for seed_key in self.root_sample.values()]
        child_seed_keys_by_slot = {
            slot: [seed_key[2] for seed_key in sample.values()] for slot, sample in self.child_samples.items()
        }
        seed_evidence = compact._evidence(self.identity.seed, roots, child_seed_keys_by_slot, {}, self.populations)
        receipt_by_name = {
            "contract": _CONTRACT,
            "mode": "seeded_complete_family_audit",
            "scope": self.identity.scope,
            "inputs": [input_digest.hex() for input_digest in self.inputs],
            "seed_selection_evidence": seed_evidence,
            "full_observed_counts": tuple(self.counts.items()),
            "source_record_count": self.context.source.source.counts[0],
            "policy": {
                "unit": "complete_source_family",
                "root_limit": self.policy_limits[0],
                "child_witness_limit": self.policy_limits[1],
            },
            "actual_audited_counts": counts,
            "actual_coverage": coverage,
            "risk_coverage": {"population": self.risk_family_count, "audited": audited_risk},
            "selected_keys_sha256": selected_digest.hex(),
            "unproven_boundaries": _UNPROVEN,
        }
        receipt_bytes = json.dumps(receipt_by_name, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        path, root_identity = _persist(self.receipt_root, receipt_bytes)
        return OwnerSemanticAudit(
            self.identity.scope,
            counts,
            coverage,
            selected_digest,
            hashlib.sha256(receipt_bytes).digest(),
            str(path),
            originals._file_stamp(path),
            root_identity,
        )

    def close(self):
        """Remove only temporary selection work; preserve the retained audit receipt."""
        if not self.is_closed:
            self.is_closed = True
            self.work.close()


class SourceOutputAudit:
    """Consume every cheap output identity while retaining only chosen family bodies."""

    def __init__(self, selection, work):
        self.selection, self.work = selection, work
        self.summaries = selection.summaries()
        self.current = None
        self.members = _Replay(work)
        try:
            self.queries = _Replay(work)
        except BaseException:
            self.summaries.close()
            self.members.close()
            raise
        self.audited_counts = dict.fromkeys(_COUNTS, 0)
        self.audited_risk = 0
        self.audited_children = {slot: 0 for kind, slot in selection.populations if kind == "child"}
        self.selected_digest = hashlib.sha256()
        self.storage = None

    def begin(self, header, family):
        """Match every original family before any optional body spool is allocated."""
        self.current = next(self.summaries, None)
        expected = self.current
        if expected is None or not _equal_native(dict(header), expected["header"]):
            raise ValueError("semantic audit complete family/header coverage differs")
        names = ("family_revision_id", "root_record_id", "root_revision_id", "entity_binding_id", "child_count")
        if any(family[name] != expected[name] for name in names) or family["retained"] is not False:
            raise ValueError("semantic audit family native identities/count differ")
        if family["raw_states"] != expected["raw_states"] or family["raw_type_overrides"] is not None:
            raise ValueError("semantic audit root scalar declarations differ")
        self.storage = _StorageAudit(self.selection.context.layout, family, self.work)
        return self.selection.chosen(expected)

    def observe(self, role, cells, family):
        """Keep compact memberships and query identities, without block decompression."""
        self.storage.observe(role, cells)
        if role == "revision_run":
            for revision, ordinal in RevisionMembership(*cells[1:]).records():
                self.work.current()
                self.members.append((family["family_revision_id"], cells[1], revision, ordinal))
        elif role == "query_record":
            self.queries.append((cells[1], cells[2], cells[0], cells[3], cells[4], cells[5], cells[6]))

    def end(self):
        """Check unchanged full storage hashes before any family can be skipped."""
        self.storage.finish()
        self.storage.close()
        self.storage = None

    def audited(self, counts):
        """Count only complete successful original/body comparisons as semantic coverage."""
        expected = self.current
        values = (1, expected["child_count"], expected["root_scalar_count"], expected["child_scalar_count"])
        if values != (1, *counts):
            raise ValueError("semantic audit decoded counts differ from complete original census")
        for name, count in zip(_COUNTS, values, strict=True):
            self.audited_counts[name] += count
        for slot, count in expected["collections"].items():
            self.audited_children[int(slot)] += count
        self.audited_risk += int(expected["risky"])
        packed = _pack_row(expected["typed_key"])
        self.selected_digest.update(len(packed).to_bytes(8, "big") + packed)

    def claims(self):
        """Require complete original/output membership and query-key equality in both directions."""
        if next(self.summaries, None) is not None:
            raise ValueError("semantic audit has trailing original families")
        with (
            closing(_sorted(self.work, self.members.rows(), lambda row: row[:3])) as actual,
            closing(self.selection.claims()) as expected,
        ):
            _equal_members(actual, expected, self.work)
        slot = self.selection.context.layout.collection_slots.get(
            self.selection.context.request.definition.query.child_collection
        )
        with (
            closing(_sorted(self.work, self.queries.rows(), lambda row: row[:3])) as actual,
            closing(self.selection.claims()) as expected,
        ):
            yield from _query_claims(
                actual, (row for row in expected if slot is not None and row[1] == slot), self.work
            )

    def receipt(self):
        """Retain actual whole-family coverage separately from bounded seed witnesses."""
        coverage = (("root", 0, self.selection.counts["family_count"], self.audited_counts["family_count"]),) + tuple(
            ("child", slot, self.selection.populations["child", slot], count)
            for slot, count in sorted(self.audited_children.items())
        )
        if self.audited_risk != self.selection.risk_family_count:
            raise ValueError("semantic audit did not complete every original risk family")
        return self.selection.receipt(
            tuple(self.audited_counts.items()), coverage, self.selected_digest.digest(), self.audited_risk
        )

    def close(self):
        """Close only this comparison's compact temporary readers and claim spools."""
        self.summaries.close()
        self.members.close()
        self.queries.close()
        if self.storage is not None:
            self.storage.close()


class _StorageAudit:
    def __init__(self, layout, family, work):
        self.manifest, self.work = layout.manifest(family), work
        self.limits = FamilyBlockLimits(
            work.limits.max_spool_bytes,
            work.limits.max_record_bytes,
            fragment_bytes=min(65536, work.limits.max_record_bytes),
            memory_bytes=work.limits.memory_bytes,
        )
        self.digest = hashlib.sha256(_ENVELOPE_DOMAIN)
        self.directory = _Replay(work)
        try:
            self.memberships = _Replay(work)
        except BaseException:
            self.directory.close()
            raise

    def observe(self, role, cells):
        """Hash all original compressed bytes while staging only their compact metadata."""
        if role == "fragment":
            metadata = cells[1], cells[2], *cells[4:]
            # Only the existing bounded header/CRC-presence guard; no decompression.
            _fragment_header(cells[3], metadata, self.limits)
            self.digest.update(len(cells[3]).to_bytes(8, "big"))
            self.digest.update(cells[3])
            self.directory.append(metadata)
        elif role == "revision_run":
            self.memberships.append(cells[1:])

    def finish(self):
        """Require the complete unchanged storage envelope without semantic decoding."""
        with closing(self.directory.rows()) as directory, closing(self.memberships.rows()) as memberships:
            digest, count = storage_envelope_sha256(
                self.manifest.codec_manifest(self.digest.digest()),
                directory,
                (RevisionMembership(*cells) for cells in memberships),
                check=self.work.current,
            )
        if digest != self.manifest.storage_sha256 or count != self.manifest.membership_count:
            raise ValueError("semantic audit full storage envelope differs")

    def close(self):
        """Release this family's bounded metadata spools."""
        self.directory.close()
        self.memberships.close()


def _equal_members(actual, expected, work):
    previous = None
    for row in expected:
        checked_record_claim(row)
        work.current()
        if row[:3] == previous or next(actual, None) != row[:4]:
            raise ValueError("semantic audit complete membership identities differ")
        previous = row[:3]
    if next(actual, None) is not None:
        raise ValueError("semantic audit has trailing native membership identities")


def _has_seed_change(sample, key, value):
    previous = sample.rows[0] if sample.rows else None
    count = len(sample.rows)
    sample.add(key, value)
    return len(sample.rows) != count or sample.rows[0] is not previous


def _check_identity(context, identity):
    request = context.request
    scope = identity.scope
    request_ids = tuple(
        getattr(request, name)
        for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
    )
    producer_scope = (
        request.fence,
        producer_token_sha256(request).hex(),
        context.source.snapshot_family_id,
        context.source.source.sha256.hex(),
    )
    if (
        len(scope) != 15
        or scope[:4] != request_ids
        or scope[6:10] != producer_scope
        or scope[13:]
        != (
            request.expected_base_generation_id,
            request.expected_pointer_version,
        )
    ):
        raise ValueError("semantic audit frozen identity belongs to a different original context")


def _query_claims(actual, expected, work):
    for row in expected:
        checked_record_claim(row)
        work.current()
        native = next(actual, None)
        if native is None or native[:3] != row[:3] or native[5] != row[9] or native[6] is not None:
            raise ValueError("semantic audit complete query identities/declarations differ")
        for identifier in native[3:5]:
            _integer(identifier, "semantic audit query key")
        yield native[3], native[4], row[6], row[7]
    if next(actual, None) is not None:
        raise ValueError("semantic audit has trailing native query identities")


def _persist(root, receipt_bytes):
    if root.is_symlink() or (root.exists() and (not root.is_dir() or stat.S_IMODE(root.stat().st_mode) != 0o700)):
        raise ValueError("semantic audit retained root custody differs")
    root.mkdir(mode=0o700, exist_ok=True)
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        opened = os.fstat(directory)
        identity = opened.st_dev, opened.st_ino
        names = os.listdir(directory)
        if names == [_FILE]:
            _existing_receipt(directory, receipt_bytes)
        elif names:
            raise ValueError("semantic audit retained root is not empty")
        else:
            _publish_receipt(directory, receipt_bytes)
        os.fsync(directory)
        current = root.lstat()
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != identity:
            raise ValueError("semantic audit retained root changed during persistence")
        return root / _FILE, identity
    finally:
        os.close(directory)


def _existing_receipt(directory, receipt_bytes):
    descriptor = os.open(_FILE, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    with os.fdopen(descriptor, "rb") as source:
        observed = os.fstat(source.fileno())
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o400
            or observed.st_size != len(receipt_bytes)
        ):
            raise ValueError("semantic audit retained retry receipt differs")
        if source.read(len(receipt_bytes) + 1) != receipt_bytes:
            raise ValueError("semantic audit retained retry receipt differs")


def _publish_receipt(directory, receipt_bytes):
    pending = ".audit-pending"
    descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
    owned = os.fstat(descriptor)
    try:
        with os.fdopen(descriptor, "wb") as receipt_stream:
            receipt_stream.write(receipt_bytes)
            receipt_stream.flush()
            os.fsync(receipt_stream.fileno())
            os.fchmod(receipt_stream.fileno(), 0o400)
            os.fsync(receipt_stream.fileno())
        os.link(pending, _FILE, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
        os.unlink(pending, dir_fd=directory)
    except BaseException:
        _remove_pending(directory, pending, owned)
        raise


def _remove_pending(directory, pending, owned):
    try:
        observed = os.stat(pending, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return
    if (observed.st_dev, observed.st_ino) != (owned.st_dev, owned.st_ino):
        raise ValueError("semantic audit pending file custody differs")
    os.unlink(pending, dir_fd=directory)


@contextmanager
def source_audit_selection(context, prepared, identity, *, work, receipt_root, register_receipt_cleanup):
    """Keep bounded census work alive through an explicitly retained optional audit."""
    selection = SourceSemanticSelection(context, prepared, identity, work, receipt_root, register_receipt_cleanup)
    try:
        yield selection
    finally:
        selection.close()
