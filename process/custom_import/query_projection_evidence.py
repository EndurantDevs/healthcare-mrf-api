# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded semantic commitments; callers separately lock and verify table custody."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import Table, select

from db.models.custom_import import CustomImportGeneration, CustomImportGenerationSeal
from db.models.custom_import_query_projection import QUERY_PROJECTION_CONTRACT, CustomImportQueryProjection
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.query_projection_binding import QueryProjectionUnavailable, _digest, lock_projection_tables
from process.custom_import.query_projection_sources import capability_sha256
from process.custom_import.query_projection_storage import query_projection_index_count
from process.custom_import.query_projection_values import configured_fields

_MIN_BIGINT, _MAX_BIGINT = -(2**63), 2**63 - 1
_SEAL_FIELDS = (
    "query_projection_contract",
    "query_projection_capability_sha256",
    "query_projection_sha256",
    "query_projection_root_count",
    "query_projection_child_count",
)
_PRODUCER_IDS = ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")


def _bigint(value, label, *, minimum=_MIN_BIGINT):
    if type(value) is not int or not minimum <= value <= _MAX_BIGINT:
        raise QueryProjectionUnavailable(f"projection {label} is invalid")
    return value


def _definition_digest(definition):
    if not isinstance(definition, CustomImportDefinition):
        raise QueryProjectionUnavailable("projection requires a parsed definition")
    try:
        return _digest(bytes.fromhex(definition.digest), "definition digest")
    except TypeError, ValueError:
        raise QueryProjectionUnavailable("projection definition digest is invalid") from None


@dataclass(frozen=True)
class QueryProjectionEvidence:
    """Allocation-independent content evidence shared by manifests and seals."""

    contract: str
    capability_sha256: bytes
    projection_sha256: bytes
    root_count: int
    child_count: int

    def __post_init__(self):
        if type(self.contract) is not str or self.contract != QUERY_PROJECTION_CONTRACT:
            raise QueryProjectionUnavailable("projection contract is invalid")
        for name in ("capability_sha256", "projection_sha256"):
            object.__setattr__(self, name, _digest(getattr(self, name), name))
        for name in ("root_count", "child_count"):
            _bigint(getattr(self, name), name, minimum=0)

    def seal_values(self):
        """Return only the five nullable columns on the immutable generation seal."""

        return dict(
            zip(
                _SEAL_FIELDS,
                (self.contract, self.capability_sha256, self.projection_sha256, self.root_count, self.child_count),
                strict=True,
            )
        )


def read_seal_projection_evidence(seal):
    """Only five explicit NULLs permit legacy fallback; missing attributes do not."""

    try:
        values = tuple(getattr(seal, name) for name in _SEAL_FIELDS)
    except AttributeError:
        raise QueryProjectionUnavailable("projection seal evidence is missing") from None
    return None if all(value is None for value in values) else QueryProjectionEvidence(*values)


def _verify_producer(row, generation, *, sealed=False):
    identities = _PRODUCER_IDS + (("capture_bundle_id",) if sealed else ())
    for name in identities:
        actual = _bigint(getattr(row, name), name, minimum=1)
        expected = _bigint(getattr(generation, name), name, minimum=1)
        if actual != expected:
            raise QueryProjectionUnavailable("projection producer binding differs")
    prefix = "sealing" if sealed else "producing"
    if _bigint(getattr(row, f"{prefix}_fence"), "fence", minimum=1) != _bigint(
        generation.producing_fence, "generation fence", minimum=1
    ) or not hmac.compare_digest(
        _digest(getattr(row, f"{prefix}_token_sha256"), "producer token"),
        _digest(generation.producing_token_sha256, "generation token"),
    ):
        raise QueryProjectionUnavailable("projection producer binding differs")


def completed_projection_evidence(manifest, *, generation, definition, seal=None):
    """Admit completed metadata after the caller pins the exact snapshot relations."""

    expected_definition = _definition_digest(definition)
    fields = configured_fields(definition)
    try:
        if (
            manifest.phase != "complete"
            or not isinstance(manifest.complete_at, dt.datetime)
            or manifest.complete_at.utcoffset() is None
            or _bigint(manifest.index_count, "index count", minimum=0) != query_projection_index_count(definition)
        ):
            raise QueryProjectionUnavailable("projection completion evidence is invalid")
        _verify_producer(manifest, generation)
        evidence = QueryProjectionEvidence(
            manifest.projection_contract,
            manifest.capability_sha256,
            manifest.projection_sha256,
            manifest.root_count,
            manifest.child_count,
        )
        if not hmac.compare_digest(
            _digest(manifest.definition_sha256, "manifest definition"), expected_definition
        ) or not hmac.compare_digest(evidence.capability_sha256, capability_sha256(definition)):
            raise QueryProjectionUnavailable("projection definition or capability differs")
        if (evidence.root_count and not any(field.collection is None for field in fields)) or (
            evidence.child_count and not any(field.collection is not None for field in fields)
        ):
            raise QueryProjectionUnavailable("projection unconfigured scope contains rows")
        _completed_digest(manifest, definition, evidence)
        if seal is not None:
            _verify_producer(seal, generation, sealed=True)
            if read_seal_projection_evidence(seal) != evidence:
                raise QueryProjectionUnavailable("projection manifest and seal differ")
    except AttributeError:
        raise QueryProjectionUnavailable("projection completion evidence is missing") from None
    return evidence


def _completed_digest(manifest, definition, evidence):
    for scope in ("root", "child"):
        expected = getattr(evidence, f"{scope}_count")
        for prefix in ("verified", "digested"):
            if _bigint(getattr(manifest, f"{prefix}_{scope}_count"), "coverage count", minimum=0) != expected:
                raise QueryProjectionUnavailable("projection verified or digested coverage differs")
    state = ProjectionDigestState(
        manifest.capability_sha256,
        manifest.digest_chain_sha256,
        manifest.digested_root_count,
        manifest.digested_child_count,
        manifest.digest_after_entity,
        manifest.digest_after_selected_value,
        manifest.digest_after_child_key,
    )
    if ProjectionDigest(definition, state=state).finish() != evidence:
        raise QueryProjectionUnavailable("projection completed content chain differs")


async def _read_projection_metadata(session, target):
    identity_by_name = {name: _bigint(getattr(target, name, None), name, minimum=1) for name in _PRODUCER_IDS[:4]}
    generation, seal, manifest = CustomImportGeneration, CustomImportGenerationSeal, CustomImportQueryProjection
    statement = (
        select(generation, seal, manifest)
        .select_from(generation)
        .join(seal, seal.generation_id == generation.generation_id)
        .outerjoin(manifest, manifest.generation_id == generation.generation_id)
        .where(*(getattr(generation, name) == value for name, value in identity_by_name.items()))
        .execution_options(populate_existing=True)
    )
    rows = (await session.execute(statement)).one_or_none()
    if rows is None:
        raise QueryProjectionUnavailable("projection pinned generation or seal is missing")
    if any(_bigint(getattr(rows[0], name, None), name, minimum=1) != value for name, value in identity_by_name.items()):
        raise QueryProjectionUnavailable("projection pinned generation differs")
    return rows


async def read_projection_tables(session, target, definition, *, snapshot_family_id) -> Mapping[str, Table] | None:
    """Bind advertised projections once inside an already authorized pinned read transaction."""

    if not session.in_transaction():
        raise QueryProjectionUnavailable("projection reads require an active transaction")
    _definition_digest(definition)
    generation, seal, manifest = await _read_projection_metadata(session, target)
    if getattr(seal, "seal_contract", None) != "custom-import-generation-seal/v1":
        raise QueryProjectionUnavailable("projection generation seal contract is invalid")
    evidence = read_seal_projection_evidence(seal)
    if evidence is None:
        try:
            _verify_producer(seal, generation, sealed=True)
        except AttributeError:
            raise QueryProjectionUnavailable("projection seal producer is missing") from None
        return None
    if manifest is None:
        raise QueryProjectionUnavailable("advertised projection manifest is missing")
    if _bigint(getattr(manifest, "family_id", None), "snapshot family", minimum=1) != _bigint(
        snapshot_family_id, "pinned snapshot family", minimum=1
    ):
        raise QueryProjectionUnavailable("projection pinned snapshot family differs")
    completed_projection_evidence(manifest, generation=generation, definition=definition, seal=seal)
    return await lock_projection_tables(session, manifest, definition)


def _encoded_text(value, label, maximum):
    try:
        encoded = value.encode("utf-8") if type(value) is str and "\x00" not in value else b""
    except UnicodeEncodeError:
        encoded = b""
    if not 1 <= len(encoded) <= maximum:
        raise QueryProjectionUnavailable(f"projection {label} is invalid")
    return encoded


def _decimal_text(value):
    """Encode the stored NUMERIC(31,12) exactly, independently of Decimal context."""

    if type(value) is not Decimal or not value.is_finite():
        raise QueryProjectionUnavailable("projection decimal value is invalid")
    if not value:
        return "0.000000000000"
    if value.adjusted() >= 19:
        raise QueryProjectionUnavailable("projection decimal value is invalid")
    _sign, digits, exponent = value.as_tuple()
    trailing = len(digits)
    while trailing and digits[trailing - 1] == 0:
        trailing -= 1
    if exponent + len(digits) - trailing < -12:
        raise QueryProjectionUnavailable("projection decimal value exceeds its stored scale")
    return format(value, ".12f")


def _typed_values(fields, values_by_field):
    if not isinstance(values_by_field, Mapping) or set(values_by_field) != {field.field_id for field in fields}:
        raise QueryProjectionUnavailable("projection values differ from the configured fields")
    values_by_id = {}
    for field in fields:
        value = values_by_field[field.field_id]
        if value is None:
            values_by_id[field.field_id] = None
        elif field.value_type == "integer":
            values_by_id[field.field_id] = _bigint(value, "integer value")
        elif field.value_type == "decimal":
            values_by_id[field.field_id] = _decimal_text(value)
        else:
            raise QueryProjectionUnavailable("projection field type is invalid")
    return values_by_id


def _add_record(digest, section, document):
    serialized = canonical_json(document).encode("utf-8")
    digest.update(section.encode("ascii") + b"\0" + len(serialized).to_bytes(8, "big"))
    digest.update(serialized)


def _chain_record(domain, previous, section, document):
    digest = hashlib.sha256(b"custom-import/v1\0" + domain.encode("ascii") + b"\0" + previous)
    _add_record(digest, section, document)
    return digest.digest()


@dataclass(frozen=True)
class ProjectionDigestState:
    """A trusted retained checkpoint, bound separately to its producer and cursor."""

    capability_sha256: bytes
    chain_sha256: bytes
    root_count: int
    child_count: int
    canonical_entity: str | None = None
    selected_value: int | None = None
    canonical_child_key: str | None = None

    def __post_init__(self):
        for name in ("capability_sha256", "chain_sha256"):
            object.__setattr__(self, name, _digest(getattr(self, name), name))
        for name in ("root_count", "child_count"):
            _bigint(getattr(self, name), name, minimum=0)
        has_rows = bool(self.root_count or self.child_count)
        if (
            (self.canonical_entity is not None) != has_rows
            or (self.selected_value is not None) != has_rows
            or (self.canonical_child_key is not None) != bool(self.child_count)
        ):
            raise QueryProjectionUnavailable("projection digest cursor and counts differ")
        if has_rows:
            _encoded_text(self.canonical_entity, "canonical entity", 512)
            _bigint(self.selected_value, "selection")
        if self.child_count:
            _encoded_text(self.canonical_child_key, "canonical child key", 8192)


class ProjectionDigest:
    """Hash ordered roots then children, with page-independent retained checkpoints."""

    def __init__(self, definition, *, state=None):
        definition_digest = _definition_digest(definition)
        self._capability = capability_sha256(definition)
        fields = configured_fields(definition)
        self._fields = tuple(
            tuple(field for field in fields if (field.collection is not None) == child) for child in (False, True)
        )
        self._chain = _chain_record(
            "query-projection-content/v1",
            b"",
            "capability",
            {
                "contract": QUERY_PROJECTION_CONTRACT,
                "definition_sha256": definition_digest.hex(),
                "capability_sha256": self._capability.hex(),
            },
        )
        self._after = None
        self._counts = [0, 0]
        self._result = None
        if state is not None:
            self._restore(state)

    def _restore(self, state):
        if type(state) is not ProjectionDigestState or not hmac.compare_digest(
            state.capability_sha256, self._capability
        ):
            raise QueryProjectionUnavailable("projection checkpoint capability differs")
        counts = [state.root_count, state.child_count]
        if any(count and not fields for count, fields in zip(counts, self._fields, strict=True)):
            raise QueryProjectionUnavailable("projection unconfigured scope contains rows")
        if not any(counts) and not hmac.compare_digest(state.chain_sha256, self._chain):
            raise QueryProjectionUnavailable("projection empty checkpoint chain differs")
        if any(counts):
            self._after = (
                bool(state.child_count),
                state.canonical_entity.encode("utf-8"),
                state.selected_value,
            )
            if state.child_count:
                self._after += (state.canonical_child_key.encode("utf-8"),)
        self._chain, self._counts = state.chain_sha256, counts

    def checkpoint(self):
        """Retain the last semantic key across scope transitions, without finalizing."""

        cursor = (None, None, None)
        if self._after is not None:
            cursor = (
                self._after[1].decode("utf-8"),
                self._after[2],
                self._after[3].decode("utf-8") if self._after[0] else None,
            )
        return ProjectionDigestState(self._capability, self._chain, *self._counts, *cursor)

    def add_root(self, canonical_entity, selected_value, values_by_field):
        """Hash one complete root context, including an all-NULL numeric row."""

        self._add(canonical_entity, selected_value, None, values_by_field)

    def add_child(self, canonical_entity, selected_value, canonical_child_key, values_by_field):
        """Hash one complete child context without its allocated IDs or key hash."""

        if canonical_child_key is None:
            raise QueryProjectionUnavailable("projection canonical child key is invalid")
        self._add(canonical_entity, selected_value, canonical_child_key, values_by_field)

    def _add(self, canonical_entity, selected_value, canonical_child_key, values_by_field):
        if self._result is not None:
            raise QueryProjectionUnavailable("projection digest is already complete")
        is_child = canonical_child_key is not None
        if not self._fields[is_child]:
            raise QueryProjectionUnavailable("projection scope has no configured fields")
        key = (is_child, _encoded_text(canonical_entity, "canonical entity", 512), _bigint(selected_value, "selection"))
        record_map = {
            "entity": canonical_entity,
            "selected_value": selected_value,
            "values": _typed_values(self._fields[is_child], values_by_field),
        }
        if is_child:
            key += (_encoded_text(canonical_child_key, "canonical child key", 8192),)
            record_map["canonical_child_key"] = canonical_child_key
        if self._after is not None and key <= self._after:
            raise QueryProjectionUnavailable("projection semantic contexts repeat or are not ordered")
        count = _bigint(self._counts[is_child] + 1, "context count", minimum=0)
        self._chain = _chain_record(
            "query-projection-step/v1", self._chain, "child" if is_child else "root", record_map
        )
        self._after, self._counts[is_child] = key, count

    def finish(self):
        """Bind final counts without changing the retained pre-finalization chain."""

        if self._result is None:
            digest = _chain_record(
                "query-projection-finish/v1",
                self._chain,
                "counts",
                {"root_count": self._counts[0], "child_count": self._counts[1]},
            )
            self._result = QueryProjectionEvidence(QUERY_PROJECTION_CONTRACT, self._capability, digest, *self._counts)
        return self._result
