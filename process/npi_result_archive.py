# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native archive mechanics for the NPI serving family and its canonical closure.

Historical six-table capture uses an MVCC snapshot and ``ACCESS SHARE`` locks.
Canonical capture retains ``SHARE ROW EXCLUSIVE`` source locks until the caller
ends its transaction; without a canonical fence this includes the address archive.
Generation-less captures are explicitly manual legacy snapshots.  Establishing
generation authority is a separate bootstrap operation and never happens here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager, nullcontext
from dataclasses import dataclass, field, replace
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import CreateIndex, CreateTable, MetaData

from db import models
from process import entity_address_snapshot_receipt as catalog_identity
from process import reference_family_archive as native_archive
from process.mrf_address_publication import (
    canonical_contribution_model,
    canonical_reference_filter,
    canonical_schema_identity,
    canonical_spatial_index,
    merge_canonical_contribution,
    validate_canonical_closure,
)
from process.npi_result_generation import (
    RELATION_NAMES,
    NpiCanonicalProvenance,
    NpiResultGenerationAuthority,
    NpiServingGeneration,
    install_npi_stage_mutation_guards,
    publish_adopted_npi_result_generation,
    read_npi_result_generation_authority,
    require_npi_automatic_generation_order,
    validate_npi_canonical_provenance,
    validate_npi_serving_generation,
)
from process.procedure_taxonomy_signals import mark_procedure_taxonomy_inputs_changed

CONTRACT = "npi-result-family.postgres.v1"
MODEL_CONTRACT = "npi-result-family.postgres.v2"
ALIAS_MODEL_CONTRACT = "npi-result-family.postgres.alias.v1"
MODEL_CONTRACTS = (MODEL_CONTRACT, ALIAS_MODEL_CONTRACT)
CANONICAL_TABLE = "npi_canonical_address"
VALIDATION_CONTRACT = "npi-result-family.validation.v1"
_STAGE_PREFIX = "npi_result_archive_"
_PREDECESSOR_PREFIX = "npi_result_predecessor_"
_LOCK_TIMEOUT = "500ms"
_CAPTURE_TIMEOUT = "5s"
_FREEZE_FUNCTION = "reject_npi_result_archive_mutation"
_FREEZE_WRITE_TRIGGER = "npi_result_archive_write_guard"
_FREEZE_TRUNCATE_TRIGGER = "npi_result_archive_truncate_guard"
_MAX_METADATA_BYTES = 16_384
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SNAPSHOT = re.compile(r"[0-9A-Fa-f-]+\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MODEL_TYPES = (
    models.NPIData,
    models.NPIAddress,
    models.NPIDataTaxonomy,
    models.NPIDataTaxonomyGroup,
    models.NPIDataOtherIdentifier,
    models.NPIPhoneStaffing,
)
if tuple(model.__tablename__ for model in _MODEL_TYPES) != RELATION_NAMES:
    raise RuntimeError("NPI archive model declaration differs")


def npi_archive_models(*, canonical=False):
    """Return only the native models recorded by this exact archive version."""
    return (*_MODEL_TYPES, canonical_contribution_model("npi")) if canonical else _MODEL_TYPES


def npi_archive_names(*, canonical=False):
    """Preserve historical six-table receipts and the new typed seven-table closure."""
    return (*RELATION_NAMES, CANONICAL_TABLE) if canonical else RELATION_NAMES


def _has_canonical(ownership):
    return CANONICAL_TABLE in dict(ownership.relation_oids)


class NpiResultArchiveError(RuntimeError):
    """An NPI archive or exact local ownership fence is invalid."""


@dataclass(frozen=True)
class NpiTableReceipt:
    """Portable schema and row-count identity for one NPI relation."""

    model_name: str
    table_name: str
    schema_sha256: str
    row_count: int

    def as_dict(self) -> dict[str, Any]:
        """Return the bounded portable table representation."""

        return {
            "model_name": self.model_name,
            "table_name": self.table_name,
            "schema_sha256": self.schema_sha256,
            "row_count": self.row_count,
        }


@dataclass(frozen=True)
class NpiResultManifest:
    """Portable NPI content evidence and explicitly classified authority."""

    tables: tuple[NpiTableReceipt, ...]
    source_metadata: Mapping[str, Any]
    source_metadata_sha256: str
    schema_sha256: str
    capture_authority: str
    source_serving_generation: NpiServingGeneration | None
    canonical_provenance: NpiCanonicalProvenance | None
    contract: str = CONTRACT

    def as_dict(self) -> dict[str, Any]:
        """Return the strict portable archive manifest."""

        return {
            "contract": self.contract,
            "tables": [table.as_dict() for table in self.tables],
            "source_metadata": dict(self.source_metadata),
            "source_metadata_sha256": self.source_metadata_sha256,
            "schema_sha256": self.schema_sha256,
            "capture_authority": self.capture_authority,
            "source_serving_generation": (
                None if self.source_serving_generation is None else self.source_serving_generation.as_dict()
            ),
            "canonical_provenance": (
                None if self.canonical_provenance is None else self.canonical_provenance.as_dict()
            ),
        }


@dataclass(frozen=True)
class NpiSourceCapture:
    """One source snapshot kept open while the six relations are cloned."""

    source_metadata: Mapping[str, Any]
    source_metadata_sha256: str
    capture_authority: str
    source_serving_generation: NpiServingGeneration | None
    canonical_provenance: NpiCanonicalProvenance | None
    schema_name: str
    postgres_snapshot: str
    canonical: bool = False


@dataclass(frozen=True)
class NpiStageOwnership:
    """Exact UUID-owned local catalog identities for one frozen stage."""

    dataset_id: UUID
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    sequence_oids: tuple[tuple[str, int, str, str], ...]
    freeze_function_oid: int | None = None
    freeze_trigger_oids: tuple[tuple[str, int, int], ...] = ()
    freeze_catalog_versions: tuple[tuple[str, int, str, str], ...] = ()

    def as_dict(self) -> dict[str, Any]:
        """Encode the declared stage names and owned sequence bindings."""
        return {
            "dataset_id": str(self.dataset_id),
            "schema_name": self.schema_name,
            "relations": [{"schema_name": self.schema_name, "relation_name": name} for name, _ in self.relation_oids],
            "sequences": [
                {"sequence_name": name, "owner_table": table, "owner_column": column}
                for name, _oid, table, column in self.sequence_oids
            ],
        }


@dataclass(frozen=True)
class NpiPreparedSource:
    """A committed frozen clone whose exact owner can be retried."""

    manifest: NpiResultManifest
    ownership: NpiStageOwnership


@dataclass(frozen=True)
class NpiStageCapture:
    """A validated clone snapshot passed to the native archive copier."""

    manifest: NpiResultManifest
    ownership: NpiStageOwnership
    postgres_snapshot: str


@dataclass(frozen=True)
class NpiIncumbent:
    """Compare-and-swap token for all-present or all-absent live relations."""

    schema_name: str
    relations: tuple[tuple[str, bool], ...]


@dataclass(frozen=True)
class NpiValidationReceipt:
    """Publisher-minted evidence for one immutable protected NPI stage."""

    package_id: str
    stage_schema: str
    stage_schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    sealed_owner_oid: int
    manifest_sha256: str
    tables: tuple[NpiTableReceipt, ...]
    validation_sha256: str
    profile_contract: str = CONTRACT

    legacy_receipt: Mapping[str, Any] | None = field(default=None, compare=False, repr=False)

    def as_dict(self) -> dict[str, Any]:
        """Preserve a saved legacy edition or encode the current named receipt."""
        if self.legacy_receipt is not None:
            return json.loads(_canonical_json(self.legacy_receipt))
        return {
            "contract": VALIDATION_CONTRACT,
            "package_id": self.package_id,
            "profile_contract": self.profile_contract,
            "stage_schema": self.stage_schema,
            "relations": [{"schema_name": self.stage_schema, "relation_name": name} for name, _ in self.relation_oids],
            "manifest_sha256": self.manifest_sha256,
            "tables": [table.as_dict() for table in self.tables],
            "validation_sha256": self.validation_sha256,
        }


@dataclass(frozen=True)
class NpiCutoverAuthority:
    """Trusted controller bindings rechecked during the short cutover."""

    package_id: str
    authority: str
    stage_read_only: bool = False


@dataclass(frozen=True)
class NpiActivationReceipt:
    """Destination-local identity captured before activation commits."""

    relations: tuple[tuple[str, str], ...]
    predecessor_relations: tuple[tuple[str, str], ...]
    predecessor_schema_name: str | None
    tables: tuple[NpiTableReceipt, ...]
    adopted_authority: NpiResultGenerationAuthority
    canonical_publication: Mapping[str, Any] | None = None


def npi_stage_schema(dataset_id: UUID) -> str:
    """Derive the only admitted stage namespace from a UUID owner."""

    if not isinstance(dataset_id, UUID):
        raise NpiResultArchiveError("NPI archive stage requires a UUID dataset_id")
    return _STAGE_PREFIX + dataset_id.hex


def npi_predecessor_schema(dataset_id: UUID) -> str:
    """Derive the retained predecessor namespace from the new UUID owner."""

    if not isinstance(dataset_id, UUID):
        raise NpiResultArchiveError("NPI predecessor requires a UUID dataset_id")
    return _PREDECESSOR_PREFIX + dataset_id.hex


def _schema_name(value: object) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(normalized) or len(normalized.encode()) > 63:
        raise NpiResultArchiveError("NPI archive schema is invalid")
    return normalized


def _quoted(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise NpiResultArchiveError("NPI archive identifier is invalid")
    return f'"{value}"'


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    except TypeError, ValueError:
        raise NpiResultArchiveError("NPI archive metadata is invalid") from None


def _source_metadata(value: object) -> tuple[dict[str, Any], str]:
    if not isinstance(value, Mapping):
        raise NpiResultArchiveError("NPI archive source metadata is invalid")
    metadata_dict = dict(value)
    encoded = _canonical_json(metadata_dict)
    if len(encoded) > _MAX_METADATA_BYTES:
        raise NpiResultArchiveError("NPI archive metadata is too large")
    return metadata_dict, hashlib.sha256(b"npi-result-source/v1\0" + encoded).hexdigest()


def _schema_digest(receipts: tuple[NpiTableReceipt, ...]) -> str:
    values = [
        {
            "model_name": receipt.model_name,
            "table_name": receipt.table_name,
            "schema_sha256": receipt.schema_sha256,
        }
        for receipt in receipts
    ]
    return hashlib.sha256(b"npi-result-schema/v1\0" + _canonical_json(values)).hexdigest()


def _manifest_digest(manifest: NpiResultManifest) -> str:
    return hashlib.sha256(_canonical_json(manifest.as_dict())).hexdigest()


def _validation_digest(values: Mapping[str, Any]) -> str:
    return hashlib.sha256(b"npi-result-validation/v1\0" + _canonical_json(values)).hexdigest()


def _require_transaction(session: Any) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise NpiResultArchiveError("NPI archive operation requires a caller transaction")


async def _timeout_value(session: Any, setting: str) -> str:
    value = await session.scalar(text(f"SHOW {setting}"))
    if not isinstance(value, str) or not value:
        raise NpiResultArchiveError("NPI archive timeout state is unavailable")
    return value


async def _set_local_timeout(session: Any, setting: str, value: str) -> None:
    await session.execute(
        text("SELECT pg_catalog.set_config(:setting, :value, true)"),
        {"setting": setting, "value": value},
    )


async def _export_stage_snapshot(session: Any) -> str:
    """Export one PostgreSQL snapshot while the short capture limits are active."""

    snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
    if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
        raise NpiResultArchiveError("NPI stage snapshot is invalid")
    return snapshot


@asynccontextmanager
async def _bounded_catalog_work(session: Any):
    previous_lock = await _timeout_value(session, "lock_timeout")
    previous_statement = await _timeout_value(session, "statement_timeout")
    await _set_local_timeout(session, "lock_timeout", _LOCK_TIMEOUT)
    await _set_local_timeout(session, "statement_timeout", _CAPTURE_TIMEOUT)
    should_restore_timeouts = True
    try:
        yield
    except DBAPIError, asyncio.CancelledError:
        # Transaction rollback restores SET LOCAL even when PostgreSQL rejects a command.
        should_restore_timeouts = False
        raise
    finally:
        if should_restore_timeouts:
            await _set_local_timeout(session, "lock_timeout", previous_lock)
            await _set_local_timeout(session, "statement_timeout", previous_statement)


async def _lock_family(
    session: Any, schema_name: str, mode: str, *, nowait: bool = False, names=RELATION_NAMES
) -> None:
    relations = ", ".join(f"{_quoted(schema_name)}.{_quoted(table_name)}" for table_name in names)
    suffix = " NOWAIT" if nowait else ""
    await session.execute(text(f"LOCK TABLE {relations} IN {mode} MODE{suffix}"))


async def _relation_oid(session: Any, schema_name: str, table_name: str) -> int | None:
    value = await session.scalar(
        text(
            "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
            "WHERE namespace.nspname=:schema_name AND relation.relname=:table_name "
            "AND relation.relkind='r' AND relation.relpersistence='p' "
            "AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity"
        ),
        {"schema_name": schema_name, "table_name": table_name},
    )
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise NpiResultArchiveError("NPI archive relation is unavailable")
    return value


async def _relation_pairs(
    session: Any,
    schema_name: str,
    names=RELATION_NAMES,
) -> tuple[tuple[str, int | None], ...]:
    relation_pairs = []
    for table_name in names:
        relation_pairs.append((table_name, await _relation_oid(session, schema_name, table_name)))
    return tuple(relation_pairs)


async def _table_receipt(
    session: Any,
    *,
    schema_name: str,
    model_type: type,
) -> NpiTableReceipt:
    table_name = model_type.__tablename__
    relation_oid = await _relation_oid(session, schema_name, table_name)
    if relation_oid is None:
        raise NpiResultArchiveError("NPI archive relation is missing")
    try:
        schema_sha256 = await _npi_schema_identity(
            session,
            relation_oid,
            schema_name,
            table_name,
        )
    except Exception as error:
        raise NpiResultArchiveError("NPI archive schema identity is unavailable") from error
    row_count = await session.scalar(text(f"SELECT count(*)::bigint FROM {_quoted(schema_name)}.{_quoted(table_name)}"))
    if type(row_count) is not int or row_count < 0:
        raise NpiResultArchiveError("NPI archive row count is invalid")
    return NpiTableReceipt(model_type.__name__, table_name, schema_sha256, row_count)


async def _npi_schema_identity(
    session: Any,
    relation_oid: int,
    schema_name: str,
    table_name: str,
) -> str:
    if table_name == CANONICAL_TABLE:
        return await canonical_schema_identity(session, relation_oid, schema_name)
    columns = await _npi_columns(session, relation_oid, schema_name, table_name)
    constraints = await catalog_identity._catalog_constraints(
        session,
        relation_oid,
        schema_name,
    )
    indexes = await catalog_identity._catalog_indexes(session, relation_oid)
    catalog_identity._reject_schema_qualified_expressions(schema_name, columns, constraints, indexes)
    return catalog_identity._canonical_digest(
        {"table_name": table_name, "columns": columns, "constraints": constraints, "indexes": indexes}
    )


async def _npi_columns(session, relation_oid, schema_name, table_name):
    """Normalize only exact owned-sequence defaults without changing historical schema meanings."""
    columns = await catalog_identity._catalog_columns(session, relation_oid)
    schema_oid = await _schema_oid(session, schema_name)
    owned_sequence_columns = {
        owner_column
        for _sequence_name, _sequence_oid, owner_table, owner_column in await _owned_sequences(
            session,
            schema_oid,
        )
        if owner_table == table_name
    }
    normalized_sequence_columns = set()
    for column in columns:
        default_expression = column.get("default_expression")
        if column.get("attname") not in owned_sequence_columns:
            continue
        if default_expression is not None:
            if (
                not isinstance(default_expression, str)
                or re.fullmatch(
                    r"nextval\('(?:''|[^'])*'::regclass\)",
                    default_expression,
                )
                is None
            ):
                raise NpiResultArchiveError("NPI owned sequence default is unsupported")
            column["default_expression"] = "npi-result-owned-sequence"
        normalized_sequence_columns.add(column["attname"])
    if normalized_sequence_columns != owned_sequence_columns:
        raise NpiResultArchiveError("NPI owned sequence column is unavailable")
    return columns


async def _manifest_tables(session: Any, schema_name: str, *, canonical=False) -> tuple[NpiTableReceipt, ...]:
    return tuple(
        [
            await _table_receipt(session, schema_name=schema_name, model_type=model_type)
            for model_type in npi_archive_models(canonical=canonical)
        ]
    )


def _validate_tables(value: object, *, canonical=False) -> tuple[NpiTableReceipt, ...]:
    model_types = npi_archive_models(canonical=canonical)
    if not isinstance(value, list) or len(value) != len(model_types):
        raise NpiResultArchiveError("NPI archive table set is invalid")
    receipts = []
    for raw_table, model_type in zip(value, model_types, strict=True):
        if not isinstance(raw_table, Mapping) or set(raw_table) != {
            "model_name",
            "table_name",
            "schema_sha256",
            "row_count",
        }:
            raise NpiResultArchiveError("NPI archive table receipt is invalid")
        if (
            raw_table["model_name"] != model_type.__name__
            or raw_table["table_name"] != model_type.__tablename__
            or _SHA256.fullmatch(str(raw_table["schema_sha256"])) is None
            or type(raw_table["row_count"]) is not int
            or raw_table["row_count"] < 0
        ):
            raise NpiResultArchiveError("NPI archive table receipt is invalid")
        receipts.append(NpiTableReceipt(**dict(raw_table)))
    return tuple(receipts)


def _validate_address_metadata(metadata, contract):
    if "npi_address_witness" in metadata and contract != ALIAS_MODEL_CONTRACT:
        raise NpiResultArchiveError("NPI alias witness profile differs")
    if contract == ALIAS_MODEL_CONTRACT:
        from process.npi_alias_witness import METADATA_KEY, validate_npi_address_witness

        try:
            validate_npi_address_witness(metadata[METADATA_KEY])
        except KeyError, RuntimeError:
            raise NpiResultArchiveError("NPI alias witness is unavailable") from None


def validate_npi_result_manifest(manifest_value: object) -> NpiResultManifest:
    """Validate content evidence without upgrading legacy ordering authority."""

    if isinstance(manifest_value, NpiResultManifest):
        manifest_value = manifest_value.as_dict()
    expected_fields = {
        "contract",
        "tables",
        "source_metadata",
        "source_metadata_sha256",
        "schema_sha256",
        "capture_authority",
        "source_serving_generation",
        "canonical_provenance",
    }
    if not isinstance(manifest_value, Mapping) or set(manifest_value) != expected_fields:
        raise NpiResultArchiveError("NPI archive manifest is invalid")
    if manifest_value["contract"] not in (CONTRACT, *MODEL_CONTRACTS) or manifest_value["capture_authority"] not in {
        "tracked-generation",
        "legacy-manual",
    }:
        raise NpiResultArchiveError("NPI archive authority classification is invalid")
    metadata, metadata_sha256 = _source_metadata(manifest_value["source_metadata"])
    _validate_address_metadata(metadata, manifest_value["contract"])
    tables = _validate_tables(manifest_value["tables"], canonical=manifest_value["contract"] in MODEL_CONTRACTS)
    try:
        serving_generation = (
            None
            if manifest_value["source_serving_generation"] is None
            else validate_npi_serving_generation(manifest_value["source_serving_generation"])
        )
        provenance = (
            None
            if manifest_value["canonical_provenance"] is None
            else validate_npi_canonical_provenance(manifest_value["canonical_provenance"])
        )
    except ValueError as error:
        raise NpiResultArchiveError("NPI archive source authority is invalid") from error
    if (manifest_value["capture_authority"] == "tracked-generation") != (serving_generation is not None):
        raise NpiResultArchiveError("NPI archive authority classification differs")
    schema_sha256 = _schema_digest(tables)
    if manifest_value["source_metadata_sha256"] != metadata_sha256 or manifest_value["schema_sha256"] != schema_sha256:
        raise NpiResultArchiveError("NPI archive manifest digest differs")
    return NpiResultManifest(
        tables,
        metadata,
        metadata_sha256,
        schema_sha256,
        manifest_value["capture_authority"],
        serving_generation,
        provenance,
        manifest_value["contract"],
    )


async def capture_npi_source(
    session: Any,
    *,
    schema_name: str,
    source_metadata: Mapping[str, Any] | None,
    source_metadata_factory: Callable[[Any], Awaitable[Mapping[str, Any]]] | None = None,
    canonical: bool = False,
) -> NpiSourceCapture:
    """Capture one coherent live family while retaining its required source fences."""

    _require_transaction(session)
    if (source_metadata is None) == (source_metadata_factory is None):
        raise NpiResultArchiveError("NPI archive requires exactly one source metadata input")
    schema = _schema_name(schema_name)
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    async with _bounded_catalog_work(session):
        if canonical:
            await _lock_npi_canonical_source(session, schema)
        await _lock_family(session, schema, "ACCESS SHARE")
        authority = await read_npi_result_generation_authority(
            session,
            schema_name=schema,
        )
        pairs = await _relation_pairs(session, schema)
        if any(relation_oid is None for _, relation_oid in pairs):
            raise NpiResultArchiveError("NPI source family is incomplete")
        if authority.serving_generation is None:
            if authority.relations is not None:
                raise NpiResultArchiveError("NPI source generation is incomplete")
            capture_authority = "legacy-manual"
        elif authority.relations != tuple((schema, name) for name in RELATION_NAMES):
            raise NpiResultArchiveError("NPI source generation is drifted")
        else:
            capture_authority = "tracked-generation"
        captured_metadata = (
            await source_metadata_factory(session) if source_metadata_factory is not None else source_metadata
        )
        metadata, metadata_sha256 = _source_metadata(captured_metadata)
        if "npi_address_witness" in metadata:
            raise NpiResultArchiveError("NPI alias output requires direct native seven-table capture")
        snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
        if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
            raise NpiResultArchiveError("NPI source snapshot is invalid")
    return NpiSourceCapture(
        metadata,
        metadata_sha256,
        capture_authority,
        authority.serving_generation,
        authority.canonical_provenance,
        schema,
        snapshot,
        canonical,
    )


async def _lock_npi_canonical_source(session, schema):
    """Lock and attest the complete canonical model before its SOURCE capture."""
    await _lock_family(
        session, schema, "SHARE ROW EXCLUSIVE", nowait=True, names=(*RELATION_NAMES, "address_archive_v2")
    )
    from process.mrf_address_publication import require_canonical_source_model, require_native_read_catalog

    await require_native_read_catalog(session, tuple(oid for _name, oid in await _relation_pairs(session, schema)))
    await require_canonical_source_model(session, schema, lambda s, n: f"{_quoted(s)}.{_quoted(n)}")


async def _source_family_sequences(
    session: Any,
    source_schema: str,
) -> tuple[tuple[str, int, str, str], ...]:
    """Return source-owned sequences belonging to the copied NPI family."""

    source_schema_oid = await _schema_oid(session, source_schema)
    return tuple(
        sequence for sequence in await _owned_sequences(session, source_schema_oid) if sequence[2] in RELATION_NAMES
    )


async def _ensure_stage_owned_sequence(
    session: Any,
    *,
    stage_schema: str,
    source_sequence_name: str,
    owner_table: str,
    owner_column: str,
    stage_sequences_by_owner: Mapping[tuple[str, str], tuple[str, int]],
) -> str:
    """Return or create the sequence owned by one copied stage column."""

    existing = stage_sequences_by_owner.get((owner_table, owner_column))
    if existing is not None:
        return existing[0]
    stage_sequence_name = _schema_name(source_sequence_name)
    stage_sequence = f"{_quoted(stage_schema)}.{_quoted(stage_sequence_name)}"
    stage_column = f"{_quoted(stage_schema)}.{_quoted(owner_table)}.{_quoted(owner_column)}"
    await session.execute(text(f"CREATE SEQUENCE {stage_sequence} AS bigint"))
    await session.execute(text(f"ALTER SEQUENCE {stage_sequence} OWNED BY {stage_column}"))
    await session.execute(
        text(
            f"ALTER TABLE {_quoted(stage_schema)}.{_quoted(owner_table)} "
            f"ALTER COLUMN {_quoted(owner_column)} SET DEFAULT nextval('{stage_sequence}'::regclass)"
        )
    )
    return stage_sequence_name


async def _advance_stage_sequence(
    session: Any,
    *,
    stage_schema: str,
    sequence_name: str,
    owner_table: str,
    owner_column: str,
) -> int | None:
    """Set one stage-owned sequence to its copied table's maximum value."""

    maximum = await _stage_sequence_maximum(session, stage_schema, owner_table, owner_column)
    if maximum is not None:
        stage_sequence = f"{_quoted(stage_schema)}.{_quoted(sequence_name)}"
        await session.execute(
            text(f"SELECT pg_catalog.setval('{stage_sequence}'::regclass,:maximum,true)"),
            {"maximum": int(maximum)},
        )
        return int(maximum)
    return None


async def _stage_sequence_maximum(session: Any, schema: str, table: str, column: str) -> int | None:
    return await session.scalar(text(f"SELECT max({_quoted(column)})::bigint FROM {_quoted(schema)}.{_quoted(table)}"))


async def _verify_stage_sequences(
    session: Any,
    ownership: NpiStageOwnership,
    *,
    align: bool = False,
) -> None:
    """Check sequence state, aligning it only while the builder can write."""

    observed_sequences = await _owned_sequences(session, ownership.schema_oid)
    if observed_sequences != ownership.sequence_oids:
        raise NpiResultArchiveError("NPI archive stage sequence ownership differs")
    for sequence_name, sequence_oid, owner_table, owner_column in observed_sequences:
        maximum = await _stage_sequence_maximum(session, ownership.schema_name, owner_table, owner_column)
        stage_sequence = f"{_quoted(ownership.schema_name)}.{_quoted(sequence_name)}"
        expected_value, is_expected_called = maximum, maximum is not None
        if maximum is None:
            expected_value = await session.scalar(
                text("SELECT seqmin::bigint FROM pg_catalog.pg_sequence WHERE seqrelid=:sequence_oid"),
                {"sequence_oid": sequence_oid},
            )
            if type(expected_value) is not int:
                raise NpiResultArchiveError("NPI archive stage sequence state is unavailable")
        if align:
            await session.execute(
                text(f"SELECT pg_catalog.setval('{stage_sequence}'::regclass,:value,:called)"),
                {"value": expected_value, "called": is_expected_called},
            )
        state = (await session.execute(text(f"SELECT last_value::bigint, is_called FROM {stage_sequence}"))).one()
        if state != (expected_value, is_expected_called):
            raise NpiResultArchiveError("NPI archive stage sequence state differs")


async def _verify_stage_sequence_owners(
    session: Any,
    stage_schema_oid: int,
    source_sequences: tuple[tuple[str, int, str, str], ...],
) -> None:
    """Require the copied stage to retain exactly the source family bindings."""

    observed_owner_keys = {
        (owner_table, owner_column)
        for _, _, owner_table, owner_column in await _owned_sequences(session, stage_schema_oid)
    }
    source_owner_keys = {(owner_table, owner_column) for _, _, owner_table, owner_column in source_sequences}
    if observed_owner_keys != source_owner_keys:
        raise NpiResultArchiveError("NPI archive stage sequence ownership differs")


async def _clone_source(
    session: Any,
    capture: NpiSourceCapture,
    stage_schema: str,
    *,
    source_copy: native_archive.ReferenceFamilySourceCopy | None = None,
    deadline: float | None = None,
    on_precreated: Callable[..., Awaitable[None]] | None = None,
) -> None:
    """Load all six protected empty heaps before constructing native indexes."""
    _require_source_copy(source_copy, on_precreated)
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise NpiResultArchiveError("NPI source COPY deadline is unavailable")
    if deadline <= asyncio.get_running_loop().time():
        raise TimeoutError("NPI source COPY deadline expired")
    if _SNAPSHOT.fullmatch(capture.postgres_snapshot) is None:
        raise NpiResultArchiveError("NPI source snapshot is invalid")
    if capture.canonical is not True:
        raise NpiResultArchiveError("new NPI source requires the canonical model contract")
    try:
        dataset_id = UUID(stage_schema.removeprefix(_STAGE_PREFIX))
    except AttributeError, TypeError, ValueError:
        raise NpiResultArchiveError("NPI source clone scope differs") from None
    if stage_schema != npi_stage_schema(dataset_id):
        raise NpiResultArchiveError("NPI source clone scope differs")
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(f"SET TRANSACTION SNAPSHOT '{capture.postgres_snapshot}'"))
    ownership = await precreate_npi_restore(session, dataset_id=dataset_id, canonical=True)
    if ownership.schema_name != stage_schema:
        raise NpiResultArchiveError("NPI source clone scope differs")
    await _verify_stage_sequence_owners(
        session, ownership.schema_oid, await _source_family_sequences(session, capture.schema_name)
    )
    await on_precreated(session, ownership)
    await _copy_source_family(session, capture, ownership, source_copy, deadline)


async def _copy_source_family(session, capture, ownership, source_copy, deadline):
    """Debit one family-wide raw budget and validate indexed payload equality."""
    stage_schema = ownership.schema_name
    remaining_bytes = source_copy.max_bytes
    for model in npi_archive_models(canonical=capture.canonical):
        columns = tuple(column.name for column in model.__table__.columns)
        query = (
            f"SELECT {', '.join(_quoted(name) for name in columns)} "
            f"FROM {_quoted(capture.schema_name)}.{_quoted('address_archive_v2' if model.__tablename__ == CANONICAL_TABLE else model.__tablename__)} canonical "
            + (
                canonical_reference_filter(
                    "npi", capture.schema_name, lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}"
                )
                if model.__tablename__ == CANONICAL_TABLE
                else ""
            )
        )
        remaining_bytes = await native_archive._copy_source_projection(
            session, source_copy, query, stage_schema, model.__tablename__, columns, remaining_bytes, deadline
        )
    await complete_npi_restore(session, ownership)
    for model in npi_archive_models(canonical=capture.canonical):
        if model.__tablename__ == CANONICAL_TABLE:
            source_oid = await _relation_oid(session, capture.schema_name, "address_archive_v2")
            stage_oid = dict(ownership.relation_oids)[CANONICAL_TABLE]
            if await canonical_schema_identity(
                session, source_oid, capture.schema_name
            ) != await canonical_schema_identity(session, stage_oid, stage_schema):
                raise NpiResultArchiveError("NPI canonical source schema is unsupported")
        else:
            await _require_source_column_shape(session, capture.schema_name, stage_schema, model.__tablename__)
        if not await native_archive._is_model_table_equal(
            session,
            model,
            left_schema=capture.schema_name,
            left_name="address_archive_v2" if model.__tablename__ == CANONICAL_TABLE else model.__tablename__,
            right_schema=stage_schema,
            right_name=model.__tablename__,
            left_predicate=(
                canonical_reference_filter(
                    "npi", capture.schema_name, lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}"
                )
                if model.__tablename__ == CANONICAL_TABLE
                else None
            ),
        ):
            raise NpiResultArchiveError("NPI source clone content differs")


async def _require_source_column_shape(session, source_schema, stage_schema, table_name):
    """Never silently discard source columns, structural constraints or index semantics."""
    source_oid = await _relation_oid(session, source_schema, table_name)
    stage_oid = await _relation_oid(session, stage_schema, table_name)
    if (
        source_oid is None
        or stage_oid is None
        or (
            await _npi_columns(session, source_oid, source_schema, table_name)
            != await _npi_columns(session, stage_oid, stage_schema, table_name)
        )
    ):
        raise NpiResultArchiveError("NPI source column catalog is unsupported")
    if await _npi_schema_identity(session, source_oid, source_schema, table_name) != await _npi_schema_identity(
        session, stage_oid, stage_schema, table_name
    ):
        raise NpiResultArchiveError("NPI source schema catalog is unsupported")


def _require_source_copy(source_copy, on_precreated):
    """Retain historical readers, but never create a new unprotected clone."""
    if not isinstance(source_copy, native_archive.ReferenceFamilySourceCopy) or not callable(on_precreated):
        raise NpiResultArchiveError("NPI protected source COPY capability is unavailable")


async def _schema_oid(session: Any, schema_name: str) -> int:
    value = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": schema_name},
    )
    if type(value) is not int or value <= 0:
        raise NpiResultArchiveError("NPI archive owned schema is unavailable")
    return value


async def _namespace_relations(session: Any, schema_oid: int) -> list[Mapping[str, Any]]:
    return list(
        (
            await session.execute(
                text(
                    "SELECT relation.oid, relation.relname, relation.relkind::text AS relkind, "
                    "indexed.indrelid AS index_table_oid FROM pg_catalog.pg_class AS relation "
                    "LEFT JOIN pg_catalog.pg_index AS indexed ON indexed.indexrelid=relation.oid "
                    "WHERE relation.relnamespace=:schema_oid ORDER BY relation.oid"
                ),
                {"schema_oid": schema_oid},
            )
        ).mappings()
    )


async def _owned_sequences(
    session: Any,
    schema_oid: int,
) -> tuple[tuple[str, int, str, str], ...]:
    sequence_rows = list(
        (
            await session.execute(
                text(
                    "SELECT sequence.relname AS sequence_name, sequence.oid AS sequence_oid, "
                    "owner_table.relname AS table_name, owner_column.attname AS column_name "
                    "FROM pg_catalog.pg_class AS sequence "
                    "JOIN pg_catalog.pg_depend AS dependency ON dependency.classid='pg_class'::regclass "
                    "AND dependency.objid=sequence.oid AND dependency.objsubid=0 "
                    "AND dependency.refclassid='pg_class'::regclass "
                    "AND dependency.deptype IN ('a','i') "
                    "JOIN pg_catalog.pg_class AS owner_table ON owner_table.oid=dependency.refobjid "
                    "AND owner_table.relnamespace=:schema_oid AND owner_table.relkind='r' "
                    "JOIN pg_catalog.pg_attribute AS owner_column "
                    "ON owner_column.attrelid=owner_table.oid "
                    "AND owner_column.attnum=dependency.refobjsubid "
                    "AND NOT owner_column.attisdropped "
                    "LEFT JOIN pg_catalog.pg_attrdef AS default_value "
                    "ON default_value.adrelid=owner_table.oid "
                    "AND default_value.adnum=owner_column.attnum "
                    "LEFT JOIN pg_catalog.pg_depend AS default_dependency "
                    "ON default_dependency.classid='pg_attrdef'::regclass "
                    "AND default_dependency.objid=default_value.oid "
                    "AND default_dependency.refclassid='pg_class'::regclass "
                    "AND default_dependency.refobjid=sequence.oid "
                    "WHERE sequence.relnamespace=:schema_oid AND sequence.relkind='S' "
                    "AND (owner_column.attidentity<>'' OR default_dependency.objid IS NOT NULL) "
                    "ORDER BY sequence.relname,owner_table.relname,owner_column.attname"
                ),
                {"schema_oid": schema_oid},
            )
        ).mappings()
    )
    return tuple(
        (
            str(sequence_row["sequence_name"]),
            int(sequence_row["sequence_oid"]),
            str(sequence_row["table_name"]),
            str(sequence_row["column_name"]),
        )
        for sequence_row in sequence_rows
    )


async def capture_npi_stage_ownership(
    session: Any,
    *,
    dataset_id: UUID,
    canonical: bool = False,
) -> NpiStageOwnership:
    """Capture exact identities for a complete, otherwise closed stage."""

    _require_transaction(session)
    schema_name = npi_stage_schema(dataset_id)
    schema_oid = await _schema_oid(session, schema_name)
    relation_oids = []
    for table_name in sorted(npi_archive_names(canonical=canonical)):
        relation_oid = await _relation_oid(session, schema_name, table_name)
        if relation_oid is None:
            raise NpiResultArchiveError("NPI archive owned relation is missing")
        relation_oids.append((table_name, relation_oid))
    owned_oids = {relation_oid for _, relation_oid in relation_oids}
    sequence_oids = await _owned_sequences(session, schema_oid)
    sequence_bindings = [(owner_table, owner_column) for _, _, owner_table, owner_column in sequence_oids]
    if len(set(sequence_bindings)) != len(sequence_bindings) or any(
        owner_table not in RELATION_NAMES for owner_table, _ in sequence_bindings
    ):
        raise NpiResultArchiveError("NPI archive owned sequence set is invalid")
    owned_sequence_oids = {sequence_oid for _, sequence_oid, _, _ in sequence_oids}
    for relation in await _namespace_relations(session, schema_oid):
        relation_oid = int(relation["oid"])
        relation_kind = relation["relkind"]
        if isinstance(relation_kind, bytes):
            relation_kind = relation_kind.decode("ascii")
        if relation_kind == "r" and relation_oid in owned_oids:
            continue
        if relation_kind == "i" and int(relation["index_table_oid"] or 0) in owned_oids:
            continue
        if relation_kind == "S" and relation_oid in owned_sequence_oids:
            continue
        raise NpiResultArchiveError("NPI archive owned schema contains an unexpected relation")
    return NpiStageOwnership(dataset_id, schema_name, schema_oid, tuple(relation_oids), sequence_oids)


async def _freeze_npi_stage_relations(session, ownership, owner_role, roles):
    """Close table, column and sequence writes before the schema grants change."""
    for name, _oid in ownership.relation_oids:
        relation = f"{_quoted(ownership.schema_name)}.{_quoted(name)}"
        grants = (
            (
                await session.execute(
                    text(
                        "SELECT DISTINCT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END AS role,acl.column_name FROM (SELECT a.grantee,NULL::text AS column_name FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.oid=to_regclass(:name) UNION SELECT a.grantee,c.attname AS column_name FROM pg_attribute c CROSS JOIN LATERAL aclexplode(c.attacl) a WHERE c.attrelid=to_regclass(:name)) acl"
                    ),
                    {"name": relation},
                )
            )
            .mappings()
            .all()
        )
        await session.execute(text(f"ALTER TABLE {relation} OWNER TO {_quoted(owner_role)}"))
        for grant in grants:
            principal = "PUBLIC" if grant["role"] == "PUBLIC" else _quoted(grant["role"])
            columns = "" if grant["column_name"] is None else " (" + _quoted(grant["column_name"]) + ")"
            await session.execute(text(f"REVOKE ALL PRIVILEGES{columns} ON TABLE {relation} FROM {principal} CASCADE"))
        await session.execute(text(f"GRANT SELECT ON TABLE {relation} TO " + ",".join(_quoted(role) for role in roles)))
    for name, _oid, _table, _column in ownership.sequence_oids:
        sequence = f"{_quoted(ownership.schema_name)}.{_quoted(name)}"
        grants = (
            (
                await session.execute(
                    text(
                        "SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('S',c.relowner))) a WHERE c.oid=to_regclass(:name)"
                    ),
                    {"name": sequence},
                )
            )
            .scalars()
            .all()
        )
        await session.execute(text(f"ALTER SEQUENCE {sequence} OWNER TO {_quoted(owner_role)}"))
        for role in grants:
            await session.execute(
                text(
                    f"REVOKE ALL ON SEQUENCE {sequence} FROM {'PUBLIC' if role == 'PUBLIC' else _quoted(role)} CASCADE"
                )
            )
        await session.execute(
            text(f"GRANT SELECT ON SEQUENCE {sequence} TO " + ",".join(_quoted(role) for role in roles))
        )


async def freeze_npi_stage(
    session: Any, *, ownership: NpiStageOwnership, owner_role=None, reader_roles=()
) -> NpiStageOwnership:
    """Administratively close candidate writes using explicit configured role names."""
    _require_transaction(session)
    if not isinstance(ownership, NpiStageOwnership):
        raise NpiResultArchiveError("NPI archive stage declaration is invalid")
    if not isinstance(owner_role, str) or not _IDENTIFIER.fullmatch(owner_role) or not reader_roles:
        raise NpiResultArchiveError("NPI freeze requires configured owner and reader roles")
    roles = tuple(dict.fromkeys((owner_role, *reader_roles)))
    if any(not isinstance(role, str) or not _IDENTIFIER.fullmatch(role) or len(role.encode()) > 63 for role in roles):
        raise NpiResultArchiveError("NPI freeze role declaration is invalid")
    await _lock_family(
        session, ownership.schema_name, "ACCESS EXCLUSIVE", names=npi_archive_names(canonical=_has_canonical(ownership))
    )
    ownership = await verify_npi_stage_ownership(session, ownership)
    await _freeze_npi_stage_relations(session, ownership, owner_role, roles)
    grants = (
        (
            await session.execute(
                text(
                    "SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.nspname=:name"
                ),
                {"name": ownership.schema_name},
            )
        )
        .scalars()
        .all()
    )
    await session.execute(text(f"ALTER SCHEMA {_quoted(ownership.schema_name)} OWNER TO {_quoted(owner_role)}"))
    for role in grants:
        await session.execute(
            text(
                f"REVOKE ALL ON SCHEMA {_quoted(ownership.schema_name)} FROM {'PUBLIC' if role == 'PUBLIC' else _quoted(role)}"
            )
        )
    await session.execute(
        text(f"GRANT USAGE ON SCHEMA {_quoted(ownership.schema_name)} TO " + ",".join(_quoted(role) for role in roles))
    )
    return ownership


async def verify_npi_stage_ownership(session: Any, ownership: NpiStageOwnership) -> NpiStageOwnership:
    """Require the complete declared stage names and sequence bindings."""
    if not isinstance(ownership, NpiStageOwnership) or ownership.schema_name != npi_stage_schema(ownership.dataset_id):
        raise NpiResultArchiveError("NPI archive stage declaration is invalid")
    observed = await capture_npi_stage_ownership(
        session, dataset_id=ownership.dataset_id, canonical=_has_canonical(ownership)
    )
    if tuple(name for name, _ in observed.relation_oids) != tuple(name for name, _ in ownership.relation_oids) or tuple(
        (name, table, column) for name, _oid, table, column in observed.sequence_oids
    ) != tuple((name, table, column) for name, _oid, table, column in ownership.sequence_oids):
        raise NpiResultArchiveError("NPI archive stage declaration differs")
    return observed


async def _validate_stage_manifest(
    session: Any,
    ownership: NpiStageOwnership,
    manifest: NpiResultManifest,
    *,
    ownership_verified: bool = False,
    npi_alias_authority=None,
) -> tuple[NpiTableReceipt, ...]:
    if not ownership_verified:
        ownership = await verify_npi_stage_ownership(session, ownership)
    observed_tables = await _manifest_tables(
        session, ownership.schema_name, canonical=manifest.contract in MODEL_CONTRACTS
    )
    if observed_tables != manifest.tables:
        raise NpiResultArchiveError("NPI restored stage differs")
    alias_arguments = await _alias_arguments(session, manifest, npi_alias_authority)
    if manifest.contract in MODEL_CONTRACTS:
        await validate_canonical_closure(
            session,
            "npi",
            ownership.schema_name,
            lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}",
            **alias_arguments,
        )
    return observed_tables


async def _alias_arguments(session, manifest, authority):
    """A new profile never upgrades portable JSON into local native authority."""
    if manifest.contract != ALIAS_MODEL_CONTRACT:
        if authority is not None:
            raise NpiResultArchiveError("NPI alias authority profile differs")
        return {}
    from process.npi_alias_witness import METADATA_KEY, require_npi_alias_authority

    await require_npi_alias_authority(session, authority, manifest.source_metadata[METADATA_KEY])
    return {"npi_alias_authority": authority}


async def capture_npi_alias_manifest(session, ownership, source_metadata, *, npi_alias_authority):
    """Capture already-produced private heaps, without legacy global-source cloning."""
    from process.npi_alias_witness import METADATA_KEY, require_npi_alias_authority

    _require_transaction(session)
    if not _has_canonical(ownership) or ownership.freeze_function_oid is not None or ownership.freeze_trigger_oids:
        raise NpiResultArchiveError("NPI alias source requires native closed custody")
    ownership = await verify_npi_stage_ownership(session, ownership)
    metadata, metadata_sha = _source_metadata(source_metadata)
    await require_npi_alias_authority(session, npi_alias_authority, metadata.get(METADATA_KEY))
    await validate_canonical_closure(
        session,
        "npi",
        ownership.schema_name,
        lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}",
        npi_alias_authority=npi_alias_authority,
    )
    tables = await _manifest_tables(session, ownership.schema_name, canonical=True)
    return validate_npi_result_manifest(
        NpiResultManifest(
            tables,
            metadata,
            metadata_sha,
            _schema_digest(tables),
            "legacy-manual",
            None,
            None,
            ALIAS_MODEL_CONTRACT,
        )
    )


async def cleanup_npi_stage(session: Any, ownership: NpiStageOwnership) -> None:
    """Drop the complete named stage with bounded restrictive DDL and atomic rollback."""
    _require_transaction(session)
    exists = await session.scalar(
        text("SELECT EXISTS(SELECT FROM pg_catalog.pg_namespace WHERE nspname=:schema_name)"),
        {"schema_name": ownership.schema_name},
    )
    if not exists:
        return
    async with session.begin_nested():
        async with _bounded_catalog_work(session):
            await _lock_family(
                session,
                ownership.schema_name,
                "ACCESS SHARE",
                nowait=True,
                names=npi_archive_names(canonical=_has_canonical(ownership)),
            )
            ownership = await verify_npi_stage_ownership(session, ownership)
            relations = ", ".join(
                f"{_quoted(ownership.schema_name)}.{_quoted(table_name)}" for table_name, _ in ownership.relation_oids
            )
            await session.execute(text(f"DROP TABLE {relations} RESTRICT"))
            remaining = await session.scalar(
                text(
                    "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                    "ON n.oid=c.relnamespace WHERE n.nspname=:schema_name"
                ),
                {"schema_name": ownership.schema_name},
            )
            if int(remaining or 0):
                raise NpiResultArchiveError("NPI archive owned schema is not empty")
            await session.execute(
                text(f"DROP FUNCTION IF EXISTS {_quoted(ownership.schema_name)}.{_quoted(_FREEZE_FUNCTION)}() RESTRICT")
            )
            await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)} RESTRICT"))


async def _shielded_cleanup(session_factory: Any, ownership: NpiStageOwnership) -> None:
    async def cleanup() -> None:
        """Clean the exact stage in an independent transaction."""

        async with session_factory() as session, session.begin():
            await cleanup_npi_stage(session, ownership)

    task = asyncio.create_task(cleanup())
    is_cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            is_cancelled = True
    task.result()
    if is_cancelled:
        raise asyncio.CancelledError


async def prepare_npi_archive_source(
    session_factory: Any,
    *,
    schema_name: str,
    source_metadata: Mapping[str, Any] | None,
    dataset_id: UUID,
    on_prepared: Callable[[Any, NpiPreparedSource], Awaitable[None]],
    source_metadata_factory: Callable[[Any], Awaitable[Mapping[str, Any]]] | None = None,
    **source_options,
) -> NpiPreparedSource:
    """Clone once and persist its exact owner before clone commit."""

    if not callable(on_prepared):
        raise NpiResultArchiveError("NPI prepared-source callback is required")
    if set(source_options) - {"source_copy", "on_precreated", "source_sessions"}:
        raise NpiResultArchiveError("NPI protected source COPY capability is unavailable")
    source_copy, on_precreated = source_options.get("source_copy"), source_options.get("on_precreated")
    _require_source_copy(source_copy, on_precreated)
    source_sessions = source_options.get("source_sessions", session_factory)
    if not callable(source_sessions):
        raise NpiResultArchiveError("NPI source owning session is unavailable")
    stage_schema = npi_stage_schema(dataset_id)
    deadline = asyncio.get_running_loop().time() + source_copy.timeout
    async with asyncio.timeout_at(deadline), source_sessions() as source_session, source_session.begin():
        capture = await capture_npi_source(
            source_session,
            schema_name=schema_name,
            source_metadata=source_metadata,
            source_metadata_factory=source_metadata_factory,
            canonical=True,
        )
        async with session_factory() as clone_session, clone_session.begin():
            await _clone_source(
                clone_session,
                capture,
                stage_schema,
                source_copy=source_copy,
                deadline=deadline,
                on_precreated=on_precreated,
            )
            ownership = await capture_npi_stage_ownership(
                clone_session,
                dataset_id=dataset_id,
                canonical=True,
            )
            manifest = await _prepared_npi_source_manifest(clone_session, capture, stage_schema)
            prepared = NpiPreparedSource(manifest, ownership)
            await on_prepared(clone_session, prepared)
    return prepared


async def _prepared_npi_source_manifest(session, capture, stage_schema):
    """Bind the copied model inventory to its unchanged, pinned source authority."""
    tables = await _manifest_tables(session, stage_schema, canonical=True)
    return NpiResultManifest(
        tables,
        capture.source_metadata,
        capture.source_metadata_sha256,
        _schema_digest(tables),
        capture.capture_authority,
        capture.source_serving_generation,
        capture.canonical_provenance,
        MODEL_CONTRACT,
    )


async def export_prepared_npi_archive(
    session_factory: Any,
    *,
    prepared: NpiPreparedSource,
    archive_copy: Callable[[NpiStageCapture], Awaitable[None]],
    verify_custody: Callable[..., Awaitable[None]] | None = None,
    authenticate_npi_alias=None,
) -> NpiResultManifest:
    """Dump one committed protected clone; only historical v1 may use freeze-only custody."""

    if not isinstance(prepared, NpiPreparedSource) or not callable(archive_copy):
        raise NpiResultArchiveError("NPI prepared source is invalid")
    if verify_custody is not None and not callable(verify_custody):
        raise NpiResultArchiveError("NPI prepared source custody verifier is invalid")
    manifest = validate_npi_result_manifest(prepared.manifest)
    async with session_factory() as stage_session, stage_session.begin():
        await stage_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        async with _bounded_catalog_work(stage_session):
            await _lock_family(
                stage_session,
                prepared.ownership.schema_name,
                "ACCESS SHARE",
                names=npi_archive_names(canonical=_has_canonical(prepared.ownership)),
            )
            await verify_npi_stage_ownership(stage_session, prepared.ownership)
        if verify_custody is not None:
            await verify_custody(stage_session, prepared)
        alias_authority = None
        if manifest.contract == ALIAS_MODEL_CONTRACT:
            from process.npi_alias_witness import METADATA_KEY

            if not callable(authenticate_npi_alias) or prepared.ownership.freeze_function_oid is not None:
                raise NpiResultArchiveError("NPI alias native custody is unavailable")
            alias_authority = await authenticate_npi_alias(stage_session, manifest.source_metadata[METADATA_KEY])
        await _validate_stage_manifest(
            stage_session,
            prepared.ownership,
            manifest,
            ownership_verified=True,
            **({"npi_alias_authority": alias_authority} if alias_authority is not None else {}),
        )
        async with _bounded_catalog_work(stage_session):
            snapshot = await _export_stage_snapshot(stage_session)
        await archive_copy(NpiStageCapture(manifest, prepared.ownership, snapshot))
    return manifest


async def export_npi_archive(
    session_factory: Any,
    *,
    schema_name: str,
    source_metadata: Mapping[str, Any],
    dataset_id: UUID,
    archive_copy: Callable[[NpiStageCapture], Awaitable[None]],
    **source_options,
) -> NpiResultManifest:
    """Convenience clone/dump flow with cancellation-safe exact cleanup."""

    if set(source_options) != {"source_copy", "on_precreated", "verify_custody"}:
        raise NpiResultArchiveError("NPI protected source COPY capability is unavailable")
    _require_source_copy(source_options["source_copy"], source_options["on_precreated"])
    if not callable(source_options["verify_custody"]):
        raise NpiResultArchiveError("NPI protected source custody verifier is unavailable")
    ownership = None
    try:

        async def retain_locally(session: Any, prepared: NpiPreparedSource) -> None:
            """Keep convenience-wrapper cleanup ownership local."""

            await source_options["verify_custody"](session, prepared)

        prepared = await prepare_npi_archive_source(
            session_factory,
            schema_name=schema_name,
            source_metadata=source_metadata,
            dataset_id=dataset_id,
            on_prepared=retain_locally,
            source_copy=source_options["source_copy"],
            on_precreated=source_options["on_precreated"],
        )
        ownership = prepared.ownership
        return await export_prepared_npi_archive(
            session_factory,
            prepared=prepared,
            archive_copy=archive_copy,
            verify_custody=source_options["verify_custody"],
        )
    finally:
        if ownership is not None:
            await _shielded_cleanup(session_factory, ownership)


def _additional_index_sql(
    schema_name: str,
    model_type: type,
    index_spec: Mapping[str, Any],
) -> str:
    allowed_keys = {"index_elements", "name", "using", "unique", "include", "where"}
    if set(index_spec) - allowed_keys:
        raise NpiResultArchiveError("NPI archive model index is unsupported")
    elements = index_spec.get("index_elements")
    if not isinstance(elements, (tuple, list)) or not elements:
        raise NpiResultArchiveError("NPI archive model index is invalid")
    suffix = index_spec.get("name", "_".join(elements))
    if not isinstance(suffix, str) or _IDENTIFIER.fullmatch(suffix) is None:
        raise NpiResultArchiveError("NPI archive model index name is invalid")
    name = f"{model_type.__tablename__}_idx_{suffix}"[:63]
    using = index_spec.get("using")
    if using is not None and using not in {"btree", "gin", "gist", "hash", "brin", "spgist"}:
        raise NpiResultArchiveError("NPI archive model index method is invalid")
    using_sql = "" if using is None else f" USING {using}"
    unique_sql = " UNIQUE" if index_spec.get("unique") is True else ""
    include = index_spec.get("include")
    include_sql = ""
    if include is not None:
        if not isinstance(include, (tuple, list)) or any(
            not isinstance(column, str) or _IDENTIFIER.fullmatch(column) is None for column in include
        ):
            raise NpiResultArchiveError("NPI archive model index include is invalid")
        include_sql = " INCLUDE (" + ", ".join(_quoted(column) for column in include) + ")"
    where = index_spec.get("where")
    where_sql = "" if where is None else f" WHERE {where}"
    normalized_elements = [
        str(element)
        .replace("Geography(ST_MakePoint", "public.Geography(public.ST_MakePoint")
        .replace("geography(st_makepoint", "public.geography(public.st_makepoint")
        for element in elements
    ]
    return (
        f"CREATE{unique_sql} INDEX {_quoted(name)} ON "
        f"{_quoted(schema_name)}.{_quoted(model_type.__tablename__)}{using_sql} "
        f"({', '.join(normalized_elements)}){include_sql}{where_sql}"
    )


async def _has_postgis(session: Any) -> bool:
    value = await session.scalar(
        text(
            "SELECT to_regprocedure('public.geography(public.geometry)') IS NOT NULL "
            "AND to_regprocedure('public.st_makepoint(double precision,double precision)') "
            "IS NOT NULL"
        )
    )
    return value is True


def _uses_postgis_index(index_spec: Mapping[str, Any]) -> bool:
    return any(
        "geography(" in str(element).lower() or "st_makepoint(" in str(element).lower()
        for element in index_spec.get("index_elements", ())
    )


async def precreate_npi_restore(
    session: Any,
    *,
    dataset_id: UUID,
    canonical: bool = False,
) -> NpiStageOwnership:
    """Create six empty unindexed heaps for a native data-only restore."""

    _require_transaction(session)
    schema_name = npi_stage_schema(dataset_id)
    await session.execute(text(f"CREATE SCHEMA {_quoted(schema_name)}"))
    metadata = MetaData(schema=schema_name)
    for model_type in npi_archive_models(canonical=canonical):
        table = native_archive._clone_model_table(model_type.__table__, metadata, schema=schema_name)
        for column in table.columns:
            for constraint in tuple(column.constraints):
                column.constraints.remove(constraint)
                table.append_constraint(constraint)
        for constraint in table.constraints:
            constraint.ddl_if(callable_=lambda *_args, **_kwargs: False)
        statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        await session.execute(text(statement))
    return await capture_npi_stage_ownership(session, dataset_id=dataset_id, canonical=canonical)


async def complete_npi_restore(session: Any, ownership: NpiStageOwnership, *, npi_alias_authority=None) -> None:
    """Finish native keys, checks and all model indexes before stage validation."""

    _require_transaction(session)
    ownership = await verify_npi_stage_ownership(session, ownership)
    metadata = MetaData(schema=ownership.schema_name)
    has_postgis = await _has_postgis(session)
    for model_type in npi_archive_models(canonical=_has_canonical(ownership)):
        table = native_archive._clone_model_table(model_type.__table__, metadata, schema=ownership.schema_name)
        await native_archive._create_table_constraints(session, table)
        for index in sorted(table.indexes, key=lambda item: item.name):
            await session.execute(CreateIndex(index))
        if model_type.__tablename__ == CANONICAL_TABLE:
            await canonical_spatial_index(
                session,
                ownership.schema_name,
                CANONICAL_TABLE,
                lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}",
            )
        primary_elements = tuple(getattr(model_type, "__my_index_elements__", ()) or ())
        if primary_elements:
            await session.execute(
                text(
                    f"CREATE UNIQUE INDEX {_quoted(model_type.__tablename__ + '_idx_primary')} "
                    f"ON {_quoted(ownership.schema_name)}.{_quoted(model_type.__tablename__)} "
                    f"({', '.join(primary_elements)})"
                )
            )
        indexes = tuple(getattr(model_type, "__my_initial_indexes__", ()) or ()) + tuple(
            getattr(model_type, "__my_additional_indexes__", ()) or ()
        )
        for index in indexes:
            if _uses_postgis_index(index) and not has_postgis:
                continue
            await session.execute(text(_additional_index_sql(ownership.schema_name, model_type, index)))
    for table in metadata.tables.values():
        await native_archive._create_table_constraints(session, table, backing_indexes=False)
    if _has_canonical(ownership):
        await validate_canonical_closure(
            session,
            "npi",
            ownership.schema_name,
            lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}",
            **({"npi_alias_authority": npi_alias_authority} if npi_alias_authority is not None else {}),
        )
    for child_name in ("npi_address", "npi_taxonomy", "npi_taxonomy_group", "npi_other_identifier"):
        child = f"{_quoted(ownership.schema_name)}.{_quoted(child_name)}"
        parent = f"{_quoted(ownership.schema_name)}.npi"
        orphan = await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {child} c WHERE NOT EXISTS(SELECT 1 FROM {parent} p WHERE p.npi=c.npi))")
        )
        if orphan is not False:
            raise NpiResultArchiveError("NPI restored child relationship differs")
    await _verify_stage_sequences(session, ownership, align=True)


async def validate_npi_stage(
    session: Any,
    *,
    ownership: NpiStageOwnership,
    manifest: Mapping[str, Any] | NpiResultManifest,
    npi_alias_authority=None,
) -> tuple[NpiTableReceipt, ...]:
    """Validate restored schema/count semantics under an ownership lock."""

    _require_transaction(session)
    validated = validate_npi_result_manifest(manifest)
    async with _bounded_catalog_work(session):
        await _lock_family(
            session, ownership.schema_name, "ACCESS SHARE", names=npi_archive_names(canonical=_has_canonical(ownership))
        )
        ownership = await verify_npi_stage_ownership(session, ownership)
    return await _validate_stage_manifest(
        session,
        ownership,
        validated,
        ownership_verified=True,
        **({"npi_alias_authority": npi_alias_authority} if npi_alias_authority is not None else {}),
    )


def _validation_inventory(value: object, *, canonical=False) -> tuple[tuple[str, None], ...]:
    names = tuple(sorted(npi_archive_names(canonical=canonical)))
    if not isinstance(value, list) or len(value) != len(names):
        raise NpiResultArchiveError("NPI validation inventory is invalid")
    observed_names = []
    for entry in value:
        if isinstance(entry, dict) and set(entry) == {"schema_name", "relation_name"}:
            _schema_name(entry["schema_name"])
            observed_names.append(entry["relation_name"])
        elif isinstance(entry, list) and len(entry) == 2:
            observed_names.append(entry[0])
        else:
            raise NpiResultArchiveError("NPI validation inventory is invalid")
    if tuple(observed_names) != names:
        raise NpiResultArchiveError("NPI validation inventory is invalid")
    return tuple((name, None) for name in names)


def validate_npi_validation_receipt(receipt_value: object) -> NpiValidationReceipt:
    """Validate named receipt semantics while retaining a recorded legacy edition."""
    if isinstance(receipt_value, NpiValidationReceipt):
        receipt_value = receipt_value.as_dict()
    common_fields = {
        "contract",
        "package_id",
        "profile_contract",
        "stage_schema",
        "manifest_sha256",
        "tables",
        "validation_sha256",
    }
    legacy_fields = {"stage_schema_oid", "relation_oids", "sealed_owner_oid"}
    if not isinstance(receipt_value, Mapping) or set(receipt_value) not in (
        common_fields | {"relations"},
        common_fields | legacy_fields,
    ):
        raise NpiResultArchiveError("NPI validation receipt is invalid")
    is_legacy = "relation_oids" in receipt_value
    if (
        receipt_value["contract"] != VALIDATION_CONTRACT
        or receipt_value["profile_contract"] not in (CONTRACT, *MODEL_CONTRACTS)
        or any(
            _SHA256.fullmatch(str(receipt_value[key])) is None
            for key in ("package_id", "manifest_sha256", "validation_sha256")
        )
    ):
        raise NpiResultArchiveError("NPI validation receipt is invalid")
    schema = _schema_name(receipt_value["stage_schema"])
    is_canonical = receipt_value["profile_contract"] in MODEL_CONTRACTS
    relations = _validation_inventory(
        receipt_value["relation_oids" if is_legacy else "relations"], canonical=is_canonical
    )
    if not is_legacy and any(entry["schema_name"] != schema for entry in receipt_value["relations"]):
        raise NpiResultArchiveError("NPI validation inventory is invalid")
    tables = _validate_tables(receipt_value["tables"], canonical=is_canonical)
    if not is_legacy and receipt_value["validation_sha256"] != _validation_digest(
        {key: field_value for key, field_value in receipt_value.items() if key != "validation_sha256"}
    ):
        raise NpiResultArchiveError("NPI validation digest differs")
    return NpiValidationReceipt(
        receipt_value["package_id"],
        schema,
        None,
        relations,
        None,
        receipt_value["manifest_sha256"],
        tables,
        receipt_value["validation_sha256"],
        receipt_value["profile_contract"],
        json.loads(_canonical_json(receipt_value)) if is_legacy else None,
    )


async def prepare_npi_activation(
    session: Any,
    *,
    ownership: NpiStageOwnership,
    manifest: Mapping[str, Any] | NpiResultManifest,
    package_id: str,
    sealed_owner_oid: int | None = None,
    npi_alias_authority=None,
) -> NpiValidationReceipt:
    """Perform long validation over an already publisher-frozen stage."""

    _require_transaction(session)
    validated = validate_npi_result_manifest(manifest)
    if _SHA256.fullmatch(str(package_id)) is None:
        raise NpiResultArchiveError("NPI validation package identity is invalid")
    ownership = await verify_npi_stage_ownership(session, ownership)
    await _verify_stage_sequences(session, ownership)
    tables = await _validate_stage_manifest(
        session,
        ownership,
        validated,
        **({"npi_alias_authority": npi_alias_authority} if npi_alias_authority is not None else {}),
    )
    digest_by_field = {
        "contract": VALIDATION_CONTRACT,
        "package_id": package_id,
        "profile_contract": validated.contract,
        "stage_schema": ownership.schema_name,
        "relations": [
            {"schema_name": ownership.schema_name, "relation_name": name} for name, _ in ownership.relation_oids
        ],
        "manifest_sha256": _manifest_digest(validated),
        "tables": [table.as_dict() for table in tables],
    }
    return validate_npi_validation_receipt(
        {**digest_by_field, "validation_sha256": _validation_digest(digest_by_field)}
    )


async def capture_npi_incumbent(session: Any, *, schema_name: str, canonical=False) -> NpiIncumbent:
    """Capture all-present or all-absent live relation OIDs for later CAS."""

    _require_transaction(session)
    schema = _schema_name(schema_name)
    async with _bounded_catalog_work(session):
        pairs = await _relation_pairs(session, schema, npi_archive_names(canonical=canonical))
        presence_flags = [relation_oid is not None for name, relation_oid in pairs if name != CANONICAL_TABLE]
        if any(presence_flags) and not all(presence_flags):
            raise NpiResultArchiveError("NPI incumbent is incomplete")
        if all(presence_flags):
            await _lock_family(session, schema, "ACCESS SHARE")
            if await _relation_pairs(session, schema, npi_archive_names(canonical=canonical)) != pairs:
                raise NpiResultArchiveError("NPI incumbent changed during capture")
    return NpiIncumbent(schema, tuple((name, oid is not None) for name, oid in pairs))


async def _lock_and_verify_activation(
    session: Any,
    ownership: NpiStageOwnership,
    incumbent: NpiIncumbent,
    *,
    stage_read_only: bool = False,
) -> NpiStageOwnership:
    lock_mode = "ACCESS SHARE" if stage_read_only else "ACCESS EXCLUSIVE"
    async with _bounded_catalog_work(session):
        await _lock_family(
            session,
            ownership.schema_name,
            lock_mode,
            names=npi_archive_names(canonical=_has_canonical(ownership)),
        )
        present_names = tuple(name for name, present in incumbent.relations if present)
        if present_names:
            await _lock_family(session, incumbent.schema_name, lock_mode, names=present_names)
        ownership = await verify_npi_stage_ownership(session, ownership)
        if (
            tuple(
                (name, oid is not None)
                for name, oid in await _relation_pairs(
                    session, incumbent.schema_name, tuple(name for name, _ in incumbent.relations)
                )
            )
            != incumbent.relations
        ):
            raise NpiResultArchiveError("NPI incumbent changed")
    return ownership


async def _has_populated_incumbent(session: Any, incumbent: NpiIncumbent) -> bool:
    if not all(present for name, present in incumbent.relations if name != CANONICAL_TABLE):
        return False
    for table_name in RELATION_NAMES:
        populated = await session.scalar(
            text(f"SELECT EXISTS (SELECT 1 FROM {_quoted(incumbent.schema_name)}.{_quoted(table_name)} LIMIT 1)")
        )
        if populated:
            return True
    return False


async def _rotate_relations(
    session: Any,
    ownership: NpiStageOwnership,
    incumbent: NpiIncumbent,
) -> str | None:
    incumbent_by_name = dict(incumbent.relations)
    predecessor_schema = None
    if any(incumbent_by_name.values()):
        predecessor_schema = npi_predecessor_schema(ownership.dataset_id)
        await session.execute(text(f"CREATE SCHEMA {_quoted(predecessor_schema)}"))
    for table_name, _oid in ownership.relation_oids:
        if incumbent_by_name[table_name]:
            await session.execute(
                text(
                    f"ALTER TABLE {_quoted(incumbent.schema_name)}.{_quoted(table_name)} "
                    f"SET SCHEMA {_quoted(predecessor_schema)}"
                )
            )
        await session.execute(
            text(
                f"ALTER TABLE {_quoted(ownership.schema_name)}.{_quoted(table_name)} "
                f"SET SCHEMA {_quoted(incumbent.schema_name)}"
            )
        )
    remaining = await session.scalar(
        text("SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=:schema_oid"),
        {"schema_oid": await _schema_oid(session, ownership.schema_name)},
    )
    if int(remaining or 0):
        raise NpiResultArchiveError("NPI stage schema is not empty after activation")
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))
    return predecessor_schema


async def _remove_npi_clone_freeze(session: Any, ownership: NpiStageOwnership) -> None:
    for table_name in RELATION_NAMES:
        relation = f"{_quoted(ownership.schema_name)}.{_quoted(table_name)}"
        await session.execute(text(f"DROP TRIGGER IF EXISTS {_quoted(_FREEZE_WRITE_TRIGGER)} ON {relation}"))
        await session.execute(text(f"DROP TRIGGER IF EXISTS {_quoted(_FREEZE_TRUNCATE_TRIGGER)} ON {relation}"))
    await session.execute(
        text(f"DROP FUNCTION IF EXISTS {_quoted(ownership.schema_name)}.{_quoted(_FREEZE_FUNCTION)}() RESTRICT")
    )


def _validate_cutover_bindings(
    ownership: NpiStageOwnership,
    manifest: NpiResultManifest,
    validation: NpiValidationReceipt,
    cutover: NpiCutoverAuthority,
) -> None:
    if (
        validation.package_id != cutover.package_id
        or validation.stage_schema != ownership.schema_name
        or tuple(name for name, _ in validation.relation_oids) != tuple(name for name, _ in ownership.relation_oids)
        or validation.manifest_sha256 != _manifest_digest(manifest)
        or validation.tables != manifest.tables
        or validation.profile_contract != manifest.contract
    ):
        raise NpiResultArchiveError("NPI validation authority differs")


async def _admit_automatic_cutover(
    session: Any,
    *,
    incumbent: NpiIncumbent,
    current_authority: NpiResultGenerationAuthority,
    source_generation: NpiServingGeneration | None,
) -> None:
    if source_generation is None:
        raise NpiResultArchiveError("NPI automatic source generation is unavailable")
    if tuple(name for name, _oid in incumbent.relations) not in (
        RELATION_NAMES,
        npi_archive_names(canonical=True),
    ):
        raise NpiResultArchiveError("NPI incumbent generation inventory is invalid")
    if current_authority.serving_generation is None:
        if await _has_populated_incumbent(session, incumbent):
            raise NpiResultArchiveError("NPI legacy incumbent requires manual adoption")
        return
    if current_authority.relations != tuple((incumbent.schema_name, name) for name in RELATION_NAMES):
        raise NpiResultArchiveError("NPI incumbent generation name differs")
    try:
        require_npi_automatic_generation_order(
            source_generation,
            current_authority.serving_generation,
        )
    except ValueError as error:
        raise NpiResultArchiveError("NPI automatic generation is stale or unrelated") from error


async def _preserve_npi_input_reads(session, ownership, incumbent):
    from process.native_publication_read_access import preserve_publication_reads

    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    for name in RELATION_NAMES:
        await preserve_publication_reads(
            driver,
            schema=incumbent.schema_name,
            live_table=name,
            stage_schema=ownership.schema_name,
            stage_table=name,
        )


async def _activate_npi_relations(
    session: Any,
    *,
    ownership: NpiStageOwnership,
    incumbent: NpiIncumbent,
    manifest: NpiResultManifest,
    validation: NpiValidationReceipt,
    stage_read_only: bool = False,
) -> NpiActivationReceipt:
    await _preserve_npi_input_reads(session, ownership, incumbent)
    if not stage_read_only:
        await _remove_npi_clone_freeze(session, ownership)
        await install_npi_stage_mutation_guards(
            session,
            schema_name=ownership.schema_name,
            stage_tables=RELATION_NAMES,
            function_schema_name=incumbent.schema_name,
        )
    async with _bounded_catalog_work(session) if stage_read_only else nullcontext():
        predecessor_schema = await _rotate_relations(session, ownership, incumbent)
    live_pairs = await _relation_pairs(
        session, incumbent.schema_name, npi_archive_names(canonical=_has_canonical(ownership))
    )
    if tuple(sorted(name for name, _ in live_pairs)) != tuple(name for name, _ in ownership.relation_oids) or any(
        relation_oid is None for _, relation_oid in live_pairs
    ):
        raise NpiResultArchiveError("NPI activated relation identity differs")
    canonical_publication = None
    if manifest.contract in MODEL_CONTRACTS:
        canonical_publication = await merge_canonical_contribution(
            session,
            "npi",
            f"{_quoted(incumbent.schema_name)}.{_quoted(CANONICAL_TABLE)}",
            f"{_quoted(incumbent.schema_name)}.{_quoted('address_archive_v2')}",
        )
    adopted = await publish_adopted_npi_result_generation(
        session,
        schema_name=incumbent.schema_name,
        source_generation=manifest.source_serving_generation,
        canonical_provenance=None if manifest.contract in MODEL_CONTRACTS else manifest.canonical_provenance,
    )
    if manifest.source_serving_generation is None:
        if adopted.serving_generation is not None or adopted.relations is not None:
            raise NpiResultArchiveError("NPI generation-less adoption differs")
    elif adopted.serving_generation != manifest.source_serving_generation or adopted.relations != tuple(
        (incumbent.schema_name, name) for name in RELATION_NAMES
    ):
        raise NpiResultArchiveError("NPI adopted generation differs")
    await mark_procedure_taxonomy_inputs_changed(
        incumbent.schema_name, lambda statement: session.execute(text(statement)), "npi_taxonomy"
    )
    return NpiActivationReceipt(
        tuple((incumbent.schema_name, name) for name, _ in live_pairs),
        tuple((predecessor_schema, name) for name, present in incumbent.relations if present),
        predecessor_schema,
        validation.tables,
        adopted,
        canonical_publication,
    )


async def activate_validated_npi_stage(
    session: Any,
    *,
    ownership: NpiStageOwnership,
    manifest: Mapping[str, Any] | NpiResultManifest,
    incumbent: NpiIncumbent,
    validation_receipt: Mapping[str, Any] | NpiValidationReceipt,
    cutover: NpiCutoverAuthority,
    on_activated: Callable[[Any, NpiActivationReceipt], Awaitable[None]],
) -> NpiActivationReceipt:
    """CAS-rotate one validated family and install its record in one transaction."""

    _require_transaction(session)
    if not isinstance(ownership, NpiStageOwnership) or not isinstance(incumbent, NpiIncumbent):
        raise NpiResultArchiveError("NPI activation ownership is invalid")
    if not isinstance(cutover, NpiCutoverAuthority) or cutover.authority not in {
        "manual",
        "automatic",
    }:
        raise NpiResultArchiveError("NPI activation authority is unsupported")
    if not callable(on_activated):
        raise NpiResultArchiveError("NPI activation record callback is required")
    validated_manifest = validate_npi_result_manifest(manifest)
    if validated_manifest.contract == ALIAS_MODEL_CONTRACT:
        raise NpiResultArchiveError("NPI alias output requires constructor-owned activation")
    validation = validate_npi_validation_receipt(validation_receipt)
    _validate_cutover_bindings(ownership, validated_manifest, validation, cutover)
    if tuple(name for name, _oid in incumbent.relations) != npi_archive_names(
        canonical=validated_manifest.contract in MODEL_CONTRACTS
    ):
        raise NpiResultArchiveError("NPI incumbent activation inventory differs")
    ownership = await _lock_and_verify_activation(
        session, ownership, incumbent, stage_read_only=cutover.stage_read_only
    )
    await _verify_stage_sequences(session, ownership)
    current_authority = await read_npi_result_generation_authority(
        session,
        schema_name=incumbent.schema_name,
        lock=True,
    )
    source_generation = validated_manifest.source_serving_generation
    if cutover.authority == "automatic":
        await _admit_automatic_cutover(
            session,
            incumbent=incumbent,
            current_authority=current_authority,
            source_generation=source_generation,
        )
    receipt = await _activate_npi_relations(
        session,
        ownership=ownership,
        incumbent=incumbent,
        manifest=validated_manifest,
        validation=validation,
        stage_read_only=cutover.stage_read_only,
    )
    await on_activated(session, receipt)
    return receipt


__all__ = [
    "CONTRACT",
    "ALIAS_MODEL_CONTRACT",
    "MODEL_CONTRACT",
    "MODEL_CONTRACTS",
    "VALIDATION_CONTRACT",
    "NpiActivationReceipt",
    "NpiCutoverAuthority",
    "NpiIncumbent",
    "NpiPreparedSource",
    "NpiResultArchiveError",
    "NpiResultManifest",
    "NpiSourceCapture",
    "NpiStageCapture",
    "NpiStageOwnership",
    "NpiTableReceipt",
    "NpiValidationReceipt",
    "activate_validated_npi_stage",
    "capture_npi_incumbent",
    "capture_npi_alias_manifest",
    "capture_npi_source",
    "capture_npi_stage_ownership",
    "cleanup_npi_stage",
    "complete_npi_restore",
    "export_npi_archive",
    "export_prepared_npi_archive",
    "freeze_npi_stage",
    "npi_predecessor_schema",
    "npi_stage_schema",
    "precreate_npi_restore",
    "prepare_npi_activation",
    "prepare_npi_archive_source",
    "validate_npi_result_manifest",
    "validate_npi_stage",
    "validate_npi_validation_receipt",
    "verify_npi_stage_ownership",
]
