# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native archive mechanics for the exact six-table NPI serving family.

Source capture uses a PostgreSQL MVCC snapshot and ``ACCESS SHARE`` locks, so
ordinary coordinate updates are not held behind the potentially long clone.
Generation-less captures are explicitly manual legacy snapshots.  Establishing
generation authority is a separate bootstrap operation and never happens here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable, MetaData

from db import models
from process import entity_address_snapshot_receipt as catalog_identity
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

CONTRACT = "npi-result-family.postgres.v1"
VALIDATION_CONTRACT = "npi-result-family.validation.v1"
_STAGE_PREFIX = "npi_result_archive_"
_PREDECESSOR_PREFIX = "npi_result_predecessor_"
_LOCK_TIMEOUT = "500ms"
_CAPTURE_TIMEOUT = "5s"
_FREEZE_FUNCTION = "reject_npi_result_archive_mutation"
_FREEZE_WRITE_TRIGGER = "npi_result_archive_write_guard"
_FREEZE_TRUNCATE_TRIGGER = "npi_result_archive_truncate_guard"
_FREEZE_FUNCTION_BODY = "BEGIN RAISE EXCEPTION 'npi_result_archive_is_frozen' USING ERRCODE='55000'; END;"
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

    def as_dict(self) -> dict[str, Any]:
        """Return the strict portable archive manifest."""

        return {
            "contract": CONTRACT,
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
        """Return the exact serializable relation, sequence, and freeze token."""

        return {
            "dataset_id": str(self.dataset_id),
            "schema_name": self.schema_name,
            "schema_oid": self.schema_oid,
            "relation_oids": [list(pair) for pair in self.relation_oids],
            "sequence_oids": [list(entry) for entry in self.sequence_oids],
            "freeze_function_oid": self.freeze_function_oid,
            "freeze_trigger_oids": [list(entry) for entry in self.freeze_trigger_oids],
            "freeze_catalog_versions": [list(entry) for entry in self.freeze_catalog_versions],
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
    relation_oids: tuple[tuple[str, int | None], ...]


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

    def as_dict(self) -> dict[str, Any]:
        """Return the closed durable validation representation."""

        return {
            "contract": VALIDATION_CONTRACT,
            "package_id": self.package_id,
            "profile_contract": CONTRACT,
            "stage_schema": self.stage_schema,
            "stage_schema_oid": self.stage_schema_oid,
            "relation_oids": [list(pair) for pair in self.relation_oids],
            "sealed_owner_oid": self.sealed_owner_oid,
            "manifest_sha256": self.manifest_sha256,
            "tables": [table.as_dict() for table in self.tables],
            "validation_sha256": self.validation_sha256,
        }


@dataclass(frozen=True)
class NpiCutoverAuthority:
    """Trusted controller bindings rechecked during the short cutover."""

    package_id: str
    sealed_owner_oid: int
    expected_stage_owner_oid: int
    authority: str


@dataclass(frozen=True)
class NpiActivationReceipt:
    """Destination-local identity captured before activation commits."""

    relation_oids: tuple[tuple[str, int], ...]
    predecessor_oids: tuple[tuple[str, int | None], ...]
    predecessor_schema_name: str | None
    tables: tuple[NpiTableReceipt, ...]
    adopted_authority: NpiResultGenerationAuthority


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
    try:
        yield
    finally:
        await _set_local_timeout(session, "lock_timeout", previous_lock)
        await _set_local_timeout(session, "statement_timeout", previous_statement)


async def _lock_family(session: Any, schema_name: str, mode: str, *, nowait: bool = False) -> None:
    relations = ", ".join(f"{_quoted(schema_name)}.{_quoted(table_name)}" for table_name in RELATION_NAMES)
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
) -> tuple[tuple[str, int | None], ...]:
    relation_pairs = []
    for table_name in RELATION_NAMES:
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
    columns = await catalog_identity._catalog_columns(session, relation_oid)
    constraints = await catalog_identity._catalog_constraints(
        session,
        relation_oid,
        schema_name,
    )
    indexes = await catalog_identity._catalog_indexes(session, relation_oid)
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
    catalog_identity._reject_schema_qualified_expressions(
        schema_name,
        columns,
        constraints,
        indexes,
    )
    return catalog_identity._canonical_digest(
        {
            "table_name": table_name,
            "columns": columns,
            "constraints": constraints,
            "indexes": indexes,
        }
    )


async def _manifest_tables(session: Any, schema_name: str) -> tuple[NpiTableReceipt, ...]:
    return tuple(
        [await _table_receipt(session, schema_name=schema_name, model_type=model_type) for model_type in _MODEL_TYPES]
    )


def _validate_tables(value: object) -> tuple[NpiTableReceipt, ...]:
    if not isinstance(value, list) or len(value) != len(_MODEL_TYPES):
        raise NpiResultArchiveError("NPI archive table set is invalid")
    receipts = []
    for raw_table, model_type in zip(value, _MODEL_TYPES, strict=True):
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
    if manifest_value["contract"] != CONTRACT or manifest_value["capture_authority"] not in {
        "tracked-generation",
        "legacy-manual",
    }:
        raise NpiResultArchiveError("NPI archive authority classification is invalid")
    metadata, metadata_sha256 = _source_metadata(manifest_value["source_metadata"])
    tables = _validate_tables(manifest_value["tables"])
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
    )


async def capture_npi_source(
    session: Any,
    *,
    schema_name: str,
    source_metadata: Mapping[str, Any] | None,
    source_metadata_factory: Callable[[Any], Awaitable[Mapping[str, Any]]] | None = None,
) -> NpiSourceCapture:
    """Capture one coherent live family without blocking routine DML."""

    _require_transaction(session)
    if (source_metadata is None) == (source_metadata_factory is None):
        raise NpiResultArchiveError("NPI archive requires exactly one source metadata input")
    schema = _schema_name(schema_name)
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    async with _bounded_catalog_work(session):
        await _lock_family(session, schema, "ACCESS SHARE")
        authority = await read_npi_result_generation_authority(
            session,
            schema_name=schema,
        )
        pairs = await _relation_pairs(session, schema)
        if any(relation_oid is None for _, relation_oid in pairs):
            raise NpiResultArchiveError("NPI source family is incomplete")
        current_oids = tuple(int(relation_oid) for _, relation_oid in pairs)
        if authority.serving_generation is None:
            if authority.relation_oids is not None:
                raise NpiResultArchiveError("NPI source generation is incomplete")
            capture_authority = "legacy-manual"
        elif authority.relation_oids != current_oids:
            raise NpiResultArchiveError("NPI source generation is drifted")
        else:
            capture_authority = "tracked-generation"
        captured_metadata = (
            await source_metadata_factory(session) if source_metadata_factory is not None else source_metadata
        )
        metadata, metadata_sha256 = _source_metadata(captured_metadata)
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
    )


async def _bind_independent_stage_sequences(
    session: Any,
    *,
    source_schema: str,
    stage_schema: str,
) -> None:
    """Replace copied source defaults with stage-owned sequence dependencies."""

    source_sequences = await _source_family_sequences(session, source_schema)
    stage_oid = await _schema_oid(session, stage_schema)
    stage_sequences_by_owner = {
        (owner_table, owner_column): (sequence_name, sequence_oid)
        for sequence_name, sequence_oid, owner_table, owner_column in await _owned_sequences(session, stage_oid)
    }
    for source_sequence_name, _source_sequence_oid, owner_table, owner_column in source_sequences:
        stage_sequence_name = await _ensure_stage_owned_sequence(
            session,
            stage_schema=stage_schema,
            source_sequence_name=source_sequence_name,
            owner_table=owner_table,
            owner_column=owner_column,
            stage_sequences_by_owner=stage_sequences_by_owner,
        )
        await _advance_stage_sequence(
            session,
            stage_schema=stage_schema,
            sequence_name=stage_sequence_name,
            owner_table=owner_table,
            owner_column=owner_column,
        )
    await _verify_stage_sequence_owners(session, stage_oid, source_sequences)


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

    maximum = await session.scalar(
        text(f"SELECT max({_quoted(owner_column)})::bigint FROM {_quoted(stage_schema)}.{_quoted(owner_table)}")
    )
    if maximum is not None:
        stage_sequence = f"{_quoted(stage_schema)}.{_quoted(sequence_name)}"
        await session.execute(
            text(f"SELECT pg_catalog.setval('{stage_sequence}'::regclass,:maximum,true)"),
            {"maximum": int(maximum)},
        )
        return int(maximum)
    return None


async def _advance_and_verify_stage_sequences(
    session: Any,
    ownership: NpiStageOwnership,
) -> None:
    """Align every exact owned sequence with its restored owner column."""

    observed_sequences = await _owned_sequences(session, ownership.schema_oid)
    if observed_sequences != ownership.sequence_oids:
        raise NpiResultArchiveError("NPI archive stage sequence ownership differs")
    for sequence_name, sequence_oid, owner_table, owner_column in observed_sequences:
        maximum = await _advance_stage_sequence(
            session,
            stage_schema=ownership.schema_name,
            sequence_name=sequence_name,
            owner_table=owner_table,
            owner_column=owner_column,
        )
        stage_sequence = f"{_quoted(ownership.schema_name)}.{_quoted(sequence_name)}"
        if maximum is None:
            minimum = await session.scalar(
                text("SELECT seqmin::bigint FROM pg_catalog.pg_sequence WHERE seqrelid=:sequence_oid"),
                {"sequence_oid": sequence_oid},
            )
            if type(minimum) is not int:
                raise NpiResultArchiveError("NPI archive stage sequence state is unavailable")
            await session.execute(
                text(f"SELECT pg_catalog.setval('{stage_sequence}'::regclass,:minimum,false)"),
                {"minimum": minimum},
            )
            expected_value = minimum
            is_expected_called = False
        else:
            expected_value = maximum
            is_expected_called = True
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


async def _clone_source(session: Any, capture: NpiSourceCapture, stage_schema: str) -> None:
    if _SNAPSHOT.fullmatch(capture.postgres_snapshot) is None:
        raise NpiResultArchiveError("NPI source snapshot is invalid")
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(f"SET TRANSACTION SNAPSHOT '{capture.postgres_snapshot}'"))
    await session.execute(text(f"CREATE SCHEMA {_quoted(stage_schema)}"))
    for table_name in RELATION_NAMES:
        source = f"{_quoted(capture.schema_name)}.{_quoted(table_name)}"
        stage = f"{_quoted(stage_schema)}.{_quoted(table_name)}"
        await session.execute(text(f"CREATE TABLE {stage} (LIKE {source} INCLUDING ALL)"))
        await session.execute(text(f"INSERT INTO {stage} SELECT * FROM {source}"))
    await _bind_independent_stage_sequences(
        session,
        source_schema=capture.schema_name,
        stage_schema=stage_schema,
    )


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


async def _freeze_seal(
    session: Any,
    schema_oid: int,
) -> tuple[
    int | None,
    tuple[tuple[str, int, int], ...],
    tuple[tuple[str, int, str, str], ...],
]:
    """Read and validate the exact always-enabled immutable-clone guard set."""

    function_row = await _read_freeze_function(session, schema_oid)
    trigger_rows = await _read_freeze_triggers(session, schema_oid)
    if function_row is None and not trigger_rows:
        return None, (), ()
    if function_row is None:
        raise NpiResultArchiveError("NPI archive freeze function differs")
    function_oid = _validate_freeze_function(function_row)
    triggers_by_table = _validate_freeze_triggers(trigger_rows, function_oid)
    # OIDs and final definitions alone do not detect disable/enable or an
    # in-place CREATE OR REPLACE. Bind catalog tuple versions as local seals;
    # ctid also detects changes within the preparing transaction. Do not bind
    # cmin: its header slot is reused by cmax, even after a rolled-back deletion.
    catalog_versions = tuple(
        sorted(
            [("function", function_oid, function_row["xmin"], function_row["ctid"])]
            + [
                ("trigger", int(trigger_row["oid"]), trigger_row["xmin"], trigger_row["ctid"])
                for trigger_row in trigger_rows
            ]
        )
    )
    return (
        function_oid,
        tuple(
            (
                table_name,
                triggers_by_table[table_name][_FREEZE_WRITE_TRIGGER],
                triggers_by_table[table_name][_FREEZE_TRUNCATE_TRIGGER],
            )
            for table_name in sorted(RELATION_NAMES)
        ),
        catalog_versions,
    )


async def _read_freeze_function(
    session: Any,
    schema_oid: int,
) -> Mapping[str, Any] | None:
    """Read the stage-local function used to freeze one NPI clone."""

    return (
        (
            await session.execute(
                text(
                    "SELECT function.oid,function.prosrc,function.prosecdef,"
                    "function.xmin::text AS xmin,"
                    "function.ctid::text AS ctid,"
                    "function.proconfig,language.lanname "
                    "FROM pg_catalog.pg_proc AS function "
                    "JOIN pg_catalog.pg_language AS language "
                    "ON language.oid=function.prolang "
                    "WHERE function.pronamespace=:schema_oid "
                    "AND function.proname=:function_name "
                    "AND function.pronargs=0 "
                    "AND function.prorettype='pg_catalog.trigger'::regtype"
                ),
                {
                    "schema_oid": schema_oid,
                    "function_name": _FREEZE_FUNCTION,
                },
            )
        )
        .mappings()
        .one_or_none()
    )


async def _read_freeze_triggers(
    session: Any,
    schema_oid: int,
) -> list[Mapping[str, Any]]:
    """Read candidate immutable-clone guard triggers from one stage schema."""

    return list(
        (
            await session.execute(
                text(
                    "SELECT owner_table.relname AS table_name,guard.oid,"
                    "guard.tgname,guard.tgenabled::text AS tgenabled,"
                    "guard.tgtype,guard.tgfoid,guard.tgqual IS NULL AS unconditional,"
                    "guard.tgnargs,guard.tgattr::text AS trigger_columns,"
                    "guard.xmin::text AS xmin,"
                    "guard.ctid::text AS ctid "
                    "FROM pg_catalog.pg_trigger AS guard "
                    "JOIN pg_catalog.pg_class AS owner_table "
                    "ON owner_table.oid=guard.tgrelid "
                    "WHERE owner_table.relnamespace=:schema_oid "
                    "AND NOT guard.tgisinternal "
                    "AND guard.tgname IN (:write_trigger,:truncate_trigger) "
                    "ORDER BY owner_table.relname,guard.tgname"
                ),
                {
                    "schema_oid": schema_oid,
                    "write_trigger": _FREEZE_WRITE_TRIGGER,
                    "truncate_trigger": _FREEZE_TRUNCATE_TRIGGER,
                },
            )
        ).mappings()
    )


def _validate_freeze_function(function_row: Mapping[str, Any]) -> int:
    """Return a freeze-function OID only when its immutable contract matches."""

    if (
        function_row["lanname"] != "plpgsql"
        or function_row["prosecdef"] is not True
        or " ".join(str(function_row["prosrc"]).split()) != _FREEZE_FUNCTION_BODY
        or "search_path=pg_catalog" not in tuple(function_row["proconfig"] or ())
    ):
        raise NpiResultArchiveError("NPI archive freeze function differs")
    return int(function_row["oid"])


def _validate_freeze_triggers(
    trigger_rows: list[Mapping[str, Any]],
    function_oid: int,
) -> dict[str, dict[str, int]]:
    """Require complete, always-enabled write and truncate guards per table."""

    triggers_by_table: dict[str, dict[str, int]] = {}
    for trigger_row in trigger_rows:
        table_name = str(trigger_row["table_name"])
        trigger_name = str(trigger_row["tgname"])
        expected_type = 30 if trigger_name == _FREEZE_WRITE_TRIGGER else 34
        if (
            table_name not in RELATION_NAMES
            or trigger_row["tgenabled"] != "A"
            or int(trigger_row["tgtype"]) != expected_type
            or int(trigger_row["tgfoid"]) != function_oid
            or trigger_row["unconditional"] is not True
            or trigger_row["tgnargs"] != 0
            or trigger_row["trigger_columns"] != ""
            or trigger_name in triggers_by_table.setdefault(table_name, {})
        ):
            raise NpiResultArchiveError("NPI archive freeze trigger differs")
        triggers_by_table[table_name][trigger_name] = int(trigger_row["oid"])
    if set(triggers_by_table) != set(RELATION_NAMES) or any(
        set(trigger_by_name) != {_FREEZE_WRITE_TRIGGER, _FREEZE_TRUNCATE_TRIGGER}
        for trigger_by_name in triggers_by_table.values()
    ):
        raise NpiResultArchiveError("NPI archive freeze trigger set differs")
    return triggers_by_table


async def capture_npi_stage_ownership(
    session: Any,
    *,
    dataset_id: UUID,
) -> NpiStageOwnership:
    """Capture exact identities for a complete, otherwise closed stage."""

    _require_transaction(session)
    schema_name = npi_stage_schema(dataset_id)
    schema_oid = await _schema_oid(session, schema_name)
    relation_oids = []
    for table_name in sorted(RELATION_NAMES):
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
    freeze_function_oid, freeze_trigger_oids, freeze_catalog_versions = await _freeze_seal(
        session,
        schema_oid,
    )
    return NpiStageOwnership(
        dataset_id,
        schema_name,
        schema_oid,
        tuple(relation_oids),
        sequence_oids,
        freeze_function_oid,
        freeze_trigger_oids,
        freeze_catalog_versions,
    )


async def _freeze_npi_clone(
    session: Any,
    ownership: NpiStageOwnership,
) -> NpiStageOwnership:
    """Install immutable clone guards before durable prepared authority exists."""

    if ownership.freeze_function_oid is not None or ownership.freeze_trigger_oids or ownership.freeze_catalog_versions:
        raise NpiResultArchiveError("NPI archive clone is already frozen")
    async with _bounded_catalog_work(session):
        await _lock_family(
            session,
            ownership.schema_name,
            "ACCESS EXCLUSIVE",
        )
        await verify_npi_stage_ownership(session, ownership)
        freeze_function = f"{_quoted(ownership.schema_name)}.{_quoted(_FREEZE_FUNCTION)}"
        await session.execute(
            text(
                f"CREATE FUNCTION {freeze_function}() RETURNS trigger "
                "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog "
                f"AS $function$ {_FREEZE_FUNCTION_BODY} $function$"
            )
        )
        await session.execute(text(f"REVOKE ALL ON FUNCTION {freeze_function}() FROM PUBLIC"))
        for table_name in RELATION_NAMES:
            await _install_freeze_triggers(
                session,
                schema_name=ownership.schema_name,
                table_name=table_name,
                freeze_function=freeze_function,
            )
    frozen = await capture_npi_stage_ownership(
        session,
        dataset_id=ownership.dataset_id,
    )
    if (
        frozen.freeze_function_oid is None
        or not frozen.freeze_trigger_oids
        or replace(
            frozen,
            freeze_function_oid=None,
            freeze_trigger_oids=(),
            freeze_catalog_versions=(),
        )
        != ownership
    ):
        raise NpiResultArchiveError("NPI archive clone freeze differs")
    return frozen


async def freeze_npi_stage(
    session: Any,
    *,
    ownership: NpiStageOwnership,
) -> NpiStageOwnership:
    """Freeze one exact, initially unfrozen stage in the caller transaction."""

    _require_transaction(session)
    if not isinstance(ownership, NpiStageOwnership):
        raise NpiResultArchiveError("NPI archive stage ownership is invalid")
    if ownership.freeze_function_oid is not None or ownership.freeze_trigger_oids or ownership.freeze_catalog_versions:
        raise NpiResultArchiveError("NPI archive clone is already frozen")
    await verify_npi_stage_ownership(session, ownership)
    return await _freeze_npi_clone(session, ownership)


async def _install_freeze_triggers(
    session: Any,
    *,
    schema_name: str,
    table_name: str,
    freeze_function: str,
) -> None:
    """Install and force the write and truncate freeze guards for one table."""

    relation = f"{_quoted(schema_name)}.{_quoted(table_name)}"
    await session.execute(
        text(
            f"CREATE TRIGGER {_quoted(_FREEZE_WRITE_TRIGGER)} "
            f"BEFORE INSERT OR UPDATE OR DELETE ON {relation} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {freeze_function}()"
        )
    )
    await session.execute(text(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_quoted(_FREEZE_WRITE_TRIGGER)}"))
    await session.execute(
        text(
            f"CREATE TRIGGER {_quoted(_FREEZE_TRUNCATE_TRIGGER)} "
            f"BEFORE TRUNCATE ON {relation} FOR EACH STATEMENT "
            f"EXECUTE FUNCTION {freeze_function}()"
        )
    )
    await session.execute(text(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_quoted(_FREEZE_TRUNCATE_TRIGGER)}"))


async def verify_npi_stage_ownership(
    session: Any,
    ownership: NpiStageOwnership,
) -> NpiStageOwnership:
    """Recheck one local owner token against current catalog identities."""

    if not isinstance(ownership, NpiStageOwnership):
        raise NpiResultArchiveError("NPI archive stage ownership is invalid")
    observed = await capture_npi_stage_ownership(session, dataset_id=ownership.dataset_id)
    if observed != ownership:
        raise NpiResultArchiveError("NPI archive stage ownership differs")
    return observed


async def _validate_stage_manifest(
    session: Any,
    ownership: NpiStageOwnership,
    manifest: NpiResultManifest,
    *,
    ownership_verified: bool = False,
) -> tuple[NpiTableReceipt, ...]:
    if not ownership_verified:
        await verify_npi_stage_ownership(session, ownership)
    observed_tables = await _manifest_tables(session, ownership.schema_name)
    if observed_tables != manifest.tables:
        raise NpiResultArchiveError("NPI restored stage differs")
    return observed_tables


async def cleanup_npi_stage(session: Any, ownership: NpiStageOwnership) -> None:
    """Drop only an unchanged UUID-owned stage using restrictive DDL."""

    _require_transaction(session)
    current_schema_oid = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": ownership.schema_name},
    )
    if current_schema_oid is None:
        return
    async with _bounded_catalog_work(session):
        await _lock_family(session, ownership.schema_name, "ACCESS EXCLUSIVE", nowait=True)
        await verify_npi_stage_ownership(session, ownership)
    relations = ", ".join(
        f"{_quoted(ownership.schema_name)}.{_quoted(table_name)}" for table_name, _ in ownership.relation_oids
    )
    await session.execute(text(f"DROP TABLE {relations} RESTRICT"))
    remaining = await session.scalar(
        text("SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=:schema_oid"),
        {"schema_oid": ownership.schema_oid},
    )
    if int(remaining or 0):
        raise NpiResultArchiveError("NPI archive owned schema is not empty")
    if ownership.freeze_function_oid is not None:
        await session.execute(text(f"DROP FUNCTION {_quoted(ownership.schema_name)}.{_quoted(_FREEZE_FUNCTION)}()"))
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))


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
) -> NpiPreparedSource:
    """Clone once and persist its exact owner before clone commit."""

    if not callable(on_prepared):
        raise NpiResultArchiveError("NPI prepared-source callback is required")
    stage_schema = npi_stage_schema(dataset_id)
    async with session_factory() as source_session, source_session.begin():
        capture = await capture_npi_source(
            source_session,
            schema_name=schema_name,
            source_metadata=source_metadata,
            source_metadata_factory=source_metadata_factory,
        )
        async with session_factory() as clone_session, clone_session.begin():
            await _clone_source(clone_session, capture, stage_schema)
            ownership = await capture_npi_stage_ownership(
                clone_session,
                dataset_id=dataset_id,
            )
            tables = await _manifest_tables(clone_session, stage_schema)
            manifest = NpiResultManifest(
                tables,
                capture.source_metadata,
                capture.source_metadata_sha256,
                _schema_digest(tables),
                capture.capture_authority,
                capture.source_serving_generation,
                capture.canonical_provenance,
            )
            ownership = await freeze_npi_stage(clone_session, ownership=ownership)
            prepared = NpiPreparedSource(manifest, ownership)
            await on_prepared(clone_session, prepared)
    return prepared


async def export_prepared_npi_archive(
    session_factory: Any,
    *,
    prepared: NpiPreparedSource,
    archive_copy: Callable[[NpiStageCapture], Awaitable[None]],
) -> NpiResultManifest:
    """Dump one committed frozen clone without recapturing live content."""

    if not isinstance(prepared, NpiPreparedSource) or not callable(archive_copy):
        raise NpiResultArchiveError("NPI prepared source is invalid")
    if prepared.ownership.freeze_function_oid is None or not prepared.ownership.freeze_trigger_oids:
        raise NpiResultArchiveError("NPI prepared source is not frozen")
    manifest = validate_npi_result_manifest(prepared.manifest)
    async with session_factory() as stage_session, stage_session.begin():
        await stage_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        async with _bounded_catalog_work(stage_session):
            await _lock_family(stage_session, prepared.ownership.schema_name, "ACCESS SHARE")
            await verify_npi_stage_ownership(stage_session, prepared.ownership)
        await _validate_stage_manifest(
            stage_session,
            prepared.ownership,
            manifest,
            ownership_verified=True,
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
) -> NpiResultManifest:
    """Convenience clone/dump flow with cancellation-safe exact cleanup."""

    ownership = None
    try:

        async def retain_locally(_session: Any, _prepared: NpiPreparedSource) -> None:
            """Keep convenience-wrapper cleanup ownership local."""

            return None

        prepared = await prepare_npi_archive_source(
            session_factory,
            schema_name=schema_name,
            source_metadata=source_metadata,
            dataset_id=dataset_id,
            on_prepared=retain_locally,
        )
        ownership = prepared.ownership
        return await export_prepared_npi_archive(
            session_factory,
            prepared=prepared,
            archive_copy=archive_copy,
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
) -> NpiStageOwnership:
    """Create an empty model-complete target for a native data-only restore."""

    _require_transaction(session)
    schema_name = npi_stage_schema(dataset_id)
    await session.execute(text(f"CREATE SCHEMA {_quoted(schema_name)}"))
    metadata = MetaData(schema=schema_name)
    has_postgis = await _has_postgis(session)
    for model_type in _MODEL_TYPES:
        table = model_type.__table__.to_metadata(metadata, schema=schema_name)
        statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        await session.execute(text(statement))
        primary_elements = tuple(getattr(model_type, "__my_index_elements__", ()) or ())
        if primary_elements:
            await session.execute(
                text(
                    f"CREATE UNIQUE INDEX {_quoted(model_type.__tablename__ + '_idx_primary')} "
                    f"ON {_quoted(schema_name)}.{_quoted(model_type.__tablename__)} "
                    f"({', '.join(primary_elements)})"
                )
            )
        indexes = tuple(getattr(model_type, "__my_initial_indexes__", ()) or ()) + tuple(
            getattr(model_type, "__my_additional_indexes__", ()) or ()
        )
        for index in indexes:
            if _uses_postgis_index(index) and not has_postgis:
                continue
            await session.execute(text(_additional_index_sql(schema_name, model_type, index)))
    return await capture_npi_stage_ownership(session, dataset_id=dataset_id)


async def validate_npi_stage(
    session: Any,
    *,
    ownership: NpiStageOwnership,
    manifest: Mapping[str, Any] | NpiResultManifest,
) -> tuple[NpiTableReceipt, ...]:
    """Validate restored schema/count semantics under an ownership lock."""

    _require_transaction(session)
    validated = validate_npi_result_manifest(manifest)
    async with _bounded_catalog_work(session):
        await _lock_family(session, ownership.schema_name, "ACCESS SHARE")
        await verify_npi_stage_ownership(session, ownership)
    return await _validate_stage_manifest(
        session,
        ownership,
        validated,
        ownership_verified=True,
    )


async def _verify_stage_owner(
    session: Any,
    ownership: NpiStageOwnership,
    expected_owner_oid: int,
) -> None:
    if type(expected_owner_oid) is not int or expected_owner_oid <= 0:
        raise NpiResultArchiveError("NPI archive stage owner is invalid")
    schema_owner = await session.scalar(
        text("SELECT nspowner FROM pg_catalog.pg_namespace WHERE oid=:schema_oid AND nspname=:schema_name"),
        {"schema_oid": ownership.schema_oid, "schema_name": ownership.schema_name},
    )
    if schema_owner != expected_owner_oid:
        raise NpiResultArchiveError("NPI archive stage owner differs")
    owner_rows = list(
        (
            await session.execute(
                text(
                    "SELECT relation.relname, relation.oid, relation.relowner "
                    "FROM pg_catalog.pg_class AS relation "
                    "WHERE relation.oid=ANY(CAST(:relation_oids AS oid[])) "
                    "ORDER BY relation.relname"
                ),
                {
                    "relation_oids": [relation_oid for _, relation_oid in ownership.relation_oids]
                    + [sequence_oid for _, sequence_oid, _, _ in ownership.sequence_oids]
                },
            )
        ).mappings()
    )
    observed_owners = [
        (owner_row["relname"], int(owner_row["oid"]), int(owner_row["relowner"])) for owner_row in owner_rows
    ]
    expected_owners = [
        (table_name, relation_oid, expected_owner_oid) for table_name, relation_oid in ownership.relation_oids
    ] + [
        (sequence_name, sequence_oid, expected_owner_oid)
        for sequence_name, sequence_oid, _, _ in ownership.sequence_oids
    ]
    expected_owners.sort()
    if observed_owners != expected_owners:
        raise NpiResultArchiveError("NPI archive stage owner differs")


def _validation_inventory(value: object) -> tuple[tuple[str, int], ...]:
    if not isinstance(value, list) or len(value) != len(RELATION_NAMES):
        raise NpiResultArchiveError("NPI validation inventory is invalid")
    pairs = []
    for raw_pair, expected_name in zip(value, sorted(RELATION_NAMES), strict=True):
        if (
            not isinstance(raw_pair, list)
            or len(raw_pair) != 2
            or raw_pair[0] != expected_name
            or type(raw_pair[1]) is not int
            or raw_pair[1] <= 0
        ):
            raise NpiResultArchiveError("NPI validation inventory is invalid")
        pairs.append((raw_pair[0], raw_pair[1]))
    return tuple(pairs)


def validate_npi_validation_receipt(receipt_value: object) -> NpiValidationReceipt:
    """Validate durable stage evidence without treating it as authority."""

    if isinstance(receipt_value, NpiValidationReceipt):
        receipt_value = receipt_value.as_dict()
    expected_fields = {
        "contract",
        "package_id",
        "profile_contract",
        "stage_schema",
        "stage_schema_oid",
        "relation_oids",
        "sealed_owner_oid",
        "manifest_sha256",
        "tables",
        "validation_sha256",
    }
    if not isinstance(receipt_value, Mapping) or set(receipt_value) != expected_fields:
        raise NpiResultArchiveError("NPI validation receipt is invalid")
    if (
        receipt_value["contract"] != VALIDATION_CONTRACT
        or receipt_value["profile_contract"] != CONTRACT
        or _SHA256.fullmatch(str(receipt_value["package_id"])) is None
        or _SHA256.fullmatch(str(receipt_value["manifest_sha256"])) is None
        or type(receipt_value["stage_schema_oid"]) is not int
        or receipt_value["stage_schema_oid"] <= 0
        or type(receipt_value["sealed_owner_oid"]) is not int
        or receipt_value["sealed_owner_oid"] <= 0
    ):
        raise NpiResultArchiveError("NPI validation receipt is invalid")
    schema_name = _schema_name(receipt_value["stage_schema"])
    relation_oids = _validation_inventory(receipt_value["relation_oids"])
    tables = _validate_tables(receipt_value["tables"])
    digest_by_field = {key: receipt_value[key] for key in expected_fields - {"validation_sha256"}}
    if receipt_value["validation_sha256"] != _validation_digest(digest_by_field):
        raise NpiResultArchiveError("NPI validation digest differs")
    return NpiValidationReceipt(
        receipt_value["package_id"],
        schema_name,
        receipt_value["stage_schema_oid"],
        relation_oids,
        receipt_value["sealed_owner_oid"],
        receipt_value["manifest_sha256"],
        tables,
        receipt_value["validation_sha256"],
    )


async def prepare_npi_activation(
    session: Any,
    *,
    ownership: NpiStageOwnership,
    manifest: Mapping[str, Any] | NpiResultManifest,
    package_id: str,
    sealed_owner_oid: int,
) -> NpiValidationReceipt:
    """Perform long validation over an already publisher-frozen stage."""

    _require_transaction(session)
    validated = validate_npi_result_manifest(manifest)
    if _SHA256.fullmatch(str(package_id)) is None:
        raise NpiResultArchiveError("NPI validation package identity is invalid")
    await verify_npi_stage_ownership(session, ownership)
    await _verify_stage_owner(session, ownership, sealed_owner_oid)
    await _advance_and_verify_stage_sequences(session, ownership)
    tables = await _validate_stage_manifest(session, ownership, validated)
    digest_by_field = {
        "contract": VALIDATION_CONTRACT,
        "package_id": package_id,
        "profile_contract": CONTRACT,
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
        "sealed_owner_oid": sealed_owner_oid,
        "manifest_sha256": _manifest_digest(validated),
        "tables": [table.as_dict() for table in tables],
    }
    return validate_npi_validation_receipt(
        {**digest_by_field, "validation_sha256": _validation_digest(digest_by_field)}
    )


async def capture_npi_incumbent(session: Any, *, schema_name: str) -> NpiIncumbent:
    """Capture all-present or all-absent live relation OIDs for later CAS."""

    _require_transaction(session)
    schema = _schema_name(schema_name)
    async with _bounded_catalog_work(session):
        pairs = await _relation_pairs(session, schema)
        presence_flags = [relation_oid is not None for _, relation_oid in pairs]
        if any(presence_flags) and not all(presence_flags):
            raise NpiResultArchiveError("NPI incumbent is incomplete")
        if all(presence_flags):
            await _lock_family(session, schema, "ACCESS SHARE")
            if await _relation_pairs(session, schema) != pairs:
                raise NpiResultArchiveError("NPI incumbent changed during capture")
    return NpiIncumbent(schema, pairs)


async def _lock_and_verify_activation(
    session: Any,
    ownership: NpiStageOwnership,
    incumbent: NpiIncumbent,
) -> None:
    async with _bounded_catalog_work(session):
        await _lock_family(session, ownership.schema_name, "ACCESS EXCLUSIVE")
        if all(relation_oid is not None for _, relation_oid in incumbent.relation_oids):
            await _lock_family(session, incumbent.schema_name, "ACCESS EXCLUSIVE")
        await verify_npi_stage_ownership(session, ownership)
        if await _relation_pairs(session, incumbent.schema_name) != incumbent.relation_oids:
            raise NpiResultArchiveError("NPI incumbent changed")


async def _has_populated_incumbent(session: Any, incumbent: NpiIncumbent) -> bool:
    if not all(relation_oid is not None for _, relation_oid in incumbent.relation_oids):
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
    incumbent_by_name = dict(incumbent.relation_oids)
    predecessor_schema = None
    if any(relation_oid is not None for relation_oid in incumbent_by_name.values()):
        predecessor_schema = npi_predecessor_schema(ownership.dataset_id)
        await session.execute(text(f"CREATE SCHEMA {_quoted(predecessor_schema)}"))
    for table_name in RELATION_NAMES:
        if incumbent_by_name[table_name] is not None:
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
        {"schema_oid": ownership.schema_oid},
    )
    if int(remaining or 0):
        raise NpiResultArchiveError("NPI stage schema is not empty after activation")
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))
    return predecessor_schema


async def _remove_npi_clone_freeze(
    session: Any,
    ownership: NpiStageOwnership,
) -> None:
    """Remove only the exact verified clone guards inside the cutover lock."""

    if ownership.freeze_function_oid is None or not ownership.freeze_trigger_oids:
        return
    await verify_npi_stage_ownership(session, ownership)
    for table_name in RELATION_NAMES:
        relation = f"{_quoted(ownership.schema_name)}.{_quoted(table_name)}"
        await session.execute(text(f"DROP TRIGGER {_quoted(_FREEZE_WRITE_TRIGGER)} ON {relation}"))
        await session.execute(text(f"DROP TRIGGER {_quoted(_FREEZE_TRUNCATE_TRIGGER)} ON {relation}"))
    await session.execute(text(f"DROP FUNCTION {_quoted(ownership.schema_name)}.{_quoted(_FREEZE_FUNCTION)}()"))


def _validate_cutover_bindings(
    ownership: NpiStageOwnership,
    manifest: NpiResultManifest,
    validation: NpiValidationReceipt,
    cutover: NpiCutoverAuthority,
) -> None:
    if (
        validation.package_id != cutover.package_id
        or validation.sealed_owner_oid != cutover.sealed_owner_oid
        or validation.stage_schema != ownership.schema_name
        or validation.stage_schema_oid != ownership.schema_oid
        or validation.relation_oids != ownership.relation_oids
        or validation.manifest_sha256 != _manifest_digest(manifest)
        or validation.tables != manifest.tables
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
    incumbent_oids = tuple(relation_oid for _, relation_oid in incumbent.relation_oids)
    if current_authority.serving_generation is None:
        if await _has_populated_incumbent(session, incumbent):
            raise NpiResultArchiveError("NPI legacy incumbent requires manual adoption")
        return
    if current_authority.relation_oids != incumbent_oids:
        raise NpiResultArchiveError("NPI incumbent generation is drifted")
    try:
        require_npi_automatic_generation_order(
            source_generation,
            current_authority.serving_generation,
        )
    except ValueError as error:
        raise NpiResultArchiveError("NPI automatic generation is stale or unrelated") from error


async def _activate_npi_relations(
    session: Any,
    *,
    ownership: NpiStageOwnership,
    incumbent: NpiIncumbent,
    manifest: NpiResultManifest,
    validation: NpiValidationReceipt,
) -> NpiActivationReceipt:
    await _remove_npi_clone_freeze(session, ownership)
    await install_npi_stage_mutation_guards(
        session,
        schema_name=ownership.schema_name,
        stage_tables=RELATION_NAMES,
        function_schema_name=incumbent.schema_name,
    )
    predecessor_schema = await _rotate_relations(session, ownership, incumbent)
    live_pairs = await _relation_pairs(session, incumbent.schema_name)
    if tuple(sorted(live_pairs)) != ownership.relation_oids or any(
        relation_oid is None for _, relation_oid in live_pairs
    ):
        raise NpiResultArchiveError("NPI activated relation identity differs")
    adopted = await publish_adopted_npi_result_generation(
        session,
        schema_name=incumbent.schema_name,
        source_generation=manifest.source_serving_generation,
        canonical_provenance=manifest.canonical_provenance,
    )
    ordered_oids = tuple(int(dict(live_pairs)[name]) for name in RELATION_NAMES)
    if manifest.source_serving_generation is None:
        if adopted.serving_generation is not None or adopted.relation_oids is not None:
            raise NpiResultArchiveError("NPI generation-less adoption differs")
    elif adopted.serving_generation != manifest.source_serving_generation or adopted.relation_oids != ordered_oids:
        raise NpiResultArchiveError("NPI adopted generation differs")
    return NpiActivationReceipt(
        tuple((name, int(relation_oid)) for name, relation_oid in live_pairs),
        incumbent.relation_oids,
        predecessor_schema,
        validation.tables,
        adopted,
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
    validation = validate_npi_validation_receipt(validation_receipt)
    _validate_cutover_bindings(ownership, validated_manifest, validation, cutover)
    await _lock_and_verify_activation(session, ownership, incumbent)
    await _verify_stage_owner(session, ownership, cutover.expected_stage_owner_oid)
    await _advance_and_verify_stage_sequences(session, ownership)
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
    )
    await on_activated(session, receipt)
    return receipt


__all__ = [
    "CONTRACT",
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
    "capture_npi_source",
    "capture_npi_stage_ownership",
    "cleanup_npi_stage",
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
