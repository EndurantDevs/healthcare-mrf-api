# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable result-revision authority for the six-table NPI serving family."""

from __future__ import annotations

import datetime
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import text

from process.npi_canonical_publication import (
    NPI_CANONICAL_TABLES,
    NpiCanonicalPublicationReceipt,
    validate_npi_canonical_publication_receipt,
)

TABLE_NAME = "npi_result_generation"
REVISION_FUNCTION = "advance_npi_result_generation"
REVISION_TRIGGER = "npi_result_generation_revision_guard"
RELATION_NAMES = NPI_CANONICAL_TABLES
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_MAX_GENERATION = (1 << 63) - 1
_MAX_CANONICAL_GENERATION = (1 << 53) - 1
_MAX_OID = (1 << 32) - 1


@dataclass(frozen=True)
class NpiCanonicalProvenance:
    """Immutable ordinary-publication provenance, not current-content proof."""

    publication_ref: str
    publication_generation: int
    chain_ref: str
    import_date: datetime.date

    def as_dict(self) -> dict[str, Any]:
        """Return the bounded portable provenance representation."""

        return {
            "publication_ref": self.publication_ref,
            "publication_generation": self.publication_generation,
            "chain_ref": self.chain_ref,
            "import_date": self.import_date.isoformat(),
        }


@dataclass(frozen=True)
class NpiServingGeneration:
    """Portable origin identity for one tracked NPI result revision."""

    origin_lineage_id: str
    origin_generation: int
    published_at: datetime.datetime

    def as_dict(self) -> dict[str, Any]:
        """Return the canonical portable generation representation."""

        return {
            "origin_lineage_id": self.origin_lineage_id,
            "origin_generation": self.origin_generation,
            "published_at": _timestamp_text(self.published_at),
        }


@dataclass(frozen=True)
class NpiResultGenerationAuthority:
    """Local counter, current portable origin, OIDs, and import provenance."""

    local_lineage_id: str
    local_generation: int
    serving_generation: NpiServingGeneration | None
    relation_oids: tuple[int, ...] | None
    canonical_provenance: NpiCanonicalProvenance | None

    def as_dict(self) -> dict[str, Any]:
        """Return the durable authority without inferring legacy history."""

        return {
            "local_lineage_id": self.local_lineage_id,
            "local_generation": self.local_generation,
            "serving_generation": (None if self.serving_generation is None else self.serving_generation.as_dict()),
            "relation_oids": None if self.relation_oids is None else list(self.relation_oids),
            "canonical_provenance": (
                None if self.canonical_provenance is None else self.canonical_provenance.as_dict()
            ),
        }


def _schema_name(value: object) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(normalized) or len(normalized.encode("utf-8")) > 63:
        raise ValueError("NPI result generation schema is invalid")
    return normalized


def _identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"NPI result generation {field_name} is invalid")
    return value


def _quoted(value: str) -> str:
    return f'"{value}"'


def _uuid_text(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except AttributeError, TypeError, ValueError:
        raise ValueError("NPI result generation lineage is invalid") from None


def _timestamp(value: object) -> datetime.datetime:
    if isinstance(value, str):
        try:
            value = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("NPI result generation time is invalid") from None
    if not isinstance(value, datetime.datetime) or value.tzinfo is None:
        raise ValueError("NPI result generation time is invalid")
    return value.astimezone(datetime.timezone.utc)


def _timestamp_text(value: datetime.datetime) -> str:
    return _timestamp(value).isoformat().replace("+00:00", "Z")


def _generation(value: object) -> int:
    if type(value) is not int or not 0 < value <= _MAX_GENERATION:
        raise ValueError("NPI serving generation is invalid")
    return value


def _canonical_generation(value: object) -> int:
    generation = _generation(value)
    if generation > _MAX_CANONICAL_GENERATION:
        raise ValueError("NPI canonical provenance is invalid")
    return generation


def _relation_oids(value: object) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)) or len(value) != len(RELATION_NAMES):
        raise ValueError("NPI serving relation identity is invalid")
    normalized_oids = tuple(value)
    if any(
        type(relation_oid) is not int or not 0 < relation_oid <= _MAX_OID for relation_oid in normalized_oids
    ) or len(set(normalized_oids)) != len(normalized_oids):
        raise ValueError("NPI serving relation identity is invalid")
    return normalized_oids


def validate_npi_serving_generation(value: object) -> NpiServingGeneration:
    """Validate one complete portable origin generation."""

    if isinstance(value, NpiServingGeneration):
        value = value.as_dict()
    if not isinstance(value, Mapping) or set(value) != {
        "origin_lineage_id",
        "origin_generation",
        "published_at",
    }:
        raise ValueError("NPI serving generation is invalid")
    return NpiServingGeneration(
        _uuid_text(value["origin_lineage_id"]),
        _generation(value["origin_generation"]),
        _timestamp(value["published_at"]),
    )


def validate_npi_canonical_provenance(value: object) -> NpiCanonicalProvenance:
    """Validate original import provenance without assigning current authority."""

    if isinstance(value, NpiCanonicalProvenance):
        value = value.as_dict()
    if not isinstance(value, Mapping) or set(value) != {
        "publication_ref",
        "publication_generation",
        "chain_ref",
        "import_date",
    }:
        raise ValueError("NPI canonical provenance is invalid")
    try:
        import_date = datetime.date.fromisoformat(str(value["import_date"]))
    except TypeError, ValueError:
        raise ValueError("NPI canonical provenance is invalid") from None
    publication_ref = str(value["publication_ref"])
    chain_ref = str(value["chain_ref"])
    generation = _canonical_generation(value["publication_generation"])
    if not re.fullmatch(r"nppub1_[A-Za-z0-9_-]{43}", publication_ref) or not re.fullmatch(
        r"penpc1_[A-Za-z0-9_-]{43}", chain_ref
    ):
        raise ValueError("NPI canonical provenance is invalid")
    return NpiCanonicalProvenance(publication_ref, generation, chain_ref, import_date)


def _row_mapping(row: object) -> Mapping[str, Any]:
    mapping = getattr(row, "_mapping", row)
    if isinstance(mapping, Mapping):
        return mapping
    keys = getattr(mapping, "keys", None)
    if callable(keys) and hasattr(mapping, "__getitem__"):
        return {key: mapping[key] for key in keys()}
    raise RuntimeError("NPI result generation authority is unavailable")


def validate_npi_result_generation_authority(authority_row: object) -> NpiResultGenerationAuthority:
    """Validate the migration-installed singleton and its optional authority."""

    authority_values = _row_mapping(authority_row)
    if authority_values.get("singleton") is not True:
        raise RuntimeError("NPI result generation authority is unavailable")
    local_generation = authority_values.get("local_generation")
    if type(local_generation) is not int or not 0 <= local_generation <= _MAX_GENERATION:
        raise RuntimeError("NPI local result generation is invalid")
    try:
        lineage_id = _uuid_text(authority_values.get("local_lineage_id"))
        origin_values = (
            authority_values.get("origin_lineage_id"),
            authority_values.get("origin_generation"),
            authority_values.get("published_at"),
            authority_values.get("relation_oids"),
        )
        if all(origin_value is None for origin_value in origin_values):
            serving_generation = None
            relation_oids = None
        elif any(origin_value is None for origin_value in origin_values):
            raise ValueError("incomplete serving generation")
        else:
            serving_generation = validate_npi_serving_generation(
                {
                    "origin_lineage_id": authority_values["origin_lineage_id"],
                    "origin_generation": authority_values["origin_generation"],
                    "published_at": authority_values["published_at"],
                }
            )
            relation_oids = _relation_oids(authority_values["relation_oids"])
        provenance_values = (
            authority_values.get("canonical_publication_ref"),
            authority_values.get("canonical_publication_generation"),
            authority_values.get("canonical_chain_ref"),
            authority_values.get("canonical_import_date"),
        )
        if all(provenance_value is None for provenance_value in provenance_values):
            provenance = None
        elif any(provenance_value is None for provenance_value in provenance_values):
            raise ValueError("incomplete canonical provenance")
        else:
            provenance = validate_npi_canonical_provenance(
                {
                    "publication_ref": authority_values["canonical_publication_ref"],
                    "publication_generation": authority_values["canonical_publication_generation"],
                    "chain_ref": authority_values["canonical_chain_ref"],
                    "import_date": str(authority_values["canonical_import_date"]),
                }
            )
    except ValueError as error:
        raise RuntimeError("NPI result generation authority is invalid") from error
    return NpiResultGenerationAuthority(
        lineage_id,
        local_generation,
        serving_generation,
        relation_oids,
        provenance,
    )


def _state_projection(schema_name: str, *, lock: bool) -> str:
    suffix = " FOR UPDATE" if lock else ""
    return (
        "SELECT singleton, local_lineage_id, local_generation, origin_lineage_id, "
        "origin_generation, published_at, relation_oids, canonical_publication_ref, "
        "canonical_publication_generation, canonical_chain_ref, canonical_import_date "
        f"FROM {_quoted(schema_name)}.{_quoted(TABLE_NAME)} "
        f"WHERE singleton IS TRUE{suffix}"
    )


async def read_npi_result_generation_authority(
    session: Any,
    *,
    schema_name: str,
    lock: bool = False,
) -> NpiResultGenerationAuthority:
    """Read the one durable NPI result authority in a caller transaction."""

    schema = _schema_name(schema_name)
    authority_row = (await session.execute(text(_state_projection(schema, lock=lock)))).mappings().one_or_none()
    if authority_row is None:
        raise RuntimeError("NPI result generation authority is unavailable")
    return validate_npi_result_generation_authority(authority_row)


async def current_npi_relation_oids(session: Any, *, schema_name: str) -> tuple[int, ...]:
    """Resolve the exact six current relation OIDs in canonical model order."""

    schema = _schema_name(schema_name)
    rows = list(
        (
            await session.execute(
                text(
                    "SELECT relation_name, "
                    "to_regclass(format('%I.%I', CAST(:schema_name AS text), relation_name))::oid::bigint "
                    "AS relation_oid FROM unnest(CAST(:relation_names AS text[])) WITH ORDINALITY "
                    "AS relations(relation_name, ordinal) ORDER BY ordinal"
                ),
                {"schema_name": schema, "relation_names": list(RELATION_NAMES)},
            )
        ).all()
    )
    try:
        names = tuple(str(row[0]) for row in rows)
        oids = _relation_oids(tuple(int(row[1]) for row in rows))
    except IndexError, TypeError, ValueError:
        raise RuntimeError("NPI serving relations are unavailable") from None
    if names != RELATION_NAMES:
        raise RuntimeError("NPI serving relations are unavailable")
    return oids


async def _matching_canonical_provenance(
    session: Any,
    *,
    schema_name: str,
    relation_oids: tuple[int, ...],
) -> NpiCanonicalProvenance | None:
    """Return only a sealed receipt for the same physical family as provenance."""

    schema = _schema_name(schema_name)
    oid_predicates = " AND ".join(
        f"receipt.{column_name}=:relation_{ordinal}"
        for ordinal, column_name in enumerate(
            (
                "npi_table_oid",
                "npi_address_table_oid",
                "npi_taxonomy_table_oid",
                "npi_taxonomy_group_table_oid",
                "npi_other_identifier_table_oid",
                "npi_phone_staffing_table_oid",
            ),
            1,
        )
    )
    receipt_row = (
        (
            await session.execute(
                text(
                    "SELECT receipt.publication_ref, receipt.publication_generation, "
                    "receipt.chain_ref, receipt.import_date FROM "
                    f"{_quoted(schema)}.{_quoted('npi_canonical_publication_receipt')} AS receipt "
                    f"JOIN {_quoted(schema)}.{_quoted('npi_canonical_publication_receipt_seal')} "
                    "AS sealed USING (publication_ref) WHERE "
                    f"{oid_predicates} ORDER BY receipt.publication_generation DESC LIMIT 1"
                ),
                {f"relation_{ordinal}": relation_oid for ordinal, relation_oid in enumerate(relation_oids, 1)},
            )
        )
        .mappings()
        .one_or_none()
    )
    if receipt_row is None:
        return None
    return validate_npi_canonical_provenance(
        {
            "publication_ref": receipt_row["publication_ref"],
            "publication_generation": receipt_row["publication_generation"],
            "chain_ref": receipt_row["chain_ref"],
            "import_date": str(receipt_row["import_date"]),
        }
    )


async def capture_npi_serving_generation(
    session: Any,
    *,
    schema_name: str,
) -> NpiResultGenerationAuthority:
    """Require a tracked generation bound to the current exact six OIDs."""

    schema = _schema_name(schema_name)
    authority = await read_npi_result_generation_authority(session, schema_name=schema)
    current_oids = await current_npi_relation_oids(session, schema_name=schema)
    if authority.serving_generation is None or authority.relation_oids != current_oids:
        raise RuntimeError("NPI serving generation is unavailable or drifted")
    return authority


async def _write_bootstrap_authority(
    session: Any,
    *,
    schema_name: str,
    next_generation: int,
    relation_oids: tuple[int, ...],
    provenance: NpiCanonicalProvenance | None,
) -> NpiResultGenerationAuthority:
    updated_row = (
        (
            await session.execute(
                text(
                    f"UPDATE {_quoted(schema_name)}.{_quoted(TABLE_NAME)} SET "
                    "local_generation=:next_generation, origin_lineage_id=local_lineage_id, "
                    "origin_generation=:next_generation, published_at=transaction_timestamp(), "
                    "relation_oids=CAST(:relation_oids AS bigint[]), "
                    "canonical_publication_ref=:publication_ref, "
                    "canonical_publication_generation=:publication_generation, "
                    "canonical_chain_ref=:chain_ref, canonical_import_date=CAST(:import_date AS date) "
                    "WHERE singleton IS TRUE RETURNING singleton, local_lineage_id, local_generation, "
                    "origin_lineage_id, origin_generation, published_at, relation_oids, "
                    "canonical_publication_ref, canonical_publication_generation, "
                    "canonical_chain_ref, canonical_import_date"
                ),
                {
                    "next_generation": next_generation,
                    "relation_oids": list(relation_oids),
                    "publication_ref": None if provenance is None else provenance.publication_ref,
                    "publication_generation": (None if provenance is None else provenance.publication_generation),
                    "chain_ref": None if provenance is None else provenance.chain_ref,
                    "import_date": None if provenance is None else provenance.import_date,
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    if updated_row is None:
        raise RuntimeError("NPI result generation authority is unavailable")
    return validate_npi_result_generation_authority(updated_row)


async def bootstrap_npi_result_generation(
    session: Any,
    *,
    schema_name: str,
) -> NpiResultGenerationAuthority:
    """Explicitly establish authority for an existing complete serving family.

    Export never calls this function.  A trusted coordinator may invoke it as
    a distinct manual bootstrap operation.  The short SHARE lock establishes a
    real revision boundary; later statements advance that revision through the
    migration-installed triggers.
    """

    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ValueError("NPI result generation bootstrap requires a caller transaction")
    schema = _schema_name(schema_name)
    relations = ", ".join(f"{_quoted(schema)}.{_quoted(table_name)}" for table_name in RELATION_NAMES)
    async with _bounded_bootstrap(session):
        await session.execute(text(f"LOCK TABLE {relations} IN SHARE MODE"))
        relation_oids = await current_npi_relation_oids(session, schema_name=schema)
        current = await read_npi_result_generation_authority(
            session,
            schema_name=schema,
            lock=True,
        )
    if current.serving_generation is not None:
        if current.relation_oids != relation_oids:
            raise RuntimeError("NPI serving generation is unavailable or drifted")
        return current
    if current.local_generation >= _MAX_GENERATION:
        raise RuntimeError("NPI local result generation is exhausted")
    provenance = await _matching_canonical_provenance(
        session,
        schema_name=schema,
        relation_oids=relation_oids,
    )
    return await _write_bootstrap_authority(
        session,
        schema_name=schema,
        next_generation=current.local_generation + 1,
        relation_oids=relation_oids,
        provenance=provenance,
    )


async def _timeout_value(session: Any, setting: str) -> str:
    value = await session.scalar(text(f"SHOW {setting}"))
    if not isinstance(value, str) or not value:
        raise RuntimeError("NPI result generation timeout state is unavailable")
    return value


async def _set_local_timeout(session: Any, setting: str, value: str) -> None:
    await session.execute(
        text("SELECT pg_catalog.set_config(:setting, :value, true)"),
        {"setting": setting, "value": value},
    )


@asynccontextmanager
async def _bounded_bootstrap(session: Any):
    previous_lock = await _timeout_value(session, "lock_timeout")
    previous_statement = await _timeout_value(session, "statement_timeout")
    await _set_local_timeout(session, "lock_timeout", "500ms")
    await _set_local_timeout(session, "statement_timeout", "5s")
    try:
        yield
    finally:
        await _set_local_timeout(session, "lock_timeout", previous_lock)
        await _set_local_timeout(session, "statement_timeout", previous_statement)


def _provenance(receipt: NpiCanonicalPublicationReceipt) -> NpiCanonicalProvenance:
    return NpiCanonicalProvenance(
        receipt.publication_ref,
        receipt.publication_generation,
        receipt.chain_ref,
        datetime.date.fromisoformat(receipt.import_date),
    )


def _guard_statements(
    schema_name: str,
    stage_tables: tuple[str, ...],
    *,
    function_schema_name: str | None = None,
) -> tuple[str, ...]:
    schema = _schema_name(schema_name)
    function_schema = _schema_name(function_schema_name or schema)
    if len(stage_tables) != len(RELATION_NAMES) or len(set(stage_tables)) != len(stage_tables):
        raise ValueError("NPI result generation stage family is invalid")
    function = f"{_quoted(function_schema)}.{_quoted(REVISION_FUNCTION)}"
    statements = []
    for stage_table_value in stage_tables:
        stage_table = _identifier(stage_table_value, field_name="stage table")
        relation = f"{_quoted(schema)}.{_quoted(stage_table)}"
        statements.append(
            f"DROP TRIGGER IF EXISTS {_quoted(REVISION_TRIGGER)} ON {relation}; "
            f"CREATE TRIGGER {_quoted(REVISION_TRIGGER)} "
            f"AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {function}(); "
            f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_quoted(REVISION_TRIGGER)};"
        )
    return tuple(statements)


async def _execute_ddl(connection: Any, statement: str) -> None:
    if hasattr(connection, "fetchrow"):
        await connection.execute(statement)
    else:
        for single_statement in statement.split(";"):
            if single_statement.strip():
                await connection.execute(text(single_statement))


async def install_npi_result_revision_guards(
    connection: Any,
    *,
    schema_name: str,
    stage_tables: tuple[str, ...],
    function_schema_name: str | None = None,
) -> None:
    """Install one statement trigger on each complete promoted stage relation."""

    for statement in _guard_statements(
        schema_name,
        stage_tables,
        function_schema_name=function_schema_name,
    ):
        await _execute_ddl(connection, statement)


async def install_npi_stage_mutation_guards(
    connection: Any,
    *,
    schema_name: str,
    stage_tables: tuple[str, ...],
    function_schema_name: str | None = None,
) -> None:
    """Install canonical write fences and result-revision triggers on a stage."""

    schema = _schema_name(schema_name)
    function_schema = _schema_name(function_schema_name or schema)
    if len(stage_tables) != len(RELATION_NAMES) or len(set(stage_tables)) != len(stage_tables):
        raise ValueError("NPI result generation stage family is invalid")
    guard_function = f"{_quoted(function_schema)}.{_quoted('guard_npi_canonical_publication_after_seal')}"
    for stage_table_value in stage_tables:
        stage_table = _identifier(stage_table_value, field_name="stage table")
        relation = f"{_quoted(schema)}.{_quoted(stage_table)}"
        await _execute_ddl(
            connection,
            f"DROP TRIGGER IF EXISTS npi_canonical_publication_postseal_write_guard ON {relation}; "
            "CREATE TRIGGER npi_canonical_publication_postseal_write_guard "
            f"BEFORE INSERT OR UPDATE OR DELETE ON {relation} FOR EACH STATEMENT "
            f"EXECUTE FUNCTION {guard_function}(); "
            f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER "
            "npi_canonical_publication_postseal_write_guard; "
            f"DROP TRIGGER IF EXISTS npi_canonical_publication_postseal_truncate_guard ON {relation}; "
            "CREATE TRIGGER npi_canonical_publication_postseal_truncate_guard "
            f"BEFORE TRUNCATE ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {guard_function}(); "
            f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER "
            "npi_canonical_publication_postseal_truncate_guard;",
        )
    await install_npi_result_revision_guards(
        connection,
        schema_name=schema,
        stage_tables=stage_tables,
        function_schema_name=function_schema,
    )


async def publish_local_npi_result_generation(
    connection: Any,
    *,
    schema_name: str,
    receipt: NpiCanonicalPublicationReceipt,
) -> NpiResultGenerationAuthority:
    """Advance and bind the result revision inside ordinary publication."""

    schema = _schema_name(schema_name)
    fixed_receipt = validate_npi_canonical_publication_receipt(receipt)
    current_row = await connection.fetchrow(_state_projection(schema, lock=True))
    if current_row is None:
        raise RuntimeError("NPI result generation authority is unavailable")
    current = validate_npi_result_generation_authority(current_row)
    if current.local_generation >= _MAX_GENERATION:
        raise RuntimeError("NPI local result generation is exhausted")
    live_oids = await connection.fetchrow(
        "SELECT "
        + ", ".join(
            f"to_regclass($1 || '.{table_name}')::oid::bigint AS relation_{ordinal}"
            for ordinal, table_name in enumerate(RELATION_NAMES, 1)
        ),
        schema,
    )
    if live_oids is None:
        raise RuntimeError("NPI serving relations are unavailable")
    relation_oids = _relation_oids(tuple(live_oids[f"relation_{ordinal}"] for ordinal in range(1, 7)))
    if relation_oids != fixed_receipt.relation_oids:
        raise RuntimeError("NPI publication relation identity differs")
    provenance = _provenance(fixed_receipt)
    updated = await connection.fetchrow(
        f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET "
        "local_generation=$1, origin_lineage_id=local_lineage_id, "
        "origin_generation=$1, published_at=$2::timestamptz, relation_oids=$3::bigint[], "
        "canonical_publication_ref=$4, canonical_publication_generation=$5, "
        "canonical_chain_ref=$6, canonical_import_date=$7::date WHERE singleton IS TRUE "
        "RETURNING singleton, local_lineage_id, local_generation, origin_lineage_id, "
        "origin_generation, published_at, relation_oids, canonical_publication_ref, "
        "canonical_publication_generation, canonical_chain_ref, canonical_import_date",
        current.local_generation + 1,
        _timestamp(fixed_receipt.created_at),
        list(relation_oids),
        provenance.publication_ref,
        provenance.publication_generation,
        provenance.chain_ref,
        provenance.import_date,
    )
    if updated is None:
        raise RuntimeError("NPI result generation authority is unavailable")
    return validate_npi_result_generation_authority(updated)


async def _adopted_generation_parameters(
    session: Any,
    *,
    schema_name: str,
    source_generation: Mapping[str, Any] | NpiServingGeneration | None,
) -> dict[str, Any]:
    if source_generation is None:
        return {
            "origin_lineage_id": None,
            "origin_generation": None,
            "published_at": None,
            "relation_oids": None,
        }
    validated_source = validate_npi_serving_generation(source_generation)
    return {
        "origin_lineage_id": validated_source.origin_lineage_id,
        "origin_generation": validated_source.origin_generation,
        "published_at": validated_source.published_at,
        "relation_oids": list(await current_npi_relation_oids(session, schema_name=schema_name)),
    }


async def publish_adopted_npi_result_generation(
    session: Any,
    *,
    schema_name: str,
    source_generation: Mapping[str, Any] | NpiServingGeneration | None,
    canonical_provenance: Mapping[str, Any] | NpiCanonicalProvenance | None,
) -> NpiResultGenerationAuthority:
    """Preserve a source origin and provenance without advancing local history."""

    schema = _schema_name(schema_name)
    current = await read_npi_result_generation_authority(session, schema_name=schema, lock=True)
    generation_params_by_name = await _adopted_generation_parameters(
        session,
        schema_name=schema,
        source_generation=source_generation,
    )
    provenance = None if canonical_provenance is None else validate_npi_canonical_provenance(canonical_provenance)
    updated_row = (
        (
            await session.execute(
                text(
                    f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET "
                    "origin_lineage_id=CAST(:origin_lineage_id AS uuid), "
                    "origin_generation=:origin_generation, published_at=CAST(:published_at AS timestamptz), "
                    "relation_oids=CAST(:relation_oids AS bigint[]), "
                    "canonical_publication_ref=:publication_ref, "
                    "canonical_publication_generation=:publication_generation, "
                    "canonical_chain_ref=:chain_ref, canonical_import_date=CAST(:import_date AS date) "
                    "WHERE singleton IS TRUE RETURNING singleton, local_lineage_id, local_generation, "
                    "origin_lineage_id, origin_generation, published_at, relation_oids, "
                    "canonical_publication_ref, canonical_publication_generation, "
                    "canonical_chain_ref, canonical_import_date"
                ),
                {
                    **generation_params_by_name,
                    "publication_ref": None if provenance is None else provenance.publication_ref,
                    "publication_generation": None if provenance is None else provenance.publication_generation,
                    "chain_ref": None if provenance is None else provenance.chain_ref,
                    "import_date": None if provenance is None else provenance.import_date,
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    if updated_row is None:
        raise RuntimeError("NPI result generation authority is unavailable")
    adopted_authority = validate_npi_result_generation_authority(updated_row)
    if (
        adopted_authority.local_lineage_id != current.local_lineage_id
        or adopted_authority.local_generation != current.local_generation
    ):
        raise RuntimeError("NPI local result generation changed during adoption")
    return adopted_authority


def require_npi_automatic_generation_order(candidate: object, incumbent: object) -> None:
    """Require a strictly newer result revision from the same origin lineage."""

    candidate_generation = validate_npi_serving_generation(candidate)
    incumbent_generation = validate_npi_serving_generation(incumbent)
    if (
        candidate_generation.origin_lineage_id != incumbent_generation.origin_lineage_id
        or candidate_generation.origin_generation <= incumbent_generation.origin_generation
    ):
        raise ValueError("NPI automatic generation order is unsupported")


__all__ = [
    "NpiCanonicalProvenance",
    "NpiResultGenerationAuthority",
    "NpiServingGeneration",
    "RELATION_NAMES",
    "bootstrap_npi_result_generation",
    "capture_npi_serving_generation",
    "current_npi_relation_oids",
    "install_npi_result_revision_guards",
    "install_npi_stage_mutation_guards",
    "publish_adopted_npi_result_generation",
    "publish_local_npi_result_generation",
    "read_npi_result_generation_authority",
    "require_npi_automatic_generation_order",
    "validate_npi_canonical_provenance",
    "validate_npi_result_generation_authority",
    "validate_npi_serving_generation",
]
