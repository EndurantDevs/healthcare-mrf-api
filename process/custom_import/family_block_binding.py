# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed family-block layout resolution after a protected build or reader pin."""

import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from sqlalchemy import BigInteger, Boolean, Integer, LargeBinary, Numeric, SmallInteger, Text, text
from sqlalchemy.dialects.postgresql import TIMESTAMP

from process.custom_import.family_block_layout import ROLE_SPECS, FamilyBlockLayout, index_specifications
from process.custom_import.query_projection_binding import _CATALOG_IDENTITY, QueryProjectionUnavailable, _digest
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_schema


def _catalog_identities():
    """Keep every canonical custody expression; batch only the name predicate."""
    source = str(_CATALOG_IDENTITY)
    substitutions = (
        (
            "SELECT c.oid::bigint AS table_oid",
            "SELECT c.relname::text AS table_name, "
            "pg_catalog.obj_description(c.oid,'pg_class') AS table_comment, c.oid::bigint AS table_oid",
        ),
        ("c.relname=:table_name", "c.relname=ANY(CAST(:table_names AS text[]))"),
    )
    for previous, changed in substitutions:
        if source.count(previous) != 1:
            raise RuntimeError("family block canonical catalog query changed")
        source = source.replace(previous, changed, 1)
    return text(source)


_CATALOG_IDENTITIES = _catalog_identities()


class FamilyBlockUnavailable(CandidateRunnerError):
    """A required layout, scope, role or native custody proof differs."""


@dataclass(frozen=True)
class FamilyBlockBinding:
    family_id: int
    layout: FamilyBlockLayout
    tables_by_role: object
    oids_by_role: object
    owner_oid: int
    columns_sha256_by_role: Mapping[str, bytes] | None = None

    def manifest(self, row):
        """Resolve a projection manifest against this checked family layout."""
        return self.layout.manifest(row)


def _column_shape(column, ordinal):
    kind = column.type
    match kind:
        case BigInteger():
            name = "bigint"
        case SmallInteger():
            name = "smallint"
        case Integer():
            name = "integer"
        case Boolean():
            name = "boolean"
        case Numeric():
            name = "numeric" if kind.precision is None else f"numeric({kind.precision},{kind.scale})"
        case LargeBinary():
            name = "bytea"
        case TIMESTAMP():
            name = "timestamp with time zone" if kind.timezone else "timestamp without time zone"
        case Text():
            name = "text"
        case _:
            # Common roles also retain varchar and JSONB; use their native compilation.
            from sqlalchemy.dialects import postgresql

            name = str(kind.compile(dialect=postgresql.dialect())).lower().replace("varchar", "character varying")
    collation = (
        ["pg_catalog", kind.collation or "default"]
        if isinstance(kind, Text) or hasattr(kind, "collation")
        else [None, None]
    )
    default = str(column.server_default.arg) if column.server_default is not None else None
    return [ordinal, column.name, name, not column.nullable, "", "", default, *collation]


def checked_binding(family_id, definition, collection_slots, comment, catalog_rows, *, persistence):
    """Check catalog observations, not cached database authority or caller SQL."""
    if persistence not in {"logged", "candidate"}:
        raise ValueError("family persistence expectation is invalid")
    try:
        layout = FamilyBlockLayout(definition, collection_slots)
        if comment != layout.comment:
            raise ValueError("layout descriptor differs")
        tables = layout.tables(family_id)
        by_role = {catalog_row["role_name"]: catalog_row for catalog_row in catalog_rows}
        if len(catalog_rows) != len(ROLE_SPECS) or set(by_role) != set(ROLE_SPECS):
            raise ValueError("role census differs")
        oids, signatures = {}, {}
        owner = None
        for role, table in tables.items():
            catalog_row = by_role[role]
            oid = catalog_row["table_oid"]
            if type(oid) is not int or oid <= 0 or oid in oids.values():
                raise ValueError("role identity differs")
            if owner is None:
                owner = catalog_row["table_owner"]
            if not owner or catalog_row["table_owner"] != owner or catalog_row["schema_owner"] != owner:
                raise ValueError("role ownership differs")
            if catalog_row["relkind"] != "r" or catalog_row["relpersistence"] not in (
                {"p"} if persistence == "logged" else {"p", "u"}
            ):
                raise ValueError("role persistence differs")
            if any(catalog_row[name] for name in ("inherited", "foreign_keys", "triggers")):
                raise ValueError("role storage hooks differ")
            expected_columns = [_column_shape(column, n) for n, column in enumerate(table.c, 1)]
            if catalog_row["columns"] != expected_columns or catalog_row["primary_key"] != list(
                table.primary_key.columns.keys()
            ):
                raise ValueError("role column or primary-key shape differs")
            oids[role] = oid
            signatures[role] = _digest(catalog_row["columns_sha256"], "native column signature")
        return FamilyBlockBinding(
            family_id, layout, tables, MappingProxyType(oids), owner, MappingProxyType(signatures)
        )
    except (TypeError, ValueError, KeyError, AttributeError, QueryProjectionUnavailable) as error:
        raise FamilyBlockUnavailable("family block layout custody differs") from error


def _positive_identity(value):
    if type(value) is not int or not 0 < value < 2**63:
        raise QueryProjectionUnavailable("family block projection identity is invalid")
    return value


def check_projection_manifest(binding, manifest):
    """Match manifest custody to existing native observations without catalog IO."""
    try:
        if (
            type(binding) is not FamilyBlockBinding
            or not isinstance(binding.oids_by_role, Mapping)
            or not isinstance(binding.columns_sha256_by_role, Mapping)
            or _positive_identity(manifest.family_id) != _positive_identity(binding.family_id)
        ):
            raise QueryProjectionUnavailable("family block projection snapshot differs")
        owner = _positive_identity(binding.owner_oid)
        observed_oids = []
        for scope in ("root", "child"):
            role = "query_" + scope
            oid = _positive_identity(binding.oids_by_role[role])
            observed_oids.append(oid)
            signature = _digest(binding.columns_sha256_by_role[role], "native column signature")
            if (
                _positive_identity(getattr(manifest, f"{scope}_table_oid")) != oid
                or _positive_identity(getattr(manifest, f"{scope}_table_owner")) != owner
                or not hmac.compare_digest(
                    signature, _digest(getattr(manifest, f"{scope}_columns_sha256"), "column signature")
                )
            ):
                raise QueryProjectionUnavailable("family block projection retained table identity differs")
        if observed_oids[0] == observed_oids[1]:
            raise QueryProjectionUnavailable("family block projection relation identities repeat")
    except (AttributeError, KeyError, TypeError) as error:
        raise QueryProjectionUnavailable("family block projection custody is missing") from error


async def resolve_family_block_binding(
    session, family_id, definition, collection_slots, *, persistence="logged", prepare=None
):
    """The caller must first use the existing protected build/generation resolver.

    Catalog comments are public metadata, not new control-table SELECT grants.
    The protected resolver has already compared their digest with owner custody.
    A missing comment returns None only for the exact original fifteen-role set.
    """
    from db.models.custom_import_storage import CustomImportSnapshotFamily

    if prepare:
        await prepare(session)
    connection = await session.connection()
    model_schema = CustomImportSnapshotFamily.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(model_schema, model_schema)
    if not schema:
        raise FamilyBlockUnavailable("family block control schema is missing")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    catalog_result = await session.execute(
        text(f"SELECT * FROM {quoted}.resolve_custom_import_snapshot_relations(CAST(:family_id AS bigint))"),
        {"family_id": family_id},
    )
    registration = catalog_result.all()
    registered_by_slot = dict(registration)
    if len(registered_by_slot) != len(registration):
        raise FamilyBlockUnavailable("family block registered roles repeat")
    if set(registered_by_slot) == set(range(1, 16)):
        return None
    if set(registered_by_slot) != {slot for slot, _ in ROLE_SPECS.values()}:
        raise FamilyBlockUnavailable("family block registered role census differs")
    return await _resolve_catalog_binding(
        session, family_id, definition, collection_slots, registered_by_slot, persistence=persistence, prepare=prepare
    )


async def resolve_pinned_family_block_binding(session, pin, definition, collection_slots):
    """Use a current protected generation pin without another EXECUTE capability."""
    from process.custom_import.read_contracts import CustomImportReadUnavailableError
    from process.custom_import.read_identity import GenerationSnapshotBinding

    if type(pin) is not GenerationSnapshotBinding:
        raise FamilyBlockUnavailable("family block generation pin is missing")
    try:
        registered = await pin.checked_block_oids(session)
        binding = await _resolve_catalog_binding(
            session,
            pin.family_id,
            definition,
            collection_slots,
            registered,
            persistence="logged",
            prepare=pin.checked_block_oids,
        )
        if await pin.checked_block_oids(session) != registered:
            raise FamilyBlockUnavailable("family block generation pin changed")
    except CustomImportReadUnavailableError as error:
        raise FamilyBlockUnavailable("family block generation pin differs") from error
    return binding


async def _resolve_catalog_binding(
    session, family_id, definition, collection_slots, registered, *, persistence, prepare
):
    if prepare:
        await prepare(session)
    observations = (
        (
            await session.execute(
                _CATALOG_IDENTITIES,
                {
                    "schema_name": snapshot_schema(family_id),
                    "table_names": [name for _, name in ROLE_SPECS.values()],
                },
            )
        )
        .mappings()
        .all()
    )
    by_name = {catalog_row["table_name"]: catalog_row for catalog_row in observations}
    if len(observations) != len(ROLE_SPECS) or set(by_name) != {name for _, name in ROLE_SPECS.values()}:
        raise FamilyBlockUnavailable("family block native relation census differs")
    catalog_rows = []
    for role, (slot, name) in ROLE_SPECS.items():
        catalog_row = by_name[name]
        if catalog_row["table_oid"] != registered[slot]:
            raise FamilyBlockUnavailable("family block native relation identity differs")
        catalog_rows.append({**catalog_row, "role_name": role})
    comment = by_name[ROLE_SPECS["family"][1]]["table_comment"]
    binding = checked_binding(family_id, definition, collection_slots, comment, catalog_rows, persistence=persistence)
    if persistence == "logged":
        await validate_family_block_indexes(session, binding, prepare=prepare)
    return binding


async def has_family_block_indexes(session, binding, *, prepare=None):
    """Check exact full custody, or report zero expected INDEX observations.

    Partial or mismatched sets refuse. Other same-name relation kinds remain
    subject to PostgreSQL's unchanged CREATE INDEX collision checks.
    """
    from process.custom_import.query_projection_binding import _CATALOG_INDEXES, _is_expected_index

    expected_by_name = {}
    for role, name, columns in index_specifications(binding.family_id, binding.layout):
        table = binding.tables_by_role[role]
        collations = [
            "pg_catalog.C"
            if column == "canonical_value"
            else (
                "pg_catalog." + (table.c[column].type.collation or "default")
                if hasattr(table.c[column].type, "collation")
                else None
            )
            for column in columns
        ]
        expected_by_name[name] = (binding.oids_by_role[role], binding.owner_oid, list(columns), collations)
    if prepare:
        await prepare(session)
    observations = (
        (
            await session.execute(
                _CATALOG_INDEXES,
                {"schema_name": snapshot_schema(binding.family_id), "index_names": list(expected_by_name)},
            )
        )
        .mappings()
        .all()
    )
    if not observations:
        return False
    if len(observations) != len(expected_by_name):
        raise FamilyBlockUnavailable("family block serving index census differs")
    for observation in observations:
        shape = expected_by_name.pop(observation["index_name"], None)
        if shape is None or not _is_expected_index(observation, shape):
            raise FamilyBlockUnavailable("family block serving index custody differs")
    return True


async def validate_family_block_indexes(session, binding, *, prepare=None):
    """Require every fixed native serving index; no captured index facts are cached."""
    if not await has_family_block_indexes(session, binding, prepare=prepare):
        raise FamilyBlockUnavailable("family block serving index census differs")
