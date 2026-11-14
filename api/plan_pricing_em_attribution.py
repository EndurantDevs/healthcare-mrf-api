# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable, release-bound E&M prices with canonical binding attribution."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, fields, replace
from types import SimpleNamespace

import asyncpg
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from api import plan_pricing_em_distance_build as builder
from api.plan_pricing_em_distance import EM_CODES
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding
from db.connection import db
from process.network_address_projection import _identifier
from process.network_bootstrap_sources import _content, _roles, _transaction
from process.network_custom_address_source import _digest
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

ATTRIBUTION_CONTRACT = "plan_pricing_em_attribution_v1"
_MAX_RECEIPT_BYTES = 1024 * 1024
_MAX_IDENTITY_BYTES = 512 * 1024
_HEX = re.compile(r"[0-9a-f]{64}", re.ASCII)
_ID = r"[0-9A-HJKMNP-TV-Z]{26}"
_COLUMNS = (
    ("binding_ordinal", "integer"),
    ("npi", "bigint"),
    ("code_index", "smallint"),
    ("minimum_rate", "numeric"),
    ("maximum_rate", "numeric"),
    ("rate_count", "bigint"),
)
_CONSTRAINT_KEYS = {
    "rates_ordinal_ck": ("c", "1"),
    "rates_npi_ck": ("c", "2"),
    "rates_code_ck": ("c", "3"),
    "rates_minimum_ck": ("c", "4"),
    "rates_maximum_ck": ("c", "4 5"),
    "rates_count_ck": ("c", "6"),
    "rates_pkey": ("p", "1 2 3"),
}


@dataclass(frozen=True)
class EMAttributionReceipt:
    identity_json: str
    identity_sha256: str
    schema_name: str
    row_count: int
    provider_count: int
    binding_ordinals: tuple[int, ...]
    code_accounting: tuple[tuple[int, int, str], ...]
    content_sha256: str
    receipt_sha256: str

    @property
    def rates_relation(self) -> str:
        """Return only the validated, fully qualified immutable rates relation."""
        _validate_receipt(self)
        return f'{_identifier(self.schema_name)}."rates"'

    def as_dict(self) -> dict:
        """Return the bounded publication document without mutable nested state."""
        return {"contract": ATTRIBUTION_CONTRACT, **asdict(self)}


def _unique_object(pairs):
    document_by_key = {}
    for key, value in pairs:
        if key in document_by_key:
            raise ValueError("duplicate attribution receipt field")
        document_by_key[key] = value
    return document_by_key


def _canonical(document):
    return json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _recipe(selection):
    if type(selection) is not PlanReleaseServingSelection:
        raise ValueError("a full serving selection is required")
    if (
        not re.fullmatch("hprelease_" + _ID, selection.plan_release_id, flags=re.ASCII)
        or not re.fullmatch("hpserve_" + _ID, selection.serving_revision_id, flags=re.ASCII)
        or _HEX.fullmatch(selection.binding_set_digest) is None
    ):
        raise ValueError("attribution release identity is invalid")
    canonical_bindings = selection.in_network_bindings
    if not 1 <= len(canonical_bindings) <= builder.MAX_PROJECTION_BINDINGS:
        raise ValueError("attribution binding bound exceeded")
    raw_bindings = []
    role_ordinals = set()
    for binding in selection.bindings:
        if (
            type(binding) is not PlanReleaseSnapshotBinding
            or type(binding.binding_ordinal) is not int
            or not (0 <= binding.binding_ordinal < 2**31)
            or binding.role not in {"in_network", "allowed_amounts"}
        ):
            raise ValueError("attribution binding identity is invalid")
        if (binding.role, binding.binding_ordinal) in role_ordinals:
            raise ValueError("attribution binding ordinals collide")
        role_ordinals.add((binding.role, binding.binding_ordinal))
        if (
            any(
                type(getattr(binding, field)) is not str or not getattr(binding, field)
                for field in ("snapshot_id", "source_key", "plan_id")
            )
            or type(binding.plan_market_type) is not str
            or type(binding.required) is not bool
            or type(binding.logical_scope_present) is not bool
        ):
            raise ValueError("attribution binding identity is invalid")
        raw_bindings.append(asdict(binding))
    recipe_by_field = {
        "contract": ATTRIBUTION_CONTRACT,
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "binding_set_digest": selection.binding_set_digest,
        "bindings": raw_bindings,
    }
    identity_json = _canonical(recipe_by_field)
    if len(identity_json.encode()) > _MAX_IDENTITY_BYTES:
        raise ValueError("attribution identity exceeds its metadata bound")
    return identity_json


def _identity(selection):
    identity_json = _recipe(selection)
    identity_sha256 = hashlib.sha256(identity_json.encode()).hexdigest()
    return identity_json, identity_sha256, "em_attribution_" + identity_sha256[:40]


def _receipt_digest(receipt):
    document_by_field = receipt.as_dict()
    document_by_field.pop("receipt_sha256")
    return _digest(document_by_field)


def _validate_receipt(receipt):
    if (
        type(receipt) is not EMAttributionReceipt
        or type(receipt.identity_json) is not str
        or len(receipt.identity_json.encode()) > _MAX_IDENTITY_BYTES
    ):
        raise ValueError("attribution receipt is invalid")
    recipe_by_field = json.loads(receipt.identity_json, object_pairs_hook=_unique_object)
    if (
        type(recipe_by_field) is not dict
        or set(recipe_by_field)
        != {"contract", "plan_release_id", "serving_revision_id", "binding_set_digest", "bindings"}
        or recipe_by_field["contract"] != ATTRIBUTION_CONTRACT
        or type(recipe_by_field["bindings"]) is not list
    ):
        raise ValueError("attribution receipt recipe is invalid")
    selection = PlanReleaseServingSelection(
        recipe_by_field["serving_revision_id"],
        recipe_by_field["plan_release_id"],
        "",
        None,
        "",
        "published",
        recipe_by_field["binding_set_digest"],
        tuple(PlanReleaseSnapshotBinding(**binding) for binding in recipe_by_field["bindings"]),
    )
    if _identity(selection) != (receipt.identity_json, receipt.identity_sha256, receipt.schema_name):
        raise ValueError("attribution receipt identity differs")
    if (
        any(
            type(fingerprint) is not str or _HEX.fullmatch(fingerprint) is None
            for fingerprint in (receipt.content_sha256, receipt.receipt_sha256)
        )
        or _receipt_digest(receipt) != receipt.receipt_sha256
    ):
        raise ValueError("attribution receipt digest differs")
    _validate_accounting(receipt, tuple(binding.binding_ordinal for binding in selection.in_network_bindings))


def _validate_accounting(receipt, canonical_ordinals):
    if (
        type(receipt.row_count) is not int
        or not 1 <= receipt.row_count <= builder.MAX_EM_ATTRIBUTED_RATE_ROWS
        or (
            type(receipt.provider_count) is not int
            or not 1 <= receipt.provider_count <= min(builder.MAX_EM_RATE_ROWS, receipt.row_count)
        )
    ):
        raise ValueError("attribution row accounting exceeds its bound")
    if (
        type(receipt.binding_ordinals) is not tuple
        or not receipt.binding_ordinals
        or receipt.binding_ordinals != tuple(sorted(set(receipt.binding_ordinals)))
        or any(type(ordinal) is not int or ordinal not in canonical_ordinals for ordinal in receipt.binding_ordinals)
    ):
        raise ValueError("attribution ordinal accounting differs")
    if type(receipt.code_accounting) is not tuple or len(receipt.code_accounting) != len(EM_CODES):
        raise ValueError("attribution code accounting differs")
    for index, stats in enumerate(receipt.code_accounting):
        if (
            type(stats) is not tuple
            or len(stats) != 3
            or type(stats[0]) is not int
            or stats[0] != index
            or (type(stats[1]) is not int or not 1 <= stats[1] <= receipt.row_count)
            or type(stats[2]) is not str
            or re.fullmatch(r"[1-9][0-9]{0,31}", stats[2], flags=re.ASCII) is None
        ):
            raise ValueError("attribution code accounting differs")
    if sum(stats[1] for stats in receipt.code_accounting) != receipt.row_count:
        raise ValueError("attribution aggregate accounting differs")


def _decode(document):
    if type(document) is not str or len(document.encode()) > _MAX_RECEIPT_BYTES:
        raise ValueError("attribution publication receipt is absent or oversized")
    receipt_by_field = json.loads(document, object_pairs_hook=_unique_object)
    if type(receipt_by_field) is not dict or receipt_by_field.pop("contract", None) != ATTRIBUTION_CONTRACT:
        raise ValueError("attribution publication contract differs")
    logical_fields = {field.name for field in fields(EMAttributionReceipt)}
    legacy_fields = {
        "owner_role",
        "runtime_roles",
        "schema_oid",
        "owner_oid",
        "table_oid",
        "table_filenode",
        "index_identities",
        "catalog_sha256",
    }
    is_legacy = set(receipt_by_field) == logical_fields | legacy_fields
    if set(receipt_by_field) != logical_fields and not is_legacy:
        raise ValueError("attribution publication fields differ")
    receipt_by_field = {name: receipt_by_field[name] for name in logical_fields}
    if type(receipt_by_field["binding_ordinals"]) is not list:
        raise ValueError("attribution receipt tuple is invalid")
    receipt_by_field["binding_ordinals"] = tuple(receipt_by_field["binding_ordinals"])
    if type(receipt_by_field["code_accounting"]) is not list or any(
        type(entry) is not list for entry in receipt_by_field["code_accounting"]
    ):
        raise ValueError("attribution receipt tuple is invalid")
    receipt_by_field["code_accounting"] = tuple(tuple(entry) for entry in receipt_by_field["code_accounting"])
    receipt = EMAttributionReceipt(**receipt_by_field)
    if is_legacy:
        # Older documents mixed physical metadata into their digest; retain only the named data recipe.
        receipt = replace(receipt, receipt_sha256=_receipt_digest(receipt))
    _validate_receipt(receipt)
    return receipt


async def _driver(session):
    if not session.in_transaction():
        raise ValueError("attribution requires a caller repeatable-read transaction")
    connection = (await (await session.connection()).get_raw_connection()).driver_connection
    await _transaction(connection)
    return connection


async def _namespace(connection, schema_name):
    return await connection.fetchrow(
        "SELECT CASE WHEN octet_length(obj_description(oid,'pg_namespace'))<=$2 "
        "THEN obj_description(oid,'pg_namespace') END AS receipt "
        "FROM pg_namespace WHERE nspname=$1",
        schema_name,
        _MAX_RECEIPT_BYTES,
    )


def _constraint_columns(constraint):
    key_columns = tuple(int(column) for column in constraint["conkey"].strip("{}").split(","))
    if constraint["contype"] == "c":
        key_columns = tuple(sorted(key_columns))
    return " ".join(map(str, key_columns))


async def _table_shape(connection, relation):
    columns = await connection.fetch(
        """SELECT a.attnum,a.attname,format_type(a.atttypid,a.atttypmod) AS type,
      a.attnotnull,a.attidentity::text,a.attgenerated::text,a.attcollation::bigint,
      pg_get_expr(d.adbin,d.adrelid) AS default_expr FROM pg_attribute a
      LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
      WHERE a.attrelid=to_regclass($1) AND a.attnum>0 ORDER BY a.attnum LIMIT 7""",
        relation,
    )
    if len(columns) != len(_COLUMNS) or any(
        dict(column)
        != {
            "attnum": index,
            "attname": name,
            "type": kind,
            "attnotnull": True,
            "attidentity": "",
            "attgenerated": "",
            "attcollation": 0,
            "default_expr": None,
        }
        for index, (column, (name, kind)) in enumerate(zip(columns, _COLUMNS, strict=True), 1)
    ):
        raise ValueError("attribution table shape differs")
    constraints = await connection.fetch(
        """SELECT conname,contype::text,conkey::text,convalidated,
      condeferrable,condeferred,pg_get_constraintdef(oid,true) AS definition FROM pg_constraint
      WHERE conrelid=to_regclass($1) AND contype<>'n' ORDER BY conname LIMIT 8""",
        relation,
    )
    if {constraint["conname"] for constraint in constraints} != set(_CONSTRAINT_KEYS) or any(
        (constraint["contype"], _constraint_columns(constraint)) != _CONSTRAINT_KEYS[constraint["conname"]]
        or constraint["convalidated"] is not True
        or constraint["condeferrable"]
        or constraint["condeferred"]
        for constraint in constraints
    ):
        raise ValueError("attribution native constraints differ")
    return {
        "columns": [dict(column) for column in columns],
        "constraints": [dict(constraint) for constraint in constraints],
    }


async def _catalog(connection, schema_name):
    """Validate the named output's columns, constraints and usable index definitions."""
    if await _namespace(connection, schema_name) is None:
        raise ValueError("attribution namespace is absent")
    relations = await connection.fetch(
        """SELECT c.relname,c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,
      c.relhastriggers,c.relhasrules,parent.relname AS table_name,i.indisvalid,i.indisready,i.indislive,
      i.indisprimary,i.indisunique,i.indnatts,i.indnkeyatts,i.indkey::text,
      i.indpred IS NULL AS no_predicate,i.indexprs IS NULL AS no_expression,am.amname
      FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid
      LEFT JOIN pg_class parent ON parent.oid=i.indrelid LEFT JOIN pg_am am ON am.oid=c.relam
      WHERE c.relnamespace=to_regnamespace($1) ORDER BY c.relname LIMIT 4""",
        schema_name,
    )
    _validate_relations(relations)
    relation = f'{_identifier(schema_name)}."rates"'
    if await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=to_regnamespace($1))
      OR EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=to_regclass($2) OR inhparent=to_regclass($2))""",
        schema_name,
        relation,
    ):
        raise ValueError("attribution contains routines or inherited relations")
    return {"relations": [dict(row) for row in relations], "shape": await _table_shape(connection, relation)}


def _validate_relations(relations):
    if len(relations) != 3 or tuple(relation["relname"] for relation in relations) != (
        "rates",
        "rates_lookup",
        "rates_pkey",
    ):
        raise ValueError("attribution object inventory differs")
    for relation in relations:
        if relation["relpersistence"] != b"p" or any(
            relation[field] for field in ("relrowsecurity", "relforcerowsecurity", "relhastriggers", "relhasrules")
        ):
            raise ValueError("attribution native storage differs")
        if relation["relname"] == "rates":
            if relation["relkind"] != b"r":
                raise ValueError("attribution heap differs")
        elif (
            relation["relkind"] != b"i"
            or relation["table_name"] != "rates"
            or not all(
                relation[field] is True
                for field in ("indisvalid", "indisready", "indislive", "no_predicate", "no_expression")
            )
            or relation["amname"] != "btree"
            or relation["indnatts"] != 3
            or relation["indnkeyatts"] != 3
            or (relation["indkey"] != ("1 2 3" if relation["relname"] == "rates_pkey" else "2 3 1"))
            or relation["indisprimary"] != (relation["relname"] == "rates_pkey")
            or (relation["indisunique"] != (relation["relname"] == "rates_pkey"))
        ):
            raise ValueError("attribution native index differs")


async def _verify(connection, receipt, identity_json, *, api_read=False):
    if type(api_read) is not bool:
        raise ValueError("attribution read mode is invalid")
    if api_read:
        from process.network_address_artifact import _require_api_read_context

        await _require_api_read_context(connection, True)
    _validate_receipt(receipt)
    if receipt.identity_json != identity_json:
        raise ValueError("attribution full recipe differs")
    namespace = await _namespace(connection, receipt.schema_name)
    if namespace is None or _decode(namespace["receipt"]) != receipt:
        raise ValueError("attribution publication differs")
    await connection.fetch(
        "SELECT binding_ordinal,npi,code_index,minimum_rate,maximum_rate,rate_count "
        f"FROM {receipt.rates_relation} LIMIT 0"
    )
    return receipt


async def _create_rates(connection, schema_name):
    relation = f'{_identifier(schema_name)}."rates"'
    await connection.execute(f"CREATE SCHEMA {_identifier(schema_name)}")
    await connection.execute(f"""CREATE TABLE {relation} (
      binding_ordinal integer NOT NULL CONSTRAINT rates_ordinal_ck CHECK(binding_ordinal>=0),
      npi bigint NOT NULL CONSTRAINT rates_npi_ck CHECK(npi>0),
      code_index smallint NOT NULL CONSTRAINT rates_code_ck CHECK(code_index BETWEEN 0 AND 5),
      minimum_rate numeric NOT NULL CONSTRAINT rates_minimum_ck
        CHECK(minimum_rate>=0 AND minimum_rate::text NOT IN ('NaN','Infinity','-Infinity')),
      maximum_rate numeric NOT NULL CONSTRAINT rates_maximum_ck
        CHECK(maximum_rate>=minimum_rate AND maximum_rate::text NOT IN ('NaN','Infinity','-Infinity')),
      rate_count bigint NOT NULL CONSTRAINT rates_count_ck CHECK(rate_count>0),
      CONSTRAINT rates_pkey PRIMARY KEY(binding_ordinal,npi,code_index))""")
    await connection.execute(
        f"INSERT INTO {relation} SELECT binding_ordinal,npi,code_index,minimum_rate,maximum_rate,rate_count "
        "FROM plan_pricing_em_distance_rate_stage"
    )
    await connection.execute(f"CREATE INDEX rates_lookup ON {relation}(npi,code_index,binding_ordinal)")
    mismatch = await connection.fetchval(f"""SELECT EXISTS(
      (SELECT * FROM {relation} EXCEPT ALL SELECT * FROM plan_pricing_em_distance_rate_stage)
      UNION ALL (SELECT * FROM plan_pricing_em_distance_rate_stage EXCEPT ALL SELECT * FROM {relation}))""")
    if mismatch is not False:
        raise ValueError("attribution copied prices differ")


async def _accounting(connection, schema_name):
    relation = f'{_identifier(schema_name)}."rates"'
    has_count_overflow = await connection.fetchval(f"""SELECT EXISTS(
      SELECT 1 FROM {relation} GROUP BY npi,code_index
      HAVING sum(rate_count::numeric)>9223372036854775807
      ORDER BY npi,code_index LIMIT 1)""")
    if has_count_overflow is not False:
        raise ValueError("attribution provider code count exceeds signed bigint")
    totals = await connection.fetchrow(f"""SELECT count(*)::bigint AS row_count,count(DISTINCT npi)::bigint AS provider_count,
      array_agg(DISTINCT binding_ordinal ORDER BY binding_ordinal) AS ordinals FROM {relation}""")
    codes = await connection.fetch(
        f"SELECT code_index,count(*)::bigint AS row_count,sum(rate_count::numeric)::text AS count "
        f"FROM {relation} GROUP BY code_index ORDER BY code_index LIMIT 7"
    )
    return (
        totals["row_count"],
        totals["provider_count"],
        tuple(totals["ordinals"] or ()),
        tuple((code["code_index"], code["row_count"], code["count"]) for code in codes),
    )


async def _publish(connection, selection, identity, owner_role, runtime_roles):
    schema_name = identity[2]
    _roles(owner_role, runtime_roles)
    await _create_rates(connection, schema_name)
    publisher = await connection.fetchval("SELECT quote_ident(current_user)")
    try:
        await freeze_fhir_source_snapshot(
            connection,
            schema_name,
            namespace_owner=owner_role,
            reader_roles=runtime_roles,
            application_roles=runtime_roles,
        )
        await connection.execute(f"SET LOCAL ROLE {_identifier(owner_role)}")
        await _catalog(connection, schema_name)
        accounting = await _accounting(connection, schema_name)
        _validate_accounting(
            SimpleNamespace(
                row_count=accounting[0],
                provider_count=accounting[1],
                binding_ordinals=accounting[2],
                code_accounting=accounting[3],
            ),
            tuple(binding.binding_ordinal for binding in selection.in_network_bindings),
        )
        row_count, content_sha256 = await _content(connection, schema_name, "rates")
        if row_count != accounting[0]:
            raise ValueError("attribution native content accounting differs")
        receipt = EMAttributionReceipt(*identity, *accounting, content_sha256, "")
        receipt = replace(receipt, receipt_sha256=_receipt_digest(receipt))
        _validate_receipt(receipt)
        document = _canonical(receipt.as_dict())
        if len(document.encode()) > _MAX_RECEIPT_BYTES:
            raise ValueError("attribution publication receipt exceeds its bound")
        quoted = await connection.fetchval("SELECT quote_literal($1::text)", document)
        await connection.execute(f"COMMENT ON SCHEMA {_identifier(schema_name)} IS {quoted}")
        return await _verify(connection, receipt, identity[0])
    finally:
        await connection.execute(f"SET LOCAL ROLE {publisher}")


async def _lock_preparation(connection, identity_sha256):
    """Refuse a competing builder without waiting on an already pinned snapshot."""
    if type(identity_sha256) is not str or _HEX.fullmatch(identity_sha256) is None:
        raise ValueError("attribution preparation lock identity is invalid")
    is_locked = await connection.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text,0))", identity_sha256
    )
    if is_locked is not True:
        raise ValueError("attribution preparation is busy; retry in a fresh transaction")


async def prepare_em_attribution_in_session(
    session, *, plan_release_id, serving_revision_id, owner_role, runtime_roles
):
    """Build and close one isolated artifact atomically, or replay its exact receipt."""
    try:
        connection = await _driver(session)
        selection = await builder._selected_release(session, plan_release_id, serving_revision_id)
        identity = _identity(selection)
        _roles(owner_role, runtime_roles)
        async with session.begin_nested():
            await _lock_preparation(connection, identity[1])
            namespace = await _namespace(connection, identity[2])
            if namespace is not None:
                receipt = _decode(namespace["receipt"])
                return await _verify(connection, receipt, identity[0])
            await builder._create_stage_tables(session)
            await session.execute(text(builder._CREATE_RATE_STAGE_SQL))
            projections = await builder._binding_projections(session, selection)
            await builder._stage_em_rates(session, builder._BuildState(hashlib.sha256()), projections)
            return await _publish(connection, selection, identity, owner_role, runtime_roles)
    except (
        asyncpg.PostgresError,
        SQLAlchemyError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        IndexError,
        OverflowError,
        UnicodeError,
        RecursionError,
    ):
        raise PlanPricingProjectionUnavailable("E&M attribution preparation is unavailable") from None


async def _resolve_em_attribution_api(session, selection):
    """Resolve the exact release recipe and named rates in the caller read-only snapshot."""
    from api.network_read_execution import _read_driver
    from process.network_address_artifact import _require_api_read_context

    try:
        identity = _identity(selection)
        connection = await _read_driver(session)
        await _require_api_read_context(connection, True)
        current = await builder._selected_release(session, selection.plan_release_id, selection.serving_revision_id)
        if _identity(current) != identity:
            raise ValueError("attribution selection is stale or filtered")
        namespace = await _namespace(connection, identity[2])
        if namespace is None:
            raise ValueError("attribution publication is absent")
        receipt = await _verify(connection, _decode(namespace["receipt"]), identity[0], api_read=True)
        return receipt
    except Exception:
        raise PlanPricingProjectionUnavailable("E&M attribution is unavailable") from None


async def resolve_em_attribution(session, selection, *, api_read=False):
    """Verify the full current release recipe and named publication without rescanning rates."""
    if type(api_read) is not bool:
        raise PlanPricingProjectionUnavailable("E&M attribution is unavailable")
    if api_read:
        return await _resolve_em_attribution_api(session, selection)
    try:
        identity = _identity(selection)
        connection = await _driver(session)
        current = await builder._selected_release(session, selection.plan_release_id, selection.serving_revision_id)
        if _identity(current) != identity:
            raise ValueError("attribution selection is stale or filtered")
        namespace = await _namespace(connection, identity[2])
        if namespace is None:
            raise ValueError("attribution publication is absent")
        return await _verify(connection, _decode(namespace["receipt"]), identity[0])
    except (
        asyncpg.PostgresError,
        SQLAlchemyError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        IndexError,
        OverflowError,
        UnicodeError,
        RecursionError,
    ):
        raise PlanPricingProjectionUnavailable("E&M attribution is unavailable") from None


async def build_plan_pricing_em_attribution(
    *, plan_release_id: str, serving_revision_id: str, owner_role: str, runtime_roles: list[str]
) -> dict:
    """Prepare an exact release artifact in one owned repeatable-read transaction."""
    if (
        type(runtime_roles) is not list
        or not 1 <= len(runtime_roles) <= 32
        or any(type(role) is not str for role in runtime_roles)
    ):
        raise ValueError("attribution runtime_roles must be a sorted unique bounded JSON list of role names")
    role_names = tuple(runtime_roles)
    _roles(owner_role, role_names)
    async with db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        receipt = await prepare_em_attribution_in_session(
            session,
            plan_release_id=plan_release_id,
            serving_revision_id=serving_revision_id,
            owner_role=owner_role,
            runtime_roles=role_names,
        )
        return receipt.as_dict()
