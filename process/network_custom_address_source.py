# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare a protected address artifact from retained sources and approved offices."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from uuid import UUID

import asyncpg

from db.models.entity_address_unified import EntityAddressUnified
from db.registry_schema import registry_schema
from process.network_address_projection import (
    NetworkAddressProjectionError,
    PinnedAddressSource,
    _copy_projection_rows,
    _identifier,
    _projection_columns,
)
from process.network_approved_site_source import copy_approved_site_rows, verify_approved_site_sources


class NetworkCustomAddressSourceError(ValueError):
    """The exact retained recipe, source identity or writer closure differs."""


class NetworkCustomAddressUnresolved(NetworkCustomAddressSourceError):
    """An approved custom office cannot resolve to a supported exact identity."""


@dataclass(frozen=True)
class CustomAddressSourceReceipt:
    composition_id: str
    approved_revision: int
    control_schema: str
    base_source: PinnedAddressSource
    npi_source: PinnedAddressSource | None
    custom_pair_count: int
    approved_records_sha256: str
    generation_sha256: str
    address_source: PinnedAddressSource
    ptg_carry_sha256: str | None = None
    aca_carry_sha256: str | None = None
    source_aliases: bool = False
    fhir_carry_sha256: str | None = None
    revision: int = 7

    def __post_init__(self):
        if type(self.revision) is not int or self.revision not in {2, 3, 4, 5, 6, 7}:
            raise NetworkCustomAddressSourceError("Composition receipt revision is invalid")
        if type(self.source_aliases) is not bool:
            raise NetworkCustomAddressSourceError("Source alias layout is invalid")
        for name in ("ptg_carry_sha256", "aca_carry_sha256", "fhir_carry_sha256"):
            digest = getattr(self, name)
            if digest is not None and (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise NetworkCustomAddressSourceError("Address carry digest is invalid")

    def as_dict(self):
        """Return the complete logical recipe and its named address edition."""
        return {"component": "network_custom_address_source", **asdict(self)}


def _digest(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _generation(receipt):
    if receipt.revision < 7:
        return receipt.generation_sha256
    document = receipt.as_dict()
    document.pop("generation_sha256")
    document.pop("address_source")
    return _digest(document)


def _arguments(base_source, composition_id, approved_revision, owner_role, runtime_roles, npi_source):
    try:
        parsed = UUID(composition_id) if type(composition_id) is str else None
    except ValueError:
        parsed = None
    if parsed is None or not parsed.int or str(parsed) != composition_id:
        raise NetworkCustomAddressSourceError("Composition identity is invalid")
    if (
        type(base_source) is not PinnedAddressSource
        or type(approved_revision) is not int
        or not 0 <= approved_revision <= 9223372036854775807
    ):
        raise NetworkCustomAddressSourceError("Pinned composition input is invalid")
    if npi_source is not None and (type(npi_source) is not PinnedAddressSource or npi_source.table_name != "npi"):
        raise NetworkCustomAddressSourceError("Pinned NPI source is invalid")
    schema_name = "network_composition_" + parsed.hex
    if base_source.schema_name == schema_name or (npi_source is not None and npi_source.schema_name == schema_name):
        raise NetworkCustomAddressSourceError("Composition sources must be external retained relations")
    return schema_name


async def _require_transaction(connection):
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        raise NetworkCustomAddressSourceError("Composition requires a caller repeatable-read transaction")


# Let exact kind/key predicates use the approved-record primary key.
_APPROVED_CTE = """WITH approved AS NOT MATERIALIZED (
 SELECT * FROM {control}.registry_approved_record WHERE approved_revision=$1
   AND record_kind IN ('provider','location','membership','site_binding')
), pairs AS MATERIALIZED (
 SELECT DISTINCT member.provider_system,member.provider_id,member.location_id
 FROM approved head CROSS JOIN LATERAL jsonb_to_recordset(
   CASE WHEN head.record_kind='membership' AND head.record_json->'archived'='false'::jsonb
     THEN head.record_json->'memberships_json' ELSE '[]'::jsonb END)
   AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text,operation text)
 WHERE coalesce(member.operation,'add')='add'
), custom AS MATERIALIZED (
 SELECT pair.*,location.record_json AS site,provider.record_json AS provider,
   encode(sha256(convert_to(jsonb_build_array('network_custom',pair.provider_system,pair.provider_id,pair.location_id)::text,'UTF8')),'hex') AS location_key
 FROM pairs pair JOIN approved location ON location.record_kind='location' AND location.record_key=pair.location_id::text
 LEFT JOIN approved provider ON provider.record_kind='provider' AND provider.record_key=pair.provider_id
)
"""


async def _approved_identity(connection, control, revision, *, retained=False):
    current = await connection.fetchval(f"SELECT approved_revision FROM {control}.registry_revision_control WHERE id=1")
    if not retained and current != revision:
        raise NetworkCustomAddressSourceError("Approved revision differs from the pinned recipe")
    summary = await connection.fetchrow(
        _APPROVED_CTE.format(control=control)
        + """
 SELECT (SELECT encode(sha256(convert_to(coalesce(string_agg(
   record_kind||':'||record_key||':'||record_revision||':'||custom_revision||':'||
     encode(sha256(convert_to(record_json::text,'UTF8')),'hex'),',' ORDER BY record_kind,record_key),''),'UTF8')),'hex')
   FROM approved) AS fingerprint,count(*)::bigint AS pairs,
   count(*) FILTER(WHERE custom.provider_system='npi')::bigint AS npi_pairs,
   count(*) FILTER(WHERE (custom.location_id IS NULL AND source_binding.count<>1)
     OR (custom.location_id IS NOT NULL AND source_binding.count<>0)
     OR (custom.location_id IS NOT NULL AND (
       custom.provider_system NOT IN ('manual','npi') OR coalesce(site->'archived'<>'false'::jsonb,true)
       OR (custom.provider_system='manual' AND (provider IS NULL OR coalesce(provider->'archived'<>'false'::jsonb,true)))
       OR coalesce(site->'canonical_address_json'->>'identity_key' NOT LIKE 'v2|%|street',true)
       OR site->'canonical_address_json'->>'address_key' IS NULL)))::bigint AS unresolved
 FROM pairs pair LEFT JOIN custom USING(provider_system,provider_id,location_id)
 LEFT JOIN LATERAL (
   SELECT count(*) AS count FROM approved binding WHERE binding.record_kind='site_binding'
     AND binding.record_json->'archived'='false'::jsonb
     AND (binding.record_json->>'provider_system',binding.record_json->>'provider_id',binding.record_json->>'location_id')
       =(pair.provider_system,pair.provider_id,pair.location_id::text)
 ) source_binding ON true
 """,
        revision,
    )
    if summary["unresolved"]:
        raise NetworkCustomAddressUnresolved("Approved custom office identity is unresolved")
    return summary


async def _verify_retained_rows(connection, base_table, address_table, copied_rows):
    accounting = await connection.fetchrow(f"""SELECT
      (SELECT count(*) FROM {base_table} WHERE row_origin<>'network_custom') AS expected_rows,
      (SELECT count(*) FROM {address_table}) AS actual_rows,
      EXISTS((SELECT * FROM {base_table} WHERE row_origin<>'network_custom'
        EXCEPT ALL SELECT * FROM {address_table}) UNION ALL
        (SELECT * FROM {address_table} EXCEPT ALL
         SELECT * FROM {base_table} WHERE row_origin<>'network_custom')) AS differs""")
    if copied_rows != accounting["expected_rows"] or copied_rows != accounting["actual_rows"] or accounting["differs"]:
        raise NetworkCustomAddressSourceError("Retained address COPY accounting or full-row parity differs")


def _has_source_aliases(receipts):
    aliases_by_key = {}
    for receipt in receipts:
        for row in receipt.records:
            alias = (row.provider_system, row.provider_id, row.location_id)
            if aliases_by_key.setdefault(row.location_key, alias) != alias:
                return True
    return False


async def _build_heaps(connection, recipe, schema_name, control, office_inputs):
    base_source, npi_source = recipe.base_source, recipe.npi_source
    revision = recipe.approved_revision
    site_sources = await verify_approved_site_sources(
        connection, approved_revision=revision, control_schema=control[1:-1]
    )
    namespace = _identifier(schema_name)
    address_table = namespace + ".entity_address_unified"
    base_table = f"{_identifier(base_source.schema_name)}.{_identifier(base_source.table_name)}"
    npi_table = f"{_identifier(npi_source.schema_name)}.{_identifier(npi_source.table_name)}" if npi_source else None
    # Validate the existing native key; retention keeps every original column.
    await _projection_columns(connection, base_table)
    await _create_artifact_schema(connection, schema_name)
    await connection.execute(f"CREATE TABLE {address_table} (LIKE {base_table} INCLUDING DEFAULTS)")
    await connection.execute(f"ALTER TABLE {address_table} ADD PRIMARY KEY(location_key)")
    copied_rows = await _copy_projection_rows(
        connection,
        schema_name,
        f"SELECT a.* FROM (SELECT * FROM {base_table} WHERE row_origin<>'network_custom') a",
    )
    await _verify_retained_rows(connection, base_table, address_table, copied_rows)
    await connection.execute(f"""CREATE TABLE {namespace}.provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    npi_check = (
        f"EXISTS(SELECT 1 FROM {npi_table} npi WHERE npi.npi=custom.provider_id::bigint)" if npi_table else "false"
    )
    missing = await connection.fetchval(
        _APPROVED_CTE.format(control=control)
        + f"SELECT EXISTS(SELECT 1 FROM custom WHERE provider_system='npi' AND NOT ({npi_check}))",
        revision,
    )
    if missing:
        raise NetworkCustomAddressUnresolved("Approved source NPI identity is unresolved")
    await connection.execute(
        _APPROVED_CTE.format(control=control) + _INSERT_ADDRESS_SQL.format(address_table=address_table), revision
    )
    await connection.execute(
        _APPROVED_CTE.format(control=control)
        + f"""INSERT INTO {namespace}.provider_location_binding
      SELECT provider_system,provider_id,location_id,location_key,provider_system,provider_id FROM custom""",
        revision,
    )
    await copy_approved_site_rows(
        connection, site_sources, schema_name, exact_rows=True, reproject_network_ids=True, control_schema=control[1:-1]
    )
    return await _ptg_address_carry(connection, office_inputs, revision, control[1:-1], target_schema=schema_name)


async def _source_alias_layout(connection, schema_name, *, writing=False):
    namespace = _identifier(schema_name)
    layout = await connection.fetchrow(f"""WITH offices AS MATERIALIZED (
      SELECT location_key,count(*)>1 AS aliases,
        count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1
          OR (bool_or(provider_system='manual') AND count(*)>1) AS invalid
      FROM {namespace}.provider_location_binding GROUP BY location_key
    ) SELECT coalesce(bool_or(aliases),false) AS source_aliases,
      coalesce(bool_or(invalid),false) OR EXISTS(
        SELECT FROM {namespace}.provider_location_binding binding
        LEFT JOIN {namespace}.entity_address_unified address USING(location_key,entity_type,entity_id)
        WHERE address.location_key IS NULL) AS invalid FROM offices""")
    if layout["invalid"] is not False or type(layout["source_aliases"]) is not bool:
        raise NetworkCustomAddressSourceError("Custom address identities collide or have orphan bindings")
    if writing and not layout["source_aliases"]:
        await connection.execute(f"ALTER TABLE {namespace}.provider_location_binding ADD UNIQUE(location_key)")
    return layout["source_aliases"]


_CUSTOM_ADDRESS_COLUMNS = """entity_type,entity_id,npi,entity_name,location_key,row_origin,address_precision,checksum,type,
 first_line,second_line,city_name,state_name,postal_code,country_code,address_key,premise_key,zip5,state_code,city_norm,
 taxonomy_array,plans_network_array,procedures_array,medications_array,canonical_network_ids"""
_CUSTOM_ADDRESS_SELECT = """SELECT provider_system,provider_id,CASE WHEN provider_system='npi' THEN provider_id::bigint
   ELSE (provider->>'npi')::bigint END,
 CASE WHEN provider_system='manual' THEN provider->>'display_name' ELSE 'NPI '||provider_id END,
 location_key,'network_custom','street',('x'||substr(location_key,1,16))::bit(64)::bigint,'practice',
 site->'address_json'->>'first_line',site->'address_json'->>'second_line',site->'address_json'->>'city',
 site->'address_json'->>'state',site->'address_json'->>'zip',site->'address_json'->>'country',
 (site->'canonical_address_json'->>'address_key')::uuid,(site->'canonical_address_json'->>'premise_key')::uuid,
 site->'canonical_address_json'->>'zip5',site->'canonical_address_json'->>'state_code',site->'canonical_address_json'->>'city_norm',
 '{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[] FROM custom"""
_INSERT_ADDRESS_SQL = "INSERT INTO {address_table} (" + _CUSTOM_ADDRESS_COLUMNS + ") " + _CUSTOM_ADDRESS_SELECT


async def _freeze(connection, schema_name, owner_role, runtime_roles):
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    try:
        await freeze_fhir_source_snapshot(
            connection,
            schema_name,
            namespace_owner=owner_role,
            reader_roles=runtime_roles,
            application_roles=runtime_roles,
        )
    except (ValueError, asyncpg.PostgresError) as error:
        raise NetworkCustomAddressSourceError("Composition native freeze failed") from error


async def _create_artifact_schema(connection, schema_name):
    await connection.execute(f"CREATE SCHEMA {_identifier(schema_name)}")


async def _source_columns(connection, source, *, address):
    """Check the named source's actual columns and required data types."""
    relation = _identifier(source.schema_name) + "." + _identifier(source.table_name)
    columns = await connection.fetch(
        "SELECT attname,atttypid::regtype::text AS type,atttypmod,attnotnull,"
        "pg_get_expr(d.adbin,d.adrelid) AS default_expr FROM pg_attribute a "
        "LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
        "WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        relation,
    )
    if address:
        if sorted(column["attname"] for column in columns) != sorted(EntityAddressUnified.__table__.columns.keys()):
            raise NetworkCustomAddressSourceError("Retained unified address shape differs")
        await _projection_columns(connection, relation)
    elif not any(column["attname"] == "npi" and column["type"] == "bigint" for column in columns):
        raise NetworkCustomAddressSourceError("Retained NPI shape differs")
    return tuple(dict(column) for column in columns)


async def _office_lookup_shape(connection, schema_name):
    index = await connection.fetchrow(
        """SELECT am.amname,i.indisvalid,i.indisready,i.indisunique,
      i.indnkeyatts,i.indnatts,i.indexprs IS NULL AS plain,i.indpred IS NULL AS complete,
      ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY key(attnum,ordinal)
        JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum ORDER BY ordinal) AS columns
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_index i ON i.indexrelid=c.oid
      JOIN pg_class target ON target.oid=i.indrelid JOIN pg_am am ON am.oid=c.relam
      WHERE n.nspname=$1 AND c.relname='entity_address_unified_network_office_lookup'
        AND target.relname='entity_address_unified'""",
        schema_name,
    )
    expected = ("btree", True, True, False, 2, 2, True, True, ["npi", "address_key"])
    if index is None or tuple(index.values()) != expected:
        raise NetworkCustomAddressSourceError("Composition exact office lookup index differs")


async def _prepare_office_lookup(connection, schema_name):
    address = _identifier(schema_name) + ".entity_address_unified"
    await connection.execute(
        f"CREATE INDEX entity_address_unified_network_office_lookup ON {address} USING btree(npi,address_key)"
    )
    await connection.execute(f"ANALYZE {address}(npi,address_key)")


async def _binding_shape(connection, binding_relation, *, source_aliases=False):
    columns = await connection.fetch(
        "SELECT attname,atttypid::regtype::text AS type,atttypmod,attnotnull FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        binding_relation,
    )
    expected_columns = [
        ("provider_system", "text", -1, True),
        ("provider_id", "text", -1, True),
        ("location_id", "uuid", -1, True),
        ("location_key", "character varying", 68, True),
        ("entity_type", "text", -1, True),
        ("entity_id", "text", -1, True),
    ]
    constraints = await connection.fetch(
        "SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype IN ('p','u') ORDER BY contype",
        binding_relation,
    )
    expected_constraints = {"PRIMARY KEY (provider_system, provider_id, location_id)"}
    if not source_aliases:
        expected_constraints.add("UNIQUE (location_key)")
    if [tuple(column.values()) for column in columns] != expected_columns or {
        constraint["definition"] for constraint in constraints
    } != expected_constraints:
        raise NetworkCustomAddressSourceError("Composition binding shape differs")


async def _verify_custom_rows(connection, receipt):
    """Compare the complete approved direct contribution using indexed named data."""
    namespace = _identifier(receipt.address_source.schema_name)
    columns = tuple(column.strip() for column in _CUSTOM_ADDRESS_COLUMNS.split(","))
    # Membership publication rebuilds and validates canonical network arrays separately.
    compared_columns = tuple(column for column in columns if column != "canonical_network_ids")
    actual_values = ",".join("address." + column for column in compared_columns)
    expected_values = ",".join("expected." + column for column in compared_columns)
    matches = await connection.fetchval(
        _APPROVED_CTE.format(control=_identifier(receipt.control_schema))
        + f""", expected_addresses ({_CUSTOM_ADDRESS_COLUMNS}) AS MATERIALIZED (
            {_CUSTOM_ADDRESS_SELECT.format()}
        ), expected_bindings AS MATERIALIZED (
            SELECT provider_system,provider_id,location_id,location_key,
              provider_system AS entity_type,provider_id AS entity_id FROM custom
        ), actual_bindings AS MATERIALIZED (
            SELECT binding.provider_system,binding.provider_id,binding.location_id,binding.location_key,
              binding.entity_type,binding.entity_id FROM {namespace}.provider_location_binding binding
            LEFT JOIN {namespace}.entity_address_unified address USING(location_key)
            WHERE (binding.provider_system,binding.provider_id,binding.location_id)
              IN (SELECT provider_system,provider_id,location_id FROM expected_bindings)
              OR binding.location_key IN (SELECT location_key FROM expected_bindings)
              OR ((address.row_origin='network_custom' OR binding.location_key=encode(sha256(convert_to(
                jsonb_build_array('network_custom',binding.provider_system,binding.provider_id,binding.location_id)::text,
                'UTF8')),'hex')) AND NOT EXISTS(
                  SELECT FROM approved retained WHERE retained.record_kind='site_binding'
                    AND retained.record_json->'archived'='false'::jsonb
                    AND (retained.record_json->>'provider_system',retained.record_json->>'provider_id',
                      retained.record_json->>'location_id')
                      =(binding.provider_system,binding.provider_id,binding.location_id::text)))
        ) SELECT NOT EXISTS(
            (SELECT * FROM expected_bindings EXCEPT ALL SELECT * FROM actual_bindings)
            UNION ALL (SELECT * FROM actual_bindings EXCEPT ALL SELECT * FROM expected_bindings)
        ) AND NOT EXISTS(
            SELECT FROM expected_addresses expected LEFT JOIN {namespace}.entity_address_unified address USING(location_key)
            WHERE address.location_key IS NULL OR ({actual_values}) IS DISTINCT FROM ({expected_values})
        )""",
        receipt.approved_revision,
    )
    if matches is not True:
        raise NetworkCustomAddressSourceError("Approved custom bindings or address data differ")


def _recipe_identity(receipt):
    return (
        receipt.composition_id,
        receipt.base_source,
        receipt.npi_source,
        receipt.approved_revision,
        receipt.control_schema,
        receipt.custom_pair_count,
        receipt.approved_records_sha256,
        receipt.ptg_carry_sha256,
        receipt.aca_carry_sha256,
        receipt.source_aliases,
        receipt.fhir_carry_sha256,
    )


async def _replay_custom_artifact(connection, recipe, schema_name, office_inputs, stored):
    """Recompute every supplied carry before reusing the exact protected recipe."""
    receipt = _from_document(stored)
    carry = await _ptg_address_carry(connection, office_inputs, recipe.approved_revision, recipe.control_schema)
    aca_carry = await _aca_address_carry(connection, office_inputs, recipe, schema_name, retained_receipt=receipt)
    fhir_digest = await _fhir_address_carry(connection, office_inputs, recipe, schema_name)
    recipe = replace(
        recipe,
        ptg_carry_sha256=carry.sha256,
        aca_carry_sha256=aca_carry.sha256,
        source_aliases=receipt.source_aliases,
        fhir_carry_sha256=fhir_digest,
    )
    if _recipe_identity(recipe) != _recipe_identity(receipt):
        raise NetworkCustomAddressSourceError("Retained composition recipe differs")
    return await verify_custom_address_source(connection, receipt)


async def _prepare_artifact(connection, recipe, schema_name, control, office_inputs):
    """Complete every data carry and alias layout before recording the named recipe."""
    stored = await connection.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema_name
    )
    if stored is not None:
        return await _replay_custom_artifact(connection, recipe, schema_name, office_inputs, stored)
    carry = await _build_heaps(
        connection,
        recipe,
        schema_name,
        control,
        office_inputs,
    )
    await _prepare_office_lookup(connection, schema_name)
    aca_carry = await _aca_address_carry(connection, office_inputs, recipe, schema_name, writing=True)
    fhir_digest = await _fhir_address_carry(connection, office_inputs, recipe, schema_name, writing=True)
    has_source_aliases = await _source_alias_layout(connection, schema_name, writing=True)
    recipe = replace(
        recipe,
        ptg_carry_sha256=carry.sha256,
        aca_carry_sha256=aca_carry.sha256,
        source_aliases=has_source_aliases,
        fhir_carry_sha256=fhir_digest,
    )
    receipt = recipe
    generation = _generation(receipt)
    receipt = replace(
        receipt, generation_sha256=generation, address_source=replace(receipt.address_source, generation_id=generation)
    )
    document = json.dumps(receipt.as_dict(), sort_keys=True, separators=(",", ":"))
    if len(document.encode()) > 65536:
        raise NetworkCustomAddressSourceError("Composition receipt exceeds its bound")
    await _verify_custom_rows(connection, receipt)
    quoted_document = await connection.fetchval("SELECT quote_literal($1::text)", document)
    await connection.execute(f"COMMENT ON SCHEMA {_identifier(schema_name)} IS {quoted_document}")
    return receipt


async def _ptg_address_carry(connection, office_inputs, approved_revision, control_schema, target_schema=None):
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.registry_ptg_address_carry import RegistryPTGAddressCarry, prepare_ptg_address_carry

    source_recipes, office_authority = office_inputs[:2]
    if not source_recipes:
        return RegistryPTGAddressCarry()
    approved = await pin_approved_membership_source(
        connection, approved_revision=approved_revision, control_schema=control_schema
    )
    return await prepare_ptg_address_carry(
        connection, source_recipes, approved, control_schema, office_authority, target_schema
    )


async def _aca_address_carry(connection, office_inputs, recipe, schema_name, *, writing=False, retained_receipt=None):
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.network_legacy_membership_source import PinnedACAMembershipSource
    from process.registry_aca_address_carry import RegistryACAAddressCarry, prepare_aca_address_carry

    source_recipes, _office_authority = office_inputs[:2]
    if not any(type(source.source_pin) is PinnedACAMembershipSource for source in source_recipes):
        return RegistryACAAddressCarry()
    if retained_receipt is not None:
        await verify_custom_address_source(connection, retained_receipt)
    approved = await pin_approved_membership_source(
        connection, approved_revision=recipe.approved_revision, control_schema=recipe.control_schema
    )
    return await prepare_aca_address_carry(
        connection, source_recipes, approved, recipe.control_schema, schema_name, writing=writing
    )


async def _fhir_address_carry(connection, office_inputs, recipe, schema_name, *, writing=False):
    from process.registry_fhir_serving_preservation import carry_fhir_preservation_addresses

    preservation = office_inputs[2] if len(office_inputs) == 3 else None
    return await carry_fhir_preservation_addresses(
        connection,
        preservation,
        schema_name,
        writing=writing,
        approved_context=(recipe.approved_revision, recipe.control_schema),
    )


@dataclass(frozen=True)
class CustomAddressSourceArguments:
    composition_id: str
    approved_revision: int
    owner_role: str
    runtime_roles: tuple[str, ...]
    control_schema: str | None = None
    npi_source: PinnedAddressSource | None = None


async def prepare_custom_address_source(
    connection,
    base_source,
    *,
    composition_id,
    approved_revision,
    owner_role,
    runtime_roles,
    control_schema=None,
    npi_source=None,
):
    """Create or replay an exact artifact without adding source-recipe offices."""
    arguments = CustomAddressSourceArguments(
        composition_id, approved_revision, owner_role, runtime_roles, control_schema, npi_source
    )
    async with connection.transaction():
        receipt = await prepare_recipe_address_source(connection, base_source, arguments)
        await _freeze(connection, receipt.address_source.schema_name, owner_role, runtime_roles)
        return receipt


async def prepare_recipe_address_source(connection, base_source, arguments, office_inputs=((), None)):
    """Stage complete address data and its logical recipe in the caller snapshot."""
    if type(arguments) is not CustomAddressSourceArguments:
        raise NetworkCustomAddressSourceError("Composition arguments are required")
    composition_id, approved_revision = arguments.composition_id, arguments.approved_revision
    owner_role, runtime_roles = arguments.owner_role, arguments.runtime_roles
    control_schema, npi_source = arguments.control_schema, arguments.npi_source
    schema_name = _arguments(base_source, composition_id, approved_revision, owner_role, runtime_roles, npi_source)
    await _require_transaction(connection)
    control_name = control_schema or registry_schema()
    control = _identifier(control_name)
    try:
        async with connection.transaction():
            summary = await _approved_identity(connection, control, approved_revision)
            await _source_columns(connection, base_source, address=True)
            effective_npi_source = npi_source or (
                PinnedAddressSource(base_source.schema_name, "npi", base_source.generation_id)
                if summary["npi_pairs"]
                else None
            )
            if effective_npi_source is not None:
                await _source_columns(connection, effective_npi_source, address=False)
            recipe = CustomAddressSourceReceipt(
                composition_id,
                approved_revision,
                control_name,
                base_source,
                effective_npi_source,
                summary["pairs"],
                summary["fingerprint"],
                "",
                PinnedAddressSource(schema_name, "entity_address_unified", "prepared"),
            )
            return await _prepare_artifact(connection, recipe, schema_name, control, office_inputs)
    except (asyncpg.PostgresError, NetworkAddressProjectionError) as error:
        raise NetworkCustomAddressSourceError("Composition native preparation failed") from error


def _receipt_object(pairs):
    document_by_field = dict(pairs)
    if len(document_by_field) != len(pairs):
        raise ValueError("Duplicate composition receipt field")
    return document_by_field


def _receipt_features(parsed, revision):
    """Keep both historical v5 forms closed; only v6 may combine their facts."""
    has_aliases, has_fhir = "source_aliases" in parsed, "fhir_carry_sha256" in parsed
    if revision < 5 and (has_aliases or has_fhir):
        raise ValueError
    if (revision == 5 and has_aliases == has_fhir) or (revision == 6 and not (has_aliases and has_fhir)):
        raise ValueError
    if has_aliases and parsed["source_aliases"] is not True:
        raise ValueError
    for name in ("ptg_carry_sha256", "aca_carry_sha256", "fhir_carry_sha256"):
        if name in parsed and (
            type(parsed[name]) is not str
            or len(parsed[name]) != 64
            or any(character not in "0123456789abcdef" for character in parsed[name])
        ):
            raise ValueError


_LOGICAL_RECEIPT_FIELDS = frozenset(CustomAddressSourceReceipt.__dataclass_fields__) - {"revision"}
_LEGACY_PHYSICAL_FIELDS = {
    "source_oids",
    "npi_oids",
    "owner_role",
    "runtime_roles",
    "schema_oid",
    "address_table_oid",
    "binding_table_oid",
    "office_lookup_index_oid",
    "owner_oid",
}
_OPTIONAL_RECEIPT_FIELDS = {"ptg_carry_sha256", "aca_carry_sha256", "source_aliases", "fhir_carry_sha256"}


def _named_receipt(parsed_by_field, revision):
    """Decode named pins and validate complete logical scope and current recipe digest."""
    for field in ("base_source", "address_source", "npi_source"):
        if parsed_by_field[field] is not None:
            parsed_by_field[field] = PinnedAddressSource(**parsed_by_field[field])
    receipt = CustomAddressSourceReceipt(**parsed_by_field, revision=revision)
    schema_name = _arguments(
        receipt.base_source, receipt.composition_id, receipt.approved_revision, None, (), receipt.npi_source
    )
    _identifier(receipt.control_schema)
    if type(receipt.custom_pair_count) is not int or not 0 <= receipt.custom_pair_count < 2**63:
        raise ValueError
    for digest in (receipt.approved_records_sha256, receipt.generation_sha256):
        if (
            type(digest) is not str
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError
    if receipt.address_source != PinnedAddressSource(schema_name, "entity_address_unified", receipt.generation_sha256):
        raise ValueError
    if revision == 7 and _generation(receipt) != receipt.generation_sha256:
        raise ValueError
    return receipt


def _from_document(document):
    if type(document) is not str or len(document.encode()) > 65536:
        raise NetworkCustomAddressSourceError("Retained composition receipt exceeds its bound")
    try:
        parsed_by_field = json.loads(document, object_pairs_hook=_receipt_object)
        if type(parsed_by_field) is not dict or parsed_by_field.pop("component") != "network_custom_address_source":
            raise ValueError
        revision = parsed_by_field.pop("revision")
        if type(revision) is not int or revision not in {2, 3, 4, 5, 6, 7}:
            raise ValueError
        if revision == 7:
            if set(parsed_by_field) != _LOGICAL_RECEIPT_FIELDS:
                raise ValueError
        elif set(parsed_by_field) != _LOGICAL_RECEIPT_FIELDS:
            expected = (_LOGICAL_RECEIPT_FIELDS - _OPTIONAL_RECEIPT_FIELDS) | _LEGACY_PHYSICAL_FIELDS
            if not expected <= set(parsed_by_field) or not set(parsed_by_field) <= expected | _OPTIONAL_RECEIPT_FIELDS:
                raise ValueError
            _receipt_features(parsed_by_field, revision)
            if revision == 2 and {"ptg_carry_sha256", "aca_carry_sha256"} & parsed_by_field.keys():
                raise ValueError
            if revision == 3 and ("ptg_carry_sha256" not in parsed_by_field or "aca_carry_sha256" in parsed_by_field):
                raise ValueError
            if revision == 4 and "aca_carry_sha256" not in parsed_by_field:
                raise ValueError
            parsed_by_field = {
                key: field_value for key, field_value in parsed_by_field.items() if key in _LOGICAL_RECEIPT_FIELDS
            }
        if revision < 7:
            features_by_name = {
                name: field_value
                for name, field_value in parsed_by_field.items()
                if name in _OPTIONAL_RECEIPT_FIELDS
                and field_value is not None
                and (name != "source_aliases" or field_value is True)
            }
            _receipt_features(features_by_name, revision)
            if revision == 2 and features_by_name or revision == 3 and set(features_by_name) != {"ptg_carry_sha256"}:
                raise ValueError
            if revision == 4 and "aca_carry_sha256" not in features_by_name:
                raise ValueError
        return _named_receipt(parsed_by_field, revision)
    except KeyError, TypeError, ValueError:
        raise NetworkCustomAddressSourceError("Retained composition receipt is invalid") from None


async def _verify_custom_address_source(connection, receipt, *, retained=False):
    if type(receipt) is not CustomAddressSourceReceipt:
        raise NetworkCustomAddressSourceError("Composition receipt is required")
    schema_name = _arguments(
        receipt.base_source, receipt.composition_id, receipt.approved_revision, None, (), receipt.npi_source
    )
    await _require_transaction(connection)
    if (
        receipt.address_source != PinnedAddressSource(schema_name, "entity_address_unified", receipt.generation_sha256)
        or receipt.revision == 7
        and _generation(receipt) != receipt.generation_sha256
    ):
        raise NetworkCustomAddressSourceError("Composition receipt digest differs")
    summary = await _approved_identity(
        connection, _identifier(receipt.control_schema), receipt.approved_revision, retained=retained
    )
    site_sources = await verify_approved_site_sources(
        connection, approved_revision=receipt.approved_revision, control_schema=receipt.control_schema
    )
    if await _source_alias_layout(connection, schema_name) != receipt.source_aliases or (
        receipt.source_aliases
        and not _has_source_aliases(site_sources)
        and receipt.ptg_carry_sha256 is None
        and receipt.aca_carry_sha256 is None
        and receipt.fhir_carry_sha256 is None
    ):
        raise NetworkCustomAddressSourceError("Composition source alias layout differs")
    if summary["fingerprint"] != receipt.approved_records_sha256 or summary["pairs"] != receipt.custom_pair_count:
        raise NetworkCustomAddressSourceError("Approved composition records differ")
    base_columns = await _source_columns(connection, receipt.base_source, address=True)
    artifact_columns = await _source_columns(connection, receipt.address_source, address=True)
    if base_columns != artifact_columns:
        raise NetworkCustomAddressSourceError("Composition address shape differs")
    if receipt.npi_source is not None:
        await _source_columns(connection, receipt.npi_source, address=False)
    await _binding_shape(
        connection, _identifier(schema_name) + ".provider_location_binding", source_aliases=receipt.source_aliases
    )
    await _verify_custom_rows(connection, receipt)
    await _office_lookup_shape(connection, schema_name)
    stored = await connection.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema_name
    )
    if stored is None or _from_document(stored) != receipt:
        raise NetworkCustomAddressSourceError("Retained composition recipe differs")
    return receipt


async def verify_custom_address_source(connection, receipt):
    """Compare the complete named recipe, current approved map and actual data layout."""
    return await _verify_custom_address_source(connection, receipt)


async def verify_retained_custom_address_source(connection, receipt):
    """Compare the recorded historical approval, named recipe and actual data layout."""
    return await _verify_custom_address_source(connection, receipt, retained=True)
