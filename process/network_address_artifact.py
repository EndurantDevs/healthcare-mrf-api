# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only pins for protected address heaps retained by native publication."""

import hashlib
import json

from sqlalchemy import text

from process import entity_address_native_publication as publication
from process import entity_address_result_generation as generation
from process import entity_address_snapshot_preparation as protected
from process.entity_address_retained_names import has_matching_live_address_generation, retained_address_name
from process.mrf_address_publication import require_native_read_catalog_subjects
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_cms_registry_address_capture import NativeReceiptSession, _NativeMappings
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_writer_closure import _protected_owner

CONTRACT = "network_address_artifact.postgres.v1"
_MAX_DESCRIPTOR_BYTES = 256 * 1024
_TABLE = "entity_address_unified"
_AUTHORITY = "entity_address_result_generation"


class NetworkAddressArtifactError(ValueError):
    """The address pin lacks its required native authority or protected custody."""


def _require(condition, reason):
    if not condition:
        raise NetworkAddressArtifactError("network_address_artifact_" + reason)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


class _CatalogMappings(_NativeMappings):
    def all(self):
        """Complete the existing receipt adapter's bounded result interface."""
        return self

    def one(self):
        """Require the single physical catalog identity requested by receipt SQL."""
        _require(len(self) == 1, "catalog_row_invalid")
        return self[0]


class _CatalogRows(_CatalogMappings):
    def mappings(self):
        """Preserve SQLAlchemy's explicit mapping view for existing receipt checks."""
        return _CatalogMappings(self)

    def all(self):
        """Unmapped receipt comparisons use ordered tuples, as SQLAlchemy does."""
        return [tuple(row.values()) for row in self]

    def one(self):
        """Expose one ordered native row for existing positional catalog readers."""
        return tuple(super().one().values())

    def scalars(self):
        """Keep the existing receipt reader's scalar-list comparisons intact."""
        return _CatalogMappings(next(iter(row.values())) for row in self)


class _CatalogSession(NativeReceiptSession):
    async def execute(self, statement, parameters=None):
        """Reuse native receipt binds/JSON decoding on the caller's transaction."""
        return _CatalogRows(await super().execute(statement, parameters))


def _roles(runtime_roles):
    _require(type(runtime_roles) is tuple and 0 < len(runtime_roles) <= 32, "roles_invalid")
    _require(tuple(sorted(set(runtime_roles))) == runtime_roles, "roles_invalid")
    for role in runtime_roles:
        _identifier(role)
    return runtime_roles


async def _database(connection):
    return dict(
        await connection.fetchrow(
            "SELECT oid::bigint AS oid,datname AS name FROM pg_catalog.pg_database WHERE datname=current_database()"
        )
    )


async def _physical(connection, oid):
    return await connection.fetchrow(
        "SELECT c.oid::bigint,c.relname,pg_catalog.pg_relation_filenode(c.oid)::bigint AS relfilenode,"
        "c.relowner::bigint AS owner_oid,n.oid::bigint AS schema_oid,n.nspname AS schema_name,"
        "n.nspowner::bigint AS schema_owner_oid,c.relkind::text,c.relpersistence::text,"
        "c.relrowsecurity,c.relforcerowsecurity,c.relispartition,"
        "EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) AS rules,"
        "EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited "
        "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1::oid",
        oid,
    )


async def _locked_table(connection, schema_name, oid):
    before = await _physical(connection, oid)
    _require(before is not None and before["schema_name"] == schema_name, "table_absent")
    _require(before["relname"] in {_TABLE, f"{_TABLE}_retained_{oid:x}"}, "table_name_invalid")
    await connection.execute(
        f"LOCK TABLE ONLY {_identifier(schema_name)}.{_identifier(before['relname'])} IN ACCESS SHARE MODE NOWAIT"
    )
    _require(
        await connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", _identifier(schema_name) + "." + _identifier(before["relname"])
        )
        == oid,
        "table_changed",
    )
    after = await _physical(connection, oid)
    _require(after == before, "table_changed")
    return after


async def _require_api_read_context(connection, api_read):
    _require(type(api_read) is bool, "read_mode_invalid")
    if api_read:
        _require(connection.is_in_transaction(), "transaction_required")
        modes = await connection.fetchrow(
            "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"
        )
        _require(tuple(modes) in (("repeatable read", "on"), ("serializable", "on")), "read_snapshot_required")


async def _owner(connection):
    role = await connection.fetchrow(
        "SELECT r.oid::bigint,r.rolcanlogin,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls "
        "FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_roles r ON r.oid=n.nspowner "
        "WHERE n.nspname='hp_snapshot_retention'"
    )
    _require(_protected_owner(role), "protected_owner_invalid")
    return role["oid"]


async def _role_custody(connection, physical, owner_oid, runtime_roles):
    safe = await connection.fetchval(
        "SELECT count(*)=cardinality($1::text[]) AND bool_and(r.oid IS NOT NULL AND NOT r.rolsuper "
        "AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication AND NOT r.rolbypassrls "
        "AND NOT pg_has_role(r.oid,$2::oid,'MEMBER') AND NOT pg_has_role(r.oid,$2::oid,'USAGE') "
        "AND NOT pg_has_role(r.oid,$2::oid,'SET') AND NOT pg_has_role(r.oid,$3::oid,'MEMBER') "
        "AND NOT pg_has_role(r.oid,$3::oid,'SET') AND NOT has_schema_privilege(r.oid,$4::oid,'CREATE') "
        "AND has_schema_privilege(r.oid,$4::oid,'USAGE') AND has_table_privilege(r.oid,$5::oid,'SELECT') "
        "AND NOT has_table_privilege(r.oid,$5::oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
        "AND NOT has_any_column_privilege(r.oid,$5::oid,'INSERT,UPDATE,REFERENCES') "
        "AND NOT EXISTS(SELECT FROM pg_roles elevated WHERE "
        "(elevated.rolsuper OR elevated.rolcreaterole OR elevated.rolcreatedb OR elevated.rolreplication OR elevated.rolbypassrls) "
        "AND (pg_has_role(r.oid,elevated.oid,'MEMBER') OR pg_has_role(r.oid,elevated.oid,'SET')))) "
        "FROM unnest($1::text[]) requested(name) LEFT JOIN pg_roles r ON r.rolname=requested.name",
        list(runtime_roles),
        owner_oid,
        physical["schema_owner_oid"],
        physical["schema_oid"],
        physical["oid"],
    )
    _require(safe is True, "runtime_privileges_invalid")
    # Ordinary namespace owners can destroy a protected table through DROP SCHEMA.
    unsafe = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_roles principal WHERE "
        + protected._ORDINARY_PRINCIPAL_SQL.replace(":owner_oid", "$1")
        + " AND (has_schema_privilege(principal.oid,$2::oid,'CREATE') "
        "OR pg_has_role(principal.oid,$3::oid,'MEMBER') OR pg_has_role(principal.oid,$3::oid,'SET') "
        "OR has_table_privilege(principal.oid,$4::oid,'MAINTAIN')))",
        owner_oid,
        physical["schema_oid"],
        physical["schema_owner_oid"],
        physical["oid"],
    )
    _require(unsafe is False, "namespace_privileges_invalid")


async def _custody(connection, session, physical, authority_oid, runtime_roles):
    owner_oid = await _owner(connection)
    authority = await _physical(connection, authority_oid)
    _require(
        authority is not None
        and authority["schema_oid"] == physical["schema_oid"]
        and authority["relname"] == _AUTHORITY,
        "authority_table_changed",
    )
    for relation in (physical, authority):
        _require(
            relation["owner_oid"] == owner_oid
            and relation["relkind"] == "r"
            and relation["relpersistence"] == "p"
            and not any(
                relation[key]
                for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "rules", "inherited")
            ),
            "heap_custody_invalid",
        )
        await _role_custody(connection, relation, owner_oid, runtime_roles)
    await protected._require_no_untrusted_mutation(session, [physical["oid"], authority_oid], owner_oid)
    subjects = tuple(sorted(set(runtime_roles + (await connection.fetchval("SELECT current_user"),))))
    await require_native_read_catalog_subjects(session, [physical["oid"], authority_oid], subjects)
    return owner_oid


async def _catalog(session, physical, authority):
    relations_by_label = {"address": (physical, _TABLE), "authority": (authority, _AUTHORITY)}
    catalog = await publication._catalog_evidence(
        session,
        {
            "schema_name": physical["schema_name"],
            "stage_relations": [
                {"table_name": logical_name, "stage_name": relation["relname"], "relation_oid": relation["oid"]}
                for relation, logical_name in relations_by_label.values()
            ],
        },
    )
    return {
        key: {
            section: [
                row for row in rows if (row[1] if section == "shapes" else row["relation_oid"]) == relation["oid"]
            ]
            for section, rows in catalog.items()
        }
        for key, (relation, _) in relations_by_label.items()
    }


def _authority_document(authority):
    serving = authority.serving_generation
    _require(serving is not None and authority.local_generation > 0, "generation_unpublished")
    return {
        "local_lineage_id": authority.local_lineage_id,
        "local_generation": authority.local_generation,
        "origin_lineage_id": serving.origin_lineage_id,
        "origin_generation": serving.origin_generation,
        "published_at": serving.published_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "relation_oids": list(authority.relation_oids),
    }


def _descriptor(binding):
    from process import network_bootstrap_address_artifact as bootstrap
    from process import network_indexed_address_artifact as indexed

    if type(binding) is dict and binding.get("contract") == bootstrap.CONTRACT:
        return bootstrap._descriptor(binding)
    if type(binding) is dict and binding.get("contract") == indexed.CONTRACT:
        return indexed._descriptor(binding)
    _require(
        type(binding) is dict
        and set(binding)
        == {
            "contract",
            "database",
            "namespace",
            "protected_owner_oid",
            "table",
            "authority_table_oid",
            "authority",
            "generation_id",
            "alias_generation",
            "alias_catalog_sha256",
            "catalog",
            "runtime_roles",
            "descriptor_sha256",
        },
        "descriptor_invalid",
    )
    _require(
        binding["contract"] == CONTRACT and len(_json(binding).encode()) <= _MAX_DESCRIPTOR_BYTES, "descriptor_invalid"
    )
    _require(
        _digest({key: field for key, field in binding.items() if key != "descriptor_sha256"})
        == binding["descriptor_sha256"],
        "descriptor_digest_invalid",
    )
    _require(binding["generation_id"] == _digest(binding["authority"]), "generation_invalid")
    _descriptor_scope(binding)
    _descriptor_origin(binding)
    return json.loads(_json(binding))


def _descriptor_scope(binding):
    _require(type(binding["table"]) is dict and set(binding["table"]) == {"oid", "relfilenode"}, "descriptor_invalid")
    _require(
        type(binding["namespace"]) is dict and set(binding["namespace"]) == {"name", "oid", "owner_oid"},
        "descriptor_invalid",
    )
    _require(type(binding["alias_generation"]) is int and binding["alias_generation"] >= 0, "descriptor_invalid")
    for oid in (
        binding["table"]["oid"],
        binding["table"]["relfilenode"],
        binding["namespace"]["oid"],
        binding["namespace"]["owner_oid"],
        binding["protected_owner_oid"],
        binding["authority_table_oid"],
    ):
        _require(type(oid) is int and 0 < oid < 2**32, "descriptor_invalid")
    _require(type(binding["database"]) is dict and set(binding["database"]) == {"oid", "name"}, "descriptor_invalid")
    _require(
        type(binding["database"]["oid"]) is int
        and 0 < binding["database"]["oid"] < 2**32
        and type(binding["database"]["name"]) is str,
        "descriptor_invalid",
    )
    _identifier(binding["namespace"]["name"])


def _descriptor_origin(binding):
    _require(
        type(binding["authority"]) is dict
        and set(binding["authority"])
        == {
            "local_lineage_id",
            "local_generation",
            "origin_lineage_id",
            "origin_generation",
            "published_at",
            "relation_oids",
        },
        "descriptor_invalid",
    )
    authority = generation.validate_entity_address_result_generation_authority(
        {"singleton": True, **binding["authority"]}
    )
    _require(
        _authority_document(authority) == binding["authority"]
        and authority.relation_oids[0] == binding["table"]["oid"],
        "generation_invalid",
    )
    _require(type(binding["runtime_roles"]) is list, "roles_invalid")
    _roles(tuple(binding["runtime_roles"]))


async def _require_capture_snapshot(connection):
    _require(connection.is_in_transaction(), "transaction_required")
    _require(
        await connection.fetchval("SHOW transaction_isolation") in {"repeatable read", "serializable"},
        "snapshot_required",
    )


async def capture_network_address_artifact(connection, source, *, runtime_roles):
    """Capture actual current origin under native locks; this descriptor grants no authority."""
    from process import network_bootstrap_address_artifact as bootstrap

    _require(type(source) is PinnedAddressSource and source.table_name == _TABLE, "source_invalid")
    if bootstrap.is_bootstrap_source(source):
        return await bootstrap.capture_bootstrap_address_artifact(connection, source, runtime_roles=runtime_roles)
    await _require_capture_snapshot(connection)
    runtime_roles = _roles(runtime_roles)
    session = _CatalogSession(connection)
    _require(
        await connection.fetchval("SELECT pg_try_advisory_xact_lock_shared(hashtext('address_numeric_grid_alias_v1'))"),
        "publication_busy",
    )
    oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", f'{_identifier(source.schema_name)}."{_TABLE}"'
    )
    _require(type(oid) is int, "table_absent")
    physical = await _locked_table(connection, source.schema_name, oid)
    authority_name = f'{_identifier(source.schema_name)}."{_AUTHORITY}"'
    await connection.execute(f"LOCK TABLE ONLY {authority_name} IN ACCESS SHARE MODE NOWAIT")
    authority_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", authority_name)
    owner_oid = await _custody(connection, session, physical, authority_oid, runtime_roles)
    authority = await generation.read_entity_address_result_generation_authority(
        session, schema_name=source.schema_name
    )
    origin = _authority_document(authority)
    _require(origin["relation_oids"][0] == oid and _digest(origin) == source.generation_id, "source_generation_changed")
    return await _captured_descriptor(connection, session, physical, authority_oid, origin, runtime_roles, owner_oid)


async def _captured_descriptor(connection, session, physical, authority_oid, origin, runtime_roles, owner_oid):
    alias_catalog = await protected._require_alias_authority(session, physical["schema_name"], owner_oid)
    _, _, alias_generation = await protected.alias._alias_state(session, physical["schema_name"])
    descriptor_by_field = {
        "contract": CONTRACT,
        "database": await _database(connection),
        "namespace": {
            "name": physical["schema_name"],
            "oid": physical["schema_oid"],
            "owner_oid": physical["schema_owner_oid"],
        },
        "protected_owner_oid": owner_oid,
        "table": {"oid": physical["oid"], "relfilenode": physical["relfilenode"]},
        "authority_table_oid": authority_oid,
        "authority": origin,
        "generation_id": _digest(origin),
        "alias_generation": alias_generation,
        "alias_catalog_sha256": alias_catalog,
        "catalog": await _catalog(session, physical, await _physical(connection, authority_oid)),
        "runtime_roles": list(runtime_roles),
    }
    descriptor_by_field["descriptor_sha256"] = _digest(descriptor_by_field)
    return _descriptor(descriptor_by_field)


async def verify_network_address_artifact(connection, descriptor, *, runtime_roles):
    """Verify a descriptor from the caller's authenticated selected published candidate.

    The self-digest checks formatting; untrusted external JSON grants no authority.
    Named relations, schema, logical origin and native permissions govern reads.
    Retained generations may outlive the current serving singleton.
    """
    from process import network_bootstrap_address_artifact as bootstrap
    from process import network_indexed_address_artifact as indexed

    descriptor = _descriptor(descriptor)
    if descriptor["contract"] == bootstrap.CONTRACT:
        return await bootstrap.verify_bootstrap_address_artifact(connection, descriptor, runtime_roles=runtime_roles)
    if descriptor["contract"] == indexed.CONTRACT:
        return await indexed.verify_indexed_address_artifact(connection, descriptor, runtime_roles=runtime_roles)
    _roles(runtime_roles)
    _require(connection.is_in_transaction(), "transaction_required")
    physical = await _locked_named_address(connection, descriptor)
    authority_name = f'{_identifier(physical["schema_name"])}."{_AUTHORITY}"'
    await connection.execute(f"LOCK TABLE ONLY {authority_name} IN ACCESS SHARE MODE NOWAIT")
    session = _CatalogSession(connection)
    authority_oid = await connection.fetchval("SELECT to_regclass($1)::oid", authority_name)
    authority = await _physical(connection, authority_oid)
    await _require_named_address_shapes(session, descriptor, physical, authority)
    return CompactAddressRelation(physical["schema_name"], physical["relname"], physical["oid"])


async def _locked_named_address(connection, descriptor):
    """Use native name resolution and locks; old IDs supply only a legacy spelling."""
    origin_by_field = {
        field: descriptor["authority"][field] for field in ("origin_lineage_id", "origin_generation", "published_at")
    }
    names = (
        retained_address_name(_TABLE, origin_by_field),
        f"{_TABLE}_retained_{descriptor['table']['oid']:x}",
        _TABLE,
    )
    schema = descriptor["namespace"]["name"]
    selected = await _existing_address_name(connection, schema, names)
    await connection.execute(
        f"LOCK TABLE ONLY {_identifier(schema)}.{_identifier(selected)} IN ACCESS SHARE MODE NOWAIT"
    )
    # A publisher may have won before the first lock; retain its original rows by their new name.
    resolved = await _existing_address_name(connection, schema, names)
    if resolved != selected:
        await connection.execute(
            f"LOCK TABLE ONLY {_identifier(schema)}.{_identifier(resolved)} IN ACCESS SHARE MODE NOWAIT"
        )
    if resolved == _TABLE:
        _require(
            await has_matching_live_address_generation(connection, schema, resolved, origin_by_field),
            "source_generation_changed",
        )
    oid = await connection.fetchval("SELECT to_regclass($1)::oid", _identifier(schema) + "." + _identifier(resolved))
    physical = await _physical(connection, oid)
    _require(physical is not None, "table_absent")
    return {**physical, "schema_name": schema, "relname": resolved}


async def _existing_address_name(connection, schema, names):
    for name in names:
        if await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", _identifier(schema) + "." + _identifier(name)
        ):
            return name
    raise NetworkAddressArtifactError("network_address_artifact_table_absent")


async def _require_named_address_shapes(session, descriptor, physical, authority):
    """Reuse the receipt's semantic schema hashes without its physical catalog inventory."""
    from process import entity_address_snapshot_receipt as receipt

    async with publication._catalog_search_path(session):
        for label, logical, relation in (("address", _TABLE, physical), ("authority", _AUTHORITY, authority)):
            _require(
                relation is not None
                and relation["relkind"] == "r"
                and relation["relpersistence"] == "p"
                and not any(
                    relation[key]
                    for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "rules", "inherited")
                ),
                "structural_catalog_changed",
            )
            expected = descriptor["catalog"][label]["shapes"]
            _require(len(expected) == 1 and expected[0][0] == logical, "structural_catalog_changed")
            actual = await receipt._schema_identity(
                session,
                relation["oid"],
                physical["schema_name"],
                logical,
                names_by_stage={relation["relname"]: logical},
            )
            if actual != expected[0][2]:
                actual = await _copied_address_shape(session, descriptor["catalog"][label], relation, logical)
            _require(actual == expected[0][2], "structural_catalog_changed")


def _legacy_address_positions(columns, recorded_columns):
    """Translate only the old hash's positional encoding through exact column names."""
    _require(type(recorded_columns) is list and bool(recorded_columns), "structural_catalog_changed")
    _require(
        all(
            type(column) is dict and type(column.get("attname")) is str and "attnum" in column
            for column in recorded_columns
        ),
        "structural_catalog_changed",
    )
    recorded_by_name = {column["attname"]: column["attnum"] for column in recorded_columns}
    observed_by_name = {column["attname"]: column["attnum"] for column in columns}
    _require(
        len(recorded_by_name) == len(recorded_columns) == len(columns) == len(observed_by_name)
        and recorded_by_name.keys() == observed_by_name.keys(),
        "structural_catalog_changed",
    )
    for positions in (tuple(recorded_by_name.values()), tuple(observed_by_name.values())):
        _require(
            all(type(number) is int and number > 0 for number in positions) and len(set(positions)) == len(positions),
            "structural_catalog_changed",
        )
    _require(
        sorted(recorded_by_name, key=recorded_by_name.get) == sorted(observed_by_name, key=observed_by_name.get),
        "structural_catalog_changed",
    )
    return {0: 0, **{observed_by_name[name]: number for name, number in recorded_by_name.items()}}


async def _copied_address_shape(session, recorded_catalog, relation, logical):
    """Check unchanged legacy metadata after copies compact dropped-column slots."""
    from process import entity_address_snapshot_receipt as receipt
    from process.network_cms_registry_address_equivalence import _numbers

    _require(
        type(recorded_catalog.get("columns")) is list and bool(recorded_catalog["columns"]),
        "structural_catalog_changed",
    )
    columns = await receipt._catalog_columns(session, relation["oid"])
    constraints = await receipt._catalog_constraints(session, relation["oid"], relation["schema_name"])
    indexes = await receipt._catalog_indexes(session, relation["oid"])
    legacy_by_position = _legacy_address_positions(columns, recorded_catalog.get("columns"))
    for column in columns:
        column["attnum"] = legacy_by_position[column["attnum"]]
    for constraint in constraints:
        _require(
            constraint["referenced_table"] is None and constraint["referenced_columns"] is None,
            "structural_catalog_changed",
        )
        _require(
            all(
                int(number) in legacy_by_position
                for number in (constraint["key_columns"] or "").strip("{}").split(",")
                if number
            ),
            "structural_catalog_changed",
        )
        constraint["key_columns"] = _numbers(constraint["key_columns"], legacy_by_position, array=True)
    for index in indexes:
        _require(
            all(int(number) in legacy_by_position for number in index["keys"].split()), "structural_catalog_changed"
        )
        index["keys"] = _numbers(index["keys"], legacy_by_position)
        for attribute in index["key_attributes"]:
            _require(attribute["attribute_number"] in legacy_by_position, "structural_catalog_changed")
            attribute["attribute_number"] = legacy_by_position[attribute["attribute_number"]]
    columns.sort(key=lambda column: column["attnum"])
    constraints.sort(key=lambda row: (str(row["contype"]), row["key_columns"] is None, row["key_columns"] or ""))
    names_by_stage = {relation["relname"]: logical}
    columns, constraints, indexes = (
        receipt._normalized_catalog_names(entries, names_by_stage) for entries in (columns, constraints, indexes)
    )
    indexes.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), default=receipt._json_scalar))
    receipt._reject_schema_qualified_expressions(relation["schema_name"], columns, constraints, indexes)
    schema_by_field = {"table_name": logical, "columns": columns, "constraints": constraints, "indexes": indexes}
    digest = receipt._canonical_digest(schema_by_field)
    if digest == recorded_catalog["shapes"][0][2]:
        return digest
    # Old hashes retain their capture locale's constraint order. Keep C-compatible
    # matches, then try the native order; other source locales still fail closed.
    schema_by_field["constraints"] = await _native_constraint_order(session, constraints)
    return receipt._canonical_digest(schema_by_field)


async def _native_constraint_order(session, constraints):
    """Restore the receipt query's native ordering without changing any schema field."""
    from process import entity_address_snapshot_receipt as receipt

    positions = (
        await session.execute(
            text(
                "SELECT ordinal FROM pg_catalog.jsonb_array_elements(CAST(:constraints AS jsonb)) "
                "WITH ORDINALITY AS entries(value,ordinal) "
                "ORDER BY CAST(value->>'contype' AS pg_catalog.\"char\"), "
                "value->>'key_columns',value->>'referenced_columns',value->>'referenced_table',ordinal"
            ),
            {"constraints": json.dumps(constraints, default=receipt._json_scalar)},
        )
    ).mappings()
    return [constraints[position["ordinal"] - 1] for position in positions]
