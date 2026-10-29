# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inert private NPPES controls with a separate retained publication owner."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_identifier, storage_relation
from process.nppes_public_evidence_writer_contract import writer_error
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles

CONTRACT = "nppes-isolated-catalog-v1"
_SCHEMA = re.compile(r"npi_source_[0-9a-f]{32}\Z")


def _require(condition):
    if not condition:
        raise writer_error()


def _fixed_catalog_sql():
    """Keep shared descriptors while bounding this inheritance-free family."""
    family, separator, descriptors = CATALOG_SQL.partition("), control_relations AS (")
    _require(family.startswith("WITH RECURSIVE family(oid) AS (") and separator)
    return (
        "WITH family(oid) AS (\n"
        " SELECT to_regclass(name)::oid FROM unnest($1::text[]) names(name)\n"
        " WHERE NOT EXISTS(SELECT FROM pg_inherits\n"
        "  WHERE inhrelid=to_regclass(name) OR inhparent=to_regclass(name))\n" + separator + descriptors
    )


_FIXED_CATALOG_SQL = _fixed_catalog_sql()


def _decoded(value):
    return json.loads(value) if isinstance(value, str) else value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class NppesIsolatedCatalog:
    """Native catalog identities retained by the independently issued manifest."""

    schema: str
    schema_oid: int
    owner_role: str
    publisher_role: str
    retained_owner_role: str
    retained_publisher_role: str
    namespace_owner_role: str
    runtime_roles: tuple[str, ...]
    producer_identity: tuple[str, str]
    tables: tuple[tuple[str, int, str], ...]
    contract: str = CONTRACT

    def as_dict(self):
        """Serialize native evidence without granting a caller publication authority."""
        return asdict(self)


def _table_definitions():
    from process import nppes_public_evidence_catalog as legacy
    from process import nppes_public_evidence_storage as storage
    from process.nppes_public_evidence_validation import _column_type

    definitions_by_table = {}
    for name, columns in legacy._CONTROL_COLUMNS.items():
        nullable = legacy._NULLABLE_RELEASE if name == legacy._SOURCE_RELEASE else set()
        definitions_by_table[name] = tuple(
            (column, legacy._CONTROL_TYPES[column], column not in nullable) for column in columns
        )
    for root, (name, _columns, _attribute, _suffix) in storage.ROOTS.items():
        definitions_by_table[name] = tuple(
            (
                column,
                _column_type(column).replace("timestamptz", "timestamp with time zone")
                if column != "created_at"
                else "timestamp with time zone",
                column not in storage._NULLABLE[root],
            )
            for column in legacy._TABLE_COLUMNS[name]
        )
    return definitions_by_table


def _table_keys():
    from process import nppes_public_evidence_catalog as legacy

    keys_by_table = {name: [] for name in _table_definitions()}
    for table, kind, columns in legacy._CONTROL_KEYS.values():
        keys_by_table[table].append((kind, columns))
    return keys_by_table


def _native_checks(columns):
    checks = []
    for name, kind, _not_null in columns:
        field = storage_identifier(name)
        if kind == "bytea":
            checks.append(f"octet_length({field})=32")
        elif name.endswith("_count") or name in {"archive_ordinal", "source_row_ordinal"}:
            checks.append(f"{field}>=0")
    if any(name == "candidate_storage_json" for name, *_unused in columns):
        checks.append("jsonb_typeof(candidate_storage_json)='object'")
    return checks


async def _role_oids(connection, policy, retained_owner_role, retained_publisher_role):
    source_owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    retained_owner, retained_readers = await storage_roles(
        connection,
        policy.schema,
        owner_role=retained_owner_role,
        publisher_role=retained_publisher_role,
        reader_roles=(*policy.runtime_roles, policy.owner_role, policy.publisher_role),
        control_reader_roles=(policy.owner_role, policy.publisher_role),
    )
    _require(source_owner != retained_owner)
    _require(
        await connection.fetchval(
            "SELECT pg_has_role($1::name,$2::name,'USAGE') AND pg_has_role($1::name,$3::name,'USAGE')",
            retained_publisher_role,
            policy.owner_role,
            retained_owner_role,
        )
    )
    _require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles r WHERE NOT r.rolsuper "
            "AND r.rolname<>ALL($1::text[]) AND (pg_has_role(r.oid,$2::name,'SET') "
            "OR pg_has_role(r.oid,$2::name,'USAGE') OR pg_has_role(r.oid,$3::name,'SET') "
            "OR pg_has_role(r.oid,$3::name,'USAGE')))",
            [policy.owner_role, policy.publisher_role, retained_owner_role, retained_publisher_role],
            policy.owner_role,
            retained_owner_role,
        )
    )
    return source_owner, retained_owner, readers, retained_readers


async def _create_table(connection, policy, name, columns):
    fields = []
    for column, kind, not_null in columns:
        declaration = f"{storage_identifier(column)} {kind}" + (" NOT NULL" if not_null else "")
        if column in {"created_at", "updated_at", "sealed_at"}:
            declaration += " DEFAULT transaction_timestamp()"
        fields.append(declaration)
    for kind, key in _table_keys()[name]:
        fields.append(
            ("PRIMARY KEY" if kind == "p" else "UNIQUE")
            + " ("
            + ",".join(storage_identifier(column) for column in key)
            + ")"
        )
    fields.extend("CHECK (" + check + ")" for check in _native_checks(columns))
    await connection.execute(f"CREATE TABLE {storage_relation(policy.schema, name)} ({','.join(fields)})")


async def _protect_header(connection, policy, retained_owner_role):
    from process.nppes_public_evidence_storage import HEADER

    table = storage_relation(policy.schema, HEADER)
    source = storage_identifier(policy.owner_role)
    retained = storage_identifier(retained_owner_role)
    await connection.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
    await connection.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
    await connection.execute(
        f"CREATE POLICY nppes_retained_control ON {table} TO {retained} USING(true) WITH CHECK(true)"
    )
    await connection.execute(f"CREATE POLICY nppes_control_read ON {table} FOR SELECT USING(true)")
    await connection.execute(
        f"CREATE POLICY nppes_loading_insert ON {table} FOR INSERT TO {source} "
        "WITH CHECK(candidate_storage_json->>'phase'='loading')"
    )
    await connection.execute(
        f"CREATE POLICY nppes_loading_update ON {table} FOR UPDATE TO {source} "
        "USING(candidate_storage_json->>'phase'='loading') "
        "WITH CHECK(candidate_storage_json->>'phase' IN ('loading','prepared'))"
    )
    await connection.execute(
        f"GRANT INSERT(admission_ref,source_release_ref,candidate_storage_json),"
        f"UPDATE(candidate_storage_json,updated_at) ON {table} TO {source}"
    )


async def provision_isolated_nppes_catalog(
    connection, policy, *, retained_owner_role, retained_publisher_role, namespace_owner_role, producer_identity
):
    """Create a fresh private family in the original independent administrator transaction."""
    _require(connection.is_in_transaction() and _SCHEMA.fullmatch(policy.schema) is not None)
    names = (
        policy.owner_role,
        policy.publisher_role,
        retained_owner_role,
        retained_publisher_role,
        namespace_owner_role,
        *policy.runtime_roles,
    )
    _require(len(set(names)) == len(names))
    for name in names:
        storage_identifier(name)
    actor = await connection.fetchrow(
        "SELECT session_user::text AS session,current_user::text AS current,"
        "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS administrator"
    )
    _require(actor["administrator"] and actor["session"] == actor["current"] and actor["session"] not in names)
    _require(await connection.fetchval("SELECT to_regnamespace($1)", policy.schema) is None)
    await connection.execute(
        f"CREATE SCHEMA {storage_identifier(policy.schema)} AUTHORIZATION {storage_identifier(namespace_owner_role)}"
    )
    await connection.execute(f"REVOKE ALL ON SCHEMA {storage_identifier(policy.schema)} FROM PUBLIC")
    principals = ",".join(storage_identifier(name) for name in names if name != namespace_owner_role)
    await connection.execute(f"GRANT USAGE ON SCHEMA {storage_identifier(policy.schema)} TO {principals}")
    await connection.execute(
        f"GRANT CREATE ON SCHEMA {storage_identifier(policy.schema)} TO "
        f"{storage_identifier(policy.owner_role)},{storage_identifier(retained_owner_role)}"
    )
    await _role_oids(connection, policy, retained_owner_role, retained_publisher_role)
    readers = ",".join(
        storage_identifier(name)
        for name in (
            *policy.runtime_roles,
            policy.owner_role,
            policy.publisher_role,
        )
    )
    for name, columns in _table_definitions().items():
        await _create_table(connection, policy, name, columns)
        relation = storage_relation(policy.schema, name)
        await connection.execute(f"ALTER TABLE {relation} OWNER TO {storage_identifier(retained_owner_role)}")
        await connection.execute(f"REVOKE ALL ON TABLE {relation} FROM PUBLIC")
        await connection.execute(f"GRANT SELECT ON TABLE {relation} TO {readers}")
    await _protect_header(connection, policy, retained_owner_role)
    return await capture_isolated_nppes_catalog(
        connection,
        policy,
        retained_owner_role=retained_owner_role,
        retained_publisher_role=retained_publisher_role,
        namespace_owner_role=namespace_owner_role,
        producer_identity=producer_identity,
    )


async def _catalog_rows(connection, policy, retained_owner, retained_readers):
    definitions = _table_definitions()
    rows = await connection.fetch(
        _FIXED_CATALOG_SQL,
        [storage_relation(policy.schema, name) for name in definitions],
        retained_owner,
        retained_readers,
    )
    _require(len(rows) == len(definitions) and {row["relname"] for row in rows} == set(definitions))
    return rows


def _verify_header_policies(policies, source_owner, retained_owner):
    loading = "((candidate_storage_json ->> 'phase'::text) = 'loading'::text)"
    prepared = "((candidate_storage_json ->> 'phase'::text) = ANY (ARRAY['loading'::text, 'prepared'::text]))"
    expected_by_name = {
        "nppes_control_read": ("r", [0], "true", None),
        "nppes_retained_control": ("*", [retained_owner], "true", "true"),
        "nppes_loading_insert": ("a", [source_owner], None, loading),
        "nppes_loading_update": ("w", [source_owner], loading, prepared),
    }
    _require({entry["polname"] for entry in policies} == set(expected_by_name))
    for entry in policies:
        command, roles, using, check = expected_by_name[entry["polname"]]
        _require(entry["polpermissive"] and entry["polcmd"] == command and entry["polroles"] == roles)
        _require(_normalized_expression(entry["using"]) == _normalized_expression(using))
        _require(_normalized_expression(entry["check"]) == _normalized_expression(check))


def _verify_header_grants(grants, source_owner, retained_owner, retained_readers):
    insert_columns = {"admission_ref", "source_release_ref", "candidate_storage_json"}
    update_columns = {"candidate_storage_json", "updated_at"}
    allowed_columns = {("INSERT", name) for name in insert_columns} | {("UPDATE", name) for name in update_columns}
    actual_columns = set()
    for grant in grants:
        if grant["grantee"] == retained_owner:
            continue
        _require(not grant["is_grantable"] and grant["grantor"] == retained_owner)
        if grant["column"] is None:
            _require(grant["grantee"] in retained_readers and grant["privilege_type"] == "SELECT")
        else:
            _require(grant["grantee"] == source_owner)
            actual_columns.add((grant["privilege_type"], grant["column"]))
    _require(actual_columns == allowed_columns)


async def _relation_grants(connection, relation_oid):
    return await connection.fetch(
        "SELECT a.grantee::bigint,a.grantor::bigint,a.privilege_type,a.is_grantable,NULL::text AS column "
        "FROM pg_class c CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a WHERE c.oid=$1 "
        "UNION ALL SELECT a.grantee::bigint,a.grantor::bigint,a.privilege_type,a.is_grantable,f.attname::text "
        "FROM pg_attribute f CROSS JOIN LATERAL aclexplode(f.attacl) a WHERE f.attrelid=$1 "
        "ORDER BY 1,2,3,5",
        relation_oid,
    )


async def _table_proof(connection, catalog_row, policy, retained_owner, source_owner, retained_readers):
    from process.nppes_public_evidence_storage import HEADER

    name = catalog_row["relname"]
    columns = _decoded(catalog_row["columns"])
    expected = _table_definitions()[name]
    _require(
        catalog_row["nspname"] == policy.schema
        and catalog_row["relowner"] == retained_owner
        and catalog_row["relkind"] == "r"
        and catalog_row["relpersistence"] == "p"
        and not catalog_row["relispartition"]
        and not catalog_row["parents"]
        and catalog_row["inert"]
        and catalog_row["native_checks"]
        and catalog_row["relrowsecurity"] is (name == HEADER)
        and catalog_row["relforcerowsecurity"] is (name == HEADER)
        and len(columns) == len(expected)
        and all(actual[:3] == [column, kind, not_null] for actual, (column, kind, not_null) in zip(columns, expected))
    )
    constraints = _decoded(catalog_row["constraints"])
    _require(all(entry["type"] in {"p", "u", "c"} and entry["valid"] for entry in constraints))
    _require(
        sorted(
            (entry["type"], tuple(columns[index - 1][0] for index in entry["columns"]))
            for entry in constraints
            if entry["type"] in {"p", "u"}
        )
        == sorted(_table_keys()[name])
    )
    policies = await connection.fetch(
        "SELECT polname,polcmd::text,polpermissive,polroles::oid[],pg_get_expr(polqual,polrelid) AS using,"
        "pg_get_expr(polwithcheck,polrelid) AS check FROM pg_policy WHERE polrelid=$1 ORDER BY polname",
        catalog_row["oid"],
    )
    if name == HEADER:
        _verify_header_policies(policies, source_owner, retained_owner)
    else:
        _require(not policies)
    grants = await _relation_grants(connection, catalog_row["oid"])
    if name == HEADER:
        _verify_header_grants(grants, source_owner, retained_owner, retained_readers)
    else:
        _require(catalog_row["direct_acl_closed"] and catalog_row["readers_closed"])
    return (
        name,
        catalog_row["oid"],
        _digest(
            {
                "columns": columns,
                "constraints": constraints,
                "indexes": _decoded(catalog_row["indexes"]),
                "policies": [dict(entry) for entry in policies],
                "grants": [dict(entry) for entry in grants],
            }
        ),
    )


async def capture_isolated_nppes_catalog(
    connection, policy, *, retained_owner_role, retained_publisher_role, namespace_owner_role, producer_identity
):
    """Capture genuine inert metadata; callers must preserve this native issuance receipt."""
    _require(connection.is_in_transaction() and _SCHEMA.fullmatch(policy.schema) is not None)
    _require(
        type(producer_identity) is tuple
        and len(producer_identity) == 2
        and re.fullmatch(r"[0-9a-f]{40}", producer_identity[0])
        and re.fullmatch(r"sha256:[0-9a-f]{64}", producer_identity[1])
    )
    source_owner, retained_owner, _readers, retained_readers = await _role_oids(
        connection,
        policy,
        retained_owner_role,
        retained_publisher_role,
    )
    namespace = await connection.fetchrow(
        "SELECT n.oid::bigint,pg_get_userbyid(n.nspowner)::text AS owner,"
        "r.rolcanlogin OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls AS elevated "
        "FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname=$1",
        policy.schema,
    )
    _require(namespace is not None and namespace["owner"] == namespace_owner_role and not namespace["elevated"])
    _require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles r WHERE NOT r.rolsuper AND r.rolname<>$1::name "
            "AND (pg_has_role(r.oid,$1::name,'SET') OR pg_has_role(r.oid,$1::name,'USAGE')))",
            namespace_owner_role,
        )
    )
    _require(
        not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1)", namespace["oid"])
    )
    catalog_rows = await _catalog_rows(connection, policy, retained_owner, retained_readers)
    proofs = tuple(
        sorted(
            [
                await _table_proof(connection, catalog_row, policy, retained_owner, source_owner, retained_readers)
                for catalog_row in catalog_rows
            ]
        )
    )
    return NppesIsolatedCatalog(
        policy.schema,
        namespace["oid"],
        policy.owner_role,
        policy.publisher_role,
        retained_owner_role,
        retained_publisher_role,
        namespace_owner_role,
        policy.runtime_roles,
        producer_identity,
        proofs,
    )


async def verify_isolated_nppes_catalog(connection, policy, catalog):
    """Recheck exact native identities, grants and policies before trusting an issued family."""
    _require(type(catalog) is NppesIsolatedCatalog and catalog.contract == CONTRACT)
    _require(
        (catalog.schema, catalog.owner_role, catalog.publisher_role, catalog.runtime_roles)
        == (policy.schema, policy.owner_role, policy.publisher_role, policy.runtime_roles)
    )
    actual = await capture_isolated_nppes_catalog(
        connection,
        policy,
        retained_owner_role=catalog.retained_owner_role,
        retained_publisher_role=catalog.retained_publisher_role,
        namespace_owner_role=catalog.namespace_owner_role,
        producer_identity=catalog.producer_identity,
    )
    _require(actual == catalog)
    return catalog


def issued_isolated_nppes_catalog():
    """Select only the original factory's issued receipt, never configuration flags or tasks."""
    from process import provider_directory_generic_source_policy as source

    factory = source._FACTORY.get()
    if factory is None:
        return None
    catalog = getattr(getattr(factory, "_manifest", None), "nppes_catalog", None)
    if catalog is not None:
        factory._require_current_policy()
        _require(type(catalog) is NppesIsolatedCatalog and catalog.schema == factory.policy.schema)
    return catalog


async def verified_isolated_nppes_catalog(connection, schema, policy):
    """Keep the physical Source publisher and immutable catalog issuance bound together."""
    from process import provider_directory_generic_source_policy as source

    catalog = issued_isolated_nppes_catalog()
    if catalog is None:
        return None
    _require(policy.schema == schema)
    if source._PUBLISHER.get() is None:
        actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
        _require(actor["session"] == actor["current"] == catalog.retained_publisher_role)
    else:
        publisher = source.bound_publisher()
        _require(publisher.connection is connection and publisher.policy == policy)
        _require(publisher.factory._manifest.nppes_catalog is catalog)
    return await verify_isolated_nppes_catalog(connection, policy, catalog)


async def _controller(connection, policy, catalog):
    await verify_isolated_nppes_catalog(connection, policy, catalog)
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    _require(actor["session"] == actor["current"] == catalog.retained_publisher_role)


async def _retain_payload(connection, policy, catalog, names):
    owner = storage_identifier(catalog.retained_owner_role)
    readers = ",".join(
        storage_identifier(name)
        for name in (
            *policy.runtime_roles,
            policy.owner_role,
            policy.publisher_role,
        )
    )
    for name in names:
        relation = storage_relation(policy.schema, name)
        await connection.execute(f"ALTER TABLE {relation} OWNER TO {owner}")
        await connection.execute(f"GRANT SELECT ON {relation} TO {readers}")


async def _insert_retained_controls(connection, schema, prepared):
    from process.nppes_public_evidence_rows import (
        ADMISSION_COLUMNS,
        SOURCE_IDENTITY_COLUMNS,
        SOURCE_RELEASE_COLUMNS,
        admission_values,
        source_identity_values,
        source_release_values,
    )
    from process.nppes_public_evidence_writer import _insert_single

    release = prepared.manifest.release
    for name, columns, values, identical in (
        ("public_evidence_source_identity", SOURCE_IDENTITY_COLUMNS, source_identity_values(release), True),
        ("public_evidence_source_release", SOURCE_RELEASE_COLUMNS, source_release_values(release), False),
        (
            "public_evidence_nppes_registry_admission",
            ADMISSION_COLUMNS,
            admission_values(prepared.admission_row),
            False,
        ),
        (
            "public_evidence_nppes_registry_admission_seal",
            ("admission_ref",),
            (prepared.admission_row.admission_ref,),
            False,
        ),
    ):
        await _insert_single(connection, schema, name, columns, values, allow_identical=identical)


async def commit_isolated_nppes_candidate(connection, policy, catalog, candidate, prepared):
    """Close exact Source writes before full validation and atomic retained publication."""
    from process import nppes_public_evidence_storage as storage
    from process.nppes_public_evidence_validation import validate_nppes_evidence_family

    await _controller(connection, policy, catalog)
    _require(type(candidate) is storage.NppesEvidenceCandidate and candidate.identity == storage._identity(prepared))
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", candidate.identity["source_release_ref"]
    )
    header_row = await connection.fetchrow(
        f"SELECT candidate_storage_json FROM {storage_relation(policy.schema, storage.HEADER)} "
        "WHERE admission_ref=$1 AND source_release_ref=$2 FOR UPDATE",
        candidate.identity["admission_ref"],
        candidate.identity["source_release_ref"],
    )
    _require(header_row is not None)
    previous = storage._binding(
        header_row["candidate_storage_json"], candidate.identity, phases=("prepared", "published")
    )
    _require(previous["policy"] == storage._policy_fields(policy))
    _require(previous["producer_identity"] == list(catalog.producer_identity) == candidate.producer_identity)
    if previous["phase"] == "published":
        await _verify_published_candidate(connection, policy, catalog, previous, prepared)
        return "already_present"
    tables = await storage._verify(connection, policy, previous)
    chunk = previous["chunks"]["table"]["name"]
    await connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, name) for name in (*tables.values(), chunk))
        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    await storage._verify(connection, policy, previous)
    await _retain_payload(connection, policy, catalog, (*tables.values(), chunk))
    frozen = await storage._capture(connection, policy, tables, indexed=True, readable=True, retained_catalog=catalog)
    await storage._capture_chunks(connection, policy, chunk, retained_catalog=catalog)
    await storage._verify_chunk_manifest(connection, policy, previous)
    await validate_nppes_evidence_family(
        connection,
        policy.schema,
        tables,
        prepared,
        prepared_chunks=chunk,
        chunk_count=previous["chunks"]["count"],
    )
    await _insert_retained_controls(connection, policy.schema, prepared)
    await connection.execute(f"DROP TABLE {storage_relation(policy.schema, chunk)}")
    updated_by_field = {
        **previous,
        "phase": "published",
        "tables": frozen,
        "chunks": {**previous["chunks"], "table": None},
    }
    if not updated_by_field["chunks"]["count"]:
        updated_by_field["chunks"]["sha256"] = storage._chunk_seed(updated_by_field)
    updated_by_field["validation_sha256"] = storage._validation_fingerprint(updated_by_field)
    await storage._store(connection, policy.schema, previous, updated_by_field)
    await _verify_published_candidate(connection, policy, catalog, updated_by_field, prepared)
    return "inserted"


async def _verify_published_candidate(connection, policy, catalog, binding, prepared):
    from process import nppes_public_evidence_storage as storage

    tables_by_root = {root: proof["name"] for root, proof in binding["tables"].items()}
    actual = await storage._capture(
        connection, policy, tables_by_root, indexed=True, readable=True, retained_catalog=catalog
    )
    _require(actual == binding["tables"])
    await storage._assert_control_rows(connection, policy.schema, prepared)
    await storage._assert_seal(connection, policy.schema, binding)


async def commit_isolated_nppes_chain(connection, policy, catalog, receipt):
    """Seal an exact complete archive vector using only the independent controller."""
    from process import nppes_public_evidence_chain_writer as chain
    from process.nppes_public_evidence_chain_rows import build_nppes_chain_storage_rows
    from process.nppes_public_evidence_writer import _insert_single

    await _controller(connection, policy, catalog)
    admission, archives = build_nppes_chain_storage_rows(receipt)
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended('nppes-isolated-chain:' || $1,0))",
        receipt.listing_sha256,
    )
    await chain._assert_release_admissions(connection, policy.schema, archives)
    if await chain._has_existing_chain(connection, policy.schema, admission, archives):
        return "already_present"
    await chain._insert_chain(connection, policy.schema, admission, archives)
    await _insert_single(
        connection,
        policy.schema,
        chain._CHAIN_SEAL_TABLE,
        ("chain_ref",),
        (admission.chain_ref,),
    )
    _require(await chain._has_existing_chain(connection, policy.schema, admission, archives))
    return "inserted"


async def _independent_callback(name, *arguments):
    from process import provider_directory_generic_source_policy as source

    catalog = issued_isolated_nppes_catalog()
    factory = source._FACTORY.get()
    _require(catalog is not None and source._PUBLISHER.get() is None and source._OWNER.get() is None)
    callback = getattr(factory, name, None)
    _require(callable(callback))
    outcome = await callback(*arguments)
    factory._require_current_policy()
    _require(issued_isolated_nppes_catalog() is catalog and source._PUBLISHER.get() is None)
    _require(outcome in {"inserted", "already_present"})
    return outcome


async def publish_isolated_nppes_candidate(database, candidate, prepared):
    """Leave PREPARED resumable if no independent freeze authority is available."""
    from process import nppes_public_evidence_storage as storage
    from process import provider_directory_generic_source_policy as source

    catalog = issued_isolated_nppes_catalog()
    async with storage._transaction(database) as (connection, policy):
        await verified_isolated_nppes_catalog(connection, policy.schema, policy)
        binding = await storage._locked(connection, policy, candidate, phases=("prepared", "published"))
        await storage._verify(connection, policy, binding)
    outcome = await _independent_callback("freeze_nppes_candidate", candidate, prepared)
    async with source.source_transaction(database):
        publisher = source.bound_publisher()
        await verified_isolated_nppes_catalog(publisher.connection, publisher.policy.schema, publisher.policy)
        binding = await storage._locked(publisher.connection, publisher.policy, candidate, phases=("published",))
        await _verify_published_candidate(publisher.connection, publisher.policy, catalog, binding, prepared)
    return outcome


async def publish_isolated_nppes_chain(database, receipt):
    """Release every Source transaction before asking the controller to seal the vector."""
    from process import nppes_public_evidence_chain_writer as chain
    from process import provider_directory_generic_source_policy as source
    from process.nppes_public_evidence_chain_rows import build_nppes_chain_storage_rows

    outcome = await _independent_callback("seal_nppes_chain", receipt)
    async with source.source_transaction(database):
        publisher = source.bound_publisher()
        await verified_isolated_nppes_catalog(publisher.connection, publisher.policy.schema, publisher.policy)
        admission, archives = build_nppes_chain_storage_rows(receipt)
        await chain._assert_release_admissions(publisher.connection, publisher.policy.schema, archives)
        _require(await chain._has_existing_chain(publisher.connection, publisher.policy.schema, admission, archives))
    return outcome
