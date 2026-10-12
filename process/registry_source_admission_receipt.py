# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate completed native CMS admissions against retained evidence."""

import hashlib
import json
import re
from dataclasses import asdict
from datetime import timezone

from process.registry_source_admission import RegistrySourceAdmissionError

_TABLES = (
    "registry_source_snapshot",
    "registry_source_admission_receipt",
    "registry_source_observation",
    "registry_identifier_observation",
    "registry_issuer_company_assertion",
    "registry_company_group_assertion",
)
_MAX_DOCUMENT_BYTES = 16384


def _edition_document(edition):
    document = asdict(edition)
    # Fetch time is first-snapshot provenance, not stable edition identity.
    # Keep existing sealed documents valid after a later conditional fetch.
    document.pop("retrieved_at")
    document["snapshot_id"] = str(edition.snapshot_id)
    if edition.published_at is not None:
        document["published_at"] = edition.published_at.astimezone(timezone.utc).isoformat()
    return document


def _canonical(document):
    encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    if len(encoded) > _MAX_DOCUMENT_BYTES:
        raise RegistrySourceAdmissionError("Source admission witness exceeds its bound")
    return encoded


def _admission_receipt(receipt, edition):
    from process.registry_source_import import _COUNTERS, _IDENTITY_COUNTERS, _NATIVE_COUNTERS, _receipt_json

    fields = {*_COUNTERS, "identity_materialization", "native_counts", "copy_sha256", "copy_bytes", "replayed"}
    if edition.source_id in {"plan-finder", "company-listings"}:
        fields.add("copy_batches")
    if edition.source_system == "naic":
        fields.add("source_members")
    if type(receipt) is not dict or set(receipt) != fields:
        raise RegistrySourceAdmissionError("Source admission receipt shape is invalid")
    if edition.source_system == "naic":
        from process.registry_naic_observations import validate_naic_manifest

        validate_naic_manifest(_canonical(receipt["source_members"]).decode(), edition.input_sha256)
        if receipt["copy_batches"] != 3:
            raise RegistrySourceAdmissionError("Company listing receipt members are incomplete")
    for name, allowed in (("identity_materialization", _IDENTITY_COUNTERS), ("native_counts", _NATIVE_COUNTERS)):
        counters_by_field = receipt[name]
        allowed = {*allowed, "replayed"} if name == "identity_materialization" else set(allowed)
        if type(counters_by_field) is not dict or not counters_by_field or set(counters_by_field) - allowed:
            raise RegistrySourceAdmissionError("Source admission receipt counters are invalid")
        if "replayed" in counters_by_field and type(counters_by_field["replayed"]) is not bool:
            raise RegistrySourceAdmissionError("Source admission receipt replay status is invalid")
        if any(
            type(counter_value) is not int or counter_value < 0
            for field, counter_value in counters_by_field.items()
            if field != "replayed"
        ):
            raise RegistrySourceAdmissionError("Source admission receipt counters are invalid")
    if (
        any(type(receipt[name]) is not int or receipt[name] < 0 for name in _COUNTERS)
        or type(receipt["copy_bytes"]) is not int
        or receipt["copy_bytes"] < 21
        or type(receipt["copy_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", receipt["copy_sha256"]) is None
        or type(receipt["replayed"]) is not bool
        or receipt["observations"] != sum(receipt[name] for name in ("accepted", "unresolved", "rejected"))
        or receipt["native_counts"].get("input_rows") != receipt["observations"]
        or receipt["identity_materialization"].get("source_rows") != receipt["observations"]
        or ("copy_batches" in receipt and (type(receipt["copy_batches"]) is not int or receipt["copy_batches"] < 1))
    ):
        raise RegistrySourceAdmissionError("Source admission receipt accounting is invalid")
    _receipt_json(edition, receipt)
    return receipt


_CUSTODY_SQL = """WITH scope AS (
          SELECT n.oid AS schema_oid,n.nspowner,n.nspacl,r.rolcanlogin,r.rolsuper,r.rolcreatedb,
            r.rolcreaterole,r.rolreplication,r.rolbypassrls FROM pg_catalog.pg_namespace n
          JOIN pg_catalog.pg_roles r ON r.oid=n.nspowner WHERE n.nspname=$1
        ), relations AS (
          SELECT c.* FROM pg_catalog.pg_class c JOIN scope s ON c.relnamespace=s.schema_oid
          WHERE c.relname=ANY($2::text[])
        ), acl AS (
          SELECT a.* FROM relations c CROSS JOIN LATERAL
            pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
          UNION ALL SELECT a.* FROM relations c JOIN pg_catalog.pg_attribute att ON att.attrelid=c.oid
            CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) a
        ) SELECT (SELECT system_identifier::text FROM pg_catalog.pg_control_system()) AS system_identifier,
          (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
          pg_catalog.current_database() AS database_name,s.schema_oid::bigint AS schema_oid,s.nspowner::bigint AS owner_oid,
          (SELECT pg_catalog.jsonb_object_agg(c.relname,c.oid::bigint) FROM relations c) AS table_oids,
          (SELECT pg_catalog.jsonb_object_agg(c.relname,pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object(
            'columns',(SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(a.attname,a.atttypid,a.atttypmod,a.attnotnull) ORDER BY a.attnum)
              FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),
            'constraints',(SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(k.conname,k.contype,k.convalidated,
              pg_catalog.pg_get_constraintdef(k.oid)) ORDER BY k.conname)
              FROM pg_catalog.pg_constraint k WHERE k.conrelid=c.oid),
            'indexes',(SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(i.indisunique,i.indisprimary,i.indisvalid,i.indisready,
              pg_catalog.pg_get_indexdef(i.indexrelid)) ORDER BY i.indexrelid)
              FROM pg_catalog.pg_index i WHERE i.indrelid=c.oid))::text,'UTF8')),'hex')) FROM relations c) AS table_layouts,
          NOT s.rolcanlogin AND NOT s.rolsuper AND NOT s.rolcreatedb AND NOT s.rolcreaterole
          AND NOT s.rolreplication AND NOT s.rolbypassrls
          AND (SELECT pg_catalog.count(*) FROM relations)=pg_catalog.cardinality($2::text[])
          AND NOT EXISTS(SELECT 1 FROM relations c WHERE c.relowner<>s.nspowner OR c.relkind<>'r'
            OR c.relpersistence<>'p' OR c.relrowsecurity OR c.relforcerowsecurity OR c.relhasrules
            OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid=c.oid
              AND i.indisprimary AND i.indisvalid AND i.indisready))
          AND NOT EXISTS(SELECT 1 FROM acl a WHERE a.grantee<>s.nspowner
            AND (a.privilege_type<>'SELECT' OR a.is_grantable))
          AND NOT EXISTS(SELECT 1 FROM acl a JOIN pg_catalog.pg_roles grantee ON grantee.oid=a.grantee
            WHERE a.grantee<>s.nspowner AND (grantee.rolsuper OR grantee.rolcreaterole
              OR pg_catalog.pg_has_role(grantee.oid,s.nspowner,'MEMBER')
              OR pg_catalog.pg_has_role(grantee.oid,s.nspowner,'SET')
              OR pg_catalog.pg_has_role(grantee.oid,'pg_write_all_data','MEMBER')))
          AND NOT EXISTS(SELECT 1 FROM pg_catalog.aclexplode(coalesce(s.nspacl,pg_catalog.acldefault('n',s.nspowner))) a
            WHERE a.grantee<>s.nspowner AND a.privilege_type='CREATE')
          AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t JOIN relations c ON c.oid=t.tgrelid
            WHERE NOT t.tgisinternal) AS closed FROM scope s"""


async def _custody(connection, schema):
    """Require protected native ownership and closed table AND column write ACLs."""
    schemas = await connection.fetchval("SELECT pg_catalog.current_schemas(true)")
    if type(schemas) not in (list, tuple) or not schemas or schemas[0] != "pg_catalog":
        return None
    if not await connection.fetchval(
        "SELECT pg_catalog.has_function_privilege(current_user,'pg_catalog.pg_control_system()','EXECUTE')"
    ):
        return None
    custody_row = await connection.fetchrow(
        _CUSTODY_SQL,
        schema,
        list(_TABLES),
    )
    if custody_row is None or custody_row["closed"] is not True:
        return None
    custody_by_field = dict(custody_row)
    custody_by_field.pop("closed")
    for name in ("table_oids", "table_layouts"):
        if type(custody_by_field[name]) is str:
            custody_by_field[name] = json.loads(custody_by_field[name])
    return custody_by_field


async def seal_registry_source_admission(connection, namespace, schema, edition, header_sha256, receipt):
    """Seal only newly validated retained content, in the admission transaction."""
    from process.registry_source_observation_store import registry_source_content_fingerprints

    if (
        await connection.fetchval(
            "SELECT pg_catalog.to_regclass($1)::oid", f"{namespace}.registry_source_admission_receipt"
        )
        is None
    ):
        return
    existing = await connection.fetchrow(
        f"SELECT manifest_json,manifest_sha256 FROM {namespace}.registry_source_admission_receipt WHERE snapshot_id=$1 FOR UPDATE",
        edition.snapshot_id,
    )
    if existing is None and receipt["replayed"]:
        return  # Historical replay does not prove that all old assertions were retained.
    custody = await _custody(connection, schema)
    if custody is None:
        return
    content = await registry_source_content_fingerprints(connection, namespace, edition)
    manifest_by_field = {
        "version": 1,
        "edition": _edition_document(edition),
        "header_sha256": header_sha256,
        "custody": custody,
        "content": content,
        "receipt": _admission_receipt(receipt, edition),
    }
    if existing is not None:
        previous = _validated_document(existing, edition, header_sha256)
        if previous is None or previous["custody"] != custody:
            return
        if previous["content"] != content:
            raise RegistrySourceAdmissionError("Completed source admission evidence is immutable")
        return
    if not _valid_retained_scope(manifest_by_field, edition):
        raise RegistrySourceAdmissionError("Source admission witness scope is invalid")
    encoded = _canonical(manifest_by_field)
    await connection.execute(
        f"INSERT INTO {namespace}.registry_source_admission_receipt(snapshot_id,manifest_sha256,manifest_json) VALUES($1,$2,$3::jsonb)",
        edition.snapshot_id,
        hashlib.sha256(encoded).hexdigest(),
        encoded.decode(),
    )


def _valid_retained_scope(document, edition):
    from process.registry_source_observation_store import source_row_limit

    custody, content = document["custody"], document["content"]
    if (
        type(custody) is not dict
        or set(custody)
        != {
            "system_identifier",
            "database_oid",
            "database_name",
            "schema_oid",
            "owner_oid",
            "table_oids",
            "table_layouts",
        }
        or type(custody["system_identifier"]) is not str
        or re.fullmatch(r"[1-9][0-9]{0,19}", custody["system_identifier"]) is None
        or type(custody["database_name"]) is not str
        or not 1 <= len(custody["database_name"]) <= 63
        or any(
            type(custody[name]) is not int or not 0 < custody[name] < 2**32
            for name in ("database_oid", "schema_oid", "owner_oid")
        )
        or type(custody["table_oids"]) is not dict
        or set(custody["table_oids"]) != set(_TABLES)
        or any(
            type(retained_value) is not int or not 0 < retained_value < 2**32
            for retained_value in custody["table_oids"].values()
        )
        or type(custody["table_layouts"]) is not dict
        or set(custody["table_layouts"]) != set(_TABLES)
        or any(
            type(retained_value) is not str or re.fullmatch(r"[0-9a-f]{64}", retained_value) is None
            for retained_value in custody["table_layouts"].values()
        )
        or type(content) is not list
        or len(content) != 4
    ):
        return False
    maximum = source_row_limit(edition)
    for content_entry, table, factor in zip(content, _TABLES[2:], (1, 3, 1, 1)):
        if (
            type(content_entry) is not dict
            or set(content_entry) != {"table", "rows", "sha256"}
            or content_entry["table"] != table
            or type(content_entry["rows"]) is not int
            or not 0 <= content_entry["rows"] <= maximum * factor
            or type(content_entry["sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", content_entry["sha256"]) is None
        ):
            return False
    return all(
        content_entry["rows"] == document["receipt"][field]
        for content_entry, field in zip(
            content, ("observations", "identifiers", "issuer_assertions", "group_assertions")
        )
    )


def _validated_document(row, edition, header_sha256):
    try:
        document = row["manifest_json"]
        if type(document) is str:
            document = json.loads(document)
        if (
            type(document) is not dict
            or set(document) != {"version", "edition", "header_sha256", "custody", "content", "receipt"}
            or type(document["version"]) is not int
            or document["version"] != 1
            or document["edition"] != _edition_document(edition)
            or document["header_sha256"] != header_sha256
            or hashlib.sha256(_canonical(document)).hexdigest() != row["manifest_sha256"]
        ):
            return None
        _admission_receipt(document["receipt"], edition)
        return document if _valid_retained_scope(document, edition) else None
    except ValueError, TypeError, KeyError:
        return None


async def read_completed_registry_source_admission(connection, namespace, schema, edition, header_sha256):
    """Return the original receipt only for matching protected native content."""
    from process.registry_source_observation_store import RegistryObservationError, registry_source_content_fingerprints

    if (
        await connection.fetchval(
            "SELECT pg_catalog.to_regclass($1)::oid", f"{namespace}.registry_source_admission_receipt"
        )
        is None
    ):
        return None
    snapshot = await connection.fetchrow(
        f"SELECT * FROM {namespace}.registry_source_snapshot WHERE snapshot_id=$1 FOR SHARE", edition.snapshot_id
    )
    if snapshot is None or any(
        snapshot[name] != expected_value for name, expected_value in asdict(edition).items() if name != "retrieved_at"
    ):
        return None
    receipt_row = await connection.fetchrow(
        f"SELECT manifest_json,manifest_sha256 FROM {namespace}.registry_source_admission_receipt WHERE snapshot_id=$1 FOR SHARE",
        edition.snapshot_id,
    )
    if receipt_row is None or (document := _validated_document(receipt_row, edition, header_sha256)) is None:
        return None
    custody = await _custody(connection, schema)
    if custody is None or document["custody"] != custody:
        return None
    try:
        content = await registry_source_content_fingerprints(connection, namespace, edition)
    except RegistryObservationError:
        return None
    if content != document["content"] or custody != await _custody(connection, schema):
        return None
    return document["receipt"]
