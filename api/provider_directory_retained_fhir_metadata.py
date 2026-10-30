# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify one authenticated generic capture's native metadata without reading it."""

from process.network_custom_address_source import _closed
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_retained_capture import _require_cursor_index, _require_indexes
from process.network_fhir_retained_capture_contract import (
    TABLES,
    RetainedFHIRCapture,
    RetainedFHIRCaptureError,
    canonical,
    digest,
    require,
    validate_capture,
)
from process.network_fhir_source_epoch import _borrowed_epoch_settings
from process.network_membership_writer_closure import _protected_owner

_SNAPSHOT_SQL = """SELECT pg_catalog.pg_backend_pid() AS backend_pid,
 (SELECT virtualxid FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid()
   AND locktype='virtualxid' AND granted) AS virtual_xid,
 pg_catalog.current_setting('transaction_isolation') AS isolation,
 pg_catalog.current_setting('transaction_read_only') AS read_only,
 pg_catalog.current_setting('session_replication_role') AS replication_role,
 session_user::pg_catalog.text AS login,current_user::pg_catalog.text AS actor,
 rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls
 FROM pg_catalog.pg_roles WHERE rolname=session_user"""

# The index deparser opens the indexed heap even for plain native indexes.
_CATALOG_SQL = """WITH index_metadata AS MATERIALIZED (
 SELECT i.indrelid,ix.relname,
   jsonb_build_object('oid',i.indexrelid::bigint,'name',ix.relname,'method',am.amname,
     'owner_oid',ix.relowner::bigint,'filenode',pg_relation_filenode(ix.oid)::bigint,
     'unique',i.indisunique,'primary',i.indisprimary,'valid',i.indisvalid,
     'ready',i.indisready,'live',i.indislive,'key_count',i.indnkeyatts,
     'attribute_count',i.indnatts,'expressions',i.indexprs IS NOT NULL,
     'predicate',i.indpred IS NOT NULL,'options',i.indoption::text,'columns',keys.columns,
     'definition','CREATE ' || CASE WHEN i.indisunique THEN 'UNIQUE ' ELSE '' END ||
       'INDEX ' || quote_ident(ix.relname) || ' ON ' || quote_ident(n.nspname) || '.' ||
       quote_ident(c.relname) || ' USING ' || quote_ident(am.amname) || ' (' || keys.definition || ')'
   ) AS catalog_index,
   coalesce(am.amname='btree' AND ix.relkind='i' AND ix.relpersistence='p'
     AND NOT i.indnullsnotdistinct AND ix.reloptions IS NULL
     AND i.indnkeyatts=i.indnatts AND i.indexprs IS NULL AND i.indpred IS NULL
     AND keys.supported,false) AS supported
 FROM pg_index i JOIN pg_class ix ON ix.oid=i.indexrelid JOIN pg_am am ON am.oid=ix.relam
 JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 CROSS JOIN LATERAL (
   SELECT jsonb_agg(a.attname ORDER BY key.ordinal) AS columns,
     string_agg(quote_ident(a.attname) || CASE
       WHEN i.indcollation[key.ordinal::int-1]<>0
         AND i.indcollation[key.ordinal::int-1]<>a.attcollation
       THEN ' COLLATE "C"' ELSE '' END,', ' ORDER BY key.ordinal) AS definition,
     count(*)=i.indnatts AND bool_and(coalesce(
       a.attnum>0 AND NOT a.attisdropped AND indexed.attnum=key.ordinal
       AND NOT indexed.attisdropped AND indexed.attoptions IS NULL
       AND i.indoption[key.ordinal::int-1]=0 AND op.opcdefault
       AND op.opcnamespace='pg_catalog'::regnamespace AND op.opcmethod=ix.relam
       AND ((op.opcintype=a.atttypid AND (SELECT count(*)=1 FROM pg_opclass candidate
         WHERE candidate.opcmethod=ix.relam AND candidate.opcdefault AND candidate.opcintype=a.atttypid)) OR
         (a.atttypid='varchar'::regtype AND op.opcintype='text'::regtype
           AND NOT EXISTS(SELECT FROM pg_opclass candidate WHERE candidate.opcmethod=ix.relam
             AND candidate.opcdefault AND candidate.opcintype=a.atttypid)
           AND (SELECT count(*)=1 AND bool_and(candidate.oid=op.oid)
             FROM pg_opclass candidate JOIN pg_type input ON input.oid=candidate.opcintype
             JOIN pg_type actual ON actual.oid=a.atttypid
             WHERE candidate.opcmethod=ix.relam AND candidate.opcdefault
               AND input.typispreferred AND input.typcategory=actual.typcategory
               AND (candidate.opcintype=a.atttypid OR EXISTS(SELECT FROM pg_cast conversion
                 WHERE conversion.castsource=a.atttypid AND conversion.casttarget=candidate.opcintype
                   AND conversion.castmethod='b' AND conversion.castcontext='i')))))
       AND (i.indcollation[key.ordinal::int-1]=a.attcollation OR
         (c.relname='provider_directory_dataset_resource' AND ix.relname='retained_lookup_1_0'
           AND a.attname IN ('resource_type','resource_id')
           AND i.indcollation[key.ordinal::int-1]='pg_catalog."C"'::regcollation)),false)) AS supported
   FROM unnest(i.indkey::smallint[]) WITH ORDINALITY key(attnum,ordinal)
   LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum
   LEFT JOIN pg_attribute indexed ON indexed.attrelid=i.indexrelid AND indexed.attnum=key.ordinal
   LEFT JOIN pg_opclass op ON op.oid=i.indclass[key.ordinal::int-1]
 ) keys WHERE n.nspname=$1
)
SELECT c.oid::bigint,c.relname,c.relkind,c.relpersistence,c.relowner::bigint,c.relhastriggers,
 c.relrowsecurity,c.relforcerowsecurity,pg_relation_filenode(c.oid)::bigint AS filenode,
 n.oid::bigint AS schema_oid,n.nspowner::bigint,
 EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
 (SELECT jsonb_agg(jsonb_build_array(a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attgenerated,a.attidentity)
    ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)::text AS columns,
 EXISTS(SELECT 1 FROM pg_attrdef d WHERE d.adrelid=c.oid) AS defaults,
 (SELECT jsonb_agg(i.catalog_index ORDER BY i.relname) FROM index_metadata i WHERE i.indrelid=c.oid)::text AS indexes,
 (SELECT jsonb_agg(jsonb_build_array(k.oid,k.contype,k.conindid,
    CASE WHEN k.contype='p' THEN pg_get_constraintdef(k.oid) END) ORDER BY k.oid)
    FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype<>'n')::text AS constraints,
 NOT EXISTS(SELECT FROM index_metadata i WHERE i.indrelid=c.oid AND NOT i.supported) AS indexes_supported
 FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid
 WHERE n.nspname=$1 AND c.relkind IN ('r','p','v','m','f','S') ORDER BY c.relname"""


async def _snapshot(connection, capture, expected=None):
    """Reject a changed native caller transaction, including a same-backend restart."""
    require(connection.is_in_transaction() is True)
    backend_pid = connection.get_server_pid()
    require(type(backend_pid) is int and 0 < backend_pid <= 2147483647)
    snapshot_by_field = await connection.fetchrow(_SNAPSHOT_SQL)
    require(
        snapshot_by_field is not None
        and snapshot_by_field["backend_pid"] == backend_pid
        and type(snapshot_by_field["virtual_xid"]) is str
        and bool(snapshot_by_field["virtual_xid"])
        and (snapshot_by_field["isolation"], snapshot_by_field["read_only"])
        in (("repeatable read", "on"), ("serializable", "on"))
        and snapshot_by_field["login"] == snapshot_by_field["actor"]
        and snapshot_by_field["actor"] in capture.runtime_roles
        and snapshot_by_field["rolcanlogin"] is True
        and snapshot_by_field["replication_role"] == "origin"
        and all(
            snapshot_by_field[name] is False
            for name in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        )
        and connection.is_in_transaction() is True
        and connection.get_server_pid() == backend_pid
    )
    snapshot_by_field = dict(snapshot_by_field)
    require(expected is None or snapshot_by_field == expected)
    return snapshot_by_field


def _capture_pin(source_pin):
    """Revalidate exact stored coordinates; CMS, live and successor pins stay closed."""
    require(
        type(source_pin) is PinnedFHIRMembershipSource
        and source_pin.source_id != "cms-npd"
        and source_pin.retained_capture is not None
        and source_pin.retained_epoch is None
        and source_pin.retained_successor is None
        and source_pin.custody_owner_role is None
    )
    PinnedFHIRMembershipSource.__post_init__(source_pin)
    capture = validate_capture(source_pin.retained_capture.as_dict())
    require(
        source_pin.read_schema_name == capture.schema_name
        and source_pin.read_resource_table_oid == dict(capture.relation_oids)[TABLES[1]]
    )
    return capture


def _require_catalog_relations(relations, owner):
    """Reject relation and owner drift before privilege and fingerprint checks."""
    require(
        _protected_owner(owner)
        and tuple(relation_record["relname"] for relation_record in relations) == tuple(sorted(TABLES))
    )
    require(
        all(
            relation_record["relkind"] == b"r"
            and relation_record["relpersistence"] == b"p"
            and relation_record["relowner"] == relation_record["nspowner"] == owner["oid"]
            and not any(
                relation_record[name]
                for name in ("relhastriggers", "relrowsecurity", "relforcerowsecurity", "inherited", "defaults")
            )
            for relation_record in relations
        )
    )


async def _catalog_metadata(connection, capture):
    """Preserve complete capture custody predicates while touching only catalogs."""
    relations = await _catalog_relations_metadata(connection, capture.schema_name)
    owner = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", capture.owner_role)
    _require_catalog_relations(relations, owner)
    require(
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1::oid) AND NOT EXISTS(SELECT FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class WHERE c.relnamespace=$1::oid)",
            capture.schema_oid,
        )
        is True
    )
    await _closed(connection, capture.schema_oid, owner["oid"], capture.runtime_roles, list(TABLES))
    require(
        await connection.fetchval(
            "WITH configured AS (SELECT r.* FROM unnest($1::text[]) selected(name) LEFT JOIN pg_roles r ON r.rolname=selected.name) "
            "SELECT NOT EXISTS(SELECT FROM configured r WHERE r.oid IS NULL OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls "
            "OR has_parameter_privilege(r.oid,'session_replication_role','SET')) AND NOT EXISTS(SELECT FROM pg_proc p "
            "WHERE p.prosecdef AND p.prorettype NOT IN ('trigger'::regtype,'event_trigger'::regtype) "
            "AND EXISTS(SELECT FROM pg_roles definer WHERE definer.oid=p.proowner AND (definer.rolsuper OR definer.rolcreaterole "
            "OR pg_has_role(definer.oid,$2::oid,'MEMBER') OR pg_has_role(definer.oid,$2::oid,'SET'))) "
            "AND EXISTS(SELECT FROM configured r WHERE has_schema_privilege(r.oid,p.pronamespace,'USAGE') AND has_function_privilege(r.oid,p.oid,'EXECUTE')))",
            list(capture.runtime_roles),
            owner["oid"],
        )
        is True
    )
    _require_indexes(relations, owner["oid"], capture.schema_name)
    await _require_cursor_index(
        connection,
        dict((relation_record["relname"], relation_record["oid"]) for relation_record in relations)[TABLES[1]],
    )
    observed = (
        relations[0]["schema_oid"],
        tuple((relation_record["relname"], relation_record["oid"]) for relation_record in relations),
        digest(
            [
                {
                    name: catalog_value.decode("ascii") if type(catalog_value) is bytes else catalog_value
                    for name, catalog_value in relation_record.items()
                }
                for relation_record in relations
            ]
        ),
    )
    require(observed == (capture.schema_oid, capture.relation_oids, capture.catalog_sha256))
    comment = await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", capture.schema_oid)
    require(comment == "retained-normalized-fhir-v1:" + canonical(capture.as_dict()))


async def _catalog_relations_metadata(connection, schema):
    """Retain native JSON bytes after proving the plain index renderer is lossless."""
    relations = []
    for observed in await connection.fetch(_CATALOG_SQL, schema):
        relation_by_field = dict(observed)
        require(relation_by_field.pop("indexes_supported") is True)
        relations.append(relation_by_field)
    return relations


async def verify_retained_fhir_metadata(connection, source_pin) -> RetainedFHIRCapture:
    """Verify an already-authenticated stored recipe pin on its native RRRO caller.

    This returns metadata only, without source-content admission or read authority.
    Actual payload reads must retain the original driver, backend and transaction,
    then verify dynamic names and consumed relation identities before exposure.
    """
    try:
        capture = _capture_pin(source_pin)
        snapshot = await _snapshot(connection, capture)
        async with _borrowed_epoch_settings(connection):
            await connection.execute("SET LOCAL search_path TO pg_catalog")
            await connection.execute("SET LOCAL jit = off")
            await _catalog_metadata(connection, capture)
            await _snapshot(connection, capture, snapshot)
        await _snapshot(connection, capture, snapshot)
        return capture
    except RetainedFHIRCaptureError:
        raise
    except Exception as error:
        raise RetainedFHIRCaptureError("retained_fhir_capture_unavailable") from error
