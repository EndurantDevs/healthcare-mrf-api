# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native NUCC parent and exact protected physical generation identity."""

from __future__ import annotations

import json
import re
from copy import deepcopy

from sqlalchemy import text

from process.entity_address_alias_guard import ORDINARY_PRINCIPAL_SQL

CONTRACT = "nucc-protected-parent.postgres.v1"
PARENT = '"mrf"."nucc_taxonomy"'
CHILD_TABLE = "nucc_taxonomy"
_CHILD_SCHEMA = re.compile(r"reference_family_archive_[0-9a-f]{32}\Z")
_COLUMNS = (
    ("int_code", 23, False),
    ("code", 1043, True),
    ("grouping", 1043, False),
    ("classification", 1043, False),
    ("specialization", 1043, False),
    ("definition", 25, False),
    ("notes", 25, False),
    ("display_name", 1043, False),
    ("section", 1043, False),
)
_OWNER_RIGHTS = frozenset(
    {
        "INSERT",
        "SELECT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
        "MAINTAIN",
    }
)
_ORDINARY_PRINCIPAL = ORDINARY_PRINCIPAL_SQL.replace("CAST(:owner_oid AS oid)", "c.relowner")
_RELATION_CATALOG_SQL = """descriptions AS (
 SELECT c.oid,jsonb_build_object('oid',c.oid::bigint,'owner',c.relowner::bigint,'kind',c.relkind::text,
 'schema',n.nspname,'schema_oid',n.oid::bigint,'schema_owner',n.nspowner::bigint,'name',c.relname,
 'filenode',c.relfilenode::bigint,'persistence',c.relpersistence::text,'is_partition',c.relispartition,
 'bound',pg_get_expr(c.relpartbound,c.oid,false),'options',c.reloptions,
 'safe',NOT c.relrowsecurity AND NOT c.relforcerowsecurity
 AND NOT EXISTS(SELECT FROM pg_trigger t WHERE t.tgrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_rewrite r WHERE r.ev_class=c.oid)
 AND NOT EXISTS(SELECT FROM pg_policy p WHERE p.polrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_attrdef d WHERE d.adrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid AND (k.contype NOT IN ('p','n')
 OR NOT k.convalidated OR k.condeferrable OR k.condeferred))
 AND NOT EXISTS(SELECT FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid
 AND d.refclassid='pg_extension'::regclass),
 'columns',COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attname,a.atttypid::bigint,a.attnotnull,
 a.atttypmod,a.attidentity,a.attgenerated,a.attisdropped,a.attacl::text,
 CASE WHEN a.attcollation=0 THEN true ELSE EXISTS(SELECT FROM pg_collation coll WHERE coll.oid=a.attcollation
 AND coll.collnamespace='pg_catalog'::regnamespace AND coll.collname='default') END) ORDER BY a.attnum)
 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0),'[]'::jsonb),
 'acl',COALESCE((SELECT jsonb_agg(jsonb_build_array(x.grantee::bigint,x.grantor::bigint,x.privilege_type,
 x.is_grantable) ORDER BY x.grantee,x.grantor,x.privilege_type,x.is_grantable)
 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) x),'[]'::jsonb),
 'ordinary_write',EXISTS(SELECT FROM pg_roles principal WHERE NOT principal.rolsuper
 AND NOT pg_has_role(principal.oid,c.relowner,'MEMBER') AND (
 has_table_privilege(principal.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR has_any_column_privilege(principal.oid,c.oid,'INSERT,UPDATE,REFERENCES')
 OR c.relkind='r' AND has_schema_privilege(principal.oid,n.oid,'CREATE'))),
 'indexes',COALESCE((SELECT jsonb_agg(jsonb_build_object('oid',ic.oid::bigint,'owner',ic.relowner::bigint,
 'primary',i.indisprimary,'unique',i.indisunique,'ready',i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate,
 'plain',i.indexprs IS NULL AND i.indpred IS NULL AND i.indnatts=i.indnkeyatts,'method',am.amname,
 'columns',ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY key(attnum,pos)
 JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum ORDER BY key.pos),
 'code_safe',NOT EXISTS(SELECT FROM unnest(i.indclass) cls(oid) JOIN pg_opclass oc ON oc.oid=cls.oid
 JOIN pg_opfamily family ON family.oid=oc.opcfamily WHERE oc.opcnamespace<>'pg_catalog'::regnamespace AND (
 NOT EXISTS(SELECT FROM pg_depend dependency JOIN pg_extension extension ON extension.oid=dependency.refobjid
 WHERE dependency.classid='pg_opclass'::regclass AND dependency.objid=oc.oid AND dependency.deptype='e'
 AND dependency.refclassid='pg_extension'::regclass AND extension.extname='btree_gin')
 OR EXISTS(SELECT FROM pg_roles principal WHERE NOT principal.rolsuper
 AND NOT pg_has_role(principal.oid,c.relowner,'MEMBER') AND (pg_has_role(principal.oid,oc.opcowner,'MEMBER')
 OR pg_has_role(principal.oid,family.opfowner,'MEMBER') OR EXISTS(SELECT FROM pg_amproc support
 JOIN pg_proc impl ON impl.oid=support.amproc WHERE support.amprocfamily=family.oid
 AND pg_has_role(principal.oid,impl.proowner,'MEMBER')) OR EXISTS(SELECT FROM pg_amop support
 JOIN pg_operator op ON op.oid=support.amopopr JOIN pg_proc impl ON impl.oid=op.oprcode
 WHERE support.amopfamily=family.oid AND (pg_has_role(principal.oid,op.oprowner,'MEMBER')
 OR pg_has_role(principal.oid,impl.proowner,'MEMBER'))))))),
 'classes',ARRAY(SELECT oc.opcname::text FROM unnest(i.indclass) WITH ORDINALITY key(oid,pos)
 JOIN pg_opclass oc ON oc.oid=key.oid ORDER BY key.pos),
 'parents',ARRAY(SELECT inhparent::bigint FROM pg_inherits edge WHERE edge.inhrelid=ic.oid ORDER BY inhparent))
 ORDER BY i.indisprimary DESC,ic.oid) FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid
 JOIN pg_am am ON am.oid=ic.relam WHERE i.indrelid=c.oid),'[]'::jsonb)) AS shape
 FROM selected c JOIN pg_namespace n ON n.oid=c.relnamespace
)"""
_RELATION_CATALOG_SQL = _RELATION_CATALOG_SQL.replace(
    "NOT principal.rolsuper\n AND NOT pg_has_role(principal.oid,c.relowner,'MEMBER')",
    _ORDINARY_PRINCIPAL,
)
CATALOG_SQL = (
    """WITH owner AS (
 SELECT r.oid::bigint AS oid,NOT (r.rolcanlogin OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb
 OR r.rolreplication OR r.rolbypassrls) AS safe FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner
 WHERE n.nspname='hp_snapshot_retention'
), parent AS (SELECT c.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='mrf' AND c.relname='nucc_taxonomy'), selected AS (
 SELECT c.* FROM parent c UNION ALL SELECT c.* FROM pg_inherits i JOIN parent p ON p.oid=i.inhparent
 JOIN pg_class c ON c.oid=i.inhrelid
), """
    + _RELATION_CATALOG_SQL
    + """SELECT jsonb_build_object('owner',(SELECT to_jsonb(owner) FROM owner),
 'parent',(SELECT d.shape || jsonb_build_object('baseline',jsonb_build_object(
 'database_oid',(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),
 'comment',obj_description(p.oid,'pg_class'))) FROM descriptions d JOIN parent p ON p.oid=d.oid),
 'partition',(SELECT jsonb_build_object('strategy',p.partstrat::text,'attributes',p.partattrs::text,
 'count',p.partnatts,'key',pg_get_partkeydef(p.partrelid),'expressions',p.partexprs::text,
 'default',p.partdefid::bigint,'classes_safe',NOT EXISTS(SELECT FROM unnest(p.partclass) cls(oid)
 JOIN pg_opclass oc ON oc.oid=cls.oid JOIN pg_am am ON am.oid=oc.opcmethod
 WHERE oc.opcnamespace<>'pg_catalog'::regnamespace OR am.amname<>'btree' OR oc.opcname NOT IN ('text_ops','varchar_ops')))
 FROM pg_partitioned_table p JOIN parent c ON c.oid=p.partrelid),
 'children',COALESCE((SELECT jsonb_agg(jsonb_build_object('relation',d.shape,'pending',i.inhdetachpending,
 'parents',(SELECT count(*) FROM pg_inherits edge WHERE edge.inhrelid=d.oid),
 'children',(SELECT count(*) FROM pg_inherits edge WHERE edge.inhparent=d.oid)) ORDER BY d.oid)
 FROM pg_inherits i JOIN parent p ON p.oid=i.inhparent JOIN descriptions d ON d.oid=i.inhrelid),'[]'::jsonb))"""
)
CHILD_CATALOG_SQL = (
    """WITH selected AS (SELECT c.* FROM pg_class c WHERE c.oid=CAST(:child_oid AS oid)), """
    + _RELATION_CATALOG_SQL
    + """ SELECT shape FROM descriptions"""
)


class NuccPartitionError(ValueError):
    """The fixed native parent or its physical child no longer matches its contract."""


def _require(condition, reason):
    if not condition:
        raise NuccPartitionError("NUCC protected parent " + reason)


def _oid(value):
    _require(type(value) is int and 0 < value < 2**32, "identity differs")
    return value


def decode_catalog(captured):
    """Decode only native JSON; do not normalize OIDs, literals or omitted fields."""
    return json.loads(captured) if isinstance(captured, str) else captured


def _require_columns(columns):
    _require(type(columns) is list and len(columns) == len(_COLUMNS), "columns differ")
    for observed, expected in zip(columns, _COLUMNS, strict=True):
        _require(
            type(observed) is list
            and len(observed) == 9
            and tuple(observed[:3]) == expected
            and observed[3:] == [-1, "", "", False, None, True],
            "columns differ",
        )


def _require_acl(entries, owner_oid):
    _require(
        type(entries) is list and len(entries) == len({tuple(entry) for entry in entries}),
        "ACL differs",
    )
    owner_privileges = set()
    for grantee, grantor, privilege, grantable in entries:
        _require(grantor == owner_oid and grantable is False, "ACL differs")
        if grantee == owner_oid:
            owner_privileges.add(privilege)
        else:
            _require(
                type(grantee) is int and grantee >= 0 and privilege == "SELECT",
                "ACL differs",
            )
    _require(owner_privileges == _OWNER_RIGHTS, "Owner ACL differs")


def _require_relation(relation, owner_oid):
    _require(type(relation) is dict, "relation missing")
    for field in ("oid", "owner", "schema_oid", "schema_owner"):
        _oid(relation[field])
    _require(
        relation["owner"] == owner_oid
        and relation["persistence"] == "p"
        and relation["safe"] is True
        and relation["ordinary_write"] is False
        and relation["options"] is None,
        "relation custody differs",
    )
    _require_columns(relation["columns"])
    _require_acl(relation["acl"], owner_oid)


def _primary_index(relation, owner_oid):
    indexes = relation["indexes"]
    _require(type(indexes) is list and indexes, "indexes missing")
    for index in indexes:
        _oid(index["oid"])
        _require(
            index["owner"] == owner_oid
            and index["ready"] is True
            and index["plain"] is True
            and index["code_safe"] is True,
            "indexes differ",
        )
    primary_indexes = [index for index in indexes if index["primary"] is True]
    _require(len(primary_indexes) == 1, "primary index differs")
    primary = primary_indexes[0]
    _require(
        primary["unique"] is True
        and primary["method"] == "btree"
        and primary["columns"] == ["code"]
        and primary["classes"] in (["text_ops"], ["varchar_ops"]),
        "primary index differs",
    )
    return primary


def _parent_shape(captured):
    """Validate shared native custody before selecting either closed physical index contract."""
    captured = decode_catalog(captured)
    _require(
        type(captured) is dict and set(captured) == {"owner", "parent", "partition", "children"},
        "catalogue differs",
    )
    owner = captured["owner"]
    _require(type(owner) is dict and owner["safe"] is True, "Owner differs")
    owner_oid = _oid(owner["oid"])
    parent = captured["parent"]
    _require_relation(parent, owner_oid)
    _require(
        parent["schema"] == "mrf"
        and parent["name"] == CHILD_TABLE
        and parent["kind"] == "p"
        and parent["is_partition"] is False
        and parent["bound"] is None
        and parent["filenode"] == 0,
        "parent differs",
    )
    _require(
        captured["partition"]
        == {
            "strategy": "r",
            "attributes": "2",
            "count": 1,
            "key": "RANGE (code)",
            "expressions": None,
            "default": 0,
            "classes_safe": True,
        },
        "partition key differs",
    )
    parent_index = _primary_index(parent, owner_oid)
    _require(
        len(parent["indexes"]) == 1 and parent_index["parents"] == [],
        "parent indexes differ",
    )
    children = captured["children"]
    _require(type(children) is list and len(children) == 1, "child set differs")
    child = _validate_child(children[0], owner_oid, parent["oid"], parent_index["oid"])
    physical_by_field = {
        "contract": CONTRACT,
        "owner_oid": owner_oid,
        "parent_oid": parent["oid"],
        "parent_index_oid": parent_index["oid"],
        "child_oid": child["oid"],
        "child_schema": child["schema"],
        "child_schema_oid": child["schema_oid"],
        "child_table": child["name"],
        "child_filenode": child["filenode"],
    }
    return physical_by_field, child


def validate_parent_catalog(captured):
    """Authenticate a new model child or the exact original Owner-marked preserved heap."""
    captured = decode_catalog(captured)
    physical_by_field, child = _parent_shape(captured)
    if len(child["indexes"]) == 2:
        _require_legacy_code_indexes(child)
        marker = _require_baseline_child(captured, child, physical_by_field)
        _require(marker["protected_native_sha256"] == catalog_digest(captured), "baseline catalogue changed")
    else:
        _require_child_indexes(child)
    return physical_by_field


def validate_preserved_parent_catalog(captured, old_child):
    """Original-admin adoption only: preserve the exact locked legacy heap before its marker exists."""
    physical_by_field, child = _parent_shape(captured)
    _require_original_indexes(old_child)
    _require_original_indexes(child)
    _require(
        child["oid"] == old_child["oid"]
        and child["filenode"] == old_child["filenode"]
        and sorted(index["oid"] for index in child["indexes"])
        == sorted(index["oid"] for index in old_child["indexes"]),
        "preserved indexes changed",
    )
    return physical_by_field


def catalog_digest(captured):
    """The native COMMENT binds this complete catalogue without hashing itself recursively."""
    from process.nucc_protected_control import receipt_digest

    projected = deepcopy(decode_catalog(captured))
    projected["parent"].pop("baseline", None)
    return receipt_digest(projected)


def _require_original_indexes(child):
    """Adoption preserves exactly the two historical or four model indexes, never partial sets."""
    if len(child["indexes"]) == 2:
        _require_legacy_code_indexes(child)
    else:
        _require_child_indexes(child)


def _require_legacy_code_indexes(child):
    primary = _primary_index(child, child["owner"])
    indexes = child["indexes"]
    _require(
        len(indexes) == 2
        and primary["classes"] == ["text_ops"]
        and all(
            index["method"] == "btree"
            and index["unique"] is True
            and index["columns"] == ["code"]
            and index["classes"] == ["text_ops"]
            for index in indexes
        )
        and all(index["parents"] == [] for index in indexes if not index["primary"]),
        "legacy code indexes differ",
    )


def _require_baseline_child(captured, child, physical_by_field):
    from process import nucc_protected_bootstrap as bootstrap
    from process.nucc_protected_control import receipt_digest

    baseline = captured["parent"].get("baseline")
    _require(type(baseline) is dict and set(baseline) == {"database_oid", "comment"}, "baseline missing")
    marker = decode_catalog(baseline["comment"])
    _require(type(marker) is dict, "baseline missing")
    try:
        marker = bootstrap.validate_baseline(marker, physical_by_field, marker.get("source_authority"))
    except bootstrap.native.ReferenceFamilyArchiveError as error:
        raise NuccPartitionError("NUCC protected parent baseline differs") from error
    _require(
        marker["database_oid"] == baseline["database_oid"]
        and marker["child_index_oids"] == sorted(index["oid"] for index in child["indexes"])
        and marker["protected_child_sha256"] == receipt_digest(child),
        "baseline physical catalogue changed",
    )
    return marker


def validate_retained_baseline_child(captured_child, captured_parent):
    """A detached old heap is readable only through the actual immutable Owner baseline identity."""
    captured_parent = decode_catalog(captured_parent)
    current = validate_parent_catalog(captured_parent)
    child = decode_catalog(captured_child)
    _require_relation(child, current["owner_oid"])
    _require(
        child["schema_owner"] == current["owner_oid"]
        and _CHILD_SCHEMA.fullmatch(child["schema"]) is not None
        and child["name"] == CHILD_TABLE
        and child["kind"] == "r"
        and child["is_partition"] is False
        and child["bound"] is None
        and type(child["filenode"]) is int
        and child["filenode"] > 0,
        "retained baseline custody differs",
    )
    _require_legacy_code_indexes(child)
    _require(_primary_index(child, current["owner_oid"])["parents"] == [], "retained baseline is attached")
    attached = deepcopy(child)
    attached.update(is_partition=True, bound="FOR VALUES FROM (MINVALUE) TO (MAXVALUE)")
    _primary_index(attached, current["owner_oid"])["parents"] = [current["parent_index_oid"]]
    physical_by_field = dict(
        current,
        child_oid=child["oid"],
        child_schema=child["schema"],
        child_schema_oid=child["schema_oid"],
        child_filenode=child["filenode"],
    )
    _require_baseline_child(captured_parent, attached, physical_by_field)
    return child


def _validate_child(edge, owner_oid, parent_oid, parent_index_oid):
    _require(
        type(edge) is dict and set(edge) == {"relation", "pending", "parents", "children"},
        "child edge differs",
    )
    _require(
        edge["pending"] is False and edge["parents"] == 1 and edge["children"] == 0,
        "child edge differs",
    )
    child = edge["relation"]
    _require_relation(child, owner_oid)
    _require(
        child["schema_owner"] == owner_oid
        and _CHILD_SCHEMA.fullmatch(child["schema"]) is not None
        and child["name"] == CHILD_TABLE
        and child["kind"] == "r"
        and child["is_partition"] is True
        and child["oid"] != parent_oid
        and type(child["filenode"]) is int
        and child["filenode"] > 0
        and child["bound"] == "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
        "child custody differs",
    )
    primary = _primary_index(child, owner_oid)
    _require(primary["parents"] == [parent_index_oid], "child primary attachment differs")
    return child


def _require_child_indexes(child):
    """Require the exact model's three additional indexes, never arbitrary executable shapes."""
    observed = sorted(
        (
            index["method"],
            index["unique"],
            tuple(index["columns"]),
            tuple(index["classes"]),
        )
        for index in child["indexes"]
        if index["primary"] is False
    )
    expected = sorted(
        [
            ("btree", False, ("int_code",), ("int4_ops",)),
            ("btree", False, ("display_name",), ("text_ops",)),
            (
                "gin",
                False,
                ("classification", "section"),
                ("varchar_ops", "varchar_ops"),
            ),
        ]
    )
    _require(observed == expected and len(child["indexes"]) == 4, "model indexes differ")


async def capture_nucc_parent(session, *, expected_child_oid=None, exclusive=False):
    """Lock real native identities and recapture before returning a generation observation."""
    from process.reference_family_archive import (
        _nucc_catalog_search_path,
        _require_transaction,
    )

    _require_transaction(session)
    async with _nucc_catalog_search_path(session):
        first = decode_catalog(await session.scalar(text(CATALOG_SQL)))
        observed = validate_parent_catalog(first)
        mode = "ACCESS EXCLUSIVE" if exclusive else "ACCESS SHARE"
        await session.execute(text(f"LOCK TABLE ONLY {PARENT} IN {mode} MODE NOWAIT"))
        child = f'"{observed["child_schema"]}"."{CHILD_TABLE}"'
        await session.execute(text(f"LOCK TABLE ONLY {child} IN ACCESS SHARE MODE NOWAIT"))
        after = decode_catalog(await session.scalar(text(CATALOG_SQL)))
        _require(after == first, "catalogue changed")
        if expected_child_oid is not None:
            _require(observed["child_oid"] == _oid(expected_child_oid), "predecessor changed")
        return observed


async def _capture_incoming(session, child_oid, owner_oid):
    """Require a complete detached immutable model, independent of its advertised receipt."""
    from process.entity_address_native_publication import (
        _require_stage_execution_catalog,
    )

    child_oid = _oid(child_oid)
    child = decode_catalog(await session.scalar(text(CHILD_CATALOG_SQL), {"child_oid": child_oid}))
    validate_detached_child(child, owner_oid=owner_oid, expected_oid=child_oid)
    _require(
        not await session.scalar(
            text("SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=:oid OR inhparent=:oid)"),
            {"oid": child_oid},
        ),
        "incoming child is attached",
    )
    await _require_stage_execution_catalog(
        session,
        {"stage_relations": [{"relation_oid": child_oid}]},
        allowed_extensions=("btree_gin",),
    )
    return child


async def _capture_exchange_target(session, child_oid, owner_oid):
    """Only the actual Owner-marked original child may bypass the new-model index contract."""
    from process.entity_address_native_publication import _require_stage_execution_catalog

    child_oid = _oid(child_oid)
    child = decode_catalog(await session.scalar(text(CHILD_CATALOG_SQL), {"child_oid": child_oid}))
    if type(child) is not dict or type(child.get("indexes")) is not list or len(child["indexes"]) != 2:
        return await _capture_incoming(session, child_oid, owner_oid), None
    parent = decode_catalog(await session.scalar(text(CATALOG_SQL)))
    physical = validate_parent_catalog(parent)
    _require(physical["owner_oid"] == _oid(owner_oid) and child["oid"] == child_oid, "baseline actor differs")
    validate_retained_baseline_child(child, parent)
    _require(
        not await session.scalar(
            text("SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=:oid OR inhparent=:oid)"),
            {"oid": child_oid},
        ),
        "baseline child is attached",
    )
    await _require_stage_execution_catalog(
        session, {"stage_relations": [{"relation_oid": child_oid}]}, allowed_extensions=("btree_gin",)
    )
    return child, parent


def validate_detached_child(captured, *, owner_oid, expected_oid):
    """Validate a detached physical generation without inventing publication authority."""
    child = decode_catalog(captured)
    child_oid = _oid(expected_oid)
    owner_oid = _oid(owner_oid)
    _require_relation(child, owner_oid)
    _require(
        child["oid"] == child_oid
        and child["schema_owner"] == owner_oid
        and _CHILD_SCHEMA.fullmatch(child["schema"]) is not None
        and child["name"] == CHILD_TABLE
        and child["kind"] == "r"
        and child["is_partition"] is False
        and child["bound"] is None
        and type(child["filenode"]) is int
        and child["filenode"] > 0,
        "incoming child differs",
    )
    _require_child_indexes(child)
    _require(
        _primary_index(child, owner_oid)["parents"] == [],
        "incoming primary is attached",
    )
    return child


async def exchange_nucc_child(session, *, expected_child_oid, incoming_child_oid):
    """Atomically replace the exact child; a caught native failure cannot commit a partial exchange."""
    from process.reference_family_archive import _require_transaction

    _require_transaction(session)
    async with session.begin_nested():
        await session.connection()
        return await _exchange_nucc_child(session, expected_child_oid, incoming_child_oid)


async def _exchange_nucc_child(session, expected_child_oid, incoming_child_oid):
    """Replace one indexed physical generation; caller owns receipt, source fence and COMMIT."""
    from process.entity_address_snapshot_preparation import _publisher_authority
    from process.reference_family_archive import _nucc_catalog_search_path

    _oid(expected_child_oid)
    _oid(incoming_child_oid)
    _require(expected_child_oid != incoming_child_oid, "incoming child reuses predecessor")
    before = await capture_nucc_parent(session, expected_child_oid=expected_child_oid, exclusive=True)
    _require(await _publisher_authority(session) == before["owner_oid"], "publisher differs")
    async with _nucc_catalog_search_path(session):
        incoming, baseline = await _capture_exchange_target(session, incoming_child_oid, before["owner_oid"])
        relation = f'"{incoming["schema"]}"."{CHILD_TABLE}"'
        await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS EXCLUSIVE MODE NOWAIT"))
        _require(
            await _capture_exchange_target(session, incoming_child_oid, before["owner_oid"]) == (incoming, baseline),
            "incoming child changed",
        )
        outgoing = f'"{before["child_schema"]}"."{CHILD_TABLE}"'
        await session.execute(text(f"LOCK TABLE ONLY {outgoing} IN ACCESS EXCLUSIVE MODE NOWAIT"))
        await _preserve_parent_readers(session, incoming)
        transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
        await session.execute(text(f"ALTER TABLE {PARENT} DETACH PARTITION {outgoing}"))
        await session.execute(
            text(f"ALTER TABLE {PARENT} ATTACH PARTITION {relation} FOR VALUES FROM (MINVALUE) TO (MAXVALUE)")
        )
        after = await capture_nucc_parent(session, expected_child_oid=incoming_child_oid, exclusive=True)
        _require(
            after["parent_oid"] == before["parent_oid"] and after["parent_index_oid"] == before["parent_index_oid"],
            "parent changed",
        )
        _require(
            await session.scalar(text("SELECT pg_current_xact_id()::text")) == transaction_id,
            "transaction changed",
        )
        return {
            "contract": CONTRACT,
            "before": before,
            "after": after,
            "transaction_id": transaction_id,
        }


async def _preserve_parent_readers(session, incoming):
    """Preserve only authenticated nondelegable parent SELECT on the actual next physical child."""
    captured = decode_catalog(await session.scalar(text(CATALOG_SQL)))
    observed = validate_parent_catalog(captured)
    relation = f'"{incoming["schema"]}"."{CHILD_TABLE}"'
    reader_oids = set()
    for grantee, grantor, privilege, grantable in captured["parent"]["acl"]:
        if grantee == observed["owner_oid"] or privilege != "SELECT":
            continue
        _require(grantor == observed["owner_oid"] and grantable is False, "parent reader differs")
        _require(grantee not in reader_oids, "parent reader repeats")
        reader_oids.add(grantee)
        role = (
            "PUBLIC"
            if grantee == 0
            else await session.scalar(
                text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"),
                {"oid": grantee},
            )
        )
        _require(type(role) is str, "parent reader missing")
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{incoming["schema"]}" TO {role}'))
        await session.execute(text(f"GRANT SELECT ON {relation} TO {role}"))
