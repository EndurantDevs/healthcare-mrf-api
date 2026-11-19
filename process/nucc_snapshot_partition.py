# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named NUCC generations with native partition and model compatibility checks."""

from __future__ import annotations

import json
import re

from sqlalchemy import text

from process.procedure_taxonomy_signals import mark_procedure_taxonomy_inputs_changed

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
_RELATION_CATALOG_SQL = """descriptions AS (
 SELECT c.oid,jsonb_build_object('kind',c.relkind::text,'schema',n.nspname,'name',c.relname,
 'persistence',c.relpersistence::text,'is_partition',c.relispartition,
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
 a.atttypmod,a.attidentity,a.attgenerated,a.attisdropped,
 CASE WHEN a.attcollation=0 THEN true ELSE EXISTS(SELECT FROM pg_collation coll WHERE coll.oid=a.attcollation
 AND coll.collnamespace='pg_catalog'::regnamespace AND coll.collname='default') END) ORDER BY a.attnum)
 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0),'[]'::jsonb),
 'indexes',COALESCE((SELECT jsonb_agg(jsonb_build_object('schema',ins.nspname,'name',ic.relname,
 'primary',i.indisprimary,'unique',i.indisunique,'ready',i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate,
 'plain',i.indexprs IS NULL AND i.indpred IS NULL AND i.indnatts=i.indnkeyatts,'method',am.amname,
 'columns',ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY key(attnum,pos)
 JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum ORDER BY key.pos),
 'code_safe',NOT EXISTS(SELECT FROM unnest(i.indclass) cls(oid) JOIN pg_opclass oc ON oc.oid=cls.oid
 WHERE oc.opcnamespace<>'pg_catalog'::regnamespace AND NOT EXISTS(SELECT FROM pg_depend dependency
 JOIN pg_extension extension ON extension.oid=dependency.refobjid WHERE dependency.classid='pg_opclass'::regclass
 AND dependency.objid=oc.oid AND dependency.deptype='e' AND dependency.refclassid='pg_extension'::regclass
 AND extension.extname='btree_gin')),
 'classes',ARRAY(SELECT oc.opcname::text FROM unnest(i.indclass) WITH ORDINALITY key(oid,pos)
 JOIN pg_opclass oc ON oc.oid=key.oid ORDER BY key.pos),
 'parents',COALESCE((SELECT jsonb_agg(jsonb_build_array(pn.nspname,pc.relname) ORDER BY pn.nspname,pc.relname)
 FROM pg_inherits edge JOIN pg_class pc ON pc.oid=edge.inhparent
 JOIN pg_namespace pn ON pn.oid=pc.relnamespace WHERE edge.inhrelid=ic.oid),'[]'::jsonb))
 ORDER BY i.indisprimary DESC,ic.relname) FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid
 JOIN pg_namespace ins ON ins.oid=ic.relnamespace JOIN pg_am am ON am.oid=ic.relam
 WHERE i.indrelid=c.oid),'[]'::jsonb)) AS shape
 FROM selected c JOIN pg_namespace n ON n.oid=c.relnamespace
)"""
CATALOG_SQL = (
    """WITH parent AS (SELECT c.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='mrf' AND c.relname='nucc_taxonomy'), selected AS (
 SELECT c.* FROM parent c UNION ALL SELECT c.* FROM pg_inherits i JOIN parent p ON p.oid=i.inhparent
 JOIN pg_class c ON c.oid=i.inhrelid), """
    + _RELATION_CATALOG_SQL
    + """
 SELECT jsonb_build_object('parent',(SELECT shape FROM descriptions d JOIN parent p ON p.oid=d.oid),
 'partition',(SELECT jsonb_build_object('strategy',p.partstrat::text,'attributes',p.partattrs::text,
 'count',p.partnatts,'key',pg_get_partkeydef(p.partrelid),'expressions',p.partexprs::text,
 'default',p.partdefid<>0,'classes_safe',NOT EXISTS(SELECT FROM unnest(p.partclass) cls(oid)
 JOIN pg_opclass oc ON oc.oid=cls.oid JOIN pg_am am ON am.oid=oc.opcmethod
 WHERE oc.opcnamespace<>'pg_catalog'::regnamespace OR am.amname<>'btree' OR oc.opcname NOT IN ('text_ops','varchar_ops')))
 FROM pg_partitioned_table p JOIN parent c ON c.oid=p.partrelid),
 'children',COALESCE((SELECT jsonb_agg(jsonb_build_object('relation',d.shape,'pending',i.inhdetachpending,
 'parents',(SELECT count(*) FROM pg_inherits edge WHERE edge.inhrelid=d.oid),
 'children',(SELECT count(*) FROM pg_inherits edge WHERE edge.inhparent=d.oid)) ORDER BY d.shape->>'schema',d.shape->>'name')
 FROM pg_inherits i JOIN parent p ON p.oid=i.inhparent JOIN descriptions d ON d.oid=i.inhrelid),'[]'::jsonb))"""
)
CHILD_CATALOG_SQL = (
    "WITH selected AS (SELECT c.* FROM pg_class c WHERE c.oid=to_regclass(:child)), "
    + _RELATION_CATALOG_SQL
    + " SELECT shape FROM descriptions"
)


class NuccPartitionError(ValueError):
    """A named generation does not match its native partition/model contract."""


def _require(condition, reason):
    if not condition:
        raise NuccPartitionError("NUCC protected parent " + reason)


def decode_catalog(captured):
    """Decode a native JSON catalog without altering its recorded values."""
    return json.loads(captured) if isinstance(captured, str) else captured


def qualified(value):
    """Validate and return one schema-qualified relation name."""
    from process.reference_family_archive import _schema_name

    _require(type(value) in (tuple, list) and len(value) == 2, "relation name differs")
    try:
        qualified_names = tuple(_schema_name(name) for name in value)
    except (TypeError, ValueError, RuntimeError) as error:
        raise NuccPartitionError("NUCC protected parent relation name differs") from error
    _require(tuple(value) == qualified_names, "relation name differs")
    return qualified_names


def relation_sql(value):
    """Quote one validated schema-qualified relation name for native SQL."""
    schema, name = qualified(value)
    return f'"{schema}"."{name}"'


def _require_columns(columns):
    _require(type(columns) is list and len(columns) == len(_COLUMNS), "columns differ")
    for observed, expected in zip(columns, _COLUMNS, strict=True):
        _require(
            type(observed) is list
            and len(observed) == 8
            and tuple(observed[:3]) == expected
            and observed[3:] == [-1, "", "", False, True],
            "columns differ",
        )


def _require_relation(relation):
    _require(type(relation) is dict, "relation missing")
    qualified((relation["schema"], relation["name"]))
    _require(
        relation["persistence"] == "p" and relation["safe"] is True and relation["options"] is None,
        "relation layout differs",
    )
    _require_columns(relation["columns"])


def _primary_index(relation):
    indexes = relation["indexes"]
    _require(type(indexes) is list and indexes, "indexes missing")
    names = []
    for index in indexes:
        _require(
            type(index) is dict and index["ready"] is True and index["plain"] is True and index["code_safe"] is True,
            "indexes differ",
        )
        names.append(qualified((index["schema"], index["name"])))
        _require(type(index["parents"]) is list, "index parents differ")
        for parent in index["parents"]:
            qualified(parent)
    _require(len(names) == len(set(names)), "indexes repeat")
    primary_indexes = [index for index in indexes if index["primary"] is True]
    _require(len(primary_indexes) == 1, "primary index differs")
    primary_indexes = primary_indexes[0]
    _require(
        primary_indexes["unique"] is True
        and primary_indexes["method"] == "btree"
        and primary_indexes["columns"] == ["code"]
        and primary_indexes["classes"] in (["text_ops"], ["varchar_ops"]),
        "primary index differs",
    )
    return primary_indexes


def _index_shapes(child):
    return sorted(
        (index["primary"], index["method"], index["unique"], tuple(index["columns"]), tuple(index["classes"]))
        for index in child["indexes"]
    )


def _require_legacy_code_indexes(child):
    primary = _primary_index(child)
    _require(
        len(child["indexes"]) == 2
        and primary["classes"] == ["text_ops"]
        and all(
            index["method"] == "btree"
            and index["unique"] is True
            and index["columns"] == ["code"]
            and index["classes"] == ["text_ops"]
            for index in child["indexes"]
        )
        and all(index["parents"] == [] for index in child["indexes"] if not index["primary"]),
        "legacy code indexes differ",
    )


def _require_child_indexes(child):
    _primary_index(child)
    observed = sorted(
        (index["method"], index["unique"], tuple(index["columns"]), tuple(index["classes"]))
        for index in child["indexes"]
        if index["primary"] is False
    )
    expected = sorted(
        [
            ("btree", False, ("int_code",), ("int4_ops",)),
            ("btree", False, ("display_name",), ("text_ops",)),
            ("gin", False, ("classification", "section"), ("varchar_ops", "varchar_ops")),
        ]
    )
    _require(observed == expected and len(child["indexes"]) == 4, "model indexes differ")


def _require_original_indexes(child):
    if len(child["indexes"]) == 2:
        _require_legacy_code_indexes(child)
    else:
        _require_child_indexes(child)


def _parent_shape(captured):
    captured = decode_catalog(captured)
    _require(type(captured) is dict and set(captured) == {"parent", "partition", "children"}, "catalogue differs")
    parent = captured["parent"]
    _require_relation(parent)
    _require(
        (parent["schema"], parent["name"]) == ("mrf", CHILD_TABLE)
        and parent["kind"] == "p"
        and parent["is_partition"] is False
        and parent["bound"] is None,
        "parent differs",
    )
    _require(
        captured["partition"]
        == dict(
            strategy="r",
            attributes="2",
            count=1,
            key="RANGE (code)",
            expressions=None,
            default=False,
            classes_safe=True,
        ),
        "partition key differs",
    )
    primary = _primary_index(parent)
    _require(len(parent["indexes"]) == 1 and primary["parents"] == [], "parent indexes differ")
    children = captured["children"]
    _require(type(children) is list and len(children) == 1, "child set differs")
    edge = children[0]
    _require(
        type(edge) is dict
        and set(edge) == {"relation", "pending", "parents", "children"}
        and edge["pending"] is False
        and edge["parents"] == 1
        and edge["children"] == 0,
        "child edge differs",
    )
    child = edge["relation"]
    _require_relation(child)
    _require(
        _CHILD_SCHEMA.fullmatch(child["schema"]) is not None
        and child["name"] == CHILD_TABLE
        and child["kind"] == "r"
        and child["is_partition"] is True
        and child["bound"] == "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)",
        "child layout differs",
    )
    child_primary = _primary_index(child)
    parent_index = (primary["schema"], primary["name"])
    _require(child_primary["parents"] == [list(parent_index)], "child primary attachment differs")
    return {
        "contract": CONTRACT,
        "parent": ("mrf", CHILD_TABLE),
        "child": (child["schema"], child["name"]),
        "parent_index": parent_index,
    }, child


def validate_parent_catalog(captured):
    """Validate the permanent parent, selected child and complete native index contract."""
    observed, child = _parent_shape(captured)
    _require_original_indexes(child)
    return observed


def validate_preserved_parent_catalog(captured, old_child):
    """Require complete model and index parity across preserved adoption."""
    observed, child = _parent_shape(captured)
    _require_original_indexes(old_child)
    _require_original_indexes(child)
    _require(_index_shapes(child) == _index_shapes(old_child), "preserved indexes changed")
    return observed


def validate_retained_baseline_child(captured_child, captured_parent):
    """Validate the saved two-index baseline as a detached rollback child."""
    validate_parent_catalog(captured_parent)
    child = validate_detached_child(captured_child, legacy=True)
    _require_legacy_code_indexes(child)
    return child


def catalog_digest(captured):
    """Hash only the named native catalog declaration with canonical encoding."""
    from process.nucc_protected_control import receipt_digest

    return receipt_digest(decode_catalog(captured))


def validate_detached_child(captured, *, expected=None, legacy=False):
    """Require the declared detached child model and its unattached primary index."""
    child = decode_catalog(captured)
    _require_relation(child)
    name = (child["schema"], child["name"])
    _require(
        _CHILD_SCHEMA.fullmatch(child["schema"]) is not None
        and child["name"] == CHILD_TABLE
        and child["kind"] == "r"
        and child["is_partition"] is False
        and child["bound"] is None,
        "incoming child differs",
    )
    if expected is not None:
        _require(name == qualified(expected), "incoming child differs")
    _require_original_indexes(child) if legacy else _require_child_indexes(child)
    _require(_primary_index(child)["parents"] == [], "incoming primary is attached")
    return child


async def capture_nucc_parent(session, *, expected_child=None):
    """Pin both declared names, validate native layout and recheck the selected child."""
    from process.reference_family_archive import _nucc_catalog_search_path, _require_transaction

    _require_transaction(session)
    async with _nucc_catalog_search_path(session):
        await session.execute(text(f"LOCK TABLE ONLY {PARENT} IN ACCESS SHARE MODE NOWAIT"))
        observed = validate_parent_catalog(await session.scalar(text(CATALOG_SQL)))
        await session.execute(text(f"LOCK TABLE ONLY {relation_sql(observed['child'])} IN ACCESS SHARE MODE NOWAIT"))
        after = validate_parent_catalog(await session.scalar(text(CATALOG_SQL)))
        _require(after == observed, "named generation changed")
        if expected_child is not None:
            _require(after["child"] == qualified(expected_child), "predecessor changed")
        return after


async def _capture_incoming(session, child):
    child = qualified(child)
    observed = validate_detached_child(
        await session.scalar(text(CHILD_CATALOG_SQL), {"child": relation_sql(child)}), expected=child
    )
    return observed


async def _capture_exchange_target(session, child):
    child = qualified(child)
    observed = validate_detached_child(
        await session.scalar(text(CHILD_CATALOG_SQL), {"child": relation_sql(child)}), expected=child, legacy=True
    )
    return observed, None


async def exchange_nucc_child(session, *, expected_child, incoming_child):
    """Exchange complete named children atomically in the caller-owned savepoint."""
    from process.reference_family_archive import _require_transaction

    _require_transaction(session)
    async with session.begin_nested():
        await session.connection()
        return await _exchange_nucc_child(session, expected_child, incoming_child)


async def _exchange_nucc_child(session, expected_child, incoming_child):
    expected_child, incoming_child = qualified(expected_child), qualified(incoming_child)
    _require(expected_child != incoming_child, "incoming child reuses predecessor")
    before = await capture_nucc_parent(session, expected_child=expected_child)
    relation = relation_sql(incoming_child)
    await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT"))
    await _capture_exchange_target(session, incoming_child)
    outgoing = relation_sql(before["child"])
    await session.execute(text(f"LOCK TABLE ONLY {outgoing} IN ACCESS SHARE MODE NOWAIT"))
    await _preserve_parent_readers(session, incoming_child)
    await session.execute(text(f"ALTER TABLE {PARENT} DETACH PARTITION {outgoing}"))
    await session.execute(
        text(f"ALTER TABLE {PARENT} ATTACH PARTITION {relation} FOR VALUES FROM (MINVALUE) TO (MAXVALUE)")
    )
    after = await capture_nucc_parent(session, expected_child=incoming_child)
    _require(after["parent"] == before["parent"] and after["parent_index"] == before["parent_index"], "parent changed")
    await mark_procedure_taxonomy_inputs_changed(after["parent"][0], lambda statement: session.execute(text(statement)))
    return {"contract": CONTRACT, "before": before, "after": after}


async def _preserve_parent_readers(session, incoming):
    from process.reference_family_archive import model_family_roles

    _owner, readers = model_family_roles()
    schema, _name = qualified(incoming)
    for role in readers:
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
        await session.execute(text(f'GRANT SELECT ON {relation_sql(incoming)} TO "{role}"'))
