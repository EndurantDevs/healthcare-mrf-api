# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic snapshot children of empty native geocoder parents, identified by name."""

import hashlib
import re

from sqlalchemy import text

from process import entity_address_publication_permissions as permissions
from process import reference_family_archive as archive

CONTRACT = "tiger.inherited-snapshot.v1"
TABLES = ("zcta5", "zip_state")
_CHILD_SCHEMA = re.compile(r"reference_family_archive_[0-9a-f]{32}\Z")

PARENT_SHAPE_SQL = """
        SELECT count(*)=2 AND bool_and(c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
          AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
          AND NOT EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid)
          AND NOT EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=c.oid)
          AND NOT EXISTS(SELECT 1 FROM pg_rewrite r WHERE r.ev_class=c.oid)
          AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype NOT IN ('p','n')))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=c.oid AND d.objsubid=0
          AND d.refclassid='pg_extension'::regclass AND d.deptype='e'
        JOIN pg_extension e ON e.oid=d.refobjid AND e.extname='postgis_tiger_geocoder'
        WHERE n.nspname='tiger' AND c.relname IN ('zip_state','zcta5')
    """


def _require(value, message):
    if not value:
        raise archive.ReferenceFamilyArchiveError("TIGER inheritance " + message)


def relation_names(inventory):
    """Project both saved editions to their recorded qualified names."""
    try:
        names = tuple(sorted((row["schema_name"], row["relation_name"]) for row in inventory["relations"]))
        return _child_names(names)
    except KeyError, TypeError, ValueError:
        raise archive.ReferenceFamilyArchiveError("TIGER inheritance inventory differs") from None


def _child_names(names, *, empty=False):
    try:
        names = tuple(names)
    except TypeError:
        raise archive.ReferenceFamilyArchiveError("TIGER inheritance child names differ") from None
    _require(
        all(
            isinstance(pair, (tuple, list)) and len(pair) == 2 and all(isinstance(part, str) for part in pair)
            for pair in names
        ),
        "child names differ",
    )
    names = tuple(tuple(pair) for pair in names)
    if empty and not names:
        return ()
    _require(
        len(names) == 2
        and tuple(name for _schema, name in names) == TABLES
        and len({schema for schema, _name in names}) == 1
        and isinstance(names[0][0], str)
        and _CHILD_SCHEMA.fullmatch(names[0][0]),
        "child namespace differs",
    )
    return names


def parent_names(inventory):
    """Old parent IDs and hashes remain opaque; fixed recorded root names are portable."""
    _require(isinstance(inventory, dict) and inventory.get("contract") == CONTRACT, "parent inventory differs")
    if "parent_oids" in inventory:
        pairs = inventory["parent_oids"]
        _require(
            isinstance(pairs, list)
            and all(isinstance(pair, (list, tuple)) and len(pair) == 2 for pair in pairs)
            and tuple(pair[0] for pair in pairs) == TABLES,
            "parent inventory differs",
        )
        return tuple(("tiger", pair[0]) for pair in pairs)
    recorded = inventory.get("parents")
    _require(
        isinstance(recorded, (list, tuple))
        and all(isinstance(pair, (list, tuple)) and len(pair) == 2 for pair in recorded),
        "parent inventory differs",
    )
    pairs = tuple(tuple(pair) for pair in recorded)
    _require(pairs == tuple(("tiger", name) for name in TABLES), "parent inventory differs")
    return pairs


def receipt_relations(receipt, inventory):
    """Join historical receipt names to its saved generation inventory, never live IDs."""
    _require(isinstance(receipt, dict) and receipt.get("contract") == CONTRACT, "receipt differs")
    names = relation_names(inventory)
    parent_names(receipt.get("parent_inventory"))
    if "relations" in receipt:
        _require(_child_names(receipt["relations"]) == names, "receipt relations differ")
    else:
        pairs = receipt.get("relation_oids")
        _require(
            isinstance(pairs, list)
            and all(isinstance(pair, (list, tuple)) and len(pair) == 2 for pair in pairs)
            and tuple(pair[0] for pair in pairs) == TABLES,
            "receipt relations differ",
        )
    return names


async def current_tiger_snapshot_children(session):
    """Resolve exactly two single-parent leaves without flattening an unknown graph."""
    rows = (
        await session.execute(
            text("""
        SELECT parent.relname,namespace.nspname,child.relname,
          NOT EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhparent=child.oid)
          AND (SELECT count(*) FROM pg_inherits i WHERE i.inhrelid=child.oid)=1 AS leaf
        FROM pg_inherits edge JOIN pg_class parent ON parent.oid=edge.inhparent
        JOIN pg_class child ON child.oid=edge.inhrelid JOIN pg_namespace namespace ON namespace.oid=child.relnamespace
        WHERE edge.inhparent IN (to_regclass('tiger.zip_state'),to_regclass('tiger.zcta5'))
        ORDER BY parent.relname
    """)
        )
    ).all()
    if not rows:
        return ()
    _require(
        len(rows) == 2 and tuple(row[0] for row in rows) == TABLES and all(row[3] and row[0] == row[2] for row in rows),
        "child graph differs",
    )
    return _child_names((row[1], row[2]) for row in rows)


async def require_tiger_snapshot_parents(session, *, lock_mode="ACCESS SHARE", empty=True):
    """Keep native root shape, sequence dependencies and real lock/read permissions."""
    archive._require_transaction(session)
    _require(lock_mode in {"ACCESS SHARE", "ACCESS EXCLUSIVE"}, "lock mode differs")
    for name in TABLES:
        await session.execute(text(f'LOCK TABLE ONLY tiger."{name}" IN {lock_mode} MODE NOWAIT'))
    valid = await session.scalar(text(PARENT_SHAPE_SQL))
    _require(valid, "parent catalog differs")
    await _require_tiger_parent_sequence(session)
    if empty:
        for name in TABLES:
            _require(
                not await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM ONLY tiger."{name}" LIMIT 1)')),
                "parent contains data",
            )
    # Existing cutover callers read parent_oids; each pair now carries the qualified name, never an OID.
    return {
        "contract": CONTRACT,
        "parents": [["tiger", name] for name in TABLES],
        "parent_oids": [[name, {"schema": "tiger", "name": name}] for name in TABLES],
    }


def _is_saved_token_pairs(pairs):
    """Recognize saved (table, oid) child pairs; the second element is an opaque historical token."""
    try:
        pairs = tuple(pairs)
    except TypeError:
        return False
    return bool(pairs) and all(
        isinstance(pair, (tuple, list)) and len(pair) == 2 and not isinstance(pair[1], str) for pair in pairs
    )


async def _expected_child_names(session, expected_children):
    """Validate saved table names and resolve their current leaves by name; otherwise require qualified names."""
    if not _is_saved_token_pairs(expected_children):
        return _child_names(expected_children, empty=True)
    _require(tuple(pair[0] for pair in expected_children) == TABLES, "child names differ")
    current = await current_tiger_snapshot_children(session)
    _require(tuple(name for _schema, name in current) == TABLES, "child fence differs")
    return current


async def capture_tiger_snapshot_parents(
    session, *, expected_children=(), expected_parent_inventory=None, owner_oid=None
):
    """Fence the logical parent/child graph atomically; PostgreSQL enforces authority."""
    parents = await require_tiger_snapshot_parents(session, lock_mode="ACCESS EXCLUSIVE")
    if expected_parent_inventory is not None:
        _require(parent_names(expected_parent_inventory) == parent_names(parents), "parent fence differs")
    expected = await _expected_child_names(session, expected_children)
    _require(await current_tiger_snapshot_children(session) == expected, "child fence differs")
    return parents


async def _require_tiger_parent_sequence(session):
    """Admit only the original owned integer sequence and canonical gid default."""
    sequence = await session.scalar(
        text("""
        SELECT s.oid::bigint FROM pg_class s JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=s.oid
          AND d.refclassid='pg_class'::regclass AND d.deptype='a'
        JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid
        JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
        JOIN pg_sequence q ON q.seqrelid=s.oid
        WHERE d.refobjid='tiger.zcta5'::regclass AND a.attname='gid' AND s.relkind='S'
          AND s.relname='zcta5_gid_seq' AND s.relnamespace='tiger'::regnamespace
          AND pg_get_expr(ad.adbin,ad.adrelid) IN ('nextval(''tiger.zcta5_gid_seq''::regclass)',
            'nextval(''zcta5_gid_seq''::regclass)')
          AND q.seqtypid='integer'::regtype AND q.seqincrement=1 AND q.seqmin=1 AND q.seqmax=2147483647
          AND q.seqstart=1 AND q.seqcache=1 AND NOT q.seqcycle
    """)
    )
    _require(sequence is not None, "parent sequence differs")


async def _require_exchange_leaf_layout(session, incoming, predecessors):
    """Read-lock frozen leaves; the native inheritance DDL takes its own write locks."""
    for schema, name in sorted((*incoming, *predecessors)):
        relation = archive._quoted(schema) + "." + archive._quoted(name)
        await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT"))
        _require(
            await session.scalar(
                text("""SELECT relkind='r' AND relpersistence='p' AND NOT relispartition
            AND NOT relrowsecurity AND NOT relforcerowsecurity FROM pg_class WHERE oid=to_regclass(:name)"""),
                {"name": relation},
            ),
            "child catalog differs",
        )
    for schema, name in incoming:
        relation = archive._quoted(schema) + "." + archive._quoted(name)
        _require(
            not await session.scalar(
                text(
                    "SELECT EXISTS(SELECT 1 FROM pg_inherits "
                    "WHERE inhrelid=to_regclass(:name) OR inhparent=to_regclass(:name))"
                ),
                {"name": relation},
            ),
            "incoming child is already inherited",
        )
        _require(
            await session.scalar(
                text("""
            SELECT ARRAY(SELECT ROW(attname,atttypid,atttypmod,attcollation,attnotnull,attidentity,attgenerated)::text
                         FROM pg_attribute WHERE attrelid=to_regclass(:name) AND attnum>0 AND NOT attisdropped ORDER BY attnum)
                = ARRAY(SELECT ROW(attname,atttypid,atttypmod,attcollation,attnotnull,attidentity,attgenerated)::text
                         FROM pg_attribute WHERE attrelid=to_regclass(:parent) AND attnum>0 AND NOT attisdropped ORDER BY attnum)
        """),
                {"name": relation, "parent": "tiger." + name},
            ),
            "child columns differ",
        )


async def exchange_tiger_snapshot_children(
    session, *, incoming_pairs, expected_children, expected_parent_inventory, owner_oid=None
):
    """Exchange named leaves in the caller transaction, retaining detached rollback data."""
    archive._require_transaction(session)
    incoming = _child_names(incoming_pairs)
    predecessors = await _expected_child_names(session, expected_children)
    _require(not set(incoming) & set(predecessors), "incoming child is already inherited")
    parents = await capture_tiger_snapshot_parents(
        session, expected_children=predecessors, expected_parent_inventory=expected_parent_inventory
    )
    await _require_exchange_leaf_layout(session, incoming, predecessors)
    for name in TABLES:
        await permissions.make_relation_read_only(session, "tiger", name)
    for schema, name in predecessors:
        await session.execute(
            text(f'ALTER TABLE {archive._quoted(schema)}.{archive._quoted(name)} NO INHERIT tiger."{name}"')
        )
    for schema, name in incoming:
        await session.execute(
            text(f'ALTER TABLE {archive._quoted(schema)}.{archive._quoted(name)} INHERIT tiger."{name}"')
        )
    await capture_tiger_snapshot_parents(session, expected_children=incoming, expected_parent_inventory=parents)
    return {
        "contract": CONTRACT,
        "parent_inventory": parents,
        "relations": [list(pair) for pair in incoming],
        "predecessors": [list(pair) for pair in predecessors],
    }


async def activate_tiger_snapshot_children(
    session, *, ownership, manifest, validation_receipt, cutover, expected_children=(), expected_parent_inventory=None
):
    """Attach a validated package using names; historical physical tokens stay opaque."""
    archive._require_transaction(session)
    family = archive.validate_reference_family_manifest(manifest)
    validation = archive.validate_reference_family_validation_receipt(validation_receipt)
    incoming_entries = tuple((ownership.schema_name, name) for name, _old_token in ownership.relation_oids)
    _require(
        ownership.importer_id == family.importer_id == validation.importer_id == "tiger"
        and ownership.schema_name == archive.reference_family_stage_schema(ownership.dataset_id)
        and cutover.authority == "manual"
        and cutover.source_serving_generation is None
        and family.publication_authority == "captured-epoch",
        "publication mode differs",
    )
    _require(
        validation.package_id == cutover.package_id
        and validation.profile_contract == cutover.profile_contract
        and validation.stage_schema == ownership.schema_name
        and validation.relations == incoming_entries
        and validation.manifest_sha256 == hashlib.sha256(archive._canonical_json(family.as_dict())).hexdigest()
        and archive._has_matching_manifest_stage_tables(family, validation.tables),
        "validation authority differs",
    )
    return await exchange_tiger_snapshot_children(
        session,
        incoming_pairs=incoming_entries,
        expected_children=expected_children,
        expected_parent_inventory=expected_parent_inventory,
    )
