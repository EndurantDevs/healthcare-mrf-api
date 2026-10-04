# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit snapshot children of empty, unchanged geocoder extension parents."""

import hashlib
import json
import re

from sqlalchemy import text

from process import reference_family_archive as archive

CONTRACT = "tiger.inherited-snapshot.v1"
TABLES = ("zcta5", "zip_state")
_CHILD_SCHEMA = re.compile(r"reference_family_archive_[0-9a-f]{32}\Z")
PARENT_CATALOG_SQL = """SELECT jsonb_build_object(
    'columns',(SELECT jsonb_agg(jsonb_build_array(a.attrelid,a.attnum,a.attname,a.atttypid,a.atttypmod,
        a.attcollation,a.attnotnull,a.attisdropped,a.attidentity,a.attgenerated,d.adbin::text)
        ORDER BY a.attrelid,a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d
        ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=ANY(CAST(:oids AS oid[])) AND a.attnum>0),
    'constraints',(SELECT jsonb_agg(jsonb_build_array(c.oid,c.conrelid,c.contype,c.conindid,
        c.convalidated,c.condeferrable,c.condeferred,pg_get_constraintdef(c.oid)) ORDER BY c.oid)
        FROM pg_constraint c WHERE c.conrelid=ANY(CAST(:oids AS oid[]))),
    'sequences',(SELECT jsonb_agg(to_jsonb(q) ORDER BY q.seqrelid) FROM pg_sequence q JOIN pg_depend d
        ON d.classid='pg_class'::regclass AND d.objid=q.seqrelid AND d.refclassid='pg_class'::regclass
        AND d.deptype='a' WHERE d.refobjid=ANY(CAST(:oids AS oid[]))))"""


def _require(value, message):
    if not value:
        raise archive.ReferenceFamilyArchiveError("TIGER inheritance " + message)


async def current_tiger_snapshot_children(session):
    """Resolve only two single-parent leaves; never flatten an unknown live graph."""
    rows = (
        await session.execute(
            text("""
        SELECT parent.relname,child.oid::bigint,namespace.nspname,
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
    _require(len(rows) == 2 and tuple(catalog_row[0] for catalog_row in rows) == TABLES, "child graph differs")
    _require(
        all(catalog_row[3] for catalog_row in rows) and len({catalog_row[2] for catalog_row in rows}) == 1,
        "child graph differs",
    )
    _require(_CHILD_SCHEMA.fullmatch(rows[0][2]), "child namespace differs")
    return tuple((name, int(oid)) for name, oid, _schema, _leaf in rows)


async def capture_tiger_snapshot_parents(session, *, owner_oid, expected_children=(), expected_parent_inventory=None):
    """Fence fixed genuine parents separately from immutable generation identity.

    Ownership must already have been provisioned by a reviewed one-time action.
    This function never changes extension membership, ownership or configuration.
    The initial receipt is stored in protected installation custody; subsequent callers
    must compare that receipt. A database restore needs independent re-admission.
    """
    archive._require_transaction(session)
    _require(type(owner_oid) is int and owner_oid > 0, "owner is invalid")
    parents = await _lock_tiger_snapshot_parent_roots(session, owner_oid)
    _require(await current_tiger_snapshot_children(session) == tuple(expected_children), "child fence differs")
    for name in TABLES:
        _require(
            not await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM ONLY tiger."{name}" LIMIT 1)')),
            "parent contains data",
        )
    extension = await _tiger_extension_inventory(session, parents)
    closure = await _tiger_parent_object_inventory(session, parents, owner_oid)
    await _require_tiger_parent_sequence(session)
    parent_inventory_map = {
        "contract": CONTRACT,
        "parent_oids": [[catalog_row["relname"], catalog_row["oid"]] for catalog_row in parents],
        "parents": [dict(catalog_row) for catalog_row in parents],
        "objects": [dict(catalog_row) for catalog_row in closure],
        "catalog_sha256": hashlib.sha256(
            json.dumps(
                await session.scalar(
                    text(PARENT_CATALOG_SQL), {"oids": [catalog_row["oid"] for catalog_row in parents]}
                ),
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest(),
        "sequence_state": list(
            (await session.execute(text("SELECT last_value,is_called FROM tiger.zcta5_gid_seq"))).one()
        ),
        "extension_sha256": hashlib.sha256(
            json.dumps(extension, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    _require(
        expected_parent_inventory is None or parent_inventory_map == expected_parent_inventory,
        "parent fence differs",
    )
    return parent_inventory_map


async def _lock_tiger_snapshot_parent_roots(session, owner_oid):
    """Lock and admit exactly two protected, inert genuine geocoder roots."""
    _require(
        await session.scalar(
            text("""SELECT count(*)=2 AND bool_and(c.relowner=:owner
          AND pg_has_role(current_user,c.relowner,'USAGE'))
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='tiger' AND c.relname IN ('zip_state','zcta5')"""),
            {"owner": owner_oid},
        ),
        "parent identity or ownership differs",
    )
    for name in TABLES:
        await session.execute(text(f'LOCK TABLE ONLY tiger."{name}" IN ACCESS EXCLUSIVE MODE NOWAIT'))
    parents = (
        (
            await session.execute(
                text("""
        SELECT c.relname,c.oid::bigint,c.relowner::bigint,c.relfilenode::bigint,
          c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
          AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
          AND NOT EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid)
          AND NOT EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=c.oid)
          AND NOT EXISTS(SELECT 1 FROM pg_rewrite r WHERE r.ev_class=c.oid)
          AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype NOT IN ('p','n')) AS inert,
          e.oid::bigint AS extension_oid,e.extowner::bigint,e.extversion
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=c.oid AND d.objsubid=0
          AND d.refclassid='pg_extension'::regclass AND d.deptype='e'
        JOIN pg_extension e ON e.oid=d.refobjid AND e.extname='postgis_tiger_geocoder'
        WHERE n.nspname='tiger' AND c.relname IN ('zip_state','zcta5') ORDER BY c.relname
    """)
            )
        )
        .mappings()
        .all()
    )
    _require(
        len(parents) == 2
        and tuple(catalog_row["relname"] for catalog_row in parents) == TABLES
        and all(catalog_row["relowner"] == owner_oid and catalog_row["inert"] for catalog_row in parents)
        and len({catalog_row["extension_oid"] for catalog_row in parents}) == 1,
        "parent identity or ownership differs",
    )
    return parents


async def _tiger_extension_inventory(session, parents):
    """Fence whole-extension ownership, members, configuration and initial ACLs."""
    extension_oid = parents[0]["extension_oid"]
    _require(
        not await session.scalar(
            text("""
        SELECT pg_has_role(current_user,extowner,'MEMBER') OR has_schema_privilege(current_user,'tiger','CREATE')
        FROM pg_extension WHERE oid=:oid
    """),
            {"oid": extension_oid},
        ),
        "publisher has extension or namespace authority",
    )
    # Include the entire extension graph/configuration/initial privileges in the
    # immutable fence; a member upgrade is not an implicit snapshot operation.
    extension = await session.scalar(
        text("""
        SELECT jsonb_build_object('oid',e.oid,'owner',e.extowner,'version',e.extversion,
          'namespace',e.extnamespace,'config',e.extconfig,'condition',e.extcondition,
          'members',(SELECT jsonb_agg(jsonb_build_array(d.classid,d.objid,d.objsubid,d.deptype)
                     ORDER BY d.classid,d.objid,d.objsubid) FROM pg_depend d
                     WHERE d.refclassid='pg_extension'::regclass AND d.refobjid=e.oid AND d.deptype='e'),
          'initial_privileges',(SELECT jsonb_agg(jsonb_build_array(p.classoid,p.objoid,p.objsubid,p.privtype,p.initprivs::text)
                     ORDER BY p.classoid,p.objoid,p.objsubid) FROM pg_init_privs p
                     WHERE EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid=p.classoid AND d.objid=p.objoid
                       AND d.refclassid='pg_extension'::regclass AND d.refobjid=e.oid AND d.deptype='e')))
        FROM pg_extension e WHERE e.oid=:oid
    """),
        {"oid": extension_oid},
    )
    return extension


async def _tiger_parent_object_inventory(session, parents, owner_oid):
    """Require the fixed protected roots, primary indexes and owned sequence."""
    extension_oid = parents[0]["extension_oid"]
    closure = (
        (
            await session.execute(
                text("""
        WITH roots AS (SELECT unnest(CAST(:oids AS oid[])) AS oid), objects AS (
          SELECT oid FROM roots UNION SELECT i.indexrelid FROM pg_index i JOIN roots r ON r.oid=i.indrelid
          UNION SELECT d.objid FROM pg_depend d JOIN roots r ON r.oid=d.refobjid
            WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass AND d.deptype='a'
        ) SELECT c.oid::bigint,c.relname,c.relkind::text,c.relowner::bigint,
          ARRAY(SELECT d.refobjid::bigint FROM pg_depend d WHERE d.classid='pg_class'::regclass
            AND d.objid=c.oid AND d.refclassid='pg_extension'::regclass AND d.deptype='e') AS extensions,
          EXISTS(SELECT 1 FROM pg_init_privs p WHERE p.classoid='pg_class'::regclass AND p.objoid=c.oid) AS initial_privileges
        FROM objects JOIN pg_class c USING(oid) ORDER BY c.oid
    """),
                {"oids": [catalog_row["oid"] for catalog_row in parents]},
            )
        )
        .mappings()
        .all()
    )
    _require(
        len(closure) == 5
        and all(
            catalog_row["relowner"] == owner_oid and not catalog_row["initial_privileges"] for catalog_row in closure
        ),
        "parent object closure differs",
    )
    _require(
        sorted(catalog_row["relkind"] for catalog_row in closure) == ["S", "i", "i", "r", "r"],
        "parent object closure differs",
    )
    _require(
        all(
            catalog_row["extensions"] == ([extension_oid] if catalog_row["relkind"] in {"r", "S"} else [])
            for catalog_row in closure
        ),
        "parent extension membership differs",
    )
    _require(
        not await session.scalar(
            text("""
        SELECT EXISTS(SELECT 1 FROM pg_extension e,unnest(COALESCE(e.extconfig,'{}'::oid[])) config(oid)
          WHERE config.oid=ANY(CAST(:oids AS oid[])))
    """),
            {"oids": [catalog_row["oid"] for catalog_row in closure]},
        ),
        "parent configuration differs",
    )
    return closure


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


async def exchange_tiger_snapshot_children(
    session, *, incoming_pairs, expected_children, owner_oid, expected_parent_inventory
):
    """Atomically exchange exact protected leaves; caller owns authority and commit."""
    archive._require_transaction(session)
    incoming_pairs = tuple(tuple(pair) for pair in incoming_pairs)
    predecessor_pairs = tuple(tuple(pair) for pair in expected_children)
    _require(
        tuple(name for name, _oid in incoming_pairs) == TABLES and len({oid for _name, oid in incoming_pairs}) == 2,
        "incoming identity differs",
    )
    parents = await capture_tiger_snapshot_parents(
        session,
        owner_oid=owner_oid,
        expected_children=predecessor_pairs,
        expected_parent_inventory=expected_parent_inventory,
    )
    pairs = incoming_pairs + predecessor_pairs
    qualified_by_oid = await _lock_tiger_snapshot_child_relations(session, pairs, owner_oid)
    _require(
        len(
            {
                relation.split(".")[0]
                for oid, relation in qualified_by_oid.items()
                if oid in dict(incoming_pairs).values()
            }
        )
        == 1,
        "incoming namespace differs",
    )
    for name, oid in incoming_pairs:
        _require(
            not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=:oid OR inhparent=:oid)"), {"oid": oid}
            ),
            "incoming child is already inherited",
        )
        columns = await session.scalar(
            text("""
            SELECT ARRAY(SELECT ROW(attname,atttypid,atttypmod,attcollation,attnotnull,attidentity,attgenerated)::text
                         FROM pg_attribute WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum)
                = ARRAY(SELECT ROW(attname,atttypid,atttypmod,attcollation,attnotnull,attidentity,attgenerated)::text
                         FROM pg_attribute WHERE attrelid=to_regclass(:parent) AND attnum>0 AND NOT attisdropped ORDER BY attnum)
        """),
            {"oid": oid, "parent": "tiger." + name},
        )
        _require(columns, "child columns differ")
    for name, oid in predecessor_pairs:
        await session.execute(text(f'ALTER TABLE {qualified_by_oid[oid]} NO INHERIT tiger."{name}"'))
    for name, oid in incoming_pairs:
        await session.execute(text(f'ALTER TABLE {qualified_by_oid[oid]} INHERIT tiger."{name}"'))
    await capture_tiger_snapshot_parents(
        session, owner_oid=owner_oid, expected_children=incoming_pairs, expected_parent_inventory=parents
    )
    return {
        "contract": CONTRACT,
        "parent_inventory": parents,
        "relation_oids": [list(pair) for pair in incoming_pairs],
        "predecessor_oids": [list(pair) for pair in predecessor_pairs],
    }


async def _lock_tiger_snapshot_child_relations(session, pairs, owner_oid):
    """Lock exact child OIDs in order and recheck protected physical identities."""
    qualified_by_oid = {}
    for _name, oid in sorted(pairs, key=lambda pair: pair[1]):
        catalog_row = (
            await session.execute(
                text("""
            SELECT n.nspname,c.relname,c.relowner,c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
              AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AS heap
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=:oid
        """),
                {"oid": oid},
            )
        ).one_or_none()
        _require(
            catalog_row is not None
            and catalog_row[1] == _name
            and catalog_row[2] == owner_oid
            and catalog_row[3]
            and _CHILD_SCHEMA.fullmatch(catalog_row[0]),
            "child identity or ownership differs",
        )
        relation = f'"{catalog_row[0]}"."{catalog_row[1]}"'
        await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS EXCLUSIVE MODE NOWAIT"))
        _require(await archive._relation_oid(session, catalog_row[0], catalog_row[1]) == oid, "child identity changed")
        qualified_by_oid[oid] = relation
    return qualified_by_oid


async def activate_tiger_snapshot_children(
    session, *, ownership, manifest, validation_receipt, cutover, expected_children=(), expected_parent_inventory=None
):
    """Attach independently validated, sealed native model children without root rotation."""
    archive._require_transaction(session)
    family = archive.validate_reference_family_manifest(manifest)
    validation = archive.validate_reference_family_validation_receipt(validation_receipt)
    _require(
        ownership.importer_id == family.importer_id == validation.importer_id == "tiger"
        and cutover.authority == "manual"
        and cutover.source_serving_generation is None
        and family.publication_authority == "captured-epoch",
        "publication mode differs",
    )
    incumbent = archive.ReferenceFamilyIncumbent("tiger", "tiger", tuple((name, None) for name in TABLES))
    archive._require_validated_cutover_binding(ownership, incumbent, family, validation, cutover)
    await archive.verify_reference_family_stage_ownership(session, ownership)
    await archive._verify_stage_owner(session, ownership, cutover.expected_stage_owner_oid)
    return await exchange_tiger_snapshot_children(
        session,
        incoming_pairs=ownership.relation_oids,
        expected_children=expected_children,
        owner_oid=cutover.sealed_owner_oid,
        expected_parent_inventory=expected_parent_inventory,
    )
