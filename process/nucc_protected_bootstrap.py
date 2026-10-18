# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-time preserved-heap NUCC adoption; a native Owner comment is not a publication."""

from __future__ import annotations

import json
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from process import nucc_snapshot_partition as partition
from process import provider_directory_cms_native_registration as registration
from process import reference_family_archive as native
from process.nucc_protected_control import receipt_digest

CONTRACT = "nucc-protected-baseline.v3"
NATIVE_CONTRACT = "nucc-protected-baseline.v4"
FIELDS = frozenset(
    {
        "contract",
        "database_oid",
        "owner_oid",
        "parent_oid",
        "parent_index_oid",
        "child_oid",
        "child_schema",
        "child_schema_oid",
        "child_filenode",
        "child_index_oids",
        "old_owner_oid",
        "old_schema_oid",
        "old_schema_owner_oid",
        "old_native_sha256",
        "protected_native_sha256",
        "protected_child_sha256",
        "semantic_sha256",
        "row_count",
        "source_authority",
        "source_revision_tracked",
        "adopted_at",
        "baseline_sha256",
    }
)


def _require(condition, reason):
    if not condition:
        raise native.ReferenceFamilyArchiveError("NUCC protected baseline " + reason)


def validate_baseline(baseline_by_field, physical, source_authority):
    """A closed comment can select the initial child only under fresh native custody and zero events."""
    baseline_by_field = partition.decode_catalog(baseline_by_field)
    _require(type(baseline_by_field) is dict, "fields differ")
    is_native_baseline = baseline_by_field.get("contract") == NATIVE_CONTRACT
    _require(
        set(baseline_by_field) == FIELDS | ({"native_registration"} if is_native_baseline else set()), "fields differ"
    )
    _require(baseline_by_field["contract"] in (CONTRACT, NATIVE_CONTRACT), "contract differs")
    if is_native_baseline:
        try:
            registration.validate_native_input_adoption(
                baseline_by_field["native_registration"], schema="mrf", relation_oid=physical["child_oid"]
            )
        except RuntimeError as error:
            raise native.ReferenceFamilyArchiveError("NUCC protected baseline native registration differs") from error
    _require(baseline_by_field["source_authority"] == source_authority, "Source predecessor differs")
    _require(
        all(
            baseline_by_field[key] == physical[key]
            for key in (
                "owner_oid",
                "parent_oid",
                "parent_index_oid",
                "child_oid",
                "child_schema",
                "child_schema_oid",
                "child_filenode",
            )
        ),
        "physical predecessor differs",
    )
    _require(
        baseline_by_field["baseline_sha256"]
        == receipt_digest({key: field for key, field in baseline_by_field.items() if key != "baseline_sha256"}),
        "digest differs",
    )
    _require(
        type(baseline_by_field["row_count"]) is int and 0 <= baseline_by_field["row_count"] <= 10000, "census differs"
    )
    for key in ("database_oid", "old_owner_oid", "old_schema_oid", "old_schema_owner_oid"):
        _require(type(baseline_by_field[key]) is int and 0 < baseline_by_field[key] < 2**32, "identity differs")
    for key in ("old_native_sha256", "protected_native_sha256", "protected_child_sha256", "semantic_sha256"):
        _require(
            type(baseline_by_field[key]) is str
            and len(baseline_by_field[key]) == 64
            and all(character in "0123456789abcdef" for character in baseline_by_field[key]),
            "hash differs",
        )
    _require(type(baseline_by_field["source_revision_tracked"]) is bool, "tracking differs")
    _require(
        type(baseline_by_field["child_index_oids"]) is list
        and len(baseline_by_field["child_index_oids"]) == len(set(baseline_by_field["child_index_oids"]))
        and len(baseline_by_field["child_index_oids"]) in (2, 4)
        and all(type(oid) is int and 0 < oid < 2**32 for oid in baseline_by_field["child_index_oids"]),
        "indexes differ",
    )
    return baseline_by_field


async def _original_admin(session):
    _require(
        isinstance(session, AsyncSession) and session.in_transaction() and not session.in_nested_transaction(),
        "original administrator transaction required",
    )
    return await _native_admin(session)


async def _native_admin(session):
    native_record = (
        (
            await session.execute(
                text("""SELECT pg_backend_pid() AS pid,pg_current_xact_id()::text AS xid,
 (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
 session_user=current_user AND caller.rolsuper
 AND current_setting('transaction_isolation')='read committed'
 AND current_setting('transaction_read_only')='off'
 AND current_setting('session_replication_role')='origin' AS admitted
 FROM pg_roles caller WHERE caller.rolname=session_user""")
            )
        )
        .mappings()
        .one()
    )
    _require(native_record["admitted"] is True, "original administrator required")
    return dict(native_record)


async def _legacy_hook(session, oid, owner_oid):
    from process.cms_doctors_immutable import _legacy_contracts
    from process.reference_source_generation import REVISION_TRIGGER

    hook_records = (
        (
            await session.execute(
                text("""SELECT t.oid::bigint AS oid,t.tgname,p.proname,p.prosrc,
 t.tgtype=60 AND t.tgenabled='A' AND NOT t.tgisinternal AND t.tgnargs=0
 AND t.tgattr::text='' AND t.tgqual IS NULL AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
 AND p.prorettype='trigger'::regtype AND p.pronargs=0 AND p.prokind='f' AND p.prosecdef
 AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND l.lanname='plpgsql'
 AND p.pronamespace='mrf'::regnamespace AND p.proowner=n.nspowner AS shape
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_language l ON l.oid=p.prolang,
 pg_namespace n WHERE t.tgrelid=:oid AND n.nspname='mrf' ORDER BY t.oid"""),
                {"oid": oid},
            )
        )
        .mappings()
        .all()
    )
    _require(len(hook_records) <= 1, "legacy hooks differ")
    native_registration = await _capture_native_registration(session, oid, owner_oid)
    if native_registration is not None:
        _require(
            len(hook_records) == 1
            and hook_records[0]["tgname"] == "cms_native_input_mutation"
            and hook_records[0]["oid"] == native_registration["trigger_oid"],
            "native hook differs",
        )
        return {**dict(hook_records[0]), "native_registration": native_registration}
    if not hook_records:
        return None
    native_record = hook_records[0]
    expected = _legacy_contracts("mrf")[REVISION_TRIGGER]
    _require(
        native_record["shape"] is True
        and native_record["tgname"] == REVISION_TRIGGER
        and native_record["proname"] == expected[1]
        and " ".join(native_record["prosrc"].split()) == " ".join(expected[3].split()),
        "legacy hooks differ",
    )
    return dict(native_record)


async def _registration_driver(session):
    connection = await session.connection()
    return (await connection.get_raw_connection()).driver_connection


async def _capture_native_registration(session, oid, owner_oid):
    candidate = registration.NativeInputCandidate(
        "mrf", partition.CHILD_TABLE, "mrf", partition.CHILD_TABLE, oid, owner_oid, oid
    )
    return await registration.capture_native_input_adoption(
        await _registration_driver(session), ledger_schema="mrf", candidate=candidate
    )


async def _old_heap(session):
    oid = await session.scalar(text("SELECT to_regclass('mrf.nucc_taxonomy')::oid::bigint"))
    _require(type(oid) is int, "legacy heap missing")
    await session.execute(text(f"LOCK TABLE ONLY {partition.PARENT} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    child = partition.decode_catalog(await session.scalar(text(partition.CHILD_CATALOG_SQL), {"child_oid": oid}))
    _require(
        child is not None
        and child["kind"] == "r"
        and child["schema"] == "mrf"
        and child["name"] == partition.CHILD_TABLE
        and child["is_partition"] is False
        and child["bound"] is None
        and child["options"] is None
        and child["persistence"] == "p"
        and child["filenode"] > 0,
        "legacy heap differs",
    )
    partition._require_columns(child["columns"])
    partition._primary_index(child, child["owner"])
    partition._require_original_indexes(child)
    hook = await _legacy_hook(session, oid, child["owner"])
    _require(
        not await session.scalar(
            text("""SELECT EXISTS(SELECT FROM pg_depend d
 WHERE d.refclassid='pg_class'::regclass AND d.refobjid=:oid
 AND (d.classid='pg_rewrite'::regclass OR d.classid='pg_constraint'::regclass
 AND EXISTS(SELECT FROM pg_constraint k WHERE k.oid=d.objid AND k.contype='f')))
 OR EXISTS(SELECT FROM pg_inherits WHERE inhparent=:oid OR inhrelid=:oid)
 OR EXISTS(SELECT FROM pg_constraint WHERE conrelid=:oid AND (contype NOT IN ('p','n') OR NOT convalidated OR condeferrable OR condeferred))
 OR EXISTS(SELECT FROM pg_depend WHERE classid='pg_class'::regclass AND objid=:oid AND refclassid='pg_extension'::regclass)
 OR EXISTS(SELECT FROM pg_policy WHERE polrelid=:oid)
 OR EXISTS(SELECT FROM pg_attrdef WHERE adrelid=:oid)
 OR EXISTS(SELECT FROM pg_class WHERE oid=:oid AND (relrowsecurity OR relforcerowsecurity))"""),
            {"oid": oid},
        ),
        "legacy dependencies differ",
    )
    return child, hook


async def _owner(session):
    native_record = (
        (
            await session.execute(
                text("""SELECT r.oid::bigint AS oid,quote_ident(r.rolname) AS name,
 NOT (r.rolcanlogin OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls) AS safe
 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname='hp_snapshot_retention'""")
            )
        )
        .mappings()
        .one()
    )
    _require(native_record["safe"] is True, "Owner differs")
    return dict(native_record)


async def _preserve_reads(session, child, owner, incoming):
    """Replay only actual nondelegable legacy reads; ordinary mutation never follows the preserved heap."""
    reader_oids = set()
    for grantee, _grantor, privilege, grantable in child["acl"]:
        if privilege != "SELECT" or grantee == owner["oid"]:
            continue
        _require(grantable is False, "delegable legacy read differs")
        if grantee in reader_oids:
            continue
        reader_oids.add(grantee)
        role = (
            "PUBLIC"
            if grantee == 0
            else await session.scalar(
                text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"),
                {"oid": grantee},
            )
        )
        _require(type(role) is str, "legacy reader missing")
        schema = incoming.split(".", 1)[0]
        await session.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {role}"))
        await session.execute(text(f"GRANT SELECT ON {partition.PARENT},{incoming} TO {role}"))


async def _adopt_heap(session, child, hook, owner, dataset_id):
    schema = native.reference_family_stage_schema(dataset_id)
    incoming = f'"{schema}"."{partition.CHILD_TABLE}"'
    await session.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION {owner["name"]}'))
    native_registration = None if hook is None else hook.get("native_registration")
    if hook is not None and native_registration is None:
        await session.execute(text(f'DROP TRIGGER "{hook["tgname"]}" ON {partition.PARENT}'))
    await session.execute(text(f'ALTER TABLE {partition.PARENT} SET SCHEMA "{schema}"'))
    await session.execute(text(f"ALTER TABLE {incoming} OWNER TO {owner['name']}"))
    ownership = await native.capture_model_family_stage_ownership(
        session,
        native.reference_family_spec("nucc"),
        dataset_id,
    )
    # The original administrator preserves read ACLs, while closing every former direct heap grant.
    grantees = {entry[0] for entry in child["acl"] if entry[0] != owner["oid"]}
    for grantee in sorted(grantees):
        role = (
            "PUBLIC"
            if grantee == 0
            else await session.scalar(
                text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"),
                {"oid": grantee},
            )
        )
        _require(type(role) is str, "legacy grant role missing")
        await session.execute(text(f"REVOKE ALL ON {incoming} FROM {role}"))
    if native_registration is not None:
        await _retire_native_guard(session, child, owner, incoming, native_registration)
    await session.execute(
        text(
            f"CREATE TABLE {partition.PARENT} (LIKE {incoming} INCLUDING DEFAULTS "
            "INCLUDING GENERATED INCLUDING IDENTITY INCLUDING STORAGE) PARTITION BY RANGE(code)"
        )
    )
    await session.execute(text(f"ALTER TABLE {partition.PARENT} OWNER TO {owner['name']}"))
    await session.execute(text(f"ALTER TABLE {partition.PARENT} ADD PRIMARY KEY(code)"))
    await _preserve_reads(session, child, owner, incoming)
    await session.execute(
        text(f"ALTER TABLE {partition.PARENT} ATTACH PARTITION {incoming} FOR VALUES FROM (MINVALUE) TO (MAXVALUE)")
    )
    return ownership


async def _retire_native_guard(session, child, owner, incoming, native_registration):
    closed_child = partition.decode_catalog(
        await session.scalar(text(partition.CHILD_CATALOG_SQL), {"child_oid": child["oid"]})
    )
    _require(
        closed_child["owner"] == owner["oid"] and closed_child["ordinary_write"] is False,
        "native writer custody differs",
    )
    _require(
        await _capture_native_registration(session, child["oid"], owner["oid"]) == native_registration,
        "native registration changed",
    )
    await session.execute(text(f'DROP TRIGGER "cms_native_input_mutation" ON {incoming}'))
    await registration.verify_retired_native_input_adoption(
        await _registration_driver(session), evidence=native_registration
    )


async def _require_adoptable_source(session, source_authority, child):
    _require(
        source_authority.relation_oids is None or source_authority.relation_oids == (child["oid"],),
        "Source relation differs",
    )
    # The protected four-ledger projection must be installed by its existing genuine operator first.
    claims = await session.scalar(
        text("SELECT count(*) FROM mrf.nucc_retained_relation_claims WHERE relation_oid=:oid"), {"oid": child["oid"]}
    )
    _require(claims == 0, "legacy heap has retained claims")
    _require(
        not await session.scalar(
            text(
                "SELECT EXISTS(SELECT FROM hp_snapshot_retention.publication "
                "WHERE importer_id='nucc' AND dataset_key='nucc')"
            )
        ),
        "Owner event already exists",
    )


async def adopt_nucc_parent(session, *, dataset_id):
    """Preserve genuine legacy storage; caller owns release admission, full manifest CAS and COMMIT."""
    admin = await _original_admin(session)
    _require(type(dataset_id) is UUID, "dataset differs")
    source_authority = await native.read_reference_family_result_generation_authority(
        session,
        importer_id="nucc",
        schema_name="mrf",
        lock=True,
    )
    child, hook = await _old_heap(session)
    await _require_adoptable_source(session, source_authority, child)
    owner = await _owner(session)
    old_count, old_semantic = await _legacy_contents(session)
    async with session.begin_nested():
        await session.connection()
        ownership = await _adopt_heap(session, child, hook, owner, dataset_id)
        physical = partition.validate_preserved_parent_catalog(await session.scalar(text(partition.CATALOG_SQL)), child)
        count, semantic = await native._projected_row_identity(
            session,
            ownership.schema_name,
            partition.CHILD_TABLE,
            row_json_sql="to_jsonb(row_value)",
        )
        await _prove_preserved(session, child, physical, (old_count, old_semantic), (count, semantic))
        baseline_by_field = await _marker(
            session,
            admin,
            child,
            physical,
            source_authority,
            count,
            semantic,
            (receipt_digest({"heap": child, "hook": hook}), None if hook is None else hook.get("native_registration")),
        )
        comment = await session.scalar(
            text("SELECT quote_literal(:comment)"), {"comment": json.dumps(baseline_by_field, sort_keys=True)}
        )
        await session.execute(text(f"COMMENT ON TABLE {partition.PARENT} IS {comment}"))
        _require(
            partition.validate_parent_catalog(await session.scalar(text(partition.CATALOG_SQL))) == physical,
            "protected baseline changed",
        )
        _require(await _native_admin(session) == admin, "administrator transaction changed")
        _require(
            await native.read_reference_family_result_generation_authority(
                session,
                importer_id="nucc",
                schema_name="mrf",
                lock=True,
            )
            == source_authority,
            "Source authority changed",
        )
        return baseline_by_field


async def _prove_preserved(session, child, physical, before_content, after_content):
    _require(before_content == after_content, "preserved content changed")
    _require(
        physical["child_oid"] == child["oid"] and physical["child_filenode"] == child["filenode"],
        "preserved storage changed",
    )
    catalog = partition.decode_catalog(await session.scalar(text(partition.CATALOG_SQL)))
    _require(
        sorted(index["oid"] for index in catalog["children"][0]["relation"]["indexes"])
        == sorted(index["oid"] for index in child["indexes"]),
        "preserved indexes changed",
    )


async def _legacy_contents(session):
    model = native.reference_family_spec("nucc").model_types[0]
    fields = " + ".join(
        f'COALESCE(octet_length("{column.name}"),0)::bigint'
        for column in model.__table__.columns
        if column.name != "int_code"
    )
    native_record = (
        (
            await session.execute(
                text(f"""SELECT count(*)::bigint AS count,
 count(*) FILTER(WHERE code IS NULL OR code !~ '^[A-Z0-9]{{10}}$' OR int_code IS NULL)::bigint AS invalid,
 count(DISTINCT code)::bigint AS distinct_codes,COALESCE(sum(36 + 2*({fields})),0)::bigint AS bytes
 FROM {partition.PARENT}""")
            )
        )
        .mappings()
        .one()
    )
    _require(
        0 <= native_record["count"] <= 10000
        and native_record["invalid"] == 0
        and native_record["distinct_codes"] == native_record["count"]
        and native_record["bytes"] <= 8 * 1024**2,
        "legacy content envelope differs",
    )
    return await native._projected_row_identity(
        session, "mrf", partition.CHILD_TABLE, row_json_sql="to_jsonb(row_value)"
    )


async def _marker(session, admin, child, physical, source_authority, count, semantic, old_native_preimage):
    old_native_sha256, native_registration = old_native_preimage
    catalog = partition.decode_catalog(await session.scalar(text(partition.CATALOG_SQL)))
    tracked = await session.scalar(
        text("SELECT source_revision_tracked FROM mrf.reference_family_result_generation WHERE importer_id='nucc'")
    )
    baseline_by_field = {
        "contract": CONTRACT if native_registration is None else NATIVE_CONTRACT,
        "database_oid": admin["database_oid"],
        **{
            key: physical[key]
            for key in (
                "owner_oid",
                "parent_oid",
                "parent_index_oid",
                "child_oid",
                "child_schema",
                "child_schema_oid",
                "child_filenode",
            )
        },
        "child_index_oids": sorted(index["oid"] for index in child["indexes"]),
        "old_owner_oid": child["owner"],
        "old_schema_oid": child["schema_oid"],
        "old_schema_owner_oid": child["schema_owner"],
        "old_native_sha256": old_native_sha256,
        "protected_native_sha256": partition.catalog_digest(catalog),
        "protected_child_sha256": receipt_digest(catalog["children"][0]["relation"]),
        "semantic_sha256": semantic,
        "row_count": count,
        "source_authority": source_authority.as_dict(),
        "source_revision_tracked": tracked,
        "adopted_at": await session.scalar(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'")),
    }
    if native_registration is not None:
        baseline_by_field["native_registration"] = native_registration
    baseline_by_field["adopted_at"] = baseline_by_field["adopted_at"].isoformat(timespec="microseconds") + "Z"
    baseline_by_field["baseline_sha256"] = receipt_digest(baseline_by_field)
    return validate_baseline(baseline_by_field, physical, source_authority.as_dict())
