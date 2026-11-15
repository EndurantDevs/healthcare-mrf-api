# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserved NUCC adoption with named snapshots and complete content validation."""

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

_LEGACY_FIELDS = FIELDS
FIELDS = frozenset(
    {
        "contract",
        "parent",
        "parent_index",
        "child",
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
    """Preserve old documents and validate saved names, content and logical provenance."""
    from process.nucc_protected_control import _digests, _source_generation

    baseline_by_field = partition.decode_catalog(baseline_by_field)
    _require(type(baseline_by_field) is dict, "fields differ")
    is_native = baseline_by_field.get("contract") == NATIVE_CONTRACT
    extra = {"native_registration"} if is_native else set()
    _require(set(baseline_by_field) in (FIELDS | extra, _LEGACY_FIELDS | extra), "fields differ")
    _require(baseline_by_field["contract"] in (CONTRACT, NATIVE_CONTRACT), "contract differs")
    child = (
        partition.qualified(baseline_by_field["child"])
        if "child" in baseline_by_field
        else (baseline_by_field["child_schema"], partition.CHILD_TABLE)
    )
    _require(child == physical["child"], "named predecessor differs")
    if "parent" in baseline_by_field:
        _require(
            partition.qualified(baseline_by_field["parent"]) == physical["parent"]
            and partition.qualified(baseline_by_field["parent_index"]) == physical["parent_index"],
            "named parent differs",
        )
        _require(
            baseline_by_field["baseline_sha256"]
            == receipt_digest({key: field for key, field in baseline_by_field.items() if key != "baseline_sha256"}),
            "digest differs",
        )
    if is_native:
        try:
            registration.validate_native_input_adoption(
                baseline_by_field["native_registration"], schema="mrf", table_name="nucc_taxonomy"
            )
        except RuntimeError as error:
            raise native.ReferenceFamilyArchiveError("NUCC protected baseline native registration differs") from error
    _require(
        _source_generation(baseline_by_field["source_authority"], predecessor=True, declared_schema=child[0])
        == _source_generation(source_authority, predecessor=True),
        "Source predecessor differs",
    )
    _require(
        type(baseline_by_field["row_count"]) is int and 0 <= baseline_by_field["row_count"] <= 10000, "census differs"
    )
    _require(type(baseline_by_field["source_revision_tracked"]) is bool, "tracking differs")
    _digests(baseline_by_field, ("semantic_sha256", "baseline_sha256"))
    return json.loads(json.dumps(baseline_by_field))


async def _original_admin(session):
    _require(
        isinstance(session, AsyncSession) and session.in_transaction() and not session.in_nested_transaction(),
        "original administrator transaction required",
    )
    return await _native_admin(session)


async def _native_admin(session):
    """Materialize the administrative transaction; native DDL supplies its authorization."""
    _require(session.in_transaction(), "administrator transaction required")
    native.model_family_roles()
    await session.execute(text("SELECT 1"))
    return {"transaction": session.get_transaction()}


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
 AND p.pronamespace='mrf'::regnamespace AS shape
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
            len(hook_records) == 1 and hook_records[0]["tgname"] == "cms_native_input_mutation",
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


async def _capture_native_registration(session, oid=None, owner_oid=None):
    candidate = registration.NativeInputCandidate("mrf", partition.CHILD_TABLE, "mrf", partition.CHILD_TABLE)
    return await registration.capture_native_input_adoption(
        await _registration_driver(session), ledger_schema="mrf", candidate=candidate
    )


async def _old_heap(session):
    oid = await session.scalar(text("SELECT to_regclass('mrf.nucc_taxonomy')::oid::bigint"))
    _require(type(oid) is int, "legacy heap missing")
    await session.execute(text(f"LOCK TABLE ONLY {partition.PARENT} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    child = partition.decode_catalog(
        await session.scalar(text(partition.CHILD_CATALOG_SQL), {"child": partition.PARENT})
    )
    _require(
        child is not None
        and child["kind"] == "r"
        and child["schema"] == "mrf"
        and child["name"] == partition.CHILD_TABLE
        and child["is_partition"] is False
        and child["bound"] is None
        and child["options"] is None
        and child["persistence"] == "p",
        "legacy heap differs",
    )
    partition._require_columns(child["columns"])
    partition._primary_index(child)
    partition._require_original_indexes(child)
    hook = await _legacy_hook(session, oid, None)
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
    native._require_transaction(session)
    owner, readers = native.model_family_roles()
    return {"name": native._quoted(owner), "role": owner, "readers": readers}


async def _preserve_reads(session, child, owner, incoming):
    schema = incoming.split(".", 1)[0]
    for role in owner["readers"]:
        await session.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {native._quoted(role)}"))
        await session.execute(text(f"GRANT SELECT ON {partition.PARENT},{incoming} TO {native._quoted(role)}"))


async def _adopt_heap(session, child, hook, owner, dataset_id):
    schema = native.reference_family_stage_schema(dataset_id)
    incoming = f'"{schema}"."{partition.CHILD_TABLE}"'
    await session.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION {owner["name"]}'))
    adoption = None if hook is None else hook.get("native_registration")
    await session.execute(text(f'ALTER TABLE {partition.PARENT} SET SCHEMA "{schema}"'))
    ownership = await native.capture_model_family_stage_ownership(
        session, native.reference_family_spec("nucc"), dataset_id
    )
    await native.seal_model_family_storage(session, ownership)
    if adoption is not None:
        await _retire_native_guard(session, child, owner, incoming, adoption)
    elif hook is not None:
        await session.execute(text(f'DROP TRIGGER "{hook["tgname"]}" ON {incoming}'))
    await session.execute(
        text(
            f"CREATE TABLE {partition.PARENT} (LIKE {incoming} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY INCLUDING STORAGE) PARTITION BY RANGE(code)"
        )
    )
    await session.execute(text(f"ALTER TABLE {partition.PARENT} OWNER TO {owner['name']}"))
    await session.execute(text(f"ALTER TABLE {partition.PARENT} ADD PRIMARY KEY(code)"))
    from process.provider_directory_generic_source_provision import _freeze_snapshot_relation

    driver = await _registration_driver(session)
    await _freeze_snapshot_relation(
        driver,
        "mrf",
        {"relname": partition.CHILD_TABLE, "relkind": "p"},
        owner["name"],
        tuple(native._quoted(reader) for reader in owner["readers"]),
    )
    await _preserve_reads(session, child, owner, incoming)
    await session.execute(
        text(f"ALTER TABLE {partition.PARENT} ATTACH PARTITION {incoming} FOR VALUES FROM (MINVALUE) TO (MAXVALUE)")
    )
    return ownership


async def _retire_native_guard(session, child, owner, incoming, native_registration):
    """Retire the existing semantic guard after closing every application writer."""
    driver = await _registration_driver(session)
    schema = incoming.split('"')[1]
    guards = await registration._native_relation_guards(driver, schema, partition.CHILD_TABLE, "mrf")
    _require(
        len(guards) == 1 and guards[0]["tgname"] == "cms_native_input_mutation" and guards[0]["safe"] is True,
        "native guard changed",
    )
    await session.execute(text(f'DROP TRIGGER "cms_native_input_mutation" ON {incoming}'))
    await registration.verify_retired_native_input_adoption(driver, evidence=native_registration)


async def _require_adoptable_source(session, child):
    from process.nucc_retention_read_view import NuccRetentionReadError, require_unretained_nucc_relations

    try:
        await require_unretained_nucc_relations(session, schema_name=child["schema"], relation_names=(child["name"],))
    except NuccRetentionReadError as error:
        raise native.ReferenceFamilyArchiveError("NUCC protected baseline legacy heap has retained claims") from error
    _require(
        not await session.scalar(
            text(
                "SELECT EXISTS(SELECT FROM hp_snapshot_retention.publication "
                "WHERE importer_id='nucc' AND dataset_key='nucc')"
            )
        ),
        "Owner event already exists",
    )


async def _relocate_source_generation(session, source_authority, physical):
    """Relocate recorded names under the existing lineage/counter CAS without changing provenance."""
    if source_authority.serving_generation is not None:
        _require(source_authority.relations == (("mrf", partition.CHILD_TABLE),), "Source predecessor name differs")
        changed = await session.scalar(
            text(
                "UPDATE mrf.reference_family_result_generation\n SET serving_schema=:schema,serving_relation_names=ARRAY['nucc_taxonomy']::text[]\n WHERE importer_id='nucc' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:generation\n AND serving_schema='mrf' AND serving_relation_names=ARRAY['nucc_taxonomy']::text[] RETURNING importer_id"
            ),
            {
                "schema": physical["child"][0],
                "lineage": source_authority.local_lineage_id,
                "generation": source_authority.local_generation,
            },
        )
        _require(changed == "nucc", "Source name relocation changed")
        source_authority = await native.read_reference_family_result_generation_authority(
            session, importer_id="nucc", schema_name="mrf", lock=True
        )
        _require(source_authority.relations == (physical["child"],), "Source child relocation differs")
    return source_authority


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
    await _require_adoptable_source(session, child)
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
        source_authority = await _relocate_source_generation(session, source_authority, physical)
        baseline_by_field = await _marker(
            session,
            admin,
            child,
            physical,
            source_authority,
            count,
            semantic,
            (None, None if hook is None else hook.get("native_registration")),
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
    catalog = partition.decode_catalog(await session.scalar(text(partition.CATALOG_SQL)))
    _require(partition.validate_preserved_parent_catalog(catalog, child) == physical, "preserved model/indexes changed")


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
    _old_token, adoption = old_native_preimage
    baseline_by_field = dict(
        contract=CONTRACT if adoption is None else NATIVE_CONTRACT,
        parent=list(physical["parent"]),
        parent_index=list(physical["parent_index"]),
        child=list(physical["child"]),
        semantic_sha256=semantic,
        row_count=count,
        source_authority=source_authority.as_dict(),
        source_revision_tracked=await session.scalar(
            text("SELECT source_revision_tracked FROM mrf.reference_family_result_generation WHERE importer_id='nucc'")
        ),
        adopted_at=(await session.scalar(text("SELECT clock_timestamp() AT TIME ZONE 'UTC'"))).isoformat(
            timespec="microseconds"
        )
        + "Z",
    )
    if adoption is not None:
        baseline_by_field["native_registration"] = adoption
    baseline_by_field["baseline_sha256"] = receipt_digest(baseline_by_field)
    return validate_baseline(baseline_by_field, physical, source_authority.as_dict())
