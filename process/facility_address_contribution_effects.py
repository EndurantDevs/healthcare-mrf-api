# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected destination-only before-images for facility archive publication."""

from uuid import UUID

from sqlalchemy import text

from process.entity_address_snapshot_alias import capture_entity_address_alias_semantic_receipt
from process.ext.address_alias_sql import alias_advisory_xact_lock_sql
from process.ext.address_canon import _archive_lock_key, _qtable, archive_table_name
from process.facility_address_contribution_merge import (
    KEYS,
    SCRATCH,
    project_observations,
    require,
    validate_observations,
)

LEDGER = "hp_snapshot_retention.facility_address_effect"
CONTRACT = "facility_address_effect.v1"


async def _lock_archive(session, schema, mode):
    """Follow resolver writer ordering and prohibit table replacement through CAS."""
    require(session.in_transaction(), "requires a transaction")
    await session.execute(text("SET LOCAL TimeZone TO 'UTC'"))
    table = archive_table_name()
    require(table == "address_archive_v2", "archive is unsupported")
    for setting, ceiling in (
        ("lock_timeout", "500ms"),
        ("statement_timeout", "1800s" if mode == "SHARE" else "2500ms"),
    ):
        await session.execute(
            text(
                "SELECT set_config(:setting,CASE WHEN current_setting(:setting)::interval=interval '0' "
                "OR current_setting(:setting)::interval>CAST(CAST(:ceiling AS text) AS interval) "
                "THEN CAST(:ceiling AS text) ELSE current_setting(:setting) END,true)"
            ),
            {"setting": setting, "ceiling": ceiling},
        )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
        {
            "key": _archive_lock_key(schema, table, "resolve"),
        },
    )
    await session.execute(text(alias_advisory_xact_lock_sql()))
    archive = _qtable(schema, table)
    await session.execute(text(f"LOCK TABLE ONLY {archive} IN {mode} MODE"))
    relation_record = (
        (
            await session.execute(
                text("""
        SELECT c.oid,c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,
          EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal) hooks,
          EXISTS(SELECT 1 FROM pg_rewrite r WHERE r.ev_class=c.oid) rules,
          EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) inheritance,
          EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0
                 AND NOT a.attisdropped AND (a.attgenerated<>'' OR a.attidentity<>'')) generated
        FROM pg_class c WHERE c.oid=to_regclass(:relation)
    """),
                {"relation": archive},
            )
        )
        .mappings()
        .one()
    )
    require(
        relation_record["relkind"] in ("r", b"r")
        and relation_record["relpersistence"] in ("p", b"p")
        and not any(
            relation_record[key]
            for key in ("relrowsecurity", "relforcerowsecurity", "hooks", "rules", "inheritance", "generated")
        ),
        "archive security hooks are unsupported",
    )
    return archive, relation_record["oid"]


async def prepare_facility_address_effects(session, *, operation_id, stage_schema, schema="mrf"):
    """Plan bounded changes under native locks; never mutate the live archive."""
    operation_id = str(UUID(str(operation_id)))
    archive, archive_oid = await _lock_archive(session, schema, "SHARE")
    contribution = _qtable(stage_schema, "facility_address_contribution")
    await session.execute(text(f"LOCK TABLE ONLY {contribution} IN SHARE MODE"))
    stage_schema_oid, contribution_oid = (
        await session.execute(
            text("SELECT relnamespace,oid FROM pg_class WHERE oid=to_regclass(:relation)"),
            {"relation": contribution},
        )
    ).one()
    metadata, alias = await validate_observations(session, stage_schema=stage_schema, schema=schema)
    existing = await session.scalar(
        text(f"SELECT EXISTS(SELECT 1 FROM {LEDGER} WHERE operation_id=:operation)"), {"operation": UUID(operation_id)}
    )
    require(not existing, "plan already exists")
    count = 0
    if metadata["enabled"]:
        await project_observations(session, stage_schema=stage_schema, schema=schema)
        await session.execute(
            text(f"""
            INSERT INTO {LEDGER}(operation_id,address_key,before_image,after_image,before_xmin)
            SELECT :operation,k.address_key,to_jsonb(a),to_jsonb(p),a.xmin::text
            FROM {KEYS} k LEFT JOIN {archive} a USING(address_key) LEFT JOIN {SCRATCH} p USING(address_key)
        """),
            {"operation": UUID(operation_id)},
        )
        count = await session.scalar(
            text(f"SELECT count(*) FROM {LEDGER} WHERE operation_id=:operation"), {"operation": UUID(operation_id)}
        )
    return {
        "contract": CONTRACT,
        "operation_id": operation_id,
        "archive_oid": archive_oid,
        "schema": schema,
        "stage_schema": stage_schema,
        "stage_schema_oid": stage_schema_oid,
        "contribution_oid": contribution_oid,
        "alias": alias,
        "row_count": count,
    }


def validate_effect_receipt(receipt):
    """Validate fixed local scope and resource limits before any catalog lookup."""
    require(
        isinstance(receipt, dict)
        and set(receipt)
        == {
            "contract",
            "operation_id",
            "archive_oid",
            "schema",
            "stage_schema",
            "stage_schema_oid",
            "contribution_oid",
            "alias",
            "row_count",
        },
        "effect receipt is invalid",
    )
    require(
        receipt["contract"] == CONTRACT
        and receipt["schema"] == "mrf"
        and isinstance(receipt["stage_schema"], str)
        and 0 < len(receipt["stage_schema"]) <= 63
        and all(
            type(receipt[key]) is int and 0 < receipt[key] < 2**32
            for key in ("archive_oid", "stage_schema_oid", "contribution_oid")
        )
        and type(receipt["row_count"]) is int
        and 0 <= receipt["row_count"] <= 4_000_000,
        "effect receipt is invalid",
    )
    UUID(receipt["operation_id"])
    return receipt


async def _fenced_effects(session, receipt, *, rollback):
    receipt = validate_effect_receipt(receipt)
    archive, oid = await _lock_archive(session, receipt["schema"], "SHARE ROW EXCLUSIVE")
    require(oid == receipt["archive_oid"], "archive identity changed")
    if receipt["alias"] is not None:
        alias = await capture_entity_address_alias_semantic_receipt(session, schema_name=receipt["schema"])
        require(alias.as_dict() == receipt["alias"], "alias identity changed")
    parameter_by_name = {"operation": UUID(receipt["operation_id"])}
    count = await session.scalar(
        text(f"SELECT count(*) FROM {LEDGER} WHERE operation_id=:operation"), parameter_by_name
    )
    require(count == receipt["row_count"], "plan inventory changed")
    image, xmin = ("after_image", "after_xmin") if rollback else ("before_image", "before_xmin")
    invalid = await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {LEDGER} e LEFT JOIN {archive} a USING(address_key)
          WHERE e.operation_id=:operation AND (
            e.{image} IS DISTINCT FROM to_jsonb(a) OR e.{xmin} IS DISTINCT FROM a.xmin::text
            OR e.applied IS DISTINCT FROM :applied OR e.reverted))
    """),
        {**parameter_by_name, "applied": rollback},
    )
    require(not invalid, "destination changed")
    return archive, parameter_by_name


async def apply_facility_address_effects(session, receipt):
    """Apply prepared row CAS checks in the caller's family/install transaction."""
    archive, parameter_by_name = await _fenced_effects(session, receipt, rollback=False)
    await _write_images(session, archive, parameter_by_name, rollback=False)
    await session.execute(
        text(f"""
        UPDATE {LEDGER} e SET applied=true,after_xmin=a.xmin::text
        FROM {archive} a WHERE e.operation_id=:operation AND a.address_key=e.address_key
    """),
        parameter_by_name,
    )
    await session.execute(
        text(f"UPDATE {LEDGER} SET applied=true WHERE operation_id=:operation AND after_image IS NULL"),
        parameter_by_name,
    )


async def rollback_facility_address_effects(session, receipt):
    """Restore exact before-images only while every recorded after-image survives."""
    archive, parameter_by_name = await _fenced_effects(session, receipt, rollback=True)
    await _write_images(session, archive, parameter_by_name, rollback=True)
    await session.execute(text(f"UPDATE {LEDGER} SET reverted=true WHERE operation_id=:operation"), parameter_by_name)


async def _write_images(session, archive, parameter_by_name, *, rollback):
    image = "before_image" if rollback else "after_image"
    columns = (
        (
            await session.execute(
                text("""
        SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(:relation)
          AND attnum>0 AND NOT attisdropped ORDER BY attnum
    """),
                {"relation": archive},
            )
        )
        .scalars()
        .all()
    )
    quoted = ",".join('"' + column.replace('"', '""') + '"' for column in columns)
    assignments = ",".join(
        '"' + column.replace('"', '""') + '"=EXCLUDED."' + column.replace('"', '""') + '"'
        for column in columns
        if column != "address_key"
    )
    await session.execute(
        text(f"""
        INSERT INTO {archive}({quoted}) SELECT p.* FROM {LEDGER} e
        CROSS JOIN LATERAL jsonb_populate_record(NULL::{archive},e.{image}) p
        WHERE e.operation_id=:operation AND e.{image} IS NOT NULL
          AND e.before_image IS DISTINCT FROM e.after_image
        ON CONFLICT(address_key) DO UPDATE SET {assignments}
    """),
        parameter_by_name,
    )
    if rollback:
        await session.execute(
            text(f"""
            DELETE FROM {archive} a USING {LEDGER} e WHERE e.operation_id=:operation
              AND e.address_key=a.address_key AND e.before_image IS NULL AND e.after_image IS NOT NULL
        """),
            parameter_by_name,
        )
