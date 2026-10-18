# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete allowed snapshot lineage using actual plan/item file-version joins."""

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256
from process.registry_ptg_allowed_plan_contract import SELECTION_MODE, validated_identity
from process.registry_ptg_producer_scope import _digest
from process.registry_ptg_published_plan_contract import _files, _require, _text


def _manifest_files(manifest):
    versions = manifest.get("source_file_versions")
    _require(type(versions) is list and 1 <= len(versions) <= 128)
    return _files(
        [
            {
                "source_file_version_id": version["engine_source_file_version_id"],
                "source_identity_sha256": version["engine_source_identity_hash"],
                "raw_sha256": version["raw_sha256"],
            }
            for version in versions
        ]
    )


def _allowed_snapshot_header(snapshot_by_field):
    """Require the original published allowed manifest and run ownership."""
    manifest = snapshot_by_field["manifest"]
    _require(type(manifest) is dict)
    source_key = snapshot_by_field["run_source_key"]
    _text(source_key, 256, characters=True)
    index = manifest.get("allowed_amount_index")
    _require(snapshot_by_field["status"] == "published" and type(index) is dict)
    _require(manifest.get("activation_status") == "not_applicable")
    _require(manifest.get("data_domains") == ["allowed_amounts"] and "serving_index" not in manifest)
    _require(
        all(
            index.get(name) == expected
            for name, expected in {
                "contract": "ptg2_allowed_amounts_v1",
                "arch_version": "postgres_binary_v3",
                "storage": "postgresql",
                "data_domain": "allowed_amounts",
                "source_key": source_key,
            }.items()
        )
    )
    _require(all(index.get(name) is True for name in ("snapshot_scoped", "allowed_amount_evidence")))
    _require(
        all(
            type(index.get(name)) is int and index[name] > 0
            for name in ("allowed_amount_payments", "allowed_amount_provider_payments")
        )
    )
    _require(snapshot_by_field["import_month"] == snapshot_by_field["run_import_month"])
    _require(manifest.get("snapshot_id") == snapshot_by_field["snapshot_id"])
    _require(
        manifest.get("source_key") == source_key and manifest.get("import_month") == snapshot_by_field["import_month"]
    )
    return manifest, source_key


def validated_allowed_inventory(snapshot_by_field, plans, file_rows):
    """Bind every immutable file and real plan, refusing missing or conflicting lineage."""
    manifest, source_key = _allowed_snapshot_header(snapshot_by_field)
    _require(type(plans) is list and 1 <= len(plans) <= 128)
    for plan in plans:
        _require(type(plan) is dict and set(plan) == {"plan_id", "plan_market_type"})
        _text(plan["plan_id"], 64, characters=True)
        if type(plan["plan_market_type"]) is not str or plan["plan_market_type"] != "":
            _text(plan["plan_market_type"], 32, characters=True)
        _require(plan["plan_market_type"] == plan["plan_market_type"].lower())
    _require(len({_digest(plan) for plan in plans}) == len(plans))
    plans = sorted(plans, key=lambda plan: (plan["plan_id"], plan["plan_market_type"]))
    _require(type(file_rows) is list and 1 <= len(file_rows) <= 128)
    for version in file_rows:
        _require(
            type(version["file_id"]) is int
            and 0 <= version["file_id"] < 2**63
            and version["source_identity_sha256"] == version["identity_hash"]
        )
    _require(len({version["file_id"] for version in file_rows}) == len(file_rows))
    file_versions = _files(
        [
            {name: version[name] for name in ("source_file_version_id", "source_identity_sha256", "raw_sha256")}
            for version in file_rows
        ]
    )
    _require(file_versions == _manifest_files(manifest))
    identity = validated_identity(
        {
            "snapshot_id": snapshot_by_field["snapshot_id"],
            "import_run_id": snapshot_by_field["import_run_id"],
            "import_month": snapshot_by_field["import_month"],
            "source_key": source_key,
            "snapshot_manifest_sha256": result_archive_manifest_sha256(manifest),
            "plan_scopes_sha256": _digest(plans),
            "file_versions_sha256": _digest(file_versions),
            "source_count": len(file_versions),
        }
    )
    return {
        "identity": identity,
        "plan_scopes": plans,
        "file_versions": file_versions,
        "source_keys": sorted(version["file_id"] for version in file_rows),
        "selection_mode": SELECTION_MODE,
    }


async def load_allowed_plan_inventory(session, schema_name, snapshot_id):
    """Load bounded complete plan and file lineage under immutable header locks."""
    schema = _identifier(schema_name)
    snapshot_rows = (
        (
            await session.execute(
                text(f"""
        SELECT snapshot.snapshot_id,snapshot.import_run_id,snapshot.import_month::text,snapshot.status,snapshot.manifest,
          internal_run.import_month::text AS run_import_month,internal_run.options->>'source_key' AS run_source_key
        FROM {schema}.ptg2_snapshot snapshot JOIN {schema}.ptg2_import_run internal_run USING(import_run_id)
        WHERE snapshot.snapshot_id=:snapshot_id FOR KEY SHARE OF snapshot,internal_run
    """),
                {"snapshot_id": snapshot_id},
            )
        )
        .mappings()
        .all()
    )
    _require(len(snapshot_rows) == 1)
    plans = (
        (
            await session.execute(
                text(f"""
        SELECT DISTINCT plan_id,coalesce(lower(plan_market_type),'') AS plan_market_type
        FROM {schema}.ptg2_allowed_amount_plan WHERE snapshot_id=:snapshot_id
        ORDER BY plan_id,plan_market_type LIMIT 129
    """),
                {"snapshot_id": snapshot_id},
            )
        )
        .mappings()
        .all()
    )
    file_rows = (
        (
            await session.execute(
                text(f"""
        WITH lineage AS (
          SELECT file_id,source_file_version_id FROM {schema}.ptg2_allowed_amount_plan WHERE snapshot_id=:snapshot_id
          UNION SELECT file_id,source_file_version_id FROM {schema}.ptg2_allowed_amount_item WHERE snapshot_id=:snapshot_id
        ) SELECT lineage.file_id,lineage.source_file_version_id,version.source_identity_hash AS source_identity_sha256,
          version.raw_sha256,identity.source_identity_hash AS identity_hash
        FROM lineage LEFT JOIN {schema}.ptg2_source_file_version version USING(source_file_version_id)
        LEFT JOIN {schema}.ptg2_source_identity identity USING(source_identity_hash)
        ORDER BY lineage.file_id,lineage.source_file_version_id LIMIT 129
    """),
                {"snapshot_id": snapshot_id},
            )
        )
        .mappings()
        .all()
    )
    return validated_allowed_inventory(
        dict(snapshot_rows[0]), [dict(plan) for plan in plans], [dict(version) for version in file_rows]
    )


async def allowed_plan_inventory(session, schema_name, ownership, *, operation_id):
    """Require real import ownership and complete reviewed source-file membership."""
    from process.ptg_parts.result_archive_allowed_authority import prepare_allowed_source_authority

    inventory = await prepare_allowed_source_authority(session, schema_name, ownership["snapshot_id"], operation_id)
    identity = inventory["identity"]
    _require(
        all(
            identity[name] == ownership[other]
            for name, other in (
                ("snapshot_id", "snapshot_id"),
                ("source_key", "source_key"),
                ("import_run_id", "engine_run_id"),
                ("import_month", "import_month"),
            )
        )
    )
    _require(
        sum(
            version["source_file_version_id"] == ownership["engine_source_file_version_id"]
            and version["source_identity_sha256"] == ownership["engine_source_identity_hash"]
            for version in inventory["file_versions"]
        )
        == 1
    )
    return inventory


async def is_allowed_snapshot(session, schema_name, snapshot_id):
    """Discriminate the real published role before invoking negotiated-only grammar."""
    schema = _identifier(schema_name)
    return (
        await session.execute(
            text(f"""
        WITH selected_snapshot AS MATERIALIZED (
          SELECT status,manifest::jsonb AS manifest
          FROM {schema}.ptg2_snapshot WHERE snapshot_id=:snapshot_id
        )
        SELECT status='published' AND manifest->>'activation_status'='not_applicable'
          AND manifest->'data_domains'='["allowed_amounts"]'::jsonb
          AND jsonb_typeof(manifest->'allowed_amount_index')='object' AND NOT manifest ? 'serving_index'
        FROM selected_snapshot
    """),
            {"snapshot_id": snapshot_id},
        )
    ).scalar_one_or_none() is True
