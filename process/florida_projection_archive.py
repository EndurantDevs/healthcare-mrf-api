# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture a published Florida projection with its retained source evidence."""

from __future__ import annotations

import importlib
import re
from collections.abc import Mapping
from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from db import models
from process import reference_family_archive as native
from process import source_profile_result_archive as profiles
from process.florida_mqa_profile import FL_MQA_SOURCE_KEY, PROFILE_SCHEMA_VERSION

IMPORTER_ID = "florida-mqa-profile"
MODELS = (*profiles.MODELS, models.ProviderProfileProjection)
TABLES = tuple(model.__tablename__ for model in MODELS)
PROJECTION = models.ProviderProfileProjection.__tablename__
_RUN = re.compile(r"(?:[0-9a-f]{32}|[0-9a-f]{64})\Z")


class FloridaProjectionArchiveError(RuntimeError):
    """The live generation, evidence graph or local stage differs."""


def require(condition, message):
    """Reject an archive assumption without mutating the transaction."""
    if not condition:
        raise FloridaProjectionArchiveError(message)


def _table(schema, name):
    return f"{native._quoted(native._schema_name(schema))}.{native._quoted(name)}"


def _role_ident(value):
    require(isinstance(value, str) and value, "Florida role is invalid")
    return '"' + value.replace('"', '""') + '"'


async def _publication_lock(session, schema):
    native._require_transaction(session)
    await session.execute(text("SET LOCAL lock_timeout='500ms'"))
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
        {"key": f"{schema}.{PROJECTION}.publication"},
    )


async def _run(session, schema, run_id):
    require(isinstance(run_id, str) and bool(_RUN.fullmatch(run_id)), "Florida run identity is invalid")
    return (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(schema, TABLES[0])} WHERE run_id=:run"),
                {"run": run_id},
            )
        )
        .mappings()
        .one_or_none()
    )


def _validate_run(run):
    require(
        isinstance(run, Mapping)
        and run.get("source_key") == FL_MQA_SOURCE_KEY
        and run.get("jurisdiction") == "FL"
        and run.get("schema_version") == PROFILE_SCHEMA_VERSION
        and run.get("status") == "completed"
        and isinstance(run.get("started_at"), datetime)
        and isinstance(run.get("finished_at"), datetime)
        and run.get("error") is None,
        "Florida completed run differs",
    )
    source, metrics = run.get("source_manifest"), run.get("metrics")
    publication = metrics.get("publication") if isinstance(metrics, Mapping) else None
    require(
        isinstance(source, Mapping)
        and isinstance(source.get("sources"), list)
        and bool(source["sources"])
        and isinstance(metrics, Mapping)
        and isinstance(publication, Mapping)
        and publication.get("publication") == "atomic_table_swap"
        and type(publication.get("published_rows")) is int
        and publication["published_rows"] > 0
        and metrics.get("published_providers") == publication["published_rows"],
        "Florida run was not published",
    )


async def _projection_identity(session, schema):
    projection = _table(schema, PROJECTION)
    generation = (
        (
            await session.execute(
                text(f"SELECT generation_id, count(*)::bigint AS rows FROM {projection} GROUP BY generation_id LIMIT 2")
            )
        )
        .mappings()
        .all()
    )
    require(len(generation) == 1 and generation[0]["rows"] > 0, "Florida live generation is invalid")
    run_id = generation[0]["generation_id"]
    require(isinstance(run_id, str) and bool(_RUN.fullmatch(run_id)), "Florida live run is invalid")
    return run_id, int(generation[0]["rows"])


async def _verify_evidence_lineage(session, schema, run_id):
    artifact = _table(schema, "provider_profile_artifact")
    source_records_table = _table(schema, "provider_profile_source_record")
    fact = _table(schema, "provider_profile_fact")
    require(
        await session.scalar(
            text(
                f"SELECT NOT EXISTS(SELECT 1 FROM {source_records_table} r LEFT JOIN {artifact} a "
                "ON a.artifact_id=r.artifact_id WHERE r.run_id=:run "
                "AND (a.artifact_id IS NULL OR a.run_id<>:run OR a.source_key<>r.source_key))"
            ),
            {"run": run_id},
        )
        is True,
        "Florida source-record lineage differs",
    )
    require(
        await session.scalar(
            text(
                f"SELECT NOT EXISTS(SELECT 1 FROM {fact} f LEFT JOIN {source_records_table} r "
                "ON r.record_id=f.source_record_id WHERE f.run_id=:run "
                "AND (r.record_id IS NULL OR r.run_id<>:run))"
            ),
            {"run": run_id},
        )
        is True,
        "Florida fact lineage differs",
    )


def validate_native_run(run):
    """Carry only a complete ordinary publication and its existing source policy."""
    florida = importlib.import_module("process.florida_mqa_profile")

    _validate_run(run)
    manifest, metrics = run["source_manifest"], run["metrics"]
    source_keys = manifest["sources"]
    require(
        manifest.get("partial_publish_reasons") == []
        and not florida._partial_publish_reasons(source_keys, None)
        and len(source_keys) == len(set(source_keys))
        and set(source_keys) <= set(florida.FLORIDA_SOURCES)
        and metrics.get("selected_sources") == source_keys
        and isinstance(metrics.get("source_metrics"), Mapping)
        and set(metrics["source_metrics"]) == set(source_keys),
        "Florida source completeness differs",
    )
    require(
        not florida._source_validation_guard_reasons(metrics["source_metrics"], expected_source_keys=source_keys),
        "Florida source validation differs",
    )
    guard = manifest.get("publication_guard")
    require(
        isinstance(guard, Mapping)
        and type(guard.get("min_first_publish_providers")) is int
        and guard["min_first_publish_providers"] > 0
        and type(guard.get("min_publish_ratio")) in (int, float)
        and 0 < guard["min_publish_ratio"] <= 1
        and type(manifest.get("allow_volume_drop")) is bool
        and run["started_at"] <= run["finished_at"],
        "Florida publication policy differs",
    )


async def validate_native_result(session, schema, run_id):
    """Check the closed typed evidence/projection set without rematching or row hashes."""
    run = await _run(session, schema, run_id)
    validate_native_run(run)
    generation_id, count = await _projection_identity(session, schema)
    require(
        generation_id == run_id and count == run["metrics"]["published_providers"],
        "Florida projection receipt differs",
    )
    await _verify_evidence_lineage(session, schema, run_id)
    artifact_table = _table(schema, TABLES[1])
    source_records_table = _table(schema, TABLES[2])
    facts = _table(schema, TABLES[3])
    projection = _table(schema, PROJECTION)
    require(
        await session.scalar(
            text(
                f"SELECT NOT EXISTS(SELECT 1 FROM {artifact_table} WHERE run_id=:run AND "
                "(NOT(source_key=ANY(CAST(:sources AS text[]))) OR content_bytes<=0 OR content_sha256 !~ '^[0-9a-f]{64}$')) "
                f"AND (SELECT count(DISTINCT source_key) FROM {artifact_table} WHERE run_id=:run)=:source_count "
                f"AND EXISTS(SELECT 1 FROM {source_records_table} WHERE run_id=:run) "
                f"AND EXISTS(SELECT 1 FROM {facts} WHERE run_id=:run) "
                f"AND NOT EXISTS(SELECT 1 FROM {facts} f JOIN {source_records_table} r ON r.record_id=f.source_record_id "
                "WHERE f.run_id=:run AND f.npi IS DISTINCT FROM r.matched_npi) "
                f"AND NOT EXISTS(SELECT 1 FROM {projection} p WHERE p.generation_id<>:run "
                "OR p.schema_version<>:version OR p.source_keys::jsonb<>CAST(:keys AS jsonb) "
                f"OR NOT EXISTS(SELECT 1 FROM {facts} f WHERE f.run_id=:run AND f.npi=p.npi)) "
                f"AND NOT EXISTS(SELECT 1 FROM {facts} f WHERE f.run_id=:run AND f.npi IS NOT NULL "
                f"AND NOT EXISTS(SELECT 1 FROM {projection} p WHERE p.npi=f.npi))"
            ),
            {
                "run": run_id,
                "sources": run["source_manifest"]["sources"],
                "source_count": len(run["source_manifest"]["sources"]),
                "version": PROFILE_SCHEMA_VERSION,
                "keys": '["florida-mqa"]',
            },
        )
        is True,
        "Florida native evidence closure differs",
    )


async def require_native_publication_order(session, prepared, schema, expected):
    """Apply ordinary source/header/volume/newer fences to the received completed run."""
    candidate = await _run(session, prepared.ownership.schema_name, prepared.manifest["run_id"])
    current = await _run(session, schema, expected["current_run_id"]) if expected["current_run_id"] else None
    return native_publication_policy(candidate, current)


def native_publication_policy(candidate, current):
    """Keep ordinary guard decisions and their audit values identical at native publication."""
    florida = importlib.import_module("process.florida_mqa_profile")

    validate_native_run(candidate)
    metrics, manifest = candidate["metrics"], candidate["source_manifest"]
    previous = current["metrics"] if current else {}
    require(
        current is None
        or not florida._is_generation_newer(
            current["started_at"], current["run_id"], candidate["started_at"], candidate["run_id"]
        ),
        "Florida newer generation is already published",
    )
    source_metrics, previous_sources = metrics["source_metrics"], previous.get("source_metrics", {})
    require(
        not florida._source_header_drift_guard_reasons(source_metrics, previous_sources),
        "Florida source header changed",
    )
    policy = manifest["publication_guard"]
    ratios = florida._source_ratio_guard_reasons(
        source_metrics, previous_sources, min_publish_ratio=policy["min_publish_ratio"]
    )
    volume = florida._publication_guard_reasons(
        candidate_provider_count=metrics["published_providers"],
        candidate_source_record_count=metrics.get("physical_source_records") or metrics.get("source_records") or 0,
        current_provider_count=previous.get("published_providers", 0),
        previous_source_record_count=previous.get("physical_source_records", previous.get("source_records")),
        min_first_publish_providers=policy["min_first_publish_providers"],
        min_publish_ratio=policy["min_publish_ratio"],
    )
    require(manifest["allow_volume_drop"] or not (ratios or volume), "Florida publication volume changed")
    return {
        "source_guard": {
            "allow_volume_drop": manifest["allow_volume_drop"],
            "min_publish_ratio": policy["min_publish_ratio"],
            "validation_reasons": [],
            "header_reasons": [],
            "ratio_reasons": ratios,
        },
        "volume_guard": {
            "allow_volume_drop": manifest["allow_volume_drop"],
            "candidate_providers": metrics["published_providers"],
            "current_providers": previous.get("published_providers", 0),
            "source_record_counter_semantics": "physical_input",
            "candidate_source_records": metrics.get("physical_source_records") or metrics.get("source_records") or 0,
            "previous_source_records": previous.get("physical_source_records", previous.get("source_records")),
            "min_first_publish_providers": policy["min_first_publish_providers"],
            "min_publish_ratio": policy["min_publish_ratio"],
            "reasons": volume,
        },
    }


async def _copy_named_projection_candidate(session, prepared, schema, cutover_id, source_copy, deadline):
    """Use the existing bounded COPY, model indexes and complete typed parity."""
    name = _cutover_name(cutover_id)
    require(await native._relation_oid(session, schema, name) is None, "Florida cutover candidate exists")
    await session.execute(
        text(
            f"CREATE TABLE {_table(schema, name)} (LIKE {_table(prepared.ownership.schema_name, PROJECTION)} INCLUDING CONSTRAINTS EXCLUDING DEFAULTS)"
        )
    )
    await native._copy_model_run_scope(
        session,
        native.ReferenceFamilySpec(IMPORTER_ID, (models.ProviderProfileProjection,)),
        source_schema=prepared.ownership.schema_name,
        target_schema=schema,
        target_names=(name,),
        run_scope=(("generation_id",), (prepared.manifest["run_id"],)),
        source_copy=source_copy,
        deadline=deadline,
    )
    from types import SimpleNamespace

    table = models.ProviderProfileProjection.__table__.to_metadata(native.MetaData(), name=name)
    await native._create_model_indexes(
        session,
        native.ReferenceFamilySpec(IMPORTER_ID, (SimpleNamespace(__tablename__=name, __table__=table),)),
        schema,
        create_constraints=True,
    )
    require(
        await native._is_model_table_equal(
            session,
            models.ProviderProfileProjection,
            left_schema=prepared.ownership.schema_name,
            left_name=PROJECTION,
            right_schema=schema,
            right_name=name,
        ),
        "Florida native projection content differs",
    )
    return name


def _cutover_name(cutover_id):
    require(isinstance(cutover_id, UUID), "Florida cutover identity is invalid")
    return f"{PROJECTION}_s_{cutover_id.hex[:16]}"


async def _projection_candidate_seal(session, schema, name, owner_oid):
    relation_oid = await native._relation_oid(session, schema, name)
    require(type(relation_oid) is int and relation_oid > 0, "Florida cutover candidate is missing")
    candidate = (
        (
            await session.execute(
                text("SELECT relkind,relpersistence FROM pg_class WHERE oid=:oid"), {"oid": relation_oid}
            )
        )
        .mappings()
        .one()
    )
    require(
        candidate["relkind"] in ("r", b"r") and candidate["relpersistence"] in ("p", b"p"),
        "Florida cutover candidate shape differs",
    )
    return {"relation_oid": relation_oid, "owner_oid": owner_oid, "table_name": name}


async def isolate_ordinary_projection(session, schema, name):
    """Close default grants in the same transaction that creates an ordinary candidate."""
    owner_oid = await session.scalar(
        text("SELECT relowner FROM pg_class WHERE oid=to_regclass(:table)"), {"table": _table(schema, name)}
    )
    require(type(owner_oid) is int and owner_oid > 0, "Florida ordinary candidate owner differs")
    await _clear_stage_grants(session, schema, name, owner_oid)
    return await _projection_candidate_seal(session, schema, name, owner_oid)


async def preserve_ordinary_projection_access(session, schema, seal):
    """Keep the candidate private until the locked incumbent's access is replayed."""
    await native._lock_family(session, schema, (seal["table_name"],), "SHARE", nowait=True)
    await native._lock_family(session, schema, (PROJECTION,), "ACCESS EXCLUSIVE")
    live_oid = await native._relation_oid(session, schema, PROJECTION)
    await _restore_projection_access(session, schema, seal["table_name"], live_oid)


async def _restore_projection_access(session, schema, temporary_name, live_oid):
    """Preserve the incumbent owner and supported explicit ACLs across every projection writer."""
    temporary = _table(schema, temporary_name)
    security = await _live_projection_security(session, live_oid)
    owner = await session.scalar(text("SELECT rolname FROM pg_roles WHERE oid=:oid"), {"oid": security["relowner"]})
    require(isinstance(owner, str), "Florida live projection owner changed")
    await session.execute(text(f"ALTER TABLE {temporary} OWNER TO {_role_ident(owner)}"))
    await _clear_stage_grants(session, schema, temporary_name, security["relowner"])
    await _replay_live_grants(session, temporary, live_oid, security["relowner"])


async def _live_projection_security(session, live_oid):
    """Reject live security features that the metadata-only cutover cannot copy."""
    security = (
        (
            await session.execute(
                text(
                    "SELECT c.relowner,c.relrowsecurity,c.relforcerowsecurity,"
                    "(SELECT count(*) FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal) AS triggers,"
                    "(SELECT count(*) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attacl IS NOT NULL) AS column_grants "
                    "FROM pg_class c WHERE c.oid=:oid"
                ),
                {"oid": live_oid},
            )
        )
        .mappings()
        .one()
    )
    require(
        not security["relrowsecurity"]
        and not security["relforcerowsecurity"]
        and security["triggers"] == 0
        and security["column_grants"] == 0,
        "Florida live projection security cannot be copied",
    )
    return security


async def _clear_stage_grants(session, schema, temporary_name, owner_oid):
    """Remove non-owner grants inherited from destination default privileges."""
    temporary = _table(schema, temporary_name)
    # Default ACLs may grant privileges absent from the incumbent.
    default_grantees = (
        (
            await session.execute(
                text(
                    "SELECT DISTINCT a.grantee,r.rolname FROM pg_class c "
                    "CROSS JOIN LATERAL aclexplode(c.relacl) a "
                    "LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=:oid"
                ),
                {"oid": await native._relation_oid(session, schema, temporary_name)},
            )
        )
        .mappings()
        .all()
    )
    await session.execute(text(f"REVOKE ALL ON TABLE {temporary} FROM PUBLIC"))
    for grantee in default_grantees:
        if grantee["grantee"] not in (0, owner_oid):
            require(isinstance(grantee["rolname"], str), "Florida cutover grant changed")
            await session.execute(text(f"REVOKE ALL ON TABLE {temporary} FROM {_role_ident(grantee['rolname'])}"))


async def _replay_live_grants(session, temporary, live_oid, owner_oid):
    """Copy the incumbent's exact supported table grants to the cutover stage."""
    grants = (
        (
            await session.execute(
                text(
                    "SELECT a.grantee,a.privilege_type,a.is_grantable,r.rolname "
                    "FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a "
                    "LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=:oid "
                    "ORDER BY a.grantee,a.privilege_type"
                ),
                {"oid": live_oid},
            )
        )
        .mappings()
        .all()
    )
    allowed_privileges = {"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
    for grant in grants:
        if grant["grantee"] == owner_oid:
            continue
        require(
            grant["privilege_type"] in allowed_privileges
            and (grant["grantee"] == 0 or isinstance(grant["rolname"], str)),
            "Florida live projection grant changed",
        )
        grantee = "PUBLIC" if grant["grantee"] == 0 else _role_ident(grant["rolname"])
        await session.execute(
            text(
                f"GRANT {grant['privilege_type']} ON TABLE {temporary} TO {grantee}"
                + (" WITH GRANT OPTION" if grant["is_grantable"] else "")
            )
        )
