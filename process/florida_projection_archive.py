# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture a published Florida projection with its retained source evidence."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.schema import CreateIndex

from db import models
from process import reference_family_archive as native
from process import source_profile_result_archive as profiles
from process import source_profile_result_pins as pins
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.florida_mqa_profile import FL_MQA_SOURCE_KEY, PROFILE_SCHEMA_VERSION

IMPORTER_ID = "florida-mqa-profile"
CONTRACT = "florida-profile-projection.postgres.v1"
MODELS = (*profiles.MODELS, models.ProviderProfileProjection)
TABLES = tuple(model.__tablename__ for model in MODELS)
PROJECTION = models.ProviderProfileProjection.__tablename__
_RUN = re.compile(r"(?:[0-9a-f]{32}|[0-9a-f]{64})\Z")
_HEX = re.compile(r"[0-9a-f]{64}\Z")


class FloridaProjectionArchiveError(RuntimeError):
    """The live generation, evidence graph or local stage differs."""


def require(condition, message):
    """Reject an archive assumption without mutating the transaction."""
    if not condition:
        raise FloridaProjectionArchiveError(message)


def _digest(value):
    return hashlib.sha256(native._canonical_json(value)).hexdigest()


def _table(schema, name):
    return f"{native._quoted(native._schema_name(schema))}.{native._quoted(name)}"


def _role_ident(value):
    require(isinstance(value, str) and value, "Florida role is invalid")
    return '"' + value.replace('"', '""') + '"'


def stage_schema(dataset_id):
    """Derive the isolated stage schema from a validated dataset identity."""
    require(isinstance(dataset_id, UUID), "Florida stage identity is invalid")
    return "florida_profile_result_" + dataset_id.hex


@dataclass(frozen=True)
class StageOwnership:
    dataset_id: UUID
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]

    importer_id = IMPORTER_ID
    sequence_oids = ()
    auxiliary_oid = None

    @property
    def schema_name(self):
        """Return the deterministic stage schema for this ownership receipt."""
        return stage_schema(self.dataset_id)


@dataclass(frozen=True)
class PreparedResult:
    manifest: dict
    ownership: StageOwnership


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


async def describe_result(session, schema, run_id):
    """Bind the five-table published generation and source manifest."""
    run = await _run(session, schema, run_id)
    _validate_run(run)
    generation_id, projected = await _projection_identity(session, schema)
    require(generation_id == run_id, "Florida projection generation differs")
    require(run["metrics"]["published_providers"] == projected, "Florida projection count differs")
    require(
        await session.scalar(
            text(
                f"SELECT NOT EXISTS(SELECT 1 FROM {_table(schema, PROJECTION)} "
                "WHERE schema_version<>:schema_version OR source_keys::jsonb<>CAST(:source_keys AS jsonb) "
                "OR published_at IS NULL OR profile_json IS NULL)"
            ),
            {"schema_version": PROFILE_SCHEMA_VERSION, "source_keys": '["florida-mqa"]'},
        )
        is True,
        "Florida projection scope differs",
    )
    await _verify_evidence_lineage(session, schema, run_id)
    receipts = []
    for model in MODELS:
        name = model.__tablename__
        where = "WHERE row_value.generation_id=:run" if name == PROJECTION else "WHERE row_value.run_id=:run"
        count, content_sha = await _projected_row_identity(
            session,
            schema,
            name,
            row_json_sql="to_jsonb(row_value)",
            where_sql=where,
            parameters={"run": run_id},
        )
        oid = await native._relation_oid(session, schema, name)
        receipts.append(
            {
                "table_name": name,
                "row_count": count,
                "content_sha256": content_sha,
                "schema_sha256": await native._family_schema_identity(session, IMPORTER_ID, oid, schema, name),
            }
        )
    require(receipts[0]["row_count"] == 1, "Florida run receipt differs")
    require(receipts[-1]["row_count"] == projected, "Florida projection receipt differs")
    require(all(receipt["row_count"] > 0 for receipt in receipts[1:4]), "Florida evidence is incomplete")
    return {
        "contract": CONTRACT,
        "importer_id": IMPORTER_ID,
        "run_id": run_id,
        "source_manifest_sha256": _digest(run["source_manifest"]),
        "source_started_at": run["started_at"].replace(tzinfo=timezone.utc).isoformat(),
        "source_completed_at": run["finished_at"].replace(tzinfo=timezone.utc).isoformat(),
        "tables": receipts,
        "dependencies": {},
    }


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


def validate_manifest(manifest):
    """Validate the exact portable Florida result authority and table receipts."""
    require(
        isinstance(manifest, Mapping)
        and set(manifest)
        == {
            "contract",
            "importer_id",
            "run_id",
            "source_manifest_sha256",
            "source_started_at",
            "source_completed_at",
            "tables",
            "dependencies",
        }
        and manifest["contract"] == CONTRACT
        and manifest["importer_id"] == IMPORTER_ID
        and isinstance(manifest["run_id"], str)
        and bool(_RUN.fullmatch(manifest["run_id"]))
        and isinstance(manifest["source_manifest_sha256"], str)
        and bool(_HEX.fullmatch(manifest["source_manifest_sha256"]))
        and manifest["dependencies"] == {},
        "Florida result manifest is invalid",
    )
    try:
        started = datetime.fromisoformat(manifest["source_started_at"])
        completed = datetime.fromisoformat(manifest["source_completed_at"])
    except TypeError, ValueError:
        raise FloridaProjectionArchiveError("Florida completion time is invalid") from None
    require(
        started.utcoffset() == timezone.utc.utcoffset(started)
        and completed.utcoffset() == timezone.utc.utcoffset(completed)
        and started <= completed,
        "Florida source times are invalid",
    )
    tables = manifest["tables"]
    require(isinstance(tables, list) and len(tables) == len(TABLES), "Florida table set differs")
    for table_receipt, name in zip(tables, TABLES, strict=True):
        require(
            isinstance(table_receipt, Mapping)
            and set(table_receipt) == {"table_name", "row_count", "content_sha256", "schema_sha256"}
            and table_receipt["table_name"] == name
            and type(table_receipt["row_count"]) is int
            and table_receipt["row_count"] >= 0
            and all(
                isinstance(table_receipt[key], str) and _HEX.fullmatch(table_receipt[key])
                for key in ("content_sha256", "schema_sha256")
            ),
            "Florida table receipt differs",
        )
    require(tables[0]["row_count"] == 1 and tables[-1]["row_count"] > 0, "Florida result counts differ")
    return dict(manifest)


async def capture_ownership(session, dataset_id):
    """Capture every owned stage relation and reject unrelated schema objects."""
    schema = stage_schema(dataset_id)
    schema_oid = await native._schema_oid(session, schema)
    pairs = []
    for name in sorted(TABLES):
        pairs.append((name, await native._relation_oid(session, schema, name)))
    pairs = tuple(pairs)
    require(all(type(oid) is int and oid > 0 for _, oid in pairs), "Florida stage relation is missing")
    oids = {oid for _, oid in pairs}
    for row in await native._namespace_relations(session, schema_oid):
        require(
            (row["relkind"] == "r" and row["oid"] in oids)
            or (row["relkind"] == "i" and row["index_table_oid"] in oids),
            "Florida stage contains an unowned relation",
        )
    return StageOwnership(dataset_id, schema_oid, pairs)


async def verify_ownership(session, ownership):
    """Require a stage's current relations to match its sealed ownership."""
    require(isinstance(ownership, StageOwnership), "Florida ownership is invalid")
    require(await capture_ownership(session, ownership.dataset_id) == ownership, "Florida ownership changed")


async def precreate_restore(session, dataset_id):
    """Create an empty isolated stage without secondary indexes."""
    await native._create_model_family(
        session, native.ReferenceFamilySpec(IMPORTER_ID, MODELS), stage_schema(dataset_id), create_indexes=False
    )
    return await capture_ownership(session, dataset_id)


async def complete_restore(session, ownership):
    """Build model indexes only after archive rows have been restored."""
    await verify_ownership(session, ownership)
    metadata = native.MetaData(schema=ownership.schema_name)
    for model in MODELS:
        table = model.__table__.to_metadata(metadata, schema=ownership.schema_name)
        for index in sorted(table.indexes, key=lambda item: item.name):
            await session.execute(CreateIndex(index))


async def validate_stage(session, ownership, manifest):
    """Require the restored stage to match its source manifest exactly."""
    manifest = validate_manifest(manifest)
    await native._lock_family(session, ownership.schema_name, TABLES, "SHARE")
    await verify_ownership(session, ownership)
    for name in TABLES:
        run_column = "generation_id" if name == PROJECTION else "run_id"
        require(
            await session.scalar(
                text(
                    f"SELECT NOT EXISTS(SELECT 1 FROM {_table(ownership.schema_name, name)} WHERE {run_column}<>:run)"
                ),
                {"run": manifest["run_id"]},
            )
            is True,
            "Florida stage contains another run",
        )
    require(
        await describe_result(session, ownership.schema_name, manifest["run_id"]) == manifest,
        "Florida restored result differs",
    )


async def prepare_source(session, *, schema, dataset_id):
    """Pin the published audit graph and clone its exact live projection."""
    await _publication_lock(session, schema)
    await native._lock_family(session, schema, TABLES, "SHARE")
    await profiles.require_pin_guards(session, schema)
    run_id, _ = await _projection_identity(session, schema)
    manifest = await describe_result(session, schema, run_id)
    await pins.record_pin(
        session,
        schema=schema,
        source_key=FL_MQA_SOURCE_KEY,
        run_id=run_id,
        pin_id=dataset_id,
        purpose="export",
        authority={"result_sha256": _digest(manifest)},
    )
    ownership = await precreate_restore(session, dataset_id)
    for name in TABLES:
        run_column = "generation_id" if name == PROJECTION else "run_id"
        await session.execute(
            text(
                f"INSERT INTO {_table(ownership.schema_name, name)} "
                f"SELECT * FROM {_table(schema, name)} WHERE {run_column}=:run"
            ),
            {"run": run_id},
        )
    await complete_restore(session, ownership)
    await validate_stage(session, ownership, manifest)
    return PreparedResult(manifest, ownership)


async def current_identity(session, schema):
    """The live name, table OID and run form one local compare-and-swap token."""
    oid = await native._relation_oid(session, schema, PROJECTION)
    run_id = await session.scalar(text(f"SELECT generation_id FROM {_table(schema, PROJECTION)} LIMIT 1"))
    if run_id is None:
        return {"run_id": None, "relation_oid": oid}
    _validate_run(await _run(session, schema, run_id))
    return {"run_id": run_id, "relation_oid": oid}


def _identity(value):
    require(
        isinstance(value, Mapping)
        and set(value) == {"run_id", "relation_oid"}
        and (value["run_id"] is None or isinstance(value["run_id"], str) and bool(_RUN.fullmatch(value["run_id"])))
        and type(value["relation_oid"]) is int
        and value["relation_oid"] > 0,
        "Florida incumbent identity is invalid",
    )
    return dict(value)


def _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected, pin_id):
    require(
        isinstance(pin_id, UUID)
        and isinstance(package_id, str)
        and bool(_HEX.fullmatch(package_id))
        and type(sealed_owner_oid) is int
        and sealed_owner_oid > 0,
        "Florida adoption authority is invalid",
    )
    _table(destination_schema, PROJECTION)
    return {
        "contract": CONTRACT + ".validation.v1",
        "package_id": package_id,
        "result_sha256": _digest(validate_manifest(prepared.manifest)),
        "ownership": {
            "dataset_id": str(prepared.ownership.dataset_id),
            "schema_oid": prepared.ownership.schema_oid,
            "relation_oids": [list(pair) for pair in prepared.ownership.relation_oids],
        },
        "sealed_owner_oid": sealed_owner_oid,
        "destination_schema": destination_schema,
        "expected": _identity(expected),
        "pin_id": str(pin_id),
    }


def _cutover_name(cutover_id):
    require(isinstance(cutover_id, UUID), "Florida cutover identity is invalid")
    return f"{PROJECTION}_s_{cutover_id.hex[:16]}"


async def _candidate_seal(session, schema, cutover_id, owner_oid):
    name = _cutover_name(cutover_id)
    relation_oid = await native._relation_oid(session, schema, name)
    require(type(relation_oid) is int and relation_oid > 0, "Florida cutover candidate is missing")
    candidate = (
        (
            await session.execute(
                text("SELECT relowner,relkind,relpersistence FROM pg_class WHERE oid=:oid"), {"oid": relation_oid}
            )
        )
        .mappings()
        .one()
    )
    require(
        candidate["relowner"] == owner_oid
        and candidate["relkind"] in ("r", b"r")
        and candidate["relpersistence"] in ("p", b"p"),
        "Florida cutover candidate owner differs",
    )
    require(
        not await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a "
                "WHERE c.oid=:oid AND a.grantee<>:owner)"
            ),
            {"oid": relation_oid, "owner": owner_oid},
        ),
        "Florida cutover candidate has external grants",
    )
    return {"relation_oid": relation_oid, "owner_oid": owner_oid, "table_name": name}


async def _build_projection_candidate(session, schema, source_schema, manifest, owner_oid, cutover_id):
    """Build, index and hash a protected cutover heap outside the short CAS."""
    name = _cutover_name(cutover_id)
    temporary = _table(schema, name)
    source = _table(source_schema, PROJECTION)
    require(await native._relation_oid(session, schema, name) is None, "Florida cutover candidate exists")
    await session.execute(text(f"CREATE TABLE {temporary} (LIKE {source} INCLUDING ALL EXCLUDING DEFAULTS)"))
    await session.execute(text(f"INSERT INTO {temporary} SELECT * FROM {source}"))
    count, content_sha = await _projected_row_identity(session, schema, name, row_json_sql="to_jsonb(row_value)")
    receipt = manifest["tables"][-1]
    require(
        count == receipt["row_count"] and content_sha == receipt["content_sha256"],
        "Florida cutover candidate content differs",
    )
    owner = await session.scalar(text("SELECT rolname FROM pg_roles WHERE oid=:oid"), {"oid": owner_oid})
    require(isinstance(owner, str), "Florida cutover owner differs")
    await session.execute(text(f"ALTER TABLE {temporary} OWNER TO {_role_ident(owner)}"))
    await _clear_stage_grants(session, schema, name, owner_oid)
    return await _candidate_seal(session, schema, cutover_id, owner_oid)


async def _verify_projection_candidate(session, schema, manifest, owner_oid, cutover_id):
    """Recheck a retry candidate's exact protected identity and contents."""
    seal = await _candidate_seal(session, schema, cutover_id, owner_oid)
    count, content_sha = await _projected_row_identity(
        session, schema, seal["table_name"], row_json_sql="to_jsonb(row_value)"
    )
    receipt = manifest["tables"][-1]
    require(
        count == receipt["row_count"] and content_sha == receipt["content_sha256"],
        "Florida cutover candidate content differs",
    )
    return seal


async def _admission(session, schema, expected):
    await _publication_lock(session, schema)
    await native._lock_family(session, schema, (PROJECTION,), "ACCESS SHARE", nowait=True)
    require(await current_identity(session, schema) == _identity(expected), "Florida incumbent changed")
    require(
        not await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {_table(schema, TABLES[0])} "
                "WHERE source_key=:source AND status IN ('running','validating'))"
            ),
            {"source": FL_MQA_SOURCE_KEY},
        ),
        "Florida importer is active",
    )


async def prepare_activation(session, *, prepared, package_id, sealed_owner_oid, destination_schema, expected, pin_id):
    """Adopt invisible audit rows and seal them before a separate cutover."""
    manifest = validate_manifest(prepared.manifest)
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await validate_stage(session, prepared.ownership, manifest)
    base_receipt = _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected, pin_id)
    candidate_name = _cutover_name(pin_id)
    if await native._relation_oid(session, destination_schema, candidate_name) is None:
        cutover = await _build_projection_candidate(
            session, destination_schema, prepared.ownership.schema_name, manifest, sealed_owner_oid, pin_id
        )
    else:
        cutover = await _verify_projection_candidate(session, destination_schema, manifest, sealed_owner_oid, pin_id)
    receipt_by_field = {**base_receipt, "cutover": cutover}
    await _admission(session, destination_schema, expected)
    await profiles.require_pin_guards(session, destination_schema)
    run_id = manifest["run_id"]
    await pins.lock_run(session, destination_schema, run_id)
    existing_pin = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(destination_schema, pins.TABLE)} WHERE pin_id=:pin FOR UPDATE"),
                {"pin": str(pin_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    if existing_pin is not None:
        require(
            existing_pin["run_id"] == run_id
            and existing_pin["source_key"] == FL_MQA_SOURCE_KEY
            and existing_pin["purpose"] == "adoption"
            and existing_pin["authority_json"] == receipt_by_field,
            "Florida adoption pin differs",
        )
        return receipt_by_field
    require(await _run(session, destination_schema, run_id) is None, "Florida result already exists locally")
    for name in TABLES[:-1]:
        await session.execute(
            text(
                f"INSERT INTO {_table(destination_schema, name)} "
                f"SELECT * FROM {_table(prepared.ownership.schema_name, name)} WHERE run_id=:run"
            ),
            {"run": run_id},
        )
    await pins.record_pin(
        session,
        schema=destination_schema,
        source_key=FL_MQA_SOURCE_KEY,
        run_id=run_id,
        pin_id=pin_id,
        purpose="adoption",
        authority=receipt_by_field,
    )
    return receipt_by_field


async def _copy_projection_access(session, schema, temporary_name, live_oid):
    """Replace inherited stage ACLs with the incumbent's exact table grants."""
    live = _table(schema, PROJECTION)
    temporary = _table(schema, temporary_name)
    security = await _live_projection_security(session, live_oid)
    owner = await session.scalar(text("SELECT rolname FROM pg_roles WHERE oid=:oid"), {"oid": security["relowner"]})
    require(isinstance(owner, str), "Florida live projection owner changed")
    require(await native._relation_oid(session, schema, temporary_name) is None, "Florida cutover stage exists")
    await session.execute(text(f"CREATE TABLE {temporary} (LIKE {live} INCLUDING ALL EXCLUDING DEFAULTS)"))
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


async def _swap_projection(session, schema, target_schema, target_run_id, pin_id):
    """Copy a sealed generation and perform Florida's atomic live/old rename."""
    live = _table(schema, PROJECTION)
    target_projection = _table(target_schema, PROJECTION)
    temporary_name = f"{PROJECTION}_s_{pin_id.hex[:16]}"
    temporary = _table(schema, temporary_name)
    old = _table(schema, PROJECTION + "_old")
    live_oid = await native._relation_oid(session, schema, PROJECTION)
    await _copy_projection_access(session, schema, temporary_name, live_oid)
    await session.execute(text(f"INSERT INTO {temporary} SELECT * FROM {target_projection}"))
    projected = await session.scalar(text(f"SELECT count(*) FROM {temporary}"))
    require(projected > 0, "Florida cutover stage is empty")
    require(
        await session.scalar(
            text(f"SELECT NOT EXISTS(SELECT 1 FROM {temporary} WHERE generation_id<>:run)"), {"run": target_run_id}
        )
        is True,
        "Florida cutover stage differs",
    )
    # Hold the DDL lock across dependency inspection and rename so a new view or
    # foreign key cannot attach to the incumbent between the two operations.
    await native._lock_family(session, schema, (PROJECTION,), "ACCESS EXCLUSIVE", nowait=True)
    dependents = await session.scalar(
        text(
            "SELECT EXISTS("
            "SELECT 1 FROM pg_constraint WHERE contype='f' AND confrelid=:oid "
            "UNION ALL SELECT 1 FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid "
            "WHERE d.classid='pg_rewrite'::regclass AND d.refobjid=:oid "
            "AND r.ev_class<>:oid)"
        ),
        {"oid": live_oid},
    )
    require(not dependents, "Florida live projection has external dependents")
    await session.execute(text(f"DROP TABLE IF EXISTS {old} RESTRICT"))
    await session.execute(text(f"ALTER TABLE {live} RENAME TO {native._quoted(PROJECTION + '_old')}"))
    await session.execute(text(f"ALTER TABLE {temporary} RENAME TO {native._quoted(PROJECTION)}"))
    return await current_identity(session, schema)


async def _cutover_prepared_projection(session, schema, run_id, cutover_id, seal):
    """Verify a sealed prepared heap, then perform only metadata work under CAS."""
    require(
        isinstance(seal, Mapping) and set(seal) == {"relation_oid", "owner_oid", "table_name"},
        "Florida cutover seal is invalid",
    )
    require(
        await _candidate_seal(session, schema, cutover_id, seal["owner_oid"]) == seal,
        "Florida cutover candidate changed",
    )
    live = _table(schema, PROJECTION)
    temporary = _table(schema, seal["table_name"])
    old = _table(schema, PROJECTION + "_old")
    await native._lock_family(session, schema, (PROJECTION,), "ACCESS EXCLUSIVE", nowait=True)
    live_oid = await native._relation_oid(session, schema, PROJECTION)
    dependents = await session.scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE contype='f' AND confrelid=:oid "
            "UNION ALL SELECT 1 FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid "
            "WHERE d.classid='pg_rewrite'::regclass AND d.refobjid=:oid AND r.ev_class<>:oid)"
        ),
        {"oid": live_oid},
    )
    require(not dependents, "Florida live projection has external dependents")
    security = await _live_projection_security(session, live_oid)
    owner = await session.scalar(text("SELECT rolname FROM pg_roles WHERE oid=:oid"), {"oid": security["relowner"]})
    require(isinstance(owner, str), "Florida live projection owner changed")
    await session.execute(text(f"ALTER TABLE {temporary} OWNER TO {_role_ident(owner)}"))
    await _clear_stage_grants(session, schema, seal["table_name"], security["relowner"])
    await _replay_live_grants(session, temporary, live_oid, security["relowner"])
    await session.execute(text(f"DROP TABLE IF EXISTS {old} RESTRICT"))
    await session.execute(text(f"ALTER TABLE {live} RENAME TO {native._quoted(PROJECTION + '_old')}"))
    await session.execute(text(f"ALTER TABLE {temporary} RENAME TO {native._quoted(PROJECTION)}"))
    return {"run_id": run_id, "relation_oid": seal["relation_oid"]}


async def activate_validated_result(
    session, *, prepared, destination_schema, expected, validation, package_id, sealed_owner_oid, pin_id
):
    """CAS the live projection only after a committed immutable audit adoption."""
    manifest = validate_manifest(prepared.manifest)
    base_receipt = _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected, pin_id)
    require(
        isinstance(validation, Mapping) and {key: validation.get(key) for key in base_receipt} == base_receipt,
        "Florida adoption validation changed",
    )
    await _admission(session, destination_schema, expected)
    await native._lock_family(session, prepared.ownership.schema_name, TABLES, "SHARE", nowait=True)
    await verify_ownership(session, prepared.ownership)
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await profiles.require_pin_guards(session, destination_schema)
    pin = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(destination_schema, pins.TABLE)} WHERE pin_id=:pin FOR UPDATE"),
                {"pin": str(pin_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    require(
        pin is not None
        and pin["run_id"] == manifest["run_id"]
        and pin["source_key"] == FL_MQA_SOURCE_KEY
        and pin["purpose"] == "adoption"
        and pin["authority_json"] == validation,
        "Florida adoption seal differs",
    )
    _validate_run(await _run(session, destination_schema, manifest["run_id"]))
    new_identity = await _cutover_prepared_projection(
        session, destination_schema, manifest["run_id"], pin_id, validation.get("cutover")
    )
    return {
        "current": new_identity,
        "previous": _identity(expected),
        "pin_id": str(pin_id),
        "result_sha256": _digest(manifest),
    }


async def rollback_validated_result(
    session, *, prepared, destination_schema, expected, package_id, sealed_owner_oid, pin_id, cutover
):
    """Restore a retained predecessor through the same CAS and atomic rename."""
    manifest = validate_manifest(prepared.manifest)
    await _admission(session, destination_schema, expected)
    await native._lock_family(session, prepared.ownership.schema_name, TABLES, "SHARE", nowait=True)
    await verify_ownership(session, prepared.ownership)
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await profiles.require_pin_guards(session, destination_schema)
    receipt = _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected, pin_id)
    pin = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(destination_schema, pins.TABLE)} WHERE pin_id=:pin FOR UPDATE"),
                {"pin": str(pin_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    require(
        pin is not None
        and pin["run_id"] == manifest["run_id"]
        and pin["source_key"] == FL_MQA_SOURCE_KEY
        and pin["purpose"] == "adoption"
        and pin["authority_json"]["package_id"] == package_id
        and pin["authority_json"]["result_sha256"] == receipt["result_sha256"]
        and pin["authority_json"]["ownership"] == receipt["ownership"]
        and pin["authority_json"]["sealed_owner_oid"] == sealed_owner_oid,
        "Florida rollback seal differs",
    )
    require(
        await _run(session, destination_schema, manifest["run_id"]) is not None, "Florida rollback audit is unavailable"
    )
    return await _cutover_prepared_projection(session, destination_schema, manifest["run_id"], cutover[0], cutover[1])


async def prepare_rollback_cutover(session, *, prepared, destination_schema, sealed_owner_oid, cutover_id):
    """Verify a retained predecessor and build its protected heap before recovery."""
    manifest = validate_manifest(prepared.manifest)
    await native._lock_family(session, prepared.ownership.schema_name, TABLES, "SHARE", nowait=True)
    await verify_ownership(session, prepared.ownership)
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await validate_stage(session, prepared.ownership, manifest)
    if await native._relation_oid(session, destination_schema, _cutover_name(cutover_id)) is not None:
        return await _verify_projection_candidate(session, destination_schema, manifest, sealed_owner_oid, cutover_id)
    return await _build_projection_candidate(
        session, destination_schema, prepared.ownership.schema_name, manifest, sealed_owner_oid, cutover_id
    )


async def cleanup_cutover_candidate(session, *, schema, cutover_id, seal):
    """Discard only the exact unpromoted heap from a failed recovery."""
    name = _cutover_name(cutover_id)
    relation_oid = await native._relation_oid(session, schema, name)
    if relation_oid is None:
        return
    require(
        await _candidate_seal(session, schema, cutover_id, seal["owner_oid"]) == seal,
        "Florida cutover candidate changed",
    )
    await session.execute(text(f"DROP TABLE {_table(schema, name)} RESTRICT"))


async def release_source_pin(session, *, schema, run_id, pin_id):
    """Release only the matching export seal after a transfer completes."""
    await _publication_lock(session, schema)
    pin = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin FOR UPDATE"),
                {"pin": str(pin_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    if pin is None:
        return "already_released"
    require(
        pin["source_key"] == FL_MQA_SOURCE_KEY and pin["run_id"] == run_id and pin["purpose"] == "export",
        "Florida export pin differs",
    )
    await pins.lock_run(session, schema, run_id)
    await session.execute(text(f"DELETE FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin"), {"pin": str(pin_id)})
    return "released"


async def cleanup_adoption(session, *, schema, run_id, pin_id):
    """Remove only an unserved failed adoption owned by this exact pin."""
    await _publication_lock(session, schema)
    require(isinstance(pin_id, UUID), "Florida cleanup pin is invalid")
    pin = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin FOR UPDATE"),
                {"pin": str(pin_id)},
            )
        )
        .mappings()
        .one_or_none()
    )
    if pin is None:
        return "already_released"
    require(
        pin["run_id"] == run_id and pin["source_key"] == FL_MQA_SOURCE_KEY and pin["purpose"] == "adoption",
        "Florida cleanup pin differs",
    )
    for name in (PROJECTION, PROJECTION + "_old"):
        if await native._relation_oid(session, schema, name) is not None:
            require(
                not await session.scalar(
                    text(f"SELECT EXISTS(SELECT 1 FROM {_table(schema, name)} WHERE generation_id=:run)"),
                    {"run": run_id},
                ),
                "Florida cleanup would remove a served generation",
            )
    require(
        not await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {_table(schema, pins.TABLE)} WHERE run_id=:run AND pin_id<>:pin)"),
            {"run": run_id, "pin": str(pin_id)},
        ),
        "Florida cleanup run has another pin",
    )
    await cleanup_cutover_candidate(session, schema=schema, cutover_id=pin_id, seal=pin["authority_json"]["cutover"])
    await pins.lock_run(session, schema, run_id)
    await session.execute(text(f"DELETE FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin"), {"pin": str(pin_id)})
    for name in reversed(TABLES[:-1]):
        await session.execute(text(f"DELETE FROM {_table(schema, name)} WHERE run_id=:run"), {"run": run_id})
    return "released"


async def cleanup_stage(session, ownership):
    """Remove only the stage relations recorded by the ownership receipt."""
    await native._lock_family(session, ownership.schema_name, TABLES, "ACCESS EXCLUSIVE", nowait=True)
    await verify_ownership(session, ownership)
    await session.execute(
        text("DROP TABLE " + ", ".join(_table(ownership.schema_name, name) for name in TABLES) + " RESTRICT")
    )
    await session.execute(text(f"DROP SCHEMA {native._quoted(ownership.schema_name)} RESTRICT"))
