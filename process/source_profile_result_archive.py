# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Archive completed source assertions and CAS one native publication pointer.

Registry matching is already retained in records/facts. Serving these results
does not read reference tables or repeat matching; captured registry provenance
is preserved as-is. Control attempts and local archive authority are not portable.
"""

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
from process import source_profile_result_pins as pins
from process.entity_address_snapshot_receipt import _projected_row_identity

CONTRACT = "source-profile-result.postgres.v1"
VALIDATION_CONTRACT = "source-profile-result.validation.v1"
SOURCES = {
    "massachusetts-borim-profile": ("massachusetts-borim", "ma-borim-profile/v1", "MA"),
    "kentucky-kbml-profile": ("kentucky-kbml", "ky-kbml-profile/v1", "KY"),
    "tennessee-tdh-profile": ("tennessee-tdh", "tn-tdh-profile/v1", "TN"),
    "rhode-island-doh-profile": ("rhode-island-doh", "ri-doh-profile/v1", "RI"),
    "new-york-nypp-profile": ("new-york-nypp", "ny-nypp-education/v1", "NY"),
}
MODELS = (
    models.ProviderProfileImportRun,
    models.ProviderProfileArtifact,
    models.ProviderProfileSourceRecord,
    models.ProviderProfileFact,
)
TABLES = tuple(model.__tablename__ for model in MODELS)
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_RUN = re.compile(r"(?:[0-9a-f]{32}|[0-9a-f]{64})\Z")
MAX_RUNS = 64


class SourceProfileArchiveError(RuntimeError):
    """An exact retained result, dependency or destination fence differs."""


def _require(condition, message):
    if not condition:
        raise SourceProfileArchiveError(message)


def source_spec(importer_id):
    """Return the closed native table family for one supported source."""
    _require(isinstance(importer_id, str) and importer_id in SOURCES, "source profile is unsupported")
    return native.ReferenceFamilySpec(importer_id, MODELS)


def _digest(value):
    return hashlib.sha256(native._canonical_json(value)).hexdigest()


def result_dependencies(value):
    """The completed assertion graph has no destination reference dependency."""
    _require(isinstance(value, Mapping) and not value, "source result dependencies are invalid")
    return {}


def stage_schema(dataset_id):
    """Derive the sole allowed stage namespace from its local UUID."""
    _require(isinstance(dataset_id, UUID), "stage identity is invalid")
    return "source_profile_result_" + dataset_id.hex


def _table(schema, name):
    return f"{native._quoted(native._schema_name(schema))}.{native._quoted(name)}"


@dataclass(frozen=True)
class StageOwnership:
    importer_id: str
    dataset_id: UUID
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]

    sequence_oids = ()
    auxiliary_oid = None

    @property
    def schema_name(self):
        """Return this stage identity without accepting a peer namespace."""
        return stage_schema(self.dataset_id)


@dataclass(frozen=True)
class PreparedResult:
    manifest: dict
    ownership: StageOwnership


async def _source_lock(session, schema, importer_id):
    native._require_transaction(session)
    source_spec(importer_id)
    key = f"{_table(schema, 'provider_profile_source_publication')}.{SOURCES[importer_id][0]}.publication"
    await session.execute(text("SET LOCAL lock_timeout='500ms'"))
    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": key})


async def _pointer(session, schema, importer_id):
    return (
        (
            await session.execute(
                text(
                    f"SELECT current_run_id,previous_run_id,published_at FROM {_table(schema, 'provider_profile_source_publication')} "
                    "WHERE source_key=:source_key"
                ),
                {"source_key": SOURCES[importer_id][0]},
            )
        )
        .mappings()
        .one_or_none()
    )


async def capture_ownership(session, importer_id, dataset_id):
    """Read exact local relation OIDs and reject extra stage objects."""
    native._require_transaction(session)
    source_spec(importer_id)
    schema = stage_schema(dataset_id)
    schema_oid = await native._schema_oid(session, schema)
    pairs = tuple([(name, await native._relation_oid(session, schema, name)) for name in sorted(TABLES)])
    _require(all(type(oid) is int and oid > 0 for _, oid in pairs), "stage relation is missing")
    oids = {oid for _, oid in pairs}
    for row in await native._namespace_relations(session, schema_oid):
        _require(
            (row["relkind"] == "r" and row["oid"] in oids)
            or (row["relkind"] == "i" and row["index_table_oid"] in oids),
            "stage contains an unowned relation",
        )
    return StageOwnership(importer_id, dataset_id, schema_oid, pairs)


async def verify_ownership(session, ownership):
    """Refuse cleanup or promotion if an owned namespace or table changed."""
    _require(isinstance(ownership, StageOwnership), "stage ownership is invalid")
    observed = await capture_ownership(session, ownership.importer_id, ownership.dataset_id)
    _require(observed == ownership, "stage ownership changed")


async def precreate_restore(session, importer_id, dataset_id):
    """Use installed model DDL; no peer SQL or source control tables are restored."""
    native._require_transaction(session)
    await native._create_model_family(session, source_spec(importer_id), stage_schema(dataset_id), create_indexes=False)
    return await capture_ownership(session, importer_id, dataset_id)


async def complete_restore(session, ownership):
    """Build model indexes after COPY, before retained content validation."""
    await verify_ownership(session, ownership)
    metadata = native.MetaData(schema=ownership.schema_name)
    for model in MODELS:
        table = model.__table__.to_metadata(metadata, schema=ownership.schema_name)
        for index in sorted(table.indexes, key=lambda item: item.name):
            await session.execute(CreateIndex(index))


def _validate_run(importer_id, run):
    source_spec(importer_id)
    source_key, schema_version, jurisdiction = SOURCES[importer_id]
    _require(
        isinstance(run, Mapping)
        and isinstance(run.get("run_id"), str)
        and bool(_RUN.fullmatch(run["run_id"]))
        and (run.get("source_key"), run.get("schema_version"), run.get("jurisdiction"))
        == (source_key, schema_version, jurisdiction)
        and run.get("status") == "completed"
        and isinstance(run.get("finished_at"), datetime)
        and run.get("error") is None,
        "retained completed source identity differs",
    )
    manifest, metrics = run.get("source_manifest"), run.get("metrics")
    _require(
        isinstance(manifest, Mapping)
        and manifest.get("max_providers", False) is None
        and isinstance(metrics, Mapping)
        and metrics.get("published") is True,
        "retained source was not published",
    )
    descriptor = manifest.get("source")
    _require(
        isinstance(descriptor, Mapping)
        and (descriptor.get("source_key"), descriptor.get("source_kind"), descriptor.get("jurisdiction"))
        == (source_key, "state_regulator", jurisdiction),
        "retained source descriptor differs",
    )
    _validate_serving_scope(run)
    return {}


def _validate_serving_scope(run):
    """Use the serving policy rather than accepting a merely well-hashed graph."""
    from api.provider_profile_states import _validate_state_publication
    from process.massachusetts_profile_store import _store as massachusetts

    manifest = run["source_manifest"]
    descriptor = manifest["source"]
    _require(
        all(
            isinstance(descriptor.get(key), str) and descriptor[key]
            for key in ("agency", "coverage_scope", "registry_generation")
        ),
        "retained serving descriptor differs",
    )
    try:
        _validate_state_publication(
            [
                {
                    "source_manifest": manifest,
                    "publication_source_key": run["source_key"],
                    "run_schema_version": run["schema_version"],
                    "run_jurisdiction": run["jurisdiction"],
                }
            ],
            descriptor,
            manifest["categories"],
        )
        if run["source_key"] == SOURCES["massachusetts-borim-profile"][0]:
            _require(
                descriptor["agency"] == "Massachusetts Board of Registration in Medicine",
                "retained serving descriptor differs",
            )
            massachusetts._manifest(run)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise SourceProfileArchiveError("retained serving scope differs") from exc


async def _run(session, schema, run_id):
    _require(isinstance(run_id, str) and bool(_RUN.fullmatch(run_id)), "run identity is invalid")
    return (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(schema, TABLES[0])} WHERE run_id=:run_id"), {"run_id": run_id}
            )
        )
        .mappings()
        .one_or_none()
    )


async def _integrity(session, schema, importer_id, run_id):
    artifacts, source_records, facts = (_table(schema, name) for name in TABLES[1:])
    runs = _table(schema, TABLES[0])
    invalid = await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {artifacts} a WHERE a.run_id=:run_id AND
            (a.source_key<>:source_key OR a.content_sha256 !~ '^[0-9a-f]{{64}}$' OR a.content_bytes<=0))
        OR NOT EXISTS(SELECT 1 FROM {artifacts} WHERE run_id=:run_id)
        OR NOT EXISTS(SELECT 1 FROM {source_records} WHERE run_id=:run_id)
        OR EXISTS(SELECT 1 FROM {source_records} r LEFT JOIN {artifacts} a ON a.artifact_id=r.artifact_id
            WHERE r.run_id=:run_id AND (r.source_key<>:source_key OR a.run_id IS DISTINCT FROM r.run_id
                OR a.source_key IS DISTINCT FROM r.source_key
                OR r.normalized_payload->>'schema_version' IS DISTINCT FROM :schema_version))
        OR EXISTS(SELECT 1 FROM {facts} f LEFT JOIN {source_records} r ON r.record_id=f.source_record_id
            LEFT JOIN {runs} source_run ON source_run.run_id=f.run_id
            WHERE f.run_id=:run_id AND (r.run_id IS DISTINCT FROM f.run_id
                OR f.npi IS DISTINCT FROM r.matched_npi
                OR r.source_key IS DISTINCT FROM :source_key
                OR f.source_json->>'source_record_id' IS DISTINCT FROM f.source_record_id
                OR (source_run.source_manifest::jsonb->'categories' ? f.category) IS DISTINCT FROM TRUE
                OR r.normalized_payload->>'visibility' IS DISTINCT FROM 'public'
                OR (f.npi IS NOT NULL AND r.match_status<>'deterministic')
                OR f.published_at IS DISTINCT FROM source_run.finished_at
                OR f.source_json->>'run_id' IS DISTINCT FROM f.run_id
                OR f.source_json->>'agency' IS DISTINCT FROM source_run.source_manifest->'source'->>'agency'
                OR f.source_json->>'jurisdiction' IS DISTINCT FROM source_run.jurisdiction
                OR f.source_json->>'source_key' IS DISTINCT FROM :source_key
                OR f.source_json->>'schema_version' IS DISTINCT FROM :schema_version))
    """),
        {"run_id": run_id, "source_key": SOURCES[importer_id][0], "schema_version": SOURCES[importer_id][1]},
    )
    _require(invalid is False, "retained payload integrity differs")
    run = await _run(session, schema, run_id)
    fact_types = await session.stream(
        text(f"SELECT DISTINCT category,fact_type FROM {facts} WHERE run_id=:run"), {"run": run_id}
    )
    async for fact in fact_types.mappings():
        _validate_fact_shape(run, fact)


def _validate_fact_shape(run, fact):
    from api import provider_profile_states as serving
    from process.massachusetts_profile_rows import FACT_FORMAT_BY_CATEGORY

    source_key = run["source_key"]
    evidence_by_field = {
        **run["source_manifest"]["source"],
        "run_id": run["run_id"],
        "schema_version": run["schema_version"],
    }
    assertion_by_field = {**fact, "run_id": run["run_id"], "npi": 0, "source_json": evidence_by_field}
    try:
        if source_key == serving.NYPP_SOURCE_KEY:
            serving._validate_new_york_fact(0, run["run_id"], assertion_by_field, source_key)
        elif source_key == serving.MASSACHUSETTS_SOURCE_KEY:
            _require(
                FACT_FORMAT_BY_CATEGORY.get(fact["category"], (None,))[0] == fact["fact_type"],
                "retained fact type differs",
            )
        else:
            validator = {
                serving.KENTUCKY_SOURCE_KEY: serving._validate_kentucky_fact,
                serving.TN_SOURCE_KEY: serving._validate_tennessee_fact,
                serving.RI_SOURCE_KEY: serving._validate_rhode_island_fact,
            }[source_key]
            validator(0, run["run_id"], assertion_by_field)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise SourceProfileArchiveError("retained fact type differs") from exc


async def describe_result(session, *, importer_id, schema, run_id):
    """Bind exact assertion rows, captured provenance and original completion time."""
    runs = await _lineage(session, importer_id, schema, run_id)
    run = runs[0]
    run_ids = [row["run_id"] for row in runs]
    for selected_id in run_ids:
        await _integrity(session, schema, importer_id, selected_id)
    tables = await _table_receipts(session, importer_id, schema, run_ids)
    return {
        "contract": CONTRACT,
        "importer_id": importer_id,
        "source_key": SOURCES[importer_id][0],
        "run_id": run_id,
        "run_ids": run_ids,
        "source_completed_at": run["finished_at"].replace(tzinfo=timezone.utc).isoformat(),
        "source_manifest_sha256": _digest(run["source_manifest"]),
        "dependencies": {},
        "tables": tables,
    }


async def _lineage(session, importer_id, schema, run_id):
    """Close Massachusetts reprocessing ancestry, never publication history."""
    runs, seen = [], set()
    while run_id is not None:
        _require(run_id not in seen and len(runs) < MAX_RUNS, "source ancestry is cyclic or too deep")
        run = await _run(session, schema, run_id)
        _validate_run(importer_id, run)
        if runs:
            await _validate_parent(session, schema, runs[-1], run)
        runs.append(run)
        seen.add(run_id)
        run_id = run["source_manifest"].get("reprocess_from")
        _require(run_id is None or importer_id == "massachusetts-borim-profile", "source ancestry is unsupported")
    return runs


async def _validate_parent(session, schema, child, parent):
    child_manifest, parent_manifest = child["source_manifest"], parent["source_manifest"]
    _require(
        all(child_manifest[key] == parent_manifest[key] for key in ("source", "cohort_sha256", "full_cohort_licenses")),
        "source ancestry scope differs",
    )
    lineage = child_manifest["reprocessing"]
    artifact_matches = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {_table(schema, TABLES[1])} WHERE run_id=:run AND artifact_id=:artifact "
            "AND content_sha256=:sha AND file_name='manifest.json' AND category='profile')"
        ),
        {"run": parent["run_id"], "artifact": lineage["artifact_id"], "sha": lineage["manifest_sha256"]},
    )
    _require(artifact_matches, "source ancestry artifact differs")


async def _table_receipts(session, importer_id, schema, run_ids):
    tables = []
    for model in MODELS:
        name = model.__tablename__
        count, content_sha = await _projected_row_identity(
            session,
            schema,
            name,
            row_json_sql="to_jsonb(row_value)",
            where_sql="WHERE row_value.run_id=ANY(CAST(:run_ids AS text[]))",
            parameters={"run_ids": run_ids},
        )
        oid = await native._relation_oid(session, schema, name)
        schema_sha = await native._family_schema_identity(session, importer_id, oid, schema, name)
        tables.append(
            {"table_name": name, "row_count": count, "content_sha256": content_sha, "schema_sha256": schema_sha}
        )
    return tables


def validate_manifest(manifest_by_field):
    """Require the exact completed assertion scope and table receipt contract."""
    _require(
        isinstance(manifest_by_field, Mapping)
        and set(manifest_by_field)
        == {
            "contract",
            "importer_id",
            "source_key",
            "run_id",
            "run_ids",
            "source_completed_at",
            "source_manifest_sha256",
            "dependencies",
            "tables",
        },
        "result manifest is invalid",
    )
    source_spec(manifest_by_field["importer_id"])
    _require(
        manifest_by_field["contract"] == CONTRACT
        and manifest_by_field["source_key"] == SOURCES[manifest_by_field["importer_id"]][0],
        "result scope differs",
    )
    _require(
        isinstance(manifest_by_field["run_id"], str) and bool(_RUN.fullmatch(manifest_by_field["run_id"])),
        "result run is invalid",
    )
    _validate_run_ids(manifest_by_field)
    try:
        completed = datetime.fromisoformat(manifest_by_field["source_completed_at"])
        _require(
            completed.utcoffset() == timezone.utc.utcoffset(completed)
            and completed.isoformat() == manifest_by_field["source_completed_at"],
            "source completion time is invalid",
        )
    except ValueError, TypeError:
        raise SourceProfileArchiveError("source completion time is invalid") from None
    _require(
        isinstance(manifest_by_field["source_manifest_sha256"], str)
        and bool(_HEX.fullmatch(manifest_by_field["source_manifest_sha256"])),
        "manifest digest is invalid",
    )
    result_dependencies(manifest_by_field["dependencies"])
    _validate_table_receipts(manifest_by_field["tables"])
    _require(manifest_by_field["tables"][0]["row_count"] == len(manifest_by_field["run_ids"]), "result audits differ")
    return dict(manifest_by_field)


def _validate_run_ids(manifest):
    run_ids = manifest["run_ids"]
    _require(
        isinstance(run_ids, list)
        and 1 <= len(run_ids) <= MAX_RUNS
        and all(isinstance(value, str) and _RUN.fullmatch(value) for value in run_ids)
        and len(set(run_ids)) == len(run_ids)
        and run_ids[0] == manifest["run_id"]
        and (len(run_ids) == 1 or manifest["importer_id"] == "massachusetts-borim-profile"),
        "result ancestry is invalid",
    )


def _validate_table_receipts(tables):
    _require(isinstance(tables, list) and len(tables) == len(TABLES), "result table set differs")
    for table, name in zip(tables, TABLES, strict=True):
        _require(
            isinstance(table, Mapping)
            and set(table)
            == {
                "table_name",
                "row_count",
                "content_sha256",
                "schema_sha256",
            },
            "table receipt is invalid",
        )
        _require(
            table["table_name"] == name
            and type(table["row_count"]) is int
            and table["row_count"] >= 0
            and all(
                isinstance(table[key], str) and bool(_HEX.fullmatch(table[key]))
                for key in ("schema_sha256", "content_sha256")
            ),
            "table identity differs",
        )


async def prepare_source(session, *, importer_id, schema, run_id, dataset_id):
    """Pin and clone the current root plus its closed reprocessing ancestry."""
    await _source_lock(session, schema, importer_id)
    await session.execute(text("SET LOCAL statement_timeout='1800s'"))
    # A shared-table lock bounds this local slice; use immutable producer generations for concurrent capture.
    await native._lock_family(session, schema, TABLES, "SHARE")
    await require_pin_guards(session, schema)
    pointer = await _pointer(session, schema, importer_id)
    _require(
        pointer is not None and run_id == pointer["current_run_id"],
        "result is not retained",
    )
    manifest = await describe_result(session, importer_id=importer_id, schema=schema, run_id=run_id)
    for selected_id in sorted(manifest["run_ids"]):
        await pins.record_pin(
            session,
            schema=schema,
            source_key=SOURCES[importer_id][0],
            run_id=selected_id,
            pin_id=dataset_id,
            purpose="export",
            authority={"result_sha256": _digest(manifest), "root_run_id": run_id, "run_ids": manifest["run_ids"]},
        )
    ownership = await precreate_restore(session, importer_id, dataset_id)
    for name in TABLES:
        await session.execute(
            text(
                f"INSERT INTO {_table(ownership.schema_name, name)} SELECT * FROM {_table(schema, name)} "
                "WHERE run_id=ANY(CAST(:run_ids AS text[]))"
            ),
            {"run_ids": manifest["run_ids"]},
        )
    await complete_restore(session, ownership)
    await validate_stage(session, ownership, manifest)
    return PreparedResult(manifest, ownership)


async def validate_stage(session, ownership, manifest):
    """Validate index/schema identity and every row, including equal-count corruption."""
    manifest = validate_manifest(manifest)
    _require(manifest["importer_id"] == ownership.importer_id, "stage source differs")
    await native._lock_family(session, ownership.schema_name, TABLES, "SHARE")
    await verify_ownership(session, ownership)
    for name in TABLES:
        foreign_rows = await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {_table(ownership.schema_name, name)} WHERE NOT(run_id=ANY(CAST(:run_ids AS text[]))))"
            ),
            {"run_ids": manifest["run_ids"]},
        )
        _require(foreign_rows is False, "stage contains another result")
    observed = await describe_result(
        session, importer_id=ownership.importer_id, schema=ownership.schema_name, run_id=manifest["run_id"]
    )
    _require(observed == manifest, "restored result differs")
    return observed


def ownership_dict(ownership):
    """Bind the local stage, never a peer's control-run authority."""
    return {
        "importer_id": ownership.importer_id,
        "dataset_id": str(ownership.dataset_id),
        "schema_name": ownership.schema_name,
        "schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
    }


def _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected_current_run_id, pin_id):
    _require(
        isinstance(pin_id, UUID)
        and isinstance(package_id, str)
        and bool(_HEX.fullmatch(package_id))
        and type(sealed_owner_oid) is int
        and sealed_owner_oid > 0,
        "adoption authority is invalid",
    )
    _table(destination_schema, TABLES[0])
    _require(
        expected_current_run_id is None
        or isinstance(expected_current_run_id, str)
        and bool(_RUN.fullmatch(expected_current_run_id)),
        "adoption predecessor is invalid",
    )
    receipt_by_field = {
        "contract": VALIDATION_CONTRACT,
        "package_id": package_id,
        "result_sha256": _digest(validate_manifest(prepared.manifest)),
        "ownership": ownership_dict(prepared.ownership),
        "sealed_owner_oid": sealed_owner_oid,
        "destination_schema": destination_schema,
        "expected_current_run_id": expected_current_run_id,
        "pin_id": str(pin_id),
    }
    return {**receipt_by_field, "validation_sha256": _digest(receipt_by_field)}


async def _admission(session, manifest, destination_schema, expected_current_run_id):
    await _source_lock(session, destination_schema, manifest["importer_id"])
    await native._lock_family(session, destination_schema, TABLES, "ACCESS SHARE", nowait=True)
    pointer = await _pointer(session, destination_schema, manifest["importer_id"])
    current = pointer["current_run_id"] if pointer else None
    _require(current == expected_current_run_id, "destination predecessor changed")
    active = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {_table(destination_schema, TABLES[0])} "
            "WHERE source_key=:source AND status IN ('running','validating'))"
        ),
        {"source": manifest["source_key"]},
    )
    _require(not active, "destination source is active")


async def prepare_activation(
    session, *, prepared, package_id, sealed_owner_oid, destination_schema, expected_current_run_id, pin_id
):
    """Validate and insert invisible rows in the long preparation transaction.

    The installed native row guard serializes with in-flight writers; the local
    adoption pin seals these rows before commit. Publication remains unchanged.
    """
    manifest = validate_manifest(prepared.manifest)
    receipt = _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected_current_run_id, pin_id)
    await _admission(session, manifest, destination_schema, expected_current_run_id)
    await session.execute(text("SET LOCAL statement_timeout='1800s'"))
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await validate_stage(session, prepared.ownership, manifest)
    await require_pin_guards(session, destination_schema)
    for selected_id in sorted(manifest["run_ids"]):
        await pins.lock_run(session, destination_schema, selected_id)
    prior = await _pin_group(session, destination_schema, pin_id)
    if prior:
        _validate_adoption_pins(prior, manifest, receipt)
        return receipt
    _require(await _run(session, destination_schema, manifest["run_id"]) is None, "result already exists locally")
    created_ids = await _adopt_rows(session, prepared, destination_schema)
    for selected_id in sorted(manifest["run_ids"]):
        await pins.record_pin(
            session,
            schema=destination_schema,
            source_key=manifest["source_key"],
            run_id=selected_id,
            pin_id=pin_id,
            purpose="adoption",
            authority={
                "validation": receipt,
                "root_run_id": manifest["run_id"],
                "run_ids": manifest["run_ids"],
                "created_here": selected_id in created_ids,
            },
        )
    return receipt


async def _adopt_rows(session, prepared, destination_schema):
    """Reuse identical local ancestors; never overwrite another result's rows."""
    manifest = prepared.manifest
    created_ids = []
    for run_id in manifest["run_ids"]:
        if await _run(session, destination_schema, run_id) is None:
            created_ids.append(run_id)
        else:
            existing = await _table_receipts(session, manifest["importer_id"], destination_schema, [run_id])
            incoming = await _table_receipts(session, manifest["importer_id"], prepared.ownership.schema_name, [run_id])
            _require(existing == incoming, "local ancestor content differs")
    for model in MODELS:
        name = model.__tablename__
        source_oid = dict(prepared.ownership.relation_oids)[name]
        destination_oid = await native._relation_oid(session, destination_schema, name)
        source_schema_sha = await native._family_schema_identity(
            session, manifest["importer_id"], source_oid, prepared.ownership.schema_name, name
        )
        destination_sha = await native._family_schema_identity(
            session, manifest["importer_id"], destination_oid, destination_schema, name
        )
        _require(source_schema_sha == destination_sha, "destination table shape differs")
        await session.execute(
            text(
                f"INSERT INTO {_table(destination_schema, name)} SELECT * FROM {_table(prepared.ownership.schema_name, name)} "
                "WHERE run_id=ANY(CAST(:run_ids AS text[]))"
            ),
            {"run_ids": created_ids},
        )
    return created_ids


async def require_pin_guards(session, schema):
    """A scoped seal cannot trust an unguarded shared destination heap."""
    _table(schema, pins.TABLE)
    guarded = await session.scalar(
        text(
            "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid "
            "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:names AS text[])) "
            "AND t.tgname='provider_profile_pinned_run_guard' AND t.tgenabled='O' "
            "AND t.tgtype=31 AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs=0 "
            "AND p.proname='provider_profile_pinned_run_guard' AND p.pronamespace=n.oid"
        ),
        {"schema": schema, "names": list(TABLES)},
    )
    _require(guarded == len(TABLES), "source profile row seal is unavailable")
    truncation_guards = await session.scalar(
        text(
            "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid "
            "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:names AS text[])) "
            "AND t.tgname='provider_profile_pinned_truncate_guard' AND t.tgenabled='O' "
            "AND t.tgtype=34 AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs=0 "
            "AND p.proname='provider_profile_pinned_truncate_guard' AND p.pronamespace=n.oid"
        ),
        {"schema": schema, "names": list(TABLES)},
    )
    _require(truncation_guards == len(TABLES), "source profile truncate seal is unavailable")


async def _pin_group(session, schema, pin_id):
    _require(isinstance(pin_id, UUID), "pin identity is invalid")
    group = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin ORDER BY run_id FOR UPDATE"),
                {"pin": str(pin_id)},
            )
        )
        .mappings()
        .all()
    )
    visible_count = await session.scalar(
        text(f"SELECT count(*) FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin"),
        {"pin": str(pin_id)},
    )
    _require(visible_count == len(group), "pin authority is unavailable")
    return group


def _validate_pin_group(group, importer_id, run_id):
    authority = group[0]["authority_json"]
    run_ids = authority.get("run_ids")
    _validate_run_ids({"run_ids": run_ids, "run_id": run_id, "importer_id": importer_id})
    _require(
        sorted(row["run_id"] for row in group) == sorted(run_ids)
        and all(
            row["source_key"] == SOURCES[importer_id][0]
            and row["authority_json"].get("root_run_id") == run_id
            and row["authority_json"].get("run_ids") == run_ids
            for row in group
        ),
        "pin ownership changed",
    )
    return run_ids


def _validate_adoption_pins(group, manifest, receipt):
    _require(bool(group), "adoption authority changed")
    run_ids = _validate_pin_group(group, manifest["importer_id"], manifest["run_id"])
    _require(
        run_ids == manifest["run_ids"]
        and all(
            row["purpose"] == "adoption"
            and row["authority_json"].get("validation") == receipt
            and type(row["authority_json"].get("created_here")) is bool
            for row in group
        ),
        "adoption authority changed",
    )


async def activate_validated_result(
    session, *, prepared, destination_schema, expected_current_run_id, validation, package_id, sealed_owner_oid, pin_id
):
    """CAS only the scoped pointer after a committed immutable adoption seal."""
    manifest = validate_manifest(prepared.manifest)
    expected = _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected_current_run_id, pin_id)
    _require(validation == expected, "adoption validation changed")
    await _admission(session, manifest, destination_schema, expected_current_run_id)
    await native._lock_family(session, prepared.ownership.schema_name, TABLES, "SHARE", nowait=True)
    await verify_ownership(session, prepared.ownership)
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await require_pin_guards(session, destination_schema)
    _validate_adoption_pins(await _pin_group(session, destination_schema, pin_id), manifest, expected)
    _validate_run(manifest["importer_id"], await _run(session, destination_schema, manifest["run_id"]))
    await session.execute(
        text(
            f"INSERT INTO {_table(destination_schema, 'provider_profile_source_publication')} "
            "(source_key,current_run_id,previous_run_id,published_at) VALUES (:source_key,:run_id,:previous,timezone('UTC',now())) "
            "ON CONFLICT (source_key) DO UPDATE SET current_run_id=EXCLUDED.current_run_id, "
            "previous_run_id=EXCLUDED.previous_run_id,published_at=EXCLUDED.published_at"
        ),
        {"source_key": manifest["source_key"], "run_id": manifest["run_id"], "previous": expected_current_run_id},
    )
    return {
        "source_key": manifest["source_key"],
        "current_run_id": manifest["run_id"],
        "previous_run_id": expected_current_run_id,
        "result_sha256": _digest(manifest),
        "pin_id": str(pin_id),
    }


async def activate_result(
    session, *, prepared, destination_schema, expected_current_run_id, package_id, sealed_owner_oid, pin_id
):
    """Local convenience wrapper; controllers commit long preparation separately."""
    activation_by_field = dict(
        prepared=prepared,
        destination_schema=destination_schema,
        expected_current_run_id=expected_current_run_id,
        package_id=package_id,
        sealed_owner_oid=sealed_owner_oid,
        pin_id=pin_id,
    )
    validation = await prepare_activation(session, **activation_by_field)
    return await activate_validated_result(session, validation=validation, **activation_by_field)


async def rollback_result(session, *, schema, importer_id, expected_current_run_id, expected_previous_run_id):
    """Revalidate the retained local predecessor and swap only this source's pointer."""
    await _source_lock(session, schema, importer_id)
    await native._lock_family(session, schema, TABLES, "SHARE ROW EXCLUSIVE")
    pointer = await _pointer(session, schema, importer_id)
    _require(
        pointer is not None
        and (pointer["current_run_id"], pointer["previous_run_id"])
        == (expected_current_run_id, expected_previous_run_id)
        and expected_previous_run_id is not None,
        "rollback predecessor changed",
    )
    predecessor = await describe_result(
        session, importer_id=importer_id, schema=schema, run_id=expected_previous_run_id
    )
    result_dependencies(predecessor["dependencies"])
    await _restore_pointer(session, schema, importer_id, expected_current_run_id, expected_previous_run_id)


async def rollback_validated_result(
    session, *, schema, importer_id, expected_current_run_id, expected_previous_run_id, pin_id, manifest, package_id
):
    """Restore one retained installation using its bounded local adoption seal."""
    manifest = validate_manifest(manifest)
    _require(
        (manifest["importer_id"], manifest["run_id"]) == (importer_id, expected_previous_run_id),
        "rollback result differs",
    )
    await _admission(session, manifest, schema, expected_current_run_id)
    pointer = await _pointer(session, schema, importer_id)
    _require(
        pointer is not None and pointer["previous_run_id"] == expected_previous_run_id, "rollback predecessor changed"
    )
    await require_pin_guards(session, schema)
    group = await _pin_group(session, schema, pin_id)
    _require(bool(group), "adoption authority changed")
    receipt = group[0]["authority_json"].get("validation", {})
    _validate_adoption_pins(group, manifest, receipt)
    _require(
        receipt.get("contract") == VALIDATION_CONTRACT
        and receipt.get("package_id") == package_id
        and receipt.get("result_sha256") == _digest(manifest)
        and receipt.get("destination_schema") == schema
        and receipt.get("pin_id") == str(pin_id)
        and receipt.get("validation_sha256")
        == _digest({key: field_value for key, field_value in receipt.items() if key != "validation_sha256"}),
        "rollback authority changed",
    )
    await _restore_pointer(session, schema, importer_id, expected_current_run_id, expected_previous_run_id)
    return {
        "source_key": manifest["source_key"],
        "current_run_id": expected_previous_run_id,
        "previous_run_id": expected_current_run_id,
        "pin_id": str(pin_id),
    }


async def _restore_pointer(session, schema, importer_id, expected_current_run_id, expected_previous_run_id):
    await session.execute(
        text(
            f"UPDATE {_table(schema, 'provider_profile_source_publication')} SET current_run_id=:previous, "
            "previous_run_id=:current,published_at=timezone('UTC',now()) WHERE source_key=:source_key"
        ),
        {
            "source_key": SOURCES[importer_id][0],
            "previous": expected_previous_run_id,
            "current": expected_current_run_id,
        },
    )


async def release_source_pin(session, *, schema, importer_id, run_id, pin_id):
    """Release only this local archive/adoption owner; publication still protects history."""
    await _source_lock(session, schema, importer_id)
    group = await _pin_group(session, schema, pin_id)
    if not group:
        return "already_released"
    run_ids = _validate_pin_group(group, importer_id, run_id)
    for selected_id in sorted(run_ids):
        await pins.lock_run(session, schema, selected_id)
    await session.execute(text(f"DELETE FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin"), {"pin": str(pin_id)})
    return "released"


async def cleanup_adoption(session, *, schema, importer_id, run_id, pin_id):
    """Remove only an owned invisible candidate, never current/previous or shared rows."""
    await _source_lock(session, schema, importer_id)
    group = await _pin_group(session, schema, pin_id)
    if not group:
        return "already_released"
    run_ids = _validate_pin_group(group, importer_id, run_id)
    _require(all(row["purpose"] == "adoption" for row in group), "adoption ownership changed")
    for selected_id in sorted(run_ids):
        await pins.lock_run(session, schema, selected_id)
    pointer = await _pointer(session, schema, importer_id)
    if pointer and run_id in (pointer["current_run_id"], pointer["previous_run_id"]):
        return "retained"
    created_ids = [row["run_id"] for row in group if row["authority_json"].get("created_here") is True]
    referenced = await _adoption_referenced(session, schema, created_ids, pin_id)
    await release_source_pin(session, schema=schema, importer_id=importer_id, run_id=run_id, pin_id=pin_id)
    if not referenced:
        for name in reversed(TABLES):
            await session.execute(
                text(f"DELETE FROM {_table(schema, name)} WHERE run_id=ANY(CAST(:run_ids AS text[]))"),
                {"run_ids": created_ids},
            )
    return "released"


async def _adoption_referenced(session, schema, run_ids, pin_id):
    """Retain shared ancestors and any audit referenced by a later local descendant."""
    return await session.scalar(
        text(f"""
        SELECT EXISTS(SELECT 1 FROM {_table(schema, pins.TABLE)}
            WHERE run_id=ANY(CAST(:runs AS text[])) AND pin_id<>:pin)
        OR EXISTS(SELECT 1 FROM {_table(schema, "provider_profile_source_publication")}
            WHERE current_run_id=ANY(CAST(:runs AS text[])) OR previous_run_id=ANY(CAST(:runs AS text[])))
        OR EXISTS(SELECT 1 FROM {_table(schema, TABLES[0])}
            WHERE NOT(run_id=ANY(CAST(:runs AS text[])))
            AND source_manifest->>'reprocess_from'=ANY(CAST(:runs AS text[])))
        """),
        {"runs": run_ids, "pin": str(pin_id)},
    )


async def cleanup_stage(session, ownership):
    """Retire the unchanged owned stage in a fresh cleanup transaction.

    Content validation uses streaming PostgreSQL cursors; its transaction must
    finish before restrictive DDL can retire indexes used by those cursors.
    """
    native._require_transaction(session)
    await native._lock_family(session, ownership.schema_name, TABLES, "ACCESS EXCLUSIVE", nowait=True)
    await verify_ownership(session, ownership)
    await session.execute(
        text("DROP TABLE " + ", ".join(_table(ownership.schema_name, name) for name in TABLES) + " RESTRICT")
    )
    await session.execute(text(f"DROP SCHEMA {native._quoted(ownership.schema_name)} RESTRICT"))
