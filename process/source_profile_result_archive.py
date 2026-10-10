# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Archive completed source assertions and CAS one native publication pointer.

Registry matching is already retained in records/facts. Serving these results
does not read reference tables or repeat matching; captured registry provenance
is preserved as-is. Control attempts and local archive authority are not portable.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

from sqlalchemy import text

from db import models
from process import reference_family_archive as native
from process import source_profile_result_pins as pins
from process.entity_address_snapshot_receipt import _projected_row_identity

CONTRACT = "source-profile-result.postgres.v2"
LEGACY_CONTRACT = "source-profile-result.postgres.v1"
VALIDATION_CONTRACT = "source-profile-result.validation.v2"
LEGACY_VALIDATION_CONTRACT = "source-profile-result.validation.v1"
PROJECTION_IMPORTER = "florida-mqa-profile"
SOURCES = {
    "massachusetts-borim-profile": ("massachusetts-borim", "ma-borim-profile/v1", "MA"),
    "kentucky-kbml-profile": ("kentucky-kbml", "ky-kbml-profile/v1", "KY"),
    "tennessee-tdh-profile": ("tennessee-tdh", "tn-tdh-profile/v1", "TN"),
    "rhode-island-doh-profile": ("rhode-island-doh", "ri-doh-profile/v1", "RI"),
    "new-york-nypp-profile": ("new-york-nypp", "ny-nypp-education/v1", "NY"),
    PROJECTION_IMPORTER: ("florida-mqa", "provider-profile/v1", "FL"),
}
MODELS = (
    models.ProviderProfileImportRun,
    models.ProviderProfileArtifact,
    models.ProviderProfileSourceRecord,
    models.ProviderProfileFact,
)
TABLES = tuple(model.__tablename__ for model in MODELS)
PUBLICATION_TABLES = tuple(name + "_published" for name in TABLES)
STAGE_TABLES = (*TABLES, *PUBLICATION_TABLES)
ATTACHMENT_CONTRACT = "source-profile-attachment.v2"
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_RUN = re.compile(r"(?:[0-9a-f]{32}|[0-9a-f]{64})\Z")
_STAGE = re.compile(r"source_profile_result_[0-9a-f]{32}\Z")
MAX_RUNS = 64
NATIVE_HANDOFF_CONTRACT = "source-profile-native-handoff.v1"
NATIVE_PUBLICATION_CONTRACT = "source-profile-native-publication.v1"
NATIVE_HANDOFF_PHASE = "source profile awaiting protected publication"
NATIVE_MAINTENANCE_PHASE = "source profile published; worker maintenance pending"
NATIVE_CAPTURE_CONTRACT = "source-profile-native-capture.v1"


class SourceProfileArchiveError(RuntimeError):
    """An exact retained result, dependency or destination fence differs."""


def _require(condition, message):
    if not condition:
        raise SourceProfileArchiveError(message)


def source_spec(importer_id, *, publication=False):
    """Return the closed native table family for one supported source."""
    _require(isinstance(importer_id, str) and importer_id in SOURCES, "source profile is unsupported")
    payload = (*MODELS, models.ProviderProfileProjection) if importer_id == PROJECTION_IMPORTER else MODELS
    return native.ReferenceFamilySpec(importer_id, (*payload, *_publication_models()) if publication else payload)


def _publication_models():
    """Alias installed models for disjoint indexed publication heaps in the same owned stage."""
    metadata = native.MetaData()
    aliases = []
    for model, name in zip(MODELS, PUBLICATION_TABLES, strict=True):
        table = model.__table__.to_metadata(metadata, name=name)
        for constraint in table.constraints:
            if constraint.name is not None:
                constraint.name = str(constraint.name) + "_published"
        for index in table.indexes:
            index.name = str(index.name) + "_published"
        aliases.append(SimpleNamespace(__tablename__=name, __table__=table))
    return tuple(aliases)


def publication_spec(importer_id):
    """Use the shared heap/index mechanics for the immutable novel-run subset."""
    source_spec(importer_id)
    return native.ReferenceFamilySpec(importer_id, _publication_models())


def _digest(value):
    return hashlib.sha256(native._canonical_json(value)).hexdigest()


async def is_native_handoff_required(session, schema):
    """An installed protected family must not return to an ordinary pointer writer."""
    native._require_transaction(session)
    return (
        await session.scalar(
            text(
                "SELECT c.relowner=n.nspowner OR NOT pg_has_role(current_user,c.relowner,'USAGE') "
                "FROM pg_class c JOIN pg_namespace n ON n.nspname='hp_snapshot_retention' "
                "WHERE c.oid=to_regclass(:relation)"
            ),
            {"relation": _table(schema, pins.TABLE)},
        )
        is True
    )


async def require_ordinary_publication_authority(session, schema):
    """Keep uncoordinated writers out once the shared family has protected custody."""
    if await is_native_handoff_required(session, schema):
        await native.protected_publisher_owner(session)


def _native_source_contract(handoff, params, manifest):
    return _digest(
        {
            "importer_id": handoff["importer_id"],
            "run_id": handoff["run_id"],
            "attempt_id": handoff["attempt_id"],
            "source_run_id": handoff["source_run_id"],
            "source_manifest": manifest,
            "params": params,
        }
    )


def native_pointer_identity(pointer):
    """Publication timestamps are reporting, not a portable compare-and-swap token."""
    return {key: value for key, value in (pointer or {}).items() if key != "published_at"}


def _validate_native_pointer(pointer, importer):
    fields = {"current_run_id", "previous_run_id"}
    if importer == PROJECTION_IMPORTER:
        fields.update(("current_relation_oid", "previous_relation_oid"))
    _require(
        isinstance(pointer, dict) and (not pointer or set(pointer) == fields), "source native predecessor shape differs"
    )
    for key, value in pointer.items():
        if key.endswith("_run_id"):
            _require(
                value is None or isinstance(value, str) and bool(_RUN.fullmatch(value)),
                "source native predecessor run differs",
            )
        else:
            _require(value is None or type(value) is int and 0 < value < 2**32, "source native predecessor OID differs")
            _require(
                pointer[key.replace("relation_oid", "run_id")] is None or value is not None,
                "source native predecessor OID is missing",
            )


def _validate_native_projection(handoff):
    if handoff["importer_id"] != PROJECTION_IMPORTER:
        _require(handoff["projection"] is None, "source native projection scope differs")
        return
    projection = handoff["projection"]
    _require(
        isinstance(projection, dict) and set(projection) == {"relation_oid", "owner_oid", "table_name", "row_count"},
        "source native projection shape differs",
    )
    _require(
        projection["table_name"] == "provider_profile_projection_" + handoff["source_run_id"][:16]
        and all(type(projection[key]) is int and 0 < projection[key] < 2**32 for key in ("relation_oid", "owner_oid"))
        and type(projection["row_count"]) is int
        and projection["row_count"] > 0,
        "source native projection identity differs",
    )


def validate_native_handoff(handoff):
    """Decode only the bounded, closed producer/attempt/location contract."""
    fields = {
        "contract",
        "importer_id",
        "run_id",
        "source_run_id",
        "attempt_id",
        "attempt_started_at",
        "schema_name",
        "node_id",
        "database_oid",
        "import_run_oid",
        "source_contract_sha256",
        "source_manifest_sha256",
        "expected",
        "metrics",
        "projection",
        "handoff_sha256",
    }
    _require(isinstance(handoff, dict) and set(handoff) == fields, "source native handoff shape differs")
    _require(handoff["contract"] == NATIVE_HANDOFF_CONTRACT, "source native handoff contract differs")
    source_spec(handoff["importer_id"])
    native._schema_name(handoff["schema_name"])
    _require(
        all(
            isinstance(handoff[key], str) and 0 < len(handoff[key]) <= 128
            for key in ("run_id", "attempt_id", "attempt_started_at", "node_id")
        )
        and isinstance(handoff["source_run_id"], str)
        and bool(_RUN.fullmatch(handoff["source_run_id"]))
        and all(type(handoff[key]) is int and 0 < handoff[key] < 2**32 for key in ("database_oid", "import_run_oid"))
        and all(
            isinstance(handoff[key], str) and bool(_HEX.fullmatch(handoff[key]))
            for key in ("source_contract_sha256", "source_manifest_sha256", "handoff_sha256")
        )
        and isinstance(handoff["metrics"], dict)
        and (handoff["projection"] is not None) == (handoff["importer_id"] == PROJECTION_IMPORTER),
        "source native handoff identity differs",
    )
    _validate_native_pointer(handoff["expected"], handoff["importer_id"])
    _validate_native_projection(handoff)
    if handoff["importer_id"] == "new-york-nypp-profile" and "bundle" in handoff["metrics"]:
        from process.new_york_profile_store import validate_bundle_reference

        validate_bundle_reference(handoff["metrics"]["bundle"], handoff["source_run_id"])
        _require("nysed_support" not in handoff["metrics"], "source native metrics are not compact")
    _require(len(native._canonical_json(handoff)) <= 393_216, "source native handoff exceeds its bound")
    _require(
        _digest({key: field_value for key, field_value in handoff.items() if key != "handoff_sha256"})
        == handoff["handoff_sha256"],
        "source native handoff digest differs",
    )
    return handoff


async def _native_control_run(session, handoff, statuses):
    row = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(handoff['schema_name'], 'import_run')} WHERE run_id=:run_id FOR UPDATE"),
                handoff,
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        row is not None
        and row["importer"] == handoff["importer_id"]
        and row["engine"] == "healthcare-mrf-api"
        and row["status"] in statuses
        and (row["progress"] or {}).get("attempt_id") == handoff["attempt_id"]
        and (row["progress"] or {}).get("attempt_started_at") == handoff["attempt_started_at"]
        and row["finished_at"] is None
        and row["error"] is None,
        "source native control attempt differs",
    )
    return row


async def _native_source_run(session, handoff):
    row = (
        (
            await session.execute(
                text(
                    f"SELECT * FROM {_table(handoff['schema_name'], TABLES[0])} WHERE run_id=:source_run_id FOR UPDATE"
                ),
                handoff,
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        row is not None
        and row["status"] in {"running", "validating"}
        and tuple(row[key] for key in ("source_key", "schema_version", "jurisdiction"))
        == SOURCES[handoff["importer_id"]]
        and isinstance(row["source_manifest"], dict)
        and row["source_manifest"].get("control_run_id") == handoff["run_id"]
        and row["finished_at"] is None
        and row["error"] is None,
        "source native producer differs",
    )
    return row


async def record_native_handoff(session, ctx, *, importer_id, schema, source_run_id, metrics, projection=None):
    """CAS a genuine ordinary attempt into durable Publisher-owned finalization."""
    context = (ctx or {}).get("context") or {}
    handoff_by_field = {
        "contract": NATIVE_HANDOFF_CONTRACT,
        "importer_id": importer_id,
        "run_id": context.get("control_run_id") or (ctx or {}).get("control_run_id"),
        "source_run_id": source_run_id,
        "attempt_id": context.get("_control_attempt_id"),
        "attempt_started_at": context.get("_control_attempt_started_at"),
        "schema_name": schema,
    }
    _require(
        all(
            isinstance(handoff_by_field[key], str) and handoff_by_field[key]
            for key in ("run_id", "attempt_id", "attempt_started_at")
        ),
        "source native managed attempt is required",
    )
    run = await _native_control_run(session, handoff_by_field, ("running",))
    await _source_lock(session, schema, importer_id)
    source_run = await _native_source_run(session, handoff_by_field)
    handoff_by_field.update(
        node_id=run["node_id"],
        metrics=dict(metrics),
        projection=projection,
        expected=native_pointer_identity(await _pointer(session, schema, importer_id)),
        database_oid=await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")),
        import_run_oid=await native._relation_oid(session, schema, "import_run"),
        source_manifest_sha256=_digest(source_run["source_manifest"]),
        source_contract_sha256=_native_source_contract(handoff_by_field, run["params"], source_run["source_manifest"]),
    )
    handoff_by_field["handoff_sha256"] = _digest(handoff_by_field)
    validate_native_handoff(handoff_by_field)
    changed = await session.scalar(
        text(
            f"UPDATE {_table(schema, 'import_run')} SET status='finalizing',phase_detail=:phase,heartbeat_at=clock_timestamp(),"
            "metrics=(COALESCE(metrics::jsonb,'{}'::jsonb)||jsonb_build_object('source_profile_handoff',CAST(:handoff AS jsonb)))::json "
            "WHERE run_id=:run_id AND status='running' AND metrics->'source_profile_handoff' IS NULL RETURNING run_id"
        ),
        {
            "run_id": handoff_by_field["run_id"],
            "phase": NATIVE_HANDOFF_PHASE,
            "handoff": native._canonical_json(handoff_by_field).decode("ascii"),
        },
    )
    _require(changed == handoff_by_field["run_id"], "source native handoff attempt changed")
    return handoff_by_field


async def require_native_handoff(session, value):
    """Reauthenticate the persisted attempt, actual database, and frozen producer inputs."""
    handoff = validate_native_handoff(value)
    run = await _native_control_run(session, handoff, ("finalizing",))
    await _source_lock(session, handoff["schema_name"], handoff["importer_id"])
    source = await _native_source_run(session, handoff)
    _require(
        run["phase_detail"] == NATIVE_HANDOFF_PHASE
        and run["node_id"] == handoff["node_id"]
        and (run["metrics"] or {}).get("source_profile_handoff") == handoff
        and _digest(source["source_manifest"]) == handoff["source_manifest_sha256"]
        and _native_source_contract(handoff, run["params"], source["source_manifest"])
        == handoff["source_contract_sha256"]
        and await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
        == handoff["database_oid"]
        and await native._relation_oid(session, handoff["schema_name"], "import_run") == handoff["import_run_oid"],
        "source native handoff binding differs",
    )
    return handoff


async def reconcile_native_handoff(database, handoff):
    """Read uncertain producer COMMIT on a fresh connection; absence alone permits failure."""

    async def is_handoff_persisted():
        """A fresh locked read resolves only this exact attempt's uncertain COMMIT."""
        async with asyncio.timeout(40), database.transaction() as session:
            await session.execute(text("SET LOCAL lock_timeout='30s'"))
            row = (
                (
                    await session.execute(
                        text(
                            f"SELECT metrics FROM {_table(handoff['schema_name'], 'import_run')} WHERE run_id=:run_id FOR SHARE"
                        ),
                        handoff,
                    )
                )
                .mappings()
                .one_or_none()
            )
            return row is not None and (row["metrics"] or {}).get("source_profile_handoff") == handoff

    task = asyncio.create_task(is_handoff_persisted())
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                raise


async def handoff_native_publication(database, ctx, **arguments):
    """Fence wrapper failure and cleanup against committed or uncertain handoff custody."""
    pins.load_role_policy()
    handoff = None
    try:
        async with asyncio.timeout(8), database.transaction() as session:
            handoff = await record_native_handoff(session, ctx, **arguments)
    except BaseException:
        if handoff is None:
            raise
        try:
            persisted = await reconcile_native_handoff(database, handoff)
        except BaseException:
            ctx.setdefault("context", {})["source_profile_commit_unknown"] = True
            raise
        if not persisted:
            raise
    context = ctx.setdefault("context", {})
    result_by_field = {**arguments["metrics"], "source_profile_handoff": handoff}
    context["_control_committed_result"] = result_by_field
    context["control_run_handoff_committed"] = True
    return result_by_field


def _native_completion(importer_id):
    """Reuse the six producers' actual completion policies, not an archive approximation."""
    from importlib import import_module

    binding_by_importer = {
        "massachusetts-borim-profile": ("massachusetts_profile_completion", "_completion"),
        "kentucky-kbml-profile": ("kentucky_profile_completion", "_completion"),
        "tennessee-tdh-profile": ("tennessee_profile_store", "completion"),
        "rhode-island-doh-profile": ("rhode_island_profile_store", "completion"),
        "new-york-nypp-profile": ("new_york_profile_store", "completion"),
    }
    _require(importer_id in binding_by_importer, "source native completion is unsupported")
    module, name = binding_by_importer[importer_id]
    return getattr(import_module("process." + module), name)


async def _finish_native_source(session, handoff):
    """Provisional source completion remains invisible until the Publisher's outer COMMIT."""
    run_by_field = dict(await _native_source_run(session, handoff))
    from process.entity_address_snapshot_preparation import _seal_published_relation

    await _seal_published_relation(
        session,
        await native._relation_oid(session, handoff["schema_name"], "provider_profile_source_publication"),
        await native.protected_publisher_owner(session),
    )
    if handoff["importer_id"] != PROJECTION_IMPORTER:
        completion = _native_completion(handoff["importer_id"])
        _require(run_by_field["source_manifest"]["max_providers"] is None, "source native bounded publication differs")
        async with models.db.bind_existing_session(session):
            completed = await completion._finish_source(run_by_field, handoff["metrics"])
        return completed, completion._terminal_progress(completed)
    return await _finish_native_projection(session, handoff)


async def _finish_native_projection(session, handoff):
    """Validate the existing sealed heap and finish its real producer before source capture."""
    from process import florida_projection_archive as projection
    from process.entity_address_snapshot_preparation import _seal_published_relation

    seal = handoff["projection"]
    await native._lock_family(session, handoff["schema_name"], (seal["table_name"],), "SHARE", nowait=True)
    _require(
        await projection._projection_candidate_seal(
            session, handoff["schema_name"], seal["table_name"], seal["owner_oid"]
        )
        == {key: seal[key] for key in ("relation_oid", "owner_oid", "table_name")},
        "source native projection changed",
    )
    await _seal_published_relation(session, seal["relation_oid"], await native.protected_publisher_owner(session))
    result_by_field = {
        **handoff["metrics"],
        "published_providers": seal["row_count"],
        "publication": {
            "publication": "atomic_table_swap",
            "published_rows": seal["row_count"],
            "stage_table": f"{handoff['schema_name']}.{seal['table_name']}",
            "published_table": f"{handoff['schema_name']}.{projection.PROJECTION}",
        },
    }
    await session.execute(
        text(
            f"UPDATE {_table(handoff['schema_name'], TABLES[0])} SET status='completed',error=NULL,"
            "finished_at=timezone('UTC',transaction_timestamp()),metrics=CAST(:metrics AS json) WHERE run_id=:run"
        ),
        {"run": handoff["source_run_id"], "metrics": native._canonical_json(result_by_field).decode("ascii")},
    )
    candidate = await _run(session, handoff["schema_name"], handoff["source_run_id"])
    previous_id = handoff["expected"]["current_run_id"]
    previous = await _run(session, handoff["schema_name"], previous_id) if previous_id else None
    result_by_field["publication"].update(projection.native_publication_policy(candidate, previous))
    await session.execute(
        text(
            f"UPDATE {_table(handoff['schema_name'], TABLES[0])} SET metrics=CAST(:metrics AS json) WHERE run_id=:run"
        ),
        {"run": handoff["source_run_id"], "metrics": native._canonical_json(result_by_field).decode("ascii")},
    )
    return result_by_field, {
        "unit": "providers",
        "done": seal["row_count"],
        "total": seal["row_count"],
        "pct": 100,
        "phase": "florida-mqa-profile published",
        "message": "succeeded",
    }


async def _capture_native_source(session, handoff, dataset_id, source_copy, owner_oid):
    """Capture ordinary canonical rows with the shared model COPY/index/set lifecycle."""
    importer, schema = handoff["importer_id"], handoff["schema_name"]
    runs = await _lineage(session, importer, schema, handoff["source_run_id"])
    run_ids = [run["run_id"] for run in runs]
    spec = source_spec(importer)
    await native._create_model_family(session, spec, stage_schema(dataset_id), create_indexes=False)
    ownership = await capture_ownership(session, importer, dataset_id)
    source_models = list(spec.model_types)
    if importer == PROJECTION_IMPORTER:
        source_models[-1] = SimpleNamespace(
            __tablename__=handoff["projection"]["table_name"], __table__=models.ProviderProfileProjection.__table__
        )
    await native._copy_model_run_scope(
        session,
        native.ReferenceFamilySpec(importer, tuple(source_models)),
        source_schema=schema,
        target_schema=ownership.schema_name,
        target_names=spec.table_names,
        run_scope=(
            tuple(
                "generation_id" if model is models.ProviderProfileProjection else "run_id" for model in spec.model_types
            ),
            run_ids,
        ),
        source_copy=source_copy,
        deadline=asyncio.get_running_loop().time() + source_copy.timeout,
    )
    await complete_restore(session, ownership)
    quoted_owner = await session.scalar(
        text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"), {"oid": owner_oid}
    )
    _require(isinstance(quoted_owner, str), "source native owner differs")
    await session.execute(text(f"ALTER SCHEMA {native._quoted(ownership.schema_name)} OWNER TO {quoted_owner}"))
    await native.seal_model_family_storage(session, ownership, owner_oid)
    manifest = await describe_result(
        session, importer_id=importer, schema=ownership.schema_name, run_id=handoff["source_run_id"], contract=CONTRACT
    )
    prepared = PreparedResult(manifest, ownership)
    await validate_stage(session, ownership, manifest)
    for model, source_model in zip(spec.model_types, source_models, strict=True):
        _require(
            await _are_source_model_rows_equal(
                session,
                importer,
                model,
                (schema, source_model.__tablename__),
                (ownership.schema_name, model.__tablename__),
                run_ids,
            ),
            "source native candidate content differs",
        )
    return prepared


async def _prepare_witnessed_source(session, handoff, dataset_id, source_copy, owner_oid):
    """Turn a genuine local producer envelope into the existing immutable model family."""
    from process import new_york_profile_store as producer

    run_by_field = dict(await _native_source_run(session, handoff))
    await producer.read_witness_bundle(session, handoff["schema_name"], run_by_field, handoff["metrics"]["bundle"])
    await _require_local_source_envelope(session, handoff)
    ownership = await precreate_restore(session, handoff["importer_id"], dataset_id, contract=CONTRACT)
    deadline = asyncio.get_running_loop().time() + source_copy.timeout
    remaining = await _load_witnessed_source(session, handoff, ownership, source_copy, deadline)
    await complete_restore(session, ownership)
    for name in TABLES:
        await session.execute(text(f"ANALYZE {_table(ownership.schema_name, name)}"))
    source_result = await _complete_witnessed_source(session, handoff, ownership.schema_name, run_by_field)
    manifest = await describe_result(
        session,
        importer_id=handoff["importer_id"],
        schema=ownership.schema_name,
        run_id=handoff["source_run_id"],
        contract=CONTRACT,
    )
    prepared = PreparedResult(manifest, ownership)
    await _remove_local_source_envelope(session, handoff)
    publication, remaining = await _prepare_publication(
        session,
        prepared,
        handoff["schema_name"],
        source_copy=native.ReferenceFamilySourceCopy(source_copy.copy_rows, remaining, source_copy.timeout),
        deadline=deadline,
    )
    owner_name = await session.scalar(
        text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"), {"oid": owner_oid}
    )
    _require(isinstance(owner_name, str), "source native owner differs")
    await session.execute(text(f"ALTER SCHEMA {native._quoted(ownership.schema_name)} OWNER TO {owner_name}"))
    await native.seal_model_family_storage(session, ownership, owner_oid)
    await validate_stage(session, ownership, manifest)
    await _validate_publication_sets(session, prepared, publication)
    await _attach_publication(session, prepared, publication, handoff["schema_name"])
    return prepared, source_result, publication


async def _require_local_source_envelope(session, handoff):
    """The new producer writes only its actual parent-local run and artifact envelope."""
    schema = handoff["schema_name"]
    for name in TABLES[2:]:
        _require(
            not await session.scalar(
                text(f"SELECT EXISTS(SELECT 1 FROM {_table(schema, name)} WHERE run_id=:source_run_id)"), handoff
            ),
            "source native envelope has ordinary payload",
        )
    for name in TABLES[:2]:
        _require(
            await session.scalar(
                text(
                    f"SELECT count(*)=1 AND bool_and(tableoid=CAST(:relation AS regclass)) "
                    f"FROM {_table(schema, name)} WHERE run_id=:source_run_id"
                ),
                {**handoff, "relation": _table(schema, name)},
            )
            is True,
            "source native envelope physical owner differs",
        )
    _require(
        not await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {_table(schema, pins.TABLE)} WHERE run_id=:source_run_id)"), handoff
        ),
        "source native envelope is already sealed",
    )


async def _load_witnessed_source(session, handoff, ownership, source_copy, deadline):
    """Debit one bounded binary COPY budget for both envelope and model-valued source."""
    from process.new_york_profile_store import witness_projection

    schema, run_id = handoff["schema_name"], handoff["source_run_id"]
    remaining = await native._copy_model_run_scope(
        session,
        native.ReferenceFamilySpec(handoff["importer_id"], MODELS[:2]),
        source_schema=schema,
        target_schema=ownership.schema_name,
        target_names=TABLES[:2],
        run_scope=(("run_id", "run_id"), [run_id]),
        source_copy=source_copy,
        deadline=deadline,
    )
    for model in MODELS[2:]:
        remaining = await native._copy_source_projection(
            session,
            source_copy,
            witness_projection(ownership.schema_name, run_id, model, ownership.schema_name),
            ownership.schema_name,
            model.__tablename__,
            tuple(column.name for column in model.__table__.columns),
            remaining,
            deadline,
        )
    return remaining


async def _complete_witnessed_source(session, handoff, schema, run):
    """Compute real candidate metrics before stamping its isolated completion metadata."""
    from process import new_york_profile_store as producer

    artifact, bundle = await producer.read_witness_bundle(session, schema, run, handoff["metrics"]["bundle"])
    counts = await producer.native_witness_counts(session, schema, run, bundle)
    counts["bundle_reference"] = producer.bundle_reference(artifact)
    metrics = producer.store._completion_metrics(run, handoff["metrics"], counts)
    incumbent = handoff["expected"].get("current_run_id")
    incumbent_metrics = None
    if incumbent is not None:
        _require(
            await session.scalar(
                text(
                    f"SELECT EXISTS(SELECT 1 FROM {_table(handoff['schema_name'], pins.TABLE)} "
                    "WHERE run_id=:run AND source_key=:source AND purpose='adoption')"
                ),
                {"run": incumbent, "source": producer.SOURCE_KEY},
            ),
            "source native incumbent custody is unavailable",
        )
        incumbent_metrics = (await producer.store._retained_counts_by_run([incumbent], session=session))[incumbent]
        incumbent_metrics["acquired_profiles"] = incumbent_metrics["retained_source_records"]
    producer.store._publication_volume(metrics, incumbent_metrics)
    completed_by_field = {**metrics, "published": True}
    finished = await session.scalar(
        text(
            f"UPDATE {_table(schema, TABLES[0])} SET status='completed',finished_at=clock_timestamp(),"
            "metrics=CAST(:metrics AS json),error=NULL WHERE run_id=:run RETURNING finished_at"
        ),
        {"run": run["run_id"], "metrics": native._canonical_json(completed_by_field).decode("ascii")},
    )
    await session.execute(
        text(f"UPDATE {_table(schema, TABLES[3])} SET published_at=:finished WHERE run_id=:run"),
        {"run": run["run_id"], "finished": finished},
    )
    return {**completed_by_field, "run_id": run["run_id"], "previous_run_id": incumbent}


async def _remove_local_source_envelope(session, handoff):
    """Replace only authenticated, never-published metadata in the owning transaction."""
    await _native_source_run(session, handoff)
    await _require_local_source_envelope(session, handoff)
    for name in reversed(TABLES[:2]):
        removed = (
            (
                await session.execute(
                    text(
                        f"DELETE FROM ONLY {_table(handoff['schema_name'], name)} "
                        "WHERE run_id=:source_run_id RETURNING run_id"
                    ),
                    handoff,
                )
            )
            .scalars()
            .all()
        )
        _require(removed == [handoff["source_run_id"]], "source native envelope changed")


async def complete_native_handoff(session, handoff, *, dataset_id, source_copy, publication_continuation):
    """Validate the real ordinary producer, then atomically register, bind and finish it."""
    native._require_transaction(session)
    _require(isinstance(dataset_id, UUID) and callable(publication_continuation), "source native continuation differs")
    _require(isinstance(source_copy, native.ReferenceFamilySourceCopy), "source native COPY capability is required")
    handoff = await require_native_handoff(session, handoff)
    schema, importer = handoff["schema_name"], handoff["importer_id"]
    owner_oid = await native.protected_publisher_owner(session)
    await _source_lock(session, schema, importer)
    _require(
        native_pointer_identity(await _pointer(session, schema, importer)) == handoff["expected"],
        "source native predecessor changed",
    )
    await native._lock_family(session, schema, TABLES, "SHARE")
    await require_pin_guards(session, schema)
    publication = None
    if handoff["importer_id"] == "new-york-nypp-profile" and "bundle" in handoff["metrics"]:
        prepared, source_result, publication = await _prepare_witnessed_source(
            session, handoff, dataset_id, source_copy, owner_oid
        )
        progress = _native_completion(importer)._terminal_progress(source_result)
    else:
        source_result, progress = await _finish_native_source(session, handoff)
        prepared = await _capture_native_source(session, handoff, dataset_id, source_copy, owner_oid)
    receipt_by_field = {
        "contract": NATIVE_PUBLICATION_CONTRACT,
        "handoff": handoff,
        "pin_id": str(dataset_id),
        "ownership": ownership_dict(prepared.ownership),
        "result": prepared.manifest,
        "sealed_owner_oid": owner_oid,
        "source_result": source_result,
        "terminal_progress": progress,
        **({"publication": publication, "destination_schema": schema} if publication is not None else {}),
        **await _native_receipt_witnesses(session, schema, prepared.manifest["run_ids"]),
    }
    receipt_by_field["validation_sha256"] = _digest(receipt_by_field)
    await _record_native_custody(session, handoff, receipt_by_field)
    transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
    completed_ids = []

    async def cutover():
        """Consume this transaction's validated source and final attempt exactly once."""
        _require(
            not completed_ids
            and session.in_transaction()
            and await session.scalar(text("SELECT pg_current_xact_id()::text")) == transaction_id,
            "source native publication transaction changed",
        )
        completed_ids.append(transaction_id)
        return await _publish_native_serving(session, prepared, receipt_by_field)

    publication = await publication_continuation(session, receipt_by_field, cutover)
    _require(completed_ids, "source native publication was not completed")
    return publication


async def _publish_native_serving(session, prepared, receipt):
    """Change the genuine serving pointer and exact managed attempt in the owning transaction."""
    handoff = receipt["handoff"]
    schema, owner_oid = handoff["schema_name"], receipt["sealed_owner_oid"]
    if handoff["importer_id"] == PROJECTION_IMPORTER:
        from process.florida_projection_archive import publish_retained_projection

        await publish_retained_projection(
            session, schema, handoff["projection"], handoff["expected"], handoff["source_run_id"], owner_oid
        )
    elif "publication" in receipt:
        await _publish_result_pointer(
            session,
            prepared,
            {
                "destination_schema": schema,
                "sealed_owner_oid": owner_oid,
                "expected_current_run_id": handoff["expected"].get("current_run_id"),
            },
            retained_serving=True,
        )
    await _finish_native_attempt(session, receipt)
    return receipt


async def _native_receipt_witnesses(session, schema, run_ids):
    """Bind the complete captured source graph to physical parents in this transaction."""
    return {
        "publisher_transaction_id": await session.scalar(text("SELECT pg_current_xact_id()::text")),
        "parents": [[name, await native._relation_oid(session, schema, name)] for name in TABLES],
        "ancestor_relations": [[run_id, await _ancestor_relations(session, schema, run_id)] for run_id in run_ids],
    }


async def _record_native_custody(session, producer, receipt):
    """Pin existing canonical rows without claiming the Publisher created their payload."""
    for run_id in sorted(receipt["result"]["run_ids"]):
        await pins.record_pin(
            session,
            schema=producer["schema_name"],
            source_key=SOURCES[producer["importer_id"]][0],
            run_id=run_id,
            pin_id=UUID(receipt["pin_id"]),
            purpose="adoption",
            authority={
                "validation": receipt,
                "root_run_id": producer["source_run_id"],
                "run_ids": receipt["result"]["run_ids"],
                "created_here": run_id in receipt.get("publication", {}).get("created_run_ids", ()),
            },
        )


def validate_native_capture(capture):
    """First-use capture has a new identity and never claims a historical control attempt."""
    _require(
        isinstance(capture, dict)
        and set(capture)
        == {
            "contract",
            "importer_id",
            "schema_name",
            "source_run_id",
            "expected",
            "database_oid",
            "source_manifest_sha256",
        }
        and capture["contract"] == NATIVE_CAPTURE_CONTRACT,
        "source native capture shape differs",
    )
    source_spec(capture["importer_id"])
    native._schema_name(capture["schema_name"])
    _validate_native_pointer(capture["expected"], capture["importer_id"])
    _require(
        isinstance(capture["source_run_id"], str)
        and bool(_RUN.fullmatch(capture["source_run_id"]))
        and isinstance(capture["expected"], dict)
        and capture["expected"].get("current_run_id") == capture["source_run_id"]
        and type(capture["database_oid"]) is int
        and 0 < capture["database_oid"] < 2**32
        and isinstance(capture["source_manifest_sha256"], str)
        and bool(_HEX.fullmatch(capture["source_manifest_sha256"])),
        "source native capture identity differs",
    )
    _require(len(native._canonical_json(capture)) <= 393_216, "source native capture exceeds its bound")
    return capture


async def capture_native_incumbent(session, capture, *, dataset_id, source_copy, publication_continuation):
    """Authenticate a real incumbent and preserve its original serving heap at first use."""
    capture = validate_native_capture(capture)
    schema, importer = capture["schema_name"], capture["importer_id"]
    _require(isinstance(dataset_id, UUID) and callable(publication_continuation), "source capture continuation differs")
    _require(isinstance(source_copy, native.ReferenceFamilySourceCopy), "source capture COPY capability is required")
    owner_oid = await native.protected_publisher_owner(session)
    await _source_lock(session, schema, importer)
    await native._lock_family(session, schema, source_spec(importer).table_names, "SHARE", nowait=True)
    await require_pin_guards(session, schema)
    _require(
        native_pointer_identity(await _pointer(session, schema, importer)) == capture["expected"],
        "source capture predecessor changed",
    )
    source_run = await _require_captured_source(session, capture)
    serving_by_field = None
    if importer == PROJECTION_IMPORTER:
        from process.florida_projection_archive import PROJECTION

        serving_by_field = {"table_name": PROJECTION, "relation_oid": capture["expected"]["current_relation_oid"]}
    prepared = await _capture_native_source(
        session, {**capture, "projection": serving_by_field}, dataset_id, source_copy, owner_oid
    )
    receipt_by_field = {
        "contract": NATIVE_CAPTURE_CONTRACT,
        "capture": capture,
        "pin_id": str(dataset_id),
        "ownership": ownership_dict(prepared.ownership),
        "result": prepared.manifest,
        "serving": serving_by_field,
        "sealed_owner_oid": owner_oid,
        "source_contract_sha256": _digest(
            {"capture": capture, "source_manifest": source_run["source_manifest"], "result": prepared.manifest}
        ),
        **await _native_receipt_witnesses(session, schema, prepared.manifest["run_ids"]),
    }
    receipt_by_field["validation_sha256"] = _digest(receipt_by_field)
    completed_ids = []

    async def seal():
        """Fence the actual live heap and pointer before its first retained registration."""
        _require(
            not completed_ids
            and session.in_transaction()
            and await session.scalar(text("SELECT pg_current_xact_id()::text"))
            == receipt_by_field["publisher_transaction_id"],
            "source capture transaction changed",
        )
        completed_ids.append(str(dataset_id))
        await _seal_captured_incumbent(session, receipt_by_field)

    publication = await publication_continuation(session, receipt_by_field, seal)
    _require(completed_ids, "source capture was not published")
    return publication


async def _require_captured_source(session, capture):
    """The new custody identity must still name this database's actual completed producer."""
    source_run = await _run(session, capture["schema_name"], capture["source_run_id"])
    _validate_run(capture["importer_id"], source_run)
    _require(
        _digest(source_run["source_manifest"]) == capture["source_manifest_sha256"]
        and await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
        == capture["database_oid"],
        "source capture producer changed",
    )
    return source_run


async def _seal_captured_incumbent(session, receipt):
    """Keep serving identity and existing reads while closing ordinary publication bypasses."""
    from process.entity_address_snapshot_preparation import _seal_published_relation

    capture = receipt["capture"]
    schema, importer = capture["schema_name"], capture["importer_id"]
    _require(
        native_pointer_identity(await _pointer(session, schema, importer)) == capture["expected"],
        "source capture predecessor changed",
    )
    if importer == PROJECTION_IMPORTER:
        from process.florida_projection_archive import seal_retained_projection

        await seal_retained_projection(session, schema, receipt["serving"], receipt["sealed_owner_oid"])
    await _seal_published_relation(
        session,
        await native._relation_oid(session, schema, "provider_profile_source_publication"),
        receipt["sealed_owner_oid"],
    )
    await _record_native_custody(session, capture, receipt)


async def _finish_native_attempt(session, receipt):
    handoff = receipt["handoff"]
    changed = await session.scalar(
        text(
            f"UPDATE {_table(handoff['schema_name'], 'import_run')} SET error=NULL,"
            "heartbeat_at=timezone('UTC',transaction_timestamp()),"
            "phase_detail=:phase,progress=(progress::jsonb||CAST(:progress AS jsonb))::json,"
            "metrics=(metrics::jsonb||CAST(:result AS jsonb)||jsonb_build_object('source_profile_native_publication',"
            "CAST(:receipt AS jsonb)))::json WHERE run_id=:run_id AND importer=:importer_id AND node_id=:node_id "
            "AND status='finalizing' AND phase_detail=:handoff_phase AND finished_at IS NULL "
            "AND (error IS NULL OR error::jsonb='null'::jsonb) "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            "AND metrics::jsonb->'source_profile_handoff'=CAST(:handoff AS jsonb) RETURNING run_id"
        ),
        {
            **handoff,
            "phase": NATIVE_MAINTENANCE_PHASE,
            "handoff_phase": NATIVE_HANDOFF_PHASE,
            "progress": native._canonical_json(
                {**receipt["terminal_progress"], "phase": NATIVE_MAINTENANCE_PHASE, "message": "maintenance pending"}
            ).decode("ascii"),
            "result": native._canonical_json(receipt["source_result"]).decode("ascii"),
            "receipt": native._canonical_json(receipt).decode("ascii"),
            "handoff": native._canonical_json(handoff).decode("ascii"),
        },
    )
    _require(changed == handoff["run_id"], "source native completion attempt changed")


def is_native_published_attempt(run):
    """Recognize only the exact published-pending phase or its terminal completion."""
    return run.get("error") is None and (
        run.get("status") == "finalizing"
        and run.get("phase_detail") == NATIVE_MAINTENANCE_PHASE
        and run.get("finished_at") is None
        or run.get("status") == "succeeded"
        and run.get("finished_at") is not None
    )


async def require_native_maintenance(session, run):
    """Authenticate sealed source custody, not a worker-supplied completion marker."""
    native._require_transaction(session)
    metrics_by_field = run.get("metrics") or {}
    receipt = metrics_by_field.get("source_profile_native_publication")
    _require(isinstance(receipt, dict), "source native publication is unavailable")
    handoff = validate_native_handoff(receipt.get("handoff"))
    manifest = validate_manifest(receipt["result"])
    schema, importer = handoff["schema_name"], handoff["importer_id"]
    _require(
        is_native_published_attempt(run)
        and receipt["contract"] == NATIVE_PUBLICATION_CONTRACT
        and receipt["result"]["importer_id"] == importer
        and receipt["result"]["run_id"] == handoff["source_run_id"]
        and receipt["validation_sha256"]
        == _digest({key: field_value for key, field_value in receipt.items() if key != "validation_sha256"})
        and run["engine"] == "healthcare-mrf-api"
        and all(
            run[key] == handoff[field]
            for key, field in (("run_id", "run_id"), ("node_id", "node_id"), ("importer", "importer_id"))
        )
        and metrics_by_field.get("source_profile_handoff") == handoff
        and all((run.get("progress") or {}).get(key) == handoff[key] for key in ("attempt_id", "attempt_started_at"))
        and all(metrics_by_field.get(key) == field_value for key, field_value in receipt["source_result"].items()),
        "source native maintenance attempt differs",
    )
    await _source_lock(session, schema, importer)
    await _require_native_maintenance_source(session, run, receipt)
    await _require_native_maintenance_authority(session, schema, receipt["sealed_owner_oid"])
    group = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin ORDER BY run_id LIMIT 65"),
                {"pin": receipt["pin_id"]},
            )
        )
        .mappings()
        .all()
    )
    _validate_adoption_pins(group, manifest, receipt)
    await _require_maintenance_storage(session, schema, receipt)
    await require_pin_guards(session, schema)
    if importer == PROJECTION_IMPORTER:
        from process.florida_projection_archive import _retained_run_projection_oid

        pointer = await _pointer(session, schema, importer)
        oid = await _retained_run_projection_oid(
            session, schema, handoff["source_run_id"], serving=pointer["current_run_id"] == handoff["source_run_id"]
        )
        _require(
            oid == handoff["projection"]["relation_oid"]
            and await session.scalar(text("SELECT relowner FROM pg_class WHERE oid=:oid"), {"oid": oid})
            == receipt["sealed_owner_oid"],
            "source native maintenance serving heap differs",
        )
    return receipt


async def _require_maintenance_storage(session, schema, receipt):
    """Authenticate either unattached capture custody or exact Publisher-created model leaves."""
    ownership = _publication_ownership(receipt)
    await verify_ownership(session, ownership)
    await native._verify_stage_owner(session, ownership, receipt["sealed_owner_oid"])
    await _require_stage_topology(session, ownership, receipt.get("publication"))
    if "publication" in receipt:
        await _require_publication_parents(session, schema, receipt["publication"])


async def _require_native_maintenance_authority(session, schema, owner_oid):
    """Keep catalog refusal in the source boundary without hiding database errors."""
    try:
        await pins.require_worker_authority(session, schema, owner_oid)
    except ValueError as error:
        raise SourceProfileArchiveError(str(error)) from error


async def _require_native_maintenance_source(session, run, receipt):
    handoff = receipt["handoff"]
    schema, importer = handoff["schema_name"], handoff["importer_id"]
    source = await _run(session, schema, handoff["source_run_id"])
    _validate_run(importer, source)
    _require(
        _digest(source["source_manifest"]) == handoff["source_manifest_sha256"]
        and _native_source_contract(handoff, run["params"], source["source_manifest"])
        == handoff["source_contract_sha256"]
        and source["metrics"]
        == {key: value for key, value in receipt["source_result"].items() if key not in {"run_id", "previous_run_id"}}
        and await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
        == handoff["database_oid"]
        and await native._relation_oid(session, schema, "import_run") == handoff["import_run_oid"]
        and await session.scalar(text("SELECT nspowner FROM pg_namespace WHERE nspname='hp_snapshot_retention'"))
        == receipt["sealed_owner_oid"]
        and await session.scalar(
            text("SELECT relowner FROM pg_class WHERE oid=to_regclass(:relation)"),
            {"relation": _table(schema, pins.TABLE)},
        )
        == receipt["sealed_owner_oid"],
        "source native maintenance producer differs",
    )
    _require(
        receipt["parents"] == [[name, await native._relation_oid(session, schema, name)] for name in TABLES]
        and receipt["ancestor_relations"]
        == [[run_id, await _ancestor_relations(session, schema, run_id)] for run_id in receipt["result"]["run_ids"]],
        "source native maintenance physical source differs",
    )


async def _require_native_abandonment(session, handoff):
    """Authenticate the same control attempt, database and never-published source under locks."""
    schema, importer = handoff["schema_name"], handoff["importer_id"]
    run = (
        (
            await session.execute(
                text(f"SELECT * FROM {_table(schema, 'import_run')} WHERE run_id=:run_id FOR UPDATE"), handoff
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        run is not None
        and run["status"] in {"canceling", "canceled", "cancelled", "failed", "dead_letter"}
        and run["importer"] == importer
        and run["node_id"] == handoff["node_id"]
        and run["engine"] == "healthcare-mrf-api"
        and (run["metrics"] or {}).get("source_profile_handoff") == handoff
        and all((run["progress"] or {}).get(key) == handoff[key] for key in ("attempt_id", "attempt_started_at")),
        "source native abandonment attempt differs",
    )
    _require(
        await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
        == handoff["database_oid"]
        and await native._relation_oid(session, schema, "import_run") == handoff["import_run_oid"],
        "source native abandonment location differs",
    )
    await _source_lock(session, schema, importer)
    source_run = await _run(session, schema, handoff["source_run_id"])
    _require(
        source_run is not None
        and source_run["status"] in {"running", "validating", "failed"}
        and _native_source_contract(handoff, run["params"], source_run["source_manifest"])
        == handoff["source_contract_sha256"],
        "source native abandonment producer differs",
    )
    pointer = await _pointer(session, schema, importer)
    _require(
        handoff["source_run_id"] not in ((pointer or {}).get("current_run_id"), (pointer or {}).get("previous_run_id")),
        "source native abandonment is published",
    )
    return run


async def _drop_native_projection_candidate(session, schema, seal, owner_oid):
    """Drop only the authenticated never-published OID, or prove that exact OID is absent."""
    oid = await native._relation_oid(session, schema, seal["table_name"])
    if oid is not None:
        await native._lock_family(session, schema, (seal["table_name"],), "ACCESS EXCLUSIVE", nowait=True)
        _require(
            await native._relation_oid(session, schema, seal["table_name"]) == oid == seal["relation_oid"]
            and await session.scalar(text("SELECT relowner FROM pg_class WHERE oid=:oid"), {"oid": oid})
            in (seal["owner_oid"], owner_oid),
            "source native abandonment ownership changed",
        )
        await session.execute(text(f"DROP TABLE {_table(schema, seal['table_name'])} RESTRICT"))
    else:
        _require(
            not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_class WHERE oid=:oid)"), {"oid": seal["relation_oid"]}
            ),
            "source native candidate moved",
        )


async def abandon_native_handoff(session, handoff):
    """Only a genuine terminal/cancellation fence releases the never-published candidate."""
    handoff = validate_native_handoff(handoff)
    owner_oid = await native.protected_publisher_owner(session)
    run = await _require_native_abandonment(session, handoff)
    schema, importer = handoff["schema_name"], handoff["importer_id"]
    if importer == PROJECTION_IMPORTER:
        await _drop_native_projection_candidate(session, schema, handoff["projection"], owner_oid)
    await session.execute(
        text(
            f"UPDATE {_table(schema, TABLES[0])} SET status='failed',finished_at=COALESCE(finished_at,clock_timestamp()),"
            "error=CAST(:error AS json) WHERE run_id=:source_run_id AND status IN ('running','validating','failed')"
        ),
        {**handoff, "error": '{"message":"managed publication canceled or failed"}'},
    )
    if run["status"] == "canceling":
        await session.execute(
            text(
                f"UPDATE {_table(schema, 'import_run')} SET status='canceled',"
                "finished_at=clock_timestamp(),heartbeat_at=clock_timestamp(),phase_detail='source profile publication canceled' "
                "WHERE run_id=:run_id AND status='canceling'"
            ),
            handoff,
        )
    receipt_by_field = {"contract": "source-profile-native-abandonment.v1", "handoff_sha256": handoff["handoff_sha256"]}
    await session.execute(
        text(
            f"UPDATE {_table(schema, 'import_run')} SET "
            "metrics=(metrics::jsonb||jsonb_build_object('source_profile_native_abandonment',CAST(:receipt AS jsonb)))::json "
            "WHERE run_id=:run_id"
        ),
        {"run_id": handoff["run_id"], "receipt": native._canonical_json(receipt_by_field).decode("ascii")},
    )
    return receipt_by_field


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
    if importer_id == PROJECTION_IMPORTER:
        from process.florida_projection_archive import _publication_lock

        return await _publication_lock(session, schema)
    key = f"{_table(schema, 'provider_profile_source_publication')}.{SOURCES[importer_id][0]}.publication"
    await session.execute(text("SET LOCAL lock_timeout='500ms'"))
    await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": key})


async def _pointer(session, schema, importer_id):
    if importer_id == PROJECTION_IMPORTER:
        from process.florida_projection_archive import publication_pointer

        return await publication_pointer(session, schema)
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
    relations = await native._namespace_relations(session, schema_oid)
    table_names = {row["relname"] for row in relations if row["relkind"] == "r"}
    _require(
        table_names
        in (set(source_spec(importer_id).table_names), set(source_spec(importer_id, publication=True).table_names)),
        "stage table set differs",
    )
    pairs = tuple([(name, await native._relation_oid(session, schema, name)) for name in sorted(table_names)])
    _require(all(type(oid) is int and oid > 0 for _, oid in pairs), "stage relation is missing")
    oids = {oid for _, oid in pairs}
    for row in relations:
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


async def precreate_restore(session, importer_id, dataset_id, *, contract=LEGACY_CONTRACT):
    """Use installed model DDL; no peer SQL or source control tables are restored."""
    native._require_transaction(session)
    _require(contract in (CONTRACT, LEGACY_CONTRACT), "restore contract is unsupported")
    _require(importer_id != PROJECTION_IMPORTER or contract == CONTRACT, "projection result requires native v2")
    await native._create_model_family(
        session,
        source_spec(importer_id, publication=contract == CONTRACT),
        stage_schema(dataset_id),
        create_indexes=False,
    )
    return await capture_ownership(session, importer_id, dataset_id)


async def complete_restore(session, ownership):
    """Build model indexes after COPY, before retained content validation."""
    await verify_ownership(session, ownership)
    await native._create_model_indexes(
        session, source_spec(ownership.importer_id), ownership.schema_name, create_constraints=True
    )


def _validate_run(importer_id, run):
    source_spec(importer_id)
    if importer_id == PROJECTION_IMPORTER:
        from process.florida_projection_archive import validate_native_run

        try:
            validate_native_run(run)
        except (KeyError, TypeError, ValueError, RuntimeError) as error:
            raise SourceProfileArchiveError("projection publication scope differs") from error
        return {}
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
    if importer_id == PROJECTION_IMPORTER:
        from process.florida_projection_archive import validate_native_result

        try:
            return await validate_native_result(session, schema, run_id)
        except (KeyError, TypeError, ValueError, RuntimeError) as error:
            raise SourceProfileArchiveError("projection evidence closure differs") from error
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
    await _require_fact_types(session, facts, importer_id, run_id)
    if importer_id == "new-york-nypp-profile":
        from process import new_york_profile_store as producer

        run_by_field = dict(await _run(session, schema, run_id))
        if run_by_field["source_manifest"].get("bundle_contract") == producer.WITNESS_CONTRACT:
            artifact, bundle = await producer.read_witness_bundle(session, schema, run_by_field)
            counts = await producer.native_witness_counts(session, schema, run_by_field, bundle)
            reference = producer.bundle_reference(artifact)
            counts["bundle_reference"] = reference
            metrics_by_field = {
                key: metric_value for key, metric_value in bundle["acquisition"].items() if key != "nysed_support"
            }
            final = producer.store._completion_metrics(run_by_field, {**metrics_by_field, "bundle": reference}, counts)
            _require(
                run_by_field["metrics"] == {**final, "published": True}, "source witnessed completion metrics differ"
            )


async def _require_fact_types(session, facts, importer_id, run_id):
    """Check the assertion set against the same trusted policy used by serving."""
    from api import provider_profile_states as serving
    from process.massachusetts_profile_rows import FACT_FORMAT_BY_CATEGORY

    source_key = SOURCES[importer_id][0]
    pairs = (
        tuple((category, shape[0]) for category, shape in FACT_FORMAT_BY_CATEGORY.items())
        if source_key == serving.MASSACHUSETTS_SOURCE_KEY
        else tuple(sorted(serving.STATE_FACT_TYPES[source_key]))
    )
    parameters_by_field = {"run_id": run_id}
    values = []
    for index, (category, fact_type) in enumerate(pairs):
        values.append(f"(:category_{index}, :fact_type_{index})")
        parameters_by_field.update({f"category_{index}": category, f"fact_type_{index}": fact_type})
    invalid = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {facts} f WHERE f.run_id=:run_id AND NOT EXISTS("
            f"SELECT 1 FROM (VALUES {','.join(values)}) AS allowed(category,fact_type) "
            "WHERE allowed.category=f.category AND allowed.fact_type=f.fact_type))"
        ),
        parameters_by_field,
    )
    _require(invalid is False, "retained fact type differs")


async def describe_result(session, *, importer_id, schema, run_id, contract=LEGACY_CONTRACT):
    """Bind the checked scope and schema; only retained v1 receipts hash rows."""
    _require(contract in (CONTRACT, LEGACY_CONTRACT), "result contract is unsupported")
    _require(importer_id != PROJECTION_IMPORTER or contract == CONTRACT, "projection result requires native v2")
    runs = await _lineage(session, importer_id, schema, run_id)
    run = runs[0]
    if importer_id == "new-york-nypp-profile":
        from process.new_york_profile_store import WITNESS_CONTRACT

        _require(
            run["source_manifest"].get("bundle_contract") != WITNESS_CONTRACT or contract == CONTRACT,
            "witnessed result requires native v2",
        )
    run_ids = [run_record["run_id"] for run_record in runs]
    for selected_id in run_ids:
        await _integrity(session, schema, importer_id, selected_id)
    tables = await _table_receipts(session, importer_id, schema, run_ids, contract=contract)
    return {
        "contract": contract,
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


async def _table_receipts(session, importer_id, schema, run_ids, *, contract=CONTRACT):
    tables = []
    for model in source_spec(importer_id).model_types:
        name = model.__tablename__
        run_column = "generation_id" if model is models.ProviderProfileProjection else "run_id"
        content_by_field = {}
        if contract == LEGACY_CONTRACT:
            count, content_sha = await _projected_row_identity(
                session,
                schema,
                name,
                row_json_sql="to_jsonb(row_value)",
                where_sql=f"WHERE row_value.{run_column}=ANY(CAST(:run_ids AS text[]))",
                parameters={"run_ids": run_ids},
            )
            content_by_field["content_sha256"] = content_sha
        else:
            _require(contract == CONTRACT, "result contract is unsupported")
            count = await session.scalar(
                text(f"SELECT count(*) FROM {_table(schema, name)} WHERE {run_column}=ANY(CAST(:run_ids AS text[]))"),
                {"run_ids": run_ids},
            )
        oid = await native._relation_oid(session, schema, name)
        schema_sha = await native._family_schema_identity(session, importer_id, oid, schema, name)
        tables.append({"table_name": name, "row_count": count, "schema_sha256": schema_sha, **content_by_field})
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
        manifest_by_field["contract"] in (CONTRACT, LEGACY_CONTRACT)
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
    _require(
        manifest_by_field["importer_id"] != PROJECTION_IMPORTER or manifest_by_field["contract"] == CONTRACT,
        "projection result requires native v2",
    )
    _validate_table_receipts(
        manifest_by_field["tables"], manifest_by_field["contract"], importer_id=manifest_by_field["importer_id"]
    )
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


def _validate_table_receipts(tables, contract, *, importer_id=None):
    names = source_spec(importer_id).table_names if importer_id is not None else TABLES
    _require(isinstance(tables, list) and len(tables) == len(names), "result table set differs")
    fields = {"table_name", "row_count", "schema_sha256"}
    digests = ("schema_sha256",)
    if contract == LEGACY_CONTRACT:
        fields.add("content_sha256")
        digests = (*digests, "content_sha256")
    for table, name in zip(tables, names, strict=True):
        _require(
            isinstance(table, Mapping) and set(table) == fields,
            "table receipt is invalid",
        )
        _require(
            table["table_name"] == name
            and type(table["row_count"]) is int
            and table["row_count"] >= 0
            and all(isinstance(table[key], str) and bool(_HEX.fullmatch(table[key])) for key in digests),
            "table identity differs",
        )


async def prepare_source(
    session, *, importer_id, schema, run_id, dataset_id, contract=LEGACY_CONTRACT, source_copy=None
):
    """Pin and clone the current root plus its closed reprocessing ancestry."""
    await _source_lock(session, schema, importer_id)
    await session.execute(text("SET LOCAL statement_timeout='1800s'"))
    # A shared-table lock bounds this local slice; use immutable producer generations for concurrent capture.
    spec = source_spec(importer_id)
    await native._lock_family(session, schema, spec.table_names, "SHARE")
    await require_pin_guards(session, schema)
    pointer = await _pointer(session, schema, importer_id)
    _require(
        pointer is not None and run_id == pointer["current_run_id"],
        "result is not retained",
    )
    _require(contract in (CONTRACT, LEGACY_CONTRACT), "result contract is unsupported")
    # Capture only bounded lineage metadata before loading. Validate payload sets after indexing.
    runs = await _lineage(session, importer_id, schema, run_id)
    run_ids = [run_record["run_id"] for run_record in runs]
    ownership = await precreate_restore(session, importer_id, dataset_id, contract=contract)
    await _load_source_models(session, spec, schema, ownership, run_ids, source_copy)
    manifest = await describe_result(
        session, importer_id=importer_id, schema=ownership.schema_name, run_id=run_id, contract=contract
    )
    _require(manifest["run_ids"] == run_ids, "cloned ancestry differs")
    prepared_result = PreparedResult(manifest, ownership)
    await _require_equal_scope(session, prepared_result, schema, run_ids)
    for selected_id in sorted(run_ids):
        await pins.record_pin(
            session,
            schema=schema,
            source_key=SOURCES[importer_id][0],
            run_id=selected_id,
            pin_id=dataset_id,
            purpose="export",
            authority={"result_sha256": _digest(manifest), "root_run_id": run_id, "run_ids": run_ids},
        )
    return prepared_result


async def _load_source_models(session, spec, schema, ownership, run_ids, source_copy):
    """Fill and index only the isolated source family before its existing set validation."""
    _require(isinstance(source_copy, native.ReferenceFamilySourceCopy), "source COPY capability is required")
    await native._copy_model_run_scope(
        session,
        spec,
        source_schema=schema,
        target_schema=ownership.schema_name,
        target_names=spec.table_names,
        run_scope=(
            tuple(
                "generation_id" if model is models.ProviderProfileProjection else "run_id" for model in spec.model_types
            ),
            run_ids,
        ),
        source_copy=source_copy,
        deadline=asyncio.get_running_loop().time() + source_copy.timeout,
    )
    await complete_restore(session, ownership)


async def validate_stage(session, ownership, manifest):
    """Check isolated schema, scope and relationships after trusted protected loading."""
    manifest = validate_manifest(manifest)
    _require(manifest["importer_id"] == ownership.importer_id, "stage source differs")
    spec = source_spec(ownership.importer_id)
    await native._lock_family(session, ownership.schema_name, spec.table_names, "SHARE")
    await verify_ownership(session, ownership)
    for model in spec.model_types:
        name = model.__tablename__
        run_column = "generation_id" if model is models.ProviderProfileProjection else "run_id"
        foreign_rows = await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {_table(ownership.schema_name, name)} WHERE NOT({run_column}=ANY(CAST(:run_ids AS text[]))))"
            ),
            {"run_ids": manifest["run_ids"]},
        )
        _require(foreign_rows is False, "stage contains another result")
    observed = await describe_result(
        session,
        importer_id=ownership.importer_id,
        schema=ownership.schema_name,
        run_id=manifest["run_id"],
        contract=manifest["contract"],
    )
    _require(observed == manifest, "restored result differs")
    return observed


async def validate_source_stage(session, *, prepared, source_schema):
    """Bind a builder-owned clone to its pinned canonical run scope before dumping."""
    manifest = await validate_stage(session, prepared.ownership, prepared.manifest)
    await native._lock_family(session, source_schema, source_spec(manifest["importer_id"]).table_names, "SHARE")
    await _require_equal_scope(session, prepared, source_schema, manifest["run_ids"])
    return manifest


async def _require_equal_scope(session, prepared, schema, run_ids):
    """Compare indexed assertion sets with typed NULL-safe model equality."""
    for model in source_spec(prepared.manifest["importer_id"]).model_types:
        equal = await _are_source_model_rows_equal(
            session,
            prepared.manifest["importer_id"],
            model,
            (schema, model.__tablename__),
            (prepared.ownership.schema_name, model.__tablename__),
            run_ids,
        )
        _require(equal, "local ancestor content differs")


async def _are_source_model_rows_equal(session, importer, model, left, right, run_ids):
    """Preserve NY's canonical JSON preimages while other source contracts keep typed equality."""
    if importer == "new-york-nypp-profile":
        from process.new_york_profile_store import is_canonical_model_equal

        return await is_canonical_model_equal(session, model, left=left, right=right, run_ids=run_ids)
    return await native._is_model_table_equal(
        session,
        model,
        left_schema=left[0],
        left_name=left[1],
        right_schema=right[0],
        right_name=right[1],
        scope=("generation_id" if model is models.ProviderProfileProjection else "run_id", tuple(run_ids)),
    )


def ownership_dict(ownership):
    """Bind the local stage, never a peer's control-run authority."""
    return {
        "importer_id": ownership.importer_id,
        "dataset_id": str(ownership.dataset_id),
        "schema_name": ownership.schema_name,
        "schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
    }


def _validation(
    prepared,
    package_id,
    sealed_owner_oid,
    destination_schema,
    expected_current_run_id,
    pin_id,
    *,
    publication=None,
    projection=None,
):
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
        "contract": _validation_contract(prepared.manifest),
        "package_id": package_id,
        "result_sha256": _digest(validate_manifest(prepared.manifest)),
        "ownership": ownership_dict(prepared.ownership),
        "sealed_owner_oid": sealed_owner_oid,
        "destination_schema": destination_schema,
        "expected_current_run_id": expected_current_run_id,
        "pin_id": str(pin_id),
    }
    if prepared.manifest["contract"] == CONTRACT:
        receipt_by_field["publication"] = _validate_publication(publication, prepared)
    else:
        _require(publication is None, "legacy publication differs")
    if prepared.manifest["importer_id"] == PROJECTION_IMPORTER:
        from process.florida_projection_archive import validate_native_cutover

        receipt_by_field["projection"] = validate_native_cutover(projection, expected_current_run_id, pin_id)
    else:
        _require(projection is None, "source pointer cannot carry a projection")
    return {**receipt_by_field, "validation_sha256": _digest(receipt_by_field)}


def _validate_publication(publication, prepared):
    """Bind only a closed local attachment layout to the verified original archive."""
    _require(
        isinstance(publication, Mapping)
        and set(publication)
        == {"contract", "created_run_ids", "reused_run_ids", "parents", "children", "ancestor_relations"},
        "attachment receipt is invalid",
    )
    created_run_ids, reused_run_ids = publication["created_run_ids"], publication["reused_run_ids"]
    _require(
        isinstance(created_run_ids, list)
        and isinstance(reused_run_ids, list)
        and created_run_ids
        and all(isinstance(run_id, str) and _RUN.fullmatch(run_id) for run_id in (*created_run_ids, *reused_run_ids))
        and len(set((*created_run_ids, *reused_run_ids))) == len(created_run_ids) + len(reused_run_ids)
        and set((*created_run_ids, *reused_run_ids)) == set(prepared.manifest["run_ids"])
        and prepared.manifest["run_id"] in created_run_ids
        and publication["contract"] == ATTACHMENT_CONTRACT,
        "attachment run subset differs",
    )
    oid_by_name = dict(prepared.ownership.relation_oids)
    expected_child_relations = [
        [name, child, oid_by_name.get(child)] for name, child in zip(TABLES, PUBLICATION_TABLES, strict=True)
    ]
    _require(
        set(oid_by_name) == set(source_spec(prepared.manifest["importer_id"], publication=True).table_names)
        and publication["children"] == expected_child_relations
        and _valid_relation_pairs(publication["parents"]),
        "attachment relations differ",
    )
    ancestors = publication["ancestor_relations"]
    _require(
        isinstance(ancestors, list)
        and len(ancestors) == len(reused_run_ids)
        and all(
            isinstance(entry, list) and len(entry) == 2 and entry[0] == run_id and _valid_relation_pairs(entry[1])
            for entry, run_id in zip(ancestors, reused_run_ids, strict=True)
        ),
        "attachment ancestor relations differ",
    )
    return dict(publication)


def _valid_relation_pairs(pairs):
    return (
        isinstance(pairs, list)
        and len(pairs) == len(TABLES)
        and all(
            isinstance(pair, list)
            and len(pair) == 2
            and pair[0] == name
            and type(pair[1]) is int
            and 0 < pair[1] < 2**32
            for pair, name in zip(pairs, TABLES, strict=True)
        )
    )


def _validation_contract(manifest):
    return VALIDATION_CONTRACT if manifest["contract"] == CONTRACT else LEGACY_VALIDATION_CONTRACT


async def _admission(session, manifest, destination_schema, expected_current_run_id, *, projection=None):
    await _source_lock(session, destination_schema, manifest["importer_id"])
    await native._lock_family(session, destination_schema, TABLES, "ACCESS SHARE", nowait=True)
    pointer = await _pointer(session, destination_schema, manifest["importer_id"])
    current = pointer["current_run_id"] if pointer else None
    _require(current == expected_current_run_id, "destination predecessor changed")
    if manifest["importer_id"] == PROJECTION_IMPORTER:
        _require(
            isinstance(projection, Mapping) and pointer == projection["expected"],
            "destination projection predecessor changed",
        )
    active = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {_table(destination_schema, TABLES[0])} "
            "WHERE source_key=:source AND status IN ('running','validating'))"
        ),
        {"source": manifest["source_key"]},
    )
    _require(not active, "destination source is active")


async def prepare_activation(
    session,
    *,
    prepared,
    package_id,
    sealed_owner_oid,
    destination_schema,
    expected_current_run_id,
    pin_id,
    publication_request=None,
):
    """Seal only the isolated v2 stage; retain the original v1 preparation semantics."""
    manifest = validate_manifest(prepared.manifest)
    projection = _publication_copy_request(manifest, publication_request)
    await _admission(session, manifest, destination_schema, expected_current_run_id, projection=projection)
    await session.execute(text("SET LOCAL statement_timeout='1800s'"))
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await validate_stage(session, prepared.ownership, manifest)
    await require_pin_guards(session, destination_schema)
    if manifest["contract"] == CONTRACT:
        publication, projection = await _prepare_adoption_storage(
            session, prepared, destination_schema, sealed_owner_oid, pin_id, publication_request
        )
        return _validation(
            prepared,
            package_id,
            sealed_owner_oid,
            destination_schema,
            expected_current_run_id,
            pin_id,
            publication=publication,
            projection=projection,
        )
    receipt = _validation(prepared, package_id, sealed_owner_oid, destination_schema, expected_current_run_id, pin_id)
    await _adopt_and_pin(session, prepared, destination_schema, pin_id, receipt)
    return receipt


def _publication_copy_request(manifest, request):
    """Keep transient COPY capability separate from the portable result and its fixed scope."""
    importer = manifest["importer_id"]
    if manifest["contract"] == CONTRACT:
        expected_fields = {"source_copy", "expected"} if importer == PROJECTION_IMPORTER else {"source_copy"}
        _require(
            isinstance(request, Mapping)
            and set(request) == expected_fields
            and isinstance(request["source_copy"], native.ReferenceFamilySourceCopy),
            "publication predecessor and COPY capability are required",
        )
        return request if importer == PROJECTION_IMPORTER else None
    _require(request is None, "source pointer cannot carry a publication request")
    return None


async def _prepare_adoption_storage(session, prepared, schema, owner_oid, pin_id, publication_request):
    """Prepare evidence and any projection under one aggregate native COPY budget."""
    source_copy = publication_request["source_copy"]
    deadline = asyncio.get_running_loop().time() + source_copy.timeout
    publication, remaining = await _prepare_publication(
        session, prepared, schema, source_copy=source_copy, deadline=deadline
    )
    projection = None
    if prepared.manifest["importer_id"] == PROJECTION_IMPORTER:
        from process.florida_projection_archive import prepare_native_cutover

        _require(remaining > 0, "publication COPY byte cap exceeded")
        projection_copy = native.ReferenceFamilySourceCopy(source_copy.copy_rows, remaining, source_copy.timeout)
        projection = await prepare_native_cutover(
            session,
            prepared=prepared,
            schema=schema,
            expected=publication_request["expected"],
            owner_oid=owner_oid,
            cutover_id=pin_id,
            source_copy=projection_copy,
            deadline=deadline,
        )
    return publication, projection


async def _prepare_publication(session, prepared, destination_schema, *, source_copy, deadline):
    """Load only novel runs into model heaps, finish all indexes, then check indexed sets."""
    oid_by_name = dict(prepared.ownership.relation_oids)
    _require(
        set(oid_by_name) == set(source_spec(prepared.manifest["importer_id"], publication=True).table_names),
        "attachment heaps are unavailable",
    )
    await _require_stage_topology(session, prepared.ownership)
    created_run_ids, reused_run_ids = [], []
    for run_id in prepared.manifest["run_ids"]:
        (created_run_ids if await _run(session, destination_schema, run_id) is None else reused_run_ids).append(run_id)
    _require(prepared.manifest["run_id"] in created_run_ids, "result already exists locally")
    if reused_run_ids:
        await _require_equal_scope(session, prepared, destination_schema, reused_run_ids)
    remaining = await _load_publication_models(session, prepared, created_run_ids, source_copy, deadline)
    await native._create_model_indexes(
        session,
        publication_spec(prepared.manifest["importer_id"]),
        prepared.ownership.schema_name,
        create_constraints=True,
    )
    publication_by_field = {
        "contract": ATTACHMENT_CONTRACT,
        "created_run_ids": created_run_ids,
        "reused_run_ids": reused_run_ids,
        "parents": [[name, await native._relation_oid(session, destination_schema, name)] for name in TABLES],
        "children": [[name, child, oid_by_name[child]] for name, child in zip(TABLES, PUBLICATION_TABLES, strict=True)],
        "ancestor_relations": [
            [run_id, await _ancestor_relations(session, destination_schema, run_id)] for run_id in reused_run_ids
        ],
    }
    await _validate_publication_sets(session, prepared, publication_by_field)
    return publication_by_field, remaining


async def _load_publication_models(session, prepared, created_run_ids, source_copy, deadline):
    """Populate inert evidence children through the selected closed source-profile contract."""
    oid_by_name = dict(prepared.ownership.relation_oids)
    _require(
        isinstance(source_copy, native.ReferenceFamilySourceCopy),
        "publication COPY capability is required",
    )
    for child in PUBLICATION_TABLES:
        _require(
            await session.scalar(
                text(f"SELECT NOT EXISTS(SELECT 1 FROM {_table(prepared.ownership.schema_name, child)})")
            ),
            "attachment heap is not empty",
        )
        _require(
            await session.scalar(
                text(
                    "SELECT NOT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=:oid) AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=:oid AND contype<>'n')"
                ),
                {"oid": oid_by_name[child]},
            ),
            "attachment heap is not inert",
        )
    return await native._copy_model_run_scope(
        session,
        native.ReferenceFamilySpec(prepared.manifest["importer_id"], MODELS),
        source_schema=prepared.ownership.schema_name,
        target_schema=prepared.ownership.schema_name,
        target_names=PUBLICATION_TABLES,
        run_scope=(("run_id",) * len(MODELS), created_run_ids),
        source_copy=source_copy,
        deadline=deadline,
    )


async def _ancestor_relations(session, destination_schema, run_id):
    """Bind an ancestor's physical family, including empty payload members, not only its parent names."""
    relation = (
        (
            await session.execute(
                text(
                    f"SELECT c.oid,c.relname,n.nspname FROM {_table(destination_schema, TABLES[0])} r JOIN pg_class c ON c.oid=r.tableoid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE r.run_id=:run"
                ),
                {"run": run_id},
            )
        )
        .mappings()
        .one()
    )
    is_parent_local = relation["nspname"] == destination_schema and relation["relname"] == TABLES[0]
    _require(
        is_parent_local or _STAGE.fullmatch(relation["nspname"]) and relation["relname"] == PUBLICATION_TABLES[0],
        "ancestor physical owner differs",
    )
    names = TABLES if is_parent_local else PUBLICATION_TABLES
    pairs = []
    for name, physical_name in zip(TABLES, names, strict=True):
        oid = await native._relation_oid(session, relation["nspname"], physical_name)
        _require(
            await session.scalar(
                text(
                    f"SELECT NOT EXISTS(SELECT 1 FROM {_table(destination_schema, name)} WHERE run_id=:run AND tableoid<>:oid)"
                ),
                {"run": run_id, "oid": oid},
            ),
            "ancestor physical rows differ",
        )
        pairs.append([name, oid])
    return pairs


async def _validate_publication_sets(session, prepared, publication):
    """Check schema, novel run scope and exact typed equality against the preserved original stage."""
    _validate_publication(publication, prepared)
    for model, child in zip(MODELS, PUBLICATION_TABLES, strict=True):
        name = model.__tablename__
        child_oid = dict(prepared.ownership.relation_oids)[child]
        observed = await native.catalog_identity._schema_identity(
            session, child_oid, prepared.ownership.schema_name, name, names_by_stage={child: name}
        )
        expected = next(table["schema_sha256"] for table in prepared.manifest["tables"] if table["table_name"] == name)
        _require(observed == expected, "attachment model schema differs")
        _require(
            await session.scalar(
                text(
                    f"SELECT NOT EXISTS(SELECT 1 FROM {_table(prepared.ownership.schema_name, child)} WHERE NOT(run_id=ANY(CAST(:runs AS text[]))))"
                ),
                {"runs": publication["created_run_ids"]},
            ),
            "attachment contains another run",
        )
        _require(
            await _are_source_model_rows_equal(
                session,
                prepared.manifest["importer_id"],
                model,
                (prepared.ownership.schema_name, name),
                (prepared.ownership.schema_name, child),
                publication["created_run_ids"],
            ),
            "attachment original content differs",
        )


async def _require_publication_preimage(session, prepared, publication, destination_schema):
    """Recheck the exact ancestor and canonical catalog preimage before attaching under its write fence."""
    _validate_publication(publication, prepared)
    parents = [[name, await native._relation_oid(session, destination_schema, name)] for name in TABLES]
    _require(parents == publication["parents"], "attachment parent catalog changed")
    for run_id in publication["created_run_ids"]:
        _require(await _run(session, destination_schema, run_id) is None, "attachment run appeared locally")
    for run_id, pairs in publication["ancestor_relations"]:
        _require(
            await _ancestor_relations(session, destination_schema, run_id) == pairs,
            "attachment ancestor catalog changed",
        )
    if publication["reused_run_ids"]:
        await _require_equal_scope(session, prepared, destination_schema, publication["reused_run_ids"])
    await _validate_publication_sets(session, prepared, publication)


async def _attach_publication(session, prepared, publication, destination_schema):
    """Attach exact indexed children only after collision checks against the full incumbent tree."""
    await native._lock_family(session, destination_schema, TABLES, "SHARE ROW EXCLUSIVE", nowait=True)
    await _require_publication_preimage(session, prepared, publication, destination_schema)
    for model, child in zip(MODELS, PUBLICATION_TABLES, strict=True):
        keys = [tuple(model.__table__.primary_key.columns)] + [
            tuple(constraint.columns)
            for constraint in model.__table__.constraints
            if isinstance(constraint, native.UniqueConstraint)
        ]
        for columns in keys:
            join = " AND ".join(
                f"c.{native._quoted(column.name)}=p.{native._quoted(column.name)}" for column in columns
            )
            _require(
                not await session.scalar(
                    text(
                        f"SELECT EXISTS(SELECT 1 FROM {_table(prepared.ownership.schema_name, child)} c JOIN {_table(destination_schema, model.__tablename__)} p ON {join})"
                    )
                ),
                "attachment key overlaps existing rows",
            )
    connection = await session.connection()
    for name, child, oid in publication["children"]:
        _require(
            not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=:oid OR inhparent=:oid)"), {"oid": oid}
            ),
            "attachment topology changed",
        )
        await connection.exec_driver_sql(
            f"ALTER TABLE {_table(prepared.ownership.schema_name, child)} INHERIT {_table(destination_schema, name)}"
        )
    await _require_stage_topology(session, prepared.ownership, publication)


async def _require_stage_topology(session, ownership, publication=None):
    """Only exact recorded leaf attachments may touch an owned archive namespace."""
    expected = (
        []
        if publication is None
        else sorted((child[2], dict(publication["parents"])[child[0]]) for child in publication["children"])
    )
    rows = await session.execute(
        text(
            "SELECT inhrelid,inhparent FROM pg_inherits WHERE inhrelid=ANY(CAST(:oids AS oid[])) OR inhparent=ANY(CAST(:oids AS oid[])) ORDER BY inhrelid,inhparent"
        ),
        {"oids": [oid for _, oid in ownership.relation_oids]},
    )
    _require([tuple(row) for row in rows] == expected, "attachment topology differs")


async def _adopt_and_pin(session, prepared, destination_schema, pin_id, receipt):
    """Publish the indexed physical family and its seals in the caller's transaction."""
    manifest = prepared.manifest
    for selected_id in sorted(manifest["run_ids"]):
        await pins.lock_run(session, destination_schema, selected_id)
    prior = await _pin_group(session, destination_schema, pin_id)
    if prior:
        _validate_adoption_pins(prior, manifest, receipt)
        if manifest["contract"] == CONTRACT:
            await _require_stage_topology(session, prepared.ownership, receipt["publication"])
        return
    _require(await _run(session, destination_schema, manifest["run_id"]) is None, "result already exists locally")
    if manifest["contract"] == CONTRACT:
        await _attach_publication(session, prepared, receipt["publication"], destination_schema)
        created_ids = receipt["publication"]["created_run_ids"]
    else:
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


async def _adopt_rows(session, prepared, destination_schema):
    """Reuse identical local ancestors; never overwrite another result's rows."""
    manifest = prepared.manifest
    created_ids = []
    for run_id in manifest["run_ids"]:
        if await _run(session, destination_schema, run_id) is None:
            created_ids.append(run_id)
        else:
            existing = await _table_receipts(
                session, manifest["importer_id"], destination_schema, [run_id], contract=LEGACY_CONTRACT
            )
            incoming = await _table_receipts(
                session, manifest["importer_id"], prepared.ownership.schema_name, [run_id], contract=LEGACY_CONTRACT
            )
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
            "AND t.tgenabled='O' AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs=0 "
            "AND t.tgattr=''::int2vector AND t.tgconstraint=0 AND NOT t.tgdeferrable AND NOT t.tginitdeferred "
            "AND p.proname='provider_profile_pinned_run_guard' AND p.pronamespace=n.oid "
            "AND p.provolatile='v' AND NOT p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp'] "
            "AND ((t.tgname='provider_profile_pinned_run_guard_insert' AND t.tgtype=4 "
            "AND t.tgoldtable IS NULL AND t.tgnewtable='profile_guard_new') "
            "OR (t.tgname='provider_profile_pinned_run_guard_update' AND t.tgtype=16 "
            "AND t.tgoldtable='profile_guard_old' AND t.tgnewtable='profile_guard_new') "
            "OR (t.tgname='provider_profile_pinned_run_guard_delete' AND t.tgtype=8 "
            "AND t.tgoldtable='profile_guard_old' AND t.tgnewtable IS NULL)) "
            "AND NOT EXISTS(SELECT 1 FROM pg_trigger legacy WHERE legacy.tgrelid=c.oid "
            "AND legacy.tgname='provider_profile_pinned_run_guard')"
        ),
        {"schema": schema, "names": list(TABLES)},
    )
    _require(guarded == 3 * len(TABLES), "source profile statement seal is unavailable")
    truncation_guards = await session.scalar(
        text(
            "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid "
            "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:names AS text[])) "
            "AND t.tgname='provider_profile_pinned_truncate_guard' AND t.tgenabled='O' "
            "AND t.tgtype=34 AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs=0 "
            "AND t.tgattr=''::int2vector AND t.tgconstraint=0 AND NOT t.tgdeferrable AND NOT t.tginitdeferred "
            "AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL "
            "AND p.proname='provider_profile_pinned_truncate_guard' AND p.pronamespace=n.oid "
            "AND p.provolatile='v' AND NOT p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp']"
        ),
        {"schema": schema, "names": list(TABLES)},
    )
    _require(truncation_guards == len(TABLES), "source profile truncate seal is unavailable")
    attachment_guards = await session.scalar(
        text(
            "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid "
            "WHERE n.nspname=:schema AND c.relname=:table "
            "AND t.tgenabled='O' AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs=0 "
            "AND t.tgattr=''::int2vector AND t.tgconstraint=0 AND NOT t.tgdeferrable AND NOT t.tginitdeferred "
            "AND p.proname='provider_profile_attached_pin_guard' AND p.pronamespace=n.oid "
            "AND p.provolatile='v' AND NOT p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp'] "
            "AND ((t.tgname='provider_profile_attached_pin_guard_update' AND t.tgtype=16 "
            "AND t.tgoldtable='profile_pin_old' AND t.tgnewtable='profile_pin_new') "
            "OR (t.tgname='provider_profile_attached_pin_guard_delete' AND t.tgtype=8 "
            "AND t.tgoldtable='profile_pin_old' AND t.tgnewtable IS NULL) "
            "OR (t.tgname='provider_profile_attached_pin_guard_truncate' AND t.tgtype=34 "
            "AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL))"
        ),
        {"schema": schema, "table": pins.TABLE},
    )
    _require(attachment_guards == 3, "source profile attachment seal is unavailable")


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
    session,
    *,
    prepared,
    destination_schema,
    expected_current_run_id,
    validation,
    package_id,
    sealed_owner_oid,
    pin_id,
):
    """Publish v2 indexed relations, pins and pointer in one native transaction."""
    manifest = validate_manifest(prepared.manifest)
    expected = _validation(
        prepared,
        package_id,
        sealed_owner_oid,
        destination_schema,
        expected_current_run_id,
        pin_id,
        publication=validation.get("publication"),
        projection=validation.get("projection"),
    )
    _require(validation == expected, "adoption validation changed")
    await _adopt_validated_result(session, prepared, expected)
    await _publish_result_pointer(session, prepared, expected)
    return _activation_result(manifest, expected)


async def activate_retained_result(session, *, prepared, validation, publication_continuation):
    """Share native adoption while keeping the retained pointer CAS in the caller's transaction."""
    expected = _validation(
        prepared,
        validation["package_id"],
        validation["sealed_owner_oid"],
        validation["destination_schema"],
        validation["expected_current_run_id"],
        UUID(validation["pin_id"]),
        publication=validation.get("publication"),
        projection=validation.get("projection"),
    )
    _require(validation == expected and callable(publication_continuation), "source retained validation differs")
    await _adopt_validated_result(session, prepared, expected)
    transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
    published_ids = []

    async def publish():
        """The caller can perform one short retained pointer change on the original transaction."""
        _require(
            not published_ids
            and session.in_transaction()
            and await session.scalar(text("SELECT pg_current_xact_id()::text")) == transaction_id,
            "source installed publication transaction changed",
        )
        published_ids.append(transaction_id)
        await _publish_result_pointer(session, prepared, expected, retained_serving=True)

    await publication_continuation(session, publish)
    _require(published_ids, "source installed publication was not completed")
    return _activation_result(prepared.manifest, expected)


async def _adopt_validated_result(session, prepared, expected):
    """Keep the original adoption, ownership, pin and complete producer checks shared."""
    manifest = prepared.manifest
    destination_schema, sealed_owner_oid = expected["destination_schema"], expected["sealed_owner_oid"]
    expected_current_run_id, pin_id = expected["expected_current_run_id"], UUID(expected["pin_id"])
    await _admission(
        session, manifest, destination_schema, expected_current_run_id, projection=expected.get("projection")
    )
    await native._lock_family(
        session,
        prepared.ownership.schema_name,
        tuple(name for name, _ in prepared.ownership.relation_oids),
        "SHARE",
        nowait=True,
    )
    await verify_ownership(session, prepared.ownership)
    await native._verify_stage_owner(session, prepared.ownership, sealed_owner_oid)
    await require_pin_guards(session, destination_schema)
    if manifest["contract"] == CONTRACT:
        await _adopt_and_pin(session, prepared, destination_schema, pin_id, expected)
    _validate_adoption_pins(await _pin_group(session, destination_schema, pin_id), manifest, expected)
    _validate_run(manifest["importer_id"], await _run(session, destination_schema, manifest["run_id"]))


def _activation_result(manifest, expected):
    return {
        "source_key": manifest["source_key"],
        "current_run_id": manifest["run_id"],
        "previous_run_id": expected["expected_current_run_id"],
        "result_sha256": _digest(manifest),
        "pin_id": expected["pin_id"],
        **({"publication": expected["publication"]} if manifest["contract"] == CONTRACT else {}),
        **({"projection": expected["projection"]} if manifest["importer_id"] == PROJECTION_IMPORTER else {}),
    }


async def _publish_result_pointer(session, prepared, validation, *, retained_serving=False):
    """Switch only the source's serving pointer or exact prepared projection in the owning transaction."""
    manifest = prepared.manifest
    destination_schema = validation["destination_schema"]
    if retained_serving:
        from process.entity_address_snapshot_preparation import _seal_published_relation

        await _seal_published_relation(
            session,
            await native._relation_oid(session, destination_schema, "provider_profile_source_publication"),
            validation["sealed_owner_oid"],
        )
    if manifest["importer_id"] == PROJECTION_IMPORTER:
        from process import florida_projection_archive as florida

        await florida.require_native_publication_order(
            session, prepared, destination_schema, validation["projection"]["expected"]
        )
        if retained_serving:
            await florida.publish_retained_projection(
                session,
                destination_schema,
                validation["projection"]["cutover"],
                validation["projection"]["expected"],
                manifest["run_id"],
                validation["sealed_owner_oid"],
            )
            return
        await florida._cutover_prepared_projection(
            session,
            destination_schema,
            manifest["run_id"],
            UUID(validation["pin_id"]),
            validation["projection"]["cutover"],
        )
    else:
        await session.execute(
            text(
                f"INSERT INTO {_table(destination_schema, 'provider_profile_source_publication')} "
                "(source_key,current_run_id,previous_run_id,published_at) VALUES (:source_key,:run_id,:previous,timezone('UTC',now())) "
                "ON CONFLICT (source_key) DO UPDATE SET current_run_id=EXCLUDED.current_run_id, "
                "previous_run_id=EXCLUDED.previous_run_id,published_at=EXCLUDED.published_at"
            ),
            {
                "source_key": manifest["source_key"],
                "run_id": manifest["run_id"],
                "previous": validation["expected_current_run_id"],
            },
        )


async def activate_result(
    session,
    *,
    prepared,
    destination_schema,
    expected_current_run_id,
    package_id,
    sealed_owner_oid,
    pin_id,
    publication_request=None,
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
    validation = await prepare_activation(session, **activation_by_field, publication_request=publication_request)
    return await activate_validated_result(session, validation=validation, **activation_by_field)


async def rollback_result(
    session, *, schema, importer_id, expected_current_run_id, expected_previous_run_id, contract=LEGACY_CONTRACT
):
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
        session, importer_id=importer_id, schema=schema, run_id=expected_previous_run_id, contract=contract
    )
    result_dependencies(predecessor["dependencies"])
    await _restore_pointer(session, schema, importer_id, expected_current_run_id, expected_previous_run_id)


async def rollback_validated_result(
    session,
    *,
    schema,
    importer_id,
    expected_current_run_id,
    expected_previous_run_id,
    pin_id,
    manifest,
    package_id,
):
    """Restore one retained installation using its bounded local adoption seal."""
    manifest = validate_manifest(manifest)
    _require(
        (manifest["importer_id"], manifest["run_id"]) == (importer_id, expected_previous_run_id),
        "rollback result differs",
    )
    await _source_lock(session, schema, importer_id)
    pointer = await _pointer(session, schema, importer_id)
    await _admission(
        session,
        manifest,
        schema,
        expected_current_run_id,
        projection={"expected": pointer} if importer_id == PROJECTION_IMPORTER else None,
    )
    _require(
        pointer is not None and pointer["previous_run_id"] == expected_previous_run_id, "rollback predecessor changed"
    )
    await require_pin_guards(session, schema)
    receipt = await _validated_rollback_seal(session, schema, pin_id, manifest, package_id)
    if manifest["contract"] == CONTRACT:
        ownership = _publication_ownership(receipt)
        await verify_ownership(session, ownership)
        await native._verify_stage_owner(session, ownership, receipt["sealed_owner_oid"])
        await _require_publication_parents(session, schema, receipt["publication"])
        await _require_stage_topology(session, ownership, receipt["publication"])
    if importer_id == PROJECTION_IMPORTER:
        from process import florida_projection_archive as florida

        prepared = PreparedResult(manifest, ownership)
        await validate_stage(session, ownership, manifest)
        await florida.rollback_native_projection(session, prepared, schema, pointer, receipt["projection"])
    else:
        await _restore_pointer(session, schema, importer_id, expected_current_run_id, expected_previous_run_id)
    return {
        "source_key": manifest["source_key"],
        "current_run_id": expected_previous_run_id,
        "previous_run_id": expected_current_run_id,
        "pin_id": str(pin_id),
    }


async def rollback_retained_result(
    session,
    *,
    schema,
    expected_current_run_id,
    pin_id,
    manifest,
    package_id,
    native_receipt=None,
    serving_relation_oid=None,
):
    """Restore exact native/installed custody without inventing installation or heap identity."""
    await native.protected_publisher_owner(session)
    manifest = validate_manifest(manifest)
    importer_id, expected_previous_run_id = manifest["importer_id"], manifest["run_id"]
    await _source_lock(session, schema, importer_id)
    pointer = await _pointer(session, schema, importer_id)
    _require(
        pointer is not None
        and (pointer["current_run_id"], pointer["previous_run_id"])
        == (expected_current_run_id, expected_previous_run_id),
        "source retained rollback predecessor changed",
    )
    await require_pin_guards(session, schema)
    if native_receipt is None:
        receipt = await _validated_rollback_seal(session, schema, pin_id, manifest, package_id)
    else:
        receipt = native_receipt
        _require(
            package_id is None
            and receipt["contract"] in {NATIVE_PUBLICATION_CONTRACT, NATIVE_CAPTURE_CONTRACT}
            and receipt["pin_id"] == str(pin_id)
            and receipt["result"] == manifest
            and receipt["validation_sha256"]
            == _digest({key: field_value for key, field_value in receipt.items() if key != "validation_sha256"}),
            "source native rollback seal differs",
        )
        _validate_adoption_pins(await _pin_group(session, schema, pin_id), manifest, receipt)
    ownership = _publication_ownership(receipt)
    await verify_ownership(session, ownership)
    await native._verify_stage_owner(session, ownership, receipt["sealed_owner_oid"])
    await _require_stage_topology(session, ownership, receipt.get("publication"))
    if "publication" in receipt:
        await _require_publication_parents(session, schema, receipt["publication"])
    if importer_id == PROJECTION_IMPORTER:
        await _restore_retained_projection(session, schema, receipt, pointer, serving_relation_oid)
    else:
        await _restore_pointer(session, schema, importer_id, expected_current_run_id, expected_previous_run_id)
    return {
        "current_run_id": expected_previous_run_id,
        "previous_run_id": expected_current_run_id,
        "pin_id": str(pin_id),
    }


async def _restore_retained_projection(session, schema, receipt, pointer, serving_relation_oid):
    """Restore only the original serving OID authenticated by its origin's local receipt."""
    from process.florida_projection_archive import publish_retained_projection, retained_projection_name

    if receipt["contract"] == NATIVE_CAPTURE_CONTRACT:
        expected_oid = receipt["serving"]["relation_oid"]
    elif receipt["contract"] == NATIVE_PUBLICATION_CONTRACT:
        expected_oid = receipt["handoff"]["projection"]["relation_oid"]
    else:
        expected_oid = receipt["projection"]["cutover"]["relation_oid"]
    _require(
        serving_relation_oid == expected_oid == pointer["previous_relation_oid"],
        "source retained rollback serving OID differs",
    )
    await publish_retained_projection(
        session,
        schema,
        {
            "relation_oid": expected_oid,
            "owner_oid": receipt["sealed_owner_oid"],
            "table_name": retained_projection_name(expected_oid),
        },
        pointer,
        pointer["previous_run_id"],
        receipt["sealed_owner_oid"],
    )


async def _validated_rollback_seal(session, schema, pin_id, manifest, package_id):
    """Authenticate the unchanged shared adoption receipt before either rollback variant."""
    group = await _pin_group(session, schema, pin_id)
    _require(bool(group), "adoption authority changed")
    receipt = group[0]["authority_json"].get("validation", {})
    _validate_adoption_pins(group, manifest, receipt)
    _require(
        receipt.get("contract") == _validation_contract(manifest)
        and receipt.get("package_id") == package_id
        and receipt.get("result_sha256") == _digest(manifest)
        and receipt.get("destination_schema") == schema
        and receipt.get("pin_id") == str(pin_id)
        and receipt.get("validation_sha256")
        == _digest({key: field_value for key, field_value in receipt.items() if key != "validation_sha256"}),
        "rollback authority changed",
    )
    return receipt


async def _restore_pointer(session, schema, importer_id, expected_current_run_id, expected_previous_run_id):
    await require_ordinary_publication_authority(session, schema)
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
    publication = group[0]["authority_json"].get("validation", {}).get("publication")
    if publication is not None:
        _require(
            not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=ANY(CAST(:oids AS oid[])))"),
                {"oids": [child[2] for child in publication["children"]]},
            ),
            "attached source seal is retained",
        )
    await session.execute(text(f"DELETE FROM {_table(schema, pins.TABLE)} WHERE pin_id=:pin"), {"pin": str(pin_id)})
    return "released"


async def cleanup_adoption(session, *, schema, importer_id, run_id, pin_id):
    """Remove only an owned invisible candidate, never current/previous or shared rows."""
    await _source_lock(session, schema, importer_id)
    group = await _pin_group(session, schema, pin_id)
    if not group:
        return "already_released"
    run_ids = _validate_pin_group(group, importer_id, run_id)
    _require(all(pin_row["purpose"] == "adoption" for pin_row in group), "adoption ownership changed")
    for selected_id in sorted(run_ids):
        await pins.lock_run(session, schema, selected_id)
    pointer = await _pointer(session, schema, importer_id)
    if pointer and run_id in (pointer["current_run_id"], pointer["previous_run_id"]):
        return "retained"
    created_ids = [pin_row["run_id"] for pin_row in group if pin_row["authority_json"].get("created_here") is True]
    referenced = await _adoption_referenced(session, schema, created_ids, pin_id)
    receipt = group[0]["authority_json"].get("validation", {})
    if "publication" in receipt:
        if referenced:
            return "retained"
        await _detach_publication(session, schema, receipt)
        await release_source_pin(session, schema=schema, importer_id=importer_id, run_id=run_id, pin_id=pin_id)
        return "released"
    await release_source_pin(session, schema=schema, importer_id=importer_id, run_id=run_id, pin_id=pin_id)
    if not referenced:
        for name in reversed(TABLES):
            await session.execute(
                text(f"DELETE FROM {_table(schema, name)} WHERE run_id=ANY(CAST(:run_ids AS text[]))"),
                {"run_ids": created_ids},
            )
    return "released"


async def _detach_publication(session, destination_schema, receipt):
    """Detach the exact leaf family before dropping its durable owning seal."""
    ownership = _publication_ownership(receipt)
    _require(receipt["destination_schema"] == destination_schema, "attachment ownership changed")
    publication = receipt["publication"]
    await native._lock_family(session, destination_schema, TABLES, "SHARE ROW EXCLUSIVE", nowait=True)
    await native._lock_family(session, ownership.schema_name, PUBLICATION_TABLES, "ACCESS EXCLUSIVE", nowait=True)
    await verify_ownership(session, ownership)
    await _require_publication_parents(session, destination_schema, publication)
    await _require_stage_topology(session, ownership, publication)
    connection = await session.connection()
    for name, child, _oid in publication["children"]:
        await connection.exec_driver_sql(
            f"ALTER TABLE {_table(ownership.schema_name, child)} NO INHERIT {_table(destination_schema, name)}"
        )
    await _require_stage_topology(session, ownership)


def _publication_ownership(receipt):
    """Reconstruct only the stage identity already bound by the local seal."""
    ownership_by_field = receipt["ownership"]
    ownership = StageOwnership(
        ownership_by_field["importer_id"],
        UUID(ownership_by_field["dataset_id"]),
        ownership_by_field["schema_oid"],
        tuple(tuple(pair) for pair in ownership_by_field["relation_oids"]),
    )
    _require(ownership_dict(ownership) == ownership_by_field, "attachment ownership changed")
    return ownership


async def _require_publication_parents(session, destination_schema, publication):
    """Bind the destination namespace to the exact recorded canonical parent OIDs."""
    _require(
        [[name, await native._relation_oid(session, destination_schema, name)] for name in TABLES]
        == publication["parents"],
        "attachment parent catalog changed",
    )


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


async def cleanup_stage(session, ownership, *, publication=None, destination_schema=None):
    """Drop only the exact unattached family after its owning seals are released."""
    native._require_transaction(session)
    names = tuple(name for name, _ in ownership.relation_oids)
    await native._lock_family(session, ownership.schema_name, names, "ACCESS EXCLUSIVE", nowait=True)
    await verify_ownership(session, ownership)
    await _require_stage_topology(session, ownership)
    if set(names) == set(source_spec(ownership.importer_id, publication=True).table_names):
        populated = any(
            [
                await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {_table(ownership.schema_name, name)})"))
                for name in PUBLICATION_TABLES
            ]
        )
        if populated:
            _require(
                isinstance(publication, Mapping) and isinstance(destination_schema, str),
                "detached publication cleanup authority is required",
            )
            await _require_publication_parents(session, destination_schema, publication)
            _require(
                publication["children"]
                == [
                    [name, child, dict(ownership.relation_oids)[child]]
                    for name, child in zip(TABLES, PUBLICATION_TABLES, strict=True)
                ],
                "detached publication ownership changed",
            )
            _require(
                not await session.scalar(
                    text(
                        f"SELECT EXISTS(SELECT 1 FROM {_table(destination_schema, pins.TABLE)} WHERE purpose='adoption' AND authority_json->'validation'->'ownership'->>'dataset_id'=:dataset)"
                    ),
                    {"dataset": str(ownership.dataset_id)},
                ),
                "publication owning seals remain",
            )
    await session.execute(
        text("DROP TABLE " + ", ".join(_table(ownership.schema_name, name) for name in names) + " RESTRICT")
    )
    await session.execute(text(f"DROP SCHEMA {native._quoted(ownership.schema_name)} RESTRICT"))
