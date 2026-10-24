# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Two prepared-input checks inside the existing frozen verification transaction."""

from __future__ import annotations

import re
from datetime import timedelta
from functools import lru_cache
from pathlib import Path

from sqlalchemy import bindparam, select, text
from sqlalchemy.dialects.postgresql import JSONB

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildVerification,
    CustomImportCaptureBundle,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportSchemaRevision,
    CustomImportSourceBindingRevision,
)
from db.models.custom_import_prepared_source import CustomImportPreparedSource
from process.custom_import.build_graph import _prepare_read, _read_authority, _require_budget, _verify_request
from process.custom_import.capture_authority import _AUTHORITY_SQL
from process.custom_import.definition import canonical_json
from process.custom_import.object_source_binding import OBJECT_SOURCE_BINDING_CONTRACT
from process.custom_import.prepared_source_codec import PREPARED_SOURCE_CONTRACT
from process.custom_import.prepared_source_metadata import (
    PreparedSourceIdentity,
    PreparedStreamSummary,
    prepared_source_summary,
)
from process.custom_import.prepared_source_progress import parse_progress, totals
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.snowflake_source_binding import SOURCE_BINDING_CONTRACTS
from process.custom_import.source_binding_store import _loaded_object_source_binding
from process.custom_import.storage_layout import snapshot_schema

# Checked against the current root-range renderer, including its complete query order.
_VERIFIER_SHA256 = bytes.fromhex("149f88f395a0f080712680ad040504fef2bafb9ca63318d19015ecdbb785ba06")
_CHECKPOINTS = {26: ("profiles", "profiles", 0), 59: ("root_scalars", "child_scalars", 1)}
_COUNTS = (
    "root_count",
    "family_count",
    "generation_family_count",
    "family_child_count",
    "winner_count",
    "profile_count",
    "root_scalar_count",
    "child_scalar_count",
)
_CURSORS = (
    "after_capture_stream_slot",
    "after_profile_slot",
    "after_root_record_id",
    "current_family_revision_id",
    "current_family_expected_child_count",
    "current_family_seen_child_count",
    "after_child_collection_slot",
    "after_child_revision_id",
    "after_root_scalar_revision_id",
    "after_root_scalar_field_slot",
    "after_child_scalar_revision_id",
    "after_child_scalar_field_slot",
    "after_winner_profile_slot",
    "after_winner_entity_binding_id",
    "after_winner_context_key_sha256",
)
_OWNER_SQL = """
SELECT count(*)=3 AND coalesce(bool_and(pg_catalog.pg_get_userbyid(p.proowner)=CURRENT_USER),false)
    AND coalesce(bool_and(p.prokind='f' AND p.prosecdef AND language.lanname='plpgsql'
        AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]),false)
    AND coalesce(bool_and(p.oid<>pg_catalog.to_regprocedure(:verify) OR
        pg_catalog.sha256(convert_to(replace(replace(replace(p.prosrc,
            :literal,'E''__CONTROL__'''),:escaped_identifier,'"__CONTROL__"'),
            :identifier,'"__CONTROL__"'),'UTF8'))=:expected_digest),false)
FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language language ON language.oid=p.prolang
WHERE p.oid IN (pg_catalog.to_regprocedure(:begin),pg_catalog.to_regprocedure(:admit),pg_catalog.to_regprocedure(:verify))
"""


def _invalid(code="prepared_source_lineage_invalid"):
    return CandidateRunnerError(code)


def _names(session):
    connection = session.connection()
    declared = CustomImportBuildAttempt.__table__.schema
    schema = (connection.get_execution_options().get("schema_translate_map") or {}).get(declared, declared)
    if not isinstance(schema, str) or not schema:
        raise _invalid()
    return schema, connection.dialect.identifier_preparer.quote_identifier(schema)


def _identity_parameters(schema, control):
    return {
        "begin": f"{control}.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)",
        "admit": f"{control}.admit_custom_import_build_page(bigint,bigint)",
        "verify": f"{control}.verify_custom_import_build_structure(bigint)",
        "literal": "E'" + schema.replace("\\", "\\\\").replace("'", "''") + "'",
        "escaped_identifier": control.replace("\\", "\\\\").replace("'", "''"),
        "identifier": control,
        "expected_digest": _VERIFIER_SHA256,
    }


def _execute(session, request, deadline, statement, parameters=None):
    _prepare_read(session, request, deadline)
    return session.execute(statement, parameters or {})


def _owner(session, request, deadline, schema, control):
    if (
        _execute(session, request, deadline, text(_OWNER_SQL), _identity_parameters(schema, control)).scalar_one()
        is not True
    ):
        raise _invalid("prepared_source_verifier_owner_or_layout_invalid")
    relations = [
        f"{control}.{name}" for name in ("custom_import_prepared_source", "custom_import_prepared_source_part")
    ]
    if (
        _execute(
            session, request, deadline, text(_AUTHORITY_SQL), {"relations": relations, "relation_count": 2}
        ).scalar_one()
        is not True
    ):
        raise _invalid("prepared_source_verifier_owner_required")


def _binding_query(build_id):
    build, execution = CustomImportBuildAttempt, CustomImportExecution
    binding, definition, schema = (
        CustomImportSourceBindingRevision,
        CustomImportDefinitionRevision,
        CustomImportSchemaRevision,
    )
    return (
        select(build, execution, binding, definition, schema)
        .join(execution, execution.execution_id == build.execution_id)
        .join(binding, binding.source_binding_revision_id == execution.source_binding_revision_id)
        .join(definition, definition.definition_revision_id == binding.definition_revision_id)
        .join(schema, schema.schema_revision_id == binding.schema_revision_id)
        .where(build.build_id == build_id)
        .execution_options(populate_existing=True)
    )


def _header_summary(header, build, execution, bundle, loaded):
    """Verify the bounded sealed header without using acquisition authority."""
    policy = loaded.binding.processing_policy
    identity = PreparedSourceIdentity(
        execution.execution_id,
        build.capture_bundle_id,
        loaded.dataset_id,
        loaded.definition_revision_id,
        loaded.schema_revision_id,
        loaded.source_binding_revision_id,
        loaded.source_binding_sha256,
        bytes(execution.request_identity_sha256),
        bytes(bundle.manifest_sha256),
        bytes.fromhex(policy.digest),
    )
    if (
        header is None
        or any(getattr(header, name) != expected for name, expected in vars(identity).items())
        or (
            header.state != "sealed"
            or header.sealed_at is None
            or header.encoding_contract != PREPARED_SOURCE_CONTRACT
            or header.canonical_processing_policy != policy.canonical
            or header.version < 1
            or header.preparation_deadline_at != build.build_deadline_at
            or build.build_deadline_at != execution.started_at + timedelta(seconds=policy.build.build_deadline_seconds)
            or bundle.capture_state != "sealed"
            or bundle.payload_contract is not None
            or (bundle.dataset_id, bundle.definition_revision_id, bundle.schema_revision_id)
            != (loaded.dataset_id, loaded.definition_revision_id, loaded.schema_revision_id)
            or execution.capture_bundle_id != build.capture_bundle_id
        )
    ):
        raise _invalid()
    progress = parse_progress(header.canonical_progress, len(loaded.definition.source_streams), policy)
    if not all(stream.eof for stream in progress) or totals(progress) != (
        header.part_count,
        header.record_count,
        header.byte_count,
        header.manifest_byte_count,
    ):
        raise _invalid()
    return _summary_streams(header, identity, progress, policy)


def _summary_streams(header, identity, progress, policy):
    """Match sealed counts and part-set digests to the canonical summary."""
    prefix = canonical_json({"contract": PREPARED_SOURCE_CONTRACT, **identity.to_mapping(), "streams": []})[:-2]
    patterns = []
    for stream in progress:
        fields_by_name = {
            name: getattr(stream, name)
            for name in ("stream_slot", "part_count", "record_count", "byte_count", "manifest_byte_count")
        }
        fields_by_name["part_set_sha256"] = ""
        patterns.append(
            re.escape(canonical_json(fields_by_name)).replace(
                re.escape('"part_set_sha256":""'), '"part_set_sha256":"([0-9a-f]{64})"'
            )
        )
    canonical = header.canonical_summary
    if type(canonical) is not str or len(canonical) > 2 * 1024 * 1024:
        raise _invalid()
    matched = re.fullmatch(re.escape(prefix) + ",".join(patterns) + re.escape("]}"), canonical)
    if matched is None:
        raise _invalid()
    streams = tuple(
        PreparedStreamSummary(
            stream.stream_slot,
            stream.part_count,
            stream.record_count,
            stream.byte_count,
            stream.manifest_byte_count,
            bytes.fromhex(digest),
        )
        for stream, digest in zip(progress, matched.groups(), strict=True)
    )
    summary = prepared_source_summary(identity, streams, stream_count=len(streams), policy=policy)
    if (canonical, header.prepared_sha256) != (summary.canonical, summary.digest):
        raise _invalid()
    return streams


def _proof_valid(proof, build):
    checkpoint = _CHECKPOINTS[proof["page_sequence"]]
    if (
        build.phase != "verifying"
        or proof["verification_contract"] != "custom-import/build-structure/v1"
        or proof["_manual_acceptance"] is not None
        or proof["verification_state"] != "scanning"
        or proof["scan_stage"] != checkpoint[0]
        or proof["verified_at"] is not None
        or proof["generation_id"] != build.generation_id
        or any(proof[name] is not None for name in _CURSORS)
        or any(proof[name] != 0 for name in _COUNTS)
        or any(
            proof[name] is None or proof[name] != getattr(build, name)
            for name in ("source_frozen_at", "graph_frozen_at", "output_frozen_at")
        )
    ):
        raise _invalid("custom_import_build_verification_progress_mismatch")
    return checkpoint


@lru_cache(maxsize=1)
def _queries():
    statements = Path(__file__).with_suffix(".sql").read_text().split("-- statement boundary --")
    if len(statements) != 2:
        raise _invalid()
    return statements


def _finality(session, request, deadline, control, build):
    fields = (
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
    )
    parameters_by_name = {name: getattr(build, name) for name in fields}
    statement = text(
        f"SELECT {control}.lock_custom_import_snapshot_finality(" + ",".join(":" + name for name in fields) + ")"
    )
    statement = statement.bindparams(
        *(bindparam(name, type_=CustomImportBuildAttempt.__table__.c[name].type) for name in fields)
    )
    return _execute(session, request, deadline, statement, parameters_by_name).scalar_one()


def verify_prepared_source_lineage(session, request, build_id, *, deadline) -> str | None:
    """Advance one prepared-input check; the caller owns transaction and rollback."""
    if not session.in_transaction():
        raise _invalid()
    observed_sequence = _execute(
        session,
        request,
        deadline,
        select(CustomImportBuildVerification.page_sequence).where(CustomImportBuildVerification.build_id == build_id),
    ).scalar_one_or_none()
    if observed_sequence not in _CHECKPOINTS:
        return None
    binding_records = _execute(session, request, deadline, _binding_query(build_id)).one_or_none()
    if binding_records is None or binding_records[2].binding_contract in SOURCE_BINDING_CONTRACTS:
        return None
    if binding_records[2].binding_contract != OBJECT_SOURCE_BINDING_CONTRACT:
        raise _invalid()
    _execute(session, request, deadline, text("SELECT pg_catalog.set_config('search_path','pg_catalog',true)"))
    schema, control = _names(session)
    _owner(session, request, deadline, schema, control)
    build, execution, loaded = _lock_prepared_build(session, request, deadline, control, build_id)
    proof = (
        _execute(
            session,
            request,
            deadline,
            text(
                f"SELECT v.*,to_jsonb(v)->'manual_acceptance' AS _manual_acceptance FROM {control}.custom_import_build_verification v "
                "WHERE build_id=:build FOR UPDATE"
            ),
            {"build": build_id},
        )
        .mappings()
        .one_or_none()
    )
    if proof is None or proof["page_sequence"] != observed_sequence:
        return None
    _stage, next_stage, query_index = _proof_valid(proof, build)
    _verify_prepared_relationship(session, request, deadline, control, build, execution, loaded, query_index)
    _advance_checkpoint(session, request, deadline, control, build_id, proof, next_stage)
    _finality(session, request, deadline, control, build)
    _read_authority(session, request, build_id)
    _require_budget(deadline)
    return "scanning"


def _lock_prepared_build(session, request, deadline, control, build_id):
    """Reload and validate retained scope after obtaining the existing build lock."""
    _execute(
        session,
        request,
        deadline,
        text(f"SELECT {control}.lock_custom_import_build(CAST(:build AS bigint))"),
        {"build": build_id},
    )
    build, execution, binding, definition, schema_row = _execute(
        session, request, deadline, _binding_query(build_id)
    ).one()
    _verify_request(build, request, execution)
    loaded = _loaded_object_source_binding(binding, definition, schema_row)
    if (execution.dataset_id, execution.definition_revision_id, execution.schema_revision_id) != (
        loaded.dataset_id,
        loaded.definition_revision_id,
        loaded.schema_revision_id,
    ) or loaded.definition != request.definition:
        raise _invalid()
    return build, execution, loaded


def _verify_prepared_relationship(session, request, deadline, control, build, execution, loaded, query_index):
    """Check frozen snapshot readiness and the selected prepared-input relationship."""
    family_id = _finality(session, request, deadline, control, build)
    _execute(
        session,
        request,
        deadline,
        text(
            f"SELECT {control}.lock_custom_import_sealed_snapshot_base(CAST(:base AS bigint),CAST(:dataset AS bigint))"
        ),
        {"base": build.base_generation_id, "dataset": build.dataset_id},
    )
    for phase in ("serving", "admission", "graph", "output"):
        _execute(
            session,
            request,
            deadline,
            text(
                f"SELECT {control}.verify_custom_import_snapshot_indexes(CAST(:family AS bigint),CAST(:phase AS text))"
            ),
            {"family": family_id, "phase": phase},
        )
    bundle = _execute(
        session,
        request,
        deadline,
        select(CustomImportCaptureBundle).where(CustomImportCaptureBundle.capture_bundle_id == build.capture_bundle_id),
    ).scalar_one()
    header = _execute(
        session,
        request,
        deadline,
        select(CustomImportPreparedSource).where(CustomImportPreparedSource.execution_id == build.execution_id),
    ).scalar_one_or_none()
    streams = _header_summary(header, build, execution, bundle, loaded)
    query = text(
        _queries()[query_index].replace("__CONTROL__", control).replace("__CANDIDATE__", snapshot_schema(family_id))
    )
    parameter_names = query.compile().params
    parameters_by_name = {name: getattr(build, name) for name in parameter_names if name != "streams"}
    parameters_by_name["streams"] = [stream.to_mapping() for stream in streams]
    query = query.bindparams(
        *(
            bindparam(name, type_=JSONB() if name == "streams" else CustomImportBuildAttempt.__table__.c[name].type)
            for name in parameter_names
        )
    )
    if _execute(session, request, deadline, query, parameters_by_name).first() is not None:
        raise _invalid("custom_import_candidate_relationship_invalid")


def _advance_checkpoint(session, request, deadline, control, build_id, proof, next_stage):
    updated = _execute(
        session,
        request,
        deadline,
        text(
            f"UPDATE {control}.custom_import_build_verification SET scan_stage=:stage,page_sequence=page_sequence+1 "
            "WHERE build_id=:build AND page_sequence=:sequence AND scan_stage=:previous RETURNING build_id"
        ),
        {"stage": next_stage, "build": build_id, "sequence": proof["page_sequence"], "previous": proof["scan_stage"]},
    ).scalar_one_or_none()
    if updated != build_id:
        raise _invalid("custom_import_build_verification_progress_mismatch")
