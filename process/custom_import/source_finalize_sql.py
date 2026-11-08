# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Ordinary SOURCE finalization for the existing entry-point owners.

Staging uses the same _page_session transaction as authorization and native
COPY; it owns rollback and the final fresh lease check. Other principals retain
the existing dispatcher. This grants no authority and retains existing
low-volume resolvers and canonical helpers.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from uuid import UUID

from sqlalchemy import BigInteger, Boolean, Integer, LargeBinary, String, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PGUUID

from db.models.custom_import import CustomImportBuildAttempt, CustomImportBuildStream, CustomImportSourceStream
from process.custom_import.build_source import _SOURCE_COPY_COLUMNS, _call, _prepare_statement
from process.custom_import.storage_layout import snapshot_schema

_MODEL_BY_PREFIX = {"b": CustomImportBuildAttempt, "s": CustomImportBuildStream, "stream": CustomImportSourceStream}
_RELATIONS_BY_NAME = {
    "custom_import_root_record_relation": ["CANDIDATE", "custom_import_root_record"],
    "custom_import_pack_relation": ["CANDIDATE", "custom_import_pack"],
    "custom_import_root_revision_relation": ["CANDIDATE", "custom_import_root_revision"],
    "custom_import_child_revision_relation": ["CANDIDATE", "custom_import_child_revision"],
    "custom_import_rejection_relation": ["CANDIDATE", "custom_import_rejection"],
    "custom_import_build_occurrence_relation": ["CANDIDATE", "custom_import_build_occurrence"],
    "source_bulk_landing_relation": ["CANDIDATE", "source_bulk_landing"],
    "custom_import_rejection_rejection_id_seq_relation": ["CONTROL", "custom_import_rejection_rejection_id_seq"],
    "custom_import_root_revision_root_revision_id_seq_relation": [
        "CONTROL",
        "custom_import_root_revision_root_revision_id_seq",
    ],
    "custom_import_child_revision_child_revision_id_seq_relation": [
        "CONTROL",
        "custom_import_child_revision_child_revision_id_seq",
    ],
}


class SourceFinalizationError(RuntimeError):
    """Preserve storage-error propagation rather than terminal runner validation."""

    sqlstate = "P0001"


def _parameter_type(name):
    prefix, _, column = name.partition("_")
    if prefix in _MODEL_BY_PREFIX:
        return _MODEL_BY_PREFIX[prefix].__table__.c[column].type
    if name in _RELATIONS_BY_NAME or name in ("kind", "label", "a_opened_by", "a_transaction_id"):
        return String()
    if name in ("a_token_hash", "key_contract"):
        return LargeBinary()
    if name == "a_accepting":
        return Boolean()
    if name in ("p_batch", "a_batch_id"):
        return PGUUID()
    if name == "p_verified_parts":
        return ARRAY(Integer())
    return BigInteger()


@lru_cache(maxsize=1)
def _load_sql():
    """Only fixed package statements; no historical function extraction."""

    fragments = Path(__file__).with_name("source_finalize.sql").read_text().split("-- query: ")
    query_by_name = dict(fragment.split("\n", 1) for fragment in fragments[1:])
    if len(query_by_name) != 60 or len(fragments) != 61:
        raise SourceFinalizationError("source finalization SQL resource is malformed")
    return query_by_name


async def _statements(session, family_id):
    connection = await session.connection()
    model_schema = CustomImportBuildAttempt.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    control_schema = schema_map.get(model_schema, model_schema)
    if not control_schema:
        raise SourceFinalizationError("source finalization requires an explicit model schema")
    quote = connection.dialect.identifier_preparer.quote_schema
    namespace_by_name = {"CONTROL": quote(control_schema), "CANDIDATE": quote(snapshot_schema(family_id))}
    statement_by_name = {}
    for name, query in _load_sql().items():
        for namespace, value in namespace_by_name.items():
            query = query.replace(f"__{namespace}__", value)
        statement = text(query)
        statement_by_name[name] = statement.bindparams(
            *(bindparam(parameter, type_=_parameter_type(parameter)) for parameter in statement.compile().params)
        )
    relation_by_name = {
        name: f"{namespace_by_name[namespace]}.{table}" for name, (namespace, table) in _RELATIONS_BY_NAME.items()
    }
    return statement_by_name, relation_by_name, connection.dialect.identifier_preparer


async def _execute_stage(session, statements, parameter_by_name, name):
    await _prepare_statement(session)
    result = await session.execute(statements[name], parameter_by_name)
    if not result.returns_rows:
        return {}
    row_by_name = dict(result.mappings().one())
    if row_by_name.get("problem") is not None:
        raise SourceFinalizationError(row_by_name["problem"])
    parameter_by_name.update(row_by_name)
    return row_by_name


async def _validate(session, statements, parameter_by_name):
    await _execute_stage(session, statements, parameter_by_name, "row_guards")
    await _execute_stage(session, statements, parameter_by_name, "relationship_guards")
    await _execute_stage(session, statements, parameter_by_name, "authority")
    await _execute_stage(session, statements, parameter_by_name, "binding")
    await _execute_stage(session, statements, parameter_by_name, "locked_authority")
    await _execute_stage(session, statements, parameter_by_name, "phase")
    await _execute_stage(session, statements, parameter_by_name, "identity")
    await _execute_stage(session, statements, parameter_by_name, "lock_landing")
    await _execute_stage(session, statements, parameter_by_name, "landing_bound")
    await _execute_stage(session, statements, parameter_by_name, "landing_owner")
    await _execute_stage(session, statements, parameter_by_name, "sealed")
    await _execute_stage(session, statements, parameter_by_name, "cursor")
    await _execute_stage(session, statements, parameter_by_name, "stream")
    await _execute_stage(session, statements, parameter_by_name, "metadata")
    await _execute_stage(session, statements, parameter_by_name, "landing_counts")
    await _execute_stage(session, statements, parameter_by_name, "batch_bounds")
    await _execute_stage(session, statements, parameter_by_name, "row_identity")
    await _execute_stage(session, statements, parameter_by_name, "analyze_landing")
    await _execute_stage(session, statements, parameter_by_name, "ordinal_bounds")
    await _execute_stage(session, statements, parameter_by_name, "key_collisions")
    await _execute_stage(session, statements, parameter_by_name, "pack_counts")
    await _execute_stage(session, statements, parameter_by_name, "last_row")
    await _execute_stage(session, statements, parameter_by_name, "verified_parts")
    await _execute_stage(session, statements, parameter_by_name, "positions")
    await _execute_stage(session, statements, parameter_by_name, "pack_identity")


async def _dictionary(session, statements, parameter_by_name):
    await _execute_stage(session, statements, parameter_by_name, "dictionary_counts")
    await _execute_stage(session, statements, parameter_by_name, "dictionary_bound")
    await _execute_stage(session, statements, parameter_by_name, "insert_global_keys")
    await _execute_stage(session, statements, parameter_by_name, "global_key_count")
    await _execute_stage(session, statements, parameter_by_name, "global_key_reads")
    await _execute_stage(session, statements, parameter_by_name, "global_key_bound")
    await _execute_stage(session, statements, parameter_by_name, "global_key_collision")
    await _execute_stage(session, statements, parameter_by_name, "insert_candidate_keys")
    await _execute_stage(session, statements, parameter_by_name, "candidate_key_count")
    await _execute_stage(session, statements, parameter_by_name, "candidate_key_reads")
    await _execute_stage(session, statements, parameter_by_name, "candidate_key_bound")
    await _execute_stage(session, statements, parameter_by_name, "candidate_key_identity")


async def _promote(session, statements, parameter_by_name):
    await _execute_stage(session, statements, parameter_by_name, "assign")
    await _execute_stage(session, statements, parameter_by_name, "insert_roots")
    await _execute_stage(session, statements, parameter_by_name, "insert_children")
    await _execute_stage(session, statements, parameter_by_name, "insert_rejections")
    parameter_by_name["outcomes_n"] = parameter_by_name["root_written_n"] + parameter_by_name["child_written_n"]
    await _execute_stage(session, statements, parameter_by_name, "outcome_count")
    await _execute_stage(session, statements, parameter_by_name, "insert_occurrences")
    await _execute_stage(session, statements, parameter_by_name, "occurrence_count")
    await _execute_stage(session, statements, parameter_by_name, "reanalyze_landing")
    if (await _execute_stage(session, statements, parameter_by_name, "needs_occurrence_analyze"))["needed"]:
        await _execute_stage(session, statements, parameter_by_name, "analyze_occurrences")
    await _execute_stage(session, statements, parameter_by_name, "stored_identity")
    await _execute_stage(session, statements, parameter_by_name, "advance_stream")
    await _execute_stage(session, statements, parameter_by_name, "advance_build")
    await _execute_stage(session, statements, parameter_by_name, "counter_cursor")
    await _execute_stage(session, statements, parameter_by_name, "completion")


async def _complete(session, statements, parameter_by_name, preparer):
    await _execute_stage(session, statements, parameter_by_name, "homes")
    await _execute_stage(session, statements, parameter_by_name, "delete_landing")
    if (await _execute_stage(session, statements, parameter_by_name, "landing_empty"))["empty"]:
        await _execute_stage(session, statements, parameter_by_name, "truncate_landing")
    await _execute_stage(session, statements, parameter_by_name, "close_authorization")
    await _execute_stage(session, statements, parameter_by_name, "fresh_lease")
    closure = await _execute_stage(session, statements, parameter_by_name, "copy_closure")
    if closure["close_copy"]:
        columns = ",".join(preparer.quote(column) for column in _SOURCE_COPY_COLUMNS)
        relation = parameter_by_name["source_bulk_landing_relation"]
        role = preparer.quote(closure["role_name"])
        await _prepare_statement(session)
        await session.execute(text(f"REVOKE INSERT ({columns}) ON TABLE {relation} FROM {role}"))
    await _call(session, "lock_custom_import_build", (("bigint", parameter_by_name["b_build_id"]),))


async def finalize_source_batch(session, batch_id: UUID, verified_parts: list[int]) -> int:
    """Finalize one authorized <=100K-row/256MiB batch in the caller's transaction.

    No commit or retry is performed here. Any error or cancellation must escape
    the enclosing page context and roll back COPY, dictionaries, outcomes,
    counters, completion, home mapping and transactional grant closure together.
    The existing canonical/provenance checks are intentionally not bypassed.
    """

    if not session.in_transaction():
        raise SourceFinalizationError("source finalization requires the authorization transaction")
    if not isinstance(batch_id, UUID):
        raise SourceFinalizationError("source finalization requires a UUID batch")
    await session.execute(text("SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"))
    binding = await _call(session, "resolve_custom_import_source_batch_snapshot", (("uuid", batch_id),))
    family_id = binding.scalar_one()
    # Positive native registry identity, not a caller-selected namespace.
    snapshot_schema(family_id)
    await _call(session, "verify_custom_import_snapshot_writers", (("bigint", family_id),))
    statements, relation_by_name, preparer = await _statements(session, family_id)
    parameter_by_name = dict(relation_by_name, p_batch=batch_id, p_verified_parts=verified_parts, family_id=family_id)
    await _execute_stage(session, statements, parameter_by_name, "scope")
    await _call(session, "resolve_custom_import_build_base_snapshot", (("bigint", parameter_by_name["b_build_id"]),))
    await _validate(session, statements, parameter_by_name)
    await _dictionary(session, statements, parameter_by_name)
    await _promote(session, statements, parameter_by_name)
    await _complete(session, statements, parameter_by_name, preparer)
    return parameter_by_name["n"]
