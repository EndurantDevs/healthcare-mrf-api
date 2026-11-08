# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical semantic hashing through bounded pages, without allocation identity."""

import json

from sqlalchemy import and_, func, select, tuple_

from db.models.custom_import import CustomImportEntityBinding
from process.custom_import.query_projection_build import _execute, _source_context
from process.custom_import.query_projection_evidence import ProjectionDigest, ProjectionDigestState
from process.custom_import.query_projection_initialize import initialize_projection
from process.custom_import.query_projection_sources import field_slots
from process.custom_import.query_projection_storage import query_projection_tables
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.runner_types import CandidateRunnerError


def _ordered_contexts(context, table, *, child, after, limit):
    binding = context.model(CustomImportEntityBinding)
    adapter = json.loads(context.definition.schema_canonical)["root"]["entity"]["adapter"]
    entity = binding.canonical_value.collate("C").label("canonical_entity")
    order_columns = [entity, table.c.selected_value]
    if child:
        order_columns.append(table.c.canonical_child_key)
    statement = select(*table.c, entity).join(
        binding,
        and_(
            binding.entity_binding_id == table.c.entity_binding_id,
            binding.dataset_id == context.dataset_id,
            binding.adapter_id == adapter,
        ),
    )
    if after is not None:
        # The leading bound lets PostgreSQL seek the canonical binding index.
        statement = statement.where(entity >= after[0], tuple_(*order_columns) > after)
    return statement.order_by(*order_columns).limit(limit).cte("semantic_projection_page")


async def _digest_page(session, request, context, manifest, *, child, after):
    table = query_projection_tables(manifest.family_id, request.definition)[
        "custom_import_derived_child" if child else "custom_import_derived_root"
    ]
    page = _ordered_contexts(context, table, child=child, after=after, limit=request.page_row_limit)
    identities = [page.c.entity_binding_id, page.c.selected_value]
    if child:
        identities.append(page.c.child_key_sha256)
    order_columns = [page.c.canonical_entity, page.c.selected_value]
    if child:
        order_columns.append(page.c.canonical_child_key)
    size = func.octet_length(page.c.canonical_entity) + 64 + 40 * len(configured_fields(request.definition))
    if child:
        size += func.octet_length(page.c.canonical_child_key)
    metadata = (await _execute(session, select(*identities, size).order_by(*order_columns))).all()
    accepted, used = [], 0
    for context_row in metadata:
        if not accepted and context_row[-1] > request.page_byte_limit:
            raise CandidateRunnerError("one semantic projection context exceeds the byte page")
        if used + context_row[-1] > request.page_byte_limit:
            break
        accepted.append(tuple(context_row[:-1]))
        used += context_row[-1]
    if not accepted:
        return []
    return (
        (await _execute(session, select(page).where(tuple_(*identities).in_(accepted)).order_by(*order_columns)))
        .mappings()
        .all()
    )


def retained_projection_digest(manifest, definition):
    """Restore only the checkpoint committed by an earlier trusted page."""

    state = None
    if manifest.digest_chain_sha256 is not None:
        state = ProjectionDigestState(
            manifest.capability_sha256,
            bytes(manifest.digest_chain_sha256),
            manifest.digested_root_count,
            manifest.digested_child_count,
            manifest.digest_after_entity,
            manifest.digest_after_selected_value,
            manifest.digest_after_child_key,
        )
    return ProjectionDigest(definition, state=state)


def _retain_digest(manifest, digest):
    checkpoint = digest.checkpoint()
    manifest.digest_chain_sha256 = checkpoint.chain_sha256
    manifest.digested_root_count = checkpoint.root_count
    manifest.digested_child_count = checkpoint.child_count
    manifest.digest_after_entity = checkpoint.canonical_entity
    manifest.digest_after_selected_value = checkpoint.selected_value
    manifest.digest_after_child_key = checkpoint.canonical_child_key


async def digest_projection_page(session, request, build, manifest):
    """Commit at most one semantic page; return True only at the final child EOF."""

    from process.custom_import.query_projection_build import _advance_phase

    is_child = manifest.phase == "digest_child"
    if manifest.phase not in {"digest_root", "digest_child"}:
        raise CandidateRunnerError("query projection digest requires closed, verified rows")
    digest = retained_projection_digest(manifest, request.definition)
    fields = tuple(
        field for field in configured_fields(request.definition) if (field.collection is not None) == is_child
    )
    count = manifest.digested_child_count if is_child else manifest.digested_root_count
    after = None
    if count:
        after = (manifest.digest_after_entity, manifest.digest_after_selected_value)
        if is_child:
            after += (manifest.digest_after_child_key,)
    context = await _source_context(session, request, build)
    context_rows = await _digest_page(session, request, context, manifest, child=is_child, after=after)
    slots = field_slots(request.definition)
    for context_row in context_rows:
        values_by_field = {field.field_id: context_row[f"field_{slots[field.field_id]}"] for field in fields}
        if is_child:
            digest.add_child(
                context_row["canonical_entity"],
                context_row["selected_value"],
                context_row["canonical_child_key"],
                values_by_field,
            )
        else:
            digest.add_root(context_row["canonical_entity"], context_row["selected_value"], values_by_field)
    _retain_digest(manifest, digest)
    if context_rows:
        last = context_rows[-1]
        manifest.after_entity_binding_id = last["entity_binding_id"]
        manifest.after_selected_value = last["selected_value"]
        manifest.after_child_key_sha256 = last["child_key_sha256"] if is_child else None
        return False
    if not is_child:
        _advance_phase(manifest)
    return is_child
