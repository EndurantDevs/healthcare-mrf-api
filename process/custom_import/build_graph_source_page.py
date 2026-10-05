# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persist a Python-validated, bounded source-child projection page."""

from db.models.custom_import import CustomImportBuildCandidateContext, CustomImportChildScalar, CustomImportFamilyChild
from process.custom_import.build_source import _call
from process.custom_import.runner_types import CandidateRunnerError

_SCALAR_COLUMNS = (
    ("bigint[]", "child_revision_id"),
    ("smallint[]", "field_slot"),
    ("text[]", "field_type"),
    ("text[]", "value_state"),
    ("text[]", "string_value"),
    ("bigint[]", "integer_value"),
    ("numeric[]", "decimal_value"),
    ("boolean[]", "boolean_value"),
    ("date[]", "date_value"),
    ("timestamptz[]", "timestamp_value"),
)
_CONTEXT_COLUMNS = (
    ("bigint[]", "context_child_revision_id"),
    ("smallint[]", "profile_slot"),
    ("text[]", "canonical_context_key"),
    ("bytea[]", "context_key_sha256"),
)


async def append_source_child_page(session, plan, progress, family, children, projections):
    """The protected SQL boundary derives scope and advances only the exact next prefix."""

    if plan.selection_kind != "source":
        raise CandidateRunnerError("source child page requires source selection")
    if any(
        not isinstance(row, (CustomImportFamilyChild, CustomImportChildScalar, CustomImportBuildCandidateContext))
        for row in projections
    ):
        raise CandidateRunnerError("source child page has an unknown projection")
    scalars = [row for row in projections if isinstance(row, CustomImportChildScalar)]
    contexts = [row for row in projections if isinstance(row, CustomImportBuildCandidateContext)]
    arguments = (
        ("bigint", plan.build_id),
        ("bigint", plan.root_record_id),
        ("bigint", family.family_revision_id),
        ("bigint", progress.attached_child_count),
        ("bigint[]", [child.child_revision_id for child in children]),
        *((kind, [getattr(row, column) for row in scalars]) for kind, column in _SCALAR_COLUMNS),
        *((kind, [getattr(row, column) for row in contexts]) for kind, column in _CONTEXT_COLUMNS),
    )
    return (await _call(session, "append_custom_import_build_source_children_page", arguments)).one()
