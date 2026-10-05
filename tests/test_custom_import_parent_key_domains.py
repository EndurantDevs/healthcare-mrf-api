# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Raw admission evidence and typed parent identities are different domains."""

from __future__ import annotations

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_prepare_page as prepare
from process.custom_import import build_graph_sets as sets
from process.custom_import import build_source as source
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_sets import family_input
from tests.test_custom_import_definition import _rate


def test_source_children_keep_raw_evidence_separate_from_typed_parent_identity():
    request = _request(page_row_limit=64)
    registry = _registry(request.definition)
    child_values = _rate("0000000001", "A00000")
    stream = next(stream for stream in request.definition.source_streams if stream.child_collection == "rates")
    encoded = source._prepare_row(request, stream, child_values)
    assert encoded.rejection is None
    assert encoded.raw_key[0] != encoded.typed_key[0]
    assert encoded.raw_key[1] != encoded.typed_key[1]

    prepared_family, children = family_input(request, 1, values=[child_values])
    child = children[0]
    assert (child.canonical_parent_key, child.parent_key_sha256) == encoded.typed_key
    assert (
        prepared_family.record.canonical_logical_key,
        prepared_family.record.logical_key_sha256,
    ) == encoded.typed_key
    state = sets._Started(
        prepared_family,
        graph.CustomImportBuildFamily(
            build_id=7, root_record_id=1, selection_kind="source", family_revision_id=9, attached_child_count=0
        ),
        graph.CustomImportFamilyRevision(
            family_revision_id=9,
            root_record_id=1,
            root_revision_id=prepared_family.root.root_revision_id,
            entity_binding_id=6001,
            family_sha256=prepared_family.family_sha256,
        ),
        False,
    )
    group = sets._ChildGroup(state, children)
    assert sets._source_projections(request, registry, group)

    candidate_models = snapshot_models(17)
    prepared, _, _ = prepare._children_statement(request, registry, 7, (1,), candidate_models)
    consumed, _, _ = sets._children_statement(request, registry, (state,), candidate_models, snapshot_models(18))
    for statement in (prepared, consumed):
        sql = str(statement.compile(dialect=postgresql.dialect()))
        for typed, raw in (
            ("parent_key_sha256", "raw_parent_key_sha256"),
            ("canonical_parent_key", "raw_parent_key_canonical"),
        ):
            assert (
                f"custom_import_child_revision.{typed} = ci_snapshot_17.custom_import_build_occurrence.{raw}" not in sql
            )
    child.canonical_parent_key, child.parent_key_sha256 = encoded.raw_key
    with pytest.raises(CandidateRunnerError, match="child identity"):
        sets._source_projections(request, registry, group)
