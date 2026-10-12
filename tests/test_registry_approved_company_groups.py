# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real compiled reduction over a bounded synthetic cursor; no database authority claim."""

import json
from uuid import UUID

import pytest

from process import registry_approved_company_groups as groups


def _row(company=1, assertion=100, group=10000, start="2024-01-01"):
    return {
        "assertion_id": str(UUID(int=assertion)),
        "company_id": str(UUID(int=company)),
        "group_id": str(UUID(int=group)),
        "group_kind": "corporate_parent",
        "relationship_kind": "reported_affiliation",
        "valid_from": start,
        "valid_to": None,
        "provenance": {
            "kind": "manual_reference",
            "evidence_ref": "Reviewed reference",
            "snapshot_id": None,
            "source_record_key": None,
        },
    }


class _CursorConnection:
    def __init__(self, families, **changes):
        self.families = families
        self.calls = []
        self.exhausted = False
        self.diagnostics = (
            dict(
                group_family_invalid_count=0,
                group_target_conflict_count=0,
                group_source_conflict_count=0,
                group_assertion_rows=sum(map(len, families)),
                group_unresolved_count=0,
            )
            | changes
        )

    async def cursor(self, query, revision, *, prefetch):
        self.calls.append((query, revision, prefetch))
        yield {"company_key": None, "family_json": None, "diagnostics_json": json.dumps(self.diagnostics)}
        if not any(
            self.diagnostics[key]
            for key in ("group_family_invalid_count", "group_target_conflict_count", "group_source_conflict_count")
        ):
            for family in self.families:
                yield {
                    "company_key": family[0]["company_id"],
                    "family_json": json.dumps(family),
                    "diagnostics_json": None,
                }
        self.exhausted = True


async def _diagnose(connection):
    return await groups.approved_company_group_diagnostics(connection, '"example"', '"selection"', 7)


@pytest.mark.asyncio
async def test_empty_map_uses_one_bounded_read_and_no_reducer(monkeypatch):
    monkeypatch.setattr(groups, "_reduce_batch", lambda *args: pytest.fail("empty family reduction"))
    connection = _CursorConnection([])
    diagnostics = await _diagnose(connection)
    assert set(diagnostics) == {
        "group_family_invalid_count",
        "group_target_conflict_count",
        "group_source_conflict_count",
        "group_assertion_rows",
        "group_review_count",
        "group_review_rows",
        "group_reviews_by_code",
        "group_unresolved_count",
    }
    assert diagnostics["group_review_count"] == diagnostics["group_unresolved_count"] == 0
    assert len(connection.calls) == 1 and connection.calls[0][1:] == (7, 8)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["group_family_invalid_count", "group_target_conflict_count", "group_source_conflict_count"]
)
async def test_set_diagnostics_refuse_before_native_batch(failure, monkeypatch):
    monkeypatch.setattr(groups, "_reduce_batch", lambda *args: pytest.fail("invalid source reduction"))
    connection = _CursorConnection([[_row()]], **{failure: 1, "group_unresolved_count": 1})
    diagnostics = await _diagnose(connection)
    assert diagnostics[failure] == diagnostics["group_unresolved_count"] == 1
    assert connection.exhausted


@pytest.mark.asyncio
async def test_actual_reducer_retains_inclusive_conflicts_and_missing_start():
    first = _row()
    first["valid_to"] = "2024-01-10"
    assertions = [first, _row(assertion=101, group=10001, start="2024-01-10"), _row(assertion=102, start=None)]
    diagnostics = await _diagnose(_CursorConnection([assertions]))
    assert diagnostics["group_review_rows"] == diagnostics["group_unresolved_count"] == 3
    assert diagnostics["group_reviews_by_code"] == {"contradictory_overlapping_groups": 1, "missing_effective_start": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["rows", "bytes", "company_scoped_id"])
async def test_batches_preserve_whole_company_and_native_bounds(mode, monkeypatch):
    if mode == "rows":
        families = [
            [_row(company=company, assertion=company * 1000 + index) for index in range(128)]
            for company in range(1, 41)
        ]
    elif mode == "bytes":
        families = [[_row(company=company, assertion=company * 1000)] for company in range(1, 4)]
        monkeypatch.setattr(groups, "_MAX_BYTES", 800)
    else:
        families = [[_row(company=1)], [_row(company=2)]]
    original = groups._reduce_batch
    batches = []

    def observed(parts, diagnostics):
        rows = json.loads(b"[" + b",".join(parts) + b"]")
        batches.append(rows)
        original(parts, diagnostics)

    monkeypatch.setattr(groups, "_reduce_batch", observed)
    assert (await _diagnose(_CursorConnection(families)))["group_review_count"] == 0
    assert len(batches) > 1
    assert sum(map(len, batches)) == sum(map(len, families))
    for family in families:
        company = family[0]["company_id"]
        assert sum(any(assertion["company_id"] == company for assertion in batch) for batch in batches) == 1
    assert all(
        len(batch) <= 5000 and len(json.dumps(batch, separators=(",", ":")).encode()) <= 8 * 1024 * 1024
        for batch in batches
    )


@pytest.mark.asyncio
async def test_malformed_whole_family_refuses_with_real_compiled_validator():
    row = _row()
    row["provenance"]["evidence_ref"] = "control\x01character"
    with pytest.raises(ValueError):
        await _diagnose(_CursorConnection([[row]]))


def test_sql_preserves_exact_source_and_prospective_predicates():
    statement = groups._DIAGNOSTICS_SQL
    for predicate in (
        "previous.approved_revision=$1",
        "AND NOT EXISTS(SELECT 1 FROM {staging}",
        "edge.company_id::text=g.record_key AND edge.company_id::text=g.assertion->>'company_id'",
        "edge.group_id IS NOT DISTINCT FROM",
        "edge.valid_from IS NOT DISTINCT FROM",
        "edge.valid_to IS NOT DISTINCT FROM",
        "edge.relationship_kind=g.assertion->>'relationship_kind'",
        "observation.status IN ('accepted','unresolved')",
        "edge.resolution_status IN ('resolved','unresolved')",
        "SELECT 1 FROM prospective target",
        "target.record_kind='group' AND target.record_key=g.assertion->>'group_id'",
        "target.record_json->'archived'='false'::jsonb",
        "target.record_json->>'group_kind'=g.assertion->>'group_kind'",
        "WHERE selected AND family<>'[]'::jsonb",
        "SELECT * FROM targets UNION SELECT * FROM sources",
        "WITH ORDINALITY entries(assertion,assertion_ordinal)",
        "AND NOT EXISTS(SELECT 1 FROM invalid) AND NOT EXISTS(SELECT 1 FROM targets) AND NOT EXISTS(SELECT 1 FROM sources)",
        "record_json->'archived'='false'::jsonb",
    ):
        assert predicate in statement
    assert "company_group_registry" not in statement


def test_history_normalizes_only_absent_legacy_group_family():
    from process.registry_approval_store import _selected_history_json_sql

    statement = _selected_history_json_sql()
    company_branch = statement.split("WHEN selected.record_kind='company_links'", 1)[0]
    defaults, retained_fields = company_branch.split("||", 1)
    assert "'group_assertions','[]'::jsonb" in defaults
    # The retained right operand preserves explicit families, including an empty one.
    assert retained_fields.strip() == "(history.record_json-'correction_predecessors')"
