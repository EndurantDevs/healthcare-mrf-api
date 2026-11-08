# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned PTG graph intersections remain candidate-only and fail closed."""

import json

import pytest

from process import cms_npd_tax_candidate_lookup as lookup
from process.cms_npd_tax_candidate_report import (
    CmsNpiTaxCandidate,
    CmsTaxCandidateOrganization,
    build_cms_tax_candidate_report,
)
from process.tin_npi_connector import FhirOrganizationEvidenceResult, FhirOrganizationEvidenceState


class _Result:
    def __init__(self, rows=(), scalar=None):
        self.rows = tuple(rows)
        self.value = scalar

    def mappings(self):
        return self

    def one_or_none(self):
        return self.rows[0] if self.rows else None

    def scalar_one(self):
        return self.value

    def __iter__(self):
        return iter(self.rows)


class _Session:
    def __init__(self, *, generation="shared_blocks_v4", read_only="on", sealed_digest="b" * 64):
        self.generation = generation
        self.read_only = read_only
        self.sealed_digest = sealed_digest
        self.group_rows = [
            {"provider_group_key": 0, "tax_identity_state": "matched_ein", "tin_key": 5},
            {"provider_group_key": 1, "tax_identity_state": "matched_ein", "tin_key": 7},
            {"provider_group_key": 2, "tax_identity_state": "missing", "tin_key": None},
        ]
        self.queries = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        self.queries.append((sql, params))
        if "transaction_read_only" in sql:
            return _Result(scalar=self.read_only)
        if "SELECT layout.generation" in sql:
            return _Result(
                rows=[
                    {
                        "generation": self.generation,
                        "content_digest": bytes.fromhex("b" * 64),
                        "sealed_tax_digest": self.sealed_digest,
                    }
                ]
            )
        if "SELECT groups.provider_group_key" in sql:
            selected_group_keys = set(params["group_keys"])
            return _Result(rows=[row for row in self.group_rows if row["provider_group_key"] in selected_group_keys])
        return _Result()


@pytest.mark.asyncio
async def test_current_tax_pin_requires_published_v4_layout_and_exact_digest():
    class _PinSession:
        def __init__(self, *, read_only="on", row=None):
            self.read_only = read_only
            self.row = row
            self.queries = []

        async def execute(self, statement, params=None):
            sql = str(statement)
            self.queries.append((sql, params))
            if "transaction_read_only" in sql:
                return _Result(scalar=self.read_only)
            return _Result(rows=() if self.row is None else (self.row,))

    pin_row_by_field = {
        "snapshot_id": "published-snapshot",
        "snapshot_key": 17,
        "layout_snapshot_key": "17",
        "content_digest": bytes.fromhex("b" * 64),
        "sealed_tax_digest": "b" * 64,
    }
    session = _PinSession(row=pin_row_by_field)
    assert await lookup.current_sealed_v4_tax_pin(session, schema_name="mrf") == (
        "published-snapshot",
        17,
        "b" * 64,
    )
    sql = session.queries[1][0]
    assert "pointer.slot = 'current'" in sql
    assert "snapshot.status = 'published'" in sql
    assert "layout.state = 'sealed'" in sql
    assert "root.state = 'complete'" in sql
    assert session.queries[1][1] == {"generation": "shared_blocks_v4"}
    assert await lookup.current_sealed_v4_tax_pin(_PinSession(), schema_name="mrf") is None
    with pytest.raises(ValueError, match="read-only"):
        await lookup.current_sealed_v4_tax_pin(_PinSession(read_only="off", row=pin_row_by_field), schema_name="mrf")
    for changed in (
        {"sealed_tax_digest": "c" * 64},
        {"layout_snapshot_key": "18"},
        {"content_digest": bytes.fromhex("c" * 64)},
    ):
        with pytest.raises(ValueError, match="pin is invalid"):
            await lookup.current_sealed_v4_tax_pin(_PinSession(row={**pin_row_by_field, **changed}), schema_name="mrf")


def _organization(*npis):
    return CmsTaxCandidateOrganization(
        "organization-a",
        "a" * 64,
        FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.MISSING_EIN,
            npi_candidates=tuple(sorted(npis)),
        ),
    )


async def _report_for_organizations(session, organizations):
    """Build a report from the same pinned lookup exercised by production."""
    selected_npis = tuple(
        sorted({npi for organization in organizations for npi in organization.extraction.npi_candidates})
    )
    matches = await lookup.lookup_pinned_tax_candidates(
        session, schema_name="mrf", snapshot_key=17, manifest_sha256="b" * 64, npis=selected_npis
    )
    return build_cms_tax_candidate_report(
        dataset_id="dataset-a",
        release_id="release-a",
        tax_snapshot_key=17,
        tax_manifest_sha256="b" * 64,
        extraction_policy_sha256="c" * 64,
        extraction_cutoff="2026-09-24T00:00:00.000000Z",
        organizations=organizations,
        candidates_by_npi=matches,
    )


@pytest.mark.asyncio
async def test_v4_lookup_preserves_ambiguity_no_match_and_snapshot_pin(monkeypatch):
    async def keys(*_args, **_kwargs):
        return {1000000004: 4, 1234567893: 5}

    async def graph(*_args, **kwargs):
        assert kwargs["max_members"] == 256 * 129
        return {4: (0, 1), 5: (2,)}

    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", graph)
    session = _Session()
    report = await _report_for_organizations(session, (_organization(1000000004, 1234567893),))
    organization_row = json.loads(report)["organizations"][0]
    assert organization_row["state"] == "ambiguous"
    assert organization_row["npis"] == [
        {
            "npi": 1000000004,
            "candidate_tin_keys": [5, 7],
            "group_count": 2,
            "groups_without_ein_match": 0,
            "state": "ambiguous",
        },
        {
            "npi": 1234567893,
            "candidate_tin_keys": [],
            "group_count": 1,
            "groups_without_ein_match": 1,
            "state": "no_match",
        },
    ]
    assert organization_row["missing_real_ein"] is True
    assert sum("SELECT layout.generation" in sql for sql, _ in session.queries) == 2
    assert all("INSERT" not in sql and "UPDATE" not in sql for sql, _ in session.queries)


@pytest.mark.asyncio
async def test_v4_lookup_uses_exact_dense_npi_graph_and_retains_absence(monkeypatch):
    async def keys(*_args, **_kwargs):
        return {1000000004: 4}

    async def graph(*_args, **kwargs):
        assert kwargs["relation"] == "npi_groups_exact"
        assert tuple(kwargs["owner_keys"]) == (4,)
        return {4: (0,)}

    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", graph)
    session = _Session(generation="shared_blocks_v4")
    matches = await lookup.lookup_pinned_tax_candidates(
        session,
        schema_name="mrf",
        snapshot_key=17,
        manifest_sha256="b" * 64,
        npis=(1000000004, 1234567893),
    )
    assert matches == {
        1000000004: CmsNpiTaxCandidate((5,), 1, 0),
        1234567893: CmsNpiTaxCandidate((), 0, 0),
    }


@pytest.mark.asyncio
async def test_v4_lookup_preserves_group_level_partial_match(monkeypatch):
    async def keys(*_args, **_kwargs):
        return {1000000004: 4}

    async def graph(*_args, **_kwargs):
        return {4: (0, 2)}

    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", graph)
    report = await _report_for_organizations(_Session(), (_organization(1000000004),))
    row = json.loads(report)["organizations"][0]
    assert row["state"] == "partial_match"
    assert row["npis"][0]["candidate_tin_keys"] == [5]
    assert row["npis"][0]["groups_without_ein_match"] == 1


@pytest.mark.asyncio
async def test_v4_lookup_records_degree_overflow_without_tax_projection(monkeypatch):
    async def keys(*_args, **_kwargs):
        return {1000000004: 4}

    async def graph(*_args, **kwargs):
        assert kwargs["limit_per_owner"] == 129
        assert kwargs["max_members"] == 256 * 129
        return {4: tuple(range(129))}

    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", graph)
    session = _Session()
    report = await _report_for_organizations(session, (_organization(1000000004),))
    organization_row = json.loads(report)["organizations"][0]
    assert organization_row["state"] == "degree_overflow"
    assert organization_row["partial_match"] is False
    assert organization_row["npis"] == [
        {
            "npi": 1000000004,
            "candidate_tin_keys": [],
            "group_count": None,
            "group_count_lower_bound": 129,
            "groups_without_ein_match": None,
            "state": "degree_overflow",
        }
    ]
    assert not any("SELECT groups.provider_group_key" in sql for sql, _ in session.queries)


@pytest.mark.asyncio
async def test_lookup_rejects_writable_transaction_or_manifest_drift(monkeypatch):
    async def keys(*_args, **_kwargs):
        return {1000000004: 4}

    async def graph(*_args, **_kwargs):
        return {4: (0,)}

    monkeypatch.setattr(lookup, "v4_npi_keys_for_values", keys)
    monkeypatch.setattr(lookup, "lookup_v4_relation_member_prefixes", graph)
    lookup_kwargs_by_name = {
        "schema_name": "mrf",
        "snapshot_key": 17,
        "manifest_sha256": "b" * 64,
        "npis": (1000000004,),
    }
    with pytest.raises(ValueError, match="read-only"):
        await lookup.lookup_pinned_tax_candidates(_Session(read_only="off"), **lookup_kwargs_by_name)
    with pytest.raises(ValueError, match="manifest mismatch"):
        await lookup.lookup_pinned_tax_candidates(_Session(sealed_digest="c" * 64), **lookup_kwargs_by_name)
    with pytest.raises(ValueError, match="manifest mismatch"):
        await lookup.lookup_pinned_tax_candidates(_Session(generation="shared_blocks_v3"), **lookup_kwargs_by_name)

    session = _Session()
    session.group_rows = []
    with pytest.raises(ValueError, match="incomplete"):
        await lookup.lookup_pinned_tax_candidates(session, **lookup_kwargs_by_name)
