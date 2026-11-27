# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from db.models import MRFAddressEvidence, PlanNetworkTierRaw, PlanNPIRaw
from process import mrf_network_identity as identity
from process.ext.utils import return_checksum

ISSUER = 12345
YEAR = 2026


def _staged_links(*records):
    """Stage plan links the way provider ingest does for each record."""
    link_rows = []
    for provider_links in records:
        for fragment, scope in identity.record_fragments(provider_links).items():
            link_rows.extend(
                (plan_id, scope["network_tier"], scope["issuer_id"], scope["year"], fragment)
                for plan_id in scope["plan_ids"]
            )
    return link_rows


def _keys_by_plan_tier(assignment):
    return {(plan_id, tier): key for plan_id, key, tier, _issuer, _year in assignment.tier_rows}


def _links(tier, *plan_ids):
    return [(plan_id, tier, ISSUER, YEAR) for plan_id in plan_ids]


def test_record_fragments_group_plans_by_tier_and_year():
    """Key one record's plans per tier and year above the published key range."""
    fragments = identity.record_fragments(
        [*_links("PREFERRED", "B", "A"), ("A", "PREFERRED", ISSUER, YEAR + 1), *_links("STANDARD", "C")]
    )
    scopes = sorted((scope["network_tier"], scope["year"], scope["plan_ids"]) for scope in fragments.values())
    assert scopes == [
        ("PREFERRED", YEAR, ("A", "B")),
        ("PREFERRED", YEAR + 1, ("A",)),
        ("STANDARD", YEAR, ("C",)),
    ]
    assert all(fragment > 2**31 for fragment in fragments)
    assert identity.fragment_key("PREFERRED", ISSUER, YEAR, ["B", "A"]) in fragments


def test_tiered_plans_keep_full_network_key_and_split_second_tier():
    """A full network keeps its key while a tiered product's second tier splits out."""
    tier_one_links = [*_links("STANDARD", "LOCAL1", "LOCAL2"), *_links("PREFERRED", "CHOICE1", "CHOICE2")]
    tier_two_links = _links("STANDARD", "LOCAL1", "LOCAL2", "CHOICE1", "CHOICE2")
    assignment = identity.assign_networks(_staged_links(tier_one_links, tier_two_links))
    keys = _keys_by_plan_tier(assignment)

    assert keys[("LOCAL1", "STANDARD")] == keys[("LOCAL2", "STANDARD")] == return_checksum(["STANDARD", ISSUER, YEAR])
    assert keys[("CHOICE1", "PREFERRED")] == return_checksum(["PREFERRED", ISSUER, YEAR])
    choice_second_tier_key = return_checksum(["STANDARD", ISSUER, YEAR, "CHOICE1", "CHOICE2"])
    assert keys[("CHOICE1", "STANDARD")] == keys[("CHOICE2", "STANDARD")] == choice_second_tier_key
    tier_one_fragments = {row[0] for row in assignment.fragment_rows if row[1] == choice_second_tier_key}
    assert len(tier_one_fragments) == 1


def test_disjoint_networks_sharing_a_tier_label_get_separate_keys():
    """Plans whose providers differ never share a key, even with one tier label."""
    assignment = identity.assign_networks(
        _staged_links(
            _links("PREFERRED", "NARROW", "BROAD"), _links("PREFERRED", "BROAD"), _links("PREFERRED", "BROAD")
        )
    )
    keys = _keys_by_plan_tier(assignment)
    legacy_key = return_checksum(["PREFERRED", ISSUER, YEAR])

    assert keys[("BROAD", "PREFERRED")] == legacy_key
    assert keys[("NARROW", "PREFERRED")] == return_checksum(["PREFERRED", ISSUER, YEAR, "NARROW"])
    narrow_fragments = {row[0] for row in assignment.fragment_rows if row[1] == keys[("NARROW", "PREFERRED")]}
    broad_fragments = {row[0] for row in assignment.fragment_rows if row[1] == legacy_key}
    assert narrow_fragments < broad_fragments


def test_assignment_refuses_colliding_network_keys(monkeypatch):
    """Two different networks must never publish under one key."""
    monkeypatch.setattr(identity, "return_checksum", lambda _values: 7)
    with pytest.raises(identity.MRFNetworkIdentityError, match="shared by"):
        identity.assign_networks(_staged_links(_links("PREFERRED", "A", "B"), _links("PREFERRED", "B")))


def test_assignment_refuses_keys_outside_int32(monkeypatch):
    monkeypatch.setattr(identity, "return_checksum", lambda _values: 2**40)
    with pytest.raises(identity.MRFNetworkIdentityError, match="int32"):
        identity.assign_networks(_staged_links(_links("PREFERRED", "A")))


class _ScriptedSession:
    def __init__(self, staged_links, unmapped_counts):
        self.staged_links = staged_links
        self.unmapped_counts = list(unmapped_counts)
        self.statements = []
        self.bound_parameter_sets = []

    async def execute(self, statement, parameter_by_name=None):
        sql = str(statement)
        self.statements.append(sql)
        if parameter_by_name:
            self.bound_parameter_sets.append(parameter_by_name)
        if sql.startswith("SELECT DISTINCT plan_id"):
            return SimpleNamespace(all=lambda: self.staged_links)
        if "NOT EXISTS" in sql:
            unmapped = self.unmapped_counts.pop(0)
            return SimpleNamespace(scalar=lambda: unmapped)
        return SimpleNamespace(scalar=lambda: 3)


def _scripted_database(session):
    @asynccontextmanager
    async def transaction():
        yield session

    return SimpleNamespace(transaction=transaction)


def _stage_classes():
    return {
        "npi_cls": SimpleNamespace(__tablename__="plan_npi_raw_d", __table__=PlanNPIRaw.__table__),
        "tier_cls": SimpleNamespace(__tablename__="plan_networktier_d", __table__=PlanNetworkTierRaw.__table__),
        "evidence_cls": SimpleNamespace(__tablename__="mrf_address_evidence_d", __table__=MRFAddressEvidence.__table__),
    }


@pytest.mark.asyncio
async def test_publish_rewrites_tiers_providers_and_evidence_in_order():
    """Publish one transaction that rewrites every staged table from the fragment map."""
    session = _ScriptedSession(_staged_links(_links("PREFERRED", "A", "B"), _links("PREFERRED", "B")), [0, 0])

    summary = await identity.publish_network_identities(_scripted_database(session), "mrf", **_stage_classes())

    assert summary == {"networks": 2, "plan_links": 2, "provider_rows": 3}
    truncated_tables = [sql.split()[1] for sql in session.statements if sql.startswith("TRUNCATE")]
    assert truncated_tables == ["mrf.plan_networktier_d", "mrf.plan_npi_raw_d", "mrf.mrf_address_evidence_d"]
    rewrite_statements = [sql for sql in session.statements if "DISTINCT ON" in sql]
    assert "fragment_map.network_key AS checksum_network" in rewrite_statements[0]
    assert "hashtextextended" in rewrite_statements[1] and "AS source_record_id" in rewrite_statements[1]
    map_rows, tier_rows = session.bound_parameter_sets
    assert set(map_rows["network_key"]) == set(tier_rows["checksum_network"])
    assert sorted(tier_rows["plan_id"]) == ["A", "B"]


@pytest.mark.asyncio
async def test_publish_refuses_provider_rows_without_plan_links():
    """Never drop provider rows whose fragment has no staged plan link."""
    session = _ScriptedSession(_staged_links(_links("PREFERRED", "A")), [2])

    with pytest.raises(identity.MRFNetworkIdentityError, match="2 staged provider rows"):
        await identity.publish_network_identities(_scripted_database(session), "mrf", **_stage_classes())
    assert not any(sql.startswith("TRUNCATE") for sql in session.statements)


def _published_links(*records):
    assignment = identity.assign_networks(_staged_links(*records))
    return [(plan_id, tier, issuer, year, key) for plan_id, key, tier, issuer, year in assignment.tier_rows]


@pytest.mark.asyncio
async def test_publish_skips_an_import_that_finalization_already_rewrote():
    """A retried finalization keeps the keys it published the first time."""
    session = _ScriptedSession(_published_links(_links("PREFERRED", "A", "B"), _links("PREFERRED", "B")), [])

    summary = await identity.publish_network_identities(_scripted_database(session), "mrf", **_stage_classes())

    assert summary == {"networks": 2, "plan_links": 2, "skipped": True}
    assert len(session.statements) == 1


@pytest.mark.asyncio
async def test_publish_refuses_mixed_fragment_and_published_keys():
    staged_links = _staged_links(_links("PREFERRED", "A")) + _published_links(_links("STANDARD", "B"))
    session = _ScriptedSession(staged_links, [])

    with pytest.raises(identity.MRFNetworkIdentityError, match="1 staged plan links already carry"):
        await identity.publish_network_identities(_scripted_database(session), "mrf", **_stage_classes())
