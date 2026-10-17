# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Default source policy SQL preserves public facts without selecting membership."""

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from api.provider_directory_cms_network_scope import (
    CMSNetworkScope,
    _selected_plans,
    cms_network_predicate,
    cms_network_target_predicate,
)
from api.provider_directory_cms_queries import cms_entity_rows, cms_relationship_rows
from api.provider_directory_entities_contract import DirectoryRead
from process.network_approved_source_bindings import APPROVED_NETWORK_BINDINGS_SQL
from process.network_fhir_membership_source import PinnedFHIRMembershipSource


def _generation(excluded=(84,), selected=None):
    parameters_by_name = dict(
        zip(
            (f"scope_{index}" for index in range(1, 8)),
            (7, "fhir", "cms-npd", "retained", "dataset", "producer", "edition"),
        )
    )
    parameters_by_name.update(
        scope_network_ids=selected,
        scope_excluded_network_ids=list(excluded or ()),
        scope_alias="alias",
        dataset_id="dataset",
        release_id="release",
    )
    bindings = APPROVED_NETWORK_BINDINGS_SQL.format(namespace='"control"')
    bindings = re.sub(r"\$(\d+)", lambda match: ":scope_" + match[1], bindings)
    pin = PinnedFHIRMembershipSource(
        "retained", "cms-npd", "endpoint", "dataset", "a" * 64, "release", 7, "alias", "2026-01-01"
    )
    scope = CMSNetworkScope(
        SimpleNamespace(schema_name="canonical"), bindings, parameters_by_name, '"retained"', pin, {}, excluded
    )
    return {**parameters_by_name, "network_scope": scope}


def _compile(predicate, generation):
    statement = text("SELECT r.resource_id FROM resources r WHERE TRUE" + predicate)
    parameters_by_name = {name: generation[name] for name in statement.compile().params}
    return statement.bindparams(**parameters_by_name).compile(dialect=postgresql.dialect())


@pytest.mark.parametrize("excluded", [(), (84,)])
@pytest.mark.parametrize("kind", ["organizations", "payers", "sites", "practitioner-roles", "providers"])
def test_default_preserves_ordinary_facets_even_without_permitted_membership(excluded, kind):
    generation = _generation(excluded)
    assert cms_network_predicate(generation, kind, "r.resource_id") == ""
    assert cms_network_target_predicate(generation, "edge.kind", "edge.reference", {kind}) == ""


@pytest.mark.parametrize("excluded", [(), (84,)])
@pytest.mark.parametrize("kind", ["networks", "plans"])
def test_default_denies_only_reviewed_identities_without_any_permitted_mapping(excluded, kind):
    generation = _generation(excluded)
    compiled = _compile(cms_network_predicate(generation, kind, "r.resource_id"), generation)
    sql = str(compiled)
    reviewed, permitted = sql.split(" EXCEPT ")
    assert "r.resource_id NOT IN (WITH approved_network_bindings" in reviewed
    assert "AND TRUE" in reviewed
    assert "scope_excluded_network_ids" not in reviewed
    assert "NOT approved.network_id=ANY(CAST(%(scope_excluded_network_ids)s AS integer[]))" in permitted
    assert "CAST(%(scope_network_ids)s AS integer[]) IS NULL" in permitted
    assert compiled.params["scope_network_ids"] is None
    assert compiled.params["scope_excluded_network_ids"] == list(excluded)
    # EXCEPT keeps mixed 42/84 mappings; NOT IN retains identities absent from reviewed evidence.
    for branch in (reviewed, permitted):
        assert "legacy.source_id='cms-npd'" in branch
        assert "legacy.resource_type='Organization'" in branch
        assert "source_scope_json" in branch and "%(scope_alias)s" in branch
    assert compiled.params["scope_1"] == 7
    assert tuple(compiled.params[f"scope_{index}"] for index in range(2, 8)) == (
        "fhir",
        "cms-npd",
        "retained",
        "dataset",
        "producer",
        "edition",
    )
    if kind == "plans":
        assert compiled.params["dataset_id"] == "dataset"
        for branch in (reviewed, permitted):
            assert "proof.source_id='cms-npd' AND proof.release_id=%(release_id)s" in branch
            assert re.search(
                r"plan\.dataset_id=%\(dataset_id\)s(?:::VARCHAR)? AND plan\.resource_type='InsurancePlan'", branch
            )
            assert "plan.acquired_resource_sha256=proof.plan_payload_sha256" in branch
            assert "plan.payload_json::jsonb->'network_refs'" in branch


def test_default_edges_filter_only_proven_local_network_and_plan_identities():
    generation = _generation()
    predicate = cms_network_target_predicate(generation, "edge.kind", "edge.reference")
    compiled = _compile(predicate, generation)
    sql = str(compiled)
    assert sql.count("CASE WHEN") >= 2
    for kind, resource_type in (("networks", "Organization"), ("plans", "InsurancePlan")):
        assert f"CASE WHEN edge.kind='{kind}' AND edge.reference ~ '^{resource_type}/[A-Za-z0-9.-]{{1,64}}$'" in sql
    assert sql.count(" THEN TRUE  AND split_part(edge.reference,'/',2) NOT IN") == 2
    assert sql.count(" ELSE TRUE END") == 2
    for kind in ("organizations", "sites", "practitioner-roles", "providers"):
        assert f"edge.kind='{kind}'" not in sql
    assert compiled.params["scope_network_ids"] is None
    assert compiled.params["scope_excluded_network_ids"] == [84]


def test_default_target_filter_respects_the_callers_relationship_fields():
    generation = _generation()
    predicate = cms_network_target_predicate(generation, "edge.kind", "edge.reference", {"networks", "sites"})
    assert "edge.kind='networks'" in predicate
    assert "edge.kind='plans'" not in predicate
    assert "edge.kind='sites'" not in predicate


@pytest.mark.parametrize("excluded", [None, (84,)])
@pytest.mark.parametrize(
    "kind", ["networks", "plans", "organizations", "payers", "sites", "practitioner-roles", "providers"]
)
def test_explicit_selection_keeps_positive_membership_and_target_filters(excluded, kind):
    generation = _generation(excluded, [42])
    predicate = cms_network_predicate(generation, kind, "r.resource_id")
    assert predicate.startswith(" AND r.resource_id IN (WITH ")
    assert " EXCEPT " not in predicate
    compiled = _compile(predicate, generation)
    assert compiled.params["scope_network_ids"] == [42]
    if kind in {"sites", "practitioner-roles", "providers"}:
        assert '"canonical".network_membership_source retained' in predicate
    if kind != "payers":
        target = cms_network_target_predicate(generation, "edge.kind", "edge.reference", {kind})
        assert "CASE WHEN edge.kind=" not in target
        assert "edge.reference ~ '^" in target


def test_plan_query_default_argument_preserves_existing_selected_sql():
    scope = _generation(None, [42])["network_scope"]
    assert _selected_plans(scope) == _selected_plans(scope, include_all=False)
    assert "AND TRUE" in _selected_plans(scope, include_all=True)


@pytest.mark.asyncio
async def test_default_predicates_reach_alias_counts_and_relationship_numbering_before_paging():
    generation = _generation()
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    query = DirectoryRead("networks", "cms-npd", "entities", None, None, 1)
    await cms_entity_rows(session, '"retained"', query, generation)
    statement, parameters_by_name = session.execute.call_args.args
    entity_sql = str(statement)
    assert entity_sql.count("NOT IN (WITH approved_network_bindings") == 2
    assert entity_sql.index("NOT IN (WITH approved_network_bindings") < entity_sql.index(") AS identity_count")
    assert entity_sql.rindex("NOT IN (WITH approved_network_bindings") < entity_sql.index("ORDER BY b.network_id LIMIT")
    assert parameters_by_name["page_size"] == 2 and parameters_by_name["scope_network_ids"] is None
    query = DirectoryRead("practitioner-roles", "cms-npd", "relationships", None, None, 1)
    await cms_relationship_rows(session, '"retained"', query, generation, "role", None)
    statement, parameters_by_name = session.execute.call_args.args
    relationship_sql = str(statement)
    assert relationship_sql.index("CASE WHEN fields.target_kind='networks'") < relationship_sql.index(
        ") SELECT * FROM numbered WHERE position>"
    )
    assert "network_membership_source" not in relationship_sql
    assert parameters_by_name["page_size"] == 2 and parameters_by_name["scope_excluded_network_ids"] == [84]
