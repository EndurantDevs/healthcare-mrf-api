# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog component checks reject unsafe transport and unreviewed guard definitions."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.migration_generic_source_candidates import _canonical_copy_guard_bodies
from db.migration_provider_directory_dataset_candidates import candidate_functions
from process import provider_directory_rooted_graph_input_fence as fence
from process import registry_fhir_catalog_observed_source as observed
from tests.test_registry_fhir_catalog_observation_helpers import physical_family, read_profile, relation

SCHEMA = "snapshot_schema"


def _guard_function(name, body, security):
    return {
        "body": body,
        "language": "plpgsql",
        "definition": dict(
            oid="7",
            proname=name,
            prosecdef=security,
            prokind="f",
            pronargs=0,
            prorettype="2279",
            proisstrict=False,
            proleakproof=False,
            proparallel="u",
            proconfig=["search_path=pg_catalog"],
        ),
    }


def _reviewed_guards():
    deny = candidate_functions(SCHEMA).split('CREATE FUNCTION "snapshot_schema".deny_pd_dataset_candidate_write()', 1)[
        1
    ]
    deny = deny.split("AS $fn$", 1)[1].split("$fn$;", 1)[0]
    return [
        _guard_function("deny_pd_dataset_candidate_write", deny, False),
        _guard_function(
            "guard_pd_dataset_candidate_parent", _canonical_copy_guard_bodies(SCHEMA, "hp_snapshot_retention")[0], True
        ),
    ]


def test_only_reviewed_guard_bodies_and_attributes_authenticate():
    guards = _reviewed_guards()
    assert set(fence._authenticate_functions({"functions": guards}, SCHEMA)) == {
        "deny_pd_dataset_candidate_write",
        "guard_pd_dataset_candidate_parent",
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("body", "BEGIN RETURN NEW; END"),
        ("language", "sql"),
        ("prosecdef", True),
        ("prokind", "p"),
        ("prorettype", "16"),
        ("proconfig", ["search_path=public"]),
        ("proparallel", "s"),
    ],
)
def test_guard_attribute_changes_fail_closed(field, value):
    function = _reviewed_guards()[0]
    (function if field in {"body", "language"} else function["definition"])[field] = value
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        fence._authenticate_functions({"functions": [function]}, SCHEMA)


@pytest.mark.parametrize(
    "root,table_name,partition",
    [
        (fence._INPUTS[0], fence._INPUTS[0], False),
        (fence._INPUTS[1], "retained_payload", True),
        (fence._INPUTS[2], fence._INPUTS[2], False),
        (fence._INPUTS[3], fence._INPUTS[3] + "_legacy", False),
        (fence._INPUTS[3], "attached_work", True),
    ],
)
def test_every_input_route_requires_its_existing_guards(root, table_name, partition):
    table_by_field = dict(
        oid=1, kind="r", persistence="p", rls=False, name=table_name, partition=partition, attributes=[]
    )
    with pytest.raises(ValueError, match="source_acquisition_hooks_changed"):
        fence._authenticate_hooks({"tables": [table_by_field], "triggers": []}, {root: {1}}, {})
    table_by_field["rls"] = True
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        fence._authenticate_hooks({"tables": [table_by_field], "triggers": []}, {root: {1}}, {})


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "origin", "bypass_absent", "missing"])
async def test_capture_refuses_incomplete_or_unsafe_catalog(monkeypatch, damage):
    document_by_field = dict(
        origin=True,
        bypass_absent=True,
        tables=[{"oid": oid} for oid in range(1, 6)],
        functions=[{"body": "reviewed-body", "definition": {"prosrc": "digest"}}],
    )
    if damage == "missing":
        document_by_field["tables"].pop()
    elif damage is not None:
        document_by_field[damage] = False
    database = SimpleNamespace(scalar=AsyncMock(side_effect=[1, 2, 3, 4, json.dumps(document_by_field)]))
    authenticate = Mock(return_value={})
    monkeypatch.setattr(fence, "_authenticate_functions", authenticate)
    monkeypatch.setattr(fence, "_authenticate_hooks", Mock())
    if damage is not None:
        with pytest.raises(RuntimeError, match="predecessor_invalid"):
            await fence.capture_input_fence(database, SCHEMA, {fence._INPUTS[0]: {5}}, "edition")
        authenticate.assert_not_called()
        return
    captured = json.loads(await fence.capture_input_fence(database, SCHEMA, {fence._INPUTS[0]: {5}}, "edition"))
    assert captured["roots"][fence._INPUTS[0]] == [1, 5]
    assert "body" not in captured["catalog"]["functions"][0]
    assert database.scalar.call_args.kwargs == {"oids": [1, 2, 3, 4, 5], "dataset_id": "edition"}


def test_sql_consumers_keep_the_same_fence_projection():
    query = fence.input_fence_query(SCHEMA, "$1", "$2::oid[]", include_bodies=True)
    assert "__BODY_FIELD__" not in query and "__CONTROL_EPOCHS__" not in query
    assert "'body',p.prosrc" in query and "h.dataset_id=$1" in query
    projected = fence.root_input_fence_sql(SCHEMA, "$1", "$2::jsonb")
    assert "jsonb_each($2::jsonb)" in projected and "'body',p.prosrc" not in projected
    protected = fence.root_input_fence_sql(SCHEMA, "$1", "$2::jsonb", variant="rooted_combined")
    assert "preparation.handoff=p.candidate_storage_json" in protected
    assert "preparation.preparation_id" not in protected
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        fence.root_input_fence_sql(SCHEMA, "$1", "$2::jsonb", variant="unknown")


@pytest.mark.parametrize("selected_oid", [1, 2, 5, 99])
def test_frozen_exception_cannot_admit_headers_or_nested_leaves(selected_oid):
    catalog_rows = physical_family()
    if selected_oid == 5:
        catalog_rows[-1]["parents"] = [7]
        catalog_rows.append(relation(7, "intermediate", parents=(2,), kind="p"))
    roots = [(item["nspname"], item["relname"]) for item in catalog_rows if not item["parents"]]
    with pytest.raises(observed.FHIRCatalogObservedSourceError, match="physical_identity_invalid"):
        observed.fhir_catalog_relation_families(
            catalog_rows, roots, read_profile(), 10, frozen_resource_oids=frozenset({selected_oid})
        )
