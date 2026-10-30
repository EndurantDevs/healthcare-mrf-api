# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address index SQL must resolve extensions under the original Source search path."""

import importlib

import pytest

address = importlib.import_module("process.entity_address_unified")


@pytest.mark.parametrize(
    "name,elements",
    (
        ("canonical_network_ids", ("canonical_network_ids public.gin__int_ops",)),
        (
            "taxonomy_plans_network",
            ("taxonomy_array public.gin__int_ops", "plans_network_array public.gin__int_ops"),
        ),
        ("service_plans_network_array", ("plans_network_array public.gin__int_ops",)),
        (
            "serving_zip5_taxonomy",
            ("(COALESCE(zip5, LEFT(COALESCE(postal_code, ''), 5)))", "taxonomy_array public.gin__int_ops"),
        ),
        ("procedures_array", ("procedures_array public.gin__int_ops",)),
        ("medications_array", ("medications_array public.gin__int_ops",)),
        (
            "geo_idx",
            ("public.Geography(public.ST_MakePoint((long)::double precision, (lat)::double precision))",),
        ),
        (
            "geo_taxonomy",
            (
                "public.Geography(public.ST_MakePoint((long)::double precision, (lat)::double precision))",
                "taxonomy_array public.gist__intbig_ops",
            ),
        ),
    ),
)
def test_stage_and_live_index_plans_resolve_exact_public_extensions(name, elements):
    """Keep predicates, index options and opclasses shared by both real builders."""
    indexes = address.EntityAddressUnified.__my_additional_indexes__
    index = next(entry for entry in indexes if entry["name"] == name)
    assert index["index_elements"] == elements
    staged_by_name = dict(
        address._stage_index_statements(
            address.EntityAddressUnified, "index_schema", indexes, {"complete_stage_indexes": True}
        )
    )
    live_statements, skipped = address._post_publish_index_plan("index_schema", "all", build_concurrently=False)
    assert not skipped
    live_by_name = dict(live_statements)
    for statement in (staged_by_name[name], live_by_name[name]):
        assert f"USING {index['using']} ({', '.join(elements)})" in statement
        if index.get("where"):
            assert statement.endswith(f" WHERE {index['where']};")


@pytest.mark.parametrize("model", [address.EntityAddressProcedureBridge, address.EntityAddressMedicationBridge])
@pytest.mark.parametrize("option", [None, "false"])
def test_prepared_support_builds_the_complete_validated_index_set(monkeypatch, model, option):
    from tests.test_entity_address_candidate_preparation_postgres import _inputs

    if option is None:
        monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", option)
    index = next(entry for entry in model.__my_additional_indexes__ if entry["name"] == "code_location")
    assert not address._is_stage_index_enabled(model, index)
    token = address.candidate_preparation._PREPARATION.set(_inputs())
    try:
        statements_by_name = dict(address._stage_index_statements(model, "index_schema", [index], {}))
        assert "code_location" in statements_by_name
        assert "CREATE INDEX" in statements_by_name["code_location"]
    finally:
        address.candidate_preparation._PREPARATION.reset(token)
    assert not address._is_stage_index_enabled(model, index)
