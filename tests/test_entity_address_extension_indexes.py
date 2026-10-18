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
