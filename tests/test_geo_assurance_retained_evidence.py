# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy evidence must stay on the explicitly retained address dependencies."""

import copy

import pytest

from api import ptg2_geo_projection as projection


def _bindings(schema="mrf"):
    return {
        f"{namespace or schema}.{name}": {
            "schema_name": "held_spatial" if namespace else "held_provider",
            "table_name": "retained_" + name,
            "relation_oid": 100 + ordinal,
            "relfilenode": 200 + ordinal,
        }
        for ordinal, (namespace, name) in enumerate(projection._PROJECTION_DEPENDENCIES)
    }


@pytest.mark.parametrize("schema", ["mrf", "synthetic_provider"])
def test_retained_evidence_uses_every_explicit_source_and_exact_anchor(schema):
    bindings = _bindings(schema)
    original = copy.deepcopy(bindings)
    sql = projection.legacy_evidence_source_id_sql(
        "office",
        schema_name=schema,
        dependency_bindings=bindings,
        unified_schema_name="held_offices",
        unified_table_name="approved_addresses",
    )
    assert bindings == original
    assert f"FROM {schema}." not in sql
    assert 'FROM "held_provider"."retained_npi_address" AS geo_nppes' in sql
    assert 'FROM "held_provider"."retained_mrf_address" AS geo_mrf' in sql
    assert 'FROM "held_provider"."retained_doctor_clinician_address" AS geo_doctor' in sql
    assert 'FROM "held_offices"."approved_addresses" AS geo_nppes_anchor' in sql
    assert 'JOIN "held_provider"."retained_npi_address" AS geo_nppes_anchor_source' in sql
    assert "geo_nppes.address_key = office.address_key" in sql
    assert "geo_nppes_anchor.premise_key = office.premise_key" in sql
    assert "geo_nppes_anchor_source.date_added IS NOT NULL" in sql
    assert "geo_mrf.source_issuer_names" in sql
    assert "geo_doctor.updated_at IS NOT NULL" in sql


def test_retained_dependencies_do_not_require_moving_the_unified_anchor():
    sql = projection.legacy_evidence_source_id_sql("office", schema_name="mrf", dependency_bindings=_bindings())
    assert "FROM mrf.entity_address_unified AS geo_nppes_anchor" in sql
    assert 'JOIN "held_provider"."retained_npi_address" AS geo_nppes_anchor_source' in sql


@pytest.mark.parametrize(
    "defect", ["missing", "extra", "duplicate_oid", "duplicate_relation", "identifier", "bool_oid"]
)
def test_retained_evidence_refuses_incomplete_or_unsafe_dependencies(defect):
    bindings = _bindings()
    if defect == "missing":
        del bindings["tiger.zip_state"]
    if defect == "extra":
        bindings["mrf.unreviewed"] = bindings["mrf.npi_address"]
    if defect == "duplicate_oid":
        bindings["mrf.mrf_address"]["relation_oid"] = bindings["mrf.npi_address"]["relation_oid"]
    if defect == "duplicate_relation":
        bindings["mrf.mrf_address"]["table_name"] = bindings["mrf.npi_address"]["table_name"]
    if defect == "identifier":
        bindings["mrf.npi_address"]["schema_name"] = 'held"; SELECT 1; --'
    if defect == "bool_oid":
        bindings["mrf.npi_address"]["relation_oid"] = True
    with pytest.raises(ValueError):
        projection.legacy_evidence_source_id_sql(
            "office", schema_name="mrf", dependency_bindings=bindings, unified_schema_name="held_offices"
        )


@pytest.mark.parametrize("field", ["unified_schema_name", "unified_table_name"])
@pytest.mark.parametrize("identifier", ["", "a.b", 'a"; SELECT 1; --', "a" * 64])
def test_retained_anchor_accepts_only_simple_bounded_identifiers(field, identifier):
    anchor_by_field = {
        "unified_schema_name": "held_offices",
        "unified_table_name": "approved_addresses",
        field: identifier,
    }
    with pytest.raises(ValueError):
        projection.legacy_evidence_source_id_sql(
            "office", schema_name="mrf", dependency_bindings=_bindings(), **anchor_by_field
        )
