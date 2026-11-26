# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit tests for source-aware automatic FHIR Organization network hygiene."""

from types import SimpleNamespace

import pytest

from process.registry_fhir_catalog_network_policy import (
    AUTOMATIC_NETWORK_HYGIENE_VERSION,
    HIGHMARK_HYGIENE_KIND,
    IBX_HYGIENE_KIND,
    SUPPORTED_NETWORK_HYGIENE_POLICIES,
    automatic_network_hygiene_kind,
    automatic_network_hygiene_policy,
    automatic_network_organizations_sql,
    hygiene_reprojected_generations,
    uses_network_targets_table,
)


@pytest.mark.parametrize(
    ("source_uri", "expected_kind"),
    [
        ("https://apicms.hmhs.com/fhir/v4/providers/HM", HIGHMARK_HYGIENE_KIND),
        ("https://apicms.hmhs.com/fhir/v4/providers/HMDE/", HIGHMARK_HYGIENE_KIND),
        ("HTTPS://APICMS.HMHS.COM/fhir/v4/providers/HMNY", HIGHMARK_HYGIENE_KIND),
        ("https://apicms.hmhs.com/fhir/v4/providers/HMWV", HIGHMARK_HYGIENE_KIND),
        ("https://apicms.hmhs.com/fhir/v4/providers/BCBSDE", HIGHMARK_HYGIENE_KIND),
        ("https://eapics.ibx.com/provider/v1/fhir", IBX_HYGIENE_KIND),
        ("HTTPS://EAPICS.IBX.COM/provider/v1/fhir/", IBX_HYGIENE_KIND),
        ("https://apicms.hmhs.com/fhir/v4/providers", None),
        ("https://fhir.example.test/provider-directory/v1", None),
        ("https://eapics.ibx.com/provider/v1/fhir/extra", None),
        (None, None),
        ("   ", None),
    ],
)
def test_automatic_hygiene_kind_classification(source_uri, expected_kind):
    descriptor_by_field = {"profile": "published_normalized", "source_uri": source_uri}
    assert automatic_network_hygiene_kind(descriptor_by_field) == expected_kind


def test_retained_or_non_dict_descriptors_skip_hygiene():
    retained_by_field = {
        "profile": "retained_normalized",
        "source_uri": "https://apicms.hmhs.com/fhir/v4/providers/HM",
    }
    assert automatic_network_hygiene_kind(retained_by_field) is None
    assert automatic_network_hygiene_kind("not-a-descriptor") is None
    assert AUTOMATIC_NETWORK_HYGIENE_VERSION == "fhir-automatic-org-hygiene-v1"
    highmark_policy = automatic_network_hygiene_policy(
        {"profile": "published_normalized", "source_uri": "https://apicms.hmhs.com/fhir/v4/providers/HM"}
    )
    ibx_policy = automatic_network_hygiene_policy(
        {"profile": "published_normalized", "source_uri": "https://eapics.ibx.com/provider/v1/fhir"}
    )
    assert highmark_policy in SUPPORTED_NETWORK_HYGIENE_POLICIES
    assert ibx_policy in SUPPORTED_NETWORK_HYGIENE_POLICIES
    assert automatic_network_hygiene_policy(retained_by_field) is None


def test_network_targets_table_usage_by_hygiene_kind():
    assert uses_network_targets_table(None) is True
    assert uses_network_targets_table(IBX_HYGIENE_KIND) is True
    assert uses_network_targets_table(HIGHMARK_HYGIENE_KIND) is False


def test_automatic_network_sql_clauses():
    default_sql = automatic_network_organizations_sql('"cat"', None)
    assert "fhir_catalog_network_targets" in default_sql
    assert "NOT ILIKE 'UCD-%'" not in default_sql
    assert "part_of_ref" not in default_sql

    highmark_sql = automatic_network_organizations_sql('"cat"', HIGHMARK_HYGIENE_KIND)
    assert "declared_network'='true'::jsonb" in highmark_sql
    assert "NOT ILIKE 'UCD-%'" in highmark_sql
    assert "fhir_catalog_network_targets" not in highmark_sql

    ibx_sql = automatic_network_organizations_sql('"cat"', IBX_HYGIENE_KIND)
    assert "fhir_catalog_network_targets" in ibx_sql
    assert "part_of_ref' IS NULL" in ibx_sql
    assert "Retail Chain" in ibx_sql


def test_hygiene_reprojected_generations_collects_matching_only():
    current_by_generation = {
        "b" * 64: SimpleNamespace(
            descriptor={"profile": "published_normalized", "source_uri": "https://eapics.ibx.com/provider/v1/fhir"}
        ),
        "a" * 64: SimpleNamespace(
            descriptor={
                "profile": "published_normalized",
                "source_uri": "https://apicms.hmhs.com/fhir/v4/providers/HM",
            }
        ),
        "c" * 64: SimpleNamespace(
            descriptor={"profile": "published_normalized", "source_uri": "https://fhir.example.test/v1"}
        ),
    }
    assert hygiene_reprojected_generations(current_by_generation) == ("a" * 64, "b" * 64)
