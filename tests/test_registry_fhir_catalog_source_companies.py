# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Registered source names are bounded metadata assertions, never company identities."""

from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_source_companies as companies
from process.registry_fhir_catalog_extraction import fhir_catalog_json_bytes


def source_company(**changes):
    return {
        "source_id": "alias-a",
        "endpoint_id": "endpoint-one",
        "display_name": "Example Coverage",
        "provenance": "source_registration",
        **changes,
    }


def source_descriptor():
    return {"endpoint_id": "endpoint-one", "source_aliases": ["alias-a", "alias-b"]}


def test_stored_source_companies_preserve_labels_and_allow_partial_aliases():
    records = [source_company(), source_company(source_id="alias-b", display_name="Example Mutual")]
    assert companies.validate_fhir_catalog_source_companies(records, descriptor=source_descriptor()) is records
    assert companies.validate_fhir_catalog_source_companies(records[:1], descriptor=source_descriptor()) == records[:1]
    assert companies.validate_fhir_catalog_source_companies([], descriptor=source_descriptor()) == []
    assert set(records[0]) == {"source_id", "endpoint_id", "display_name", "provenance"}


@pytest.mark.parametrize("records", [None, {}, (), [None], [{}], [source_company(company_id="unknown")]])
def test_stored_source_companies_require_a_list_of_closed_records(records):
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies(records)


@pytest.mark.parametrize("field", ["source_id", "endpoint_id", "display_name", "provenance"])
def test_stored_source_companies_require_every_field(field):
    record = source_company()
    del record[field]
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies([record])


@pytest.mark.parametrize("field", ["source_id", "endpoint_id", "display_name"])
@pytest.mark.parametrize(
    "value", [None, True, "", " ", " leading", "trailing ", "control\nname", "a\x7fb", "a\x85b", "\ud800"]
)
def test_source_text_refuses_blank_untrimmed_controls_and_invalid_utf8(field, value):
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies([source_company(**{field: value})])


@pytest.mark.parametrize("field,maximum", [("source_id", 128), ("endpoint_id", 64), ("display_name", 512)])
def test_source_text_limits_count_utf8_bytes(field, maximum):
    record = source_company(**{field: "é" * (maximum // 2)})
    assert companies.validate_fhir_catalog_source_companies([record]) == [record]
    record[field] += "a"
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies([record])


@pytest.mark.parametrize("value", [None, True, "", "organization", "source_registration "])
def test_source_provenance_has_one_exact_value(value):
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies([source_company(provenance=value)])


@pytest.mark.parametrize("source_ids", [("alias-a", "alias-a"), ("alias-b", "alias-a")])
def test_source_companies_reject_duplicates_and_unsorted_aliases(source_ids):
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies(
            [source_company(source_id=source_id) for source_id in source_ids]
        )


@pytest.mark.parametrize("changes", [{"source_id": "foreign-alias"}, {"endpoint_id": "foreign-endpoint"}])
def test_source_companies_must_match_the_admitted_descriptor(changes):
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies([source_company(**changes)], descriptor=source_descriptor())


@pytest.mark.parametrize("descriptor", [[], {}, {"source_aliases": "alias-a", "endpoint_id": "endpoint-one"}])
def test_source_companies_refuse_malformed_descriptor_alias_boundaries(descriptor):
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies([], descriptor=descriptor)


def test_source_company_count_is_bounded_without_truncation():
    records = [source_company(source_id=f"source-{index:04}") for index in range(1001)]
    assert companies.validate_fhir_catalog_source_companies(records[:1000]) == records[:1000]
    with pytest.raises(ValueError):
        companies.validate_fhir_catalog_source_companies(records)


def test_source_company_byte_bound_includes_json_framing_and_utf8(monkeypatch):
    records = [source_company(display_name="Example 界é")]
    exact = len(fhir_catalog_json_bytes(records))
    monkeypatch.setattr(companies, "DEFAULT_BYTES", exact)
    assert companies.validate_fhir_catalog_source_companies(records) == records
    monkeypatch.setattr(companies, "DEFAULT_BYTES", exact - 1)
    with pytest.raises(ValueError, match="source_companies_bounds"):
        companies.validate_fhir_catalog_source_companies(records)


@pytest.mark.asyncio
async def test_optional_callback_absence_differs_from_present_empty_labels():
    assert await companies.collect_fhir_catalog_source_companies({}, source_descriptor()) is None
    with pytest.raises(ValueError, match="registry_fhir_catalog_source_companies_unavailable"):
        await companies.collect_fhir_catalog_source_companies({}, source_descriptor(), required=True)
    callback = AsyncMock(return_value=())
    assert (
        await companies.collect_fhir_catalog_source_companies({"source_companies": callback}, source_descriptor()) == []
    )
    callback.assert_awaited_once_with(source_descriptor())


@pytest.mark.asyncio
async def test_source_callback_awaits_tuple_and_validates_stored_list():
    callback = AsyncMock(return_value=(source_company(),))
    export_by_name = {"source_companies": callback, "inventory": Mock(), "replay": Mock()}
    selected = source_descriptor()
    assert await companies.collect_fhir_catalog_source_companies(export_by_name, selected, required=True) == [
        source_company()
    ]
    callback.assert_awaited_once_with(selected)
    export_by_name["inventory"].assert_not_called()
    export_by_name["replay"].assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("output", [None, [], {}, [source_company()], (source_company(source_id="foreign-alias"),)])
async def test_source_callback_must_return_a_tuple_of_admitted_records(output):
    with pytest.raises(ValueError):
        await companies.collect_fhir_catalog_source_companies(
            {"source_companies": AsyncMock(return_value=output)}, source_descriptor()
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("callback", [None, (), Mock(return_value=(source_company(),))])
async def test_source_callback_must_be_callable_and_awaitable(callback):
    with pytest.raises(ValueError):
        await companies.collect_fhir_catalog_source_companies({"source_companies": callback}, source_descriptor())
