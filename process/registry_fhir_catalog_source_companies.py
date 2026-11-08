# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded registered-source labels pinned to catalog metadata, without ownership claims."""

from inspect import isawaitable

from process.registry_fhir_catalog_extraction import DEFAULT_BYTES, MAX_ROWS, fhir_catalog_json_bytes, fhir_catalog_text

_FIELDS = {"source_id", "endpoint_id", "display_name", "provenance"}


def validate_fhir_catalog_source_companies(records, *, descriptor=None):
    """Validate one bounded source list without changing its assertions."""
    if type(records) is not list or len(records) > MAX_ROWS:
        raise ValueError("registry_fhir_catalog_source_companies_invalid")
    validate_fhir_catalog_source_company_records(records, descriptor=descriptor)
    if len(fhir_catalog_json_bytes(records)) > DEFAULT_BYTES:
        raise ValueError("registry_fhir_catalog_source_companies_bounds")
    return records


def validate_fhir_catalog_source_company_records(records, *, descriptor=None):
    """Validate closed, ordered labels; callers bound the containing payload."""
    if type(records) is not list:
        raise ValueError("registry_fhir_catalog_source_companies_invalid")
    if descriptor is not None and (
        type(descriptor) is not dict
        or type(descriptor.get("source_aliases")) is not list
        or type(descriptor.get("endpoint_id")) is not str
    ):
        raise ValueError("registry_fhir_catalog_source_companies_invalid")
    previous_source_id = None
    for company_by_field in records:
        _validate_source_company(company_by_field, descriptor)
        if previous_source_id is not None and company_by_field["source_id"] <= previous_source_id:
            raise ValueError("registry_fhir_catalog_source_companies_invalid")
        previous_source_id = company_by_field["source_id"]
    return records


def _validate_source_company(company_by_field, descriptor):
    """Require one closed registered label and its optional admitted endpoint/alias boundary."""
    if (
        type(company_by_field) is not dict
        or set(company_by_field) != _FIELDS
        or company_by_field["provenance"] != "source_registration"
    ):
        raise ValueError("registry_fhir_catalog_source_companies_invalid")
    for field, maximum in (("source_id", 128), ("endpoint_id", 64), ("display_name", 512)):
        text = company_by_field[field]
        if (
            type(text) is not str
            or not text
            or text != text.strip()
            or fhir_catalog_text(text, maximum, strip=False) != text
            or any(127 <= ord(character) <= 159 for character in text)
        ):
            raise ValueError("registry_fhir_catalog_source_companies_invalid")
    if descriptor is not None and (
        company_by_field["endpoint_id"] != descriptor["endpoint_id"]
        or company_by_field["source_id"] not in descriptor["source_aliases"]
    ):
        raise ValueError("registry_fhir_catalog_source_companies_invalid")


async def collect_fhir_catalog_source_companies(export, descriptor, *, required=False):
    """Await the optional source-owned callback and validate its closed tuple contract."""
    if "source_companies" not in export:
        if required:
            raise ValueError("registry_fhir_catalog_source_companies_unavailable")
        return None
    callback = export["source_companies"]
    if not callable(callback):
        raise ValueError("registry_fhir_catalog_source_companies_invalid")
    pending = callback(descriptor)
    if not isawaitable(pending):
        raise ValueError("registry_fhir_catalog_source_companies_invalid")
    records = await pending
    if type(records) is not tuple:
        raise ValueError("registry_fhir_catalog_source_companies_invalid")
    return validate_fhir_catalog_source_companies(list(records), descriptor=descriptor)
