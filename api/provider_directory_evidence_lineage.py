# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve existing address provenance to exact immutable evidence datasets."""

from collections import defaultdict

from sqlalchemy import text

from api.provider_directory_dataset_sql import _selected_dataset_metadata_ctes_sql
from api.provider_directory_profile_lineage import profile_source_datasets


def _rows(result):
    return [getattr(evidence_row, "_mapping", evidence_row) for evidence_row in result.all()]


async def _overlay_dataset_rows(npi, references, *, session):
    schema = npi._runtime_db_schema()
    return _rows(
        await npi._execute_stmt(
            text(f"""
        WITH {npi._directory_current_dataset_ctes_sql(schema)}, requested_overlay_refs AS (
            SELECT source_record_id, npi, address_key, phone_number
              FROM unnest(CAST(:source_record_ids AS varchar[]), CAST(:npis AS bigint[]), CAST(:address_keys AS uuid[]), CAST(:phone_numbers AS varchar[]))
                   AS requested(source_record_id, npi, address_key, phone_number)
        )
        SELECT overlay.source_id, overlay.resource_type, overlay.resource_id,
               dataset.dataset_id, dataset.endpoint_id, dataset.canonical_api_base
          FROM requested_overlay_refs AS requested
          JOIN {npi._schema_cache_key("provider_directory_address_overlay")} AS overlay
            ON overlay.source_record_id = requested.source_record_id
           AND overlay.npi = requested.npi
           AND ((requested.address_key IS NOT NULL AND overlay.address_key = requested.address_key)
                OR (requested.phone_number IS NOT NULL AND overlay.phone_number = requested.phone_number))
          JOIN current_datasets AS dataset
            ON dataset.source_id = overlay.source_id
           AND dataset.run_id = overlay.last_seen_run_id
          JOIN {schema}.provider_directory_dataset_resource AS resource
            ON resource.dataset_id = dataset.dataset_id
           AND resource.resource_type = overlay.resource_type
           AND resource.resource_id = overlay.resource_id
        """),
            session=session,
            params={
                "source_record_ids": [record_id for record_id, _npi, _address, _phone in references],
                "npis": [provider_npi for _record, provider_npi, _address, _phone in references],
                "address_keys": [address for _record, _npi, address, _phone in references],
                "phone_numbers": [phone for _record, _npi, _address, phone in references],
            },
        )
    )


async def _profile_dataset_rows(npi, generation_id, *, session):
    source_datasets = await profile_source_datasets(npi, generation_id, session=session)
    schema = npi._runtime_db_schema()
    rows = _rows(
        await npi._execute_stmt(
            text(f"""
        WITH {_selected_dataset_metadata_ctes_sql(schema, "")}
        SELECT source_id, dataset_id, endpoint_id, canonical_api_base
          FROM current_dataset_metadata
        """),
            session=session,
            params={
                "evidence_source_ids": [source_id for source_id, _dataset in source_datasets],
                "evidence_dataset_ids": [dataset for _source, dataset in source_datasets],
            },
        )
    )
    if {(evidence_row["source_id"], evidence_row["dataset_id"]) for evidence_row in rows} != set(source_datasets):
        raise RuntimeError("provider_directory_evidence_retained_input_missing")
    return rows


def _unique_source_datasets(keys, resource_type, overlay_rows, historical_rows):
    by_key = defaultdict(set)
    for evidence_row in overlay_rows:
        if evidence_row["resource_type"] == resource_type:
            by_key[(evidence_row["source_id"], evidence_row["resource_id"])].add(evidence_row["dataset_id"])
    historical_by_source = {evidence_row["source_id"]: evidence_row["dataset_id"] for evidence_row in historical_rows}
    by_source = defaultdict(set)
    for key in keys:
        datasets = by_key.get(key) or ({historical_by_source[key[0]]} if key[0] in historical_by_source else set())
        if len(datasets) != 1:
            raise RuntimeError("provider_directory_evidence_lineage_unavailable")
        by_source[key[0]].update(datasets)
    if any(len(datasets) != 1 for datasets in by_source.values()):
        raise RuntimeError("provider_directory_evidence_lineage_ambiguous")
    return tuple(sorted((source_id, next(iter(datasets))) for source_id, datasets in by_source.items()))


def _address_references(npi, addresses, phone_digits):
    references = set()
    record_ids = set()
    for address in addresses:
        if not isinstance(address, dict):
            continue
        record_ids.update(
            str(record_id)
            for record_id in npi._provider_directory_record_ids_from_address(address)
            if str(record_id).startswith("provider_directory_fhir:")
        )
        if not str(address.get("npi", "")).isdigit() or not 1000000000 <= int(address["npi"]) <= 9999999999:
            continue
        if address.get("address_key"):
            for record_id in address.get("source_record_ids") or ():
                if str(record_id).startswith("provider_directory_fhir:"):
                    references.add((str(record_id), int(address["npi"]), str(address["address_key"]), None))
        if phone_digits:
            for record_id in address.get("phone_source_record_ids") or ():
                if str(record_id).startswith("provider_directory_fhir:"):
                    references.add((str(record_id), int(address["npi"]), None, str(phone_digits)))
    limit = npi.MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS * npi.MAX_PROVIDER_DIRECTORY_ROLE_REFERENCE_DETAILS
    if len(record_ids) > limit or len(references) > limit:
        raise RuntimeError("provider_directory_evidence_scope_too_large")
    return sorted(references, key=lambda reference: tuple(str(part or "") for part in reference))


async def resolve_address_evidence(
    npi, addresses, *, profile_generation_id=None, include_role_evidence=True, phone_digits=None, session=None
):
    """Keep observed overlay roots; use exact Profile history only when needed."""
    references = _address_references(npi, addresses, phone_digits)
    overlay_rows = await _overlay_dataset_rows(npi, references, session=session) if references else []
    role_keys = npi._provider_directory_role_keys_from_addresses(addresses) if include_role_evidence else []
    affiliation_keys = (
        npi._provider_directory_affiliation_keys_from_addresses(addresses) if include_role_evidence else []
    )
    observed_keys = {
        (evidence_row["source_id"], evidence_row["resource_type"], evidence_row["resource_id"])
        for evidence_row in overlay_rows
    }
    missing_keys = {(source_id, "PractitionerRole", resource) for source_id, resource in role_keys}
    missing_keys.update((source_id, "OrganizationAffiliation", resource) for source_id, resource in affiliation_keys)
    missing_sources = set(npi._provider_directory_source_ids_from_addresses(addresses)) - {
        evidence_row["source_id"] for evidence_row in overlay_rows
    }
    historical_rows = (
        await _profile_dataset_rows(npi, profile_generation_id, session=session)
        if missing_keys - observed_keys or missing_sources
        else []
    )
    role_datasets = _unique_source_datasets(role_keys, "PractitionerRole", overlay_rows, historical_rows)
    affiliation_datasets = _unique_source_datasets(
        affiliation_keys, "OrganizationAffiliation", overlay_rows, historical_rows
    )
    bindings = defaultdict(set)
    for evidence_row in [*overlay_rows, *historical_rows]:
        bindings[evidence_row["source_id"]].add((evidence_row["endpoint_id"], evidence_row["canonical_api_base"]))
    if any(len(source_bindings) != 1 for source_bindings in bindings.values()):
        raise RuntimeError("provider_directory_evidence_endpoint_ambiguous")
    return (
        {source_id: next(iter(source_bindings)) for source_id, source_bindings in bindings.items()},
        role_datasets,
        affiliation_datasets,
    )


def bind_source_details(detail_by_id, source_ids, bindings):
    """Preserve genuine aliases and their recorded endpoint identity."""
    if any(source_id not in detail_by_id or source_id not in bindings for source_id in source_ids):
        raise RuntimeError("provider_directory_evidence_source_alias_unavailable")
    bound_detail_by_id = dict(detail_by_id)
    for source_id, (endpoint_id, canonical_api_base) in bindings.items():
        if source_id in bound_detail_by_id:
            bound_detail_by_id[source_id] = {
                **bound_detail_by_id[source_id],
                "endpoint_id": endpoint_id,
                "canonical_api_base": canonical_api_base,
            }
    return bound_detail_by_id
