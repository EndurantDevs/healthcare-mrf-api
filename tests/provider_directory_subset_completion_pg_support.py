# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic proof builders for the disposable subset-v3 PostgreSQL proof."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json

from process.provider_directory_fhir_subset_completion import (
    build_subset_replay_evidence,
    canonical_payload_sha256,
    canonical_sha256,
)
from process.provider_directory_fhir_subset_identity import (
    CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
    CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD,
    SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
    server_issued_subset_source_scope_payload,
)
from tests.provider_directory_fhir_subset_completion_support import (
    CUTOFF,
    PAGE_COUNT,
    build_subset_contract,
    build_proof_pair,
)


RESOURCE_TYPES = (
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)


def _valid_resource_rows(*, alternate=False):
    resource_rows = []
    for resource_type in RESOURCE_TYPES:
        resource_id = f"{resource_type.lower()}-a"
        payload_by_field = {
            "id": resource_id,
            "resource_type": resource_type,
            "fhir_fetch_url": "/private/transport-coordinate",
        }
        if resource_type == "Organization":
            payload_by_field["résumé"] = {
                "display": "Žluťoučký 医療",
                "enabled": True,
                "missing": None,
                "score": 1.0,
                "signed_zero": -0.0,
                "values": [1, 1.25, "line\nvalue"],
            }
        if alternate and resource_type == "Organization":
            payload_by_field["name"] = "Different synthetic content"
        hash_payload_by_field = {
            key: field_value
            for key, field_value in payload_by_field.items()
            if key not in {
                "resource_url",
                "fhir_self_url",
                "fhir_fetch_url",
                "fhir_fetch_mode",
            }
        }
        resource_rows.append(
            {
                "resource_type": resource_type,
                "resource_id": resource_id,
                "payload_json": payload_by_field,
                "payload_hash": canonical_payload_sha256(
                    hash_payload_by_field
                ),
                "acquired_resource_sha256": canonical_sha256(
                    {
                        "resourceType": resource_type,
                        "id": resource_id,
                        **(
                            {"name": "Different synthetic content"}
                            if alternate and resource_type == "Organization"
                            else {}
                        ),
                    }
                ),
            }
        )
    return tuple(resource_rows)


VALID_RESOURCE_ROWS = _valid_resource_rows()
ALTERNATE_RESOURCE_ROWS = _valid_resource_rows(alternate=True)


def _legacy_subset_contract():
    return build_subset_contract(
        strategy_version=SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        completion_scopes=SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    )


def valid_source_metadata(candidate_status, *, contract=None):
    selected_contract = contract or _legacy_subset_contract()
    canonical_base = "https://directory.example.test/fhir"
    return {
        "provider_directory_supported_resources": list(RESOURCE_TYPES),
        "provider_directory_fully_enumerable_resources": [],
        "provider_directory_expected_nonempty_resources": list(RESOURCE_TYPES),
        "provider_directory_resource_page_count_caps": {
            resource_type: PAGE_COUNT for resource_type in RESOURCE_TYPES
        },
        "provider_directory_acquisition_enabled": True,
        "provider_directory_coverage_mode": selected_contract.semantics,
        "provider_directory_manual_only": True,
        "provider_directory_server_issued_subset_resources": list(
            RESOURCE_TYPES
        ),
        CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD: selected_contract.semantics,
        CURRENT_VERSION_CENSUS_CONTRACT_VERSION_FIELD: (
            selected_contract.contract_version
        ),
        CURRENT_VERSION_CENSUS_PAGE_COUNT_FIELD: selected_contract.page_count,
        CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD: (
            selected_contract.strategy_version
        ),
        CURRENT_VERSION_CENSUS_TRAVERSAL_VERSION_FIELD: (
            selected_contract.traversal_version
        ),
        CURRENT_VERSION_CENSUS_CANONICALIZATION_VERSION_FIELD: (
            selected_contract.canonicalization_version
        ),
        CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD: list(
            selected_contract.completion_scopes
        ),
        CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD: (
            selected_contract.continuation_strategy
        ),
        CURRENT_VERSION_CENSUS_START_URLS_FIELD: dict(
            selected_contract.start_urls
        ),
        "provider_directory_verification_campaign_id": (
            selected_contract.campaign_id
        ),
        "provider_directory_configured_endpoint_id": "endpoint-a",
        "provider_directory_candidate_status": candidate_status,
        "provider_directory_confirmed_base": canonical_base,
    }


def valid_source_record(candidate_status, *, contract=None):
    return {
        "source_id": "synthetic-source",
        "endpoint_id": "endpoint-a",
        "canonical_api_base": "https://directory.example.test/fhir",
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "metadata_json": valid_source_metadata(
            candidate_status,
            contract=contract,
        ),
    }


VALID_SOURCE_SCOPE_SHA256 = canonical_sha256(
    server_issued_subset_source_scope_payload(
        valid_source_record("pending_two_matching_reviewed_subset_acquisitions"),
        ("synthetic-source",),
        CUTOFF,
        "https://directory.example.test/fhir",
    )
)


def _line_sha256(values):
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()


def _content_bound_proof(proof_by_field, resource_rows):
    proof_by_field = deepcopy(proof_by_field)
    identity_by_type = {}
    acquired_entry_by_type = {resource_type: [] for resource_type in RESOURCE_TYPES}
    for resource_row in resource_rows:
        resource_type = resource_row["resource_type"]
        identity_by_type[resource_type] = canonical_sha256_identity(
            resource_row
        )
        acquired_entry_by_type[resource_type].append(
            {
                "resource_id": resource_row["resource_id"],
                "sha256": resource_row["acquired_resource_sha256"],
            }
        )
    resource_hash_by_type = {
        resource_type: _line_sha256([identity_by_type[resource_type]])
        for resource_type in RESOURCE_TYPES
    }
    acquired_hash_by_type = {
        resource_type: canonical_sha256(acquired_entry_by_type[resource_type])
        for resource_type in RESOURCE_TYPES
    }
    dataset_hash = _line_sha256(
        [identity_by_type[resource_type] for resource_type in RESOURCE_TYPES]
    )
    proof_by_field["dataset"].update(
        hash=dataset_hash,
        resource_hashes=resource_hash_by_type,
        acquired_resource_hashes=acquired_hash_by_type,
    )
    for resource_type in RESOURCE_TYPES:
        proof_by_field["resources"][resource_type].update(
            content_sha256=resource_hash_by_type[resource_type],
            acquired_content_sha256=acquired_hash_by_type[resource_type],
        )
    return proof_by_field


def canonical_sha256_identity(row):
    """Return the compact projected identity consumed by dataset hashing."""

    return json.dumps(
        [row["resource_type"], row["resource_id"], row["payload_hash"]],
        sort_keys=True,
        separators=(",", ":"),
    )


def valid_evidence_pairs(*, rows=VALID_RESOURCE_ROWS, contract=None):
    proof_by_field, proof_sha256, execution_proof_by_type = build_proof_pair(
        contract=contract or _legacy_subset_contract(),
    )
    proof_by_field = _content_bound_proof(proof_by_field, rows)
    proof_sha256 = canonical_sha256(proof_by_field)
    replay_by_field, replay_sha256 = build_subset_replay_evidence(
        resource_proof_by_type=execution_proof_by_type,
        completion_proof=proof_by_field,
        completion_sha256=proof_sha256,
    )
    return proof_by_field, proof_sha256, replay_by_field, replay_sha256


def malformed_proof_pair():
    proof_by_field, _, _, _ = valid_evidence_pairs()
    proof_by_field = deepcopy(proof_by_field)
    proof_by_field["resources"] = {
        resource_type: {} for resource_type in RESOURCE_TYPES
    }
    proof_by_field["dataset"]["resource_hashes"] = {}
    proof_by_field["dataset"]["resource_counts"] = {}
    proof_by_field["dataset"]["acquired_resource_hashes"] = {}
    return proof_by_field, canonical_sha256(proof_by_field)


def malformed_replay_pair(completion_sha256):
    replay_by_field = {
        "version": "provider-directory-fhir-server-issued-replay-evidence-v1",
        "completion_proof_sha256": completion_sha256,
        "resources": {},
    }
    return replay_by_field, canonical_sha256(replay_by_field)


def twin_proof(proof_by_field, proof_sha256, root_run_id):
    return {
        "endpoint_id": "endpoint-a",
        "acquisition_root_run_id": root_run_id,
        "source_ids": ["synthetic-source"],
        "selected_resources": list(RESOURCE_TYPES),
        "expected_resources": list(RESOURCE_TYPES),
        "verification_campaign_id": proof_by_field["campaign_id"],
        "verification_source_scope_hash": VALID_SOURCE_SCOPE_SHA256,
        "dataset_hash": proof_by_field["dataset"]["hash"],
        "resource_count": proof_by_field["dataset"]["count"],
        "resource_hashes": proof_by_field["dataset"]["resource_hashes"],
        "resource_counts": proof_by_field["dataset"]["resource_counts"],
        "completion_proof": proof_by_field,
        "completion_proof_sha256": proof_sha256,
    }


def coverage_from_proof(proof_by_field, proof_sha256, twin_state):
    """Build the exact sanitized coverage projection for one terminal row."""

    coverage_by_resource = {
        resource_type: _resource_coverage(
            proof_by_field["cutoff"],
            resource_proof,
        )
        for resource_type, resource_proof in proof_by_field["resources"].items()
    }
    return {
        "cutoff": proof_by_field["cutoff"],
        "scope": "server_issued_traversal_subset",
        **{
            field_name: sum(
                resource_proof[field_name]
                for resource_proof in proof_by_field["resources"].values()
            )
            for field_name in (
                "advertised_pre",
                "advertised_post",
                "returned_unique",
                "deficit",
            )
        },
        "resources": coverage_by_resource,
        "traversal_complete": True,
        "twin_state": twin_state,
        "proof_sha256": proof_sha256,
        "unresolved_reference_count": None,
        "unresolved_reference_counts": None,
        "missing_target_semantics": "preserved_not_synthesized",
        "absence_semantics": "unknown_under_subset",
        "publication_state_at_completion": "not_published",
    }


def _resource_coverage(cutoff, resource_proof):
    return {
        "cutoff": cutoff,
        "scope": "server_issued_traversal_subset",
        **{
            field_name: resource_proof[field_name]
            for field_name in (
                "advertised_pre",
                "advertised_post",
                "returned_unique",
                "deficit",
            )
        },
        "geometry": {
            "pages": resource_proof["pages"],
            "logical_terminal_offset": resource_proof[
                "logical_terminal_offset"
            ],
            "sparse_pages": resource_proof["sparse_pages"],
            "empty_pages": resource_proof["empty_pages"],
            "page_entry_counts_sha256": canonical_sha256(
                resource_proof["page_entry_counts"]
            ),
            "geometry_sha256": resource_proof["geometry_sha256"],
        },
        "continuation": {
            "validated_hops": resource_proof["pages"] - 1,
            "chain_sha256": resource_proof[
                "continuation_shape_chain_sha256"
            ],
        },
        "twin_state": "pending_matching_reviewed_root",
        "proof_state": "resource_terminal_verified",
        "unresolved_reference_count": None,
        "absence_semantics": "unknown_under_subset",
    }


def _resource_diagnostics_from_proof(proof_by_field):
    """Build the production-shaped diagnostics required at publication."""

    return {
        resource_type: {
            "server_issued_subset_completeness": {
                "cutoff": proof_by_field["cutoff"],
                "page_count": proof_by_field["page_count"],
                "campaign_id": proof_by_field["campaign_id"],
                **{
                    field_name: resource_proof[field_name]
                    for field_name in (
                        "advertised_pre",
                        "advertised_post",
                        "returned_unique",
                        "deficit",
                    )
                },
            }
        }
        for resource_type, resource_proof in proof_by_field["resources"].items()
    }


def terminal_metadata(
    proof_by_field,
    proof_sha256,
    replay_by_field,
    replay_sha256,
    root_run_id,
    *,
    baseline_dataset_id=None,
    baseline_root_run_id=None,
    mismatch_fields=None,
):
    """Build exact baseline-or-candidate metadata for one direct SQL proof."""

    is_candidate = baseline_dataset_id is not None
    role = "verification_candidate" if is_candidate else "baseline_candidate"
    verification_by_field = {
        "role": "verification_candidate" if is_candidate else "baseline",
        "admission_role": role,
        "result": (
            "mismatch"
            if mismatch_fields
            else ("matched" if is_candidate else "baseline_recorded")
        ),
        "proof": twin_proof(proof_by_field, proof_sha256, root_run_id),
    }
    if is_candidate:
        verification_by_field.update(
            baseline_dataset_id=baseline_dataset_id,
            baseline_acquisition_root_run_id=baseline_root_run_id,
            mismatch_fields=list(mismatch_fields or ()),
        )
    return {
        "acquisition_root_run_id": root_run_id,
        "requires_twin_root_verification": True,
        "verification_campaign_id": proof_by_field["campaign_id"],
        "verification_source_scope_hash": VALID_SOURCE_SCOPE_SHA256,
        "verification_role": role,
        "verification_baseline_dataset_id": baseline_dataset_id,
        "source_ids": ["synthetic-source"],
        "selected_resources": list(RESOURCE_TYPES),
        "expected_resources": list(RESOURCE_TYPES),
        "twin_root_verification_v1": verification_by_field,
        "server_issued_subset_replay_evidence": replay_by_field,
        "server_issued_subset_replay_evidence_sha256": replay_sha256,
        "server_issued_subset_coverage": coverage_from_proof(
            proof_by_field,
            proof_sha256,
            verification_by_field["result"],
        ),
        "resource_diagnostics": _resource_diagnostics_from_proof(
            proof_by_field
        ),
    }


def terminal_sql(scenario, dataset_id):
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET dataset_hash = $1, status = $2::varchar,
               validated_at = CASE WHEN $2::varchar = 'validated'
                                   THEN transaction_timestamp()
                                   ELSE validated_at END,
               publication_metadata_json = $3::jsonb,
               completion_proof_json = $4::jsonb,
               completion_proof_sha256 = $5
         WHERE dataset_id = '{dataset_id}'
    """


def terminal_parameters(
    proof_by_field,
    proof_sha256,
    metadata_by_field,
    status,
):
    return (
        proof_by_field["dataset"]["hash"],
        status,
        json.dumps(metadata_by_field),
        json.dumps(proof_by_field),
        proof_sha256,
    )


def invalid_cutoff_evidence_pairs():
    proof_by_field, _, replay_by_field, _ = valid_evidence_pairs()
    proof_by_field = deepcopy(proof_by_field)
    proof_by_field["cutoff"] = "2026-02-30T12:00:00.000000Z"
    proof_sha256 = canonical_sha256(proof_by_field)
    replay_by_field = deepcopy(replay_by_field)
    replay_by_field["completion_proof_sha256"] = proof_sha256
    return (
        proof_by_field,
        proof_sha256,
        replay_by_field,
        canonical_sha256(replay_by_field),
    )
