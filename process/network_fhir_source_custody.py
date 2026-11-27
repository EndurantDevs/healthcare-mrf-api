# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for retained CMS guards and the partitioned source layout."""

from __future__ import annotations

from process.provider_directory_cms_routine_settings import approved_cms_routine_settings

_TABLES = (
    "provider_directory_endpoint_dataset",
    "provider_directory_dataset_resource",
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_source_binding",
    "provider_directory_insurance_network_plan_evidence",
    "provider_directory_cms_npd_resource_witness",
    "provider_directory_cms_npd_relationship",
    "provider_directory_cms_npd_relationship_receipt",
    "provider_directory_cms_candidate_coverage",
    "provider_directory_cms_serving_coverage",
)
_GUARDS = {
    "provider_directory_endpoint_dataset": {
        "tin_npi_connector_endpoint_dataset_guard": 31,
        "provider_directory_endpoint_dataset_admission_seal_guard": 23,
        "provider_directory_endpoint_dataset_admission_raw_guard": 19,
        "cms_npd_covered_truncate_forbidden": 34,
    },
    "provider_directory_dataset_resource": {
        "cms_npd_published_resource_immutable": 4,
        "cms_npd_published_resource_update_immutable": 16,
        "cms_npd_published_resource_delete_immutable": 8,
        "cms_npd_covered_truncate_forbidden": 34,
    },
    "provider_directory_cms_npd_resource_witness": {
        "cms_npd_resource_witness_insert_guard": 4,
        "cms_npd_resource_witness_guard": 27,
        "cms_npd_resource_witness_no_truncate": 34,
    },
    "provider_directory_cms_npd_relationship": {
        "cms_npd_published_relationship_insert_immutable": 4,
        "cms_npd_published_relationship_immutable": 27,
        "cms_npd_relationship_truncate_forbidden": 34,
    },
    "provider_directory_cms_npd_relationship_receipt": {
        "cms_npd_relationship_receipt_insert_guard": 4,
        "cms_npd_relationship_receipt_mutation_guard": 27,
        "cms_npd_relationship_receipt_no_truncate": 34,
    },
    "provider_directory_cms_candidate_coverage": {
        "cms_npd_candidate_coverage_insert": 7,
        "cms_npd_candidate_coverage_immutable": 58,
    },
    "provider_directory_cms_serving_coverage": {
        "cms_npd_serving_coverage_immutable": 27,
        "cms_npd_covered_truncate_forbidden": 34,
    },
}
for _table in _TABLES[2:6]:
    _GUARDS[_table] = {"cms_npd_serving_evidence_immutable": 27, "cms_npd_covered_truncate_forbidden": 34}
_GUARDS["provider_directory_insurance_network_plan_evidence"]["cms_npd_covered_network_witness_immutable"] = 4
_GUARD_FUNCTIONS = {
    "tin_npi_connector_endpoint_dataset_guard": "guard_tin_npi_connector_endpoint_dataset",
    "provider_directory_endpoint_dataset_admission_seal_guard": "guard_provider_directory_endpoint_dataset_admission_seal",
    "provider_directory_endpoint_dataset_admission_raw_guard": "guard_provider_directory_endpoint_dataset_admission_seal",
    "cms_npd_covered_truncate_forbidden": "guard_cms_npd_covered_truncate",
    "cms_npd_published_resource_immutable": "guard_cms_npd_published_resource",
    "cms_npd_published_resource_update_immutable": "guard_cms_npd_published_resource",
    "cms_npd_published_resource_delete_immutable": "guard_cms_npd_published_resource",
    "cms_npd_resource_witness_insert_guard": "guard_cms_npd_resource_witness_insert",
    "cms_npd_resource_witness_guard": "guard_cms_npd_resource_witness",
    "cms_npd_resource_witness_no_truncate": "guard_cms_npd_resource_witness",
    "cms_npd_published_relationship_insert_immutable": "guard_cms_npd_published_relationship_insert",
    "cms_npd_published_relationship_immutable": "guard_cms_npd_published_relationship",
    "cms_npd_relationship_truncate_forbidden": "guard_cms_npd_relationship_truncate",
    "cms_npd_relationship_receipt_insert_guard": "guard_cms_npd_relationship_receipt_insert",
    "cms_npd_relationship_receipt_mutation_guard": "guard_cms_npd_relationship_receipt_mutation",
    "cms_npd_relationship_receipt_no_truncate": "guard_cms_npd_relationship_truncate",
    "cms_npd_candidate_coverage_insert": "guard_cms_npd_candidate_coverage_insert",
    "cms_npd_candidate_coverage_immutable": "guard_cms_npd_serving_coverage",
    "cms_npd_serving_coverage_immutable": "guard_cms_npd_serving_coverage",
    "cms_npd_serving_evidence_immutable": "guard_cms_npd_serving_evidence",
    "cms_npd_covered_network_witness_immutable": "guard_cms_npd_covered_network_witness",
}
_UPDATE_COLUMNS = {
    "tin_npi_connector_endpoint_dataset_guard": (
        "dataset_id",
        "endpoint_id",
        "import_run_id",
        "acquisition_root_run_id",
        "previous_dataset_id",
        "dataset_hash",
        "status",
        "is_current",
        "resource_count",
        "created_at",
        "validated_at",
        "published_at",
        "superseded_at",
        "publication_metadata_json",
        "completion_proof_required_version",
        "completion_proof_json",
        "completion_proof_sha256",
    ),
    "provider_directory_endpoint_dataset_admission_seal_guard": (
        "publication_metadata_summary_json",
        "publication_metadata_sha256",
        "content_proof_admission_version",
        "content_proof_admission_kind",
        "content_proof_admission_sha256",
        "content_proof_resource_types",
    ),
    "provider_directory_endpoint_dataset_admission_raw_guard": ("publication_metadata_json",),
}
_TRANSITION_TABLES = {
    "cms_npd_published_resource_immutable": (None, "changed_new"),
    "cms_npd_published_resource_update_immutable": ("changed_old", "changed_new"),
    "cms_npd_published_resource_delete_immutable": ("changed_old", None),
    "cms_npd_resource_witness_insert_guard": (None, "inserted_witness_rows"),
    "cms_npd_published_relationship_insert_immutable": (None, "inserted"),
    "cms_npd_relationship_receipt_insert_guard": (None, "inserted"),
    "cms_npd_covered_network_witness_immutable": (None, "inserted_witnesses"),
}


# Language, volatility, security definer and settings of routines the retained guards reach.
_REACHABLE_FUNCTIONS = {
    "guard_cms_npd_candidate_coverage_insert": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_covered_network_witness": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_covered_truncate": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_published_relationship": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_published_relationship_insert": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_published_resource": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_relationship_receipt_insert": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_relationship_receipt_mutation": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_relationship_truncate": (
        "plpgsql",
        "v",
        False,
        None,
    ),
    "guard_cms_npd_resource_witness": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_resource_witness_insert": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_serving_coverage": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_cms_npd_serving_evidence": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "guard_pd_endpoint_dataset_subset_replay_evidence": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_pd_uhc_flex_practitioner_dataset_parent": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_endpoint_dataset_admission_seal": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_endpoint_dataset_admission_truncate": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_exact_logical_current": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_reviewed_subset_activation_dataset": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_rooted_graph_dependency": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_subset_abandonment_child": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_subset_abandonment_dataset": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_terminal_root_retirement_child": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_provider_directory_terminal_root_retirement_parent": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_tin_npi_connector_dataset_resource": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "guard_tin_npi_connector_endpoint_dataset": (
        "plpgsql",
        "v",
        True,
        ("search_path=pg_catalog",),
    ),
    "pd_entity_redirect_parent_guard": (
        "plpgsql",
        "v",
        False,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_endpoint_dataset_admission_metadata_sha256": (
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_rooted_graph_dataset_intrinsic_valid": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_abandonment_valid": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_canonical_sha256": (
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_completion_canonical_json": (
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_completion_proof_pair_valid": (
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_completion_proof_shape_valid": (
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_content_proof_valid": (
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_coverage_shape_valid": (
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_payload_canonical_json": (
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_payload_sha256": (
        "sql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_replay_evidence_shape_valid": (
        "plpgsql",
        "i",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_terminal_disposition_v4_valid": (
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_terminal_disposition_v5_valid": (
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_subset_terminal_disposition_valid": (
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_eligible": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_evidence": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog", "TimeZone=UTC"),
    ),
    "provider_directory_terminal_root_retirement_marker_valid": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_relation_evidence": (
        "plpgsql",
        "s",
        True,
        ("search_path=pg_catalog", "TimeZone=UTC"),
    ),
    "provider_directory_terminal_root_retirement_v2_eligible": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_terminal_root_retirement_v2_evidence": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog", "TimeZone=UTC"),
    ),
    "provider_directory_terminal_root_retirement_v2_marker_valid": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_uhc_flex_practitioner_dataset_ready": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
    "provider_directory_uhc_flex_practitioner_dataset_valid": (
        "sql",
        "s",
        True,
        ("search_path=pg_catalog",),
    ),
}


class FHIRSourceCustodyError(ValueError):
    """Retained source custody is unavailable or has changed."""


def _require_function(function_record, schema, owner_oid):
    """Recognize a reachable routine by name, language, volatility, definer and settings.

    Bodies and owners are not compared: PostgreSQL grants decide who may replace a routine.
    """
    expected = _REACHABLE_FUNCTIONS.get(function_record["proname"])
    if (
        expected is None
        or (function_record["lanname"], function_record["provolatile"], function_record["prosecdef"]) != expected[:3]
        or function_record["proconfig"]
        not in approved_cms_routine_settings(None if expected[3] is None else list(expected[3]))
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


def _require_guard(guard_record, schema, owner_oid, name, event_mask):
    """Bind a native trigger's complete invocation to its reviewed guard function shape."""
    if guard_record is None:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    function_name = _GUARD_FUNCTIONS[name]
    if function_name == "guard_tin_npi_connector_endpoint_dataset":
        _require_function(guard_record, schema, owner_oid)
    expected_arguments = b"raw\x00" if name == "provider_directory_endpoint_dataset_admission_raw_guard" else b""
    is_security_definer = function_name in {
        "guard_provider_directory_endpoint_dataset_admission_seal",
        "guard_tin_npi_connector_endpoint_dataset",
    }
    settings = None if function_name == "guard_cms_npd_relationship_truncate" else ["search_path=pg_catalog"]
    if (
        guard_record["tgtype"] != event_mask
        or guard_record["tgenabled"] not in {"O", "A"}
        or not guard_record["unconditional"]
        or guard_record["tgdeferrable"]
        or guard_record["tginitdeferred"]
        or tuple(guard_record["update_columns"]) != _UPDATE_COLUMNS.get(name, ())
        or (guard_record["tgoldtable"], guard_record["tgnewtable"]) != _TRANSITION_TABLES.get(name, (None, None))
        or guard_record["tgnargs"] != bool(expected_arguments)
        or bytes(guard_record["tgargs"]) != expected_arguments
        or guard_record["nspname"] != schema
        or guard_record["proname"] != function_name
        or guard_record["pronargs"] != 0
        or guard_record["lanname"] != "plpgsql"
        or not guard_record["returns_trigger"]
        or guard_record["prosecdef"] is not is_security_definer
        or guard_record["proconfig"] not in approved_cms_routine_settings(settings)
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")


async def _generic_custody_family(connection, source_pin, owner_role, runtime_roles, source_policy):
    """Verify the Reader or exact bound publisher before checking child custody."""
    from process.provider_directory_generic_source_provision import verify_generic_source_read_family

    if source_policy is None:
        family_rows = await verify_generic_source_read_family(
            connection, source_pin.schema_name, owner_role, runtime_roles
        )
    else:
        from process.provider_directory_generic_source_policy import bound_publisher
        from process.provider_directory_generic_source_provision import verify_generic_source_family

        publisher = bound_publisher()
        if (
            publisher.connection is not connection
            or publisher.policy is not source_policy
            or (source_policy.schema, source_policy.owner_role, source_policy.runtime_roles)
            != (source_pin.schema_name, owner_role, runtime_roles)
        ):
            raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
        family_rows = await verify_generic_source_family(connection, source_policy)
    return family_rows


async def _generic_partitioned_custody(connection, source_pin, owner_role, runtime_roles, *, source_policy=None):
    """Authenticate every child and the exact published acquisition binding."""
    from db.plan_release_storage import storage_relation
    from process.provider_directory_generic_source_storage import (
        HEADER,
        RELATIONS,
        _json,
        _validate_binding,
    )

    family_rows = await _generic_custody_family(connection, source_pin, owner_role, runtime_roles, source_policy)
    header = await connection.fetchrow(
        f"SELECT endpoint_id,acquisition_root_run_id,candidate_storage_json FROM "
        f"{storage_relation(source_pin.schema_name, HEADER)} WHERE dataset_id=$1 AND endpoint_id=$2 AND dataset_hash=$3",
        source_pin.dataset_id,
        source_pin.endpoint_id,
        source_pin.dataset_sha256,
    )
    if header is None:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    binding = _json(header["candidate_storage_json"])
    if type(binding) is not dict or type(binding.get("identity")) is not dict:
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    identity = binding["identity"]
    _validate_binding(binding, identity, phases=("published",))
    if (
        identity["dataset_id"] != source_pin.dataset_id
        or identity["endpoint_id"] != source_pin.endpoint_id
        or identity["acquisition_root_run_id"] != header["acquisition_root_run_id"]
        or identity["metadata"].get("source_ids") != ["cms-npd"]
        or identity["metadata"].get("source_release", {}).get("vector_sha256") != source_pin.release_id
    ):
        raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    by_name = {relation_by_field["relname"]: relation_by_field for relation_by_field in family_rows}
    for root in RELATIONS:
        # Named and stored legacy proofs both bind each published child by its recorded name.
        relation_by_field = by_name.get(binding["tables"][root]["name"])
        if relation_by_field is None or list(relation_by_field["parents"]) != [by_name[root]["oid"]]:
            raise FHIRSourceCustodyError("fhir_source_custody_unavailable")
    return {"binding": binding, "family": [dict(relation_by_field) for relation_by_field in family_rows]}
