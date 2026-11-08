# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep completed isolated candidates distinct from pending acquisition work."""

from .provider_directory_generic_source_policy import bound_publisher, current_source_storage_binding

_CANDIDATE_FIELDS = (
    "dataset_id",
    "endpoint_id",
    "acquisition_root_run_id",
    "import_run_id",
    "status",
    "dataset_hash",
    "resource_count",
    "completion_proof_sha256",
)


async def _has_published_candidate(candidate_by_field):
    if current_source_storage_binding() is None:
        return False
    publisher = bound_publisher()
    factory = publisher.factory
    verify = getattr(factory, "has_published_source_candidate", None)
    if verify is None:
        return False
    result = await verify(
        publisher.connection,
        publisher.policy,
        candidate_by_field={key: candidate_by_field[key] for key in _CANDIDATE_FIELDS},
    )
    if type(result) is not bool or bound_publisher() is not publisher or publisher.factory is not factory:
        raise RuntimeError("provider_directory_source_candidate_admission_changed")
    return result


async def locked_private_verification_state(directory, connection, dataset_ref, parameters_by_name):
    """Keep contenders locked; only the original factory can prove prior publication."""
    query = directory._private_verification_state_sql(dataset_ref)
    rejected_dataset_ids = []
    while True:
        state_record = await connection.first(query, **parameters_by_name, rejected_dataset_ids=rejected_dataset_ids)
        state_by_field = directory._pagination_checkpoint_row_mapping(state_record)
        if not state_by_field:
            return {}
        is_ordinary_validated = (
            not parameters_by_name["include_twin_states"]
            and state_by_field["status"] == directory.ENDPOINT_DATASET_VALIDATED
        )
        is_blocking = not is_ordinary_validated or directory._is_private_candidate_blocking(state_by_field)
        if is_ordinary_validated and is_blocking and await _has_published_candidate(state_by_field):
            rejected_dataset_ids.append(state_by_field["dataset_id"])
            continue
        if is_blocking:
            for field in ("import_run_id", "has_admission_receipt", "retired_candidate"):
                state_by_field.pop(field)
            if not parameters_by_name["include_twin_states"]:
                state_by_field["publication_metadata_json"] = None
            return state_by_field
        rejected_dataset_ids.append(state_by_field["dataset_id"])
