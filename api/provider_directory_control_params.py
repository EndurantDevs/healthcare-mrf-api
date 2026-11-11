# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pre-admission validation for proof-bearing Provider Directory operations."""

from process.provider_directory_profile_selection_contract import ProviderDirectoryProfileSelectionError


def validate_provider_directory_params(importer, params, *, profile_validator):
    """Validate exact rooted work and preserve existing Profile proof handling."""
    if "rooted_graph" in params:
        if importer != "provider-directory-fhir":
            raise ValueError("rooted_graph_requires_provider_directory_importer")
        from process.provider_directory_rooted_graph_worker import validated_rooted_graph_task

        validated_rooted_graph_task(params)
    if importer != "provider-directory-fhir" or not any(
        name in params
        for name in (
            "provider_directory_profile_contract_id",
            "provider_directory_profile_generation",
            "provider_directory_profile_selection_attestation",
        )
    ):
        return
    try:
        profile_validator(params)
    except ProviderDirectoryProfileSelectionError as error:
        raise ValueError(str(error)) from error
