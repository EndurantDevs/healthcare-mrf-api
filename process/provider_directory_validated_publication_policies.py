# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed automatic-publication policy identities."""

AUTOMATIC_VALIDATED_PUBLICATION_POLICY = "automatic_after_verified_twin_v1"
AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY = (
    "automatic_after_generic_admission_seal_v1"
)
AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY = (
    "automatic_after_configured_generic_admission_bootstrap_v1"
)
AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY = (
    "automatic_after_reviewed_single_root_admission_v1"
)
AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY = (
    "automatic_after_reviewed_twin_root_activation_v1"
)

GENERIC_PUBLICATION_POLICIES = frozenset(
    {
        AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY,
        AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY,
    }
)
REVIEWED_PUBLICATION_POLICIES = frozenset(
    {
        AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY,
        AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY,
    }
)
