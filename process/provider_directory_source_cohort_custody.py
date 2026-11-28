# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cohort migration names and logical CHECK normalization."""

HEADER = "provider_directory_uhc_flex_npi_cohort"
MEMBER = "provider_directory_uhc_flex_npi_member"
HISTORY = MEMBER + "_legacy"
_HEADER_GUARD = "guard_provider_directory_uhc_flex_npi_cohort"
_MEMBER_GUARD = "guard_provider_directory_uhc_flex_npi_member"
_INSERT_GUARD = "validate_provider_directory_uhc_flex_npi_members"


def _history_hooks(legacy):
    return (
        {
            "pd_uhc_flex_npi_member_guard": (_MEMBER_GUARD, 27, (), None, ()),
            "pd_uhc_flex_npi_member_guard_truncate": (_MEMBER_GUARD, 34, (), None, ()),
            "pd_uhc_flex_npi_member_insert_guard": (_INSERT_GUARD, 4, (), "new_rows", ()),
        }
        if legacy
        else {}
    )
