# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cohort migration names and logical CHECK normalization."""

import re

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


def _simple_check(expression):
    """Ignore only grouping and explicit built-in scalar casts in the fixed small CHECK grammar."""
    expression = re.sub(r"::(?:text|bigint|charactervarying)(?:\[\])?", "", expression)
    expression = re.sub(r"'([0-9]+)'", r"\1", expression)
    return expression.replace("(", "").replace(")", "")
