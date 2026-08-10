# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Policy-bearing source selection for reviewed subset abandonment."""

from __future__ import annotations

from copy import deepcopy

import pytest

from process.provider_directory_fhir_subset_abandonment_contract import (
    ReviewedSubsetAbandonmentError,
)
from process.provider_directory_fhir_subset_abandonment_selection import (
    selected_reviewed_subset_abandonment,
)
from tests.provider_directory_fhir_subset_abandonment_support import (
    AbandonmentDatabase,
    RESOURCE_TYPES,
)


POLICY_ONE = {
    "policy_version": "provider-directory-reviewed-root-policy-v1",
    "required_root_count": 1,
}


def _policy_database() -> AbandonmentDatabase:
    database = AbandonmentDatabase()
    database.source_row["metadata_json"].update(
        provider_directory_candidate_status=(
            "pending_reviewed_subset_acquisition"
        ),
        provider_directory_reviewed_root_policy_v1=deepcopy(POLICY_ONE),
    )
    database.candidate_row["publication_metadata_json"][
        "provider_directory_reviewed_root_policy_v1"
    ] = deepcopy(POLICY_ONE)
    return database


@pytest.mark.asyncio
async def test_policy_one_pending_source_selects_exact_candidate():
    selection, _checkpoint_rows = await selected_reviewed_subset_abandonment(
        _policy_database(),
        "source-a",
        RESOURCE_TYPES,
    )

    assert selection.prior_status == "failed"


@pytest.mark.asyncio
async def test_policy_one_source_rejects_candidate_policy_drift():
    database = _policy_database()
    database.candidate_row["publication_metadata_json"][
        "provider_directory_reviewed_root_policy_v1"
    ]["required_root_count"] = 2

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(
            database,
            "source-a",
            RESOURCE_TYPES,
        )

    assert error.value.code == "evidence"
