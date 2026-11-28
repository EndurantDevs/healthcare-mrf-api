# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite proof ACL recognition retains exact Source, body and profile bindings."""

import pytest

from tests.test_provider_directory_cms_callee_custody import _catalog_rows
from tests.test_provider_directory_cms_callee_custody import _verify as legacy_verify

_PROOFS = (
    "provider_directory_subset_abandonment_valid",
    "provider_directory_subset_terminal_disposition_valid",
    "provider_directory_terminal_root_retirement_evidence",
    "provider_directory_terminal_root_retirement_v2_evidence",
)


def test_legacy_role_labels_do_not_authorize_source2_proof_callers():
    rows = _catalog_rows("snapshot_schema")
    row = next(row for row in rows if row["definition"]["proname"] == _PROOFS[0])
    row["acl"].append([103, int(row["definition"]["proowner"]), "EXECUTE", False])
    with pytest.raises(ValueError, match="acl_changed"):
        legacy_verify(rows)
