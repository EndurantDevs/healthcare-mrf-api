from __future__ import annotations

import pytest

from api.ptg2_candidate_audit_capacity import retain_unique_integer_keys


def test_unique_integer_keys_reject_nonpositive_count_limit():
    with pytest.raises(ValueError, match="limit must be positive"):
        retain_unique_integer_keys(
            (),
            None,
            category="test keys",
            maximum_count=0,
        )
