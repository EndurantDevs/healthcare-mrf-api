# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Status-event privacy boundaries for singleton direct PTG input."""

from __future__ import annotations

import pytest

from process.ptg_parts import frozen_rate_privacy
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    DIRECT_RATE_FILE_PUBLIC_MARKER,
)


@pytest.mark.parametrize("selector_field", ("in_network_url", "allowed_url"))
def test_direct_status_event_projects_nested_digest_without_frozen_marker(
    selector_field,
):
    """Retain only the opaque direct marker when a nested digest is valid."""

    private_url = "https://rates.example.test/direct.json.gz"
    private_source_key = "private-source-key"
    private_source_file_id = "private-source-file"
    direct_digest = "a" * 64
    private_raw_hash = "b" * 64
    status_event_by_field = {
        "params": {
            selector_field: private_url,
            "source_key": private_source_key,
            "source_file_id": private_source_file_id,
            DIRECT_RATE_FILE_INTENT_SHA256_FIELD: "invalid-outer-digest",
            "invalid_price_exclusion_policy": {
                "sources": [{"raw_source_sha256": private_raw_hash}]
            },
        },
        "metrics": {
            "message": f"processing {private_url} {private_source_key}",
        },
        "progress": (
            {
                "items": [
                    {DIRECT_RATE_FILE_INTENT_SHA256_FIELD: direct_digest}
                ]
            },
        ),
    }

    projected_event = frozen_rate_privacy.project_frozen_status_event(
        status_event_by_field
    )

    for section_name in ("params", "metrics"):
        section = projected_event[section_name]
        assert section[DIRECT_RATE_FILE_PUBLIC_MARKER] is True
        assert section[DIRECT_RATE_FILE_INTENT_SHA256_FIELD] == direct_digest
        assert "frozen_rate_file_set_protected" not in section
    assert projected_event["metrics"]["message"] == (
        "[protected frozen source]"
    )
    rendered = repr(projected_event)
    assert private_url not in rendered
    assert private_source_key not in rendered
    assert private_source_file_id not in rendered
    assert private_raw_hash not in rendered


@pytest.mark.parametrize("selector_field", ("in_network_url", "allowed_url"))
def test_ordinary_direct_status_event_redacts_without_digest_marker(
    selector_field,
):
    """Redact an ordinary direct selector without claiming signed evidence."""

    private_url = "https://rates.example.test/ordinary.json.gz"
    private_source_key = "ordinary-source-key"
    projected_event = frozen_rate_privacy.project_frozen_status_event(
        {
            "params": {
                selector_field: private_url,
                "source_key": private_source_key,
                "max_files": 1,
            },
            "metrics": {
                "message": f"processing {private_url} {private_source_key}",
            },
        }
    )

    assert projected_event["params"] == {"max_files": 1}
    assert projected_event["metrics"] == {
        "message": "[protected frozen source]"
    }
    assert DIRECT_RATE_FILE_PUBLIC_MARKER not in repr(projected_event)
