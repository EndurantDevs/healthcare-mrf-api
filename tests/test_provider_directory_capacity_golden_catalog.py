# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep signed capacity fixtures independent of source-catalog additions."""

from process import provider_directory_profile as profile
from tests import provider_directory_profile_capacity_signing_guard_test_support as support
from tests.test_provider_directory_profile_capacity_attestation import (
    _golden_body,
    _signed_envelope,
    _verify,
)


def test_catalog_additions_preserve_golden_but_change_current_selection(monkeypatch):
    original_execution = support.synthetic_profile_execution()
    original_golden = _golden_body()
    configured_ids = profile.configured_profile_source_ids()
    monkeypatch.setattr(
        profile,
        "configured_profile_source_ids",
        lambda: (*configured_ids, "synthetic_catalog_addition"),
    )

    changed_execution = support.synthetic_profile_execution()
    assert changed_execution.attestation.profile_input_digest != (original_execution.attestation.profile_input_digest)
    assert changed_execution.attestation.proof_id != original_execution.attestation.proof_id
    assert _golden_body() == original_golden
    assert _verify().attestation_id == original_golden["attestation_id"]

    envelope = _signed_envelope()
    task = support.synthetic_profile_task(envelope)
    signed_execution = envelope["lease"]["signing_preflight_guard"]["healthcare_request"]["profile_execution"]
    assert (
        task["provider_directory_profile_selection_attestation"]
        == (signed_execution["provider_directory_profile_selection_attestation"])
    )
    assert task["provider_directory_profile_capacity_attestation"] is envelope
