# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep synthetic native capacity leases bound to their actual storage observation."""

from copy import deepcopy
from datetime import datetime

from tests import cms_registry_complete_publication_postgres_support as native
from tests import provider_directory_profile_capacity_signing_guard_test_support as signing
from tests.provider_directory_cms_capacity_test_support import rehash_guard
from tests.test_provider_directory_profile_capacity_attestation import _golden_body


def test_native_signer_preserves_bound_storage_observation_without_changing_golden_input():
    original = _golden_body()
    guard = deepcopy(original["signing_preflight_guard"])
    receipt = guard["healthcare_receipt"]
    receipt["database_binding"] = {
        **{name: original[name] for name in ("database_system_identifier", "database_oid", "database_name")},
        **{name: original["tablespaces"][0][name] for name in ("tablespace_oid", "tablespace_name")},
    }
    receipt["runtime_observation"].update(
        **{
            name: original["runtime_witness"][name]
            for name in native.runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
        }
    )
    rehash_guard(guard)
    timing = signing._GuardTiming(
        *(
            datetime.fromisoformat(original[name].replace("Z", "+00:00"))
            for name in ("observed_at", "issued_at", "expires_at", "max_build_deadline")
        ),
        receipt["request_nonce"],
    )
    signed = native.sign_native_receipt(guard, receipt, timing, reservation="example-native-reservation")
    observed_by_class = {
        volume["volume_class"]: volume for volume in guard["control_plane_request"]["storage_observation"]["volumes"]
    }
    required = receipt["required_reservation_bytes_by_storage_class"]
    assert signed["lease"]["volumes"] == [
        {**observed_by_class[volume["volume_class"]], "reserved_bytes": required[volume["volume_class"]]}
        for volume in original["volumes"]
    ]
    assert _golden_body() == original
