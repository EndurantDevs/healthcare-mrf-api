# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed validation edges for Provider Profile capacity geometry."""

from __future__ import annotations

import dataclasses
import datetime

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_attestation as attestation
from process import provider_directory_profile_capacity_geometry as geometry
from process import (
    provider_directory_profile_capacity_geometry_contract as geometry_contract,
)
from process import (
    provider_directory_profile_capacity_runtime_config as runtime_config,
)
from process import (
    provider_directory_profile_capacity_runtime_geometry as runtime_geometry,
)
from process import (
    provider_directory_profile_capacity_trust_config as trust_config,
)
from process import (
    provider_directory_profile_capacity_trust_validation as trust_validation,
)
from process.provider_directory_profile_capacity_types import (
    _MAX_SIGNED_BIGINT,
)
from tests.provider_directory_profile_capacity_trust_fixtures import (
    capacity_trust,
)
from tests.test_provider_directory_profile_capacity import (
    _geometry_payload,
    _relation_byte_caps,
)
from tests.test_provider_directory_profile_capacity_attestation import (
    VALIDATION_TIME,
)
from tests.test_provider_directory_profile_capacity_projection import (
    _projection_geometry,
)
from tests.test_provider_directory_profile_capacity_runtime import (
    _geometry_inputs,
    _limits_payload,
)
from tests.test_provider_directory_profile_capacity_trust import (
    _active_key,
    _trust_payload,
)


UTC = datetime.timezone.utc


def _assert_capacity_errors(error_type, failure_operations):
    for failure_operation in failure_operations:
        with pytest.raises(error_type):
            failure_operation()


@pytest.mark.parametrize(
    ("operation", "error"),
    (
        (
            lambda: runtime_config._positive_integer(
                False,
                field_name="max_profile_rows",
            ),
            "max_profile_rows_invalid",
        ),
        (
            lambda: runtime_config._relation_byte_caps([]),
            "relation_byte_caps_invalid",
        ),
        (
            lambda: runtime_config._validated_relation_cap(
                {},
                "profile_target",
            ),
            "relation_byte_caps_invalid",
        ),
        (
            lambda: runtime_config.configured_capacity_limits("{"),
            "json_invalid",
        ),
    ),
)
def test_capacity_limit_configuration_rejects_invalid_edges(operation, error):
    with pytest.raises(
        runtime_config.ProviderDirectoryProfileCapacityConfigurationError,
        match=error,
    ):
        operation()


def test_runtime_geometry_requires_typed_limits_and_inputs():
    limits = runtime_config.validated_capacity_limits(_limits_payload())
    inputs = _geometry_inputs()
    error_type = (
        runtime_config.ProviderDirectoryProfileCapacityConfigurationError
    )

    with pytest.raises(error_type, match="limits_type_invalid"):
        runtime_geometry.build_capacity_geometry({}, inputs)
    with pytest.raises(error_type, match="inputs_type_invalid"):
        runtime_geometry.build_capacity_geometry(limits, {})


@pytest.mark.parametrize(
    ("operation", "error"),
    (
        (
            lambda: trust_config._trust_text(
                None,
                field_name="environment_id",
            ),
            "trust_environment_id_invalid",
        ),
        (
            lambda: trust_config._trust_digest(
                None,
                field_name="release_digest",
            ),
            "trust_release_digest_invalid",
        ),
        (
            lambda: trust_config._trust_database_name(None),
            "trust_database_name_invalid",
        ),
        (
            lambda: trust_config._trust_system_identifier("0"),
            "trust_database_system_identifier_invalid",
        ),
        (
            lambda: trust_config._trust_public_key("z" * 64),
            "trust_public_key_invalid",
        ),
        (
            lambda: trust_config._trust_timestamp(
                "2026-02-30T12:00:00Z",
                field_name="retired_at",
            ),
            "trust_retired_at_invalid",
        ),
    ),
)
def test_trust_configuration_rejects_noncanonical_scalars(operation, error):
    with pytest.raises(
        runtime_config.ProviderDirectoryProfileCapacityConfigurationError,
        match=error,
    ):
        operation()


def test_trust_configuration_rejects_rotation_and_storage_drift():
    active_key_by_field = {
        **_active_key(),
        "retired_at": "2026-07-30T12:00:00Z",
    }
    retired_key_by_field = {
        **_active_key(),
        "status": "retired",
        "retired_at": "2026-07-30T12:00:00Z",
        "verify_until": "2026-07-30T12:00:00Z",
    }
    rotation_cases = (
        (active_key_by_field, "active_retirement_invalid"),
        ({**_active_key(), "status": "unknown"}, "status_invalid"),
        (retired_key_by_field, "retirement_window_invalid"),
    )
    error_type = (
        runtime_config.ProviderDirectoryProfileCapacityConfigurationError
    )

    for key_by_field, error in rotation_cases:
        with pytest.raises(error_type, match=error):
            trust_config._trust_rotation(key_by_field)
    _assert_capacity_errors(
        error_type,
        (
            lambda: trust_config._trust_tablespace_entry(
                {},
                expected_usage="data",
            ),
            lambda: trust_config._validated_trust_tablespaces([]),
            lambda: trust_config._validated_trust_volumes([]),
        ),
    )


def test_trust_configuration_rejects_storage_binding_mismatch():
    tablespace = attestation.CapacityLeaseTrustTablespace(
        tablespace_name="pg_default",
        tablespace_oid=1663,
        usage="data",
        volume_digest="11" * 32,
    )
    volume = attestation.CapacityLeaseTrustVolume(
        volume_class="data",
        volume_digest="22" * 32,
    )

    with pytest.raises(
        runtime_config.ProviderDirectoryProfileCapacityConfigurationError,
        match="storage_binding_invalid",
    ):
        trust_config._assert_trust_storage_binding(
            (tablespace,),
            (volume,),
        )


def test_validated_trust_rejects_unsorted_active_identity():
    trust_by_field = _trust_payload(
        active_key_id="key-b",
        keys=[_active_key("key-b"), _active_key("key-a")],
    )

    with pytest.raises(
        runtime_config.ProviderDirectoryProfileCapacityConfigurationError,
        match="key_order_or_active_invalid",
    ):
        trust_config.validated_capacity_lease_trust(trust_by_field)


def test_runtime_trust_rejects_key_and_rotation_drift():
    valid_trust = capacity_trust()
    valid_key = valid_trust.keys[0]
    retired_key = dataclasses.replace(
        valid_key,
        status="retired",
        retired_at=datetime.datetime(2026, 7, 30, 12, tzinfo=UTC),
        verify_until=datetime.datetime(2026, 7, 30, 12, tzinfo=UTC),
    )
    unsorted_key = dataclasses.replace(valid_key, key_id="a-key")
    late_key = dataclasses.replace(valid_key, key_id="z-key")

    _assert_capacity_errors(
        attestation.ProviderDirectoryCapacityLeaseError,
        (
            lambda: trust_validation._trust_key_entry(None),
            lambda: trust_validation._trust_key_entry(
                dataclasses.replace(valid_key, status="unknown")
            ),
            lambda: trust_validation._trust_key_entry(
                dataclasses.replace(valid_key, public_key=b"short")
            ),
            lambda: trust_validation._assert_rotation_metadata(
                dataclasses.replace(
                    valid_key,
                    retired_at=datetime.datetime(2026, 7, 30, tzinfo=UTC),
                )
            ),
            lambda: trust_validation._assert_rotation_metadata(retired_key),
            lambda: trust_validation._validated_trust_keys(
                dataclasses.replace(valid_trust, keys=())
            ),
            lambda: trust_validation._validated_trust_keys(
                dataclasses.replace(valid_trust, keys=("not-a-key",))
            ),
            lambda: trust_validation._validated_trust_keys(
                dataclasses.replace(
                    valid_trust,
                    keys=(late_key, unsorted_key),
                )
            ),
            lambda: trust_validation._assert_active_key(
                dataclasses.replace(valid_trust, active_key_id="other-key")
            ),
        ),
    )


def test_runtime_trust_rejects_storage_identity_drift():
    valid_trust = capacity_trust()
    data_tablespace, temp_tablespace = valid_trust.tablespaces
    data_volume, temp_volume, wal_volume = valid_trust.volumes
    failure_operations = (
        lambda: trust_validation._validated_trust_tablespace(
            None,
            expected_usage="data",
        ),
        lambda: trust_validation._validated_trust_tablespace(
            temp_tablespace,
            expected_usage="data",
        ),
        lambda: trust_validation._validated_trust_volume(
            None,
            expected_class="data",
        ),
        lambda: trust_validation._assert_trust_storage(
            dataclasses.replace(valid_trust, tablespaces=())
        ),
        lambda: trust_validation._assert_trust_storage(
            dataclasses.replace(valid_trust, volumes=())
        ),
        lambda: trust_validation._assert_trust_storage(
            dataclasses.replace(
                valid_trust,
                tablespaces=(
                    data_tablespace,
                    dataclasses.replace(
                        temp_tablespace,
                        tablespace_name="other_space",
                    ),
                ),
            )
        ),
        lambda: trust_validation._assert_trust_storage(
            dataclasses.replace(
                valid_trust,
                volumes=(
                    data_volume,
                    dataclasses.replace(
                        temp_volume,
                        volume_digest="99" * 32,
                    ),
                    wal_volume,
                ),
            )
        ),
        lambda: trust_validation.capacity_trust_key_for_assigned_lease(
            object(),
            {},
            now=VALIDATION_TIME,
        ),
    )
    _assert_capacity_errors(
        attestation.ProviderDirectoryCapacityLeaseError,
        failure_operations,
    )


@pytest.mark.parametrize(
    ("operation", "error"),
    (
        (
            lambda: geometry_contract._exact_text(
                {"field": " padded "},
                "field",
                maximum_length=20,
            ),
            "field_invalid",
        ),
        (
            lambda: geometry_contract._database_system_identifier(
                {"database_system_identifier": "0"}
            ),
            "database_system_identifier_invalid",
        ),
        (
            lambda: geometry_contract._assert_relation_cap_shape(
                "unknown_relation",
                {
                    "max_scratch_bytes": 0,
                    "max_target_growth_bytes": 1,
                    "max_deleted_logical_bytes": 1,
                    "max_temp_bytes": 1,
                    "max_wal_bytes": 1,
                },
            ),
            "relation_name_invalid",
        ),
    ),
)
def test_geometry_contract_rejects_noncanonical_identity(operation, error):
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=error,
    ):
        operation()


def test_geometry_rechecks_session_and_postgres_runtime():
    valid_geometry = _projection_geometry()
    failure_operations = (
        lambda: geometry._assert_wave_geometry(
            dataclasses.replace(
                valid_geometry,
                pool_reserve_connections=1,
            )
        ),
        lambda: geometry._assert_postgres_storage_limits(
            dataclasses.replace(
                valid_geometry,
                postgres_wal_segment_size_bytes=8193,
            )
        ),
        lambda: geometry._assert_postgres_wal_limits(
            dataclasses.replace(
                valid_geometry,
                postgres_full_page_writes=False,
            )
        ),
        lambda: geometry._assert_postgres_wal_limits(
            dataclasses.replace(valid_geometry, postgres_wal_level="minimal")
        ),
        lambda: geometry._assert_postgres_wal_limits(
            dataclasses.replace(
                valid_geometry,
                postgres_default_toast_compression="unknown",
            )
        ),
        lambda: geometry._assert_postgres_wal_limits(
            dataclasses.replace(
                valid_geometry,
                postgres_wal_compression="unknown",
            )
        ),
    )
    _assert_capacity_errors(
        capacity.ProviderDirectoryProfileCapacityError,
        failure_operations,
    )


def test_geometry_rechecks_metadata_identity_and_payload_type():
    valid_geometry = _projection_geometry()
    valid_payload = _geometry_payload()
    failure_operations = (
        lambda: geometry._assert_metadata_capacity_limits(
            dataclasses.replace(
                valid_geometry,
                metadata_data_upper_bound_bytes=1,
            )
        ),
        lambda: geometry._validate_scalar_identity(
            {
                **valid_payload,
                "physical_projection_contract_id": "wrong-contract",
            }
        ),
        lambda: geometry._validate_scalar_flags(
            {
                **valid_payload,
                "postgres_full_page_writes": 1,
            }
        ),
        lambda: geometry.capacity_geometry_payload(None),
    )
    _assert_capacity_errors(
        capacity.ProviderDirectoryProfileCapacityError,
        failure_operations,
    )


def test_geometry_rejects_aggregate_storage_reservation_overflow():
    relation_caps = _relation_byte_caps()
    for relation_cap in relation_caps[:2]:
        relation_cap["max_scratch_bytes"] = _MAX_SIGNED_BIGINT
    geometry_by_field = _geometry_payload(relation_byte_caps=relation_caps)

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="storage_class_reservation_overflow",
    ):
        geometry.validated_capacity_geometry(geometry_by_field)
