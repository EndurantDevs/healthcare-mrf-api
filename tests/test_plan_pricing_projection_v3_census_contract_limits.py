"""Limit and database-attribution gates for the projection-v3 census."""

from copy import deepcopy

import pytest

from scripts.research import plan_pricing_projection_v3_census as census
from scripts.research import plan_pricing_projection_v3_census_contract as contract
from tests.test_plan_pricing_projection_v3_census_contract import (
    _TARGET,
    _accepted_inputs,
    _authority,
    _is_accepted,
    _staged_counts,
)


def test_acceptance_requires_exact_census_database_settings() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    receipt_by_field["database_session_settings"] = {
        **receipt_by_field["database_session_settings"],
        "work_mem": "32MB",
    }

    assert not _is_accepted(receipt_by_field, measurement_by_field)


def test_acceptance_rejects_boolean_one_binding_release_count() -> None:
    """A bool must not pass an exact one-binding integer identity check."""

    receipt_by_field, measurement_by_field = _accepted_inputs()
    target_by_field = {
        **_TARGET,
        "binding_count": 1,
        "in_network_binding_count": 1,
        "distinct_snapshot_count": 1,
    }
    receipt_by_field["expected_target"] = deepcopy(target_by_field)
    measurement_by_field["serving_shape"] = deepcopy(target_by_field)
    measurement_by_field["release"]["binding_count"] = True
    authority_by_field = _authority()
    authority_by_field["expected_target"] = deepcopy(target_by_field)

    assert not _is_accepted(
        receipt_by_field,
        measurement_by_field,
        authority_by_field=authority_by_field,
    )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda receipt: receipt.update(database_run_token="0" * 12),
        lambda receipt: receipt.update(database_backend_pid=True),
        lambda receipt: receipt.update(database_application_name="changed"),
        lambda receipt: receipt["database_stage_resources"].pop("final_measurement"),
        lambda receipt: receipt["database_stage_resources"][
            "measurement_complete"
        ].update(before_count=-1),
    ),
)
def test_acceptance_rejects_malformed_database_attribution(mutation) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(receipt_by_field)

    assert not _is_accepted(receipt_by_field, measurement_by_field)


@pytest.mark.parametrize(
    ("observed", "cap"),
    ((True, 100), (1.0, 100), (-1, 100), (1, True), (1, 0)),
)
def test_headroom_rejects_invalid_types_or_values(observed, cap) -> None:
    assert not contract._has_headroom(observed, cap)


@pytest.mark.parametrize(
    ("field_name", "gate_name", "cap"),
    (
        (
            "provider_set_count",
            "provider_set_count_within_cap",
            contract.MAX_PROJECTION_PROVIDER_SETS,
        ),
        (
            "provider_membership_count",
            "provider_membership_count_within_cap",
            contract.MAX_PROJECTION_PROVIDER_MEMBERSHIPS,
        ),
        (
            "maximum_provider_set_membership_count",
            "maximum_provider_set_membership_count_within_cap",
            contract.MAX_PROVIDER_NPIS_PER_SET,
        ),
        (
            "provider_cell_count",
            "provider_cell_count_within_cap",
            contract.MAX_PROJECTION_PROVIDER_CELLS,
        ),
        (
            "provider_fragment_byte_count",
            "provider_fragment_byte_count_within_cap",
            contract.MAX_PROJECTION_PROVIDER_FRAGMENT_BYTES,
        ),
        (
            "price_membership_cached_block_count",
            "price_membership_cached_block_count_within_cap",
            contract.MAX_PRICE_MEMBERSHIP_CACHED_BLOCKS,
        ),
        (
            "price_membership_identity_retained_bytes",
            "price_membership_identity_retained_bytes_within_cap",
            contract.MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES,
        ),
        (
            "price_membership_metadata_fragment_count",
            "price_membership_metadata_fragment_count_within_cap",
            contract.MAX_PRICE_MEMBERSHIP_CACHED_FRAGMENTS,
        ),
        (
            "price_membership_singleton_peak_bytes",
            "price_membership_singleton_peak_bytes_within_cap",
            contract.MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES,
        ),
    ),
)
def test_provider_gates_require_twenty_five_percent_headroom(
    field_name,
    gate_name,
    cap,
) -> None:
    staged_by_field = _staged_counts()
    staged_by_field[field_name] = cap * 4 // 5
    gates = contract.fixed_cap_gates(census._empty_metrics(), staged_by_field)
    assert frozenset(gates) == contract.EXPECTED_FIXED_CAP_GATE_KEYS
    assert gates[gate_name]

    staged_by_field[field_name] += 1
    gates = contract.fixed_cap_gates(census._empty_metrics(), staged_by_field)
    assert not gates[gate_name]
