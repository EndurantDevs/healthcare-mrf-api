# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed receipt gates for the projection-v3 census."""

from __future__ import annotations

import pytest

from scripts.research import plan_pricing_projection_v3_census as census
from scripts.research import plan_pricing_projection_v3_census_contract as contract

_RUNTIME = {
    "job_name": "census-job",
    "pod_uid": "pod-uid",
    "image_digest": "sha256:" + "c" * 64,
}


def _database_receipt() -> dict:
    run_token = contract.census_database_run_token(_RUNTIME)
    resources_by_stage = {}
    for stage in contract.CENSUS_DATABASE_STAGE_KEYS:
        resource_by_field = {
            "before_count": 1,
            "before_backend_memory_context_bytes_maximum": 1,
            "before_temporary_relation_bytes_maximum": 0,
        }
        if stage != "measurement_complete":
            resource_by_field.update(
                after_count=1,
                after_backend_memory_context_bytes_maximum=1,
                after_temporary_relation_bytes_maximum=0,
            )
        resources_by_stage[stage] = resource_by_field
    return {
        "runtime": _RUNTIME,
        "database_run_token": run_token,
        "database_backend_pid": 123,
        "database_session_settings": (
            contract.expected_census_database_settings(run_token)
        ),
        "database_stage": "measurement_complete",
        "database_application_name": contract.census_database_application_name(
            run_token,
            "measurement_complete",
        ),
        "database_stage_resources": resources_by_stage,
    }


def _staged_counts() -> dict[str, int]:
    return dict.fromkeys(
        (
            "provider_set_count",
            "provider_membership_count",
            "maximum_provider_set_membership_count",
            "provider_cell_count",
            "provider_fragment_byte_count",
            "provider_npi_count",
            "pending_npi_count",
            "referenced_empty_provider_set_count",
            "price_membership_cached_block_count",
            "price_membership_identity_retained_bytes",
            "price_membership_metadata_fragment_count",
            "price_membership_maximum_fragments_per_block",
            "price_membership_singleton_peak_bytes",
        ),
        0,
    )


def _accepted_inputs() -> tuple[dict, dict]:
    work_by_field = census._empty_metrics()
    for field_name in ("membership_probe_rows", "member_cell_rows"):
        work_by_field[field_name] = {"total": 1, "maximum_per_code": 1}
    staged_by_field = _staged_counts()
    return (
        {
            **_database_receipt(),
            "rollback_complete": True,
            "temporary_relations_after_rollback": [],
            "postflight": {"accepted": True},
        },
        {
            "work": work_by_field,
            "staged": staged_by_field,
            "fixed_cap_gates": contract.fixed_cap_gates(work_by_field, staged_by_field),
            "observed_work_limits": contract.observed_work_limits(work_by_field),
        },
    )


@pytest.mark.parametrize(
    ("collection_name", "mutation"),
    (
        ("fixed_cap_gates", lambda values: values.pop(next(iter(values)))),
        ("fixed_cap_gates", lambda values: values.update(extra_gate=True)),
        ("fixed_cap_gates", lambda values: values.update({next(iter(values)): 1})),
        ("observed_work_limits", lambda values: values.pop(next(iter(values)))),
        ("observed_work_limits", lambda values: values.update(extra_limit=1)),
        (
            "observed_work_limits",
            lambda values: values.update({next(iter(values)): True}),
        ),
        (
            "observed_work_limits",
            lambda values: values.update({next(iter(values)): 1.0}),
        ),
        (
            "observed_work_limits",
            lambda values: values.update({next(iter(values)): -1}),
        ),
    ),
)
def test_acceptance_rejects_malformed_gate_or_limit_contract(
    collection_name,
    mutation,
) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(measurement_by_field[collection_name])

    assert not contract.is_accepted(receipt_by_field, measurement_by_field, True)


@pytest.mark.parametrize(
    ("mutation"),
    (
        lambda measurement: measurement["work"].pop(next(iter(measurement["work"]))),
        lambda measurement: measurement["work"].update(
            extra={"total": 0, "maximum_per_code": 0}
        ),
        lambda measurement: measurement["work"]["membership_probe_rows"].update(
            total=True
        ),
        lambda measurement: measurement["work"]["membership_probe_rows"].update(
            total=0, maximum_per_code=1
        ),
        lambda measurement: measurement["work"]["membership_probe_rows"].update(
            total=1.0
        ),
        lambda measurement: measurement["staged"].pop(
            next(iter(measurement["staged"]))
        ),
        lambda measurement: measurement["staged"].update(extra=0),
        lambda measurement: measurement["staged"].update(provider_set_count=True),
        lambda measurement: measurement["staged"].update(provider_set_count=-1),
        lambda measurement: measurement["staged"].update(
            price_membership_identity_retained_bytes=1
        ),
        lambda measurement: measurement["staged"].update(
            price_membership_singleton_peak_bytes=1
        ),
        lambda measurement: measurement["staged"].update(
            price_membership_metadata_fragment_count=0,
            price_membership_maximum_fragments_per_block=1,
            price_membership_singleton_peak_bytes=(
                contract.PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT
            ),
        ),
    ),
)
def test_acceptance_rejects_malformed_measurement_contract(mutation) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(measurement_by_field)

    assert not contract.is_accepted(receipt_by_field, measurement_by_field, True)


def test_acceptance_recomputes_derived_gates_and_limits() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    oversized_occurrence_count = contract.MAX_CODE_OCCURRENCES * 4 // 5 + 1
    measurement_by_field["work"]["normalized_occurrence_rows"] = {
        "total": oversized_occurrence_count,
        "maximum_per_code": oversized_occurrence_count,
    }
    assert not contract.is_accepted(receipt_by_field, measurement_by_field, True)

    receipt_by_field, measurement_by_field = _accepted_inputs()
    measurement_by_field["observed_work_limits"][
        "maximum_code_membership_probe_rows"
    ] = 2
    assert not contract.is_accepted(receipt_by_field, measurement_by_field, True)


def test_acceptance_requires_exact_census_database_settings() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    receipt_by_field["database_session_settings"] = {
        **receipt_by_field["database_session_settings"],
        "work_mem": "32MB",
    }

    assert not contract.is_accepted(receipt_by_field, measurement_by_field, True)


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

    assert not contract.is_accepted(receipt_by_field, measurement_by_field, True)


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
