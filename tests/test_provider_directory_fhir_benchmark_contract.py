# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import math

import pytest

from scripts.research.provider_directory_fhir_benchmark_contract import (
    OBSERVATION_FIELDS,
    RESOURCE_FIELDS,
    build_tuning_report,
    main,
    parse_benchmark_document,
    parse_observation,
)


def _resource(
    resource_type="Practitioner",
    count=100,
    elapsed=50,
    *,
    requests=200,
    retry=0,
    throttle=False,
    peak=1024,
    backlog=5000,
):
    return {
        "resource_type": resource_type,
        "pre_count": count,
        "processed_rows": count,
        "unique_staged_ids": count,
        "post_count": count,
        "pages": 1,
        "elapsed_seconds": elapsed,
        "requests": requests,
        "transient_retries": retry,
        "unresolved_throttling": throttle,
        "peak_memory_bytes": peak,
        "peak_db_backlog_rows": backlog,
    }


def _observation_payload(
    concurrency=2,
    elapsed=50,
    *,
    resources=None,
    **overrides,
):
    observation_by_field = {
        "concurrency": concurrency,
        "cutoff": "2026-08-06T12:00:00Z",
        "elapsed_seconds": elapsed,
        "deadline_seconds": 100,
        "memory_budget_bytes": 2048,
        "db_backlog_budget_rows": 5000,
        "proxy_route": "dev-server",
        "resources": resources or [_resource(elapsed=elapsed)],
    }
    observation_by_field.update(overrides)
    return observation_by_field


def _observation(concurrency=2, elapsed=50, **overrides):
    return parse_observation(
        _observation_payload(concurrency, elapsed, **overrides)
    )


def _trial_observations(one=60, two=50, four=40):
    return [
        _observation(1, one),
        _observation(2, two),
        _observation(4, four),
    ]


def _document_payload():
    resources = [_resource("Practitioner"), _resource("Organization")]
    return {
        "observations": [
            _observation_payload(4, 40, resources=resources),
            _observation_payload(1, 60, resources=resources),
            _observation_payload(2, 50, resources=resources),
        ]
    }


def test_report_allows_four_only_for_safe_material_improvement():
    report = build_tuning_report(_trial_observations())

    assert report["recommended_resource_concurrency"] == 4
    assert all(
        observation["passes_safety_gates"]
        for observation in report["observations"]
    )


def test_report_keeps_two_when_four_is_not_materially_faster():
    report = build_tuning_report(_trial_observations(four=46))

    assert report["recommended_resource_concurrency"] == 2


def test_report_keeps_one_when_two_is_not_faster():
    report = build_tuning_report(_trial_observations(one=50, two=60))

    assert report["recommended_resource_concurrency"] == 1


def test_report_requires_a_safe_concurrency_one_baseline():
    inexact_resources = []
    for elapsed in (60, 50, 40):
        resource_by_field = _resource(elapsed=elapsed)
        resource_by_field["processed_rows"] = 99
        resource_by_field["unique_staged_ids"] = 99
        inexact_resources.append(resource_by_field)
    observations = [
        _observation(1, 60, resources=[inexact_resources[0]]),
        _observation(2, 50, resources=[inexact_resources[1]]),
        _observation(4, 40, resources=[inexact_resources[2]]),
    ]

    report = build_tuning_report(observations)

    assert report["recommended_resource_concurrency"] == 0


def test_report_falls_back_when_higher_candidates_are_not_safe():
    observations = [
        _observation(1, 60),
        _observation(2, 50, resources=[_resource(elapsed=50, throttle=True)]),
        _observation(4, 40, resources=[_resource(elapsed=40, throttle=True)]),
    ]

    report = build_tuning_report(observations)

    assert report["recommended_resource_concurrency"] == 1
    assert report["observations"][1]["passes_safety_gates"] is False
    assert report["observations"][2]["passes_safety_gates"] is False


@pytest.mark.parametrize(
    "candidate_overrides",
    [
        {"proxy_route": "direct"},
        {"elapsed_seconds": 71},
        {"resources": [_resource(peak=2049)]},
        {"resources": [_resource(backlog=5001)]},
        {"resources": [_resource(throttle=True)]},
    ],
)
def test_candidate_safety_gates_fail_closed(candidate_overrides):
    observations = [
        _observation(1, 60),
        _observation(2, 50, **candidate_overrides),
        _observation(4, 40),
    ]

    report = build_tuning_report(observations)

    assert report["recommended_resource_concurrency"] == 1
    assert report["observations"][1]["passes_safety_gates"] is False


def test_per_resource_retry_rate_cannot_be_hidden_by_aggregate_volume():
    safe_resources = [
        _resource("Organization", requests=1000),
        _resource("Practitioner", requests=1000),
    ]
    diluted_retry_resources = [
        _resource("Organization", requests=2, retry=1),
        _resource("Practitioner", requests=1000),
    ]
    observations = [
        _observation(1, 60, resources=safe_resources),
        _observation(2, 50, resources=diluted_retry_resources),
        _observation(4, 40, resources=safe_resources),
    ]

    report = build_tuning_report(observations)
    candidate = report["observations"][1]

    assert candidate["retry_rate"] < 0.01
    assert candidate["retry_rates_within_limit"] is False
    assert candidate["passes_safety_gates"] is False
    assert report["recommended_resource_concurrency"] == 1


def test_top_level_document_rejects_missing_unknown_and_wrong_shapes():
    invalid_documents = [
        None,
        [],
        {},
        {"observations": [], "unexpected": True},
        {"observations": {}},
    ]

    for invalid_document in invalid_documents:
        with pytest.raises(ValueError):
            parse_benchmark_document(invalid_document)


def test_observation_requires_every_field_and_rejects_unknown_fields():
    baseline = _observation_payload()
    for field_name in sorted(OBSERVATION_FIELDS):
        observation_by_field = {**baseline}
        observation_by_field.pop(field_name)
        with pytest.raises(ValueError, match="missing="):
            parse_observation(observation_by_field)

    with pytest.raises(ValueError, match="unknown="):
        parse_observation({**baseline, "unexpected": True})


def test_resource_requires_every_field_and_rejects_unknown_fields():
    baseline = _observation_payload()
    baseline_resource = baseline["resources"][0]
    for field_name in sorted(RESOURCE_FIELDS):
        resource_by_field = {**baseline_resource}
        resource_by_field.pop(field_name)
        with pytest.raises(ValueError, match="missing="):
            parse_observation({**baseline, "resources": [resource_by_field]})

    unknown_resource_by_field = {**baseline_resource, "unexpected": True}
    with pytest.raises(ValueError, match="unknown="):
        parse_observation({**baseline, "resources": [unknown_resource_by_field]})


@pytest.mark.parametrize("invalid_value", [True, 1.0, "1", None, [], {}])
def test_integer_fields_reject_non_integer_json_scalars(invalid_value):
    observation_fields = (
        "concurrency",
        "memory_budget_bytes",
        "db_backlog_budget_rows",
    )
    resource_fields = (
        "pre_count",
        "processed_rows",
        "unique_staged_ids",
        "post_count",
        "pages",
        "requests",
        "transient_retries",
        "peak_memory_bytes",
        "peak_db_backlog_rows",
    )
    for field_name in observation_fields:
        with pytest.raises(ValueError):
            parse_observation(
                _observation_payload(**{field_name: invalid_value})
            )
    for field_name in resource_fields:
        resource_by_field = {**_resource(), field_name: invalid_value}
        with pytest.raises(ValueError):
            parse_observation(_observation_payload(resources=[resource_by_field]))


@pytest.mark.parametrize(
    "invalid_value",
    [True, "1", None, [], {}, 0, -1, math.nan, math.inf, -math.inf],
)
def test_duration_fields_reject_non_positive_or_non_finite_numbers(
    invalid_value,
):
    for field_name in ("elapsed_seconds", "deadline_seconds"):
        with pytest.raises(ValueError):
            parse_observation(
                _observation_payload(**{field_name: invalid_value})
            )
    resource_by_field = {**_resource(), "elapsed_seconds": invalid_value}
    with pytest.raises(ValueError):
        parse_observation(_observation_payload(resources=[resource_by_field]))


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("concurrency", 0),
        ("memory_budget_bytes", 0),
        ("db_backlog_budget_rows", 0),
    ],
)
def test_observation_integer_fields_enforce_positive_ranges(
    field_name,
    invalid_value,
):
    with pytest.raises(ValueError):
        parse_observation(
            _observation_payload(**{field_name: invalid_value})
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("pre_count", -1),
        ("processed_rows", -1),
        ("unique_staged_ids", -1),
        ("post_count", -1),
        ("pages", 0),
        ("requests", 0),
        ("transient_retries", -1),
        ("peak_memory_bytes", -1),
        ("peak_db_backlog_rows", -1),
    ],
)
def test_resource_integer_fields_enforce_bounded_ranges(
    field_name,
    invalid_value,
):
    resource_by_field = {**_resource(), field_name: invalid_value}
    with pytest.raises(ValueError):
        parse_observation(_observation_payload(resources=[resource_by_field]))


def test_retry_count_cannot_exceed_request_count():
    resource = _resource(requests=2, retry=3)

    with pytest.raises(ValueError, match="must not exceed"):
        parse_observation(_observation_payload(resources=[resource]))


@pytest.mark.parametrize("invalid_value", [0, 1, "false", None, [], {}])
def test_throttling_field_requires_a_boolean(invalid_value):
    resource_by_field = {
        **_resource(),
        "unresolved_throttling": invalid_value,
    }

    with pytest.raises(ValueError):
        parse_observation(_observation_payload(resources=[resource_by_field]))


@pytest.mark.parametrize("invalid_value", [None, True, 1, ""])
def test_identity_fields_require_non_empty_strings(invalid_value):
    for field_name in ("cutoff", "proxy_route"):
        with pytest.raises(ValueError):
            parse_observation(
                _observation_payload(**{field_name: invalid_value})
            )
    resource_by_field = {**_resource(), "resource_type": invalid_value}
    with pytest.raises(ValueError):
        parse_observation(_observation_payload(resources=[resource_by_field]))


@pytest.mark.parametrize("invalid_resources", [None, {}, (), "resource"])
def test_resources_field_requires_an_array(invalid_resources):
    payload = _observation_payload()
    payload["resources"] = invalid_resources

    with pytest.raises(ValueError, match="array"):
        parse_observation(payload)


@pytest.mark.parametrize("invalid_resource", [None, [], "resource"])
def test_resource_entries_require_objects(invalid_resource):
    with pytest.raises(ValueError, match="object"):
        parse_observation(
            _observation_payload(resources=[invalid_resource])
        )


def test_resource_identity_is_unique_after_normalization():
    duplicate_resources = [
        _resource("Practitioner"),
        _resource(" Practitioner "),
    ]

    with pytest.raises(ValueError, match="unique"):
        parse_observation(
            _observation_payload(resources=duplicate_resources)
        )


@pytest.mark.parametrize(
    "concurrencies",
    [
        (),
        (1,),
        (1, 2),
        (1, 2, 2, 4),
        (1, 4),
    ],
)
def test_report_requires_exactly_one_observation_for_each_concurrency(
    concurrencies,
):
    observations = [
        _observation(concurrency, 70 - concurrency * 5)
        for concurrency in concurrencies
    ]

    with pytest.raises(ValueError, match="exactly one observation"):
        build_tuning_report(observations)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"cutoff": "2026-08-07T12:00:00Z"}, "same cutoff"),
        ({"deadline_seconds": 101}, "same deadline"),
        ({"memory_budget_bytes": 2049}, "same deadline"),
        ({"db_backlog_budget_rows": 5001}, "same deadline"),
        ({"resources": [_resource(count=101)]}, "same per-resource census"),
        (
            {"resources": [{
                **_resource(),
                "processed_rows": 99,
                "unique_staged_ids": 99,
            }]},
            "same per-resource census",
        ),
        ({"resources": [_resource("Organization")]}, "same per-resource census"),
    ],
)
def test_report_rejects_mismatched_trial_contracts(mutation, message):
    observations = [
        _observation(1, 60),
        _observation(2, 50),
        _observation(4, 40, **mutation),
    ]

    with pytest.raises(ValueError, match=message):
        build_tuning_report(observations)


def test_report_canonicalizes_observation_and_resource_order():
    document = _document_payload()
    forward = build_tuning_report(parse_benchmark_document(document))
    for observation in document["observations"]:
        observation["resources"].reverse()
    document["observations"].reverse()
    reverse = build_tuning_report(parse_benchmark_document(document))

    assert reverse == forward
    assert [
        observation["concurrency"] for observation in forward["observations"]
    ] == [1, 2, 4]
    for observation in forward["observations"]:
        resource_names = [
            resource["resource_type"]
            for resource in observation["resources"]
        ]
        assert resource_names == sorted(resource_names)


def test_cli_stdout_is_canonical_json(tmp_path, capsys):
    input_path = tmp_path / "benchmark.json"
    input_path.write_text(json.dumps(_document_payload()), encoding="utf-8")

    assert main([str(input_path)]) == 0

    report_text = capsys.readouterr().out
    report = json.loads(report_text)
    assert report_text == json.dumps(report, indent=2, sort_keys=True) + "\n"
    assert report["contract_version"] == (
        "provider-directory-fhir-benchmark-v1"
    )
    assert report["recommended_resource_concurrency"] == 4


def test_cli_output_file_is_json_and_stdout_stays_empty(tmp_path, capsys):
    input_path = tmp_path / "benchmark.json"
    output_path = tmp_path / "nested" / "report.json"
    input_path.write_text(json.dumps(_document_payload()), encoding="utf-8")

    assert main([str(input_path), "--output", str(output_path)]) == 0

    assert capsys.readouterr().out == ""
    report_text = output_path.read_text(encoding="utf-8")
    assert report_text.endswith("\n")
    assert json.loads(report_text)["recommended_resource_concurrency"] == 4


@pytest.mark.parametrize(
    "document_text",
    [
        "{",
        "[]",
        "{}",
        '{"observations": []}',
        '{"observations": [], "unexpected": true}',
        '{"observations": {}}',
    ],
)
def test_cli_rejects_malformed_json_or_top_level_contract(
    tmp_path,
    document_text,
):
    input_path = tmp_path / "invalid.json"
    input_path.write_text(document_text, encoding="utf-8")

    with pytest.raises((json.JSONDecodeError, ValueError)):
        main([str(input_path)])
