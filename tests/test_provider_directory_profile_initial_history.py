# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Known historical metadata changes cannot weaken fresh initial admission."""

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_guards as guards
from process import provider_directory_profile_initial_history as history
from tests.test_provider_directory_profile_initial import _target
from tests.test_provider_directory_profile_initial_recovery import _witness


def _payload(table):
    constraint = history._NEW_PURPOSE if table == history._CONSUMPTION else guards._REACTIVATED_KEY_CHECK
    return {
        "relation": {"relation_oid": 40, "schema_name": "fixture", "relation_name": table, "relfilenode": 41},
        "attributes": [
            {
                "relation_oid": 40,
                "attnum": 4,
                "attname": "singleton_key",
                "attnotnull": True,
                "default_expression": guards._REACTIVATED_KEY_DEFAULT,
            }
        ],
        "indexes": [{"index_oid": 42, "relfilenode": 43, "indisunique": True}],
        "constraints": [
            {"relation_oid": 40, "constraint_type": "c", "convalidated": True, "constraint_definition": constraint}
        ],
        "triggers": [{"trigger_function_oid": 44, "trigger_definition": "original immutable trigger"}],
        "guard_dependencies": {
            "function_oid": 45,
            "function_source_sha256": hashlib.sha256(
                (" " + guards._reactivated_receipt_matches_sql("fixture") + " ").encode()
            ).hexdigest(),
        },
    }


@pytest.mark.parametrize("table", [guards.RECEIPT_TABLE, history._SERVING, history._CONSUMPTION])
async def test_historical_projection_preserves_native_identity_and_frozen_original(table):
    current = _payload(table)
    before = deepcopy(current)
    original = history.original_fingerprint_payload(current, schema="fixture", table=table)
    assert current == before
    assert original["relation"] == current["relation"]
    if table == guards.RECEIPT_TABLE:
        assert original["attributes"] == [
            {**current["attributes"][0], "default_expression": guards._LEGACY_KEY_DEFAULT}
        ]
    else:
        assert original["attributes"] == current["attributes"]
    assert original["indexes"] == current["indexes"]
    assert original["triggers"] == current["triggers"]
    assert original["guard_dependencies"]["function_oid"] == 45
    if table == history._CONSUMPTION:
        assert original["constraints"][0]["constraint_definition"] == history._OLD_PURPOSE
        assert original["guard_dependencies"] == current["guard_dependencies"]
    else:
        assert (
            original["guard_dependencies"]["function_source_sha256"]
            == hashlib.sha256((" " + guards._receipt_matches_sql("fixture") + " ").encode()).hexdigest()
        )


@pytest.mark.parametrize(
    "changed", ["body", "check", "default", "schema", "table", "duplicate_check", "duplicate_default", "unvalidated"]
)
def test_historical_projection_refuses_unknown_or_mixed_native_changes(changed):
    current = _payload(guards.RECEIPT_TABLE)
    match changed:
        case "body":
            current["guard_dependencies"]["function_source_sha256"] = "a" * 64
        case "check":
            current["constraints"][0]["constraint_definition"] = guards._LEGACY_KEY_CHECK
        case "default":
            current["attributes"][0]["default_expression"] = guards._LEGACY_KEY_DEFAULT
        case "schema":
            current["relation"]["schema_name"] = "different"
        case "table":
            current["relation"]["relation_name"] = "unrelated"
        case "duplicate_check":
            current["constraints"].append(deepcopy(current["constraints"][0]))
        case "duplicate_default":
            current["attributes"].append(deepcopy(current["attributes"][0]))
        case "unvalidated":
            current["constraints"][0]["convalidated"] = False
    with pytest.raises(RuntimeError, match="history_catalog_changed"):
        history.original_fingerprint_payload(current, schema="fixture", table=guards.RECEIPT_TABLE)


def _guard_row(version):
    checks = (
        guards._RECEIPT_CHECKS
        if version == 1
        else (guards._RECEIPT_CHECKS - {guards._LEGACY_KEY_CHECK}) | {guards._REACTIVATED_KEY_CHECK}
    )
    body = (
        guards._receipt_matches_sql("fixture") if version == 1 else guards._reactivated_receipt_matches_sql("fixture")
    )
    return dict(
        function_oid=45,
        function_schema="fixture",
        function_name="pd_profile_initial_receipt_matches",
        prosrc=" " + body + " ",
        prosecdef=False,
        provolatile="v",
        proconfig=["search_path=pg_catalog"],
        pronargs=1,
        proargnames=["receipt"],
        returns_boolean=True,
        receipt_argument=True,
        lanname="sql",
        receipt_checks=sorted(checks),
        receipt_default=guards._LEGACY_KEY_DEFAULT if version == 1 else guards._REACTIVATED_KEY_DEFAULT,
    )


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("mixed", [None, "prosrc", "receipt_default", "receipt_checks"])
async def test_guard_checks_and_function_body_are_one_coherent_contract(version, mixed):
    row = _guard_row(version)
    if mixed:
        row[mixed] = _guard_row(3 - version)[mixed]
    database = SimpleNamespace(all=AsyncMock(return_value=[SimpleNamespace(_mapping=row)]))
    if mixed:
        with pytest.raises(RuntimeError, match="(guard|storage)_shape_changed"):
            await guards.initial_guard_dependencies(database, "fixture")
    else:
        dependencies = await guards.initial_guard_dependencies(database, "fixture")
        assert "receipt_checks" not in dependencies
        assert "receipt_default" not in dependencies
        assert dependencies["function_source_sha256"] == hashlib.sha256(row["prosrc"].encode()).hexdigest()


@pytest.mark.parametrize("changed", ["serving", "consumption", "oid", "unrelated", "keys"])
async def test_readback_compatibility_is_closed_to_known_metadata_fields(monkeypatch, changed):
    expected_by_field = dict(
        serving_generation_oid=40,
        serving_generation_storage_fingerprint="a" * 64,
        capacity_consumption_oid=41,
        capacity_consumption_storage_fingerprint="b" * 64,
        database_oid=42,
    )
    observed_by_field = dict(expected_by_field)
    name = {
        "serving": "serving_generation_storage_fingerprint",
        "consumption": "capacity_consumption_storage_fingerprint",
        "oid": "serving_generation_oid",
        "unrelated": "database_oid",
        "keys": "extra",
    }[changed]
    observed_by_field[name] = "changed"
    matcher = AsyncMock(return_value=True)
    monkeypatch.setattr(history, "is_historical_fingerprint", matcher)
    accepted = await history.is_historical_metadata(object(), "fixture", observed_by_field, expected_by_field)
    assert accepted is (changed in {"serving", "consumption"})
    assert matcher.await_count == (1 if accepted else 0)


def test_reactivated_key_keeps_fresh_global_and_unique_finite_control_generations():
    original = _target()
    assert initial._receipt_key(original, 7) == "global"
    from process.provider_directory_profile_initial_recovery import recovered_target_state

    recovered = recovered_target_state(original, _witness(original))
    assert initial._receipt_key(recovered, 7) == "7"
    assert initial._receipt_key(recovered, 16) == "10"
    assert initial._receipt_key(recovered, 2**63 - 1) == "7fffffffffffffff"
    for generation in (0, -1, True, 2**63):
        with pytest.raises(RuntimeError, match="generation_invalid"):
            initial._receipt_key(recovered, generation)


@pytest.mark.parametrize("recovered", [False, True])
async def test_receipt_writer_preserves_column_grants_and_uses_one_statement(recovered):
    target = _target()
    if recovered:
        from process.provider_directory_profile_initial_recovery import recovered_target_state

        target = recovered_target_state(target, _witness(target))
    status = AsyncMock()
    fhir = SimpleNamespace(db=SimpleNamespace(status=status), _unscoped_qt=lambda schema, table: f"{schema}.{table}")
    admission = SimpleNamespace(
        build_id="build",
        run_id="run",
        lease=SimpleNamespace(attestation_id="attestation"),
        admitted_identity=SimpleNamespace(initial_targets=SimpleNamespace(payload=target)),
    )
    await initial._insert_receipt(
        fhir, admission, SimpleNamespace(schema="fixture", generation_id="generation"), {"control_generation": 16}, {}
    )
    assert status.await_count == 1
    statement = status.call_args.args[0]
    assert "singleton_key" not in statement
    assert ("receipt_key" in status.call_args.kwargs) is recovered
    assert ("AS MATERIALIZED" in statement) is recovered
    if recovered:
        assert status.call_args.kwargs["receipt_key"] == "10"
        assert "CROSS JOIN receipt_key WHERE receipt_key.value=:receipt_key" in statement
    else:
        assert statement.startswith("INSERT INTO fixture.")
        assert "set_config" not in statement


@pytest.mark.parametrize("purpose", [history._NEW_PURPOSE, history._INVERSE_PURPOSE])
def test_consumption_history_normalization_preserves_every_other_native_fact(purpose):
    current = _payload(history._CONSUMPTION)
    current["constraints"][0]["constraint_definition"] = purpose
    before = deepcopy(current)
    original = history.original_fingerprint_payload(current, schema="fixture", table=history._CONSUMPTION)
    assert current == before
    original["constraints"][0]["constraint_definition"] = purpose
    assert original == current


@pytest.mark.parametrize("damage", ["unknown", "duplicate", "unvalidated", "mixed"])
def test_consumption_history_refuses_unrecognized_purpose_evolution(damage):
    current = _payload(history._CONSUMPTION)
    current["constraints"][0]["constraint_definition"] = history._INVERSE_PURPOSE
    if damage == "unknown":
        current["constraints"][0]["constraint_definition"] += " OR true"
    elif damage == "unvalidated":
        current["constraints"][0]["convalidated"] = False
    else:
        current["constraints"].append(deepcopy(current["constraints"][0]))
        if damage == "mixed":
            current["constraints"][1]["constraint_definition"] = history._NEW_PURPOSE
    with pytest.raises(RuntimeError, match="history_catalog_changed"):
        history.original_fingerprint_payload(current, schema="fixture", table=history._CONSUMPTION)


@pytest.mark.parametrize("previous_purpose", [history._OLD_PURPOSE, history._NEW_PURPOSE])
@pytest.mark.parametrize("changed", [False, True])
async def test_inverse_history_readback_accepts_exact_two_or_three_purpose_predecessor(
    monkeypatch, previous_purpose, changed
):
    from process import provider_directory_cms_nonprofile_observation as observation

    payload = _payload(history._CONSUMPTION)
    payload["constraints"][0]["constraint_definition"] = history._INVERSE_PURPOSE
    original = deepcopy(payload)
    original["constraints"][0]["constraint_definition"] = previous_purpose
    if changed:
        original["indexes"][0]["index_oid"] += 1
    reader = SimpleNamespace(
        _profile_capacity_relation_row=AsyncMock(return_value=(payload["relation"], None)),
        _profile_capacity_relation_catalog=AsyncMock(return_value=([], [], [], [])),
    )
    monkeypatch.setattr(observation, "catalog_observation", lambda *_args: reader)
    digest = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
    fhir = SimpleNamespace(
        profile_capacity_projection=SimpleNamespace(assert_relation_guards=AsyncMock(return_value=None)),
        _profile_capacity_fingerprint_payloads=lambda *_args: (payload, {}),
        _identity_hash=digest,
    )
    assert (
        await history.is_historical_fingerprint(fhir, "fixture", history._CONSUMPTION, 40, digest(original))
        is not changed
    )
