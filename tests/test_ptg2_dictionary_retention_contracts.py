# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retention and corruption contracts for dense price dictionaries."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


_ATTRIBUTE_SPECS = (
    ("setting", "setting_key", "text", "setting"),
    ("service_code", "service_code_key", "array", "service_code"),
    ("billing_class", "billing_class_key", "text", "billing_class"),
)


def _price_atom(*attribute_keys, negotiated_rate="125.00"):
    return SimpleNamespace(
        attribute_keys=attribute_keys,
        negotiated_rate=negotiated_rate,
    )


@pytest.mark.parametrize(
    ("dictionary_row", "error_match"),
    [
        ({"attribute_kind": None, "attribute_key": 1}, None),
        (
            {
                "attribute_kind": "service_code",
                "attribute_key": 1,
                "value": "not-json",
            },
            "array dictionary value is malformed",
        ),
        (
            {
                "attribute_kind": "billing_code_modifier",
                "attribute_key": 1,
                "value": "{}",
            },
            "array dictionary value is malformed",
        ),
    ],
)
def test_dictionary_entry_rejects_incomplete_or_malformed_arrays(
    dictionary_row,
    error_match,
):
    """Ignore incomplete rows and reject malformed array-valued atoms."""

    if error_match:
        with pytest.raises(serving.PTG2ManifestArtifactError, match=error_match):
            serving._version_three_dictionary_entry(dictionary_row)
        return
    assert serving._version_three_dictionary_entry(dictionary_row) is None


def test_required_dictionary_keys_deduplicate_and_skip_embedded_values():
    """Charge each required key once while skipping constants and nulls."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)
    atoms_by_key = {
        1: _price_atom(11, None, 31),
        2: _price_atom(11, None, 31),
    }

    required_keys, retained_bytes = serving._required_version_three_dictionary_keys(
        atoms_by_key,
        _ATTRIBUTE_SPECS,
        {"billing_class": "professional"},
        budget,
    )

    assert required_keys == {("setting", 11)}
    assert retained_bytes == (
        serving._HYDRATION_KEY_SET_BYTES
        + serving._HYDRATION_DICTIONARY_ENTRY_BYTES
    )
    assert budget.retained_bytes == retained_bytes


def test_required_dictionary_keys_release_budget_after_corrupt_atom():
    """Release temporary key retention when atom arity is invalid."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)

    with pytest.raises(serving.PTG2ManifestArtifactError, match="attribute-key count"):
        serving._required_version_three_dictionary_keys(
            {1: _price_atom(11)},
            _ATTRIBUTE_SPECS,
            {},
            budget,
        )

    assert budget.retained_bytes == 0


def test_decoded_dictionary_values_skip_incomplete_and_charge_unique_keys():
    """Retain one value per key even when the dictionary repeats a row."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)
    dictionary_rows = [
        {"attribute_kind": None, "attribute_key": 1, "value": "ignored"},
        {"attribute_kind": "setting", "attribute_key": 11, "value": "office"},
        {"attribute_kind": "setting", "attribute_key": 11, "value": "clinic"},
    ]

    values_by_key = serving._decoded_version_three_dictionary_values(
        dictionary_rows,
        {("setting", 11)},
        budget,
    )

    assert values_by_key == {("setting", 11): "clinic"}
    assert budget.retained_bytes == (
        serving._HYDRATION_DICTIONARY_MAP_BYTES
        + serving._HYDRATION_DICTIONARY_ENTRY_BYTES
    )


def test_decoded_dictionary_values_release_budget_when_key_is_missing():
    """Do not retain a partial dictionary map after a missing-key failure."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)

    with pytest.raises(serving.PTG2ManifestArtifactError, match="key is missing"):
        serving._decoded_version_three_dictionary_values(
            [],
            {("setting", 11)},
            budget,
        )

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("query_fails", [False, True])
async def test_dictionary_value_query_releases_required_key_retention(
    monkeypatch,
    query_fails,
):
    """Release the required-key set after successful or failed dictionary I/O."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)
    budget.claim(64, category="test required keys")
    monkeypatch.setattr(
        serving,
        "_required_version_three_dictionary_keys",
        lambda *_args: ({("setting", 11)}, 64),
    )
    query = AsyncMock(
        side_effect=RuntimeError("dictionary unavailable")
        if query_fails
        else None,
        return_value=[],
    )
    monkeypatch.setattr(serving, "_version_three_dictionary_query", query)
    monkeypatch.setattr(
        serving,
        "_decoded_version_three_dictionary_values",
        lambda *_args: {("setting", 11): "office"},
    )

    if query_fails:
        with pytest.raises(RuntimeError, match="dictionary unavailable"):
            await serving._version_three_dictionary_values(
                object(), strict_v3_tables(), {1: object()}, budget
            )
    else:
        assert await serving._version_three_dictionary_values(
            object(), strict_v3_tables(), {1: object()}, budget
        ) == {("setting", 11): "office"}

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_dictionary_value_query_releases_empty_required_key_set(monkeypatch):
    """Return exact emptiness without querying when atoms need no dictionary."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)
    budget.claim(64, category="test empty required keys")
    monkeypatch.setattr(
        serving,
        "_required_version_three_dictionary_keys",
        lambda *_args: (set(), 64),
    )
    query = AsyncMock(side_effect=AssertionError("empty key set must not query"))
    monkeypatch.setattr(serving, "_version_three_dictionary_query", query)

    values_by_key = await serving._version_three_dictionary_values(
        object(), strict_v3_tables(), {1: object()}, budget
    )

    assert values_by_key == {}
    assert budget.retained_bytes == 0
    query.assert_not_awaited()


def test_price_payload_rejects_arity_and_defaults_null_arrays(monkeypatch):
    """Require exact atom arity and preserve array-shaped null defaults."""

    monkeypatch.setattr(serving, "_ptg2_price_atom_attr_specs", lambda: _ATTRIBUTE_SPECS)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="attribute-key count"):
        serving._version_three_price_payload(_price_atom(11), {}, {})

    payload = serving._version_three_price_payload(
        _price_atom(11, None, 31),
        {("setting", 11): "office"},
        {"billing_class": "professional"},
    )

    assert payload == {
        "negotiated_rate": "125.00",
        "setting": "office",
        "service_code": [],
        "billing_class": "professional",
    }


def test_atom_payload_projection_releases_budget_on_corruption(monkeypatch):
    """Release projected payload retention if any atom cannot be decoded."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)
    monkeypatch.setattr(
        serving,
        "_version_three_price_payload",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("corrupt atom")),
    )

    with pytest.raises(RuntimeError, match="corrupt atom"):
        serving._version_three_payloads_by_atom_key(
            {1: object()},
            {},
            {},
            budget,
        )

    assert budget.retained_bytes == 0


@pytest.mark.parametrize("atom_key_bits", [None, 25])
def test_atom_key_width_requires_supported_integer(atom_key_bits):
    """Reject absent and unsupported dense atom widths."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match="atom_key_bits"):
        serving._version_three_atom_key_bits(
            strict_v3_tables(atom_key_bits=atom_key_bits)
        )


@pytest.mark.asyncio
async def test_empty_price_hydration_releases_key_retention():
    """Return an exact empty hydration without retaining temporary key state."""

    budget = serving.CandidateAuditDecodedRetentionBudget(maximum_bytes=16_384)

    hydration = await serving._version_three_price_hydration(
        object(),
        strict_v3_tables(),
        (),
        retention_budget=budget,
    )

    assert hydration == serving._VersionThreePriceHydration({}, {})
    assert budget.retained_bytes == 0
