# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""NPI-only witnessed aliases preserve ordinary strict/effective behavior."""

import hashlib
from dataclasses import asdict
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import mrf_address_publication as canonical
from process import npi_alias_witness as witnesses
from process import npi_isolated_addresses as addresses
from process import npi_result_archive as archive
from process.npi_isolated_execution import NpiInputRelation


def _raw(street="100 OAK ST", key=None):
    return {
        "rn": 1,
        "source_ctid": "(0,1)",
        "staged_address_key": key,
        "first_line": street,
        "second_line": None,
        "city_name": "EXAMPLE",
        "state_name": "PA",
        "postal_code": "19000",
        "raw_country_code": "US",
    }


def _key(street):
    return addresses.canon.address_key_v1(street, None, "EXAMPLE", "PA", "19000", "US")


def test_python_normalization_preserves_strict_encoding_and_rejects_mismatch():
    key = _key("100 OAK ST")
    encoded = addresses.normalized_record(_raw(" 100 OAK ST ", key))
    assert encoded[3] == encoded[4] == key
    assert encoded[14] == "100 OAK ST"
    with pytest.raises(RuntimeError):
        addresses.normalized_record(_raw(key=uuid4()))


def _witness():
    input_map = {
        name: asdict(NpiInputRelation("example_inputs", name, i + 1, i + 20)) for i, name in enumerate(witnesses.INPUTS)
    }
    return {
        "contract": witnesses.WITNESS_CONTRACT,
        "run_id": "run_example",
        "attempt_id": "run_example:attempt",
        "attempt_started_at": "2026-01-01T00:00:00+00:00",
        "observation_time": "2026-01-01T00:00:01+00:00",
        "input_receipt_sha256": "a" * 64,
        "inputs": input_map,
        "alias_generation": 3,
        "counts": dict.fromkeys(witnesses.SETS, 0),
        "hashes": dict.fromkeys(witnesses.SETS, "b" * 64),
        "identity_version": 2,
        "ruleset_version": 4,
        "formatter_version": 2,
        "strict_observations": 0,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("strict_observations", 1),
        ("strict_observations", False),
        ("alias_generation", True),
        ("observation_time", "2025-12-31T00:00:00+00:00"),
        ("input_receipt_sha256", "z" * 64),
        ("input_receipt_sha256", int("1" * 64)),
        ("identity_version", 2.0),
    ],
)
def test_portable_witness_is_closed_data_without_authority(field, value):
    witness_data = _witness()
    parsed = witnesses.validate_npi_address_witness(witness_data)
    assert parsed.as_dict() == witness_data
    witness_data[field] = value
    with pytest.raises(RuntimeError):
        witnesses.validate_npi_address_witness(witness_data)
    with pytest.raises(RuntimeError):
        witnesses.NpiAliasWitnessAuthority(None, None, (1, 1, 1, 1), None, (), parsed, AsyncMock(), object())


def test_sealed_observation_time_and_default_v2_stay_separate():
    argument_map = dict(archive="baseline", deduplicated="observed", strict="strict", formatted="formatted")
    ordinary = canonical.canonical_resolution_projection("npi", **argument_map)
    sealed = canonical.canonical_resolution_projection("npi", **argument_map, observed_at="2026-01-01T00:00:01+00:00")
    assert "transaction_timestamp()" in ordinary
    assert "transaction_timestamp()" not in sealed
    assert "TIMESTAMPTZ '2026-01-01T00:00:01.000000+00:00'" in sealed
    assert "updated_at" not in sealed


@pytest.mark.parametrize("instant", ["invalid-instant", "2026-01-01T00:00:00", "2026-01-01T00:00:00+01:00"])
def test_witness_rejects_invalid_or_unsealed_observation_time(instant):
    witness_data = _witness()
    witness_data["observation_time"] = instant
    with pytest.raises(RuntimeError):
        witnesses.validate_npi_address_witness(witness_data)


def test_typed_portable_witness_roundtrip_is_detached_and_profile_bound():
    portable = witnesses.validate_npi_address_witness(_witness())
    reconstructed = witnesses.validate_npi_address_witness(portable)
    assert reconstructed.encoded == portable.encoded
    assert reconstructed.sha256 == portable.sha256 == hashlib.sha256(portable.encoded).hexdigest()
    decoded = reconstructed.as_dict()
    decoded["counts"]["raw"] = 1
    assert portable.as_dict()["counts"]["raw"] == 0
    with pytest.raises(archive.NpiResultArchiveError, match="alias witness profile differs"):
        archive._validate_address_metadata({witnesses.METADATA_KEY: portable.as_dict()}, archive.MODEL_CONTRACT)


@pytest.mark.parametrize("metadata_by_key", [{}, {witnesses.METADATA_KEY: {}}])
def test_alias_profile_requires_complete_portable_witness(metadata_by_key):
    with pytest.raises(archive.NpiResultArchiveError, match="alias witness is unavailable"):
        archive._validate_address_metadata(metadata_by_key, archive.ALIAS_MODEL_CONTRACT)
