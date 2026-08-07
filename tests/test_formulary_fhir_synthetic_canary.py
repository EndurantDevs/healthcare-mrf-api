# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit contracts for the fixed synthetic formulary seed candidate."""

from __future__ import annotations

import aiohttp
import asyncio
import socket

import pytest

import process.formulary_fhir.source as source_module
import process.formulary_fhir.synthetic_canary as canary_module
from process.formulary_fhir.synthetic_canary_contract import CANARY_CUTOFF
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_BASE
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_SOURCE_DISPLAY_NAME,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import (
    SyntheticCanaryContractError,
)
from process.formulary_fhir.synthetic_canary_contract import canary_metadata
from process.formulary_fhir.synthetic_canary_contract import canary_runtime_config
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_canary_transport import SyntheticCanaryClient
from process.formulary_fhir.synthetic_canary_transport import SyntheticCanarySession
from process.formulary_fhir.types import enabled_source_config


def _source_row(*, enabled: bool = True) -> dict[str, object]:
    return {
        "source_id": CANARY_SOURCE_ID,
        "canonical_base": CANARY_SOURCE_BASE,
        "display_name": CANARY_SOURCE_DISPLAY_NAME,
        "enabled": enabled,
        "runtime_config_json": canary_runtime_config(),
        "metadata_json": canary_metadata(),
    }


def test_fixed_source_binding_matches_checked_in_configuration_hash():
    binding = source_module._binding_from_row(CANARY_SOURCE_ID, _source_row())

    assert binding.configuration_hash == expected_evidence()[
        "source_configuration_hash"
    ]
    assert binding.config.canonical_base == CANARY_SOURCE_BASE
    assert canary_module._is_exact_source(_source_row(), enabled=True)
    assert canary_module._is_exact_source(
        _source_row(enabled=False),
        enabled=False,
    )
    numeric_metadata_row = _source_row()
    numeric_metadata_row["metadata_json"] = {
        "canary_contract": "formulary-fhir-synthetic-v1",
        "synthetic": 1,
    }
    float_runtime_row = _source_row()
    float_runtime_row["runtime_config_json"] = canary_runtime_config()
    float_runtime_row["runtime_config_json"]["timeout_seconds"] = 5.0
    assert not canary_module._is_exact_source(
        numeric_metadata_row,
        enabled=True,
    )
    assert not canary_module._is_exact_source(float_runtime_row, enabled=True)


@pytest.mark.asyncio
async def test_real_client_uses_exact_nine_request_socket_free_contract(monkeypatch):
    def forbid_connector(*_args, **_kwargs):
        raise AssertionError("network connector constructed")

    def forbid_session(*_args, **_kwargs):
        raise AssertionError("network session constructed")

    def forbid_socket(*_args, **_kwargs):
        raise AssertionError("network socket constructed")

    async def forbid_connection(*_args, **_kwargs):
        raise AssertionError("network connection opened")

    monkeypatch.setattr(aiohttp, "TCPConnector", forbid_connector)
    monkeypatch.setattr(aiohttp, "ClientSession", forbid_session)
    monkeypatch.setattr(socket, "socket", forbid_socket)
    monkeypatch.setattr(asyncio, "open_connection", forbid_connection)
    config = enabled_source_config(
        canonical_base=CANARY_SOURCE_BASE,
        enabled=True,
        runtime_config_json=canary_runtime_config(),
    )
    async with SyntheticCanaryClient(config) as client:
        coverage_census = await client.coverage_plan_current_census(
            cutoff=CANARY_CUTOFF
        )
        first_census = await client.medication_current_census(
            "SYNTH-A",
            cutoff=CANARY_CUTOFF,
        )
        second_census = await client.medication_current_census(
            "SYNTH-B",
            cutoff=CANARY_CUTOFF,
        )

    assert client.request_count == 9
    assert client.synthetic_session.call_count == 9
    assert coverage_census.search_contract_hash == (
        "fb2b6fea5fc0d821e02aac1f92c95a11a9e30bd33548a6a9220d5ef3a2857588"
    )
    assert first_census.resources[0]["id"] == "synthetic-drug-a"
    assert second_census.resources[0]["id"] == "synthetic-drug-b"


@pytest.mark.asyncio
async def test_real_client_allows_exact_completed_alias_skip():
    config = enabled_source_config(
        canonical_base=CANARY_SOURCE_BASE,
        enabled=True,
        runtime_config_json=canary_runtime_config(),
    )
    async with SyntheticCanaryClient(config) as client:
        coverage_census = await client.coverage_plan_current_census(
            cutoff=CANARY_CUTOFF
        )
        second_census = await client.medication_current_census(
            "SYNTH-B",
            cutoff=CANARY_CUTOFF,
        )

    assert coverage_census.exact_total == 1
    assert second_census.resources[0]["id"] == "synthetic-drug-b"
    assert client.request_count == 6
    assert client.synthetic_session.call_count == 6


@pytest.mark.asyncio
async def test_real_client_rejects_unknown_out_of_order_and_extra_aliases():
    config = enabled_source_config(
        canonical_base=CANARY_SOURCE_BASE,
        enabled=True,
        runtime_config_json=canary_runtime_config(),
    )
    async with SyntheticCanaryClient(config) as partial_client:
        await partial_client.coverage_plan_current_census(cutoff=CANARY_CUTOFF)
        with pytest.raises(SyntheticCanaryContractError):
            await partial_client.medication_current_census(
                "UNKNOWN",
                cutoff=CANARY_CUTOFF,
            )
    async with SyntheticCanaryClient(config) as complete_client:
        await complete_client.coverage_plan_current_census(cutoff=CANARY_CUTOFF)
        for alias in ("SYNTH-A", "SYNTH-B"):
            await complete_client.medication_current_census(
                alias,
                cutoff=CANARY_CUTOFF,
            )
        with pytest.raises(SyntheticCanaryContractError):
            await complete_client.medication_current_census(
                "SYNTH-A",
                cutoff=CANARY_CUTOFF,
            )


@pytest.mark.asyncio
async def test_real_client_error_exit_does_not_claim_complete_sequence():
    config = enabled_source_config(
        canonical_base=CANARY_SOURCE_BASE,
        enabled=True,
        runtime_config_json=canary_runtime_config(),
    )
    client = SyntheticCanaryClient(config)

    with pytest.raises(RuntimeError, match="synthetic interruption"):
        async with client:
            raise RuntimeError("synthetic interruption")

    assert client.synthetic_session.call_count == 0


def test_synthetic_session_rejects_any_unexpected_request():
    config = enabled_source_config(
        canonical_base=CANARY_SOURCE_BASE,
        enabled=True,
        runtime_config_json=canary_runtime_config(),
    )
    session = SyntheticCanarySession(config)

    with pytest.raises(SyntheticCanaryContractError):
        session.get(
            "https://different.example.invalid/List",
            params=(),
            timeout=aiohttp.ClientTimeout(total=5),
            allow_redirects=False,
        )
    with pytest.raises(SyntheticCanaryContractError):
        session.require_valid_stop()
