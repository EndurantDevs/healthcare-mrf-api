# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact bootstrap successors recover only their sealed NPI input."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import network_bootstrap_sources as bootstrap
from process import network_compact_custom_preparation as custom
from process.registry_publication_queue import publication_json
from process.registry_publication_execution import _compact_address_sources
from tests.test_network_bootstrap_address_artifact import _descriptor, _receipt


@pytest.mark.parametrize("with_custom", [False, True])
def test_compact_bootstrap_recovers_sealed_npi_with_or_without_custom_rows(monkeypatch, with_custom):
    receipt = _receipt()
    decode = bootstrap._decode

    def decode_canonical(document):
        decoded = decode(document)
        assert document == publication_json(decoded.as_dict())
        return decoded

    monkeypatch.setattr(bootstrap, "_decode", decode_canonical)
    monkeypatch.setattr(
        custom,
        "compact_custom_receipt",
        lambda _: SimpleNamespace(npi_source=receipt.npi_source) if with_custom else None,
    )
    candidate_by_name = {"validation_json": {"serving_readiness": {"address_artifact": _descriptor()}}}
    sources = _compact_address_sources(candidate_by_name, receipt.address_source)
    assert sources.base_source == receipt.address_source and sources.npi_source == receipt.npi_source
    assert sources.compact is True


@pytest.mark.parametrize("changed", ["address", "npi"])
def test_compact_bootstrap_refuses_another_input_generation(monkeypatch, changed):
    receipt = _receipt()
    npi = replace(receipt.npi_source, generation_id="d" * 64) if changed == "npi" else receipt.npi_source
    address = (
        replace(receipt.address_source, generation_id="d" * 64) if changed == "address" else receipt.address_source
    )
    monkeypatch.setattr(custom, "compact_custom_receipt", lambda _: SimpleNamespace(npi_source=npi))
    candidate_by_name = {"validation_json": {"serving_readiness": {"address_artifact": _descriptor()}}}
    with pytest.raises(ValueError, match="registry_source_rollback_address_changed"):
        _compact_address_sources(candidate_by_name, address)


@pytest.mark.parametrize("with_custom", [False, True])
def test_other_compact_address_contract_keeps_existing_npi_behavior(monkeypatch, with_custom):
    receipt = _receipt()
    npi = receipt.npi_source if with_custom else None
    monkeypatch.setattr(
        custom, "compact_custom_receipt", lambda _: SimpleNamespace(npi_source=npi) if with_custom else None
    )
    candidate_by_name = {"validation_json": {"serving_readiness": {"address_artifact": {"contract": "other"}}}}
    assert _compact_address_sources(candidate_by_name, receipt.address_source).npi_source == npi
