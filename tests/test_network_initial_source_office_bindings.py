# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact typed/legacy ACA office contract and bounded native projection choices."""

from types import SimpleNamespace

import pytest

from process import network_initial_source_office_bindings as office_bindings
from process import reference_family_archive as archive
from process.network_address_projection import PinnedAddressSource
from process.network_initial_source_office_bindings import InitialACAOfficeBindingError

pytestmark = pytest.mark.asyncio


def _canonical_contract_pin(*, typed, canonical_oid=31, auxiliary_oid=None, auxiliary_contract=None):
    manifest = SimpleNamespace(
        auxiliary={"contract": archive.MODEL_CLOSURE_CONTRACT if typed else auxiliary_contract},
        as_dict=lambda: {"contract": archive.TYPED_MRF_CONTRACT if typed else archive.CONTRACT},
    )
    relation_oids = (("mrf_address_evidence", 30),) + ((("mrf_canonical_address", canonical_oid),) if typed else ())
    return SimpleNamespace(
        prepared=SimpleNamespace(
            manifest=manifest,
            ownership=SimpleNamespace(
                schema_name="reference_family_archive_example",
                relation_oids=relation_oids,
                auxiliary_oid=auxiliary_oid,
            ),
        )
    )


@pytest.mark.parametrize(
    "typed,model_contract",
    [(False, None), (True, archive.MODEL_CLOSURE_CONTRACT), (True, archive.ACCOUNTED_MODEL_CLOSURE_CONTRACT)],
)
async def test_canonical_contract_selects_exact_native_oid_and_minimal_query_projection(typed, model_contract):
    pin = _canonical_contract_pin(typed=typed, auxiliary_oid=None if typed else 32)
    pin.prepared.manifest.auxiliary["contract"] = model_contract
    canonical_oid, canonical_sql = office_bindings._canonical_source(pin)
    assert canonical_oid == (31 if typed else 32)
    if typed:
        assert canonical_sql.startswith("jsonb_build_object(")
        assert "c.payload" not in canonical_sql and "to_jsonb(c)" not in canonical_sql
        assert all("c." + field in canonical_sql for field in office_bindings._TYPED_CANONICAL_FIELDS)
        assert "premise_identity_key" not in canonical_sql and "c.telephone_number" not in canonical_sql
    else:
        assert canonical_sql == "c.payload"
    calls = []

    async def fetchrow(statement, *arguments):
        calls.append((statement, arguments))
        return {"invalid": False, "pairs": 0, "covered_pairs": 0, "records_json": "[]"}

    batch = SimpleNamespace(source=pin, input_bytes=b"[]", accepted_evidence_checksums=())
    for name, coordinate in (
        ("import_id", "import-one"),
        ("issuer_id", 7),
        ("year", 2026),
        ("source_url", "https://source.example.test/providers"),
    ):
        setattr(pin, name, coordinate)
    assert (
        await office_bindings._office_records(
            SimpleNamespace(fetchrow=fetchrow),
            batch,
            PinnedAddressSource("retained_addresses", "entity_address_unified", "edition-one"),
        )
        == []
    )
    assert canonical_sql + " AS canonical" in calls[0][0]
    assert calls[0][1] == ("[]", "import-one", 7, 2026, "https://source.example.test/providers", [])


@pytest.mark.parametrize("typed", [False, True])
@pytest.mark.parametrize("oid", [None, False, 0, 2**32])
async def test_canonical_contract_refuses_missing_or_invalid_native_oid_without_payload_fallback(typed, oid):
    pin = _canonical_contract_pin(typed=typed, canonical_oid=oid, auxiliary_oid=None if typed else oid)
    with pytest.raises(InitialACAOfficeBindingError, match="input_invalid"):
        office_bindings._canonical_source(pin)


async def test_canonical_contract_refuses_mixed_or_unknown_layouts():
    for pin in (
        _canonical_contract_pin(typed=True, auxiliary_oid=32),
        _canonical_contract_pin(typed=False, auxiliary_oid=32, auxiliary_contract="unrecognized"),
    ):
        with pytest.raises(InitialACAOfficeBindingError, match="input_invalid"):
            office_bindings._canonical_source(pin)


async def test_typed_fixture_populates_actual_model_columns_without_payload_storage():
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from process.mrf_address_publication import canonical_contribution_model
    from tests.test_network_initial_source_office_bindings_postgres import _insert_canonical_office

    canonical_by_field = {field: None for field in office_bindings._TYPED_CANONICAL_FIELDS}
    canonical_by_field.update(address_key=str(uuid4()), premise_key=str(uuid4()), premise_identity_key="premise")
    session = SimpleNamespace(execute=AsyncMock())
    await _insert_canonical_office(session, "typed_offices", None, canonical_by_field, typed=True)
    statement, parameters_by_name = session.execute.await_args.args
    assert '"typed_offices".mrf_canonical_address' in str(statement)
    assert "payload" not in str(statement) and "premise_identity_key" not in parameters_by_name
    assert set(parameters_by_name) == set(office_bindings._TYPED_CANONICAL_FIELDS)
    assert set(parameters_by_name) <= set(canonical_contribution_model("mrf").__table__.columns.keys())
    assert str(parameters_by_name["address_key"]) == canonical_by_field["address_key"]
