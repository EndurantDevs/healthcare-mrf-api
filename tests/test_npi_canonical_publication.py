# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure and store-boundary tests for canonical-NPI publication receipts."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.npi_canonical_publication import (
    NPI_CANONICAL_PUBLICATION_CONTRACT,
    NpiCanonicalPublicationError,
    NpiCanonicalPublicationInput,
    NpiCanonicalPublicationReceipt,
    build_npi_canonical_publication_receipt,
    receipt_insert_values,
    receipt_metrics,
    validate_npi_canonical_publication_receipt,
)
from process.npi_canonical_publication_store import (
    canonical_relation_oids,
    insert_npi_publication_receipt,
    lock_npi_publication_attempt,
)


RUN_ID = "run_publication_unit"
ATTEMPT_ID = RUN_ID + ":" + "a" * 32
ATTEMPT_STARTED_AT = "2026-08-09T01:02:03.456789+00:00"
CHAIN_REF = "penpc1_" + "b" * 43
RELATION_OIDS = (101, 102, 103, 104, 105, 106)
ROW_COUNTS = (1, 2, 3, 4, 5, 6)
CREATED_AT = "2026-08-09T02:03:04.567890+00:00"
TERMINAL_AT = dt.datetime(2026, 8, 9, 2, 4, 5, 678901)


def _publication_input(**overrides):
    input_values_by_field = {
        "run_id": RUN_ID,
        "attempt_id": ATTEMPT_ID,
        "attempt_started_at": ATTEMPT_STARTED_AT,
        "chain_ref": CHAIN_REF,
        "import_date": "2026-08-09",
        "relation_oids": RELATION_OIDS,
        "row_counts": ROW_COUNTS,
        **overrides,
    }
    return NpiCanonicalPublicationInput(**input_values_by_field)


def _receipt(*, generation: int = 7, row_counts=ROW_COUNTS):
    return build_npi_canonical_publication_receipt(
        _publication_input(row_counts=row_counts),
        publication_generation=generation,
        created_at=CREATED_AT,
    )


def _stored_row(*, sealed: bool | None = None, row_counts=ROW_COUNTS):
    receipt = _receipt(row_counts=row_counts)
    stored_row_by_field = {
        "publication_generation": receipt.publication_generation,
        "publication_ref": receipt.publication_ref,
        "contract": receipt.contract,
        "contract_sha256": bytes.fromhex(receipt.contract_sha256),
        "run_id": receipt.run_id,
        "attempt_id": receipt.attempt_id,
        "attempt_started_at": dt.datetime.fromisoformat(receipt.attempt_started_at),
        "chain_ref": receipt.chain_ref,
        "import_date": dt.date.fromisoformat(receipt.import_date),
        "publication_state": receipt.publication_state,
        "evidence_serving_authority": receipt.evidence_serving_authority,
        "evidence_publication_enabled": receipt.evidence_publication_enabled,
        "created_at": dt.datetime.fromisoformat(receipt.created_at),
    }
    oid_columns = (
        "npi_table_oid",
        "npi_address_table_oid",
        "npi_taxonomy_table_oid",
        "npi_taxonomy_group_table_oid",
        "npi_other_identifier_table_oid",
        "npi_phone_staffing_table_oid",
    )
    count_columns = tuple(column.replace("table_oid", "row_count") for column in oid_columns)
    stored_row_by_field.update(dict(zip(oid_columns, RELATION_OIDS, strict=True)))
    stored_row_by_field.update(dict(zip(count_columns, row_counts, strict=True)))
    if sealed is not None:
        stored_row_by_field["is_sealed"] = sealed
    return stored_row_by_field


def test_publication_receipt_is_frozen_and_value_safe():
    receipt = _receipt()
    assert receipt.contract == NPI_CANONICAL_PUBLICATION_CONTRACT
    assert receipt.publication_ref.startswith("nppub1_")
    assert len(receipt.contract_sha256) == 64
    assert validate_npi_canonical_publication_receipt(receipt) == receipt
    assert receipt_metrics(receipt) == {
        "publication_generation": 7,
        "publication_ref": receipt.publication_ref,
        "chain_ref": CHAIN_REF,
        "row_counts": {
            "npi": 1,
            "npi_address": 2,
            "npi_taxonomy": 3,
            "npi_taxonomy_group": 4,
            "npi_other_identifier": 5,
            "npi_phone_staffing": 6,
        },
    }
    assert repr(receipt) == "<npi-canonical-publication-receipt generation=7>"
    assert repr(_publication_input()) == "<npi-canonical-publication-input>"


@pytest.mark.parametrize(
    "overrides",
    (
        {"publication_generation": 0},
        {"run_id": ""},
        {"run_id": "run\nunsafe"},
        {"run_id": "r" * 65},
        {"attempt_id": RUN_ID + ":" + "A" * 32},
        {"attempt_started_at": "not-a-timestamp"},
        {"attempt_started_at": "2026-08-09T01:02:03Z"},
        {"chain_ref": "penpc1_invalid"},
        {"import_date": "not-a-date"},
        {"import_date": "2026-8-9"},
        {"relation_oids": (1, 2, 3)},
        {"relation_oids": (True, 2, 3, 4, 5, 6)},
        {"row_counts": (1, 2, 3, 4, 5, -1)},
    ),
)
def test_publication_receipt_rejects_noncanonical_boundaries(overrides):
    override_values_by_field = dict(overrides)
    generation = override_values_by_field.pop("publication_generation", 1)
    with pytest.raises(NpiCanonicalPublicationError) as caught:
        build_npi_canonical_publication_receipt(
            _publication_input(**override_values_by_field),
            publication_generation=generation,
            created_at=CREATED_AT,
        )
    assert str(caught.value) == "npi_canonical_publication_invalid"
    assert RUN_ID not in repr(caught.value)


def test_publication_receipt_rejects_wrong_outer_types_and_forgery():
    with pytest.raises(NpiCanonicalPublicationError):
        build_npi_canonical_publication_receipt(
            tuple(_publication_input()),
            publication_generation=1,
            created_at=CREATED_AT,
        )
    with pytest.raises(NpiCanonicalPublicationError):
        validate_npi_canonical_publication_receipt(tuple(_receipt()))
    forged = NpiCanonicalPublicationReceipt(
        *_receipt()._replace(publication_state="forged")
    )
    with pytest.raises(NpiCanonicalPublicationError):
        receipt_insert_values(forged)


@pytest.mark.asyncio
async def test_relation_oid_lookup_requires_six_ordinary_relations():
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={f"relation_{ordinal}": 100 + ordinal for ordinal in range(1, 7)}
        )
    )
    assert await canonical_relation_oids(connection, schema="mrf") == RELATION_OIDS
    query = connection.fetchrow.await_args.args[0]
    assert "pg_catalog.pg_class" in query
    assert "relation.relkind IN ('r','p')" in query
    assert connection.fetchrow.await_args.args[1:] == (
        "mrf",
        "npi",
        "npi_address",
        "npi_taxonomy",
        "npi_taxonomy_group",
        "npi_other_identifier",
        "npi_phone_staffing",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row_or_error",
    (
        None,
        {**{f"relation_{ordinal}": 100 + ordinal for ordinal in range(1, 7)}, "relation_3": None},
        RuntimeError("private database detail"),
    ),
)
async def test_relation_oid_lookup_fails_closed(row_or_error):
    fetchrow = (
        AsyncMock(side_effect=row_or_error)
        if isinstance(row_or_error, BaseException)
        else AsyncMock(return_value=row_or_error)
    )
    with pytest.raises(NpiCanonicalPublicationError) as caught:
        await canonical_relation_oids(
            SimpleNamespace(fetchrow=fetchrow),
            schema="mrf",
        )
    assert str(caught.value) == "npi_canonical_publication_invalid"


@pytest.mark.asyncio
async def test_publication_attempt_lock_requires_exact_running_owner():
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={
                "importer": "npi",
                "status": "running",
                "attempt_id": ATTEMPT_ID,
                "attempt_started_at": ATTEMPT_STARTED_AT,
            }
        )
    )
    await lock_npi_publication_attempt(
        connection,
        schema="mrf",
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        attempt_started_at=ATTEMPT_STARTED_AT,
    )
    query = connection.fetchrow.await_args.args[0]
    assert "FOR UPDATE" in query
    assert connection.fetchrow.await_args.args[1:] == (RUN_ID,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row_or_error",
    (
        None,
        {
            "importer": "npi",
            "status": "canceling",
            "attempt_id": ATTEMPT_ID,
            "attempt_started_at": ATTEMPT_STARTED_AT,
        },
        RuntimeError("private database detail"),
    ),
)
async def test_publication_attempt_lock_fails_closed(row_or_error):
    fetchrow = (
        AsyncMock(side_effect=row_or_error)
        if isinstance(row_or_error, BaseException)
        else AsyncMock(return_value=row_or_error)
    )
    with pytest.raises(NpiCanonicalPublicationError):
        await lock_npi_publication_attempt(
            SimpleNamespace(fetchrow=fetchrow),
            schema="mrf",
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            attempt_started_at=ATTEMPT_STARTED_AT,
        )


@pytest.mark.asyncio
async def test_receipt_insert_and_exact_replay_converge():
    inserted_connection = SimpleNamespace(fetchrow=AsyncMock(return_value=_stored_row()))
    inserted = await insert_npi_publication_receipt(
        inserted_connection,
        schema="mrf",
        publication_input=_publication_input(),
    )
    assert inserted == _receipt()
    assert "ON CONFLICT (run_id) DO NOTHING" in inserted_connection.fetchrow.await_args.args[0]

    replay_connection = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=[None, _stored_row(sealed=True)])
    )
    replayed = await insert_npi_publication_receipt(
        replay_connection,
        schema="mrf",
        publication_input=_publication_input(),
    )
    assert replayed == inserted


@pytest.mark.asyncio
async def test_receipt_replay_rejects_unsealed_or_mismatched_state():
    for existing in (
        _stored_row(sealed=False),
        _stored_row(sealed=True, row_counts=(9, 2, 3, 4, 5, 6)),
    ):
        connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=[None, existing]))
        with pytest.raises(NpiCanonicalPublicationError):
            await insert_npi_publication_receipt(
                connection,
                schema="mrf",
                publication_input=_publication_input(),
            )


@pytest.mark.asyncio
async def test_receipt_insert_rejects_invalid_inputs_and_store_failures():
    with pytest.raises(NpiCanonicalPublicationError):
        await insert_npi_publication_receipt(
            SimpleNamespace(fetchrow=AsyncMock()),
            schema="mrf",
            publication_input=tuple(_publication_input()),
        )
    for side_effect in (
        [RuntimeError("private insert detail")],
        [None, RuntimeError("private replay detail")],
    ):
        connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=side_effect))
        with pytest.raises(NpiCanonicalPublicationError) as caught:
            await insert_npi_publication_receipt(
                connection,
                schema="mrf",
                publication_input=_publication_input(),
            )
        assert "private" not in repr(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stored_row_update",
    (
        {"publication_generation": "7"},
        {"created_at": CREATED_AT},
        {"created_at": dt.datetime(2026, 8, 9)},
        {"contract_sha256": object()},
    ),
)
async def test_receipt_insert_rejects_malformed_stored_rows(stored_row_update):
    stored_row_by_field = _stored_row()
    stored_row_by_field.update(stored_row_update)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=stored_row_by_field))
    with pytest.raises(NpiCanonicalPublicationError):
        await insert_npi_publication_receipt(
            connection,
            schema="mrf",
            publication_input=_publication_input(),
        )
