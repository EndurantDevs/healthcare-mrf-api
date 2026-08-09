# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation-bound cache proof for canonical NPI publication receipts."""

from __future__ import annotations

from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


PUBLICATION_ONE = "1:nppub1_" + "a" * 43
PUBLICATION_TWO = "2:nppub1_" + "b" * 43


class _RowsResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


@pytest.mark.asyncio
async def test_publication_identity_requires_one_sealed_live_oid_match(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    execute_stmt = AsyncMock(
        return_value=_RowsResult([(7, "nppub1_" + "c" * 43)])
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)

    identity = await npi_module._npi_canonical_publication_identity()

    assert identity == "7:nppub1_" + "c" * 43
    statement = str(execute_stmt.await_args.args[0])
    assert "npi_canonical_publication_receipt_seal" in statement
    assert statement.count("to_regclass") == 6
    assert set(execute_stmt.await_args.kwargs["params"]) == {
        "npi_ref",
        "npi_address_ref",
        "npi_taxonomy_ref",
        "npi_taxonomy_group_ref",
        "npi_other_identifier_ref",
        "npi_phone_staffing_ref",
    }
    assert set(execute_stmt.await_args.kwargs["params"].values()) == {
        f"mrf.{table_name}"
        for table_name in (
            "npi",
            "npi_address",
            "npi_taxonomy",
            "npi_taxonomy_group",
            "npi_other_identifier",
            "npi_phone_staffing",
        )
    }


@pytest.mark.parametrize(
    ("runtime_schema", "legacy_schema", "expected"),
    [
        (None, None, "mrf"),
        ("runtime_schema", None, "runtime_schema"),
        (None, "legacy_schema", "legacy_schema"),
        ("shared_schema", "shared_schema", "shared_schema"),
    ],
)
def test_runtime_schema_resolver_matches_migration_configuration(
    monkeypatch,
    runtime_schema,
    legacy_schema,
    expected,
):
    if runtime_schema is None:
        monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", runtime_schema)
    if legacy_schema is None:
        monkeypatch.delenv("DB_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("DB_SCHEMA", legacy_schema)

    assert npi_module._runtime_db_schema() == expected
    assert npi_module._schema_cache_key("npi") == f"{expected}.npi"


@pytest.mark.parametrize(
    ("runtime_schema", "legacy_schema", "message"),
    [
        ("runtime_schema", "legacy_schema", "configuration_conflicts"),
        ("invalid-schema", None, "schema_invalid"),
    ],
)
def test_runtime_schema_resolver_rejects_conflicts_and_invalid_names(
    monkeypatch,
    runtime_schema,
    legacy_schema,
    message,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", runtime_schema)
    if legacy_schema is None:
        monkeypatch.delenv("DB_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("DB_SCHEMA", legacy_schema)

    with pytest.raises(RuntimeError, match=message):
        npi_module._runtime_db_schema()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity_rows",
    [
        [],
        [(1, "nppub1_" + "a" * 43), (2, "nppub1_" + "b" * 43)],
        [(True, "nppub1_" + "a" * 43)],
        [(0, "nppub1_" + "a" * 43)],
        [(1, "invalid")],
    ],
)
async def test_publication_identity_fails_closed_on_ambiguous_shape(
    monkeypatch,
    identity_rows,
):
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(return_value=_RowsResult(identity_rows)),
    )

    assert await npi_module._npi_canonical_publication_identity() is None


@pytest.mark.asyncio
async def test_publication_identity_query_failure_bypasses_cache(monkeypatch):
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(side_effect=RuntimeError("synthetic read failure")),
    )

    assert await npi_module._npi_canonical_publication_identity() is None


@pytest.mark.asyncio
async def test_primary_count_cache_misses_after_publication_change(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(
        npi_module,
        "_NPI_PRIMARY_TOTAL_CACHE_STATE",
        {"entry": None},
    )
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(side_effect=[PUBLICATION_ONE, PUBLICATION_ONE, PUBLICATION_TWO]),
    )
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.NPIAddress),
    )
    scalar = AsyncMock(side_effect=[11, 22])
    monkeypatch.setattr(npi_module.db, "scalar", scalar)

    assert await npi_module._fast_primary_npi_count() == 11
    assert await npi_module._fast_primary_npi_count() == 11
    assert await npi_module._fast_primary_npi_count() == 22
    assert scalar.await_count == 2


@pytest.mark.asyncio
async def test_primary_count_cache_misses_after_unified_address_swap(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(
        npi_module,
        "_NPI_PRIMARY_TOTAL_CACHE_STATE",
        {"entry": None},
    )
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value=PUBLICATION_ONE),
    )
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.EntityAddressUnified),
    )
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(
            side_effect=[_ScalarResult(101), _ScalarResult(101), _ScalarResult(202)]
        ),
    )
    scalar = AsyncMock(side_effect=[11, 22])
    monkeypatch.setattr(npi_module.db, "scalar", scalar)

    assert await npi_module._fast_primary_npi_count() == 11
    assert await npi_module._fast_primary_npi_count() == 11
    assert await npi_module._fast_primary_npi_count() == 22
    assert scalar.await_count == 2


@pytest.mark.asyncio
async def test_insurance_count_cache_misses_after_publication_change(monkeypatch):
    counts = iter((31, 42))
    execute = AsyncMock(
        side_effect=lambda _statement: SimpleNamespace(
            scalar=lambda: next(counts)
        )
    )

    class _SessionContext:
        async def __aenter__(self):
            return SimpleNamespace(execute=execute)

        async def __aexit__(self, *_error):
            return False

    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi_module, "_NPI_HAS_INSURANCE_TOTAL_CACHE", {})
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(side_effect=[PUBLICATION_ONE, PUBLICATION_ONE, PUBLICATION_TWO]),
    )
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.NPIAddress),
    )
    monkeypatch.setattr(
        npi_module,
        "db",
        SimpleNamespace(session=lambda: _SessionContext()),
    )

    assert await npi_module._fast_has_insurance_count(None, None) == 31
    assert await npi_module._fast_has_insurance_count(None, None) == 31
    assert await npi_module._fast_has_insurance_count(None, None) == 42
    assert execute.await_count == 2


@pytest.mark.asyncio
async def test_classification_cache_misses_after_publication_change(monkeypatch):
    query_rows = iter(([(1003000100,)], [(1003000118,)]))
    execute = AsyncMock(side_effect=lambda *_args, **_kwargs: _RowsResult(next(query_rows)))
    session = SimpleNamespace(execute=execute)
    monkeypatch.setattr(npi_module, "_CLASSIFICATION_NPI_CACHE", {})
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(side_effect=[PUBLICATION_ONE, PUBLICATION_ONE, PUBLICATION_TWO]),
    )
    monkeypatch.setattr(
        npi_module,
        "_get_taxonomy_codes_for_classification",
        AsyncMock(return_value=["282N00000X"]),
    )

    assert await npi_module._get_classification_npi_list(
        "Hospital", session=session
    ) == [1003000100]
    assert await npi_module._get_classification_npi_list(
        "Hospital", session=session
    ) == [1003000100]
    assert await npi_module._get_classification_npi_list(
        "Hospital", session=session
    ) == [1003000118]
    assert execute.await_count == 2


def test_detail_response_cache_key_changes_with_publication_identity(monkeypatch):
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", OrderedDict())
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    common_options_by_name = {
        "npi": 1003000100,
        "view": "detail",
        "include_chain": False,
        "extra_info": False,
        "sync_geocode": False,
        "lookup_stored_geocode": False,
    }
    first_key = npi_module._npi_detail_cache_key(
        **common_options_by_name,
        canonical_publication_identity=PUBLICATION_ONE,
    )
    second_key = npi_module._npi_detail_cache_key(
        **common_options_by_name,
        canonical_publication_identity=PUBLICATION_TWO,
    )

    npi_module._npi_detail_response_cache_set(first_key, b"generation-one")
    assert npi_module._npi_detail_response_cache_get(first_key) == b"generation-one"
    assert npi_module._npi_detail_response_cache_get(second_key) is None
