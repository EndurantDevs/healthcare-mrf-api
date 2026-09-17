# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import npi_result_generation as generation


class _Result:
    def __init__(self, value=None):
        self.value = value

    def mappings(self):
        return self

    def one_or_none(self):
        return self.value

    def all(self):
        return self.value


def _authority(**overrides):
    authority_by_field = {
        "singleton": True,
        "local_lineage_id": str(uuid4()),
        "local_generation": 4,
        "origin_lineage_id": None,
        "origin_generation": None,
        "published_at": None,
        "relation_oids": None,
        "canonical_publication_ref": None,
        "canonical_publication_generation": None,
        "canonical_chain_ref": None,
        "canonical_import_date": None,
    }
    authority_by_field.update(overrides)
    return authority_by_field


def _tracked_authority(**overrides):
    values = _authority(
        origin_lineage_id=str(uuid4()),
        origin_generation=3,
        published_at="2026-09-14T12:00:00Z",
        relation_oids=[10, 11, 12, 13, 14, 15],
    )
    values.update(overrides)
    return generation.validate_npi_result_generation_authority(values)


def _provenance():
    return generation.NpiCanonicalProvenance(
        "nppub1_" + "a" * 43,
        2,
        "penpc1_" + "b" * 43,
        datetime.date(2026, 9, 14),
    )


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda: generation._schema_name("bad-name"), "schema"),
        (lambda: generation._identifier(3, field_name="stage table"), "stage table"),
        (lambda: generation._uuid_text("bad"), "lineage"),
        (lambda: generation._timestamp("bad"), "time"),
        (lambda: generation._timestamp(datetime.datetime(2026, 1, 1)), "time"),
        (lambda: generation._generation(True), "serving generation"),
        (lambda: generation._canonical_generation(1 << 53), "canonical provenance"),
        (lambda: generation._relation_oids([1]), "relation identity"),
        (lambda: generation.validate_npi_serving_generation({}), "serving generation"),
        (lambda: generation.validate_npi_canonical_provenance({}), "canonical provenance"),
        (
            lambda: generation.validate_npi_canonical_provenance(
                {
                    "publication_ref": "nppub1_" + "a" * 43,
                    "publication_generation": 1,
                    "chain_ref": "penpc1_" + "b" * 43,
                    "import_date": "bad",
                }
            ),
            "canonical provenance",
        ),
        (
            lambda: generation.validate_npi_canonical_provenance(
                {
                    "publication_ref": "bad",
                    "publication_generation": 1,
                    "chain_ref": "penpc1_" + "b" * 43,
                    "import_date": "2026-09-14",
                }
            ),
            "canonical provenance",
        ),
    ],
)
def test_generation_value_guards(call, message) -> None:
    with pytest.raises(ValueError, match=message):
        call()


def test_generation_value_objects_serialize_and_revalidate() -> None:
    serving = generation.NpiServingGeneration(
        str(uuid4()),
        3,
        datetime.datetime(2026, 9, 14, 12, tzinfo=datetime.UTC),
    )
    provenance = _provenance()
    authority = generation.NpiResultGenerationAuthority(
        str(uuid4()),
        4,
        serving,
        (10, 11, 12, 13, 14, 15),
        provenance,
    )

    assert generation.validate_npi_serving_generation(serving) == serving
    assert generation.validate_npi_canonical_provenance(provenance) == provenance
    assert authority.as_dict()["canonical_provenance"] == provenance.as_dict()


class _KeyedRow:
    def __init__(self, values):
        self._values = values

    def keys(self):
        return self._values.keys()

    def __getitem__(self, key):
        return self._values[key]


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (object(), "authority is unavailable"),
        (_authority(singleton=False), "authority is unavailable"),
        (_authority(local_generation=-1), "local result generation is invalid"),
    ],
)
def test_generation_authority_rejects_unusable_rows(row, message) -> None:
    with pytest.raises(RuntimeError, match=message):
        generation.validate_npi_result_generation_authority(row)


def test_generation_authority_accepts_keyed_row_adapter() -> None:
    validated = generation.validate_npi_result_generation_authority(_KeyedRow(_authority()))
    assert validated.local_generation == 4


@pytest.mark.asyncio
async def test_generation_queries_reject_missing_or_malformed_rows() -> None:
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(None)))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.read_npi_result_generation_authority(session, schema_name="mrf")

    session.execute.return_value = _Result([("npi", None)])
    with pytest.raises(RuntimeError, match="serving relations are unavailable"):
        await generation.current_npi_relation_oids(session, schema_name="mrf")

    rows = list(zip(reversed(generation.RELATION_NAMES), range(10, 16), strict=True))
    session.execute.return_value = _Result(rows)
    with pytest.raises(RuntimeError, match="serving relations are unavailable"):
        await generation.current_npi_relation_oids(session, schema_name="mrf")


@pytest.mark.asyncio
async def test_matching_provenance_accepts_exact_receipt() -> None:
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=_Result(
                {
                    "publication_ref": "nppub1_" + "a" * 43,
                    "publication_generation": 2,
                    "chain_ref": "penpc1_" + "b" * 43,
                    "import_date": datetime.date(2026, 9, 14),
                }
            )
        )
    )
    assert (
        await generation._matching_canonical_provenance(
            session,
            schema_name="mrf",
            relation_oids=(10, 11, 12, 13, 14, 15),
        )
        == _provenance()
    )


@pytest.mark.asyncio
async def test_capture_requires_current_tracked_identity(monkeypatch) -> None:
    authority = generation.validate_npi_result_generation_authority(_authority())
    monkeypatch.setattr(
        generation,
        "read_npi_result_generation_authority",
        AsyncMock(return_value=authority),
    )
    monkeypatch.setattr(
        generation,
        "current_npi_relation_oids",
        AsyncMock(return_value=(10, 11, 12, 13, 14, 15)),
    )

    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_npi_serving_generation(object(), schema_name="mrf")

    tracked = _tracked_authority()
    generation.read_npi_result_generation_authority.return_value = tracked
    assert await generation.capture_npi_serving_generation(object(), schema_name="mrf") == tracked


@pytest.mark.asyncio
async def test_bootstrap_rejects_invalid_transaction_and_terminal_states(monkeypatch) -> None:
    invalid = SimpleNamespace(in_transaction=lambda: False)
    with pytest.raises(ValueError, match="caller transaction"):
        await generation.bootstrap_npi_result_generation(invalid, schema_name="mrf")

    @asynccontextmanager
    async def no_limits(_session):
        yield

    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    monkeypatch.setattr(generation, "_bounded_bootstrap", no_limits)
    monkeypatch.setattr(
        generation,
        "current_npi_relation_oids",
        AsyncMock(return_value=(10, 11, 12, 13, 14, 15)),
    )
    read = AsyncMock(return_value=_tracked_authority(relation_oids=[20, 21, 22, 23, 24, 25]))
    monkeypatch.setattr(generation, "read_npi_result_generation_authority", read)
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.bootstrap_npi_result_generation(session, schema_name="mrf")

    tracked = _tracked_authority()
    read.return_value = tracked
    assert await generation.bootstrap_npi_result_generation(session, schema_name="mrf") == tracked

    read.return_value = generation.validate_npi_result_generation_authority(
        _authority(local_generation=generation._MAX_GENERATION)
    )
    with pytest.raises(RuntimeError, match="exhausted"):
        await generation.bootstrap_npi_result_generation(session, schema_name="mrf")


@pytest.mark.asyncio
async def test_bootstrap_helpers_reject_missing_state() -> None:
    with pytest.raises(RuntimeError, match="timeout state"):
        await generation._timeout_value(SimpleNamespace(scalar=AsyncMock(return_value=None)), "lock_timeout")

    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(None)))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation._write_bootstrap_authority(
            session,
            schema_name="mrf",
            next_generation=1,
            relation_oids=(10, 11, 12, 13, 14, 15),
            provenance=None,
        )


@pytest.mark.asyncio
async def test_generation_guard_installers_validate_shape_and_adapters() -> None:
    with pytest.raises(ValueError, match="stage family"):
        generation._guard_statements("mrf", ("npi",))
    with pytest.raises(ValueError, match="stage family"):
        await generation.install_npi_stage_mutation_guards(
            SimpleNamespace(execute=AsyncMock()),
            schema_name="mrf",
            stage_tables=("npi",),
        )

    asyncpg_connection = SimpleNamespace(fetchrow=AsyncMock(), execute=AsyncMock())
    await generation._execute_ddl(asyncpg_connection, "SELECT 1")
    asyncpg_connection.execute.assert_awaited_once_with("SELECT 1")


def _receipt():
    return SimpleNamespace(
        publication_ref="nppub1_" + "a" * 43,
        publication_generation=2,
        chain_ref="penpc1_" + "b" * 43,
        import_date="2026-09-14",
        created_at="2026-09-14T12:00:00Z",
        relation_oids=(10, 11, 12, 13, 14, 15),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing-current", "exhausted", "missing-relations", "drift", "missing-update"])
async def test_local_publication_rejects_invalid_state(monkeypatch, failure) -> None:
    monkeypatch.setattr(generation, "validate_npi_canonical_publication_receipt", lambda value: value)
    current = _authority()
    if failure == "exhausted":
        current = _authority(local_generation=generation._MAX_GENERATION)
    responses = [None] if failure == "missing-current" else [current]
    if failure not in {"missing-current", "exhausted"}:
        if failure == "missing-relations":
            responses.append(None)
        else:
            responses.append({f"relation_{ordinal}": 9 + ordinal for ordinal in range(1, 7)})
            if failure == "missing-update":
                responses.append(None)
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=responses))
    expected = {
        "missing-current": "authority is unavailable",
        "exhausted": "exhausted",
        "missing-relations": "serving relations are unavailable",
        "drift": "relation identity differs",
        "missing-update": "authority is unavailable",
    }[failure]
    receipt = _receipt()
    if failure == "drift":
        receipt.relation_oids = (20, 21, 22, 23, 24, 25)
    with pytest.raises(RuntimeError, match=expected):
        await generation.publish_local_npi_result_generation(
            connection,
            schema_name="mrf",
            receipt=receipt,
        )


@pytest.mark.asyncio
async def test_adoption_rejects_missing_or_changed_local_authority(monkeypatch) -> None:
    current = generation.validate_npi_result_generation_authority(_authority())
    monkeypatch.setattr(
        generation,
        "read_npi_result_generation_authority",
        AsyncMock(return_value=current),
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(None)))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.publish_adopted_npi_result_generation(
            session,
            schema_name="mrf",
            source_generation=None,
            canonical_provenance=None,
        )

    changed = _authority(local_generation=5)
    session.execute.return_value = _Result(changed)
    with pytest.raises(RuntimeError, match="changed during adoption"):
        await generation.publish_adopted_npi_result_generation(
            session,
            schema_name="mrf",
            source_generation=None,
            canonical_provenance=None,
        )
