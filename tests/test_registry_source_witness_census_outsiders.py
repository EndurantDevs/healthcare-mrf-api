# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Census outsider admission contract, independent of a live database."""

from unittest.mock import AsyncMock, Mock

import pytest

from process import network_candidate_custody as custody
from process import registry_source_recipe_composition as composition
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

pytestmark = pytest.mark.asyncio


def _runtime(outsiders=("output_owner",)):
    return composition.SourceWitnessRuntime(
        Mock(side_effect=AssertionError("Source must not open in a census test")),
        (
            FHIRCatalogSourceReadProfile("source_one", "profile_owner_b", ("source_reader",)),
            FHIRCatalogSourceReadProfile("source_two", "profile_owner_a", ("source_reader",)),
        ),
        "capture_owner",
        ("census_execution", "census_reader"),
        census_outsiders=outsiders,
    )


def _connection(result):
    return Mock(fetchval=AsyncMock(return_value=result))


async def test_outsider_admission_binds_every_protected_owner_to_the_native_proof():
    runtime, connection = _runtime(), _connection(True)
    await composition.require_source_witness_census_outsiders(connection, runtime, ("output_owner",))
    sql, outsiders, protected, capture_owner = connection.fetchval.await_args.args
    assert sql is composition._CENSUS_OUTSIDERS_SQL
    assert outsiders == ["output_owner"] and capture_owner == "capture_owner"
    assert protected == ["capture_owner", "census_execution", "census_reader", "profile_owner_a", "profile_owner_b"]


@pytest.mark.parametrize("result", [False, None, 1, "t"])
async def test_outsider_admission_requires_an_exact_true_native_proof(result):
    connection = _connection(result)
    with pytest.raises(
        composition.RegistrySourceCompositionError, match="^registry_source_witness_runtime_census_differs$"
    ):
        await composition.require_source_witness_census_outsiders(connection, _runtime(), ("output_owner",))


async def test_undeclared_outsider_is_refused_before_reading():
    connection = _connection(True)
    with pytest.raises(
        composition.RegistrySourceCompositionError, match="^registry_source_witness_runtime_census_differs$"
    ):
        await composition.require_source_witness_census_outsiders(connection, _runtime(()), ("output_owner",))
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize(
    "declared", [["output_owner"], ("b_owner", "a_owner"), ("census_reader",), ("capture_owner",), ('owner"; --',)]
)
def test_declared_outsiders_must_be_sorted_identifiers_outside_the_census(declared):
    with pytest.raises(ValueError):
        _runtime(declared)


async def test_no_outsider_needs_no_database_read():
    connection = _connection(False)
    await composition.require_source_witness_census_outsiders(connection, _runtime(), ())
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize(
    "outsiders",
    [("b_owner", "a_owner"), ("output_owner", "output_owner"), ("census_reader",), ("capture_owner",)],
)
async def test_unsorted_duplicate_and_census_outsiders_refuse_before_reading(outsiders):
    connection = _connection(True)
    with pytest.raises(
        composition.RegistrySourceCompositionError, match="^registry_source_witness_runtime_census_differs$"
    ):
        await composition.require_source_witness_census_outsiders(connection, _runtime(), outsiders)
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("arguments", [(object(), ("output_owner",)), (None, ["output_owner"])])
async def test_duck_runtime_or_mutable_outsiders_are_invalid(arguments):
    runtime, outsiders = arguments
    connection = _connection(True)
    with pytest.raises(composition.RegistrySourceCompositionError, match="^registry_source_witness_runtime_invalid$"):
        await composition.require_source_witness_census_outsiders(
            connection, _runtime() if runtime is None else runtime, outsiders
        )
    connection.fetchval.assert_not_awaited()


async def test_unsafe_outsider_identifier_refuses_before_reading():
    connection = _connection(True)
    with pytest.raises(ValueError):
        await composition.require_source_witness_census_outsiders(connection, _runtime(), ('owner"; --',))
    connection.fetchval.assert_not_awaited()


async def test_sealing_requires_census_actor_and_proven_outsiders(monkeypatch):
    runtime = _runtime()
    proof = AsyncMock()
    monkeypatch.setattr(composition, "require_source_witness_census_outsiders", proof)
    connection = object()
    roles = ("census_reader", "output_owner")
    await custody._require_source_witness_census(connection, runtime, roles, {"current": "census_execution"})
    proof.assert_awaited_once_with(connection, runtime, ("output_owner",))
    proof.reset_mock()
    proof.side_effect = composition.RegistrySourceCompositionError("registry_source_witness_runtime_census_differs")
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody._require_source_witness_census(connection, runtime, roles, {"current": "census_execution"})
    proof.reset_mock(side_effect=True)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody._require_source_witness_census(connection, runtime, roles, {"current": "outside_operator"})
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody._require_source_witness_census(connection, object(), roles, {"current": "census_execution"})
    proof.assert_not_awaited()
