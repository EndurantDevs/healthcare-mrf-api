# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic sampling and refusal checks; these confer no native capture custody."""

import asyncio
import copy
import hashlib
import heapq
import json
import random
import sys
from contextlib import asynccontextmanager
from dataclasses import asdict
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from asyncpg import InsufficientPrivilegeError

from process import network_fhir_sampled_validation as sampled
from process import network_fhir_typed_capture as capture
from process.network_fhir_source_witness_contract import canonical, database_identity, digest, source_origin
from process.network_fhir_typed_contract import PAIR_COLUMNS, TypedFHIRGraphError, graph_document
from process.network_fhir_typed_source import _raw_projection
from tests.test_network_fhir_source_witness_contract import database_fields, origin_fields

_SEED = "1" * 64
_NETWORKS = ("network-one",)


@pytest.fixture
def sample_scope(monkeypatch):
    state = SimpleNamespace(active=False)

    @asynccontextmanager
    async def replay_scope(connection, replay):
        state.active = True
        try:
            yield
        finally:
            state.active = False

    monkeypatch.setattr(sampled, "_sample_replay_batch", replay_scope)
    return state


class _BottomPairs:
    """Frozen Python reference for native parity; production must require the native sampler."""

    def __init__(self, seed, network):
        self.seed, self.network = seed, network
        self.selected, self.heap = {}, []
        self.provenance_rows, self.overflow = 0, False

    def add(self, pair):
        """Count one provenance row while retaining only the distinct lowest ranked pairs."""
        self.provenance_rows += 1
        if pair in self.selected:
            return
        encoded = canonical([self.network, *map(str, pair)])
        rank = int.from_bytes(hashlib.sha256((sampled.CONTRACT + "\0" + self.seed + "\0" + encoded).encode()).digest())
        entry = (rank, encoded, pair)
        if len(self.selected) < sampled._LIMIT:
            self.selected[pair] = rank
            heapq.heappush_max(self.heap, entry)
        else:
            self.overflow = True
            if (rank, encoded) < self.heap[0][:2]:
                removed = heapq.heapreplace_max(self.heap, entry)[2]
                del self.selected[removed]
                self.selected[pair] = rank

    def receipt(self):
        """Describe exact small populations and the explicit large-population lower bound."""
        selected = len(self.selected)
        return {
            "unit": "distinct-provider-office-pair",
            "population": None if self.overflow else selected,
            "population_lower_bound": sampled._LIMIT + 1 if self.overflow else selected,
            "provenance_rows": self.provenance_rows,
            "selected": selected,
            "series_counts": sampled._series_counts(selected),
            "witness_rows": 0,
            "failures": 0,
        }


def _origin():
    fields = origin_fields()
    counts = (
        ("Location", 1),
        ("Organization", 1),
        ("OrganizationAffiliation", 0),
        ("Practitioner", 1),
        ("PractitionerRole", 2000),
    )
    fields.update(
        resource_counts=counts, descriptor_counts=counts, resource_hashes=tuple((kind, "a" * 64) for kind, _ in counts)
    )
    return source_origin(fields)


def _identity():
    return asdict(database_identity(database_fields()))


def _graph():
    counts_by_kind = dict.fromkeys(
        ("source_rows", "projected_rows", "pair_rows", "reference_rows", "provider_only_roles"), 0
    )
    return graph_document(_origin(), _identity(), "2026-01-01", counts_by_kind)


def _export(*, population=1):
    roles = [["PractitionerRole", f"role-{index}"] for index in range(min(population, 1000))]
    return {
        "contract": sampled.SOURCE_CONTRACT,
        "origin": _origin(),
        "database_identity": _identity(),
        "sampling": {
            "unit": "source-role-witness",
            "algorithm": sampled.ALGORITHM,
            "seed": _SEED,
            "series_count": 10,
            "items_per_series": 100,
            "networks": [
                {
                    "network_source_key": "network-one",
                    "population": population,
                    "series": [roles[offset : offset + 100] for offset in range(0, 1000, 100)],
                }
            ],
        },
        "resources": lambda _: None,
    }


def _pair(index):
    return ("network-one", "npi", f"{index:010}", UUID(int=index + 1), UUID(int=index + 2), "synthetic|street")


def _direction(*, source, population=1, failures=0):
    selected = min(population, 1000)
    direction_by_field = {
        "unit": "source-role-witness" if source else "distinct-provider-office-pair",
        "population": population,
        "selected": selected,
        "series_counts": sampled._series_counts(selected),
        "failures": failures,
    }
    direction_by_field.update(
        {"derived_pair_rows": selected}
        if source
        else {"population_lower_bound": population, "provenance_rows": selected, "witness_rows": selected}
    )
    return direction_by_field


def _receipt():
    receipt_by_field = {
        "contract": sampled.CONTRACT,
        "mode": "sampled",
        "seed": _SEED,
        "algorithm": sampled.ALGORITHM,
        "series_count": 10,
        "items_per_series": 100,
        "source_origin_sha256": digest(asdict(_origin())),
        "capture_id": str(UUID(int=1)),
        "observed_at": "2026-01-02T00:00:00.000000+00:00",
        "capture_sha256": "2" * 64,
        "database_identity": _identity(),
        "membership_as_of": "2026-01-01",
        "networks": [
            {
                "network_source_key": "network-one",
                "source_to_candidate": _direction(source=True),
                "candidate_to_source": _direction(source=False),
            }
        ],
        "limitations": list(sampled._LIMITATIONS),
    }
    receipt_by_field["receipt_sha256"] = digest(receipt_by_field)
    return receipt_by_field


def test_bottom_pairs_are_distinct_bounded_and_order_independent():
    rows = [_pair(index) for index in range(1400)] * 2
    first = _BottomPairs(_SEED, "network-one")
    for pair_by_field in rows:
        first.add(pair_by_field)
    random.Random(7).shuffle(rows)
    second = _BottomPairs(_SEED, "network-one")
    for pair_by_field in rows:
        second.add(pair_by_field)
    assert first.selected == second.selected and len(first.heap) == 1000
    assert first.receipt()["population"] is None
    assert first.receipt()["population_lower_bound"] == 1001
    assert first.receipt()["provenance_rows"] == 2800
    assert any(pair_by_field not in first.selected for pair_by_field in rows), (
        "A rare bad edge can remain outside the sample."
    )


def test_smaller_population_is_exhausted_without_duplicate_inflation():
    selection = _BottomPairs(_SEED, "network-one")
    for index in [1, 2, 1]:
        selection.add(_pair(index))
    assert selection.receipt() == {
        "unit": "distinct-provider-office-pair",
        "population": 2,
        "population_lower_bound": 2,
        "selected": 2,
        "series_counts": [2] + [0] * 9,
        "provenance_rows": 3,
        "witness_rows": 0,
        "failures": 0,
    }


def _native_sampler():
    native = pytest.importorskip("ptg2_address_canon")
    if not hasattr(native, "NetworkValidationSampler"):
        pytest.skip("native network validation sampler is not installed")
    return sampled._candidate_sampler(_NETWORKS, sampled.SemanticSamplingPolicy(_SEED))


@pytest.mark.parametrize("missing", ["class", "filter"])
def test_native_sampler_absence_refuses_without_python_fallback(monkeypatch, missing):
    native = SimpleNamespace()
    if missing == "filter":
        native.NetworkValidationSampler = Mock(return_value=SimpleNamespace(add_candidate_batch=Mock()))
    monkeypatch.setitem(sys.modules, "ptg2_address_canon", native)
    with pytest.raises(TypedFHIRGraphError, match="native_sampler_unavailable"):
        sampled._candidate_sampler(_NETWORKS, sampled.SemanticSamplingPolicy(_SEED))


@pytest.mark.asyncio
async def test_candidate_cursor_passes_canonical_uuid_strings_in_bounded_native_batches(monkeypatch):
    pair_by_field = dict(zip(sampled._PAIR_FIELDS, _pair(1), strict=True))
    cursor = SimpleNamespace(fetch=AsyncMock(side_effect=[[pair_by_field] * 1000, [pair_by_field], []]))
    connection = SimpleNamespace(cursor=AsyncMock(return_value=cursor))
    native = SimpleNamespace(add_candidate_batch=Mock())
    monkeypatch.setattr(sampled, "_candidate_sampler", Mock(return_value=native))
    result = await sampled._candidate_selection(
        connection, SimpleNamespace(relation=lambda _: "closed.pairs"), _NETWORKS, sampled.SemanticSamplingPolicy(_SEED)
    )
    assert result is native and cursor.fetch.await_count == 3
    assert [len(call.args[0]) for call in native.add_candidate_batch.call_args_list] == [1000, 1]
    assert native.add_candidate_batch.call_args.args[0][0] == tuple(map(str, _pair(1)))


def test_native_sampler_preserves_exact_reference_rank_reordering_and_duplicates():
    native, reordered = _native_sampler(), _native_sampler()
    rows = [(*_pair(index)[:-1], f'café-{index}|"quoted"|street') for index in range(1400)] * 2
    reference = _BottomPairs(_SEED, "network-one")
    for pair in rows:
        reference.add(pair)
    for offset in range(0, len(rows), 1000):
        native.add_candidate_batch(tuple(tuple(map(str, pair)) for pair in rows[offset : offset + 1000]))
    random.Random(7).shuffle(rows)
    for offset in range(0, len(rows), 1000):
        reordered.add_candidate_batch(tuple(tuple(map(str, pair)) for pair in rows[offset : offset + 1000]))
    expected_pairs = [
        list(map(str, pair)) for pair, _ in sorted(reference.selected.items(), key=lambda entry: entry[1])
    ]
    assert native.selected("network-one") == reordered.selected("network-one") == expected_pairs
    for field in ("population", "population_lower_bound", "provenance_rows", "selected", "series_counts"):
        assert native.coverage("network-one")[field] == reference.receipt()[field]


def test_native_filter_handles_unicode_and_scope_without_mutating_selection():
    native = _native_sampler()
    pair = (*_pair(1)[:-1], 'café|"quoted"|street')
    wire_values = tuple(map(str, pair))
    foreign = ("other-network", *wire_values[1:])
    native.add_candidate_batch((wire_values, wire_values, foreign))
    assert native.selected("network-one") == [list(wire_values)]
    before = native.coverage("network-one")
    assert native.candidate_selected_indices((foreign, wire_values, wire_values)) == [1, 2]
    assert native.coverage("network-one") == before


@pytest.mark.asyncio
async def test_candidate_second_pass_filters_native_batch_before_semantic_replay(monkeypatch):
    pair_by_field = dict(
        zip(
            PAIR_COLUMNS,
            (*_pair(1), "PractitionerRole", "role-one", "office-one", "role_network_direct_location", 1),
            strict=True,
        )
    )
    cursor = SimpleNamespace(fetch=AsyncMock(side_effect=[[pair_by_field, pair_by_field], []]))
    connection = SimpleNamespace(cursor=AsyncMock(return_value=cursor))
    native = SimpleNamespace(candidate_selected_indices=Mock(return_value=[1]))
    monkeypatch.setattr(sampled, "_candidate_coverage", Mock(return_value={"network-one": _direction(source=False)}))
    monkeypatch.setattr(sampled, "_candidate_batch", AsyncMock())
    await sampled._candidate_direction(
        {},
        connection,
        SimpleNamespace(relation=lambda _: "closed.pairs"),
        object(),
        _origin(),
        sampled.policy_date("2026-01-01"),
        native,
    )
    assert sampled._candidate_batch.await_args.args[3] == [pair_by_field]
    assert len(native.candidate_selected_indices.call_args.args[0]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("selected_counts", "batch_sizes"),
    [((1, 0, 2), [3]), ((500, 501), [1000, 1]), ((1000, 1000), [1000, 1000]), ((0, 0), [])],
    ids=("sparse", "final-partial", "dense", "empty"),
)
async def test_candidate_replay_batches_selected_rows_across_cursor_pages(monkeypatch, selected_counts, batch_sizes):
    pages = [
        [
            dict(
                zip(
                    PAIR_COLUMNS,
                    (
                        *_pair(page * 1000 + index),
                        "PractitionerRole",
                        f"role-{page}-{index}",
                        "office-one",
                        "direct",
                        1,
                    ),
                    strict=True,
                )
            )
            for index in range(1000)
        ]
        for page in range(len(selected_counts))
    ]
    # Duplicate provenance must survive a cursor-page boundary without changing its order.
    pages[-1][0] = pages[0][0]
    indices = [list(range(count)) for count in selected_counts]
    expected_rows = [page[index] for page, selected in zip(pages, indices, strict=True) for index in selected]
    cursor = SimpleNamespace(fetch=AsyncMock(side_effect=[*pages, []]))
    connection = SimpleNamespace(cursor=AsyncMock(return_value=cursor))
    native = SimpleNamespace(candidate_selected_indices=Mock(side_effect=indices))
    batches = []

    async def record_batch(_export, _connection, _replay, pair_rows, *_arguments):
        batches.append(list(pair_rows))

    monkeypatch.setattr(sampled, "_candidate_coverage", Mock(return_value={}))
    monkeypatch.setattr(sampled, "_candidate_batch", record_batch)
    await sampled._candidate_direction(
        {}, connection, SimpleNamespace(relation=lambda _: "closed.pairs"), object(), _origin(), None, native
    )
    assert [len(batch) for batch in batches] == batch_sizes
    assert [pair_by_field for batch in batches for pair_by_field in batch] == expected_rows
    assert native.candidate_selected_indices.call_count == len(pages)
    assert all(call.args == (1000,) for call in cursor.fetch.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_at", ("cursor", "replay"))
async def test_candidate_replay_cancellation_never_flushes_pending_again(monkeypatch, cancel_at):
    pair_by_field = dict(zip(sampled._PAIR_FIELDS, _pair(1), strict=True))
    pages = [[pair_by_field] * 1000, asyncio.CancelledError()] if cancel_at == "cursor" else [[pair_by_field] * 1000]
    cursor = SimpleNamespace(fetch=AsyncMock(side_effect=pages))
    connection = SimpleNamespace(cursor=AsyncMock(return_value=cursor))
    native = SimpleNamespace(
        candidate_selected_indices=Mock(return_value=[0] if cancel_at == "cursor" else list(range(1000)))
    )
    replay = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr(sampled, "_candidate_coverage", Mock(return_value={}))
    monkeypatch.setattr(sampled, "_candidate_batch", replay)
    with pytest.raises(asyncio.CancelledError):
        await sampled._candidate_direction(
            {}, connection, SimpleNamespace(relation=lambda _: "closed.pairs"), object(), _origin(), None, native
        )
    assert replay.await_count == (cancel_at == "replay")
    assert cursor.fetch.await_count == (2 if cancel_at == "cursor" else 1)


@pytest.mark.parametrize("population", [0, 1, 99, 100, 101, 1000, 1700])
def test_source_selection_declares_actual_role_coverage(population):
    networks = sampled._source_selection(
        _export(population=population), _origin(), _identity(), sampled.SemanticSamplingPolicy(_SEED), _NETWORKS
    )
    assert networks[0]["population"] == population
    assert sum(map(len, networks[0]["series"])) == min(population, 1000)


@pytest.mark.parametrize(
    "damage", ["contract", "seed", "unit", "origin", "database", "duplicate", "kind", "population", "series"]
)
def test_missing_or_unsupported_sampling_contract_refuses(damage):
    export = _export()
    match damage:
        case "contract":
            export["contract"] = "fhir-source-membership-witness-v1"
        case "seed" | "unit":
            export["sampling"][damage] = "other"
        case "origin":
            export["origin"] = source_origin(origin_fields())
        case "database":
            export["database_identity"]["database_oid"] += 1
        case "duplicate":
            export["sampling"]["networks"][0]["series"][0] *= 2
        case "kind":
            export["sampling"]["networks"][0]["series"][0][0][0] = "Organization"
        case "population":
            export["sampling"]["networks"][0]["population"] = True
        case "series":
            export["sampling"]["networks"][0]["series"].pop()
    with pytest.raises(ValueError):
        sampled._source_selection(export, _origin(), _identity(), sampled.SemanticSamplingPolicy(_SEED), _NETWORKS)


def test_raw_dependencies_preserve_scoped_refs_dates_npi_and_site_identity():
    pair_by_field = {
        "resource_type": "PractitionerRole",
        "resource_id": "role-one",
        "payload_hash": "a" * 64,
        "site_id": None,
        "payload_json": json.dumps(
            {
                "practitioner_ref": "Practitioner/provider-one",
                "npi": "1234567893",
                "network_refs": [
                    "Organization/network-one",
                    "https://foreign.example.test/fhir/Organization/network-one",
                ],
                "location_refs": ["Location/office-one", "Location/office-one?version=1"],
                "period_end": "2025-12-31",
                "accepting_patients": [{"from_network_ref": "Organization/indirect"}],
            }
        ),
    }
    cells, checks = _raw_projection(pair_by_field, _origin().source_uri, sampled.policy_date("2026-01-01"))
    assert checks[3:7] == ("provider-one", ["network-one"], ["office-one"], False)
    assert checks[-1] is True and cells[-1] is None


@pytest.mark.asyncio
async def test_subset_reader_rejects_unrequested_payload_and_preserves_true_eof(monkeypatch):
    closed_events = []

    async def resources(keys):
        try:
            yield (
                {
                    "dataset_id": _origin().dataset_id,
                    "resource_type": "PractitionerRole",
                    "resource_id": "other-role",
                    "payload_hash": "a" * 64,
                    "payload_json": "{}",
                    "acquired_resource_sha256": None,
                },
            )
        finally:
            closed_events.append(True)

    monkeypatch.setattr(sampled, "_check_page", AsyncMock())
    with pytest.raises(TypedFHIRGraphError, match="sample_resource_scope"):
        await sampled._fetch_resources(
            {"resources": resources},
            object(),
            object(),
            (("PractitionerRole", "role-one"),),
            _origin(),
            sampled.policy_date("2026-01-01"),
        )
    assert closed_events == [True]
    sampled._check_page.assert_not_awaited()


@pytest.mark.asyncio
async def test_absent_dependency_is_accepted_only_after_subset_eof(monkeypatch):
    finished_events = []

    async def resources(keys):
        if False:
            yield
        finished_events.append(True)

    monkeypatch.setattr(sampled, "_check_page", AsyncMock())
    assert (
        await sampled._fetch_resources(
            {"resources": resources},
            object(),
            object(),
            (("Location", "missing"),),
            _origin(),
            sampled.policy_date("2026-01-01"),
        )
        == set()
    )
    assert finished_events == [True]


@pytest.mark.asyncio
async def test_missing_sampled_role_refuses_before_dependency_replay(monkeypatch):
    replay = SimpleNamespace(verify=AsyncMock(), names={"cells": "cells"}, relation=lambda _: "pg_temp.cells")
    monkeypatch.setattr(sampled, "_fetch_resources", AsyncMock(return_value=set()))
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(TypedFHIRGraphError, match="sample_role_missing"):
        await sampled._replay_roles(
            {},
            connection,
            replay,
            [("PractitionerRole", "missing")],
            _origin(),
            sampled.policy_date("2026-01-01"),
            required=True,
        )
    connection.execute.assert_awaited_once_with("DELETE FROM pg_temp.cells")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bound_fanout", "role_count", "fits"), [(True, 2, False), (True, 2, True), (True, 1, False), (False, 2, False)]
)
async def test_candidate_fanout_budget_follows_complete_role_stream(monkeypatch, bound_fanout, role_count, fits):
    roles = [("PractitionerRole", f"role-{index}") for index in range(role_count)]
    events = []

    async def resources(keys):
        yield tuple(
            {
                "dataset_id": _origin().dataset_id,
                "resource_type": kind,
                "resource_id": identity,
                "payload_hash": "a" * 64,
                "payload_json": "{}",
                "acquired_resource_sha256": None,
            }
            for kind, identity in keys
        )
        events.append("source-eof")

    async def budget(query, limit):
        assert events == ["source-eof"] and limit == 100_000
        assert "cardinality(network_ids)::bigint * cardinality(location_ids)" in query
        assert query.endswith("FROM pg_temp.checks")
        events.append("budget")
        return fits

    async def dependencies(*_args, **_kwargs):
        events.append("dependencies")
        if False:
            yield

    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(side_effect=budget), cursor=dependencies)
    replay = SimpleNamespace(verify=AsyncMock(), names={"checks": "checks"}, relation=lambda kind: "pg_temp." + kind)
    monkeypatch.setattr(sampled, "_check_page", AsyncMock())
    monkeypatch.setattr(sampled, "_canonicalize", AsyncMock())
    monkeypatch.setattr(sampled, "_pairs", AsyncMock())
    replay_outcome = await sampled._replay_roles(
        {"resources": resources}, connection, replay, roles, _origin(), None, required=False, bound_fanout=bound_fanout
    )
    is_refused = bound_fanout and role_count > 1 and not fits
    assert (replay_outcome is False) == is_refused
    assert ("dependencies" in events) == (not is_refused)
    assert connection.fetchval.await_count == (bound_fanout and role_count > 1)
    assert sampled._pairs.await_count == (not is_refused)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "missing", "cancel", "source-error"))
async def test_candidate_fanout_retry_rolls_back_before_ordered_halves(monkeypatch, fault):
    pair_rows = [
        dict(
            zip(PAIR_COLUMNS, (*_pair(index), "PractitionerRole", f"role-{index}", "office", "direct", 1), strict=True)
        )
        for index in range(3)
    ]
    pair_rows.insert(2, pair_rows[1])
    scope = SimpleNamespace(is_active=False, opened=0, closed=0, completed_roles=[], selected_roles=[])

    @asynccontextmanager
    async def rollback_scope(*_args):
        assert not scope.is_active
        scope.is_active = True
        scope.opened += 1
        try:
            yield
        finally:
            scope.is_active = False
            scope.closed += 1

    async def replay_roles(_export, _connection, _replay, roles, *_args, **kwargs):
        assert scope.is_active and kwargs == {"required": False, "bound_fanout": True}
        if len(roles) > 1:
            return False
        scope.selected_roles = roles
        if roles[0][1] == "role-1":
            if fault == "cancel":
                raise asyncio.CancelledError()
            if fault == "source-error":
                raise InsufficientPrivilegeError("synthetic Source refusal")
        scope.completed_roles.extend(roles)
        return None

    async def actual_pairs(*_args):
        assert scope.is_active
        return [
            pair_by_field
            for pair_by_field in pair_rows
            if (pair_by_field["resource_type"], pair_by_field["resource_id"]) in scope.selected_roles
            and not (fault == "missing" and pair_by_field["resource_id"] == "role-1")
        ]

    monkeypatch.setattr(sampled, "_sample_replay_batch", rollback_scope)
    monkeypatch.setattr(sampled, "_replay_roles", replay_roles)
    directions_by_network = {"network-one": {"witness_rows": 0, "failures": 0}}
    connection = SimpleNamespace(fetch=actual_pairs)
    replay = SimpleNamespace(relation=lambda _: "pg_temp.pairs")
    operation = sampled._candidate_batch({}, connection, replay, pair_rows, _origin(), None, directions_by_network)
    if fault in {"cancel", "source-error"}:
        with pytest.raises(asyncio.CancelledError if fault == "cancel" else InsufficientPrivilegeError):
            await operation
        assert directions_by_network["network-one"]["witness_rows"] == 1
    else:
        await operation
        assert scope.completed_roles == [(pair["resource_type"], pair["resource_id"]) for pair in pair_rows]
        assert directions_by_network["network-one"] == {"witness_rows": 4, "failures": 2 if fault == "missing" else 0}
    assert not scope.is_active and scope.opened == scope.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [None, -1, 0, 256 * 1024**2 + 1])
async def test_sampler_refuses_unbounded_spill_without_changing_privileges(monkeypatch, limit):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=limit))
    monkeypatch.setattr(sampled, "_set_content_settings", AsyncMock())
    with pytest.raises(TypedFHIRGraphError, match="sample_spill_limit"):
        await sampled._sampling_settings(connection)
    sampled._set_content_settings.assert_not_awaited()


@pytest.mark.asyncio
async def test_sampler_preserves_standing_spill_cap(monkeypatch):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=256 * 1024**2))
    monkeypatch.setattr(sampled, "_set_content_settings", AsyncMock())
    await sampled._sampling_settings(connection)
    sampled._set_content_settings.assert_awaited_once_with(connection, ["work_mem"], ["16MB"])


@pytest.mark.asyncio
async def test_source_omission_check_uses_independent_roles_and_indexed_pair_keys(monkeypatch, sample_scope):
    async def counts(*arguments):
        assert sample_scope.active
        return {"total": 2, "failures": 1}

    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=counts))
    replay, sealed = (
        SimpleNamespace(relation=lambda _: "pg_temp.raw_pairs"),
        SimpleNamespace(relation=lambda _: "closed.pairs"),
    )
    monkeypatch.setattr(sampled, "_replay_roles", AsyncMock())
    network = _export()["sampling"]["networks"][0]
    result = await sampled._source_direction(
        {}, connection, sealed, replay, _origin(), sampled.policy_date("2026-01-01"), network
    )
    assert result["derived_pair_rows"] == 2 and result["failures"] == 1
    assert sampled._replay_roles.await_args.args[3] == [("PractitionerRole", "role-0")]
    query = connection.fetchrow.await_args.args[0]
    for field in PAIR_COLUMNS:
        comparison = "=" if field in sampled._PAIR_KEYS else " IS NOT DISTINCT FROM "
        assert "actual." + field + comparison + "expected." + field in query


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "invented", "multiplicity", "valid"])
async def test_candidate_selected_provenance_compares_every_strict_pair_field(monkeypatch, damage, sample_scope):
    pair_by_field = dict(
        zip(
            PAIR_COLUMNS,
            (*_pair(1), "PractitionerRole", "role-one", "office-one", "role_network_direct_location", 1),
            strict=True,
        )
    )
    actual_by_field = dict(pair_by_field)
    if damage == "invented":
        actual_by_field["provider_id"] = "1000000004"
    elif damage == "multiplicity":
        actual_by_field["reference_rows"] = 2

    async def actual_pairs(*arguments):
        assert sample_scope.active
        return [] if damage == "missing" else [actual_by_field]

    connection = SimpleNamespace(fetch=AsyncMock(side_effect=actual_pairs))
    monkeypatch.setattr(sampled, "_replay_roles", AsyncMock())
    directions_by_network = {"network-one": _direction(source=False)}
    directions_by_network["network-one"]["witness_rows"] = 0
    await sampled._candidate_batch(
        {},
        connection,
        SimpleNamespace(relation=lambda _: "pg_temp.synthetic_pairs"),
        [pair_by_field],
        _origin(),
        sampled.policy_date("2026-01-01"),
        directions_by_network,
    )
    assert directions_by_network["network-one"]["witness_rows"] == 1
    assert directions_by_network["network-one"]["failures"] == (damage != "valid")


@pytest.mark.asyncio
async def test_cancelled_start_still_closes_savepoint(monkeypatch):
    parent, commands = asyncio.current_task(), []

    async def execute(query):
        commands.append(query)
        if query.startswith("SAVEPOINT "):
            parent.cancel()
            await asyncio.sleep(0)

    monkeypatch.setattr(sampled, "_sample_replay_indexes", AsyncMock(return_value={}))
    connection = SimpleNamespace(execute=execute, terminate=Mock())
    with pytest.raises(asyncio.CancelledError):
        async with sampled._sample_replay_batch(connection, SimpleNamespace(oids={})):
            pytest.fail("cancelled batch started replay")
    while parent.cancelling():
        parent.uncancel()
    assert len(commands) == 2
    savepoint = commands[0].removeprefix("SAVEPOINT ")
    assert commands[1] == "ROLLBACK TO SAVEPOINT " + savepoint + "; RELEASE SAVEPOINT " + savepoint
    connection.terminate.assert_not_called()


@pytest.mark.asyncio
async def test_sample_cleanup_failure_closes_connection(monkeypatch):
    monkeypatch.setattr(sampled, "_sample_replay_indexes", AsyncMock(return_value={}))
    connection = SimpleNamespace(
        execute=AsyncMock(side_effect=[None, RuntimeError("cleanup failed")]), terminate=Mock()
    )
    with pytest.raises(RuntimeError, match="cleanup failed"):
        async with sampled._sample_replay_batch(connection, SimpleNamespace(oids={})):
            assert connection.execute.await_count == 1
    connection.terminate.assert_called_once_with()


def test_receipt_is_closed_bound_and_contains_no_provider_payload():
    receipt = _receipt()
    assert sampled.validate_sampled_semantic_receipt(receipt) == receipt
    assert "provider_id" not in json.dumps(receipt) and "payload_json" not in json.dumps(receipt)
    for key, value in [("mode", "exhaustive"), ("extra", "field"), ("capture_sha256", "3" * 64)]:
        changed = copy.deepcopy(receipt)
        changed[key] = value
        with pytest.raises(TypedFHIRGraphError, match="sample_receipt"):
            sampled.validate_sampled_semantic_receipt(changed)


@pytest.mark.asyncio
async def test_read_cannot_be_forged_mutated_or_promoted_to_full_authority():
    receipt = _receipt()
    graph = _graph()
    stage = SimpleNamespace(
        document=graph, verify=AsyncMock(), checked=False, relation=lambda kind: "synthetic." + kind
    )
    sealed = capture.SealedTypedGraphRead(
        stage,
        {"capture_sha256": receipt["capture_sha256"]},
        capture.TypedGraphAnchor(receipt["capture_id"], receipt["observed_at"]),
        capture._SEALED_READ,
    )
    read = sampled.SampledTypedGraphRead(sealed, json.dumps(receipt), sampled._READ_BRAND)
    read.receipt["seed"] = "changed"
    await read.verify(object())
    assert read.receipt == receipt and stage.checked is False and not hasattr(read, "checked")
    assert read.relation("pairs") == "synthetic.pairs"
    with pytest.raises(TypedFHIRGraphError, match="sample_read"):
        await sampled.SampledTypedGraphRead(sealed, json.dumps(receipt), object()).verify(object())


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["source", "candidate", "success"])
async def test_verification_refuses_sample_failure_and_attaches_payload_free_receipt(monkeypatch, direction):
    receipt = _receipt()
    graph = _graph()
    document_by_field = {
        "capture_id": receipt["capture_id"],
        "observed_at": receipt["observed_at"],
        "capture_sha256": receipt["capture_sha256"],
        "graph": graph,
    }
    stage = SimpleNamespace(document=graph, verify=AsyncMock(), checked=False)
    sealed = capture.SealedTypedGraphRead(
        stage,
        document_by_field,
        capture.TypedGraphAnchor(receipt["capture_id"], receipt["observed_at"]),
        capture._SEALED_READ,
    )

    @asynccontextmanager
    async def context(*args, **kwargs):
        yield _export()

    monkeypatch.setitem(
        sys.modules, "directory_membership_runtime", SimpleNamespace(open_membership_source_sample=context)
    )
    monkeypatch.setattr(sampled, "_original_source_factory", Mock())
    monkeypatch.setattr(sampled, "load_typed_graph_capture", AsyncMock(return_value=sealed))
    monkeypatch.setattr(sampled, "_borrowed_epoch_settings", context)
    monkeypatch.setattr(sampled, "_replay_workspace", context)
    monkeypatch.setattr(sampled, "_sampling_settings", AsyncMock())
    monkeypatch.setattr(sampled, "_candidate_selection", AsyncMock(return_value={}))
    monkeypatch.setattr(
        sampled,
        "_source_direction",
        AsyncMock(return_value=_direction(source=True, failures=int(direction == "source"))),
    )
    monkeypatch.setattr(
        sampled,
        "_candidate_direction",
        AsyncMock(return_value={"network-one": _direction(source=False, failures=int(direction == "candidate"))}),
    )
    arguments_by_name = dict(
        scope=sampled.SampledCaptureScope(
            SimpleNamespace(schema_name=_origin().schema_name), _origin(), sealed.anchor, _NETWORKS
        ),
        policy=sampled.SemanticSamplingPolicy(_SEED),
    )
    if direction == "success":
        read = await sampled.verify_sampled_typed_capture(
            partial(str, "runtime"), object(), document_by_field, **arguments_by_name
        )
        assert type(read) is sampled.SampledTypedGraphRead and stage.checked is False
    else:
        with pytest.raises(TypedFHIRGraphError, match="sample_failed") as failure:
            await sampled.verify_sampled_typed_capture(
                partial(str, "runtime"), object(), document_by_field, **arguments_by_name
            )
        assert sampled.validate_sampled_semantic_receipt(failure.value.receipt)
        assert "provider_id" not in str(failure.value) + json.dumps(failure.value.receipt)
    sampled._original_source_factory.assert_called_once()
