# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Detached handoff identities and recovery refuse changed or ambiguous evidence."""

import copy
import json
from dataclasses import asdict
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import migration_rooted_publication_freeze as freeze
from process import provider_directory_rooted_graph_publication_candidate as candidate
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
)
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.provider_directory_rooted_graph_publication_test_support import twin_admission
from tests.test_provider_directory_rooted_graph_publication_freeze import _current_bodies


@pytest.fixture
def rooted_handoff():
    binding = FHIRSourceStorageBinding("catalog", "candidate", 1, 2, 3, 4, 5, "a" * 64)
    issued = SimpleNamespace(storage_binding=binding, producer_identity=("b" * 40, "c" * 40))
    storage_by_field = {**asdict(binding), "producer": list(issued.producer_identity)}
    handoff_by_field = {
        "contract": freeze.CONTRACT,
        "phase": "ready_for_freeze",
        "operation": {},
        "preparation_id": "12345678-1234-5678-1234-567812345678",
        "storage": storage_by_field,
        "identity": {
            "dataset_id": "pdrgpd_" + "d" * 48,
            "source_id": "example-source",
            "metadata": {
                "source_ids": ["example-source"],
                "source_run_id": "run_" + "e" * 32,
                "source_attempt_id": "attempt-one",
                "query_filter_recipe_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
            },
        },
        "admission": json.loads(candidate.candidate_json(asdict(twin_admission()))),
        "tables": {
            root: {"oid": 100 + index, "name": "pdgs_" + "f" * 32 + "_" + chr(97 + index), "owner": 4}
            for index, root in enumerate(candidate.ROOTS)
        },
        "catalog": [],
        "dataset_hash": "1" * 64,
        "counts": {"Practitioner": 1},
        "snapshot": {},
        "lineage": {},
        "predecessor_proof": {},
    }
    return SimpleNamespace(
        document=handoff_by_field,
        storage=storage_by_field,
        runtime=SimpleNamespace(manifest=issued, candidate_schema="candidate"),
    )


def test_handoff_has_canonical_dates_and_detached_json(rooted_handoff):
    document = rooted_handoff.document
    assert candidate.validate_rooted_handoff_document(document, rooted_handoff.storage) is document
    encoded = candidate.candidate_json({"date": date(2026, 1, 1), "value": document})
    decoded = json.loads(encoded)
    assert decoded["date"] == "2026-01-01" and decoded["value"] == document
    decoded["value"]["counts"]["Practitioner"] = 2
    assert document["counts"] == {"Practitioner": 1}
    with pytest.raises(ValueError):
        candidate.candidate_json({"number": float("nan")})
    with pytest.raises(TypeError, match="rooted_candidate_value_invalid"):
        candidate.candidate_json({"value": object()})


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("extra", "shape"),
        ("phase", "handoff"),
        ("storage", "storage"),
        ("missing-table", "inventory"),
        ("duplicate-oid", "inventory"),
        ("duplicate-name", "inventory"),
        ("unsafe-name", "inventory"),
        ("owner", "inventory"),
        ("identity", "identity"),
        ("hash", "hash"),
        ("source", "source_attempt"),
        ("run", "source_attempt"),
        ("attempt", "source_attempt"),
        ("negative-count", "proof"),
        ("boolean-count", "proof"),
        ("snapshot", "proof"),
        ("large", "too_large"),
    ],
)
def test_handoff_rejects_changed_identity_inventory_and_unbounded_evidence(rooted_handoff, mutation, code):
    document = copy.deepcopy(rooted_handoff.document)
    first, second = list(document["tables"].values())[:2]
    mutations_by_name = {
        "extra": lambda: document.update(extra=True),
        "phase": lambda: document.update(phase="published"),
        "storage": lambda: document.update(storage={}),
        "missing-table": lambda: document["tables"].pop(candidate.HEADER),
        "duplicate-oid": lambda: second.update(oid=first["oid"]),
        "duplicate-name": lambda: second.update(name=first["name"]),
        "unsafe-name": lambda: first.update(name="unrelated"),
        "owner": lambda: first.update(owner=9),
        "identity": lambda: document["identity"].update(dataset_id="unrelated"),
        "hash": lambda: document.update(dataset_hash="invalid"),
        "source": lambda: document["identity"]["metadata"].update(source_ids=["other-source"]),
        "run": lambda: document["identity"]["metadata"].update(source_run_id="unrelated"),
        "attempt": lambda: document["identity"]["metadata"].update(source_attempt_id=""),
        "negative-count": lambda: document["counts"].update(Practitioner=-1),
        "boolean-count": lambda: document["counts"].update(Practitioner=True),
        "snapshot": lambda: document.update(snapshot=[]),
        "large": lambda: document["snapshot"].update(padding="x" * 262144),
    }
    mutations_by_name[mutation]()
    with pytest.raises(GenericCandidateError, match=code):
        candidate.validate_rooted_handoff_document(document, rooted_handoff.storage)


@pytest.mark.asyncio
async def test_retained_recovery_is_read_only_and_returns_detached_evidence(rooted_handoff):
    document = rooted_handoff.document
    encoded = candidate.candidate_json(document)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=encoded), fetch=AsyncMock())
    handoff = await candidate.retained_rooted_handoff(
        rooted_handoff.runtime, connection, document["identity"]["dataset_id"]
    )
    assert handoff.document == document
    connection.fetch.assert_not_awaited()
    connection.fetchval.return_value = None
    assert await candidate.retained_rooted_handoff(rooted_handoff.runtime, connection, "missing") is None
    connection.fetch.return_value = [{"candidate_storage_json": document}]
    assert (
        await candidate.retained_rooted_handoff(
            rooted_handoff.runtime, connection, publication_acquisition_id="operation"
        )
    ).document == document
    assert "LIMIT 2" in connection.fetch.call_args.args[0]
    connection.fetch.return_value = []
    assert (
        await candidate.retained_rooted_handoff(
            rooted_handoff.runtime, connection, publication_acquisition_id="missing"
        )
        is None
    )
    connection.fetch.return_value = [{"candidate_storage_json": encoded}] * 2
    with pytest.raises(GenericCandidateError, match="ambiguous"):
        await candidate.retained_rooted_handoff(
            rooted_handoff.runtime, connection, publication_acquisition_id="operation"
        )


@pytest.mark.asyncio
async def test_lineage_follows_exact_ancestors_and_refuses_cycles_or_missing_evidence():
    cohort_by_field = {"cohort_id": "example-cohort", "cohort": {"dataset_id": "official"}}
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"root_dataset_id": "legacy", "root_dataset_variant": "uhc_flex_practitioner"}),
        fetchval=AsyncMock(return_value=json.dumps(cohort_by_field)),
    )
    assert await candidate._lineage_snapshot(connection, "catalog", "rooted", "rooted_combined") == cohort_by_field
    connection.fetchrow.return_value = {"root_dataset_id": "rooted", "root_dataset_variant": "rooted_combined"}
    with pytest.raises(GenericCandidateError, match="cycle"):
        await candidate._lineage_snapshot(connection, "catalog", "rooted", "rooted_combined")
    connection.fetchval.return_value = None
    with pytest.raises(GenericCandidateError, match="lineage_changed"):
        await candidate._lineage_snapshot(connection, "catalog", "legacy", "uhc_flex_practitioner")
    connection.fetchrow.return_value = None
    with pytest.raises(GenericCandidateError, match="lineage_changed"):
        await candidate._lineage_snapshot(connection, "catalog", "missing", "rooted_combined")
    with pytest.raises(GenericCandidateError, match="lineage_changed"):
        await candidate._lineage_snapshot(connection, "catalog", "rooted", "unknown")


@pytest.mark.asyncio
async def test_lineage_depth_is_bounded():
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            side_effect=[
                {"root_dataset_id": "rooted-" + str(index), "root_dataset_variant": "rooted_combined"}
                for index in range(1024)
            ]
        )
    )
    with pytest.raises(RuntimeError, match="too_deep"):
        await candidate._lineage_snapshot(connection, "catalog", "initial", "rooted_combined")
    assert connection.fetchrow.await_count == 1024


def test_frozen_query_preserves_reviewed_predicates_and_pinned_root_reads(rooted_handoff):
    original = _current_bodies()[freeze.ROOTED]
    successor = freeze.wrapped_body(freeze.ROOTED, "snapshot_schema", original)
    query = candidate._frozen_validation_query(original, "snapshot_schema", rooted_handoff.document)
    assert candidate._frozen_validation_query(successor, "snapshot_schema", rooted_handoff.document) == query
    assert "UNION ALL" in query and "WHERE dataset_id=$2" in query
    assert "candidate_dataset_id" not in query
    for body in ("SELECT true;", original + "\n-- changed validator\n"):
        with pytest.raises(GenericCandidateError, match="validator_changed"):
            candidate._frozen_validation_query(body, "snapshot_schema", rooted_handoff.document)
