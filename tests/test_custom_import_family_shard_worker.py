# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual routed families, complete audit, decoded blocks and spawned CPU work."""

import json
import multiprocessing
import pickle
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from decimal import Decimal
from pathlib import Path

import pytest

from process.custom_import import family_shard_worker as worker
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import CandidateRejected, RootFamily
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_directory import RevisionMembership
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_storage_codec import StoredFamilyManifest, verify_stored_family
from process.custom_import.family_shard_facts import PreparedSourceFamily, SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_input import ShardInputLimits, cleanup_spool
from process.custom_import.family_shard_input_records import decode_record, encode_record
from process.custom_import.family_shard_route import prepare_routed_families, route_capture
from process.custom_import.runner_codec import digest_text, new_family_hash
from tests.test_custom_import_family_shard_input import _capture_input, _child, _partition, _root
from tests.test_custom_import_snowflake_bundle import _definition


def _limits():
    facts = SourceFactLimits(64 * 1024**2, 1024**2, 4096)
    return worker.SourceWorkerLimits(
        facts, FamilyBlockLimits(64 * 1024**2, 1024**2, 4096, 65536), facts, 10000, 8 * 1024**2
    )


def _projected_definition(*, collapse=False):
    document = json.loads(_definition().canonical)
    for fields in (document["schema"]["root"]["fields"], document["schema"]["children"][0]["fields"]):
        for field in fields:
            field["projection_slot"] = field["slot"]
    document["query"] = {
        "root_fields": ["npi", "score", "enabled"],
        "child": {"collection": "details", "fields": ["detail_id", "amount"]},
        "order": [],
    }
    if collapse:
        document["streams"][1]["duplicate_policy"] = "collapse_identical"
    return CustomImportDefinition.from_mapping(document)


def _plan(shard, definition, root):
    records, families, key_ids_by_key, expected = {}, {}, {}, []
    with prepare_routed_families(
        shard, definition, root=root, register_cleanup=lambda path: None, limits=_limits().facts
    ) as prepared:
        for index, family in enumerate(prepared, 1):
            if family.typed_key is not None:
                families[family.typed_key] = (7000 + index * 17, 8000 + index * 13)
            for record, facts, decision in family.rows():
                coordinate = record.stream_slot, record.source_ordinal
                if family.typed_key is not None:
                    records[coordinate] = (
                        record.stream_slot * 1000 + record.source_ordinal * 19 + 3,
                        9000 + record.stream_slot * 10 + record.part_ordinal,
                        6000 + index * 11,
                    )
                if record.child_key is not None:
                    key_ids_by_key[(digest_text("child-key", record.child_key), record.child_key)] = (1, 1)
                expected.append((encode_record(record), asdict(facts), asdict(decision)))
    key_ids_by_key = {key: (index, 1) for index, key in enumerate(sorted(key_ids_by_key), 1)}
    return worker.SourceShardPlan(
        shard, definition.canonical, {"details": 7}, 101, 103, 19, records, families, key_ids_by_key, True
    ), expected


@asynccontextmanager
async def _routed(tmp_path, roots, children, *, definition=None, shard_count=4):
    definition, source, parts = _capture_input(
        {"root_source": [roots], "detail_source": [children]}, definition=definition or _projected_definition()
    )
    partition = await _partition(
        tmp_path, definition, source, parts, limits=ShardInputLimits(32 * 1024**2, shard_count, 1024)
    )
    try:
        with route_capture(
            partition, definition, root=tmp_path / "route", register_cleanup=lambda path: None, limits=_limits().facts
        ) as shards:
            plans = [_plan(shard, definition, tmp_path / f"inspect-{shard.shard_id}") for shard in shards]
            yield definition, plans
    finally:
        cleanup_spool(partition)


def _rows(worker_output):
    rows_by_role = {role: [] for role in worker_output.receipt["role_counts"]}
    for batch in worker_output.batches(maximum_rows=3, maximum_bytes=2 * 1024**2):
        for role, rows in batch.items():
            rows_by_role[role].extend(rows)
    return rows_by_role


def _decoded(rows, layout):
    for family in rows["family"]:
        family_id = family[0]
        manifest = StoredFamilyManifest(layout.definition.digest, *family[4:12])
        fragments = (
            ((audit_row[1], audit_row[2], *audit_row[4:]), audit_row[3])
            for audit_row in rows["fragment"]
            if audit_row[0] == family_id
        )
        members = (
            RevisionMembership(*audit_row[1:]) for audit_row in rows["revision_run"] if audit_row[0] == family_id
        )
        with verify_stored_family(
            layout.definition, layout.collection_slots, fragments, members, manifest, limits=_limits().output
        ) as decoded_families:
            yield family, tuple(decoded_families.records()), tuple(decoded_families.records("details"))


@pytest.mark.asyncio
async def test_actual_facts_blocks_scalars_hashes_and_full_audit_match(tmp_path):
    source_child_rows = [_child(key="b", amount="1.2300"), _child(key="a", amount="2.00")]
    async with _routed(tmp_path, [_root()], source_child_rows) as (definition, plans):
        for plan, expected in plans:
            root, registered_roots = tmp_path / f"worker-{plan.shard.shard_id}", []
            with worker.build_source_shard(
                plan, root=root, register_cleanup=registered_roots.append, limits=_limits()
            ) as worker_output:
                assert registered_roots == [root] and worker_output.status == "source_eof"
                audit_rows = list(worker_output.audit_rows())
                assert audit_rows[-1] == ("eof", worker_output.counts)
                assert [
                    (audit_row[2], audit_row[3], audit_row[4])
                    for audit_row in audit_rows
                    if audit_row[0] == "occurrence"
                ] == expected
                assert worker_output.counts["records"] == plan.shard.records
                decoded_families = list(
                    _decoded(_rows(worker_output), FamilyBlockLayout(definition, plan.collection_slots))
                )
                for family, roots, stored_children in decoded_families:
                    assert len(roots) == 1 and len(stored_children) == 2
                    assert roots[0].scalars["score"].integer_value == 7
                    assert roots[0].scalars["enabled"].boolean_value is True
                    assert {child.scalars["amount"].decimal_value for child in stored_children} == {
                        Decimal("1.23"),
                        Decimal("2"),
                    }
                    expected_hash = new_family_hash(
                        definition, RootFamily(("1003000126",), _root(), {"details": tuple(source_child_rows)})
                    )
                    assert family[4] == expected_hash and family[6] is False
                    assert {child.revision_id for child in stored_children} == {
                        1000 * 2 + ordinal * 19 + 3 for ordinal in range(2)
                    }
            assert not root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("collapse", [False, True])
async def test_duplicate_policy_keeps_all_source_evidence_and_last_child_identity(tmp_path, collapse):
    child = _child(amount="1.00")
    async with _routed(tmp_path, [_root()], [child, child], definition=_projected_definition(collapse=collapse)) as (
        definition,
        plans,
    ):
        for plan, expected in plans:
            with worker.run_source_shard(plan, tmp_path / f"w{plan.shard.shard_id}", _limits()) as worker_output:
                audit_rows = list(worker_output.audit_rows())
                assert [
                    (audit_row[2], audit_row[3], audit_row[4])
                    for audit_row in audit_rows
                    if audit_row[0] == "occurrence"
                ] == expected
                if not plan.shard.records:
                    continue
                assert worker_output.status == "source_eof" and worker_output.counts["candidate_errors"] == 0
                if collapse:
                    family, _roots, children = next(
                        _decoded(_rows(worker_output), FamilyBlockLayout(definition, plan.collection_slots))
                    )
                    assert len(children) == 1 and children[0].revision_id == 2022 and children[0].source_ordinal == 1
                else:
                    assert worker_output.counts["absent"] == 1 and _rows(worker_output)["family"] == []
                    header = next(audit_row[1] for audit_row in audit_rows if audit_row[0] == "family")
                    assert header["root_resolution"]["event"]["code"] == "duplicate_child_key"


@pytest.mark.asyncio
async def test_candidate_errors_complete_audit_but_refuse_every_copy_batch(tmp_path):
    refused = 0
    async with _routed(tmp_path, [_root()], [_child("1234567893")]) as (_definition_value, plans):
        for plan, expected in plans:
            with worker.run_source_shard(plan, tmp_path / f"w{plan.shard.shard_id}", _limits()) as worker_output:
                audit_rows = list(worker_output.audit_rows())
                assert [
                    (audit_row[2], audit_row[3], audit_row[4])
                    for audit_row in audit_rows
                    if audit_row[0] == "occurrence"
                ] == expected
                if worker_output.counts["error"]:
                    refused += 1
                    assert worker_output.status == "refused" and audit_rows[-1][0] == "eof"
                    _assert_refused(worker_output)
    assert refused > 0


def _assert_refused(worker_output):
    with pytest.raises(CandidateRejected):
        list(worker_output.batches(maximum_rows=3, maximum_bytes=4096))


@pytest.mark.asyncio
async def test_structural_errors_finish_the_audit_and_refuse_source_output(tmp_path, monkeypatch):
    async with _routed(tmp_path, [_root()], [_child()], shard_count=1) as (_definition_value, plans):
        plan = plans[0][0]
        prepare = PreparedSourceFamily._prepare

        def _fatal_family(family):
            prepare(family)
            family.fatal_problem = "synthetic independent collision witness"
            family.fatal_order = (1, 0)

        monkeypatch.setattr(PreparedSourceFamily, "_prepare", _fatal_family)
        with worker.run_source_shard(plan, tmp_path / "worker", _limits()) as worker_output:
            audit_rows = list(worker_output.audit_rows())
            assert worker_output.status == "refused" and worker_output.counts["fatal_families"] == 1
            assert audit_rows[-1][0] == "eof" and worker_output.counts["records"] == plan.shard.records
            _assert_refused(worker_output)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["allocation", "root", "query", "census", "audit_budget", "output_budget", "metadata"]
)
async def test_late_failure_cleans_only_exact_worker_root(tmp_path, failure):
    async with _routed(tmp_path, [_root()], [_child()]) as (_definition_value, plans):
        plan = next(plan for plan, _expected in plans if plan.shard.records)
        bounds, root = _limits(), tmp_path / "worker"
        plan, bounds = _failed_plan(plan, bounds, failure)
        sibling = tmp_path / "preserved"
        sibling.write_text("preserve")
        with pytest.raises((ValueError, SourceFactBudgetExceeded, FamilyBlockResourceLimit)):
            worker.run_source_shard(plan, root, bounds)
        assert not root.exists() and sibling.read_text() == "preserve"


def _failed_plan(plan, bounds, failure):
    if failure == "allocation":
        return replace(
            plan, record_ids={key: allocation for key, allocation in plan.record_ids.items() if key[0] == 1}
        ), bounds
    if failure == "root":
        assignment_by_coordinate = {
            key: (allocation[0], allocation[1], allocation[2] + int(key[0] == 2))
            for key, allocation in plan.record_ids.items()
        }
        return replace(plan, record_ids=assignment_by_coordinate), bounds
    if failure == "query":
        return replace(plan, query_key_ids={}), bounds
    if failure == "census":
        return replace(plan, shard=replace(plan.shard, records=plan.shard.records + 1)), bounds
    if failure == "audit_budget":
        return plan, replace(bounds, audit=SourceFactLimits(128, 128, 128))
    if failure == "output_budget":
        return plan, replace(bounds, output=FamilyBlockLimits(1024, 128, 128, 128))
    return plan, replace(bounds, maximum_metadata_bytes=1)


@pytest.mark.asyncio
async def test_failure_after_an_earlier_completed_family_removes_all_pending_rows(tmp_path):
    async with _routed(tmp_path, [_root(), _root("1234567893")], [_child(), _child("1234567893")], shard_count=1) as (
        _definition_value,
        plans,
    ):
        plan = plans[0][0]
        later = sorted(plan.family_ids, key=lambda key: key[1])[-1]
        plan = replace(
            plan, family_ids={key: allocation for key, allocation in plan.family_ids.items() if key != later}
        )
        root = tmp_path / "worker"
        with pytest.raises(ValueError, match="family lacks observed native identities"):
            worker.run_source_shard(plan, root, _limits())
        assert not root.exists()


@pytest.mark.asyncio
async def test_audit_tampering_refuses_evidence_before_yielding(tmp_path):
    async with _routed(tmp_path, [_root()], [_child()]) as (_definition_value, plans):
        plan = next(plan for plan, _expected in plans if plan.shard.records)
        with worker.run_source_shard(plan, tmp_path / "worker", _limits()) as worker_output:
            worker_output.path.chmod(0o600)
            payload = worker_output.path.read_bytes()
            worker_output.path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
            worker_output.path.chmod(0o400)
            with pytest.raises(ValueError, match="audit bytes"):
                next(worker_output.audit_rows())


@pytest.mark.asyncio
async def test_real_spawned_worker_tasks_and_closed_outputs_are_pickleable(tmp_path):
    async with _routed(tmp_path, [_root(), _root("1234567893")], [_child(), _child("1234567893")]) as (
        _definition_value,
        plans,
    ):
        tasks = [pickle.loads(pickle.dumps(plan)) for plan, _expected in plans]
        roots = [str(tmp_path / f"spawn-{plan.shard.shard_id}") for plan in tasks]
        registered_roots = list(roots)
        with ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("spawn")) as executor:
            outputs = list(executor.map(worker.run_source_shard, tasks, roots, [_limits()] * len(tasks)))
        try:
            assert sum(worker_output.counts["source"] for worker_output in outputs) == 2
            for worker_output, (_plan_value, expected) in zip(outputs, plans, strict=True):
                assert worker_output.status == "source_eof"
                assert [
                    (audit_row[2], audit_row[3], audit_row[4])
                    for audit_row in worker_output.audit_rows()
                    if audit_row[0] == "occurrence"
                ] == expected
                assert _rows(worker_output)["family"] or not worker_output.counts["source"]
        finally:
            for worker_output in outputs:
                worker_output.close()
        assert all(not Path(root).exists() for root in registered_roots)
