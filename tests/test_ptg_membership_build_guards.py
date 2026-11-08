# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate complete bounded inputs before sealing a membership candidate."""

import hashlib
import struct
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_serving as serving
from api import ptg_provider_membership_build as build
from api.ptg2_v4_graph import V4GraphRoot
from api.ptg_membership_contract import SPEC
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg2_physical_binding import _binding
from tests.test_ptg_membership_projection import PARAMS


def _result(row):
    return SimpleNamespace(
        mappings=lambda: SimpleNamespace(one=lambda: row, all=lambda: row),
        scalars=lambda: SimpleNamespace(all=lambda: row),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "digest", "entry_count"])
async def test_root_authentication_checks_complete_cas_summary(monkeypatch, drift):
    expected_by_field = dict(
        object_kind_count=1,
        map_pack_count=2,
        coordinate_count=3,
        entry_count=4,
        logical_byte_count=5,
        stored_map_byte_count=6,
    )
    root = V4GraphRoot(19, "pattern_v1", b"a" * 32)
    actual = SimpleNamespace(**expected_by_field, map_digest=root.map_digest)
    if drift == "digest":
        actual.map_digest = b"b" * 32
    elif drift:
        actual.entry_count += 1
    summarize = AsyncMock(return_value=actual)
    session = SimpleNamespace(execute=AsyncMock(return_value=_result(expected_by_field)))
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "synthetic")
    monkeypatch.setattr(build, "summarize_persisted_v4_snapshot_maps", summarize)
    if drift:
        with pytest.raises(PTG2ManifestArtifactError, match="CAS mapping root"):
            await build._authenticate_root(session, object(), root)
    else:
        assert await build._authenticate_root(session, object(), root) == 6
    summarize.assert_awaited_once_with(
        session, schema_name="synthetic", snapshot_key=19, batch_rows=32, cancel_gc_candidates=False
    )
    assert session.execute.call_args.args[1] == {"key": 19}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"stored_map_byte_count": 0},
        {"stored_map_byte_count": build._MAX_SOURCE_READ_BYTES + 1},
        {"coordinate_count": build.MAX_BUILD_EDGES + 1},
    ],
)
async def test_root_bounds_refuse_before_cas_read(monkeypatch, changes):
    expected = dict(stored_map_byte_count=1, coordinate_count=1) | changes
    session = SimpleNamespace(execute=AsyncMock(return_value=_result(expected)))
    summarize = AsyncMock()
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "synthetic")
    monkeypatch.setattr(build, "summarize_persisted_v4_snapshot_maps", summarize)
    with pytest.raises(ValueError, match="bounded build"):
        await build._authenticate_root(session, object(), V4GraphRoot(19, "pattern_v1", b"a" * 32))
    summarize.assert_not_awaited()


def _factor(monkeypatch, *, owners=257, count=5140, members=None, span=256):
    manifest = SimpleNamespace(owner_base=0, owner_count=owners, logical_member_count=count, locator_owner_span=span)
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "synthetic")
    monkeypatch.setattr(build, "load_v4_relation_manifest", AsyncMock(return_value=manifest))
    lookup = AsyncMock(side_effect=lambda _session, **kw: {owner: tuple(range(20)) for owner in kw["owner_keys"]})
    if members is not None:
        lookup.side_effect = None
        lookup.return_value = members
    monkeypatch.setattr(build, "lookup_v4_relation_members", lookup)
    copy = AsyncMock()
    progress = Mock()
    monkeypatch.setattr(build.native, "native_copy_record_batch", copy)
    monkeypatch.setattr(build, "enqueue_live_progress", progress)
    return lookup, copy, progress


@pytest.mark.asyncio
async def test_complete_factor_copies_bounded_chunks_without_losing_owner_boundary(monkeypatch):
    lookup, copy, progress = _factor(monkeypatch)
    hasher = hashlib.sha256()
    result = await build._copy_relation(
        object(),
        object(),
        V4GraphRoot(19, "pattern_v1", b"a" * 32),
        "npi_patterns",
        SimpleNamespace(schema_name="synthetic"),
        build.PTGMembershipFirst,
        hasher,
        edge_allowance=6000,
    )
    expected_rows = [(owner, member) for owner in range(257) for member in range(20)]
    copied_rows = [row for call in copy.call_args_list for row in call.kwargs["records"]]
    assert copied_rows == expected_rows
    assert [len(call.kwargs["records"]) for call in copy.call_args_list] == [5000, 120, 20]
    assert [len(call.kwargs["owner_keys"]) for call in lookup.call_args_list] == [256, 1]
    assert [call.kwargs["max_members"] for call in lookup.call_args_list] == [6000, 880]
    expected_hash = hashlib.sha256(b"npi_patterns\0" + b"".join(struct.pack(">II", *row) for row in expected_rows))
    assert hasher.digest() == expected_hash.digest()
    assert result == dict(relation="npi_patterns", owner_base=0, owner_count=257, edge_count=5140)
    assert [call.kwargs["done"] for call in progress.call_args_list] == [0, 5140]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "members,message",
    [
        ({}, "owner coverage"),
        ({0: (1, 1)}, "member allowance"),
        ({0: (2, 1)}, "member allowance"),
        ({0: (-1,)}, "noncanonical"),
        ({0: (2**32,)}, "noncanonical"),
        ({0: ()}, "member count"),
    ],
)
async def test_factor_refuses_incomplete_or_noncanonical_vectors(monkeypatch, members, message):
    _, copy, _ = _factor(monkeypatch, owners=1, count=1, members=members)
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await build._copy_relation(
            object(),
            object(),
            V4GraphRoot(19, "pattern_v1", b"a" * 32),
            "npi_patterns",
            SimpleNamespace(schema_name="synthetic"),
            build.PTGMembershipFirst,
            hashlib.sha256(),
            edge_allowance=1,
        )
    copy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owners,count", [(build.MAX_BUILD_OWNERS + 1, 0), (1, 2)])
async def test_factor_manifest_overflow_never_reads_or_copies_members(monkeypatch, owners, count):
    lookup, copy, _ = _factor(monkeypatch, owners=owners, count=count)
    with pytest.raises(ValueError, match="bounded build"):
        await build._copy_relation(
            object(),
            object(),
            V4GraphRoot(19, "pattern_v1", b"a" * 32),
            "npi_patterns",
            SimpleNamespace(schema_name="synthetic"),
            build.PTGMembershipFirst,
            hashlib.sha256(),
            edge_allowance=1,
        )
    lookup.assert_not_awaited()
    copy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("counts", [[1, 1], [1, 1, 1], [3]])
async def test_physical_code_query_scopes_and_bounds_complete_occurrences(monkeypatch, counts):
    binding = _binding()
    tables = SimpleNamespace(physical_binding=binding, shared_snapshot_key=19, uses_shared_blocks=True)
    rows = [dict(code_key=index, rate_count=count) for index, count in enumerate(counts)]
    session = SimpleNamespace(execute=AsyncMock(return_value=_result(rows)))
    monkeypatch.setattr(build, "MAX_CODE_OCCURRENCES", 2)
    if len(counts) > 2 or sum(counts) > 2:
        with pytest.raises(ValueError, match="requested code"):
            await build._physical_code_rows(session, tables, "CPT", "99213")
    else:
        assert await build._physical_code_rows(session, tables, "CPT", "99213") == rows
    sql, params = session.execute.call_args.args
    assert binding.relation("ptg2_v3_code") in str(sql)
    assert "code_metadata.snapshot_key=:snapshot_key" in str(sql)
    assert "code_metadata.reported_code_system" in str(sql)
    assert "code_metadata.reported_code = :reported_code" in str(sql)
    assert "ORDER BY code_metadata.code_key LIMIT :row_limit" in str(sql)
    assert params["snapshot_key"] == 19 and params["row_limit"] == 3
    assert params["reported_code"] == "99213" and params["reported_code_system"] == "CPT"


@pytest.mark.asyncio
async def test_forward_occurrence_mismatch_never_copies_code_rows(monkeypatch):
    monkeypatch.setattr(build, "_physical_code_rows", AsyncMock(return_value=[dict(code_key=7, rate_count=2)]))
    for name, value in (
        ("_required_shared_snapshot_key", 19),
        ("_required_source_count", 1),
        ("_version_three_forward_lookup_hints", {}),
        ("_payload_schema", "synthetic"),
    ):
        monkeypatch.setattr(serving, name, lambda _tables, value=value: value)
    monkeypatch.setattr(build, "lookup_code_prefix_occurrences_from_db", AsyncMock(return_value=[(1, 0, 0)]))
    copy = AsyncMock()
    monkeypatch.setattr(build, "_copy", copy)
    with pytest.raises(PTG2ManifestArtifactError, match="occurrence coverage"):
        await build._copy_code(
            object(),
            object(),
            PARAMS | {"finalizer": {"state": "legacy_relational"}},
            object(),
            hashlib.sha256(),
            byte_allowance=1024,
        )
    copy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "values,message",
    [
        ([0], "COPY accounting"),
        ([1, 1, 1, 1, True], "second owner domain"),
        ([1, 1, 1, 1, False, True], "explicit coverage"),
        ([1, 1, 1, 1, False, False], None),
    ],
)
async def test_set_validation_refuses_counts_domain_and_coverage(monkeypatch, values, message):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=values))
    factors = [dict(edge_count=1), dict(edge_count=1, owner_base=10, owner_count=1)]
    coverage_rows = [dict(set_count=1)]
    if message:
        with pytest.raises(PTG2ManifestArtifactError, match=message):
            await build._validate_candidate(session, SimpleNamespace(schema_name="synthetic"), factors, coverage_rows)
    else:
        await build._validate_candidate(session, SimpleNamespace(schema_name="synthetic"), factors, coverage_rows)
    if len(values) > 1:
        assert session.scalar.call_args_list[4].args[1] == {"lower": 10, "upper": 11}


@pytest.mark.asyncio
@pytest.mark.parametrize("physical", [False, True])
@pytest.mark.parametrize("count", [2, 65])
async def test_reader_grants_derive_from_exact_source_relation_with_bounded_inventory(monkeypatch, physical, count):
    binding = _binding() if physical else None
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "synthetic")
    roles = ["PUBLIC", '"synthetic_reader"'] if count == 2 else [f"reader_{i}" for i in range(count)]
    session = SimpleNamespace(execute=AsyncMock(return_value=_result(roles)))
    tables = SimpleNamespace(physical_binding=binding)
    if count > 64:
        with pytest.raises(ValueError, match="read-principal inventory"):
            await build._reader_principals(session, tables)
    else:
        assert await build._reader_principals(session, tables) == roles
    sql, params = session.execute.call_args.args
    assert params == {
        "relation": binding.relation("ptg2_v4_npi_scope") if physical else '"synthetic".ptg2_v4_npi_scope'
    }
    assert "acl.privilege_type='SELECT' LIMIT 65" in str(sql)


@pytest.mark.asyncio
async def test_source_drift_after_candidate_validation_prevents_seal(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    tables, root = object(), V4GraphRoot(19, "pattern_v1", b"a" * 32)
    resolve = AsyncMock(side_effect=[(tables, root, PARAMS), (tables, root, PARAMS | {"code": "99214"})])
    monkeypatch.setattr(build, "source_identity", resolve)
    for name in ("lock_identity", "source_pin", "_copy", "_validate_candidate"):
        monkeypatch.setattr(build, name, AsyncMock())
    monkeypatch.setattr(build, "locate", AsyncMock(return_value=None))
    monkeypatch.setattr(build, "_copy_relation", AsyncMock(return_value=dict(edge_count=0)))
    monkeypatch.setattr(build, "_authenticate_root", AsyncMock(return_value=1))
    monkeypatch.setattr(build, "_copy_code", AsyncMock(return_value=([], (), 0)))
    monkeypatch.setattr(build, "_copy_code_factor", AsyncMock(return_value=dict(edge_count=0)))
    monkeypatch.setattr(build, "enqueue_live_progress", Mock())
    monkeypatch.setattr(build.native, "protected_publisher_owner", AsyncMock(return_value=101))
    monkeypatch.setattr(
        build.native, "precreate_model_family_stage", AsyncMock(return_value=SimpleNamespace(schema_name="synthetic"))
    )
    monkeypatch.setattr(build.native, "complete_model_family_stage", AsyncMock())
    seal = AsyncMock()
    monkeypatch.setattr(build, "seal", seal)
    with pytest.raises(PTG2ManifestArtifactError, match="source changed"):
        await build.build_in_session(session, **PARAMS)
    seal.assert_not_awaited()
    assert [str(call.args[0]) for call in session.execute.call_args_list] == [
        f'ANALYZE "synthetic"."{table}"' for table in SPEC.table_names
    ]
    assert resolve.call_args_list[1].kwargs == PARAMS | {"tables": tables}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_worker_owns_transaction_and_propagates_build_failure(monkeypatch, fail):
    session, outcome = object(), []

    @asynccontextmanager
    async def transaction():
        try:
            yield session
        except PTG2ManifestArtifactError:
            outcome.append("rollback")
            raise
        else:
            outcome.append("commit")

    operation = AsyncMock(return_value={"state": "ready"})
    if fail:
        operation.side_effect = PTG2ManifestArtifactError("source changed")
    monkeypatch.setattr(build.db, "transaction", transaction)
    monkeypatch.setattr(build, "build_in_session", operation)
    if fail:
        with pytest.raises(PTG2ManifestArtifactError, match="source changed"):
            await build.build_provider_membership_projection(**PARAMS)
    else:
        assert await build.build_provider_membership_projection(**PARAMS) == {"state": "ready"}
    operation.assert_awaited_once_with(session, **PARAMS)
    assert outcome == ["rollback" if fail else "commit"]
