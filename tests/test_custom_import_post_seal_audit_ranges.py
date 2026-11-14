# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Full root pages retain canonical hashes without definition-document limits."""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import post_seal_audit as audit
from process.custom_import.definition import MAX_DEFINITION_NODES, DefinitionError


@pytest.mark.parametrize("count", [0, 1, 17, MAX_DEFINITION_NODES - 1])
def test_root_range_digest_preserves_existing_canonical_bytes(count):
    roots = list(range(1, count + 1))
    assert audit._root_range_digest(roots) == audit._digest(roots)


def test_root_range_digest_accepts_the_full_production_page():
    assert audit.ROOT_PAGE_ROWS == 16_384
    roots = list(range(1, audit.ROOT_PAGE_ROWS + 1))
    with pytest.raises(DefinitionError, match="structural limits"):
        audit._digest(roots)
    expected = hashlib.sha256(audit.CONTRACT.encode() + b"\0" + json.dumps(roots, separators=(",", ":")).encode())
    assert audit._root_range_digest(roots) == expected.digest()


@pytest.mark.parametrize("roots", [[True], [0], [-1], [1.0], ["1"], [None], [2, 1], [1, 1]])
def test_root_range_digest_rejects_invalid_ids(roots):
    with pytest.raises(audit.PostSealAuditRefused, match="root range"):
        audit._root_range_digest(roots)


def test_root_range_digest_keeps_the_existing_page_bound():
    with pytest.raises(audit.PostSealAuditRefused, match="root range"):
        audit._root_range_digest(list(range(1, audit.ROOT_PAGE_ROWS + 2)))


def _range_context():
    names = (
        "generation_id dataset_id definition_revision_id schema_revision_id execution_id "
        "capture_bundle_id producing_fence producing_token_sha256 base_generation_id"
    ).split()
    generation = SimpleNamespace(**dict.fromkeys(names, 1))
    generation.producing_token_sha256 = b"t" * 32
    return SimpleNamespace(
        generation=generation,
        build=SimpleNamespace(build_id=1),
        quoted='"synthetic"',
        family=SimpleNamespace(family_id=2),
        base_family=None,
    )


async def test_run_arm_transports_a_full_root_page_without_changing_its_ids():
    roots = list(range(1, audit.ROOT_PAGE_ROWS + 1))
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=roots), execute=AsyncMock(return_value=Mock(first=Mock(return_value=None)))
    )
    previous = SimpleNamespace(next_ordinal=41, checkpoint=40, root_after_id=None)
    progress = await audit._run_arm(session, _range_context(), previous)
    assert session.execute.await_args.args[1]["p_10"] is roots
    assert progress == {
        "checkpoint": 41,
        "arm_ordinal": 41,
        "next_ordinal": 41,
        "root_after_id": roots[-1],
        "root_eof": False,
        "outcome": "range_page",
        "page_root_count": len(roots),
        "range_sha256": audit._root_range_digest(roots),
        "state": "scanning",
        "failure_code": None,
        "counts": None,
    }


async def test_run_arm_refuses_a_repeated_root_before_the_invariant_query():
    session = SimpleNamespace(scalar=AsyncMock(return_value=[5, 6]), execute=AsyncMock())
    previous = SimpleNamespace(next_ordinal=41, checkpoint=40, root_after_id=5)
    with pytest.raises(audit.PostSealAuditRefused, match="root range"):
        await audit._run_arm(session, _range_context(), previous)
    session.execute.assert_not_awaited()


def _index_context(physical_seed):
    namespace = "ci_snapshot_2"
    rows = []
    for ordinal, spec in enumerate(audit._sql_resource()[0]["indexes"]):
        rows.append(
            {
                "namespace": namespace,
                "name": spec["name"],
                "table_name": spec["table"],
                "keys": [audit.re.sub(r" DESC$", "", key) for key in spec["keys"]],
                "options": [3 if key.endswith(" DESC") else 0 for key in spec["keys"]],
                "predicate": spec["predicate"],
                "amname": "btree",
                "unique_index": False,
                "live": True,
                "persistent": True,
                "oid": physical_seed + ordinal,
                "owner_matches": False,
            }
        )
    session = SimpleNamespace(
        execute=AsyncMock(return_value=Mock(mappings=Mock(return_value=Mock(all=Mock(return_value=rows)))))
    )
    context = SimpleNamespace(
        family=SimpleNamespace(family_id=2),
        identity={"candidate": {"family_id": 2}, "counts": {"root_count": 3}},
        tables=('"ci_snapshot_2"."custom_import_root_record"',),
    )
    return session, context, rows


async def test_audit_resume_uses_logical_content_and_ignores_physical_catalog_identity():
    original_session, context, _ = _index_context(100)
    copied_session, _, _ = _index_context(500)
    binding = await audit._binding(original_session, context)
    assert await audit._binding(copied_session, context) == binding
    assert set(binding) == {"contract", "identity", "root_page_rows"}
    header = SimpleNamespace(
        sql_sha256=audit._sql_resource()[1], binding=binding, binding_sha256=audit._digest(binding)
    )
    await audit._current_binding(copied_session, header, context)
    context.identity["counts"]["root_count"] += 1
    with pytest.raises(audit.PostSealAuditRefused, match="binding changed"):
        await audit._current_binding(copied_session, header, context)


async def test_audit_resume_does_not_transfer_an_old_physical_binding():
    session, context, _ = _index_context(100)
    binding = await audit._binding(session, context)
    binding["heaps"] = {"100": {"heap_node": 200}}
    header = SimpleNamespace(
        sql_sha256=audit._sql_resource()[1], binding=binding, binding_sha256=audit._digest(binding)
    )
    with pytest.raises(audit.PostSealAuditRefused, match="binding changed"):
        await audit._current_binding(session, header, context)


async def test_audit_keeps_native_index_shape_validation():
    session, context, rows = _index_context(100)
    rows[0]["keys"] = ["wrong_column"]
    with pytest.raises(audit.PostSealAuditRefused, match="index differs"):
        await audit._binding(session, context)
