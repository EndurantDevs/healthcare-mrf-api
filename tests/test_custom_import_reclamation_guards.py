# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reclamation requires intact audit evidence and inactive, unreferenced producers."""

import datetime as dt
import json
import uuid
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import build_scratch as scratch
from process.custom_import import post_seal_audit as audit
from process.custom_import import retirement
from tests.test_custom_import_publication import _generation_seal_fixtures
from tests.test_custom_import_retirement_owner_schema import _owner_session


def _rows(values):
    return SimpleNamespace(all=lambda: values)


async def _ledger(*, counts_by_name=None, final_counts_by_name=None):
    counts_by_name = dict.fromkeys(scratch._COUNT_NAMES, 0) if counts_by_name is None else counts_by_name
    binding_by_name = {
        "contract": audit.CONTRACT,
        "root_page_rows": 2,
        "identity": {"counts": counts_by_name, "candidate": {"generation_id": 9}},
    }
    header = audit.Audit(
        audit_id=uuid.UUID(int=1),
        family_id=7,
        engine_id="engine_a",
        build_id=8,
        generation_id=9,
        dataset_id=10,
        sql_sha256=audit._sql_resource()[1],
        binding=binding_by_name,
        binding_sha256=audit._digest(binding_by_name),
    )
    session = SimpleNamespace(add=Mock(), flush=AsyncMock())
    previous, checkpoints = None, []
    for ordinal in range(63):
        outcome = {0: "started", 41: "range_eof", 42: "range_eof", 61: "not_applicable", 62: "counts"}.get(
            ordinal, "passed"
        )
        previous = await audit._append(
            session,
            header,
            previous,
            checkpoint=ordinal,
            arm_ordinal=ordinal,
            next_ordinal=ordinal + 1,
            root_after_id=None,
            root_eof=True,
            outcome=outcome,
            page_root_count=0,
            range_sha256=audit._digest([]) if ordinal in (41, 42) else None,
            state="complete" if ordinal == 62 else "scanning",
            failure_code=None,
            counts=(counts_by_name if final_counts_by_name is None else final_counts_by_name)
            if ordinal == 62
            else None,
        )
        checkpoints.append(previous)
    assert session.add.call_count == session.flush.await_count == len(checkpoints)
    return header, checkpoints


def _ledger_session(header, checkpoints):
    session = _owner_session()
    session.get = AsyncMock(return_value=header)
    session.scalars = AsyncMock(side_effect=[_rows(checkpoints), _rows([])])
    return session


@pytest.mark.parametrize("damage", ["contract", "count_checkpoint", "missing_arm", "sql_digest", "applicability"])
def test_sql_resource_rejects_substitution(tmp_path, monkeypatch, damage):
    document = deepcopy(audit._sql_resource()[0])
    if damage == "contract":
        document["contract"] = "unsupported"
    elif damage == "count_checkpoint":
        document["counts_checkpoint"] = 61
    elif damage == "missing_arm":
        document["arms"].pop()
    elif damage == "sql_digest":
        document["arms"][0]["sql"] += " "
    else:
        document["arms"][0]["root_range"] = True
    (tmp_path / "post_seal_audit_sql.json").write_text(json.dumps(document))
    monkeypatch.setattr(audit, "__file__", str(tmp_path / "audit.py"))
    audit._sql_resource.cache_clear()
    try:
        with pytest.raises(audit.PostSealAuditRefused, match="SQL"):
            audit._sql_resource()
    finally:
        audit._sql_resource.cache_clear()


async def test_appended_ledger_yields_historical_receipt():
    header, checkpoints = await _ledger()
    session = _ledger_session(header, checkpoints)
    receipt = await audit.completed_audit_receipt(session, header.audit_id, engine_id="engine_a", family_id=7)
    assert receipt == {
        "contract": audit.CONTRACT,
        "audit_id": str(header.audit_id),
        "binding_sha256": header.binding_sha256.hex(),
        "sql_sha256": header.sql_sha256.hex(),
        "checkpoint_sha256": checkpoints[-1].checkpoint_sha256.hex(),
    }
    assert "custom_import_post_seal_audit_checkpoint.checkpoint >" in str(session.scalars.await_args_list[1].args[0])


@pytest.mark.parametrize("damage", ["page_bound", "gap", "counts", "count_type"])
async def test_audit_chain_rejects_corruption(damage):
    counts_by_name = dict.fromkeys(scratch._COUNT_NAMES, 0)
    if damage == "count_type":
        counts_by_name["root_count"] = True
    final_counts_by_name = dict(counts_by_name, root_count=1) if damage == "counts" else None
    header, checkpoints = await _ledger(counts_by_name=counts_by_name, final_counts_by_name=final_counts_by_name)
    if damage == "page_bound":
        header.binding["root_page_rows"] = True
    elif damage == "gap":
        checkpoints.pop(1)
    with pytest.raises(
        audit.PostSealAuditRefused,
        match={
            "page_bound": "page bound",
            "gap": "coverage has a gap",
            "counts": "counts differ",
            "count_type": "count types differ",
        }[damage],
    ):
        await audit._validate_chain(_ledger_session(header, checkpoints), header)


@pytest.mark.parametrize(
    ("ordinal", "changes_by_name", "message"),
    [
        (41, {"outcome": "passed"}, "range was not checked"),
        (41, {"outcome": "range_eof", "root_eof": True, "page_root_count": 1}, "range EOF"),
        (41, {"page_root_count": 0}, "coverage is incomplete"),
        (41, {"range_sha256": None}, "coverage is incomplete"),
        (41, {"root_after_id": 2}, "cursor regressed"),
        (61, {"outcome": "passed"}, "incorrectly skipped"),
        (1, {"outcome": "not_applicable"}, "outcome differs"),
    ],
)
def test_audit_transition_requires_complete_coverage(ordinal, changes_by_name, message):
    previous = SimpleNamespace(next_ordinal=ordinal, root_after_id=2)
    checkpoint = SimpleNamespace(
        **(
            {
                "arm_ordinal": ordinal,
                "state": "scanning",
                "outcome": "range_page",
                "root_eof": False,
                "page_root_count": 2,
                "range_sha256": b"r" * 32,
                "root_after_id": 4,
            }
            | changes_by_name
        )
    )
    with pytest.raises(audit.PostSealAuditRefused, match=message):
        audit._validate_transition(previous, checkpoint, 2)


@pytest.mark.parametrize("damage", ["missing", "engine", "family", "binding", "candidate"])
async def test_completed_receipt_rejects_identity_changes(damage):
    header, checkpoints = await _ledger()
    session = _ledger_session(None if damage == "missing" else header, checkpoints)
    engine, family, candidate_by_name = "engine_a", 7, None
    if damage == "engine":
        engine = "engine_b"
    elif damage == "family":
        family = 8
    elif damage == "binding":
        header.binding_sha256 = b"x" * 32
    elif damage == "candidate":
        candidate_by_name = {"generation_id": 10}
    with pytest.raises(audit.PostSealAuditRefused, match="identity differs"):
        await audit.completed_audit_receipt(
            session, header.audit_id, engine_id=engine, family_id=family, expected_candidate=candidate_by_name
        )
    session.scalars.assert_not_awaited()


@pytest.mark.parametrize("checkpoint", [-1, True, "1"])
async def test_resume_checkpoint_requires_nonnegative_integer(checkpoint):
    session = SimpleNamespace(get=AsyncMock())
    with pytest.raises(audit.PostSealAuditRefused, match="expected checkpoint"):
        await audit.advance_post_seal_audit(
            session, uuid.UUID(int=1), engine_id="engine_a", expected_checkpoint=checkpoint
        )
    session.get.assert_not_awaited()


@pytest.mark.parametrize("identifier", [None, "invalid"])
async def test_audit_identifier_requires_uuid(identifier):
    session = SimpleNamespace(get=AsyncMock())
    with pytest.raises(audit.PostSealAuditRefused, match="identifier"):
        await audit.advance_post_seal_audit(session, identifier, engine_id="engine_a", expected_checkpoint=0)
    session.get.assert_not_awaited()


def _retirement_family(**changes):
    values_by_name = dict(zip(retirement._IDENTITY_NAMES, (7, 10, 11, 12, 13, 14, 15, b"t" * 32, 9), strict=True))
    return SimpleNamespace(**(values_by_name | {"frozen_at": None} | changes))


@pytest.mark.parametrize("missing", ["execution", "lease"])
async def test_retirement_requires_producer_identities(missing):
    execution, lease = SimpleNamespace(state="failed"), SimpleNamespace(fence=15, expires_at=None)
    session = SimpleNamespace(
        scalar=AsyncMock(
            side_effect=[None if missing == "execution" else execution, None if missing == "lease" else lease]
        )
    )
    with pytest.raises(retirement.SnapshotRetirementRefused, match="producer identity is missing"):
        await retirement._eligibility(session, _retirement_family(), None, None)


@pytest.mark.parametrize("pointer", ["current", "rollback"])
async def test_retirement_preserves_retained_generations(pointer):
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[SimpleNamespace(state="failed"), SimpleNamespace(fence=99)])
    )
    with pytest.raises(retirement.SnapshotRetirementRefused, match="current or retained for rollback"):
        await retirement._eligibility(
            session, _retirement_family(), 9 if pointer == "current" else None, 9 if pointer == "rollback" else None
        )
    assert session.scalar.await_count == 2


@pytest.mark.parametrize("expired", [True, False])
async def test_abandoned_retirement_requires_expired_lease(expired):
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    lease = SimpleNamespace(fence=15, expires_at=now + dt.timedelta(seconds=-1 if expired else 1))
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[SimpleNamespace(state="failed"), lease, now]))
    if expired:
        assert await retirement._eligibility(session, _retirement_family(generation_id=None), None, None) == "abandoned"
    else:
        with pytest.raises(retirement.SnapshotRetirementRefused, match="not eligible"):
            await retirement._eligibility(session, _retirement_family(generation_id=None), None, None)


@pytest.mark.parametrize("sealed", [True, False])
async def test_no_change_retirement_requires_seal(sealed):
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[SimpleNamespace(state="no_change"), SimpleNamespace(fence=15), sealed])
    )
    if sealed:
        assert await retirement._eligibility(session, _retirement_family(), None, None) == "no_change"
    else:
        with pytest.raises(retirement.SnapshotRetirementRefused, match="not eligible"):
            await retirement._eligibility(session, _retirement_family(), None, None)
    query = str(session.scalar.await_args.args[0])
    assert "candidate_generation_id" in query and "execution_id" in query


@pytest.mark.parametrize("damage", ["identity", "engine", "storage", "references"])
async def test_retirement_replay_preserves_custody(damage):
    family = _retirement_family()
    tombstone = SimpleNamespace(
        **{name: getattr(family, name) for name in retirement._IDENTITY_NAMES},
        engine_id="engine_a",
        reason="superseded",
        retained_bytes=0,
    )
    if damage == "identity":
        tombstone.producing_token_sha256 = b"x" * 32
    elif damage == "engine":
        tombstone.engine_id = "engine_b"
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[99] if damage == "storage" else [None, True]))
    with pytest.raises(retirement.SnapshotRetirementRefused, match="replay"):
        await retirement._retirement_replay(session, family, tombstone, "engine_a")
    if damage in {"identity", "engine"}:
        session.scalar.assert_not_awaited()


def _scratch_case():
    generation, _materialization, seal, _token = _generation_seal_fixtures()
    family = SimpleNamespace(
        **{name: getattr(generation, name) for name in scratch._IDENTITY[1:]},
        family_id=7,
        frozen_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    )
    build = SimpleNamespace(build_id=8)
    proof = SimpleNamespace(verification_contract="custom-import/build-structure/v1")
    session = SimpleNamespace(
        get=AsyncMock(side_effect=[SimpleNamespace(state="completed"), None, generation]),
        scalars=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: build)),
        scalar=AsyncMock(side_effect=[family.frozen_at, proof]),
    )
    return family, generation, proof, session


@pytest.mark.parametrize(
    ("family_changes_by_name", "generation_changes_by_name", "has_active_lease", "proof_contract"),
    [
        ({"frozen_at": None}, {}, False, "custom-import/build-structure/v1"),
        ({}, {}, True, "custom-import/build-structure/v1"),
        ({}, {"producing_token_sha256": b"x" * 32}, False, "custom-import/build-structure/v1"),
        ({}, {}, False, None),
        ({}, {}, False, "unsupported"),
        ({}, {}, False, "custom-import/manual-acceptance/v1"),
    ],
)
async def test_scratch_requires_inactive_proven_producers(
    family_changes_by_name, generation_changes_by_name, has_active_lease, proof_contract
):
    family, generation, _proof, session = _scratch_case()
    family = SimpleNamespace(**(vars(family) | family_changes_by_name))
    generation = SimpleNamespace(**(vars(generation) | generation_changes_by_name))
    lease = SimpleNamespace(expires_at=family.frozen_at + dt.timedelta(seconds=1)) if has_active_lease else None
    session.get.side_effect = [SimpleNamespace(state="completed"), lease, generation]
    proof = None if proof_contract is None else SimpleNamespace(verification_contract=proof_contract)
    session.scalar.side_effect = [family.frozen_at, proof]
    with pytest.raises(scratch.BuildScratchCleanupRefused):
        await scratch._eligible(session, family)
    if family.frozen_at is None:
        session.get.assert_not_awaited()


@pytest.mark.parametrize("damage", ["missing", "different"])
async def test_scratch_replay_requires_audit_receipt(damage):
    header, checkpoints = await _ledger()
    receipt = await audit.completed_audit_receipt(
        _ledger_session(header, checkpoints), header.audit_id, engine_id="engine_a", family_id=7
    )
    if damage == "missing":
        receipt.pop("audit_id")
    else:
        receipt["checkpoint_sha256"] = "ff" * 32
    marker = SimpleNamespace(aggregate_evidence={"post_seal_audit": receipt})
    with pytest.raises(scratch.BuildScratchCleanupRefused, match="post-seal verification"):
        await scratch._post_audit(
            _ledger_session(header, checkpoints),
            SimpleNamespace(family_id=7),
            SimpleNamespace(verification_contract="custom-import/manual-acceptance/v1"),
            header.binding["identity"]["candidate"],
            "engine_a",
            marker,
        )


async def test_scratch_evidence_stays_bounded():
    session = SimpleNamespace(add=Mock(), flush=AsyncMock())
    with pytest.raises(scratch.BuildScratchCleanupRefused, match="aggregate evidence exceeds"):
        await scratch._record_cleanup(session, {}, "engine_a", {"extra": "x" * 32768})
    session.add.assert_not_called()
    session.flush.assert_not_awaited()


@pytest.mark.parametrize("catalog", ["control", "functions", "projection", "heap", "writer", "column_writer"])
async def test_audit_rejects_incomplete_catalog_custody(catalog):
    generation, _materialization, _seal, _token = _generation_seal_fixtures()
    session = SimpleNamespace(scalar=AsyncMock(return_value=None), execute=AsyncMock(return_value=_rows([])))
    with pytest.raises(audit.PostSealAuditRefused):
        if catalog == "control":
            await audit._row_digests(session, '"synthetic"', generation)
        elif catalog == "functions":
            await audit._function_fingerprints(session, '"synthetic"')
        elif catalog == "projection":
            session.scalar.return_value = SimpleNamespace(family_id=8)
            await audit._projection_tables(session, SimpleNamespace(family_id=7, generation_id=9))
        else:
            heaps = (
                []
                if catalog == "heap"
                else [{"writable": catalog == "writer", "column_writable": catalog == "column_writer"}]
            )
            session.execute.return_value = SimpleNamespace(mappings=lambda: _rows(heaps))
            await audit._heap_fingerprints(session, ('"synthetic"."records"',))


@pytest.mark.parametrize("damage", ["foreign_owner", "procedure"])
async def test_retirement_requires_owned_plain_functions(damage):
    routine = SimpleNamespace(
        oid=1,
        signature='"ci_snapshot_7"."guard"()',
        owned=damage != "foreign_owner",
        prokind="p" if damage == "procedure" else "f",
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=_rows([routine])))
    with pytest.raises(retirement.SnapshotRetirementRefused, match="routine ownership"):
        await retirement._physical_drop(session, '"synthetic"', _retirement_family())
    session.execute.assert_awaited_once()
    assert str(session.execute.await_args.args[0]).lstrip().startswith("SELECT")


async def test_retirement_refuses_cyclic_routine_dependencies():
    routines = [
        SimpleNamespace(oid=identifier, signature=f'"ci_snapshot_7"."guard_{identifier}"()', owned=True, prokind="f")
        for identifier in (1, 2)
    ]
    session = SimpleNamespace(execute=AsyncMock(side_effect=[_rows(routines), _rows([(1, 2), (2, 1)])]))
    with pytest.raises(retirement.SnapshotRetirementRefused, match="cyclic"):
        await retirement._physical_drop(session, '"synthetic"', _retirement_family())
    assert session.execute.await_count == 2
    assert all(str(call.args[0]).lstrip().startswith("SELECT") for call in session.execute.await_args_list)


async def test_retirement_requires_a_known_locked_family():
    session = SimpleNamespace(get=AsyncMock(return_value=None), scalar=AsyncMock(), execute=AsyncMock())
    with pytest.raises(retirement.SnapshotRetirementRefused, match="family is unknown"):
        await retirement._locked_family(session, 7, "engine_a")
    assert session.get.await_count == 2
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("engine", ["", "Upper", True])
async def test_lifecycle_lock_requires_valid_engine_identity(engine):
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(retirement.SnapshotRetirementRefused, match="engine identity"):
        await retirement.lock_snapshot_lifecycle(session, engine_id=engine, dataset_id=7)
    session.execute.assert_not_awaited()


async def test_scratch_requires_complete_relation_census():
    session = SimpleNamespace(execute=AsyncMock(), scalars=AsyncMock(return_value=_rows([])))
    with pytest.raises(scratch.BuildScratchCleanupRefused, match="relation census"):
        await scratch._manifest(session, '"synthetic"', SimpleNamespace(family_id=7, landing_table_oid=1))
    session.execute.assert_awaited_once()


async def test_retirement_requires_consistent_publication_history():
    session = SimpleNamespace(
        scalar=AsyncMock(
            side_effect=[
                SimpleNamespace(generation_id=9, pointer_version=2),
                SimpleNamespace(to_generation_id=9, committed_pointer_version=3),
            ]
        )
    )
    with pytest.raises(retirement.SnapshotRetirementRefused, match="publication history disagrees"):
        await retirement._retained_pointers(session, _retirement_family())


async def test_retirement_rechecks_identity_after_lifecycle_lock():
    session = SimpleNamespace(
        get=AsyncMock(side_effect=[_retirement_family(), None]),
        scalar=AsyncMock(side_effect=[10, None, None]),
        execute=AsyncMock(),
    )
    with pytest.raises(retirement.SnapshotRetirementRefused, match="identity disappeared while locking"):
        await retirement._locked_family(session, 7, "engine_a")
    assert session.scalar.await_count == 3
    assert "pg_advisory_xact_lock" in str(session.execute.await_args.args[0])


async def test_audit_preserves_retained_base_dataset():
    session = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(dataset_id=11)))
    with pytest.raises(audit.PostSealAuditRefused, match="retained base identity differs"):
        await audit._base_context(session, _retirement_family(), SimpleNamespace(base_generation_id=8))


def test_audit_requires_complete_relation_binding():
    context = SimpleNamespace(quoted='"synthetic"', family=SimpleNamespace(family_id=7), base_family=None)
    with pytest.raises(audit.PostSealAuditRefused, match="relation binding is incomplete"):
        audit._compiled("SELECT * FROM __UNKNOWN__", context)
