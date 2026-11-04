# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native root checkpoints preserve authority, rollback and terminal sealing."""

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import import build_output as output
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_output_postgres import _build, _records, _request_for
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_staged_verification_postgres import _install as _install_staged
from tests.test_custom_import_staged_verification_postgres import _state, _step
from tests.test_custom_import_verification_arms_postgres import _install as _install_arms
from tests.test_custom_import_verification_root_ranges import _migration


def _test_migration(root_batch_size):
    migration = _migration()
    body = migration._body
    migration._body = lambda schema: body(schema, root_batch_size=root_batch_size)
    return migration


def _install(connection, schema, root_batch_size=8):
    migration = _test_migration(root_batch_size)
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


def _install_parent(connection, schema, root_batch_size):
    migration = _test_migration(root_batch_size)
    arms = migration._arms()
    staged = arms._staged()
    previous = staged._previous
    staged._previous = lambda name: migration if name == migration.revision else previous(name)
    arms._staged = lambda: staged
    arms._schema = lambda: schema
    arms.op = Operations(MigrationContext.configure(connection))
    arms.upgrade()


async def _frozen(case, monkeypatch):
    request = await _request_for(case, _records(17, 2), page_rows=8)
    build_id, generation_id = await _build(case, request)

    def interrupted(*_):
        raise ConnectionError("synthetic freeze boundary")

    with monkeypatch.context() as patch:
        patch.setattr(output, "_verify_page", interrupted)
        with pytest.raises(ConnectionError, match="freeze boundary"):
            await output.build_output(case.sessions, request, build_id)
    async with case.engine.begin() as connection:
        await connection.run_sync(_install_staged, case.schema_name)
        await connection.run_sync(_install_arms, case.schema_name)
    for _ in range(40):
        assert await _step(case, request, build_id) == "scanning"
    assert await _state(case, build_id) == ("verifying", ("scanning", "profiles", 40, None))
    return request, build_id, generation_id


async def _cursor(case, build_id):
    async with case.sessions() as session:
        return await session.scalar(
            text(
                f'SELECT after_root_record_id FROM "{case.schema_name}".custom_import_build_verification WHERE build_id=:id'
            ),
            {"id": build_id},
        )


async def _reject_cursor_writes(case, build_id):
    async with case.sessions() as session, session.begin():
        for assignment in (
            "after_root_record_id=after_root_record_id-1",
            "after_root_record_id=after_root_record_id+1,winner_count=1",
            "after_root_record_id=after_root_record_id+1,scan_stage='families'",
        ):
            with pytest.raises(DBAPIError, match="build_immutable"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f'UPDATE "{case.schema_name}".custom_import_build_verification SET {assignment} WHERE build_id=:id'
                        ),
                        {"id": build_id},
                    )


async def _reject_old_cursor(case, build_id):
    async with case.engine.begin() as connection:
        with pytest.raises(RuntimeError, match="incompatible_progress"):
            async with connection.begin_nested():
                await connection.execute(
                    text(
                        f'UPDATE "{case.schema_name}".custom_import_build_verification '
                        "SET page_sequence=page_sequence+1,after_root_record_id=1 WHERE build_id=:id"
                    ),
                    {"id": build_id},
                )
                await connection.run_sync(_install, case.schema_name)


@pytest.mark.parametrize("root_batch_size", (8, 16_384))
async def test_ranges_resume_from_existing_proof_and_seal_after_both_directions(monkeypatch, root_batch_size):
    async with _source_case() as case:
        request, build_id, generation_id = await _frozen(case, monkeypatch)
        await _reject_old_cursor(case, build_id)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name, root_batch_size)
        assert await _step(case, request, build_id) == "scanning"
        first = await _cursor(case, build_id)
        assert first is not None
        assert await _state(case, build_id) == ("verifying", ("scanning", "profiles", 40, None))
        await _reject_cursor_writes(case, build_id)

        def interrupted(_):
            raise ConnectionError("synthetic uncommitted root range")

        with monkeypatch.context() as patch:
            patch.setattr(output, "_require_budget", interrupted)
            with pytest.raises(ConnectionError, match="uncommitted root range"):
                await _step(case, request, build_id)
        assert await _cursor(case, build_id) == first
        async with case.engine.begin() as connection:
            # Normal delivery crosses the parent without reinterpreting a temporary range cursor.
            await connection.run_sync(_install_parent, case.schema_name, root_batch_size)
            await connection.run_sync(_install, case.schema_name, root_batch_size)
        assert await _cursor(case, build_id) == first
        for expected_sequence in (40, 41):
            seen_roots = []
            while (await _state(case, build_id))[1][2] == expected_sequence:
                before = await _cursor(case, build_id)
                assert await _step(case, request, build_id) == "scanning"
                after = await _cursor(case, build_id)
                if after is not None:
                    assert before is None or after > before
                    seen_roots.append(after)
            assert len(seen_roots) >= 2 if root_batch_size == 8 else len(seen_roots) <= 1
            assert await _cursor(case, build_id) is None
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id


async def test_later_range_failure_keeps_cursor_and_cannot_seal(monkeypatch):
    async with _source_case() as case:
        request, build_id, _ = await _frozen(case, monkeypatch)
        migration = _test_migration(8)
        original = migration._range_query
        previous_body = " " + migration._body(case.schema_name) + " "

        def faulty_query(arms, query):
            query = original(arms, query)
            return query.replace(
                "SELECT 1 FROM accepted_children o WHERE o.build_id=plan.build_id",
                "SELECT 1 FROM accepted_children o WHERE false AND o.build_id=plan.build_id",
            )

        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        assert await _step(case, request, build_id) == "scanning"
        first = await _cursor(case, build_id)
        migration._schema = lambda: case.schema_name
        migration._range_query = faulty_query

        def install_fault(connection):
            continuation = migration._arms()._staged()._previous("20261019010000_custom_import_build_continuation")
            continuation.op = Operations(MigrationContext.configure(connection))
            continuation._schema = lambda: case.schema_name
            continuation._refresh(
                f'"{case.schema_name}".verify_custom_import_build_structure(bigint)',
                previous_body,
                " " + migration._body(case.schema_name) + " ",
            )

        async with case.engine.begin() as connection:
            await connection.run_sync(install_fault)
        with pytest.raises(DBAPIError, match="candidate_relationship_invalid"):
            await _step(case, request, build_id)
        assert await _cursor(case, build_id) == first
        assert (await _state(case, build_id))[1][2] == 40
        with pytest.raises(CandidateRunnerError, match="complete frozen structural proof"):
            await output._terminal_seal(case.sessions, request, build_id, None)


async def test_root_union_and_complete_groups_preserve_null_duplicate_and_ordinal_semantics():
    async with _source_case() as case:
        arms = _migration()._arms()
        common = """WITH occurrences(build_id,origin,stream_slot,root_record_id,collection_slot,
            raw_parent_key_sha256,child_key_sha256,source_ordinal,child_revision_id,resolved_rejection_id) AS (VALUES
            (1,'source',1,1,1,1,1,1,11,NULL),(1,'source',1,1,1,1,1,2,12,NULL),
            (1,'source',1,2,1,2,2,NULL,21,NULL),(1,'source',1,2,1,2,2,2,22,NULL),
            (1,'source',1,3,1,NULL,3,1,31,NULL),(1,'source',1,3,1,NULL,3,2,32,NULL),
            (1,'source',1,4,1,4,4,1,41,NULL),(1,'source',1,4,1,4,4,2,42,9),
            (1,'source',1,5,1,5,5,1,51,NULL),(1,'source',1,5,1,5,5,1,52,NULL),
            (1,'source',2,6,1,6,6,1,61,NULL),(1,'source',2,6,1,6,6,2,62,NULL),
            (1,'source',3,7,1,7,7,1,71,NULL),(1,'source',3,7,1,7,7,2,72,NULL),
            (1,'source',1,11,1,11,11,1,111,NULL),(1,'source',1,NULL,1,12,12,1,121,NULL),
            (2,'source',1,99,1,99,99,1,991,NULL)
        ), plans(build_id,root_record_id) AS (
            VALUES (1,-9),(1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7),(2,99)
        ), streams(stream_slot,record_kind,collection_slot,duplicate_policy) AS (
            VALUES (1,'child',1,'collapse_identical'),(2,'child',1,'reject'),(3,'child',1,NULL)
        ), page_occurrences AS (
            SELECT * FROM occurrences WHERE build_id=1 AND root_record_id=ANY(ARRAY[-9,1,2,3,4,5,6,7,11])
        ), """
        accepted = arms._GROUPED_CHILDREN.removeprefix("        ), ").removesuffix("        ), plan_roots AS (") + ")"
        accepted = accepted.replace("__CANDIDATE__.custom_import_build_occurrence", "occurrences")
        selected = " SELECT o.child_revision_id FROM accepted_children o JOIN plans p USING(build_id,root_record_id) WHERE o.build_id=1"
        full = "WITH " + accepted + selected
        ranged = "WITH " + accepted.replace("FROM occurrences", "FROM page_occurrences") + selected
        roots = (
            _migration()
            ._ROOTS.replace("__CANDIDATE__.custom_import_build_family", "plans")
            .replace("__CANDIDATE__.custom_import_build_occurrence", "occurrences")
        )
        roots = roots.replace("$1", "1").replace("$2", "NULL::bigint").replace("$3", "20")
        statement = (
            common
            + f"""full_rows AS ({full}), ranged_rows AS ({ranged}), differences AS (
            (SELECT * FROM full_rows EXCEPT ALL SELECT * FROM ranged_rows)
            UNION ALL (SELECT * FROM ranged_rows EXCEPT ALL SELECT * FROM full_rows)
        ) SELECT (SELECT count(*) FROM differences) AS difference_rows,
            (SELECT array_agg(child_revision_id ORDER BY child_revision_id) FROM ranged_rows) AS accepted,
            ({roots}) AS root_ids"""
        )
        async with case.engine.connect() as connection:
            semantic_result = (await connection.execute(text(statement))).one()
        assert semantic_result.difference_rows == 0
        assert semantic_result.accepted == [12, 21, 22, 31, 32, 51, 52, 61, 62, 72]
        assert semantic_result.root_ids == [-9, 1, 2, 3, 4, 5, 6, 7, 11]


async def test_inventory_pages_stay_server_side_and_reuse_only_in_live_transaction(monkeypatch):
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_pages_stay_server_side_and_reuse_only_in_live_transaction as verify,
    )

    await verify(monkeypatch)


@pytest.mark.parametrize("operation", ["insert", "update"])
async def test_inventory_excludes_owner_writes_and_discards_cross_commit_evidence(operation):
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_excludes_owner_writes_and_discards_cross_commit_evidence as verify,
    )

    await verify(operation)


@pytest.mark.parametrize("reference", [9000000, 9223372036854775807])
async def test_inventory_detects_wrong_parent_and_maximum_bigint_references(monkeypatch, reference):
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_detects_wrong_parent_and_maximum_bigint_references as verify,
    )

    await verify(monkeypatch, reference)


@pytest.mark.parametrize("kind,revision_kind", [("root", 1), ("child", 2)])
async def test_inventory_preserves_origin_revision_kind_and_null_references(kind, revision_kind):
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_preserves_origin_revision_kind_and_null_references as verify,
    )

    await verify(kind, revision_kind)


@pytest.mark.parametrize("change", ["truncate", "new_family", "missing_index", "temporary_rows"])
async def test_inventory_refuses_changed_frozen_or_temporary_evidence(change):
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_refuses_changed_frozen_or_temporary_evidence as verify,
    )

    await verify(change)


async def test_inventory_is_bound_to_its_actual_connection_and_backend():
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_is_bound_to_its_actual_connection_and_backend as verify,
    )

    await verify()


async def test_inventory_does_not_certify_interrupted_preparation(monkeypatch):
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_does_not_certify_interrupted_preparation as verify,
    )

    await verify(monkeypatch)


async def test_inventory_excludes_only_the_target_and_completed_retired_sources():
    from tests.custom_import_retirement_inventory_postgres import (
        test_inventory_excludes_only_the_target_and_completed_retired_sources as verify,
    )

    await verify()


async def test_manual_audit_resumes_and_authorizes_only_complete_cleanup(monkeypatch):
    from tests.custom_import_post_seal_audit_postgres import (
        test_manual_audit_resumes_and_authorizes_only_complete_cleanup as verify,
    )

    await verify(monkeypatch)


async def test_audit_stales_on_same_oid_heap_and_authority_changes(monkeypatch):
    from tests.custom_import_post_seal_audit_postgres import (
        test_audit_stales_on_same_oid_heap_and_authority_changes as verify,
    )

    await verify(monkeypatch)


async def test_audit_records_real_invariant_failure_without_waiving_it(monkeypatch):
    from tests.custom_import_post_seal_audit_postgres import (
        test_audit_records_real_invariant_failure_without_waiving_it as verify,
    )

    await verify(monkeypatch)


async def test_audit_metadata_is_owner_only_and_append_chain_is_checked(monkeypatch):
    from tests.custom_import_post_seal_audit_postgres import (
        test_audit_metadata_is_owner_only_and_append_chain_is_checked as verify,
    )

    await verify(monkeypatch)


async def test_audit_reads_trimmed_retained_base_and_rejects_base_rewrite(monkeypatch):
    from tests.custom_import_post_seal_audit_postgres import (
        test_audit_reads_trimmed_retained_base_and_rejects_base_rewrite as verify,
    )

    await verify(monkeypatch)


async def test_audit_binds_completed_projection_heaps_and_manifest():
    from tests.custom_import_post_seal_audit_postgres import (
        test_audit_binds_completed_projection_heaps_and_manifest as verify,
    )

    await verify()


def test_audit_sql_matches_current_verifier():
    from tests.custom_import_post_seal_audit_postgres import (
        test_audit_sql_matches_current_verifier as verify,
    )

    verify()
