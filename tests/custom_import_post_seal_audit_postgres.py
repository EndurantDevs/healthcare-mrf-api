# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Current SQL parity, sealed audit checkpoints and exact cleanup authorization."""

import hashlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import_post_seal_audit import CustomImportPostSealAudit as Audit
from db.models.custom_import_post_seal_audit import (
    CustomImportPostSealAuditCheckpoint as Checkpoint,
)
from process.custom_import import build_scratch as cleanup
from process.custom_import import post_seal_audit as audit
from tests import custom_import_build_scratch_postgres as scratch
from tests import custom_import_query_projection_build_postgres as projection_fixture
from tests import test_custom_import_manual_acceptance_postgres as manual
from tests.custom_import_postgres_support import _migration
from tests.test_custom_import_build_source_postgres import _source_case


def install_audit_migration(connection, schema):
    scratch.install_cleanup_migration(connection, schema)
    identity = f'"{schema}".custom_import_post_seal_audit'
    if connection.scalar(text("SELECT to_regclass(:relation)"), {"relation": identity}) is None:
        path = Path(__file__).resolve().parents[1] / "alembic/versions/20261102040000_custom_import_post_seal_audit.py"
        migration = _migration(path, "custom_import_post_seal_audit_test_migration")
        migration._schema = lambda: schema
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()


def test_audit_sql_matches_current_verifier():
    root = Path(__file__).resolve().parents[1]
    ranges = _migration(
        root / "alembic/versions/20261020190000_custom_import_verification_root_ranges.py", "audit_parity_ranges"
    )
    arms = ranges._arms()
    staged = arms._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    resource = audit._sql_resource()[0]
    body = ranges._body("audit_template")
    for index, query in enumerate(arms._queries(staged)):
        if (
            len(arms._LABELS[0]) <= index < 60
            and "WITH violations(failure_code,row_identity) AS MATERIALIZED (" not in query
        ):
            query = arms._materialized_query(query, replace)
        if index in (40, 41):
            query = ranges._range_query(arms, replace(query, arms._ACCEPTED_CHILDREN, arms._GROUPED_CHILDREN))
        assert storage._literal(bulk._control_sql(storage, "audit_template", query)) in body
        normalized = (
            query.replace("__CANDIDATE__", "__LEAF__")
            .replace("__BASE__", "__BASE_LEAF__")
            .replace("__ORIGIN_QUERY__", "")
        )
        assert resource["arms"][index]["sql"] == normalized
        assert hashlib.sha256(normalized.encode()).hexdigest() == resource["arms"][index]["sha256"]
    assert resource["root_page_sql"] == ranges._ROOTS.replace("__CANDIDATE__", "__LEAF__")
    assert resource["counts_sql"] == staged._finality()._COUNTS_QUERY.replace("__CANDIDATE__", "__LEAF__")
    expected_indexes = [
        {key: index_attribute for key, index_attribute in spec.items() if key != "ddl"}
        for spec in json.loads(staged._finality()._INDEX_SPECIFICATIONS)
    ]
    assert resource["indexes"] == expected_indexes


async def _ready(case, monkeypatch):
    async with case.engine.begin() as connection:
        await connection.run_sync(install_audit_migration, case.schema_name)
    request, build_id, generation_id = await manual._ready(case, monkeypatch)
    assert await manual._step(case, request, build_id) == "scanning"
    async with case.sessions() as session, session.begin():
        await manual._accept(session, case, build_id, request=request)
    assert await manual._step(case, request, build_id) == "complete"
    sealed = await manual.output.build_output(case.sessions, request, build_id)
    async with case.engine.begin() as connection:
        await connection.execute(text("SELECT pg_stat_force_next_flush()"))
    return request, build_id, sealed, await scratch._family(case, request)


async def _begin(case, family_id, **options):
    async with case.sessions() as session, session.begin():
        await scratch._bounds(session)
        return await audit.begin_post_seal_audit(session, family_id, engine_id="synthetic_engine", **options)


async def _step(case, progress, *, expected=None):
    async with case.sessions() as session, session.begin():
        await scratch._bounds(session)
        return await audit.advance_post_seal_audit(
            session,
            progress.audit_id,
            engine_id="synthetic_engine",
            expected_checkpoint=progress.checkpoint if expected is None else expected,
        )


async def _finish(case, progress):
    for _ in range(2000):
        if progress.state != "scanning":
            return progress
        progress = await _step(case, progress)
    raise AssertionError("bounded fixture audit did not reach EOF")


async def _assert_complete(case, progress):
    assert progress.state == "complete" and progress.next_ordinal == 63
    async with case.sessions() as session, session.begin():
        await scratch._bounds(session)
        header = await session.get(Audit, progress.audit_id)
        receipt = await audit.completed_audit_receipt(
            session, progress.audit_id, engine_id="synthetic_engine", family_id=header.family_id
        )
        checkpoints = (
            await session.scalars(
                select(Checkpoint).where(Checkpoint.audit_id == progress.audit_id).order_by(Checkpoint.checkpoint)
            )
        ).all()
        assert [row.arm_ordinal for row in checkpoints if row.outcome == "range_eof"] == [41, 42]
        assert all(row.page_root_count <= 2 for row in checkpoints)
        assert [row.arm_ordinal for row in checkpoints if row.outcome == "not_applicable"] == [61]
        assert len([row for row in checkpoints if row.outcome == "range_page"]) > 2
        assert receipt["contract"] == audit.CONTRACT


async def _verify_full_native_root_page(publication_case, family_id):
    """Read the unchanged native root-page SQL at its full production cardinality."""
    async with publication_case.sessions() as session, session.begin():
        await scratch._bounds(session)
        await session.execute(
            text("CREATE TEMP TABLE custom_import_build_family (build_id bigint,root_record_id bigint) ON COMMIT DROP")
        )
        await session.execute(
            text(
                "CREATE TEMP TABLE custom_import_build_occurrence (build_id bigint,origin text,"
                "child_revision_id bigint,root_record_id bigint) ON COMMIT DROP"
            )
        )
        await session.execute(text("INSERT INTO pg_temp.custom_import_build_family SELECT 1,generate_series(1,16385)"))
        await session.execute(
            text("INSERT INTO pg_temp.custom_import_build_occurrence VALUES (1,'source',1,16384),(1,'source',2,16384)")
        )
        context = SimpleNamespace(quoted='"pg_temp"', family=SimpleNamespace(family_id=family_id), base_family=None)
        sql = audit._sql_resource()[0]["root_page_sql"].replace("__LEAF__", "pg_temp")
        statement = audit._compiled(sql, context)
        parameters_by_name = {"p_1": 1, "p_2": None, "p_3": audit.ROOT_PAGE_ROWS}
        roots = await session.scalar(statement, parameters_by_name)
        assert roots == list(range(1, 16_385))
        expected = hashlib.sha256(audit.CONTRACT.encode() + b"\0" + json.dumps(roots, separators=(",", ":")).encode())
        assert audit._root_range_digest(roots) == expected.digest()
        parameters_by_name["p_2"] = roots[-1]
        assert await session.scalar(statement, parameters_by_name) == [16_385]
        parameters_by_name["p_2"] = 16_385
        assert await session.scalar(statement, parameters_by_name) is None


async def test_manual_audit_resumes_and_authorizes_only_complete_cleanup(monkeypatch):
    monkeypatch.setattr(audit, "ROOT_PAGE_ROWS", 2)
    async with _source_case() as case:
        request, build_id, sealed, family_id = await _ready(case, monkeypatch)
        await scratch.fixture._activate(case, request, sealed.generation_id)
        original_proof = await manual._proof(case, build_id)
        before_read = await scratch._read(case, request, sealed.generation_id)
        progress = await _begin(case, family_id)
        assert (await _begin(case, family_id)).replayed
        with pytest.raises(cleanup.BuildScratchCleanupRefused, match="full automated"):
            await scratch._trim(case, family_id)
        original = audit._run_arm

        async def interrupted(*arguments):
            await original(*arguments)
            raise ConnectionError("synthetic audited arm acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(audit, "_run_arm", interrupted)
            with pytest.raises(ConnectionError, match="arm acknowledgement"):
                await _step(case, progress)
        first = await _step(case, progress)
        assert first.checkpoint == 1 and (await _step(case, progress)).replayed
        progress = await _finish(case, first)
        await _assert_complete(case, progress)
        with monkeypatch.context() as full_page:
            full_page.setattr(audit, "ROOT_PAGE_ROWS", 16_384)
            await _verify_full_native_root_page(case, family_id)
        assert await manual._proof(case, build_id) == original_proof
        assert not (await scratch._trim(case, family_id)).replayed
        assert (await scratch._trim(case, family_id)).replayed
        assert await manual._proof(case, build_id) == original_proof
        assert await scratch._read(case, request, sealed.generation_id) == before_read


async def test_audit_stales_on_same_oid_heap_and_authority_changes(monkeypatch):
    async with _source_case() as case:
        _request, _build_id, _sealed, family_id = await _ready(case, monkeypatch)
        progress = await _begin(case, family_id)
        async with case.sessions() as session, session.begin():
            await scratch._bounds(session)
            with pytest.raises(audit.PostSealAuditRefused, match="binding changed"):
                async with session.begin_nested():
                    await session.execute(text(f'TRUNCATE "ci_snapshot_{family_id}".source_bulk_landing'))
                    await audit.advance_post_seal_audit(
                        session, progress.audit_id, engine_id="synthetic_engine", expected_checkpoint=0
                    )
        async with case.sessions() as session, session.begin():
            await scratch._bounds(session)
            with pytest.raises(audit.PostSealAuditRefused, match="binding changed"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f'ALTER FUNCTION "{case.schema_name}".verify_custom_import_build_structure(bigint) COST 101'
                        )
                    )
                    await audit.advance_post_seal_audit(
                        session, progress.audit_id, engine_id="synthetic_engine", expected_checkpoint=0
                    )
        document, _old_hash = audit._sql_resource()
        with monkeypatch.context() as patch:
            patch.setattr(audit, "_sql_resource", lambda: (document, b"s" * 32))
            with pytest.raises(audit.PostSealAuditRefused, match="SQL changed"):
                await _step(case, progress)
        assert (await _step(case, progress)).checkpoint == 1


async def test_audit_records_real_invariant_failure_without_waiving_it(monkeypatch):
    async with _source_case() as case:
        _request, build_id, _sealed, family_id = await _ready(case, monkeypatch)
        original = await manual._proof(case, build_id)
        async with case.engine.begin() as connection:
            await connection.execute(
                text(
                    f'UPDATE "ci_snapshot_{family_id}".custom_import_build_family '
                    "SET root_key_sha256=CAST(:digest AS bytea)"
                ),
                {"digest": b"z" * 32},
            )
            await connection.execute(text("SELECT pg_stat_force_next_flush()"))
        progress = await _finish(case, await _begin(case, family_id))
        assert progress.state == "failed" and progress.failure_code == "invariant_violation"
        with pytest.raises(audit.PostSealAuditRefused, match="checkpoint"):
            await _step(case, progress)
        with pytest.raises(audit.PostSealAuditRefused, match="failed"):
            await _begin(case, family_id)
        assert (await _begin(case, family_id, restart=True)).audit_id != progress.audit_id
        with pytest.raises(cleanup.BuildScratchCleanupRefused, match="full automated"):
            await scratch._trim(case, family_id)
        assert await manual._proof(case, build_id) == original


async def _refuse_audit_nonowner(case, family_id):
    """An ordinary database role cannot create protected audit authority."""
    role = "postseal_fixture_" + uuid.uuid4().hex[:12]
    async with case.engine.begin() as connection:
        await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
    try:
        async with case.sessions() as session, session.begin():
            await scratch._bounds(session)
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            with pytest.raises(
                (
                    audit.PostSealAuditRefused,
                    cleanup.BuildScratchCleanupRefused,
                    DBAPIError,
                    cleanup.retirement.SnapshotRetirementRefused,
                )
            ):
                await audit.begin_post_seal_audit(session, family_id, engine_id="synthetic_engine")
    finally:
        async with case.engine.begin() as connection:
            await connection.execute(text(f'DROP ROLE "{role}"'))


async def _assert_audit_storage_custody(case):
    """Audit tables retain owner-only access without adding foreign keys or triggers."""
    async with case.sessions() as session:
        for name in (Audit.__tablename__, Checkpoint.__tablename__):
            relation = f'"{case.schema_name}".{name}'
            assert not await session.scalar(
                text(
                    "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=to_regclass(:relation) AND contype='f')"
                ),
                {"relation": relation},
            )
            assert not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass(:relation))"),
                {"relation": relation},
            )
            assert not await session.scalar(
                text("""SELECT EXISTS(SELECT 1 FROM pg_class c,
              LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
              WHERE c.oid=to_regclass(:relation) AND a.grantee<>c.relowner)"""),
                {"relation": relation},
            )


async def test_audit_metadata_is_owner_only_and_append_chain_is_checked(monkeypatch):
    """Audit rows require owner custody, an intact chain and every applicable check."""
    async with _source_case() as case:
        _request, _build_id, _sealed, family_id = await _ready(case, monkeypatch)
        progress = await _begin(case, family_id)
        await _refuse_audit_nonowner(case, family_id)
        async with case.sessions() as session, session.begin():
            await scratch._bounds(session)
            with pytest.raises(audit.PostSealAuditRefused, match="hash differs"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f'UPDATE "{case.schema_name}".custom_import_post_seal_audit_checkpoint '
                            "SET checkpoint_sha256=CAST(:digest AS bytea) WHERE audit_id=:audit"
                        ),
                        {"digest": b"c" * 32, "audit": progress.audit_id},
                    )
                    header = await session.get(Audit, progress.audit_id)
                    await audit._validate_chain(session, header, require_complete=False)
        async with case.sessions() as session, session.begin():
            await scratch._bounds(session)
            with pytest.raises(audit.PostSealAuditRefused, match="every applicable"):
                async with session.begin_nested():
                    header = await session.get(Audit, progress.audit_id)
                    first = await session.get(Checkpoint, (progress.audit_id, 0))
                    await audit._append(
                        session,
                        header,
                        first,
                        checkpoint=1,
                        arm_ordinal=62,
                        next_ordinal=63,
                        root_after_id=None,
                        root_eof=True,
                        outcome="counts",
                        page_root_count=0,
                        range_sha256=None,
                        state="complete",
                        failure_code=None,
                        counts=header.binding["identity"]["counts"],
                    )
                    await audit._validate_chain(session, header)
        await _assert_audit_storage_custody(case)


async def test_audit_reads_trimmed_retained_base_and_rejects_base_rewrite(monkeypatch):
    monkeypatch.setattr(audit, "ROOT_PAGE_ROWS", 2)
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_audit_migration, case.schema_name)
            await connection.run_sync(scratch.install_current_verification, case.schema_name)
        first_request = await scratch.fixture._request_for(case, scratch.fixture._records(3, 5))
        _build_id, first = await scratch.fixture._complete(case, first_request)
        await scratch.fixture._activate(case, first_request, first.generation_id)
        base_family = await scratch._family(case, first_request)
        await scratch._trim(case, base_family)
        request = await scratch.fixture._request_for(
            case, {"providers": [[]], "rates": [[]]}, seed=first_request, base=first.generation_id, version=1
        )
        _build_id, _sealed = await scratch.fixture._complete(case, request)
        family_id = await scratch._family(case, request)
        async with case.engine.begin() as connection:
            await connection.execute(text("SELECT pg_stat_force_next_flush()"))
        progress = await _finish(case, await _begin(case, family_id))
        await _assert_complete(case, progress)
        async with case.sessions() as session, session.begin():
            await scratch._bounds(session)
            with pytest.raises(audit.PostSealAuditRefused, match="binding changed"):
                async with session.begin_nested():
                    await session.execute(text(f'TRUNCATE "ci_snapshot_{base_family}".source_bulk_landing'))
                    context = await audit.audit_context_for_trim(session, family_id, engine_id="synthetic_engine")
                    await audit.verified_audit_for_trim(session, context, engine_id="synthetic_engine")


async def test_audit_binds_completed_projection_heaps_and_manifest():
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_audit_migration, case.schema_name)
            await connection.run_sync(scratch.install_current_verification, case.schema_name)
        request = await scratch.fixture._request_for(
            case, projection_fixture._records(), definition=projection_fixture._definition(), page_rows=64
        )
        await scratch.fixture._complete(case, request)
        family_id = await scratch._family(case, request)
        async with case.engine.begin() as connection:
            await connection.execute(text("SELECT pg_stat_force_next_flush()"))
        progress = await _begin(case, family_id)
        async with case.sessions() as session, session.begin():
            await scratch._bounds(session)
            header = await session.get(Audit, progress.audit_id)
            derived_oids = (
                await session.scalars(
                    text(
                        "SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname=:namespace AND c.relname IN ('custom_import_derived_root','custom_import_derived_child')"
                    ),
                    {"namespace": f"ci_snapshot_{family_id}"},
                )
            ).all()
            assert len(derived_oids) == 2 and all(str(oid) in header.binding["heaps"] for oid in derived_oids)
            assert "custom_import_query_projection" in header.binding["identity"]["candidate_rows"]
            with pytest.raises(audit.PostSealAuditRefused, match="binding changed"):
                async with session.begin_nested():
                    await session.execute(text(f'TRUNCATE "ci_snapshot_{family_id}".custom_import_derived_root'))
                    await audit.advance_post_seal_audit(
                        session, progress.audit_id, engine_id="synthetic_engine", expected_checkpoint=0
                    )
