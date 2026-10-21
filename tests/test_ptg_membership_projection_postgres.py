# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody, retention and exact SQL parity on a disposable local database."""

import asyncio
import os
import re
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import ptg_provider_membership_build as build
from api.ptg2_v4_graph import V4GraphRoot, V4RelationManifest
from api.ptg_membership_contract import CONTRACT, SPEC
from api.ptg_membership_contract import identity as make_identity
from api.ptg_membership_reader import membership_statement
from api.ptg_membership_storage import locate, receipt, retire_unreferenced
from process import reference_family_archive as native
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def _url():
    raw = os.getenv("HLTHPRT_PTG_MEMBERSHIP_TEST_DSN")
    if not raw:
        pytest.skip("HLTHPRT_PTG_MEMBERSHIP_TEST_DSN is not set")
    url = make_url(raw)
    if (
        url.host not in {"127.0.0.1", "localhost"}
        or url.port not in {5432, 5440, 55486}
        or not re.fullmatch(
            r"(?:hc_ptg_membership_[0-9a-f]{32}|distance_factor_proto_[0-9]{8}_[0-9a-f]{8})", url.database or ""
        )
    ):
        pytest.fail("membership test database must be task-owned local PostgreSQL")
    return url.set(drivername="postgresql+asyncpg")


@asynccontextmanager
async def _database(monkeypatch):
    url = _url()
    suffix = uuid4().hex
    owner, publisher, reader = ("pm_" + role + "_" + suffix for role in ("owner", "publisher", "reader"))
    admin = create_async_engine(url)
    publisher_engine = None
    stages = []
    original_precreate = native.precreate_model_family_stage

    async def track_stage(*args, **kwargs):
        ownership = await original_precreate(*args, **kwargs)
        stages.append(ownership)
        return ownership

    monkeypatch.setattr(native, "precreate_model_family_stage", track_stage)
    try:
        async with admin.begin() as session:
            # The supplied database is isolated; never replace pre-existing namespaces.
            assert not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname IN ('mrf','hp_snapshot_retention'))")
            )
            await session.execute(text(f'CREATE ROLE "{owner}" NOLOGIN'))
            await session.execute(text(f'CREATE ROLE "{publisher}" LOGIN'))
            await session.execute(text(f'CREATE ROLE "{reader}" LOGIN'))
            await session.execute(text(f'GRANT "{owner}" TO "{publisher}"'))
            await session.execute(text(f'GRANT CREATE,CONNECT ON DATABASE "{url.database}" TO "{publisher}"'))
            await session.execute(text(f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{owner}"'))
            await session.execute(text(f'CREATE SCHEMA mrf AUTHORIZATION "{publisher}"'))
            await session.execute(text(f'GRANT USAGE ON SCHEMA mrf,hp_snapshot_retention TO "{reader}"'))
            await session.execute(text("CREATE TABLE mrf.ptg2_snapshot(snapshot_id text PRIMARY KEY)"))
            await session.execute(
                text(
                    "CREATE TABLE mrf.ptg2_snapshot_pin(owner_type text,owner_id text,snapshot_id text REFERENCES mrf.ptg2_snapshot(snapshot_id) ON DELETE RESTRICT,reason text,PRIMARY KEY(owner_type,owner_id,snapshot_id))"
                )
            )
            await session.execute(text("INSERT INTO mrf.ptg2_snapshot VALUES ('synthetic-snapshot')"))
            for table, columns in (
                ("ptg2_current_snapshot", "slot text,snapshot_id text,previous_snapshot_id text"),
                ("ptg2_current_source_snapshot", "source_key text,snapshot_id text,previous_snapshot_id text"),
                ("ptg2_current_plan_source", "plan_source_key text,snapshot_id text,previous_snapshot_id text"),
                (
                    "plan_release_snapshot_binding",
                    "serving_revision_id text,role text,binding_ordinal int,snapshot_id text",
                ),
            ):
                await session.execute(text(f"CREATE TABLE mrf.{table}({columns})"))
            await session.execute(
                text(f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA mrf TO "{publisher}"')
            )
            await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA mrf TO "{reader}"'))
        publisher_engine = create_async_engine(url.set(username=publisher))
        sessions = async_sessionmaker(publisher_engine, expire_on_commit=False)
        yield SimpleNamespace(admin=admin, sessions=sessions, reader=reader, stages=stages, url=url)
    finally:
        if publisher_engine is not None:
            await publisher_engine.dispose()
        async with admin.begin() as session:
            # Exact task-owned database and roles: remove views first, then the captured heaps.
            views = (
                (
                    await session.execute(
                        text(
                            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='hp_snapshot_retention' AND c.relkind='v' AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=:owner)"
                        ),
                        {"owner": owner},
                    )
                )
                .scalars()
                .all()
            )
            for name in views:
                await session.execute(text(f'DROP VIEW hp_snapshot_retention."{name}" RESTRICT'))
            for stage in stages:
                exists = await session.scalar(
                    text("SELECT to_regnamespace(:schema)::oid"), {"schema": stage.schema_name}
                )
                if exists is not None:
                    names = ",".join(f'"{stage.schema_name}"."{table}"' for table in SPEC.table_names)
                    await session.execute(text(f"DROP TABLE {names} RESTRICT"))
                    await session.execute(text(f'DROP SCHEMA "{stage.schema_name}" RESTRICT'))
            for name in (
                "ptg2_snapshot_pin",
                "plan_release_snapshot_binding",
                "ptg2_current_plan_source",
                "ptg2_current_source_snapshot",
                "ptg2_current_snapshot",
                "ptg2_snapshot",
            ):
                await session.execute(text(f"DROP TABLE IF EXISTS mrf.{name} RESTRICT"))
            await session.execute(text("DROP SCHEMA IF EXISTS mrf RESTRICT"))
            await session.execute(text("DROP SCHEMA IF EXISTS hp_snapshot_retention RESTRICT"))
            await session.execute(text(f'REVOKE CREATE,CONNECT ON DATABASE "{url.database}" FROM "{publisher}"'))
            for role in (reader, publisher, owner):
                await session.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
        await admin.dispose()


def _source(monkeypatch, *, fail_at=None, empty_code=False):
    tables = SimpleNamespace(source_key="synthetic-source", snapshot_id="synthetic-snapshot", physical_binding=None)
    root = V4GraphRoot(1, "pattern_v1", bytes.fromhex("ab" * 32))
    identity = make_identity(
        tables,
        root,
        {
            "state": "legacy_relational",
            "map_digest": None,
            "canonical_mapping_digest": None,
            "target_identity_digest": None,
        },
        "CPT",
        "99213",
    )
    first = {0: (0, 1), 1: (1,), 2: ()}
    second = {0: (10, 11), 1: (11, 12)}

    async def source(*_args, **_kwargs):
        return tables, root, identity | {"code": _kwargs.get("code", "99213")}

    async def authenticate(*_args):
        if fail_at == "root":
            raise PTG2ManifestArtifactError("source CAS root differs")

    async def manifest(_session, *, relation, **_kwargs):
        values = first if relation == "npi_patterns" else second
        count = sum(map(len, values.values()))
        return V4RelationManifest(
            1, relation, "members", "locators", 0, len(values), count, count, 4, 16384, 12288, 1024
        )

    async def members(_session, *, relation, owner_keys, **_kwargs):
        if fail_at == "factor" and relation == "pattern_sets":
            raise asyncio.CancelledError
        values = first if relation == "npi_patterns" else second
        return {key: values[key] for key in owner_keys}

    async def codes(*_args):
        return [] if empty_code else [{"code_key": 7, "rate_count": 3}]

    async def forward(*_args, **_kwargs):
        return [
            SimpleNamespace(provider_set_key=10),
            SimpleNamespace(provider_set_key=10),
            SimpleNamespace(provider_set_key=11),
        ]

    from api import ptg2_serving as serving

    monkeypatch.setattr(build, "source_identity", source)
    monkeypatch.setattr(build, "_authenticate_root", authenticate)
    monkeypatch.setattr(build, "load_v4_relation_manifest", manifest)
    monkeypatch.setattr(build, "lookup_v4_relation_members", members)
    monkeypatch.setattr(build, "_physical_code_rows", codes)
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "mrf")
    monkeypatch.setattr(serving, "_shared_forward_entries_for_code_rows", forward)
    return tables, identity


async def _build(db, code="99213"):
    async with db.sessions() as session, session.begin():
        return await build.build_in_session(
            session, source_key="synthetic-source", snapshot_id="synthetic-snapshot", code_system="CPT", code=code
        )


@pytest.mark.asyncio
async def test_native_candidate_code_parity_adoption_dependency_retention_and_safe_retirement(monkeypatch):
    tables, identity = _source(monkeypatch)
    async with _database(monkeypatch) as db:

        async def principals(*_args):
            return ['"' + db.reader + '"']

        monkeypatch.setattr(build, "_reader_principals", principals)
        result = await _build(db)
        assert result["state"] == "ready"
        assert result["code_coverage"][0]["set_count"] == 2
        assert result["code_coverage"][0]["occurrence_count"] == 3
        assert result["source_pin"]["owner_type"] == CONTRACT
        assert len(result["active_attachment"]["name"]) == 55
        # Duplicate normal runs converge on the same immutable candidate/storage.
        assert await _build(db) == result
        assert len(db.stages) == 1
        schema = db.stages[0].schema_name
        async with db.sessions() as session, session.begin():
            row = (
                (
                    await session.execute(
                        membership_statement(schema),
                        {
                            "npis": [1000000001, 1000000002, 1000000003],
                            "keys": [0, 1, 2],
                            "code_keys": [7],
                            "projection_limit": 100,
                            "retained_limit": 100,
                        },
                    )
                )
                .mappings()
                .one()
            )
            assert row["pairs"] == [[1000000001, 10], [1000000001, 11], [1000000002, 11]]
            assert row["first_count"] == 3
            assert row["priced_count"] == 3
        # The retained native view protects all heaps after a GET transaction ends.
        from sqlalchemy.exc import DBAPIError

        async with db.sessions() as session:
            with pytest.raises(DBAPIError):
                async with session.begin():
                    await native.cleanup_model_family_stage(session, SPEC, db.stages[0])
        async with db.sessions() as session, session.begin():
            assert receipt(await locate(session, identity)) == result
            await session.execute(
                text(
                    "INSERT INTO mrf.plan_release_snapshot_binding VALUES('release','in_network',0,'synthetic-snapshot')"
                )
            )
        async with db.sessions() as session:
            with pytest.raises(PTG2ManifestArtifactError, match="retention references"):
                async with session.begin():
                    await retire_unreferenced(session, identity)
        async with db.sessions() as session, session.begin():
            await session.execute(text("DELETE FROM mrf.plan_release_snapshot_binding"))
        async with db.sessions() as pinned, pinned.begin():
            await locate(pinned, identity)
            async with db.sessions() as retire:
                with pytest.raises(DBAPIError):
                    async with retire.begin():
                        await retire_unreferenced(retire, identity)
        async with db.sessions() as session, session.begin():
            assert await retire_unreferenced(session, identity) is True
        async with db.sessions() as session, session.begin():
            assert await locate(session, identity) is None
            assert await session.scalar(text("SELECT count(*) FROM mrf.ptg2_snapshot_pin")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["root", "factor"])
async def test_failed_or_lost_worker_leaves_no_ready_attachment_or_source_pin(monkeypatch, failure):
    _, identity = _source(monkeypatch, fail_at=failure)
    async with _database(monkeypatch) as db:
        with pytest.raises((PTG2ManifestArtifactError, asyncio.CancelledError)):
            await _build(db)
        async with db.sessions() as session, session.begin():
            assert await locate(session, identity) is None
            assert await session.scalar(text("SELECT count(*) FROM mrf.ptg2_snapshot_pin")) == 0
            for stage in db.stages:
                assert (
                    await session.scalar(text("SELECT to_regnamespace(:schema)::oid"), {"schema": stage.schema_name})
                    is None
                )


@pytest.mark.asyncio
async def test_explicit_empty_code_coverage_is_ready_and_not_an_absent_index(monkeypatch):
    _, identity = _source(monkeypatch, empty_code=True)
    async with _database(monkeypatch) as db:

        async def principals(*_args):
            return ['"' + db.reader + '"']

        monkeypatch.setattr(build, "_reader_principals", principals)
        result = await _build(db)
        assert result["code_coverage"] == []
        async with db.sessions() as session, session.begin():
            assert (await locate(session, identity)) is not None
            assert (
                await session.scalar(text(f'SELECT count(*) FROM "{db.stages[0].schema_name}".ptg_membership_code'))
                == 0
            )


@pytest.mark.asyncio
async def test_concurrent_duplicate_builds_adopt_one_identity_and_reader_cannot_mutate_it(monkeypatch):
    _, identity = _source(monkeypatch)
    async with _database(monkeypatch) as db:

        async def principals(*_args):
            return ['"' + db.reader + '"']

        monkeypatch.setattr(build, "_reader_principals", principals)
        one, two = await asyncio.gather(_build(db), _build(db))
        assert one == two
        assert len(db.stages) == 1
        reader_engine = create_async_engine(db.url.set(username=db.reader))
        try:
            from sqlalchemy.exc import DBAPIError

            async with reader_engine.connect() as session:
                assert (
                    await session.scalar(
                        text(f'SELECT count(*) FROM "{db.stages[0].schema_name}".ptg_membership_first')
                    )
                    == 3
                )
            with pytest.raises(DBAPIError):
                async with reader_engine.begin() as session:
                    await session.execute(
                        text(
                            f"UPDATE \"{db.stages[0].schema_name}\".ptg_membership_manifest SET content_digest=repeat('0',64)"
                        )
                    )
        finally:
            await reader_engine.dispose()
        async with db.sessions() as session, session.begin():
            await session.execute(
                text(f'UPDATE "{db.stages[0].schema_name}".ptg_membership_code_coverage SET set_count=3')
            )
        async with db.sessions() as session:
            with pytest.raises(PTG2ManifestArtifactError, match="coverage differs"):
                async with session.begin():
                    await locate(session, identity)


@pytest.mark.asyncio
async def test_sibling_code_index_pin_does_not_make_unreferenced_indexes_mutually_unretirable(monkeypatch):
    _, identity = _source(monkeypatch)
    async with _database(monkeypatch) as db:

        async def principals(*_args):
            return ['"' + db.reader + '"']

        monkeypatch.setattr(build, "_reader_principals", principals)
        await _build(db)
        sibling = await _build(db, code="99214")
        assert len(db.stages) == 2
        async with db.sessions() as session, session.begin():
            assert await retire_unreferenced(session, identity) is True
            pins = (await session.execute(text("SELECT owner_id FROM mrf.ptg2_snapshot_pin"))).scalars().all()
            assert pins == [sibling["projection_id"]]
            assert await locate(session, identity | {"code": "99214"}) is not None
        async with db.sessions() as session, session.begin():
            assert await retire_unreferenced(session, identity | {"code": "99214"}) is True
            assert await session.scalar(text("SELECT count(*) FROM mrf.ptg2_snapshot_pin")) == 0
