# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe the real signed Profile family and its independently pinned Reader."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from dataclasses import asdict

from sqlalchemy import text


def _coordinate(root, batch):
    return root, json.dumps(asdict(batch), sort_keys=True, separators=(",", ":"))


class ProfileAcceptance:
    """Keep observations separate from every original authority and outcome."""

    def __init__(self, component, *, cancel_reader):
        self.component = component
        self.cancel_reader = cancel_reader
        self.projection = None
        self.relations = {}
        self.copies = {}
        self.settled = set()
        self.zero_probes = []
        self.catalogs = {}
        self.analyzed = []
        self.parity = []
        self.reads = []
        self.indexes_finished = False
        self.validated = False
        self.reader_task = None
        self.reader_release = asyncio.Event()
        self.old = None
        self.reader_pid = None
        self.rollbacks = 0
        self.commits = 0

    async def _snapshot(self, session):
        from api import provider_profile_snapshot as snapshots

        identity_fields = tuple(
            (
                await session.execute(
                    text(
                        "SELECT session_user,current_user,pg_backend_pid(),"
                        "current_setting('transaction_isolation'),current_setting('transaction_read_only')"
                    )
                )
            ).one()
        )
        assert identity_fields[:2] == (self.component.reader, self.component.reader)
        assert identity_fields[3:] == ("repeatable read", "on")
        profile_rows = (
            await session.execute(
                text(
                    "SELECT npi,generation_id,profile_json::text,evidence_json::text "
                    'FROM "mrf".provider_directory_profile ORDER BY npi LIMIT 101'
                )
            )
        ).all()
        assert len(profile_rows) <= 100
        return identity_fields[2], deepcopy(
            (
                tuple(tuple(profile_row) for profile_row in profile_rows),
                session.info.get("provider_profile_cms_serving_receipt"),
                snapshots._SNAPSHOT.get(),
            )
        )

    async def start_reader(self):
        from api.provider_profile_snapshot import provider_profile_read_snapshot

        ready = asyncio.get_running_loop().create_future()

        async def read():
            try:
                async with provider_profile_read_snapshot(
                    self.component.database, "mrf", include_detail=True
                ) as session:
                    self.reader_pid, self.old = await self._snapshot(session)
                    ready.set_result(None)
                    await self.reader_release.wait()
                    assert await self._snapshot(session) == (self.reader_pid, self.old)
            except BaseException as error:
                if not ready.done():
                    ready.set_exception(error)
                raise

        self.reader_task = asyncio.create_task(read())
        await asyncio.wait_for(ready, 5)

    async def finish_reader(self, *, cancel=False):
        task = self.reader_task
        if task is None:
            return
        self.reader_task = None
        if cancel:
            task.cancel()
        else:
            self.reader_release.set()
        try:
            await asyncio.wait_for(task, 5)
        except asyncio.CancelledError:
            if not cancel or asyncio.current_task().cancelling():
                raise
            assert task.cancelled()
        finally:
            if not task.done():
                task.cancel()
                await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
        assert self.component.database._reader_database.engine.pool.checkedout() == 0
        async with self.component.admin_engine.connect() as connection:
            assert await connection.scalar(
                text("SELECT NOT EXISTS(SELECT FROM pg_locks WHERE pid=:pid AND locktype='relation')"),
                {"pid": self.reader_pid},
            )

    async def fresh_snapshot(self):
        from api.provider_profile_snapshot import provider_profile_read_snapshot

        async with provider_profile_read_snapshot(self.component.database, "mrf", include_detail=True) as session:
            _pid, result = await self._snapshot(session)
            return result

    def assert_family(self):
        assert self.projection is not None and len(self.projection.tables) == len(self.relations) == 9
        expected_copy, expected_zero = set(), set()
        for table in self.projection.tables:
            for batch in table.batches:
                target = expected_copy if batch.projected_rows else expected_zero
                target.add(_coordinate(table.table_name, batch))
        assert set(self.copies) == expected_copy
        assert self.settled == expected_copy
        assert len(self.zero_probes) == len(expected_zero) and set(self.zero_probes) == expected_zero
        assert self.indexes_finished and self.validated
        assert len(self.analyzed) == 9 and set(self.analyzed) == set(self.relations.values())
        assert len(self.parity) == 9 and set(self.parity) == set(self.relations)
        assert set(self.catalogs) == set(self.relations)
        assert self.reads and all(set(roots) == set(self.relations) for roots in self.reads)
        indexes = [index for facts in self.catalogs.values() for index in json.loads(facts)["indexes"]]
        assert sum(index["primary"] for index in indexes) == 9
        assert sum(not index["primary"] for index in indexes) == 2

    def commit(self, original):
        async def commit(fhir, execution, prepared, **options):
            self.assert_family()
            # The serving Reader's ACCESS SHARE pin must finish before every real AX apply.
            await self.finish_reader(cancel=self.cancel_reader)
            try:
                result = await original(fhir, execution, prepared, **options)
            except RuntimeError as error:
                if str(error) == "late rollback":
                    assert await self.fresh_snapshot() == self.old
                    self.rollbacks += 1
                raise
            current = await self.fresh_snapshot()
            assert current != self.old
            rows, receipt, _oids = current
            assert receipt is not None and rows
            assert receipt["payload"]["selection"]["proof_id"] == execution.attestation.proof_id
            assert all(row[1] == receipt["payload"]["profile"]["generation_id"] for row in rows)
            self.commits += 1
            return result

        return commit

    def install(self, patches):
        from process import provider_directory_profile_native_copy as native
        from process import provider_directory_profile_scope_copy as copying
        from process import provider_directory_profile_scope_reads as reads

        directory = importlib.import_module("process.provider_directory_fhir")
        self._observe_scope(patches, copying)
        self._observe_copy(patches, native)
        self._observe_payload(patches, copying)
        self._observe_indexes(patches, directory)
        self._observe_validation(patches, copying, native)
        self._observe_reads(patches, reads, native)

    def _observe_scope(self, patches, copying):
        original_scope = copying.materialization_scope

        @contextmanager
        def scope(fhir, projection):
            with original_scope(fhir, projection):
                if copying.is_enabled(fhir):
                    assert self.projection is None
                    self.projection = projection
                    admission = fhir._provider_directory_profile_capacity_admission()
                    self.relations = {
                        model.__tablename__: fhir._provider_directory_artifact_scope_table_name(
                            model.__tablename__, admission.run_id
                        )
                        for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS)
                    }
                    assert {table.table_name for table in projection.tables} == set(self.relations)
                yield

        patches.setattr(copying, "materialization_scope", scope)

    def _observe_copy(self, patches, native):
        original_copy = native.execute_profile_copy

        async def copy(fhir, request, pages, *, outcome):
            result = await original_copy(fhir, request, pages, outcome=outcome)
            assert request.scope_projection is self.projection
            assert outcome.retained_pid != self.reader_pid
            key = _coordinate(request.model.__tablename__, request.batch)
            assert key not in self.copies and result.affected_rows == request.batch.projected_rows
            self.copies[key] = (outcome, result)
            return result

        patches.setattr(native, "execute_profile_copy", copy)
        original_matches = native.matches

        def matches(witness, outcome, task):
            result = original_matches(witness, outcome, task)
            if result:
                for key, (original_outcome, original_witness) in self.copies.items():
                    if witness is original_witness:
                        assert outcome is original_outcome and task is asyncio.current_task()
                        self.settled.add(key)
            return result

        patches.setattr(native, "matches", matches)

    def _observe_payload(self, patches, copying):
        original_payload = copying.payload_transaction

        @asynccontextmanager
        async def payload(fhir, kind, relation, batch, *args, **options):
            async with original_payload(fhir, kind, relation, batch, *args, **options):
                yield
            if self.projection is not None and copying.is_enabled(fhir) and batch.projected_rows == 0:
                root = next(root for root, name in self.relations.items() if relation == fhir._unscoped_qt("mrf", name))
                self.zero_probes.append(_coordinate(root, batch))

        patches.setattr(copying, "payload_transaction", payload)

    def _observe_indexes(self, patches, directory):
        original_indexes = directory._finish_artifact_scope_indexes

        async def indexes(schema, names, models):
            if names == self.relations:
                assert not self.indexes_finished
                assert self.reader_task is not None and not self.reader_task.done()
                assert self.settled == set(self.copies)
                assert all(
                    outcome.is_committed and outcome.measurement is not None
                    for outcome, _witness in self.copies.values()
                )
            result = await original_indexes(schema, names, models)
            if names == self.relations:
                self.indexes_finished = True
            return result

        patches.setattr(directory, "_finish_artifact_scope_indexes", indexes)
        original_analyze = directory._analyze_artifact_scope_table

        async def analyze(schema, name):
            result = await original_analyze(schema, name)
            if name in self.relations.values():
                assert self.indexes_finished
                self.analyzed.append(name)
            return result

        patches.setattr(directory, "_analyze_artifact_scope_table", analyze)

    def _observe_validation(self, patches, copying, native):
        original_parity = copying._parity_sql

        def parity(fhir, model, *args):
            result = original_parity(fhir, model, *args)
            assert copying.scope_projection(fhir) is self.projection
            self.parity.append(model.__tablename__)
            return result

        patches.setattr(copying, "_parity_sql", parity)
        original_catalog = native.completed_target_catalog

        async def catalog(fhir, model, relation, oid, *, outcome):
            result = await original_catalog(fhir, model, relation, oid, outcome=outcome)
            facts = json.loads(result)["native_catalog"]
            # Store decoded native facts, including each actual index OID and definition.
            facts["indexes"] = native.decoded(facts["indexes"])
            encoded = json.dumps(facts, sort_keys=True)
            prior = self.catalogs.setdefault(model.__tablename__, encoded)
            assert prior == encoded and self.relations[model.__tablename__] == relation
            return result

        patches.setattr(native, "completed_target_catalog", catalog)
        original_validation = copying.validate_snapshot

        async def validate(*args):
            assert self.indexes_finished and len(self.analyzed) == 9
            result = await original_validation(*args)
            assert self.reader_task is not None and not self.reader_task.done()
            assert copying._SCOPE.get()["closed"] is True
            self.validated = True
            return result

        patches.setattr(copying, "validate_snapshot", validate)

    def _observe_reads(self, patches, reads, native):
        original_read = reads._attest

        async def read(scope, state, *, reusable_origin=None):
            result = await original_read(scope, state, reusable_origin=reusable_origin)
            assert self.validated and json.loads(scope.names_json) == self.relations
            facts = json.loads(state["catalog_by_root"])
            assert set(facts) == set(self.relations)
            for root, catalog in facts.items():
                catalog["indexes"] = native.decoded(catalog["indexes"])
                assert json.dumps(catalog, sort_keys=True) == self.catalogs[root]
            self.reads.append(tuple(facts))
            return result

        patches.setattr(reads, "_attest", read)


@asynccontextmanager
async def profile9_acceptance(component, monkeypatch, *, cancel_reader, rollback=False):
    """Add observations while every production operation retains its original delegate."""
    from tests.provider_profile_snapshot_postgres_support import READ_TABLES

    schema = component.policy.schema
    async with component.admin_engine.begin() as connection:
        for name in READ_TABLES:
            relation = f'"{schema}"."{name}"'
            if await connection.scalar(text("SELECT to_regclass(:relation)"), {"relation": relation}):
                await connection.execute(text(f'GRANT SELECT ON {relation} TO "{component.reader}"'))
                assert (
                    await connection.scalar(
                        text("SELECT has_table_privilege(:reader,:relation,'SELECT')"),
                        {"reader": component.reader, "relation": relation},
                    )
                    is True
                )
    observation = ProfileAcceptance(component, cancel_reader=cancel_reader)
    with monkeypatch.context() as patches:
        observation.install(patches)
        try:
            await observation.start_reader()
            yield observation
            observation.assert_family()
            assert observation.commits == (0 if rollback else 1)
            assert observation.rollbacks == (1 if rollback else 0)
        finally:
            await observation.finish_reader(cancel=True)
