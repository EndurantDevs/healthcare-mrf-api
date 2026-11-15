# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic runtime harness shared by rooted-graph tests."""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import SimpleNamespace

from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphAcquisitionConfig,
    ProviderDirectoryRootedGraphAcquisitionDependencies,
)
from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPError,
    ProviderDirectoryRootedGraphHTTPResult,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
    ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
    build_insurance_plan_census_query,
    build_provider_directory_organization_affiliation_query,
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphAcquisitionSummary,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphCensusClaim,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    API_BASE,
    SessionLedger,
    claim_for_query,
    identity,
    missing_outcome_json_text,
    replay_claim,
    snapshot,
)


class _RootedGenerationRuntime:
    """Keep one weak-referenceable runtime identity for the real predecessor-proof cache."""

    def __init__(self, **bindings_by_name):
        self.__dict__.update(bindings_by_name)


def configure_rooted_generation(monkeypatch, *, schema="mrf", candidate_schema=None, source_transaction=False):
    """Route rooted calls to isolated inputs while preserving other publisher scopes."""
    from process import provider_directory_rooted_graph_source_runtime as source_runtime
    from process.provider_directory_generic_source_policy import source_transaction as open_source_transaction

    candidate_schema = candidate_schema or schema
    rooted_transaction = ContextVar("rooted_fixture_transaction", default=False)
    inputs = {
        "provider_directory_rooted_graph_" + suffix
        for suffix in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
    }

    def relation_coordinates(relation_schema, root, scoped_name):
        if root in inputs:
            return candidate_schema, root
        if scoped_name == root or not rooted_transaction.get():
            return schema, scoped_name
        return candidate_schema, scoped_name

    @asynccontextmanager
    async def transaction(database, **_options):
        opened = (
            open_source_transaction(database, isolation=_options.get("isolation", "read committed"))
            if source_transaction
            else database.transaction()
        )
        async with opened as session:
            token = rooted_transaction.set(True)
            try:
                yield SimpleNamespace(session=session)
            finally:
                rooted_transaction.reset(token)

    async def copy_candidate_records(database, _table_name, relation, columns, records):
        """COPY through the bound original publisher, like the Core path without an operator."""
        from process.provider_directory_generic_source_storage import _copy
        from process.provider_directory_projection_stage import _copy_driver

        driver = await _copy_driver(database._transaction_binding().session)
        copy_schema = candidate_schema if rooted_transaction.get() else schema
        return await _copy(driver, SimpleNamespace(schema=copy_schema), relation, columns, records)

    runtime = _RootedGenerationRuntime(
        transaction=transaction,
        catalog_schema=schema,
        candidate_schema=candidate_schema,
        relation_coordinates=relation_coordinates,
        copy_candidate_records=copy_candidate_records,
    )
    monkeypatch.setattr(source_runtime, "current_rooted_graph_runtime", lambda database=None: runtime)
    return runtime


@asynccontextmanager
async def rooted_generation_fixture(context, monkeypatch):
    """Register exact private-namespace cleanup before provisioning its input heaps."""
    try:
        await prepare_rooted_generation_fixture(context, monkeypatch)
        yield context
    finally:
        if hasattr(context, "rooted_input_schema"):
            await cleanup_rooted_generation_fixture(context)


async def prepare_rooted_generation_fixture(context, monkeypatch):
    """Prepare genuine plain input heaps without changing guarded canonical storage."""
    from uuid import uuid4

    from db.migration_generic_source_candidates import upgrade_generic_source_candidates
    from process.provider_directory_generic_source_policy import is_source_storage_required

    context.rooted_input_schema = "rooted_fixture_" + uuid4().hex
    roles = (
        ()
        if not hasattr(context, "component")
        else (
            context.component.policy.owner_role,
            context.component.policy.publisher_role,
        )
    )
    async with context.engine.begin() as connection:
        await connection.run_sync(lambda native: upgrade_generic_source_candidates(native, context.schema_name))
        await _copy_fixture_rooted_inputs(connection, context.schema_name, context.rooted_input_schema, roles)
    return configure_rooted_generation(
        monkeypatch,
        schema=context.schema_name,
        candidate_schema=context.rooted_input_schema,
        source_transaction=is_source_storage_required(),
    )


async def _copy_fixture_rooted_inputs(connection, catalog, candidate, roles):
    """Use the operator's LIKE recipe; legacy guards remain on their original heaps."""
    from sqlalchemy import text

    from db.plan_release_storage import storage_relation

    owner = f' AUTHORIZATION "{roles[0]}"' if roles else ""
    await connection.execute(text(f'CREATE SCHEMA "{candidate}"{owner}'))
    for suffix in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission"):
        root = "provider_directory_rooted_graph_" + suffix
        legacy = root + "_legacy" if suffix in ("work", "resource", "edge") else root
        if not await connection.scalar(text("SELECT to_regclass(:name)"), {"name": storage_relation(catalog, legacy)}):
            legacy = root
        template, relation = storage_relation(catalog, legacy), storage_relation(candidate, root)
        await connection.execute(
            text(
                f"CREATE TABLE {relation} (LIKE {template} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING STORAGE INCLUDING INDEXES INCLUDING CONSTRAINTS)"
            )
        )
        checks = (
            await connection.execute(
                text(
                    "SELECT conname FROM pg_constraint WHERE conrelid=to_regclass(:name) AND conname LIKE 'pdrg_%_legacy_scope'"
                ),
                {"name": relation},
            )
        ).scalars()
        for check in checks:
            await connection.execute(text(f'ALTER TABLE {relation} DROP CONSTRAINT "{check}"'))
        await connection.execute(text(f"INSERT INTO {relation} SELECT * FROM {storage_relation(catalog, root)}"))
        if roles:
            await connection.execute(text(f'ALTER TABLE {relation} OWNER TO "{roles[0]}"'))
        for role in roles:
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{candidate}" TO "{role}"'))
            await connection.execute(text(f'GRANT SELECT,INSERT,UPDATE,DELETE ON {relation} TO "{role}"'))


async def cleanup_rooted_generation_fixture(context):
    """Remove only the exact task-created private input namespace before role cleanup."""
    from sqlalchemy import text

    async with context.engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA IF EXISTS "{context.rooted_input_schema}" CASCADE'))


class RuntimeHarness:
    def __init__(self) -> None:
        self.ledger = SessionLedger()
        self.events: list[tuple[object, ...]] = []
        self.status_by_acquisition: dict[str, str] = {}
        self.generic_pending: dict[str, list[object]] = defaultdict(list)
        self.census_status: dict[str, str] = {}
        self.fetch_attempts: dict[str, int] = defaultdict(int)
        self.transient_once: set[str] = set()
        self.block_fetch = False
        self.fetch_started = asyncio.Event()
        self.allow_fetch = asyncio.Event()
        self.block_completion = False
        self.completion_started = asyncio.Event()
        self.allow_completion = asyncio.Event()
        self.heartbeat_count = 0
        self.clock = 0.0

    def _claims(self, role: str):
        role_query = build_provider_directory_practitioner_role_query(
            API_BASE,
            "practitioner.synthetic-1",
        )
        direct_query = build_rooted_graph_direct_read(
            api_base=API_BASE,
            resource_type="Organization",
            resource_id="network.synthetic-1",
        )
        affiliation_query = build_provider_directory_organization_affiliation_query(
            API_BASE,
            "network.synthetic-1",
        )
        census_query = build_insurance_plan_census_query(API_BASE)
        return {
            "role": claim_for_query(role_query, role=role),
            "direct": claim_for_query(direct_query, role=role),
            "affiliation": claim_for_query(
                affiliation_query,
                role=role,
                closure_scope="plan",
            ),
            "census": claim_for_query(
                census_query,
                role=role,
                closure_scope="census",
            ),
        }

    async def revalidate_inputs(self, root, *, database):
        status = self.status_by_acquisition.get(root.acquisition_id, "absent")
        self.events.append(("revalidate", root.acquisition_role, status))
        return snapshot(root.acquisition_role, status=status)

    async def initialize_root(self, root, *, database):
        is_created = root.acquisition_id not in self.status_by_acquisition
        self.status_by_acquisition[root.acquisition_id] = "building"
        if is_created:
            self.generic_pending[root.acquisition_id].append(self._claims(root.acquisition_role)["role"])
            self.census_status[root.acquisition_id] = "pending"
        self.events.append(("initialize", root.acquisition_role, is_created))
        return int(is_created)

    async def claim_work(
        self,
        acquisition_id,
        *,
        query_id=None,
        lease_seconds,
        database,
    ):
        pending = self.generic_pending[acquisition_id]
        index = next(
            (item_index for item_index, item in enumerate(pending) if query_id is None or item.query_id == query_id),
            None,
        )
        if index is None:
            self.events.append(("generic_empty", acquisition_id))
            return None
        claim = pending.pop(index)
        self.events.append(("claim", acquisition_id, claim.kind, claim.query_id))
        return claim

    async def claim_census(self, root, *, lease_seconds, database):
        state = self.census_status.get(root.acquisition_id, "absent")
        self.events.append(("claim_census", root.acquisition_role, state))
        if state != "pending":
            return None
        self.census_status[root.acquisition_id] = "leased"
        return ProviderDirectoryRootedGraphCensusClaim(
            work_claim=self._claims(root.acquisition_role)["census"],
            root_network_references=("Organization/network.synthetic-1",),
        )

    async def census_state(self, acquisition_id, *, database):
        return self.census_status.get(acquisition_id, "absent")

    @staticmethod
    def _missing_result(claim):
        missing_response_json_text = missing_outcome_json_text(404)
        missing_response = missing_response_json_text.encode("utf-8")
        return ProviderDirectoryRootedGraphHTTPResult(
            query_id=claim.query_id,
            resources=(),
            advertised_total=None,
            terminal_page_count=1,
            total_bytes=len(missing_response),
            missing_http_status=404,
            missing_response_sha256=hashlib.sha256(missing_response).hexdigest(),
            missing_response_json_text=missing_response_json_text,
        )

    @staticmethod
    def _census_result(claim):
        resources = (
            {
                "resourceType": "InsurancePlan",
                "id": "plan.synthetic-1",
                "network": [{"reference": "Organization/network.synthetic-1"}],
            },
        )
        return ProviderDirectoryRootedGraphHTTPResult(
            query_id=claim.query_id,
            resources=resources,
            advertised_total=1,
            terminal_page_count=1,
            total_bytes=100,
        )

    @staticmethod
    def _search_result(claim):
        if claim.resource_type == "PractitionerRole":
            resources = (
                {
                    "resourceType": "PractitionerRole",
                    "id": "role.synthetic-1",
                    "practitioner": {"reference": "Practitioner/practitioner.synthetic-1"},
                    "organization": {"reference": "Organization/network.synthetic-1"},
                },
            )
        else:
            resources = (
                {
                    "resourceType": "OrganizationAffiliation",
                    "id": "affiliation.synthetic-1",
                    "participatingOrganization": {"reference": "Organization/network.synthetic-1"},
                },
            )
        return ProviderDirectoryRootedGraphHTTPResult(
            query_id=claim.query_id,
            resources=resources,
            advertised_total=None,
            terminal_page_count=1,
            total_bytes=100,
        )

    async def fetch(self, session, api_base, claim, *, bounds):
        """Return the deterministic fake response for one claimed shape."""

        self.fetch_attempts[claim.query_id] += 1
        self.events.append(
            (
                "fetch",
                session["session_id"],
                claim.kind,
                claim.query_id,
                claim.attempt,
            )
        )
        if claim.query_id in self.transient_once and claim.attempt == 1:
            raise ProviderDirectoryRootedGraphHTTPError(
                "http_transient",
                retryable=True,
                retry_after_seconds=0.0,
            )
        if self.block_fetch:
            self.fetch_started.set()
            await self.allow_fetch.wait()
        if claim.kind == ROOTED_GRAPH_QUERY_DIRECT_READ:
            return self._missing_result(claim)
        if claim.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS:
            return self._census_result(claim)
        return self._search_result(claim)

    async def heartbeat(self, claim, *, lease_seconds, database):
        self.heartbeat_count += 1
        self.events.append(("heartbeat", claim.query_id))

    async def complete_result(self, claim, result, *, database):
        self.events.append(("complete", claim.acquisition_id, claim.kind))
        if self.block_completion:
            self.completion_started.set()
            await self.allow_completion.wait()
        role = "baseline" if claim.acquisition_id == identity().acquisition_id else "candidate"
        claims = self._claims(role)
        if claim.resource_type == "PractitionerRole":
            self.generic_pending[claim.acquisition_id].append(claims["direct"])
        if claim.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS:
            self.census_status[claim.acquisition_id] = "completed"
            self.generic_pending[claim.acquisition_id].append(claims["affiliation"])

    async def complete_missing(
        self,
        claim,
        *,
        missing_http_status,
        missing_response_sha256,
        missing_response_bytes,
        missing_response_json_text,
        database,
    ):
        self.events.append(
            (
                "missing",
                claim.acquisition_id,
                missing_http_status,
                missing_response_sha256,
                missing_response_bytes,
                missing_response_json_text,
            )
        )

    async def complete_error(self, claim, *, error_code, database):
        self.events.append(("error", claim.acquisition_id, error_code))

    async def release_work(self, claim, *, database):
        self.events.append(("release", claim.acquisition_id, claim.query_id))
        replayed_claim = replay_claim(claim, claim.attempt + 1)
        if claim.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS:
            self.census_status[claim.acquisition_id] = "pending"
        else:
            self.generic_pending[claim.acquisition_id].insert(0, replayed_claim)

    async def seal_root(self, root, *, database):
        self.events.append(("seal", root.acquisition_role))
        self.status_by_acquisition[root.acquisition_id] = "sealed"
        return ProviderDirectoryRootedGraphAcquisitionSummary(
            acquisition_id=root.acquisition_id,
            scope_id=root.scope_id,
            completed_count=4,
            error_count=0,
            resource_count=3,
            edge_count=3,
            terminal_set_sha256="1" * 64,
            resource_set_sha256="2" * 64,
            edge_set_sha256="3" * 64,
            rooted_graph_sha256="4" * 64,
            rooted_graph_complete=True,
            endpoint_collection_complete=False,
            endpoint_complete=False,
        )

    async def sleep(self, delay_seconds):
        self.events.append(("sleep", delay_seconds))

    def monotonic(self):
        self.clock += 0.25
        return self.clock

    def dependencies(self):
        return ProviderDirectoryRootedGraphAcquisitionDependencies(
            revalidate_inputs=self.revalidate_inputs,
            initialize_root=self.initialize_root,
            claim_work=self.claim_work,
            claim_census=self.claim_census,
            census_state=self.census_state,
            fetch=self.fetch,
            heartbeat=self.heartbeat,
            complete_result=self.complete_result,
            complete_missing=self.complete_missing,
            complete_error=self.complete_error,
            release_work=self.release_work,
            seal_root=self.seal_root,
            session_scope=self.ledger.scope,
            sleep=self.sleep,
            monotonic=self.monotonic,
        )


def enabled_config(**changes):
    return ProviderDirectoryRootedGraphAcquisitionConfig(
        enabled=True,
        concurrency=1,
        lease_seconds=30,
        heartbeat_seconds=0.01,
        retry_base_seconds=0.001,
        max_retry_seconds=0.01,
        timeout_seconds=0.1,
        root_timeout_seconds=1.0,
        **changes,
    )


__all__ = ("enabled_config", "RuntimeHarness")
