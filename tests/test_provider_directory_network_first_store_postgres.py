# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native COPY, cursor rollback, unique frontier and late membership validation."""

import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import provider_directory_network_first_store as storage

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def graph(monkeypatch):
    dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_POSTGRES_DSN")
    if not dsn:
        pytest.skip("explicit disposable PostgreSQL DSN required")
    connection = await asyncpg.connect(dsn)
    database = await connection.fetchval("SELECT current_database()")
    if "test" not in database:
        await connection.close()
        pytest.fail("disposable test database required")
    schema = "network_graph_" + uuid4().hex[:16]
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    policy = SimpleNamespace(schema=schema, runtime_roles=(), control_runtime_roles=())
    publisher = SimpleNamespace(connection=connection, policy=policy)

    @asynccontextmanager
    async def transaction(_database):
        async with connection.transaction():
            yield

    monkeypatch.setattr(storage, "source_transaction", transaction)
    monkeypatch.setattr(storage, "bound_publisher", lambda: publisher)
    model = object()
    source_by_field = {
        "source_id": "source",
        "api_base": "https://directory.example/fhir",
        "_endpoint_dataset_id": "candidate",
        "metadata_json": {
            "provider_directory_network_snapshot": {"name": "Root", "owner_reference": "Organization/payer"}
        },
    }
    directory = SimpleNamespace(
        db=object(),
        _source_metadata=lambda row: row["metadata_json"],
        parse_fhir_resource=lambda source_id, resource, **kwargs: (model, resource),
        RESOURCE_MODELS_BY_TYPE={"Organization": model, "OrganizationAffiliation": model},
        _rest_bundle_acquisition=lambda *args: {},
    )
    store = storage.GraphStore(directory, source_by_field)
    await connection.execute(
        f'CREATE TABLE "{schema}".candidate_payload(resource_type text,id text,payload jsonb,PRIMARY KEY(resource_type,id))'
    )

    async def persist(model, rows):
        for row in rows:
            await connection.execute(
                f'INSERT INTO "{schema}".candidate_payload VALUES($1,$2,$3::jsonb)',
                row["resourceType"],
                row["id"],
                json.dumps(row),
            )
        return len(rows)

    try:
        yield SimpleNamespace(store=store, connection=connection, persist=persist, schema=schema)
    finally:
        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()


def result(**changes):
    return {
        "entries": [],
        "frontier": [],
        "memberships": [],
        "ids": [],
        "total": None,
        "next_url": None,
        "missing_ids": [],
        **changes,
    }


def work(kind="network", key="n1"):
    return {"kind": kind, "key": key, "url": None, "payload": {}}


async def test_atomic_page_resume_count_reconciliation_and_late_union(graph):
    store = graph.store
    await store.prepare([work()])
    async with store.transaction():
        first = (await store.pending())[0]
        await store.apply(
            [first],
            result(
                ids=["a"],
                memberships=[("a", "Organization/n1")],
                total=2,
                next_url="https://directory.example/fhir/OrganizationAffiliation?cursor=opaque",
            ),
            graph.persist,
        )
    # A resumed object reuses durable work and does not reset the exact cursor.
    resumed = storage.GraphStore(store.directory, store.source)
    await resumed.prepare([work()])
    async with resumed.transaction():
        second = (await resumed.pending())[0]
        assert second["pages"] == 1 and second["url"].endswith("cursor=opaque")
    with pytest.raises(ValueError, match="query_count_mismatch"):
        async with resumed.transaction():
            await resumed.apply([second], result(ids=[], total=2), graph.persist)
    async with resumed.transaction():
        assert (await resumed.pending())[0] == second
        await resumed.apply([second], result(ids=["b"], memberships=[("b", "Organization/n1")], total=2), graph.persist)
        assert await resumed.pending_affiliations() == {"a": {"Organization/n1"}, "b": {"Organization/n1"}}
        entries = [
            {
                "resource": {
                    "resourceType": "OrganizationAffiliation",
                    "id": key,
                    "network": [{"reference": "Organization/n1"}],
                }
            }
            for key in ("a", "b")
        ]
        await resumed.retain_entries(entries, graph.persist)
        assert await resumed.pending_affiliations() == {}
        # Stage keeps keys/digests/network IDs, never a second canonical JSON copy.
        fields = await graph.connection.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2",
            graph.schema,
            resumed.prefix + "_resources",
        )
        assert {column["column_name"] for column in fields} == {
            "resource_type",
            "resource_id",
            "content_digest",
            "network_refs",
            "institution_ids",
        }
        await graph.connection.execute(f"INSERT INTO {resumed.table('members')} VALUES('a','Organization/n2')")
    with pytest.raises(ValueError, match="late_network_conflict"):
        async with resumed.transaction():
            await resumed.finish()


async def test_failed_payload_write_rolls_back_frontier_and_resource_key(graph):
    store = graph.store
    await store.prepare([work("owner", "payer")])
    async with store.transaction():
        item = (await store.pending())[0]

    async def fail(*args):
        raise RuntimeError("COPY failure")

    with pytest.raises(RuntimeError, match="COPY failure"):
        async with store.transaction():
            await store.apply(
                [item], result(entries=[{"resource": {"resourceType": "Organization", "id": "payer"}}]), fail
            )
    async with store.transaction():
        assert (await store.pending())[0] == item
        assert await store.counts() == {}
        await store.apply(
            [item], result(entries=[{"resource": {"resourceType": "Organization", "id": "payer"}}]), graph.persist
        )
        assert await store.pending() == []
        assert await store.counts() == {"Organization": 1}


async def _install_normal_group(graph, monkeypatch):
    import dataclasses
    import inspect
    from unittest.mock import AsyncMock

    from process import provider_directory_network_first_intake as intake
    from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage
    from tests.test_process_provider_directory_fhir_unit import (
        _patch_parallel_resource_flow,
        importer,
    )

    monkeypatch.setattr(
        intake, "current_source_storage_binding", lambda: NamedFHIRSourceStorage("synthetic_catalog", graph.schema)
    )
    original_checkpoint_signature = inspect.signature(importer._save_pagination_checkpoint)
    source_record = _patch_parallel_resource_flow(monkeypatch, AsyncMock(side_effect=AssertionError("ordinary fetch")))
    source_record.update(graph.store.source)
    source_record["canonical_api_base"] = source_record["api_base"]
    source_record["_partition_checkpoint_source_ids"] = ("source",)
    source_record["_pagination_checkpoint_context"] = dataclasses.replace(
        source_record["_pagination_checkpoint_context"],
        dataset_id="candidate",
        source_ids=("source",),
        canonical_api_base=source_record["api_base"],
    )
    monkeypatch.setattr(intake.traversal, "is_source", lambda row: row["source_id"] == "source")
    monkeypatch.setattr(intake.traversal, "seed_work", lambda row: [work("owner", "payer"), work("roots", "Root")])
    monkeypatch.setattr(intake, "concurrency_limit", lambda directory, source, requested: requested)
    monkeypatch.setattr(importer, "parse_fhir_resource", graph.store.directory.parse_fhir_resource)
    monkeypatch.setattr(importer, "RESOURCE_MODELS_BY_TYPE", graph.store.directory.RESOURCE_MODELS_BY_TYPE)
    monkeypatch.setattr(importer, "_rest_bundle_acquisition", graph.store.directory._rest_bundle_acquisition)
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", AsyncMock())
    await graph.connection.execute(
        f'CREATE TABLE "{graph.schema}".checkpoints(resource_type text PRIMARY KEY,payload jsonb)'
    )

    async def checkpoint(context, resource_type, **kwargs):
        original_checkpoint_signature.bind(context, resource_type, **kwargs)
        await graph.connection.execute(
            f'INSERT INTO "{graph.schema}".checkpoints VALUES($1,$2::jsonb) ON CONFLICT(resource_type) DO UPDATE SET payload=EXCLUDED.payload',
            resource_type,
            json.dumps(kwargs),
        )

    async def persist(model, rows, **kwargs):
        return await graph.persist(model, rows)

    monkeypatch.setattr(importer, "_save_pagination_checkpoint", checkpoint)
    monkeypatch.setattr(importer, "_upsert_deferred_resource_rows", persist)

    return source_record


def _install_traversal(monkeypatch):
    from unittest.mock import AsyncMock

    from process import provider_directory_network_first_intake as intake

    def org(key):
        return {"resource": {"resourceType": "Organization", "id": key}}

    async def step(host, source, item, **kwargs):
        if item["kind"] == "owner":
            return result(entries=[org("payer")], ids=["payer"])
        if item["kind"] == "roots":
            return result(ids=["n1"], total=1, frontier=[work("organization", "n1")])
        return result(ids=["a"], total=1, memberships=[("a", "Organization/n1")])

    async def read_organizations(host, source, ids, **kwargs):
        return result(
            entries=[org(key) for key in ids],
            ids=ids,
            total=len(ids),
            frontier=[work("network", "n1")] if "n1" in ids else [],
        )

    async def hydrate(host, source, expected, **kwargs):
        assert expected == {"a": {"Organization/n1"}}
        return [
            {
                "resource": {
                    "resourceType": "OrganizationAffiliation",
                    "id": "a",
                    "network": [{"reference": "Organization/n1"}],
                }
            }
        ]

    upstream = AsyncMock(side_effect=step)
    hydration_read = AsyncMock(side_effect=hydrate)
    monkeypatch.setattr(intake.traversal, "step", upstream)
    monkeypatch.setattr(intake.traversal, "read_organizations", read_organizations)
    monkeypatch.setattr(intake.hydration, "hydrate", hydration_read)
    monkeypatch.setattr(
        intake.traversal,
        "affiliation_result",
        lambda source, entries: result(entries=entries, frontier=[work("organization", "institution")]),
    )
    return upstream, hydration_read


async def test_normal_import_group_uses_durable_graph_and_returns_exact_counters(graph, monkeypatch):
    """Exercise the normal group twice against durable PostgreSQL graph work."""
    from process import provider_directory_network_first_intake as intake
    from tests.test_process_provider_directory_fhir_unit import _source_http_test_session, importer

    source_record = await _install_normal_group(graph, monkeypatch)
    upstream, hydration_read = _install_traversal(monkeypatch)
    completion_by_type = {}
    stats_by_type = {}
    with _source_http_test_session():
        for _ in range(2):
            counts = await importer._import_resources(
                [source_record],
                resources=list(intake.RESOURCE_TYPES),
                per_resource_limit=0,
                page_limit=0,
                page_count=100,
                timeout=3,
                run_id="run_1",
                stream_batch_size=100,
                source_concurrency=1,
                resource_scan_concurrency=2,
                is_pagination_checkpointing_enabled=True,
                defer_typed_materialization=True,
                resource_completion=completion_by_type,
                resource_fetch_stats=stats_by_type,
            )
            assert counts == {"Organization": 3, "OrganizationAffiliation": 1}
    assert completion_by_type == {kind: {"source"} for kind in intake.RESOURCE_TYPES}
    assert hydration_read.await_count == 1 and upstream.await_count == 3
    checkpoints = await graph.connection.fetch(f'SELECT resource_type,payload FROM "{graph.schema}".checkpoints')
    assert len(checkpoints) == 2
    for saved in checkpoints:
        checkpoint_payload = json.loads(saved["payload"])
        assert (
            checkpoint_payload["next_url"] is None
            and checkpoint_payload["rows_processed"] == counts[saved["resource_type"]]
        )
        assert checkpoint_payload["pages_processed"] > 0
    assert stats_by_type["Organization"]["sources_completed"] == 2


async def test_cleanup_requires_published_and_preserves_unrelated_tables(graph):
    store = graph.store
    await store.prepare([work()])
    await graph.connection.execute(
        f'CREATE TABLE "{graph.schema}".provider_directory_endpoint_dataset(dataset_id text PRIMARY KEY,status text)'
    )
    await graph.connection.execute(
        f"INSERT INTO \"{graph.schema}\".provider_directory_endpoint_dataset VALUES('candidate','validated')"
    )
    candidate = SimpleNamespace(dataset_id="candidate", source_ids=("source",))
    with pytest.raises(ValueError, match="cleanup_publication_required"):
        await storage.cleanup_published(store.directory, candidate)
    assert await graph.connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL", f'"{graph.schema}"."{store.prefix}_state"'
    )
    await graph.connection.execute(
        f"UPDATE \"{graph.schema}\".provider_directory_endpoint_dataset SET status='published'"
    )
    await storage.cleanup_published(store.directory, candidate)
    assert not await graph.connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL", f'"{graph.schema}"."{store.prefix}_state"'
    )
    assert await graph.connection.fetchval("SELECT to_regclass($1) IS NOT NULL", f'"{graph.schema}".candidate_payload')
    await storage.cleanup_published(store.directory, candidate)


async def test_overlapping_root_queries_resume_separately_and_deduplicate_organizations(graph):
    store = graph.store
    await store.prepare([work("roots", "A"), work("roots", "B")])
    for key, ids in (("A", ["shared"]), ("B", ["shared", "isolated"])):
        async with store.transaction():
            [query] = await store.pending()
            assert (query["kind"], query["key"]) == ("roots", key)
            await store.apply(
                [query], result(ids=ids, total=len(ids), frontier=[work("organization", i) for i in ids]), graph.persist
            )
    assert await store.pending() == []
    assert [row["key"] for row in await store.pending(organizations=True)] == ["isolated", "shared"]
    assert await graph.connection.fetchval(f"SELECT count(*) FROM {store.table('ids')}") == 3
    assert await graph.connection.fetchval(f"SELECT count(*) FROM {store.table('roots')}") == 2


async def test_organization_batch_with_one_stale_cursor_cannot_advance_other_rows(graph):
    store = graph.store
    await store.prepare([work("organization", key) for key in ("one", "two", "three")])
    first, *others = await store.pending(organizations=True)
    async with store.transaction():
        await store.apply([first], result(), graph.persist)
    with pytest.raises(ValueError, match="cursor_changed"):
        async with store.transaction():
            await store.apply([first, *others], result(), graph.persist)
    assert await store.pending(organizations=True) == others
    async with store.transaction():
        await store.apply(others, result(), graph.persist)
    assert await store.pending(organizations=True) == []


async def test_unmeasured_chains_get_first_round_then_largest_total_first(graph):
    store = graph.store
    await store.prepare([work(key=key) for key in ("unmeasured", "small", "large")])
    async with store.transaction():
        await graph.connection.execute(
            f"UPDATE {store.table('work')} SET total=CASE key WHEN 'small' THEN 2 WHEN 'large' THEN 100 ELSE NULL END"
        )
        assert [item["key"] for item in await store.pending()] == ["unmeasured", "large", "small"]


async def test_completed_owned_network_cannot_omit_canonical_positive(graph):
    store = graph.store
    await store.prepare([work(key="n1"), work(key="n2")])
    async with store.transaction():
        by_key = {item["key"]: item for item in await store.pending()}
        await store.apply(
            [by_key["n1"]], result(ids=["a"], memberships=[("a", "Organization/n1")], total=1), graph.persist
        )
        await store.apply([by_key["n2"]], result(ids=[], total=0), graph.persist)
        await store.retain_entries(
            [
                {
                    "resource": {
                        "resourceType": "OrganizationAffiliation",
                        "id": "a",
                        "network": [{"reference": "Organization/n1"}, {"reference": "Organization/n2"}],
                    }
                }
            ],
            graph.persist,
        )
    with pytest.raises(ValueError, match="canonical_membership_omitted"):
        async with store.transaction():
            await store.finish()


async def test_copy_uses_candidate_schema(graph):
    """Rolling back the page also removes its private COPY scratch relation."""
    await graph.store.prepare([])
    async with graph.store.transaction():
        scratch_name = graph.store.table("batch")
    with pytest.raises(RuntimeError, match="rollback page"):
        async with graph.store.transaction():
            scratch = await graph.store.copy_stage(("id",), [("n",)], "id text")
            assert scratch == scratch_name
            assert await graph.connection.fetchval(f"SELECT id FROM {scratch}") == "n"
            raise RuntimeError("rollback page")
    assert await graph.connection.fetchval("SELECT to_regclass($1)", scratch_name) is None


async def test_owned_root_cannot_be_replaced_by_a_different_completed_network(graph):
    store = graph.store
    await store.prepare([work("network", "related")])
    async with store.transaction():
        await store.apply(await store.pending(), result(ids=[], total=0), graph.persist)
        await store._retain_owned_roots(result(frontier=[work("organization", "root")]))
    async with store.transaction():
        assert (await store.proof())["membership_queries_complete"] == 1
        with pytest.raises(ValueError, match="root_network_unreconciled"):
            await store.finish()


async def test_late_contradictory_page_rolls_back_and_corrected_filter_resumes(graph):
    store = graph.store
    await store.prepare([work(key="n2")])
    retained_entry_by_field = {
        "resource": {
            "resourceType": "OrganizationAffiliation",
            "id": "a",
            "network": [{"reference": "Organization/n1"}],
        }
    }
    async with store.transaction():
        await store.retain_entries([retained_entry_by_field], graph.persist)
        cursor = (await store.pending())[0]
        original_digest = await graph.connection.fetchval(
            f"SELECT content_digest FROM {store.table('resources')} WHERE resource_id='a'"
        )
        retained_payload_json = await graph.connection.fetchval(
            f'SELECT payload FROM "{graph.schema}".candidate_payload WHERE id=$1', "a"
        )
    with pytest.raises(ValueError, match="late_network_conflict"):
        async with store.transaction():
            await store.apply(
                [cursor], result(ids=["a"], memberships=[("a", "Organization/n2")], total=1), graph.persist
            )
    async with store.transaction():
        assert (await store.pending())[0] == cursor
        assert await graph.connection.fetchval(f"SELECT count(*) FROM {store.table('members')}") == 0
        assert (
            await graph.connection.fetchval(
                f"SELECT content_digest FROM {store.table('resources')} WHERE resource_id='a'"
            )
            == original_digest
        )
        assert (
            await graph.connection.fetchval(f'SELECT payload FROM "{graph.schema}".candidate_payload WHERE id=$1', "a")
            == retained_payload_json
        )
        # The corrected upstream filter can now finish this same uncommitted page.
        await store.apply([cursor], result(ids=[], total=0), graph.persist)
        assert await store.pending() == []
        assert await graph.connection.fetchval(f"SELECT count(*) FROM {store.table('resources')}") == 1
    with pytest.raises(ValueError, match="fresh_candidate_required:resource_changed"):
        async with store.transaction():
            await store.retain_entries(
                [{"resource": {**retained_entry_by_field["resource"], "network": [{"reference": "Organization/n2"}]}}],
                graph.persist,
            )
    async with store.transaction():
        assert (
            await graph.connection.fetchval(
                f"SELECT content_digest FROM {store.table('resources')} WHERE resource_id='a'"
            )
            == original_digest
        )


async def test_institution_count_uses_only_distinct_resolved_native_affiliation_references(graph):
    store = graph.store
    await store.prepare([])
    organizations = [
        {"resource": {"resourceType": "Organization", "id": key}}
        for key in ("payer", "network-only", "unlinked-support", "institution", "organization-party")
    ]
    affiliations = [
        {
            "resource": {
                "resourceType": "OrganizationAffiliation",
                "id": key,
                "network": [{"reference": "Organization/network-only"}],
                "organization": {"reference": "Organization/organization-party"},
                "participatingOrganization": {"reference": reference},
            }
        }
        for key, reference in (
            ("a", "Organization/institution"),
            ("b", "https://directory.example/fhir/Organization/institution"),
            ("c", "Organization/missing"),
        )
    ]
    async with store.transaction():
        await store.retain_entries(organizations + affiliations, graph.persist)
        await store._retain_missing(result(missing_ids=["missing"]))
        counts = await store.proof()
        assert counts["participating_organizations"] == 2
        assert counts["unresolved_organization_references"] == 1
        assert (await store.counts())["Organization"] == 5
        assert await graph.connection.fetchval(
            f"SELECT institution_ids FROM {store.table('resources')} WHERE resource_type='OrganizationAffiliation' AND resource_id='b'"
        ) == ["institution", "organization-party"]


@pytest.mark.parametrize("reference", [[], {"reference": "https://other.example/fhir/Organization/foreign"}])
async def test_malformed_or_foreign_institution_reference_cannot_persist(graph, reference):
    store = graph.store
    await store.prepare([])
    affiliation_by_field = {
        "resource": {
            "resourceType": "OrganizationAffiliation",
            "id": "a",
            "network": [{"reference": "Organization/n1"}],
            "organization": reference,
        }
    }
    with pytest.raises(ValueError, match="canonical_affiliation_"):
        async with store.transaction():
            await store.retain_entries([affiliation_by_field], graph.persist)
    async with store.transaction():
        assert await store.counts() == {}


def _published_graph_fixture(graph):
    """Describe the current institution-only edition used in publication tests."""
    from process import provider_directory_network_snapshot_scope as scope
    from process.provider_directory_network_traversal import CONFIG, SOURCE_ID
    from tests.test_provider_directory_network_snapshot_scope import example

    catalog = graph.schema + "_catalog"
    edition = graph.schema + "_edition"
    prior = graph.schema + "_prior"
    dataset = SimpleNamespace(
        dataset_id="candidate",
        source_id=SOURCE_ID,
        endpoint_id="endpoint",
        evidence_run_id="root",
        storage_schema=edition,
        selected_resources=scope.RESOURCE_TYPES,
        dataset_hash="a" * 64,
    )
    reviewed_scope = scope.make_scope(
        root_name=CONFIG["name"],
        owner_reference=CONFIG["owner_reference"],
        discovery_counts=example()["discovery_counts"],
        additional_names=list(CONFIG["name_search_characters"]),
    )
    diagnostics_by_type = {kind: {scope.SCOPE_KEY: reviewed_scope} for kind in scope.RESOURCE_TYPES}
    metadata_by_field = {
        "source_ids": [SOURCE_ID],
        "selected_resources": list(scope.RESOURCE_TYPES),
        "resource_diagnostics": diagnostics_by_type,
        **scope.publication_scope_metadata(diagnostics_by_type),
    }
    cleanup_by_field = dict(
        catalog_schema=catalog,
        candidate_schema=graph.schema,
        source_id=SOURCE_ID,
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        acquisition_root_run_id=dataset.evidence_run_id,
        dataset_hash=dataset.dataset_hash,
    )
    return SimpleNamespace(dataset=dataset, options=cleanup_by_field, metadata=metadata_by_field, prior=prior)


@pytest.fixture
async def published_graph(graph, monkeypatch):
    """Create the current and previous named editions in isolated test schemas."""
    from process import provider_directory_named_edition as named

    fixture = _published_graph_fixture(graph)
    connection = graph.connection
    dataset = fixture.dataset
    catalog = fixture.options["catalog_schema"]
    edition = dataset.storage_schema
    prior = fixture.prior
    monkeypatch.setattr(named, "POINTER_SCHEMA", catalog)
    try:
        for schema in (catalog, edition, prior):
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(
                f'CREATE TABLE "{schema}".{named.HEADER}(dataset_id text PRIMARY KEY, endpoint_id text, '
                "acquisition_root_run_id text, dataset_hash text, status text, is_current boolean, "
                "published_at timestamptz, superseded_at timestamptz, resource_count bigint, publication_metadata_json jsonb)"
            )
        await connection.execute(
            f'CREATE TABLE "{catalog}".{named.POINTER_TABLE}(catalog_schema text,endpoint_id text, dataset_id text, '
            "schema_name text, previous_schema_name text, previous_dataset_id text, PRIMARY KEY(catalog_schema,endpoint_id))"
        )
        await connection.execute(
            f'INSERT INTO "{catalog}".{named.POINTER_TABLE} VALUES($1,$2,$3,$4,$5,$6)',
            catalog,
            dataset.endpoint_id,
            dataset.dataset_id,
            edition,
            prior,
            "old",
        )
        await connection.execute(f'CREATE TABLE "{catalog}".provider_directory_source(source_id text,endpoint_id text)')
        await connection.execute(
            f'INSERT INTO "{catalog}".provider_directory_source VALUES($1,$2)', dataset.source_id, dataset.endpoint_id
        )
        for schema, dataset_id in ((edition, dataset.dataset_id), (prior, "old")):
            await connection.execute(
                f"INSERT INTO \"{schema}\".{named.HEADER} VALUES($1,$2,$3,$4,'published',true,now(),NULL,1,$5)",
                dataset_id,
                dataset.endpoint_id,
                dataset.evidence_run_id,
                dataset.dataset_hash,
                json.dumps(fixture.metadata),
            )
        yield fixture
    finally:
        for schema in (prior, edition, catalog):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_followup_reads_exact_named_header_with_absent_or_stale_legacy_header(graph, published_graph):
    from process import provider_directory_named_edition as named
    from process import provider_directory_network_snapshot_scope as scope

    connection, dataset = graph.connection, published_graph.dataset
    catalog = published_graph.options["catalog_schema"]

    @asynccontextmanager
    async def snapshot(source_ids):
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            yield await named.named_current_editions(connection, catalog, source_ids)

    async def legacy_header(dataset_id):
        row = await connection.fetchrow(f'SELECT * FROM "{catalog}".{named.HEADER} WHERE dataset_id=$1', dataset_id)
        return dict(row) if row is not None else {}

    directory = SimpleNamespace(
        _artifact_selection_snapshot=snapshot, _endpoint_dataset_state=legacy_header, _json_object=storage.decoded
    )
    for legacy_present in (False, True):
        if legacy_present:
            await connection.execute(
                f'INSERT INTO "{catalog}".{named.HEADER} SELECT * FROM "{dataset.storage_schema}".{named.HEADER}'
            )
            await connection.execute(f"UPDATE \"{catalog}\".{named.HEADER} SET publication_metadata_json='{{}}'")
        followup = await scope.followup_scope_metadata(directory, dataset, {"dataset_id": dataset.dataset_id})
        assert followup[scope.SCOPE_KEY] == published_graph.metadata[scope.SCOPE_KEY]
        assert followup["endpoint_complete"] is False and followup["dataset_id"] == dataset.dataset_id

    await connection.execute(
        f'UPDATE "{dataset.storage_schema}".{named.HEADER} SET publication_metadata_json=$1',
        json.dumps({"source_ids": [dataset.source_id]}),
    )
    with pytest.raises(RuntimeError, match="scope_invalid"):
        await scope.followup_scope_metadata(directory, dataset, {})
    await connection.execute(
        f'UPDATE "{catalog}".{named.POINTER_TABLE} SET schema_name=previous_schema_name,dataset_id=previous_dataset_id'
    )
    with pytest.raises(RuntimeError, match="scope_invalid"):
        await scope.followup_scope_metadata(directory, dataset, {})


async def test_named_cleanup_requires_committed_matching_edition_and_preserves_predecessor(graph, published_graph):
    from process import provider_directory_named_edition as named

    connection, options = graph.connection, published_graph.options
    store = storage.GraphStore(graph.store.directory, {**graph.store.source, "source_id": options["source_id"]})
    other = storage.GraphStore(graph.store.directory, {**store.source, "_endpoint_dataset_id": "other"})
    await store.prepare([])
    await other.prepare([])
    state = store.table("state")
    with pytest.raises(ValueError, match="cleanup_candidate_changed"):
        await storage.cleanup_named_published_graph(connection, **options)
    await connection.execute(f"UPDATE {state} SET complete=true")
    async with connection.transaction():
        with pytest.raises(ValueError, match="cleanup_publication_required"):
            await storage.cleanup_named_published_graph(connection, **options)
    for change in ({"dataset_id": "old"}, {"acquisition_root_run_id": "wrong"}, {"dataset_hash": "b" * 64}):
        with pytest.raises(ValueError, match="cleanup_publication_required"):
            await storage.cleanup_named_published_graph(connection, **(options | change))

    async def remaining_tables(candidate):
        return await connection.fetchval(
            "SELECT count(*) FROM pg_tables WHERE schemaname=$1 AND starts_with(tablename,$2)",
            graph.schema,
            candidate.prefix + "_",
        )

    assert await remaining_tables(store) == await remaining_tables(other) == 9
    await storage.cleanup_named_published_graph(connection, **options)
    await storage.cleanup_named_published_graph(connection, **options)
    assert await remaining_tables(store) == 0 and await remaining_tables(other) == 9
    assert await connection.fetchval(f'SELECT dataset_id FROM "{published_graph.prior}".{named.HEADER}') == "old"
    assert (
        await connection.fetchval(
            f'SELECT previous_dataset_id FROM "{options["catalog_schema"]}".{named.POINTER_TABLE}'
        )
        == "old"
    )
