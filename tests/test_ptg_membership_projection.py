# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded membership admission preserves the original priced response path."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_imports, control_workers
from api import ptg2_serving as serving
from api import ptg_membership_reader as reader
from api import ptg_membership_storage as storage
from api.ptg_membership_contract import IMPORTER, canonical_code, validate_params
from api.ptg_membership_storage import attachment_name
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

PARAMS = {"source_key": "synthetic-network", "snapshot_id": "synthetic-snapshot", "code_system": "CPT", "code": "99213"}


def test_fixed_importer_uses_existing_audit_single_job_contract():
    entry = next(entry for entry in control_imports.importer_registry() if entry["name"] == IMPORTER)
    assert entry["family"] == "mrf"
    assert entry["queue"] == "arq:PTGCandidateAudit"
    assert {param["name"] for param in entry["params_schema"]} == set(PARAMS)
    assert entry["cancelable"] is False
    assert IMPORTER in control_imports.ALL_STATUS_IDEMPOTENCY_IMPORTERS
    adapter = control_imports._SINGLE_JOB_ADAPTERS[IMPORTER]
    assert adapter["target_function"] == "build_provider_membership_projection"
    worker = next(spec for spec in control_workers._START_WORKERS if spec.worker_class == "process.PTGCandidateAudit")
    assert IMPORTER in worker.importers
    assert (
        control_workers._single_job_worker_target(worker, {"run_id": "run", "importer": IMPORTER})
        == "ptg_provider_membership_projection_run"
    )


@pytest.mark.parametrize(
    "changed",
    [
        {"schema": "other"},
        {"graph_root": "other"},
        {"plan_release_id": "other"},
        {"snapshot_id": ""},
        {"source_key": " padded "},
        {"code_system": "cpt"},
        {"code": " 99213"},
    ],
)
def test_closed_physical_run_rejects_overrides_and_noncanonical_input(changed):
    with pytest.raises(ValueError):
        validate_params(PARAMS | changed)


def test_attachment_identifier_contains_full_digest_within_native_name_limit():
    validate_params(PARAMS)
    assert canonical_code("CPT", "99213") == ("CPT", "99213")
    name = attachment_name(PARAMS)
    assert len(name.encode("ascii")) == 55
    assert name != attachment_name(PARAMS | {"snapshot_id": "next-snapshot"})


def _missing_source_catalog(parameters):
    assert parameters == {
        "source_header": None,
        "bindings": None,
        "header": '"synthetic_schema".plan_release_serving_revision',
    }
    return SimpleNamespace(
        all=lambda: [{"source_header_exists": False, "bindings_exist": False, "header_exists": False}]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blocking_group",
    [
        None,
        "global",
        "source",
        "plan",
        "previous_global",
        "previous_source",
        "previous_plan",
        "pin",
        "release_binding",
        "release_pin",
    ],
)
async def test_retirement_reads_all_reference_groups_and_refuses_every_external_reference(monkeypatch, blocking_group):
    """Use the shared loader, including default snapshot scope and ordered release pins."""
    monkeypatch.setattr(serving, "PTG2_SCHEMA", "synthetic_schema")
    monkeypatch.delenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", raising=False)
    own_pin = storage._pin(PARAMS)
    queries = []
    catalog_queries = []
    specs = [
        ("global", "ptg2_current_snapshot", "slot", "snapshot_id", "slot"),
        ("source", "ptg2_current_source_snapshot", "source_key", "snapshot_id", "source_key"),
        ("plan", "ptg2_current_plan_source", "plan_source_key", "snapshot_id", "plan_source_key"),
        ("previous_global", "ptg2_current_snapshot", "slot", "previous_snapshot_id", "slot"),
        ("previous_source", "ptg2_current_source_snapshot", "source_key", "previous_snapshot_id", "source_key"),
        ("previous_plan", "ptg2_current_plan_source", "plan_source_key", "previous_snapshot_id", "plan_source_key"),
        ("pin", "ptg2_snapshot_pin", "owner_type, owner_id", "snapshot_id", "owner_type, owner_id"),
        (
            "release_binding",
            "plan_release_snapshot_binding",
            "serving_revision_id, role, binding_ordinal",
            "snapshot_id",
            "serving_revision_id, role, binding_ordinal",
        ),
        (
            "release_pin",
            "plan_release_snapshot_pin",
            "'plan_release_serving_revision'::text AS owner_type, serving_revision_id AS owner_id",
            "snapshot_id",
            "serving_revision_id",
        ),
    ]

    async def execute(statement, parameters):
        query = str(statement)
        if "AS source_header_exists" in query:
            catalog_queries.append(query)
            return _missing_source_catalog(parameters)
        group, table, selected, reference, order = specs[len(queries)]
        assert query == (
            f'SELECT {selected} FROM "synthetic_schema"."{table}" WHERE "{reference}"=:snapshot_id ORDER BY {order}'
        )
        assert parameters == {"snapshot_id": PARAMS["snapshot_id"]}
        queries.append(query)
        rows = [own_pin] if group == "pin" else []
        if group == blocking_group:
            rows.append({"owner_type": "plan_release_serving_revision", "owner_id": "synthetic-release"})
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    if blocking_group is None:
        assert await storage._require_no_references(session, PARAMS) == own_pin
    else:
        with pytest.raises(PTG2ManifestArtifactError, match="retention references"):
            await storage._require_no_references(session, PARAMS)
    assert len(queries) == len(specs)
    assert len(catalog_queries) == 1


def _request(**overrides):
    budget = serving.ForwardReadBudget(
        maximum_fragments=256, maximum_raw_payload_bytes=4194304, maximum_row_capacity=6701
    )
    limits = serving._V4GeoRateForwardLimits(65536, 131072, 6700, 6600, 32, budget)
    request = serving._LocalDistanceGraphRequest(100, [{"code_key": 7}], limits, "CPT", "99213")
    return replace(request, **overrides)


class _Result:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one(self):
        return self.row


def _index():
    return (
        SimpleNamespace(schema_name="reference_family_archive_0123456789abcdef0123456789abcdef"),
        {
            "counts": {"code_coverage": [{"code_key": 7}]},
        },
        12,
        13,
        {},
    )


async def _prepare(monkeypatch, row, *, found=None):
    resolve = AsyncMock(return_value=(object(), object(), PARAMS))
    locate = AsyncMock(return_value=_index() if found is None else found)
    keys = AsyncMock(side_effect=lambda _session, **kw: {npi: index for index, npi in enumerate(kw["npis"])})
    monkeypatch.setattr(reader, "source_identity", resolve)
    monkeypatch.setattr(reader, "locate", locate)
    monkeypatch.setattr(reader, "v4_npi_keys_for_values", keys)
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(row)))
    return session, resolve, locate


@pytest.mark.asyncio
async def test_index_intersects_complete_code_scope_and_preserves_missing_npis(monkeypatch):
    projection_by_field = {"first_count": 2, "priced_count": 2, "pairs": [[1000000001, 31], [1000000001, 32]]}
    session, resolve, locate = await _prepare(monkeypatch, projection_by_field)
    state = serving._LocalDistanceGraphState()
    tables = SimpleNamespace(source_key="synthetic-network", snapshot_id="synthetic-snapshot", shared_snapshot_key=1)
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "mrf")
    legacy = AsyncMock(side_effect=AssertionError("an exact ready index must not expand all-procedure graph pages"))
    monkeypatch.setattr(serving, "_v4_sets_by_npi", legacy)
    actual = await serving._local_v4_memberships(session, tables, (1000000001, 1000000002), _request(), state)
    assert actual == {1000000001: (31, 32), 1000000002: ()}
    assert state.retained_memberships == 2
    assert state.indexed_projection_members == 6
    assert state.code_sets == {31, 32}
    assert ":code_keys" in str(session.execute.call_args.args[0])
    assert session.execute.call_args.args[1]["code_keys"] == [7]
    resolve.assert_awaited_once()
    locate.assert_awaited_once()
    legacy.assert_not_awaited()
    classifier = AsyncMock(
        side_effect=AssertionError("already authenticated code membership must not reclassify broad sets")
    )
    monkeypatch.setattr(serving, "_scoped_rate_provider_set_keys", classifier)
    await serving._classify_local_code_sets(session, tables, actual, _request(), state)
    classifier.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_absent_index_keeps_original_guarded_graph_path(monkeypatch):
    monkeypatch.setattr(reader, "source_identity", AsyncMock(return_value=(None, None, PARAMS)))
    monkeypatch.setattr(reader, "locate", AsyncMock(return_value=None))
    graph = AsyncMock(return_value={1000000001: (17,)})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph)
    result = await serving._local_v4_memberships(
        object(),
        SimpleNamespace(source_key="network", snapshot_id="snapshot"),
        (1000000001,),
        _request(),
        serving._LocalDistanceGraphState(),
    )
    assert result == {1000000001: (17,)}
    graph.assert_awaited_once()


@pytest.mark.asyncio
async def test_present_corrupt_index_never_uses_guarded_fallback(monkeypatch):
    monkeypatch.setattr(reader, "source_identity", AsyncMock(return_value=(None, None, PARAMS)))
    monkeypatch.setattr(reader, "locate", AsyncMock(side_effect=PTG2ManifestArtifactError("custody differs")))
    graph = AsyncMock()
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph)
    with pytest.raises(PTG2ManifestArtifactError, match="custody"):
        await serving._local_v4_memberships(
            object(),
            SimpleNamespace(source_key="network", snapshot_id="snapshot"),
            (1000000001,),
            _request(),
            serving._LocalDistanceGraphState(),
        )
    graph.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["retained", "projection", "sets", "bytes", "batches", "coverage"])
async def test_native_path_keeps_cumulative_admission_caps(monkeypatch, kind):
    projection_by_field = {"first_count": 1, "priced_count": 1, "pairs": [[1000000001, 31]]}
    session, _, _ = await _prepare(monkeypatch, projection_by_field)
    state = serving._LocalDistanceGraphState()
    request = _request()
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "mrf")
    if kind == "retained":
        state.retained_memberships = request.forward_limits.maximum_retained_memberships
    elif kind == "projection":
        state.indexed_projection_members = request.forward_limits.maximum_projection_members - 2
    elif kind == "sets":
        state.code_sets = set(range(100, 6700))
    elif kind == "coverage":
        request = replace(request, code_rows=[{"code_key": 999}])
    max_bytes = 0 if kind == "bytes" else 4194304
    max_batches = 0 if kind == "batches" else 32
    with serving.v4_graph_taxonomy_projection_scope(
        maximum_members=131072, maximum_pages=256, maximum_bytes=max_bytes, maximum_batches=max_batches
    ):
        with pytest.raises(
            (serving.PTG2OnlineWorkBudgetExceeded, PTG2ManifestArtifactError, serving.PTG2SharedBlockError)
        ):
            await reader.indexed_memberships(
                session, SimpleNamespace(source_key="network", snapshot_id="snapshot"), (1000000001,), request, state
            )


@pytest.mark.asyncio
async def test_distance_prefix_keeps_later_addresses_without_reloading_npi_membership(monkeypatch):
    locations = [
        {"npi": 1000000001, "location_hash": "a", "distance_miles": 1},
        {"npi": 1000000002, "location_hash": "b", "distance_miles": 2},
        {"npi": 1000000001, "location_hash": "c", "distance_miles": 3},
        {"npi": 1000000002, "location_hash": "d", "distance_miles": 4},
    ]
    probes = []

    async def location_rows(*_args, limit, **_kwargs):
        probes.append(limit)
        return locations[:limit]

    projection_by_field = {"first_count": 2, "priced_count": 2, "pairs": [[1000000001, 31], [1000000002, 32]]}
    session, _, _ = await _prepare(monkeypatch, projection_by_field)
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "mrf")
    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    tables = SimpleNamespace(source_key="network", snapshot_id="snapshot")
    candidates = await serving._scan_local_distance_graph(
        session,
        tables,
        {},
        replace(_request(), candidate_limit=4),
        2,
        20,
        candidate_npis=(1000000001, 1000000002),
    )
    assert candidates.location_rows == locations
    assert candidates.provider_set_keys_by_npi == {1000000001: {31}, 1000000002: {32}}
    assert len(probes) == 2
    assert session.execute.await_count == 1


def test_same_npi_with_changed_address_cannot_pass_the_prefix_proof():
    state = serving._LocalDistanceGraphState()
    serving._new_local_taxonomy_locations([{"npi": 1000000001, "location_hash": "first", "distance_miles": 1}], state)
    with pytest.raises(PTG2ManifestArtifactError, match="prefix changed"):
        serving._new_local_taxonomy_locations(
            [{"npi": 1000000001, "location_hash": "replacement", "distance_miles": 1}], state
        )


@pytest.mark.asyncio
async def test_source_custody_is_fresh_with_supplied_descriptor_and_actual_audit_digest(monkeypatch):
    from api import ptg_membership_source as source
    from api.ptg2_v4_graph import V4GraphRoot

    tables = SimpleNamespace(
        source_key="synthetic-network", snapshot_id="synthetic-snapshot", physical_binding=None, uses_v4_graph=True
    )
    resolver = AsyncMock(return_value=tables)
    root = AsyncMock(return_value=V4GraphRoot(1, "pattern_v1", b"x" * 32))
    monkeypatch.setattr(source, "snapshot_serving_tables", resolver)
    monkeypatch.setattr(source, "load_v4_graph_root", root)
    monkeypatch.setattr(source, "has_complete_v4_finalizer_map", AsyncMock(return_value=False))
    monkeypatch.setattr(serving, "_require_strict_shared_v3", lambda _tables: None)
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "synthetic")
    names = sorted(
        {
            "ptg2_v4_snapshot_map_root",
            "ptg2_v4_relation_manifest",
            "ptg2_v4_snapshot_map_pack",
            "ptg2_v3_block",
            "ptg2_v3_snapshot_block",
            "ptg2_v4_npi_scope",
            "ptg2_v3_code",
            "ptg2_v3_provider_set",
            "ptg2_v3_provider_group",
        }
    )
    storage_rows = [
        {"database_oid": 42, "schema_oid": 43, "relname": name, "relation_oid": index + 100}
        for index, name in enumerate(names)
    ]
    storage_result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: storage_rows))
    session = SimpleNamespace(execute=AsyncMock(return_value=storage_result), scalar=AsyncMock(return_value="ab" * 32))
    _, _, identity = await source.source_identity(session, **PARAMS, tables=tables)
    resolver.assert_awaited_once_with(session, PARAMS["snapshot_id"])
    root.assert_awaited_once_with(session, 1, schema_name="synthetic", refresh=True)
    assert identity["source_storage"]["attestation_digest"] == "ab" * 32
    assert identity["source_storage"]["relation_oids"] == [[name, index + 100] for index, name in enumerate(names)]
    assert identity["finalizer"] == {
        "state": "legacy_relational",
        "map_digest": None,
        "canonical_mapping_digest": None,
        "target_identity_digest": None,
    }
    session.scalar.return_value = None
    with pytest.raises(PTG2ManifestArtifactError, match="audit attestation"):
        await source.source_identity(session, **PARAMS, tables=tables)


@pytest.mark.asyncio
async def test_explicit_root_refresh_does_not_authorize_from_a_warm_cache(monkeypatch):
    from collections import OrderedDict

    from api import ptg2_v4_graph as graph

    monkeypatch.setattr(graph, "_ROOT_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_graph_cache_scope", lambda *_args: "synthetic-database")
    fields_by_name = {
        "snapshot_key": 1,
        "representation": "pattern_v1",
        "map_digest": b"a" * 32,
        "format_version": graph.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": graph.PTG2_V4_MAP_FORMAT,
        "projection_id_scope": graph.PTG2_V4_PROJECTION_ID_SCOPE,
    }
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=lambda *_args: SimpleNamespace(first=lambda: dict(fields_by_name)))
    )
    assert (await graph.load_v4_graph_root(session, 1, schema_name="synthetic")).map_digest == b"a" * 32
    fields_by_name["map_digest"] = b"b" * 32
    assert (await graph.load_v4_graph_root(session, 1, schema_name="synthetic")).map_digest == b"a" * 32
    assert (await graph.load_v4_graph_root(session, 1, schema_name="synthetic", refresh=True)).map_digest == b"b" * 32
    assert session.execute.await_count == 2


@pytest.mark.asyncio
async def test_readiness_get_has_exact_absent_shape_and_is_authenticated(monkeypatch):
    from contextlib import asynccontextmanager

    from sanic.request import RequestParameters

    from api import control_ptg_membership as control

    routes_by_path = {}

    class Blueprint:
        def get(self, path):
            def register(handler):
                routes_by_path[path] = handler
                return handler

            return register

    session = object()

    @asynccontextmanager
    async def transaction():
        yield session

    auth = SimpleNamespace(calls=0)

    def require_auth(_request):
        auth.calls += 1

    resolve = AsyncMock(return_value=(None, None, dict(PARAMS, contract="ptg_provider_membership_v1")))
    monkeypatch.setattr(control, "require_control_auth", require_auth)
    monkeypatch.setattr(control.db, "transaction", transaction)
    monkeypatch.setattr(control, "source_identity", resolve)
    monkeypatch.setattr(control, "locate", AsyncMock(return_value=None))
    control.register_membership_projection_routes(Blueprint())
    assert set(routes_by_path) == {"/ptg/provider-membership-projection"}
    handler = routes_by_path["/ptg/provider-membership-projection"]
    response = await handler(
        SimpleNamespace(args=RequestParameters({name: [parameter_value] for name, parameter_value in PARAMS.items()}))
    )
    import json

    assert json.loads(response.body) == dict(PARAMS, contract="ptg_provider_membership_v1", state="absent")
    assert auth.calls == 1
    resolve.assert_awaited_once_with(session, **PARAMS)
    monkeypatch.setattr(control, "locate", AsyncMock(side_effect=PTG2ManifestArtifactError("corrupt attachment")))
    with pytest.raises(PTG2ManifestArtifactError, match="corrupt"):
        await handler(
            SimpleNamespace(
                args=RequestParameters({name: [parameter_value] for name, parameter_value in PARAMS.items()})
            )
        )


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "postgres"])
def test_native_membership_fixture_accepts_disposable_ci_database(monkeypatch, host):
    """Allow loopback and the CI service only with an isolated database name."""
    from tests.test_ptg_membership_projection_postgres import _url

    database_name = "hc_ptg_membership_" + "a" * 32
    monkeypatch.setenv("HLTHPRT_PTG_MEMBERSHIP_TEST_DSN", f"postgresql://synthetic@{host}:5432/{database_name}")
    url = _url()
    assert url.host == host
    assert url.database == database_name
    assert url.drivername == "postgresql+asyncpg"


@pytest.mark.parametrize(
    "host,port,database_name",
    [
        ("remote.example", 5432, "hc_ptg_membership_" + "a" * 32),
        ("postgres", 6432, "hc_ptg_membership_" + "a" * 32),
        ("postgres", 5432, "postgres"),
    ],
)
def test_native_membership_fixture_refuses_other_databases(monkeypatch, host, port, database_name):
    """Keep remote hosts, other ports and shared databases outside the fixture."""
    from tests.test_ptg_membership_projection_postgres import _url

    monkeypatch.setenv("HLTHPRT_PTG_MEMBERSHIP_TEST_DSN", f"postgresql://synthetic@{host}:{port}/{database_name}")
    with pytest.raises(pytest.fail.Exception, match="task-owned local PostgreSQL"):
        _url()
