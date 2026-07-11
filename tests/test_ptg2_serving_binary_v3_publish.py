# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import config as ptg_config
from process.ptg_parts import ptg2_manifest_publish as manifest_publish
from process.ptg_parts import ptg2_serving_binary as serving_binary


def _compact_sql(sql):
    return " ".join(sql.split())


def _publish_options(progress_callback=None):
    return serving_binary._V3PublishOptions(
        schema_name="mrf",
        source_table="ptg2_serving_source",
        target_table="ptg2_serving_binary_target",
        expected_row_count=4,
        price_set_atom_table="ptg2_price_set_atom",
        price_atom_table="ptg2_price_atom",
        source_layout=serving_binary.PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
        code_count_table=None,
        provider_set_dictionary_table=None,
        price_atom_table_layout="lean_dict_v2",
        price_atom_constant_keys={"setting_key": 0},
        expected_price_set_count=2,
        progress_callback=progress_callback,
    )


@pytest.mark.parametrize("alias", ["postgres_binary_v3", "db_binary_v3", "binary_v3"])
def test_v3_config_aliases(monkeypatch, alias):
    monkeypatch.setenv(ptg_config.PTG2_SNAPSHOT_ARCH_ENV, alias)
    arch_version = ptg_config._ptg2_snapshot_arch_from_env()
    assert arch_version == ptg_config.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3
    assert ptg_config._is_postgres_binary_snapshot_arch(arch_version)
    assert ptg_config._is_postgres_binary_v3_arch(arch_version)
    assert ptg_config._uses_postgres_binary_provider_membership_graph(arch_version)


def test_v3_sql_uses_shared_maps():
    """V3 assigns dense keys once and omits a second provider/code source scan."""
    forward_sql = serving_binary._v3_assigned_by_code_sql(
        qualified_source='"mrf"."serving"',
        qualified_price_key_map='"mrf"."price_map"',
        source_layout=serving_binary.PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
        qualified_code_count_table=None,
        qualified_provider_set_dictionary_table=None,
    )
    dictionary_sql = serving_binary._v3_price_dictionary_sql(qualified_price_key_map='"mrf"."price_map"')
    membership_sql = serving_binary._v3_price_membership_sql(
        qualified_price_set_atom_table='"mrf"."price_set_atom"',
        qualified_price_key_map='"mrf"."price_map"',
        qualified_atom_key_map='"mrf"."atom_map"',
    )
    atom_sql = serving_binary._v3_price_atom_sql(
        qualified_price_atom_table='"mrf"."price_atom"',
        qualified_atom_key_map='"mrf"."atom_map"',
        constant_key_by_column={"setting_key": 7},
    )

    assert _compact_sql(forward_sql) == (
        'SELECT serving.code_key, serving.provider_set_key, serving.provider_count, '
        'price_key_map.price_key FROM "mrf"."serving" serving JOIN "mrf"."price_map" '
        'price_key_map ON price_key_map.price_set_global_id_128 = '
        'serving.price_set_global_id_128 ORDER BY serving.code_key, '
        'serving.provider_set_key, price_key_map.price_key'
    )
    assert _compact_sql(dictionary_sql) == (
        'SELECT price_key, price_set_global_id_128 FROM "mrf"."price_map" '
        'ORDER BY price_set_global_id_128'
    )
    assert _compact_sql(membership_sql) == (
        'SELECT DISTINCT price_key_map.price_key, atom_key_map.atom_key FROM '
        '"mrf"."price_set_atom" price_set_atom LEFT JOIN "mrf"."price_map" price_key_map '
        'ON price_key_map.price_set_global_id_128 = price_set_atom.price_set_global_id_128 '
        'LEFT JOIN "mrf"."atom_map" atom_key_map ON atom_key_map.price_atom_global_id_128 = '
        'price_set_atom.price_atom_global_id_128 ORDER BY price_key_map.price_key, '
        'atom_key_map.atom_key'
    )
    compact_atom_sql = _compact_sql(atom_sql)
    assert compact_atom_sql.startswith(
        'SELECT atom_key_map.atom_key, price_atom.negotiated_rate, '
        'price_atom.negotiated_type_key::bigint AS negotiated_type_key'
    )
    assert "7::bigint AS setting_key" in compact_atom_sql
    assert compact_atom_sql.endswith("ORDER BY atom_key_map.price_atom_global_id_128")
    stream_sql_by_kind = serving_binary._v3_stream_sql_by_kind(
        _publish_options(),
        serving_binary._v3_stage_tables(_publish_options().target_table),
    )
    assert serving_binary.PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND not in stream_sql_by_kind
    assert len(stream_sql_by_kind) == 4


@pytest.mark.asyncio
async def test_price_map_is_distinct_and_dense(monkeypatch):
    status_statements = []

    async def fake_status(sql):
        status_statements.append(sql)

    async def fake_first(_sql):
        return {
            "row_count": 3,
            "distinct_id_count": 3,
            "distinct_key_count": 3,
            "minimum_key": 0,
            "maximum_key": 2,
        }

    monkeypatch.setattr(serving_binary.db, "status", fake_status)
    monkeypatch.setattr(
        serving_binary.db,
        "scalar",
        AsyncMock(side_effect=AssertionError("dense map validation must not rescan with scalar")),
    )
    monkeypatch.setattr(serving_binary.db, "first", fake_first)

    summary = await serving_binary._create_v3_price_key_stage(
        schema_name="mrf",
        price_set_atom_table="price_set_atom_source",
        stage_table="price_key_map",
        expected_price_set_count=3,
    )

    assert "SET (n_distinct = 3)" in _compact_sql(status_statements[0])
    assert _compact_sql(status_statements[1]) == 'ANALYZE "mrf"."price_set_atom_source";'
    create_sql = _compact_sql(status_statements[2])
    assert "CREATE UNLOGGED TABLE" in create_sql
    assert "SELECT DISTINCT price_set_global_id_128" in create_sql
    assert "ROW_NUMBER() OVER (ORDER BY price_set_global_id_128)" in create_sql
    assert "FROM \"mrf\".\"price_set_atom_source\"" in create_sql
    assert sum("CREATE UNIQUE INDEX" in sql for sql in status_statements) == 1
    assert summary["maximum_key"] == 2


@pytest.mark.asyncio
async def test_v3_streams_start_together(monkeypatch):
    calls = []
    all_started = asyncio.Event()

    async def fake_stream(**kwargs):
        calls.append(kwargs)
        if len(calls) == 4:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=1)
        return {"artifact_kind": kwargs["kind"]}

    monkeypatch.setattr(serving_binary, "_serving_binary_stream_tasks", lambda: 5)
    monkeypatch.setattr(serving_binary, "_stream_serving_binary_copy", fake_stream)
    stream_kinds = (
        serving_binary.PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND,
        serving_binary.PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND,
        serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
        serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
    )

    await serving_binary._run_v3_streams(
        sql_by_kind={kind: f"SELECT '{kind}'" for kind in stream_kinds},
        schema_name="mrf",
        target_table="binary",
        target_copy_format="binary",
        atom_count=10,
        atom_key_bits=24,
    )

    assert {call["kind"] for call in calls} == set(stream_kinds)
    assert {call["source_copy_format"] for call in calls} == {"binary"}
    options_by_kind = {call["kind"]: tuple(call["encoder_options"]) for call in calls}
    assert options_by_kind[serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND] == (
        "24",
    )
    assert options_by_kind[serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND] == ("24",)
    assert options_by_kind[serving_binary.PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND] == ()


def _stream_summary_by_kind():
    provider_code_summary_by_field = {
        "artifact_kind": serving_binary.PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
        "row_count": 3,
        "pair_count": 3,
        "code_count": 3,
        "duplicate_pair_count": 0,
        "provider_set_count": 2,
    }
    return {
        serving_binary.PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
            "row_count": 4, "group_count": 3, "price_set_count": 2,
            "maximum_price_key": 1, "price_key_upper_bound": 2, "provider_set_count": 2,
            "provider_set_codes": provider_code_summary_by_field,
        },
        serving_binary.PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
            "row_count": 2, "price_set_count": 2, "id_bytes": 16,
        },
        serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
            "row_count": 3, "atom_reference_count": 3, "price_set_count": 2,
            "maximum_price_key": 1, "atom_key_bits": 24, "atom_key_bytes": 3,
        },
        serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND: {
            "artifact_kind": serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
            "atom_count": 2, "attribute_count": 7, "atom_key_bits": 24, "atom_key_bytes": 3,
        },
    }


@pytest.mark.asyncio
async def test_v3_finish_uses_stream_summaries_without_source_precounts(monkeypatch):
    summary_by_kind = _stream_summary_by_kind()
    reject_db_scan = AsyncMock(side_effect=AssertionError("v3 finish must not pre-count sources"))
    for method_name in ("all", "first", "scalar", "status"):
        monkeypatch.setattr(serving_binary.db, method_name, reject_db_scan)
    monkeypatch.setattr(
        serving_binary,
        "_run_v3_streams",
        AsyncMock(return_value=summary_by_kind),
    )
    finalize = AsyncMock(return_value={"arch_version": "postgres_binary_v3"})
    monkeypatch.setattr(serving_binary, "_finalize_v3_serving_binary", finalize)
    dense_stat_by_name = {
        "row_count": 2,
        "distinct_id_count": 2,
        "distinct_key_count": 2,
        "minimum_key": 0,
        "maximum_key": 1,
    }

    publish_summary = await serving_binary._finish_v3_serving_binary(
        publish_options=_publish_options(),
        stage_tables=serving_binary._v3_stage_tables(_publish_options().target_table),
        price_map_stats=dense_stat_by_name,
        atom_map_stats=dense_stat_by_name,
        timing_by_stage={},
        started_at=0.0,
    )

    assert publish_summary["arch_version"] == "postgres_binary_v3"
    build_state = finalize.await_args.args[1]
    assert build_state.provider_stats["pair_count"] == 3
    assert build_state.membership_stats["atom_reference_count"] == 3
    assert reject_db_scan.await_count == 0


@pytest.mark.asyncio
async def test_v3_stage_cleanup_allows_retry(monkeypatch):
    dropped_stage_pairs = []
    map_build_attempts = []

    async def fake_drop(*, schema_name, stage_tables):
        dropped_stage_pairs.append((schema_name, stage_tables))

    async def fake_maps(*_args):
        map_build_attempts.append(1)
        if len(map_build_attempts) == 1:
            raise RuntimeError("map build failed")
        dense_stat_by_name = {
            "row_count": 2,
            "distinct_id_count": 2,
            "distinct_key_count": 2,
            "minimum_key": 0,
            "maximum_key": 1,
        }
        return dense_stat_by_name, dense_stat_by_name

    monkeypatch.setattr(serving_binary, "_drop_v3_stage_tables", fake_drop)
    monkeypatch.setattr(serving_binary, "_create_v3_stage_maps", fake_maps)
    monkeypatch.setattr(serving_binary, "create_ptg2_serving_binary_table", AsyncMock())
    monkeypatch.setattr(
        serving_binary,
        "_finish_v3_serving_binary",
        AsyncMock(return_value={"arch_version": "postgres_binary_v3"}),
    )

    with pytest.raises(RuntimeError, match="map build failed"):
        await serving_binary._write_v3_serving_binary(_publish_options())
    retry_summary = await serving_binary._write_v3_serving_binary(_publish_options())

    assert retry_summary["arch_version"] == "postgres_binary_v3"
    assert len(dropped_stage_pairs) == 4
    assert len({pair[1] for pair in dropped_stage_pairs}) == 1


@pytest.mark.asyncio
async def test_v3_failure_never_falls_back(monkeypatch):
    dropped_targets = []

    async def fail_v3(_publish_options):
        raise RuntimeError("v3 stream failed")

    async def capture_cleanup(**kwargs):
        dropped_targets.append((kwargs["schema_name"], kwargs["target_table"]))

    async def reject_v2(**_kwargs):
        raise AssertionError("v3 failure must not invoke a v1/v2 writer")

    monkeypatch.setattr(serving_binary, "_serving_binary_rust_enabled", lambda: True)
    monkeypatch.setattr(serving_binary, "_use_serving_binary_stream", lambda: True)
    monkeypatch.setattr(serving_binary, "_ptg2_rust_scanner_binary", lambda: "scanner")
    monkeypatch.setattr(serving_binary, "_write_v3_serving_binary", fail_v3)
    monkeypatch.setattr(serving_binary, "_cleanup_v3_target", capture_cleanup)
    monkeypatch.setattr(serving_binary, "_write_serving_binary_rust_stream", reject_v2)
    monkeypatch.setattr(serving_binary, "_write_ptg2_serving_binary_table_python", reject_v2)

    with pytest.raises(RuntimeError, match="v3 stream failed"):
        await serving_binary.write_ptg2_serving_binary_table(
            schema_name="mrf",
            source_table="serving",
            target_table="binary",
            expected_row_count=1,
            price_set_atom_table="price_set_atom",
            price_atom_table="price_atom",
            arch_version="postgres_binary_v3",
            price_atom_table_layout="lean_dict_v2",
        )

    assert dropped_targets == [("mrf", "binary")]


@pytest.mark.asyncio
@pytest.mark.parametrize("arch_version", ["postgres_binary_v2", "postgres_binary_v3"])
async def test_multi_file_scope_dedupes_before_index(monkeypatch, tmp_path, arch_version):
    statements = []
    copied_paths = []
    scope_rows = []

    first_scope = tmp_path / "scope-a.copy"
    second_scope = tmp_path / "scope-b.copy"
    first_scope.write_text("1234567890\n1234567891\n", encoding="ascii")
    second_scope.write_text("1234567890\n", encoding="ascii")

    async def fake_copy(copy_path, *, target_table, columns):
        copied_paths.append(copy_path)
        assert target_table == "ptg2_provider_npi_scope_snapshot"
        assert columns == manifest_publish.PTG2_MANIFEST_PROVIDER_NPI_SCOPE_COLUMNS
        scope_rows.extend(int(value) for value in copy_path.read_text(encoding="ascii").splitlines())

    async def fake_exact_rows(_schema_name, _table_name):
        return len(scope_rows)

    async def fake_status(sql):
        statements.append(sql)
        if "DELETE FROM" in sql:
            scope_rows[:] = list(dict.fromkeys(scope_rows))
        if "CREATE UNIQUE INDEX" in sql:
            assert len(scope_rows) == len(set(scope_rows))

    assert ptg_config._uses_postgres_binary_provider_membership_graph(arch_version)
    monkeypatch.setattr(manifest_publish, "_copy_ptg2_manifest_file", fake_copy)
    monkeypatch.setattr(manifest_publish, "_exact_table_rows", fake_exact_rows)
    monkeypatch.setattr(manifest_publish.db, "status", fake_status)

    await manifest_publish._copy_provider_npi_scope_file(
        first_scope,
        target_table="ptg2_provider_npi_scope_snapshot",
    )
    await manifest_publish._copy_provider_npi_scope_file(
        second_scope,
        target_table="ptg2_provider_npi_scope_snapshot",
    )

    metrics = await manifest_publish._finalize_provider_npi_scope_table(
        "mrf",
        "ptg2_provider_npi_scope_snapshot",
    )

    assert metrics == {"before": 3, "after": 2, "dropped": 1}
    assert copied_paths == [first_scope, second_scope]
    assert "ROW_NUMBER() OVER (PARTITION BY npi ORDER BY ctid)" in statements[0]
    assert "DELETE FROM" in statements[0]
    assert "CREATE UNIQUE INDEX" in statements[1]
    assert statements[1].index("CREATE UNIQUE INDEX") >= 0


@pytest.mark.asyncio
async def test_finalize_rejects_unlogged_table(monkeypatch):
    monkeypatch.setattr(serving_binary.db, "status", AsyncMock())
    monkeypatch.setattr(
        serving_binary.db,
        "first",
        AsyncMock(return_value={"relation_persistence": "unlogged"}),
    )

    with pytest.raises(RuntimeError, match="is not logged"):
        await serving_binary.finalize_ptg2_serving_binary_table(
            schema_name="mrf",
            target_table="ptg2_serving_binary_snapshot",
        )


def test_v3_required_artifacts_exclude_v2_reverse():
    assert serving_binary.PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND not in (
        serving_binary._V3_REQUIRED_ARTIFACT_KINDS
    )
    assert serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND not in (
        serving_binary._V3_REQUIRED_ARTIFACT_KINDS
    )


def test_v3_always_drops_transient_serving_table(monkeypatch):
    monkeypatch.setattr(manifest_publish, "_ptg2_manifest_drop_serving_table_after_sidecars", lambda: False)
    assert manifest_publish._should_drop_manifest_serving_table("postgres_binary_v3")
    assert not manifest_publish._should_drop_manifest_serving_table("postgres_binary_v2")


def test_v3_serving_manifest_promotes_atom_key_width():
    serving_binary_manifest_by_field = {"dense_atom_keys": {"atom_count": 7, "atom_key_bits": 24}}

    assert manifest_publish._serving_binary_atom_key_bits(serving_binary_manifest_by_field) == 24


def test_v3_requires_lean_serving_layout(monkeypatch):
    monkeypatch.setattr(manifest_publish, "_ptg2_manifest_serving_layout", lambda: "")
    with pytest.raises(RuntimeError, match="lean provider-key"):
        manifest_publish._require_v3_serving_layout("postgres_binary_v3")


_GRAPH_NAMES = ("provider_forward", "provider_inverted", "provider_group_npi", "provider_npi_group")


async def _published_v3_graph_manifest(monkeypatch, tmp_path):
    sidecars = []
    for name in (*_GRAPH_NAMES, "price_forward", "serving_by_code", "serving_by_provider_set"):
        path = tmp_path / f"{name}.ptg2sc"
        path.write_bytes(name.encode("ascii"))
        sidecars.append(
            {"name": name, "path": str(path), "sha256": f"sha-{name}", "byte_count": path.stat().st_size}
        )
    uploaded_names = []

    async def fake_store(_path, **kwargs):
        metadata = dict(kwargs["metadata"])
        artifact_name = kwargs["name"]
        uploaded_names.append(artifact_name)
        metadata.update({"artifact_id": f"db-{artifact_name}", "storage_uri": f"db://ptg2_artifact/db-{artifact_name}"})
        return metadata

    monkeypatch.setattr(manifest_publish, "ptg2_artifact_db_store_enabled", lambda: False)
    monkeypatch.setattr(manifest_publish, "store_ptg2_artifact_file_in_db", fake_store)
    retained = manifest_publish._retain_v3_provider_graph_artifacts({"sidecars": sidecars})
    stored = await manifest_publish._store_ptg2_manifest_sidecar_artifacts_in_db(
        schema_name="mrf", snapshot_id="ptg2:v3-test", sidecar_artifacts=retained, require_db_storage=True,
    )
    db_graph = manifest_publish._require_v3_graph_db_artifacts(stored)
    artifact_manifest = manifest_publish._ptg2_manifest_artifacts_manifest(
        artifacts={"source_trace_set_hash": "trace"}, sidecar_artifacts=db_graph,
    )
    graph_by_name = {entry["name"]: entry for entry in artifact_manifest["sidecars"]}
    assert set(graph_by_name) == set(_GRAPH_NAMES)
    assert set(uploaded_names) == set(_GRAPH_NAMES)
    assert all(entry["storage_uri"].startswith("db://ptg2_artifact/") for entry in graph_by_name.values())
    assert all("path" not in entry and "cache_path" not in entry for entry in graph_by_name.values())
    return artifact_manifest


@pytest.mark.asyncio
async def test_v3_manifest_db_graph_resolves_npi_without_local_files(monkeypatch, tmp_path):
    from api import ptg2_serving as api_serving

    artifact_manifest = await _published_v3_graph_manifest(monkeypatch, tmp_path)
    npi = 1234567890
    group_id = "11" * 16
    provider_set_id = "22" * 16
    lookup_names = []

    async def fake_db_lookup(_session, entry, owners, **_kwargs):
        assert "path" not in entry
        lookup_names.append(entry["name"])
        owner_bytes = tuple(bytes.fromhex(owner) if isinstance(owner, str) else bytes(owner) for owner in owners)
        member_id = group_id if entry["name"] == "provider_npi_group" else provider_set_id
        return {owner: (bytes.fromhex(member_id),) for owner in owner_bytes}

    monkeypatch.setattr(api_serving, "lookup_global_sidecar_members_many_from_db", fake_db_lookup)
    monkeypatch.setattr(
        api_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args: (_ for _ in ()).throw(AssertionError("v3 must not resolve local files")),
    )
    api_serving._PTG2_MANIFEST_SIDECAR_CACHE.clear()
    serving_tables = SimpleNamespace(
        artifacts=artifact_manifest,
        provider_npi_scope_table="mrf.ptg2_provider_npi_scope_v3",
        uses_sidecar_provider_scope=True,
        effective_arch_version="postgres_binary_v3",
    )

    assert api_serving._has_membership_graph(serving_tables)
    resolved_provider_sets = await api_serving._provider_sets_from_membership_graph(
        object(), serving_tables, npi
    )

    assert resolved_provider_sets == (provider_set_id,)
    assert lookup_names == ["provider_npi_group", "provider_inverted"]
