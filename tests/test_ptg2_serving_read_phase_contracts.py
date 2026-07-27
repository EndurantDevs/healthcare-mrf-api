"""Behavioral contracts for the decomposed PTG serving read phases."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving


def _rate_row(**overrides):
    row_by_field = {
        "provider_set_global_id_128": "01" * 16,
        "serving_content_hash_128": "02" * 16,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "ffs",
        "_ptg_provider_set_key": 1,
    }
    row_by_field.update(overrides)
    return row_by_field


def _v4_sources(**overrides):
    source_fields_by_name = {
        "provider_set_key_by_id": {"01" * 16: 1},
        "provider_count_by_id": {"01" * 16: 2},
        "prefix_override_by_id": {},
        "pattern_keys_by_set": None,
        "component_keys_by_set": {},
    }
    source_fields_by_name.update(overrides)
    return serving._V4NpiGroupSources(**source_fields_by_name)


def test_read_phase_normalizers_fail_closed_and_preserve_empty_filters(
    monkeypatch,
):
    """Malformed optional input never broadens a graph or code lookup."""

    assert serving._coerce_int_payload("not-an-integer") is None
    monkeypatch.setattr(serving, "_ptg2_manifest_id", lambda _value: "z" * 32)
    assert serving._ptg2_manifest_id_bytes("malformed") == b""
    assert serving._ptg2_reported_code_lookup_values(None, " 99213 ") == (
        "99213",
    )

    filters: list[str] = []
    parameters_by_name: dict[str, object] = {}
    serving._append_reported_code_value_filter(
        filters,
        parameters_by_name,
        column="code",
        param_name="reported_code",
        values=(),
    )
    serving._append_reported_code_system_filter(
        filters,
        parameters_by_name,
        column="code_system",
        code_system=None,
    )
    assert filters == ["code_system IS NULL"]
    assert parameters_by_name == {}
    assert serving._ptg2_npi_from_member_id("00") is None


def test_address_and_optional_query_fallbacks_are_non_destructive(
):
    """Absent address evidence cannot suppress otherwise valid fields."""

    provider_by_field = {"first_line": "1 Test Way"}
    serving._apply_address_display_policy(provider_by_field, {})
    assert provider_by_field == {"first_line": "1 Test Way"}


@pytest.mark.asyncio
async def test_optional_query_and_directory_fallbacks_preserve_provider_rows(
    monkeypatch,
):
    """Optional relation failures cannot discard otherwise valid providers."""

    class FailingSession:
        @staticmethod
        def rollback():
            raise RuntimeError("optional rollback unavailable")

    await serving._rollback_optional_ptg2_query(SimpleNamespace())
    await serving._rollback_optional_ptg2_query(FailingSession())
    monkeypatch.setattr(serving, "_is_relation_available", AsyncMock(return_value=False))
    assert await serving._ptg2_provider_directory_corroboration_table(object()) is None

    provider_rows = [
        {"npi": object(), "address_key": "11111111-1111-1111-1111-111111111111"}
    ]
    monkeypatch.setattr(
        serving,
        "_ptg2_provider_directory_corroboration_table",
        AsyncMock(return_value="mrf.corroboration"),
    )
    monkeypatch.setattr(
        serving,
        "_provider_directory_corroboration_by_key",
        AsyncMock(return_value={(1, "unused"): {"npi": 1}}),
    )
    assert await serving._overlay_provider_directory_corroboration(
        object(),
        provider_rows,
        plan_id="plan",
    ) == provider_rows


@pytest.mark.asyncio
async def test_optional_relation_probe_and_valid_directory_owner_paths(
    monkeypatch,
):
    """Unavailable optional relations return false; valid owners retain lookup order."""

    class FailingSession:
        @staticmethod
        async def execute(*_args, **_kwargs):
            raise RuntimeError("optional relation unavailable")

    assert not await serving._is_relation_available(
        FailingSession(),
        "mrf.optional_relation",
    )

    member_loader = AsyncMock(return_value={"01" * 16: ("member",)})
    monkeypatch.setattr(serving, "_shared_graph_members_by_id", member_loader)
    assert await serving._shared_graph_members_for_id(
        object(),
        object(),
        "provider_forward",
        "01" * 16,
    ) == ("member",)


@pytest.mark.asyncio
async def test_shared_graph_empty_and_cold_paths_keep_exact_ownership(
    monkeypatch,
):
    """Empty owners stay empty and cold graph reads deduplicate in owner order."""

    serving_tables = SimpleNamespace(uses_shared_blocks=True)
    assert await serving._shared_graph_members_by_id(
        object(),
        serving_tables,
        "provider_forward",
        (),
    ) == {}
    assert await serving._shared_provider_group_ids_for_keys(
        object(),
        object(),
        (),
    ) == {}
    assert await serving._shared_provider_group_keys_for_ids(
        object(),
        object(),
        (),
    ) == {}
    assert await serving._shared_graph_members_for_id(
        object(),
        object(),
        "provider_forward",
        "",
    ) == ()
    assert await serving._manifest_sets_by_group(object(), object(), []) == {}

    graph_reads = AsyncMock(
        side_effect=(
            {"set-a": ("group-1", "group-2"), "set-b": ("group-2",)},
            {"group-1": ("npi-1", "npi-2"), "group-2": ("npi-2", "npi-3")},
        )
    )
    monkeypatch.setattr(serving, "_shared_graph_members_by_id", graph_reads)
    assert await serving._cold_npi_members_by_set(
        object(),
        object(),
        ("set-a", "set-b"),
        limit_per_set=None,
    ) == {
        "set-a": ("npi-1", "npi-2", "npi-3"),
        "set-b": ("npi-2", "npi-3"),
    }
    assert graph_reads.await_count == 2


@pytest.mark.asyncio
async def test_shared_metadata_empty_paths_avoid_io():
    """Empty dictionary lookups avoid I/O and retain exact snapshot filters."""

    serving_tables = SimpleNamespace(
        uses_shared_blocks=True,
        shared_snapshot_key=7,
        uses_v4_graph=True,
    )
    filters: list[str] = []
    parameters_by_name: dict[str, object] = {}
    serving._append_shared_snapshot_filter(
        serving_tables,
        filters,
        parameters_by_name,
        column="code.snapshot_key",
    )
    assert filters == ["code.snapshot_key = :shared_snapshot_key"]
    assert parameters_by_name == {"shared_snapshot_key": 7}
    assert await serving._provider_set_ids_for_keys(
        object(),
        serving_tables,
        (),
    ) == {}
    assert await serving._provider_set_metadata_for_ids(
        object(),
        serving_tables,
        (),
    ) == {}
    await serving._hydrate_provider_set_network_names(
        object(),
        serving_tables,
        [{}],
    )
    projection, join = serving._v4_prefix_query_fragments(serving_tables)
    assert "prefix_member_count" in projection
    assert "LEFT JOIN" in join


@pytest.mark.asyncio
async def test_shared_rate_scope_delegation_preserves_exact_groups(monkeypatch):
    """Rate-scope helpers preserve the exact sealed provider-group identities."""

    serving_tables = SimpleNamespace(
        uses_shared_blocks=True,
        shared_snapshot_key=7,
        uses_v4_graph=True,
    )
    shared_rate_provider_groups = serving._shared_rate_provider_groups
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_groups",
        AsyncMock(return_value=("01" * 16,)),
    )
    scope = await serving._shared_rate_scope(
        object(),
        serving_tables,
        plan_id="plan",
        reported_code="99213",
        code_system="CPT",
    )
    assert scope.id_count == 1
    assert scope.group_id_bytes == frozenset({bytes.fromhex("01" * 16)})

    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        AsyncMock(return_value=(1, 2)),
    )
    monkeypatch.setattr(
        serving,
        "_shared_group_ids_for_set_keys",
        AsyncMock(return_value=("01" * 16, "02" * 16)),
    )
    assert await shared_rate_provider_groups(
        object(),
        serving_tables,
        plan_id="plan",
        reported_code="99213",
        code_system="CPT",
    ) == ("01" * 16, "02" * 16)


def test_v4_prefix_helpers_enforce_stable_bounded_identity():
    """Merged prefixes are unique, bounded, and reject inconsistent seals."""

    assert serving._merge_sorted_group_keys(
        ((1, 2, 4), (1, 3, 5)),
        4,
    ) == (1, 2, 3, 4)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="invalid key"):
        serving._v4_npi_prefix_digest((-1,))

    with pytest.raises(serving.PTG2ManifestArtifactError, match="incomplete"):
        serving._v4_npi_source_metadata({}, ("01" * 16,))

    with pytest.raises(serving.PTG2ManifestArtifactError, match="counts are incomplete"):
        serving._v4_npi_targets_by_set(
            _v4_sources(provider_count_by_id={}),
            2,
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="negative"):
        serving._v4_npi_targets_by_set(
            _v4_sources(provider_count_by_id={"01" * 16: -1}),
            2,
        )

    state = serving._V4NpiPrefixState.for_provider_sets(("01" * 16,))
    state.group_keys_by_set["01" * 16] = (1, 2)
    changed_prefix = serving._V4GroupPrefixRound(
        {"01" * 16: (1, 3)},
        {"01" * 16: False},
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="prefix changed"):
        serving._index_new_v4_groups(changed_prefix, state)


@pytest.mark.asyncio
async def test_v4_relation_reads_reject_partial_dictionary_results(
    monkeypatch,
):
    """Each physical V4 relation must return every authenticated owner."""

    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_member_prefixes",
        AsyncMock(return_value={}),
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="set-component"):
        await serving._load_v4_component_sources(
            object(),
            snapshot_key=1,
            overflow_set_keys=(1,),
            maximum_component_degree=2,
        )
    assert await serving._load_v4_group_sources(
        object(),
        snapshot_key=1,
        provider_set_keys=(),
        maximum_pattern_degree=2,
        maximum_component_degree=2,
    ) == serving._V4SetGroupSources({}, {})
    with pytest.raises(serving.PTG2ManifestArtifactError, match="set-pattern"):
        await serving._load_v4_group_sources(
            object(),
            snapshot_key=1,
            provider_set_keys=(1,),
            maximum_pattern_degree=2,
            maximum_component_degree=2,
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="provider-group"):
        await serving._read_v4_group_prefixes(
            object(),
            1,
            _v4_sources(),
            ("01" * 16,),
            2,
        )

    state = serving._V4NpiPrefixState.for_provider_sets(("01" * 16,))
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(return_value={}),
    )
    state.selected_npi_keys_by_set["01" * 16] = [7]
    with pytest.raises(serving.PTG2ManifestArtifactError, match="dictionary"):
        await serving._resolve_v4_npi_member_ids(
            object(),
            1,
            ("01" * 16,),
            state,
        )


def test_v4_override_metadata_rejects_count_drift():
    """Sparse-prefix metadata must match its authenticated provider count."""

    metadata = serving._ProviderSetGraphMetadata(
        provider_set_key=1,
        provider_count=2,
        prefix_member_count=1,
        prefix_member_digest=b"digest",
    )
    group_sources = _v4_sources(prefix_override_by_id={"01" * 16: metadata})
    with pytest.raises(serving.PTG2ManifestArtifactError, match="count"):
        serving._v4_override_metadata_by_key(group_sources, 2)


@pytest.mark.asyncio
async def test_v4_override_and_online_work_seals_reject_drift(monkeypatch):
    """Sparse-prefix authentication and online work limits fail closed."""

    metadata = serving._ProviderSetGraphMetadata(
        provider_set_key=1,
        provider_count=1,
        prefix_member_count=1,
        prefix_member_digest=serving._v4_npi_prefix_digest((7,)),
    )
    group_sources = _v4_sources(
        provider_count_by_id={"01" * 16: 1},
        prefix_override_by_id={"01" * 16: metadata},
    )
    state = serving._V4NpiPrefixState.for_provider_sets(("01" * 16,))
    monkeypatch.setattr(
        serving,
        "lookup_v4_ordered_npi_prefix_overrides",
        AsyncMock(return_value={}),
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="incomplete"):
        await serving._apply_v4_npi_prefix_overrides(
            object(),
            1,
            group_sources,
            {"01" * 16: 1},
            state,
            2,
        )

    with pytest.raises(serving.PTG2ManifestArtifactError, match="positive"):
        await serving._walk_v4_npi_prefixes(
            object(),
            1,
            _v4_sources(),
            ("01" * 16,),
            {"01" * 16: 1},
            serving._V4NpiPrefixState.for_provider_sets(("01" * 16,)),
            0,
            1,
        )

    monkeypatch.setattr(
        serving,
        "_v4_hot_prefix_limits",
        lambda _tables: SimpleNamespace(target=1),
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="sealed target"):
        await serving._v4_npi_prefixes_by_set(
            object(),
            object(),
            ("01" * 16,),
            limit_per_set=2,
        )
