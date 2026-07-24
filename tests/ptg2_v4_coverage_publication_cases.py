from __future__ import annotations

from tests.ptg2_v4_coverage_support import (
    AuditCandidate,
    SharedLayoutBuildOwnership,
    SimpleNamespace,
    _Result,
    _ScriptedSession,
    _owner_row,
    _relation_row,
    asynccontextmanager,
    audit,
    pytest,
    snapshot_maps,
)

def _configure_publication_spies(monkeypatch):
    lock_calls: list[tuple[int, str]] = []
    batches: list[tuple[str, tuple[int, ...]]] = []

    async def fake_lock(
        _session,
        *,
        snapshot_key: int,
        build_token: str,
        **_kwargs,
    ):
        lock_calls.append((snapshot_key, build_token))

    async def fake_npi_batch(_session, *, npi_rows, **_kwargs):
        batches.append(("npi", tuple(row["npi_key"] for row in npi_rows)))

    async def fake_component_batch(_session, *, component_rows, **_kwargs):
        batches.append(
            ("component", tuple(row["component_key"] for row in component_rows))
        )

    async def fake_pattern_batch(_session, *, pattern_rows, **_kwargs):
        batches.append(
            ("pattern", tuple(row["pattern_key"] for row in pattern_rows))
        )

    async def fake_dense(_session, *, expected_count: int, **_kwargs):
        return expected_count

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        fake_lock,
    )
    monkeypatch.setattr(snapshot_maps, "_publish_v4_npi_batch", fake_npi_batch)
    monkeypatch.setattr(
        snapshot_maps,
        "_publish_v4_component_batch",
        fake_component_batch,
    )
    monkeypatch.setattr(
        snapshot_maps,
        "_publish_v4_pattern_batch",
        fake_pattern_batch,
    )
    monkeypatch.setattr(snapshot_maps, "_verify_dense_table_keys", fake_dense)
    return fake_lock, lock_calls, batches


async def _assert_dictionary_publications(lock_calls, batches) -> None:
    npi_publication = await snapshot_maps.publish_v4_npi_dictionary(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        entries=((0, 1_234_567_890), {"npi_key": 1, "npi": 1_234_567_891}),
        batch_rows=1,
    )
    component_publication = await snapshot_maps.publish_v4_provider_components(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        entries=((0, b"a" * 16), {"component_key": 1, "component_global_id_128": b"b" * 16}),
        batch_rows=1,
    )
    pattern_publication = await snapshot_maps.publish_v4_patterns(
        object(),
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        entries=((0, b"a" * 32, 2), {"pattern_key": 1, "pattern_digest": b"b" * 32, "set_count": 3}),
        batch_rows=1,
    )
    assert npi_publication.row_count == 2
    assert component_publication.row_count == 2
    assert pattern_publication.row_count == 2
    assert lock_calls == [(17, "token")] * 3
    assert batches == [
        ("npi", (0,)),
        ("npi", (1,)),
        ("npi", ()),
        ("component", (0,)),
        ("component", (1,)),
        ("component", ()),
        ("pattern", (0,)),
        ("pattern", (1,)),
        ("pattern", ()),
    ]


async def _assert_dictionary_row_rejections() -> None:
    with pytest.raises(ValueError, match="batch size"):
        await snapshot_maps.publish_v4_npi_dictionary(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            build_token="token",
            entries=(),
            batch_rows=0,
        )
    with pytest.raises(ValueError, match="contiguous"):
        snapshot_maps._normalized_npi_row((1, 1_234_567_890), 0)
    with pytest.raises(ValueError, match="invalid NPI"):
        snapshot_maps._normalized_npi_row((0, 1), 0)
    with pytest.raises(ValueError, match="16 bytes"):
        snapshot_maps._normalized_component_row((0, b"x"), 0)
    with pytest.raises(ValueError, match="digest"):
        snapshot_maps._normalized_pattern_row((0, b"x", 1), 0)
    with pytest.raises(ValueError, match="non-negative"):
        snapshot_maps._normalized_pattern_row((0, b"x" * 32, -1), 0)
    with pytest.raises(ValueError, match="must contain"):
        snapshot_maps._validated_v4_name("", field_name="name", max_bytes=4)


async def _assert_relation_publications(monkeypatch, fake_lock) -> None:
    relation_rows = snapshot_maps._normalized_relation_rows(
        (_relation_row("a"), _relation_row("b"))
    )
    relation_session = _ScriptedSession(
        _Result(),
        _Result(rows=relation_rows),
    )
    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        fake_lock,
    )
    relation_publication = await snapshot_maps.publish_v4_relation_manifests(
        relation_session,
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        entries=relation_rows,
        batch_rows=2,
    )
    assert relation_publication.row_count == 2

    owner_rows = snapshot_maps._normalized_heavy_owner_rows(
        (_owner_row("a", 1), _owner_row("b", 2))
    )
    owner_session = _ScriptedSession(
        _Result(),
        _Result(rows=owner_rows),
    )
    owner_publication = await snapshot_maps.publish_v4_heavy_owners(
        owner_session,
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        entries=owner_rows,
        batch_rows=2,
    )
    assert owner_publication.row_count == 2


@pytest.mark.asyncio
async def test_snapshot_publication_helpers_batch_and_verify(monkeypatch) -> None:
    """Publish dictionary and relation batches while enforcing row contracts."""
    fake_lock, lock_calls, batches = _configure_publication_spies(monkeypatch)
    await _assert_dictionary_publications(lock_calls, batches)
    await _assert_dictionary_row_rejections()
    await _assert_relation_publications(monkeypatch, fake_lock)


async def _assert_reservation_creation() -> None:
    session = _ScriptedSession(_Result(scalar=17), _Result())
    created = await snapshot_maps._create_v4_reservation(
        session,
        schema='"mrf"',
        fingerprint=b"f" * 32,
        build_token="token",
        storage_shard_id=2,
    )
    assert (created.snapshot_key, created.reused) == (17, False)

    no_key = _ScriptedSession(_Result(scalar=None))
    with pytest.raises(RuntimeError, match="did not return"):
        await snapshot_maps._create_v4_reservation(
            no_key,
            schema='"mrf"',
            fingerprint=b"f" * 32,
            build_token="token",
            storage_shard_id=2,
        )

    building = await snapshot_maps._existing_v4_reservation(
        object(),
        schema='"mrf"',
        existing={
            "state": "building",
            "generation": snapshot_maps.PTG2_V4_SHARED_GENERATION,
            "build_token": "token",
            "snapshot_key": 18,
        },
        build_token="token",
    )
    assert (building.snapshot_key, building.reused) == (18, False)
    with pytest.raises(RuntimeError, match="another build"):
        await snapshot_maps._existing_v4_reservation(
            object(),
            schema='"mrf"',
            existing={"state": "building"},
            build_token="token",
        )


async def _assert_public_reservation(monkeypatch) -> None:
    async def fake_load(*_args, **_kwargs):
        return None

    async def fake_create(*_args, **_kwargs):
        return SimpleNamespace(snapshot_key=19, reused=False)

    monkeypatch.setattr(snapshot_maps, "_load_v4_layout_reservation", fake_load)
    monkeypatch.setattr(snapshot_maps, "_create_v4_reservation", fake_create)
    reserve_session = _ScriptedSession(_Result())
    reserved = await snapshot_maps.reserve_v4_shared_layout(
        reserve_session,
        schema_name="mrf",
        semantic_fingerprint=b"s" * 32,
        build_token="token",
    )
    assert reserved.snapshot_key == 19


async def _assert_layout_write_lock() -> None:
    await snapshot_maps.lock_v4_layout_map_write(
        _ScriptedSession(_Result(scalar=17)),
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
    )
    with pytest.raises(RuntimeError, match="lost build ownership"):
        await snapshot_maps.lock_v4_layout_map_write(
            _ScriptedSession(_Result(scalar=None)),
            schema_name="mrf",
            snapshot_key=17,
            build_token="token",
        )
    await snapshot_maps.touch_v4_shared_layout_build(
        _ScriptedSession(_Result(scalar=17)),
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
    )
    with pytest.raises(RuntimeError, match="heartbeat lost"):
        await snapshot_maps.touch_v4_shared_layout_build(
            _ScriptedSession(_Result(scalar=None)),
            schema_name="mrf",
            snapshot_key=17,
            build_token="token",
        )


async def _assert_snapshot_map_root() -> None:
    expected_root_by_field = {
        "state": "building",
        "format_version": snapshot_maps.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "representation": "pattern_v1",
        "projection_id_scope": snapshot_maps.PTG2_V4_PROJECTION_ID_SCOPE,
    }
    await snapshot_maps._initialize_v4_snapshot_map_root(
        _ScriptedSession(_Result(), _Result(rows=(expected_root_by_field,))),
        schema_name="mrf",
        snapshot_key=17,
        representation="pattern_v1",
    )
    with pytest.raises(ValueError, match="unsupported"):
        await snapshot_maps._initialize_v4_snapshot_map_root(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            representation="bad",
        )
    with pytest.raises(RuntimeError, match="conflicts"):
        await snapshot_maps._initialize_v4_snapshot_map_root(
            _ScriptedSession(
                _Result(),
                _Result(rows=({**expected_root_by_field, "state": "complete"},)),
            ),
            schema_name="mrf",
            snapshot_key=17,
            representation="pattern_v1",
        )


async def _assert_snapshot_binding() -> None:
    await snapshot_maps.bind_snapshot_to_v4_layout(
        _ScriptedSession(_Result(), _Result(scalar="snapshot")),
        schema_name="mrf",
        snapshot_id="snapshot",
        snapshot_key=17,
    )
    await snapshot_maps.bind_snapshot_to_v4_layout(
        _ScriptedSession(
            _Result(),
            _Result(scalar=None),
            _Result(scalar=17),
        ),
        schema_name="mrf",
        snapshot_id="snapshot",
        snapshot_key=17,
    )
    with pytest.raises(RuntimeError, match="not bindable"):
        await snapshot_maps.bind_snapshot_to_v4_layout(
            _ScriptedSession(
                _Result(),
                _Result(scalar=None),
                _Result(scalar=18),
            ),
            schema_name="mrf",
            snapshot_id="snapshot",
            snapshot_key=17,
        )


@pytest.mark.asyncio
async def test_snapshot_reservation_root_and_binding_lifecycle(monkeypatch) -> None:
    """Exercise reservation ownership, root initialization, and snapshot binding."""
    await _assert_reservation_creation()
    await _assert_public_reservation(monkeypatch)
    await _assert_layout_write_lock()
    await _assert_snapshot_map_root()
    await _assert_snapshot_binding()



def _configure_audit_publication(monkeypatch):
    verify_providers = audit._verified_provider_npis_by_candidate
    candidates = (AuditCandidate(1, 7, 2, 0, 1, 0),)
    witness = audit.V4ProviderSetAuditWitness(7, 11, 1_234_567_890)
    transaction_session = object()

    @asynccontextmanager
    async def fake_transaction():
        yield transaction_session

    monkeypatch.setattr(audit.db, "transaction", fake_transaction)
    monkeypatch.setattr(
        audit,
        "load_audit_candidates",
        lambda _summary: (candidates, {"candidate_count": 1}),
    )
    monkeypatch.setattr(
        audit,
        "load_v4_audit_witnesses",
        lambda *_args, **_kwargs: {7: witness},
    )

    async def no_op(*_args, **_kwargs):
        return None

    monkeypatch.setattr(audit, "_validate_building_v4_context", no_op)
    monkeypatch.setattr(audit, "_validate_candidate_provider_counts", no_op)
    monkeypatch.setattr(audit, "_validate_snapshot_source_dictionary", no_op)

    async def fake_prices(*_args, **_kwargs):
        return {2: ((0, 9),)}

    async def fake_npis(*_args, **_kwargs):
        return {0: (1_234_567_890,)}

    monkeypatch.setattr(audit, "_price_memberships", fake_prices)
    monkeypatch.setattr(audit, "_verified_provider_npis_by_candidate", fake_npis)
    occurrence = SimpleNamespace(candidate_ordinal=0)
    monkeypatch.setattr(
        audit,
        "build_audit_occurrences",
        lambda **_kwargs: (occurrence,),
    )
    monkeypatch.setattr(audit, "_insert_occurrences", no_op)
    def fake_publication_metadata(**kwargs):
        hydration_work = kwargs["hydration_work"]
        assert hydration_work.price_candidate_count == 1
        assert hydration_work.provider_candidate_count == 1
        assert hydration_work.represented_source_count == 1
        assert hydration_work.provider_selection_budget == 1
        assert hydration_work.provider_selection_count == 1
        return {"sample_count": 1}

    monkeypatch.setattr(audit, "_publication_metadata", fake_publication_metadata)
    return SimpleNamespace(
        selected_layout="pattern",
        verify_providers=verify_providers,
    )


async def _assert_audit_publication(
    compilation,
    *,
    expected_representation: str,
    expected_verification: str,
) -> None:
    publication = await audit.publish_v4_audit_sample(
        schema_name="mrf",
        build_ownership=SharedLayoutBuildOwnership(17, "token"),
        logical_snapshot_id="snapshot",
        finalizer_summary={"source_count": 1},
        mapping_digest=b"m" * 32,
        core_support_digest=b"s" * 32,
        atom_key_bits=16,
        price_membership_block_span=256,
        graph_compilation=compilation,
    )
    assert publication.row_count == 1
    assert (
        publication.metadata["provider_graph_representation"]
        == expected_representation
    )
    assert publication.metadata["provider_graph_verification"] == expected_verification
    assert len(publication.support_digest) == 32


async def _assert_audit_publication_rejections(compilation) -> None:
    with pytest.raises(ValueError, match="mapping digest"):
        await audit.publish_v4_audit_sample(
            schema_name="mrf",
            build_ownership=SharedLayoutBuildOwnership(17, "token"),
            logical_snapshot_id="snapshot",
            finalizer_summary={"source_count": 1},
            mapping_digest=b"short",
            core_support_digest=b"s" * 32,
            atom_key_bits=16,
            price_membership_block_span=256,
            graph_compilation=compilation,
        )
    with pytest.raises(ValueError, match="core support digest"):
        await audit.publish_v4_audit_sample(
            schema_name="mrf",
            build_ownership=SharedLayoutBuildOwnership(17, "token"),
            logical_snapshot_id="snapshot",
            finalizer_summary={"source_count": 1},
            mapping_digest=b"m" * 32,
            core_support_digest=b"short",
            atom_key_bits=16,
            price_membership_block_span=256,
            graph_compilation=compilation,
        )
    with pytest.raises(RuntimeError, match="unsupported"):
        await audit.publish_v4_audit_sample(
            schema_name="mrf",
            build_ownership=SharedLayoutBuildOwnership(17, "token"),
            logical_snapshot_id="snapshot",
            finalizer_summary={"source_count": 1},
            mapping_digest=b"m" * 32,
            core_support_digest=b"s" * 32,
            atom_key_bits=16,
            price_membership_block_span=256,
            graph_compilation=SimpleNamespace(selected_layout="unknown"),
        )
    async def missing_edges(_relation, edges):
        return {edge: False for edge in edges}

    candidate = AuditCandidate(1, 7, 2, 0, 1, 0)
    witness = audit.V4ProviderSetAuditWitness(7, 11, 1_234_567_890)
    reader = SimpleNamespace(
        representation="direct_v1",
        contains_edges=missing_edges,
    )
    with pytest.raises(RuntimeError, match="absent from the direct graph"):
        await compilation.verify_providers(
            object(),
            schema_name="mrf",
            snapshot_key=17,
            candidates=(candidate,),
            witnesses={7: witness},
            reader=reader,
        )


@pytest.mark.asyncio
async def test_audit_publish_orchestration(monkeypatch) -> None:
    """Orchestrate audit publication and reject incomplete derived outputs."""
    compilation = _configure_audit_publication(monkeypatch)
    await _assert_audit_publication(
        compilation,
        expected_representation="pattern_v1",
        expected_verification="set_patterns_pattern_groups_group_npis_exact_v1",
    )
    await _assert_audit_publication(
        SimpleNamespace(selected_layout="direct"),
        expected_representation="direct_v1",
        expected_verification="set_groups_direct_group_npis_exact_v1",
    )
    await _assert_audit_publication_rejections(compilation)
