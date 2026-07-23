from __future__ import annotations

from tests.ptg2_v4_orchestration_support import (
    AsyncMock,
    Path,
    SimpleNamespace,
    _v4_reuse_manifest,
    asynccontextmanager,
    ptg,
    pytest,
)

def _assert_complete_reuse_contract(monkeypatch):
    monkeypatch.setattr(
        ptg,
        "validate_source_witness_manifest",
        lambda value, **_kwargs: {"validated": value},
    )
    monkeypatch.setattr(
        ptg,
        "validate_provider_identifier_quarantine",
        lambda value: {"validated": value},
    )
    manifest = _v4_reuse_manifest()
    reused = ptg._reused_shared_v3_serving_index(
        manifest,
        source_key="source",
        shared_snapshot_key=17,
        expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
    )
    assert reused["source_key"] == "source"
    assert reused["shared_snapshot_key"] == 17
    assert reused["storage_generation"] == ptg.PTG2_V4_SHARED_GENERATION
    assert reused["source_witness"]["validated"] == {"contract": "test"}
    assert reused["provider_identifier_quarantine"]["validated"] == {
        "contract": "test"
    }
    assert reused["serving_binary_table"] is None
    assert reused["materialized_tables"] == {}

    serving_index = manifest["serving_index"]
    assert isinstance(serving_index, dict)
    return manifest, serving_index


def _assert_incompatible_reuse_markers(serving_index) -> None:
    invalid_values_by_field = {
        "arch_version": "other",
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "other",
        "price_membership_semantics": "other",
        "serving_multiplicity_semantics": "other",
        "shared_block_layout": "dense_shared_blocks_v3",
    }
    for field_name, invalid_value in invalid_values_by_field.items():
        candidate_manifest_by_field = {
            "serving_index": {
                **serving_index,
                field_name: invalid_value,
            }
        }
        expected_error = (
            "incompatible architecture"
            if field_name == "arch_version"
            else "shared cold-read contract"
        )
        with pytest.raises(RuntimeError, match=expected_error):
            ptg._reused_shared_v3_serving_index(
                candidate_manifest_by_field,
                source_key="source",
                shared_snapshot_key=17,
                expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
            )


def _assert_invalid_packed_graph_contracts(manifest, serving_index) -> None:
    with pytest.raises(RuntimeError, match="unsupported generation"):
        ptg._reused_shared_v3_serving_index(
            manifest,
            source_key="source",
            shared_snapshot_key=17,
            expected_generation="v99",
        )
    with pytest.raises(RuntimeError, match="serving manifest"):
        ptg._reused_shared_v3_serving_index(
            {"serving_index": "not-an-object"},
            source_key="source",
            shared_snapshot_key=17,
        )

    for field_name, invalid_value in (
        ("type", "ptg2_shared_blocks_v3"),
        ("provider_scope_strategy", "postgres_shared_graph"),
        ("snapshot_map", "not-an-object"),
        ("serving_binary", "not-an-object"),
        ("serving_binary", {"provider_graph_v4": "not-an-object"}),
        (
            "serving_binary",
            {"provider_graph_v4": {"contract": "wrong"}},
        ),
    ):
        candidate_manifest_by_field = {
            "serving_index": {
                **serving_index,
                field_name: invalid_value,
            }
        }
        with pytest.raises(RuntimeError, match="packed graph contract"):
            ptg._reused_shared_v3_serving_index(
                candidate_manifest_by_field,
                source_key="source",
                shared_snapshot_key=17,
                expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
            )


def test_reused_v4_serving_index_accepts_only_complete_packed_contract(
    monkeypatch,
) -> None:
    """Accept complete packed indexes and reject every incompatible marker."""
    manifest, serving_index = _assert_complete_reuse_contract(monkeypatch)
    _assert_incompatible_reuse_markers(serving_index)
    _assert_invalid_packed_graph_contracts(manifest, serving_index)


def _assert_invalid_reuse_counts(serving_index) -> None:
    invalid_counts_by_field = {
        "source_count": (
            (None, "missing source_count"),
            ("bad", "missing source_count"),
            (0, "invalid source_count"),
            (-1, "invalid source_count"),
        ),
        "code_count": (
            (None, "missing code_count"),
            ("bad", "missing code_count"),
            (-1, "invalid code_count"),
        ),
    }
    for field_name, invalid_counts in invalid_counts_by_field.items():
        for invalid_count, expected_error in invalid_counts:
            with pytest.raises(RuntimeError, match=expected_error):
                ptg._reused_shared_v3_serving_index(
                    {"serving_index": {**serving_index, field_name: invalid_count}},
                    source_key="source",
                    shared_snapshot_key=17,
                    expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
                )


def _assert_invalid_reuse_evidence(monkeypatch, manifest) -> None:
    def invalid_witness(*_args, **_kwargs):
        raise ValueError("invalid")

    monkeypatch.setattr(ptg, "validate_source_witness_manifest", invalid_witness)
    with pytest.raises(RuntimeError, match="source witness"):
        ptg._reused_shared_v3_serving_index(
            manifest,
            source_key="source",
            shared_snapshot_key=17,
            expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
        )
    monkeypatch.setattr(
        ptg,
        "validate_source_witness_manifest",
        lambda value, **_kwargs: value,
    )

    def invalid_quarantine(*_args, **_kwargs):
        raise ValueError("invalid")

    monkeypatch.setattr(
        ptg,
        "validate_provider_identifier_quarantine",
        invalid_quarantine,
    )
    with pytest.raises(RuntimeError, match="quarantine"):
        ptg._reused_shared_v3_serving_index(
            manifest,
            source_key="source",
            shared_snapshot_key=17,
            expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
        )


def test_reused_v4_serving_index_rejects_invalid_counts_and_evidence(
    monkeypatch,
) -> None:
    """Reject invalid reuse counts, source witnesses, and quarantine evidence."""
    manifest = _v4_reuse_manifest()
    serving_index = manifest["serving_index"]
    assert isinstance(serving_index, dict)
    monkeypatch.setattr(
        ptg,
        "validate_source_witness_manifest",
        lambda value, **_kwargs: value,
    )
    monkeypatch.setattr(
        ptg,
        "validate_provider_identifier_quarantine",
        lambda value: value,
    )
    _assert_invalid_reuse_counts(serving_index)
    _assert_invalid_reuse_evidence(monkeypatch, manifest)


@pytest.mark.asyncio
async def test_failed_v4_layout_is_left_for_generation_aware_gc(
    monkeypatch,
) -> None:
    assert await ptg._is_failed_shared_layout_abandoned(
        None,
        build_token="token",
    ) is None
    assert await ptg._is_failed_shared_layout_abandoned(
        SimpleNamespace(reused=True, snapshot_key=17),
        build_token="token",
    ) is None

    transaction = AsyncMock()
    monkeypatch.setattr(ptg.db, "transaction", transaction)
    reservation = SimpleNamespace(reused=False, snapshot_key=17)
    assert await ptg._is_failed_shared_layout_abandoned(
        reservation,
        build_token="token",
        expected_generation=ptg.PTG2_V4_SHARED_GENERATION,
    ) is None
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_failed_v3_layout_abandonment_retries_without_masking_failure(
    monkeypatch,
) -> None:
    reservation = SimpleNamespace(reused=False, snapshot_key=17)
    attempts = [RuntimeError("one"), RuntimeError("two"), True]

    @asynccontextmanager
    async def transaction():
        yield object()

    async def abandon(*_args, **_kwargs):
        outcome = attempts.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(ptg.db, "transaction", transaction)
    monkeypatch.setattr(ptg, "is_shared_layout_build_abandoned", abandon)
    monkeypatch.setattr(ptg.asyncio, "sleep", AsyncMock())
    assert await ptg._is_failed_shared_layout_abandoned(
        reservation,
        build_token="token",
    ) is True
    assert ptg.asyncio.sleep.await_count == 2

    async def always_fail(*_args, **_kwargs):
        raise RuntimeError("failed")

    monkeypatch.setattr(ptg, "is_shared_layout_build_abandoned", always_fail)
    ptg.asyncio.sleep.reset_mock()
    assert await ptg._is_failed_shared_layout_abandoned(
        reservation,
        build_token="token",
    ) is None
    assert ptg.asyncio.sleep.await_count == 2


def test_scanner_identity_binds_v4_compiler_and_policy(
    tmp_path: Path,
    monkeypatch,
) -> None:
    scanner = tmp_path / "scanner"
    graph_compiler = tmp_path / "compiler"
    scanner.write_bytes(b"scanner")
    graph_compiler.write_bytes(b"compiler")
    monkeypatch.setattr(ptg, "_ptg2_rust_scanner_binary", lambda: scanner)
    monkeypatch.setattr(
        ptg,
        "sha256_file",
        lambda path: (
            ("aa" * 32, 7)
            if Path(path) == scanner
            else ("bb" * 32, 8)
        ),
    )
    monkeypatch.setattr(ptg, "_env_bool", lambda *_args, **_kwargs: False)
    identity = ptg._shared_v3_scanner_identity()
    assert identity["contract_version"] == 3
    assert "provider_graph_compiler_sha256" not in identity

    monkeypatch.setattr(ptg, "_env_bool", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        ptg,
        "_resolve_v4_graph_compiler_binary",
        lambda: graph_compiler,
    )
    monkeypatch.setattr(
        ptg,
        "v4_graph_encoding_policy",
        lambda: {"member_page_bytes": 16_384},
    )
    identity = ptg._shared_v3_scanner_identity()
    assert identity["contract_version"] == 4
    assert identity["storage_generation"] == ptg.PTG2_V4_SHARED_GENERATION
    assert identity["provider_graph_compiler_sha256"] == "bb" * 32
    assert identity["provider_graph_compiler_bytes"] == 8
    assert identity["provider_graph_encoding_policy"] == {
        "member_page_bytes": 16_384
    }

    monkeypatch.setattr(
        ptg,
        "_resolve_v4_graph_compiler_binary",
        lambda: None,
    )
    with pytest.raises(RuntimeError, match="provider graph compiler"):
        ptg._shared_v3_scanner_identity()
    monkeypatch.setattr(ptg, "_ptg2_rust_scanner_binary", lambda: None)
    with pytest.raises(RuntimeError, match="Rust scanner"):
        ptg._shared_v3_scanner_identity()
