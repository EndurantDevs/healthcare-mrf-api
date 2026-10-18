# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Common publication locks also apply when only compatibility outputs change."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_bundle_preparation as bundles
from process import provider_directory_cms_publication as cms


@pytest.mark.asyncio
async def test_empty_artifact_bundle_locks_all_outputs_before_publication_callback():
    events = []

    async def lock(schema, names):
        events.append(("lock", schema, names))

    async def publish():
        events.append("publish")

    directory = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: object()),
        _schema=lambda: "source_schema",
        _ordered_provider_directory_artifact_bundle=lambda stages: stages,
        _lock_provider_directory_artifact_live_tables=lock,
    )
    await bundles.apply_prepared_artifact_bundle(
        directory, (), before_swaps=publish, extra_live_relations=("first", "second")
    )
    assert events == [("lock", "source_schema", ("first", "second")), "publish"]


@pytest.mark.asyncio
async def test_live_lock_refusal_prevents_callback_even_without_derived_stages():
    publish = AsyncMock()
    directory = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: object()),
        _schema=lambda: "source_schema",
        _ordered_provider_directory_artifact_bundle=lambda stages: stages,
        _lock_provider_directory_artifact_live_tables=AsyncMock(side_effect=TimeoutError("busy Reader")),
    )
    with pytest.raises(TimeoutError, match="busy Reader"):
        await bundles.apply_prepared_artifact_bundle(
            directory, (), before_swaps=publish, extra_live_relations=("first", "second")
        )
    publish.assert_not_awaited()


def test_cms_composes_one_sorted_live_set_for_the_common_lock_phase():
    prepared = SimpleNamespace(stages=(SimpleNamespace(target_relation="overlay"),))
    address = SimpleNamespace(swaps=(SimpleNamespace(live_cls=SimpleNamespace(__main_table__="address")),))
    doctors = object()
    names = cms._live_swap_relations(prepared, address, doctors)
    assert names == tuple(sorted({"overlay", "address", *cms.RELATION_NAMES_BY_IMPORTER["cms-doctors"]}))


@pytest.fixture
def cms_read_cutover(monkeypatch):
    """Observe the original CMS callback between its common locks and rename boundary."""
    events = []
    session, driver = SimpleNamespace(execute=AsyncMock()), object()
    directory = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session)),
        _schema=lambda: "sample",
        profile_initial=SimpleNamespace(build_from_stages=lambda *_args: True),
    )
    stages = tuple(
        SimpleNamespace(schema="sample", target_relation=name, stage_table=name + "_candidate")
        for name in ("overlay", "catalog")
    )
    prepared = SimpleNamespace(
        stages=stages,
        profile_delta=None,
        nonprofile_admission=None,
        assert_ready=AsyncMock(side_effect=lambda **_kwargs: events.append("ready")),
    )

    async def apply(_directory, _stages, *, before_swaps, **_options):
        events.append("locked")
        await before_swaps()
        events.append("swap")

    monkeypatch.setattr(cms, "apply_prepared_artifact_bundle", apply)
    monkeypatch.setattr(cms, "_copy_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(cms.coverage, "assert_sealed_cms_candidate_coverage", AsyncMock())
    monkeypatch.setattr(cms, "_seal_published_coverage", AsyncMock(side_effect=lambda *_args: events.append("seal")))
    monkeypatch.setattr(cms.receipts, "capture_native_dependencies", AsyncMock(return_value={}))
    monkeypatch.setattr(cms, "_receipt_payload", lambda *_args: {})
    monkeypatch.setattr(cms, "_registry_preparation", lambda *_args: None)
    monkeypatch.setattr(cms.receipts, "append_serving_receipt", AsyncMock(return_value="receipt"))
    monkeypatch.setattr(cms, "_bind_registry_source_receipt", AsyncMock())
    return SimpleNamespace(directory=directory, prepared=prepared, driver=driver, events=events)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_rejected", [False, True])
async def test_cms_preserves_reads_after_common_locks_and_before_any_swap(monkeypatch, cms_read_cutover, is_rejected):
    fixture = cms_read_cutover
    refusal = RuntimeError("publication_read_access_relations_changed")

    async def preserve(driver, **coordinates):
        assert driver is fixture.driver
        name = coordinates["live_table"]
        assert coordinates == dict(schema="sample", live_table=name, stage_table=name + "_candidate")
        fixture.events.append("read_" + name)
        if is_rejected:
            raise refusal

    monkeypatch.setattr(cms, "preserve_publication_reads", preserve)
    operation = cms._apply_prepared_results(fixture.directory, object(), fixture.prepared, None, {}, None, None)
    if is_rejected:
        with pytest.raises(RuntimeError) as caught:
            await operation
        assert caught.value is refusal
        assert fixture.events == ["ready", "locked", "ready", "read_overlay"]
    else:
        assert await operation == ("receipt", {})
        assert fixture.events == ["ready", "locked", "ready", "read_overlay", "read_catalog", "swap", "seal"]
