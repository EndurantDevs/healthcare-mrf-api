# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current-binding and immutable effect checks; PostgreSQL qualification is separate."""

import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from db.models import CodeCatalog, CodeCrosswalk, CodeRelationship, CodeSynonym
from process import code_sets_result_archive as codes
from process import ms_drg_result_generation as drg
from process import reference_family_archive as native
from process import reference_family_dictionary as dictionary
from process import scoped_catalog_binding as binding


def _code_generation(oid=10):
    return codes.CodeSetsGeneration(
        UUID(int=1), 3, UUID(int=2), 2, datetime(2026, 1, 1, tzinfo=timezone.utc), oid, 3, "a" * 64
    )


def _drg_generation(oid=10):
    tables = [
        dict(
            table=name,
            sources=list(sources),
            relation_oid=oid + index,
            row_count=1,
            row_sha256="a" * 64,
            schema_sha256="b" * 64,
        )
        for index, (name, sources) in enumerate(drg._SCOPES)
    ]
    return dict(
        local_lineage_id=UUID(int=1),
        local_generation=3,
        origin_lineage_id=UUID(int=2),
        origin_generation=2,
        published_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        include_relationships=True,
        receipt=dict(contract=drg.CONTRACT, content_sha256="c" * 64, tables=tables),
    )


def _session():
    return SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=True), commit=AsyncMock()
    )


def _stub_binding(monkeypatch, pairs=(("code_catalog", 20),)):
    monkeypatch.setattr(binding, "lock_catalog_binding", AsyncMock(return_value=pairs))
    monkeypatch.setattr(binding, "_lock_generation", AsyncMock(return_value=99))
    monkeypatch.setattr(binding, "require_closed_catalog_binding", AsyncMock(return_value=90))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "generation", "unpublished", "rows", "digest", "late", "custody"])
async def test_ms_drg_rebind_preserves_authority_and_changes_only_physical_receipt(monkeypatch, failure):
    """Rebinding never advances origin or writes before generation, custody and content agree."""
    _stub_binding(monkeypatch)
    expected, session = _drg_generation(), _session()
    if failure == "unpublished":
        expected["origin_lineage_id"] = None
    locked = {**expected, "local_generation": 4} if failure == "generation" else expected
    session.execute.return_value = SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: locked))
    current = deepcopy(_drg_generation(20)["receipt"])
    if failure == "rows":
        current["tables"][1]["row_sha256"] = "d" * 64
    if failure == "digest":
        current["content_sha256"] = "d" * 64
    rebound_by_field = {**expected, "receipt": current}
    capture = AsyncMock(return_value=current)
    read = AsyncMock(
        return_value={**rebound_by_field, "local_generation": 4} if failure == "late" else rebound_by_field
    )
    monkeypatch.setattr(drg, "capture_result", capture)
    monkeypatch.setattr(drg, "read_current_generation", read)
    if failure == "custody":
        binding.require_closed_catalog_binding.side_effect = RuntimeError("open custody")
    if failure in {"generation", "rows", "digest", "late", "custody"}:
        with pytest.raises(RuntimeError, match="generation|source changed|custody"):
            await binding.rebind_ms_drg_generation(session, "serving", expected)
    else:
        assert await binding.rebind_ms_drg_generation(session, "serving", expected) == (
            expected if failure == "unpublished" else rebound_by_field
        )
    assert "FOR UPDATE" in str(session.execute.await_args_list[0].args[0])
    assert session.execute.await_count == (2 if failure in {None, "late"} else 1)
    if failure in {None, "late"}:
        query, parameters = session.execute.await_args.args
        assert str(query) == 'UPDATE "serving".ms_drg_result_generation SET receipt=CAST(:receipt AS jsonb) WHERE id=1'
        assert json.loads(parameters["receipt"]) == current
    if failure in {"generation", "unpublished", "custody"}:
        capture.assert_not_awaited()
    if failure != "late" and failure is not None:
        read.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oid,required", [(None, False), (None, True), (True, True), (0, True), ("10", True)])
async def test_generation_pin_rejects_unavailable_or_untyped_identity_before_lock(monkeypatch, oid, required):
    """Only optional absence is accepted; invalid control identities never reach native reads."""
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=oid))
    lock, read = AsyncMock(), AsyncMock()
    monkeypatch.setattr(native, "_lock_family", lock)
    monkeypatch.setattr(native, "require_native_read_catalog", read)
    if oid is None and not required:
        assert await binding._lock_generation(_session(), "serving", drg.TABLE) is None
    else:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="unavailable"):
            await binding._lock_generation(_session(), "serving", drg.TABLE, required=required)
    lock.assert_not_awaited()
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_generation_pin_rechecks_identity_after_native_lock(monkeypatch):
    """A replacement between discovery and lock cannot authorize a generation query."""
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(side_effect=[10, 11]))
    monkeypatch.setattr(native, "_lock_family", AsyncMock())
    read = AsyncMock()
    monkeypatch.setattr(native, "require_native_read_catalog", read)
    with pytest.raises(native.ReferenceFamilyArchiveError, match="identity changed"):
        await binding._lock_generation(_session(), "serving", drg.TABLE, required=True)
    read.assert_not_awaited()


@pytest.mark.asyncio
async def test_binding_rejects_unsupported_model_family_before_catalog_access(monkeypatch):
    """A partial MS-DRG family is not a supported publication or read binding."""
    inventory = AsyncMock()
    monkeypatch.setattr(native, "_incumbent_pairs", inventory)
    with pytest.raises(native.ReferenceFamilyArchiveError, match="family differs"):
        await binding.lock_catalog_binding(_session(), "serving", (CodeCatalog, CodeSynonym))
    inventory.assert_not_awaited()


@pytest.mark.asyncio
async def test_rebinding_rejects_changed_current_identity_before_authority_queries(monkeypatch):
    """Matching generation content cannot authorize a different current catalog relation."""
    _stub_binding(monkeypatch)
    session = _session()
    with pytest.raises(native.ReferenceFamilyArchiveError, match="current identity changed"):
        await binding.require_current_catalog_rebinding(session, "serving", 99)
    binding._lock_generation.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_unpublished_code_set_rebind_never_creates_source_authority(monkeypatch):
    """Generation zero remains unpublished and never acquires a physical-receipt UPDATE."""
    _stub_binding(monkeypatch)
    expected = replace(_code_generation(), origin_lineage_id=None, origin_generation=None)
    monkeypatch.setattr(codes, "read_generation", AsyncMock(return_value=expected))
    session = _session()
    assert await binding.rebind_code_sets_generation(session, "serving", expected) == expected
    binding.require_closed_catalog_binding.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_unchanged_native_binding_does_not_require_relocation_custody(monkeypatch, importer):
    """Exact physical bindings are still content-checked but do not impersonate a protected relocation."""
    session = _session()
    if importer == "code-sets":
        _stub_binding(monkeypatch, (("code_catalog", 10),))
        installed = _code_generation()
        monkeypatch.setattr(codes, "read_generation", AsyncMock(return_value=installed))
        monkeypatch.setattr(codes, "scope_receipt", AsyncMock(return_value=(3, "a" * 64, 10)))
        monkeypatch.setattr(codes, "_column_signature", AsyncMock(return_value=("shape",)))
        assert (
            await binding.require_code_sets_binding(
                session, "serving", installed, schema_sha256=codes._schema_digest(("shape",))
            )
            == installed
        )
    else:
        installed = _drg_generation()
        _stub_binding(monkeypatch, tuple((name, 10 + index) for index, (name, _) in enumerate(drg._SCOPES)))
        monkeypatch.setattr(drg, "read_current_generation", AsyncMock(return_value=installed))
        assert await binding.require_ms_drg_binding(session, "serving", installed) == installed
    binding.require_closed_catalog_binding.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [False, True])
async def test_generation_pin_uses_authenticated_lock_and_read_boundary(monkeypatch, read_only):
    """Successful native generation pins preserve mode, identity and read-only custody boundaries."""
    session = _session()
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=10))
    lock, catalog, custody = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(native, "_lock_family", lock)
    monkeypatch.setattr(native, "require_native_read_catalog", catalog)
    monkeypatch.setattr(binding, "require_closed_catalog_read_binding", custody)
    assert await binding._lock_generation(session, "serving", drg.TABLE, required=True, read_only=read_only) == 10
    lock.assert_awaited_once_with(
        session, "serving", (drg.TABLE,), "ACCESS SHARE" if read_only else "SHARE", nowait=True
    )
    catalog.assert_awaited_once_with(session, (10,))
    if read_only:
        custody.assert_awaited_once_with(session, "serving", ((drg.TABLE, 10),))
    else:
        custody.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oid", [None, True, 0, "10"])
async def test_text_normalization_refuses_missing_native_source_before_alter(monkeypatch, oid):
    """The isolated clone cannot be altered on an unavailable or untyped source identity."""
    monkeypatch.setattr(native, "_lock_family", AsyncMock())
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=oid))
    columns = AsyncMock()
    monkeypatch.setattr(binding, "_require_model_columns", columns)
    session = _session()
    with pytest.raises(native.ReferenceFamilyArchiveError, match="source relation is unavailable"):
        await binding.match_catalog_text_columns(session, "serving", "candidate", "code_catalog")
    columns.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_effect_rebinding_authenticates_ms_drg_complete_current_family(monkeypatch, changed):
    """MS-DRG authority requires its full family and the exact current catalog OID."""
    pairs = (("code_catalog", 20), ("code_crosswalk", 21))
    drg_pairs = tuple((name, 20 + index) for index, (name, _) in enumerate(drg._SCOPES))
    _stub_binding(monkeypatch)
    binding.lock_catalog_binding.side_effect = [pairs, drg_pairs]
    binding._lock_generation.side_effect = [None, 99]
    monkeypatch.setattr(drg, "read_current_generation", AsyncMock(return_value=_drg_generation(22 if changed else 20)))
    session = _session()
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="content changed"):
            await binding.require_current_catalog_rebinding(session, "serving", 20)
    else:
        await binding.require_current_catalog_rebinding(session, "serving", 20)
    binding.require_closed_catalog_binding.assert_any_await(session, "serving", drg_pairs + ((drg.TABLE, 99),))
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [True, False, None])
async def test_source_pin_uses_select_only_locks_exclusively_for_protected_storage(monkeypatch, protected):
    session = _session()
    session.scalar.return_value = protected
    family, generation = AsyncMock(), AsyncMock()
    monkeypatch.setattr(binding, "lock_catalog_binding", family)
    monkeypatch.setattr(binding, "_lock_generation", generation)
    is_read_only = protected is True
    assert await binding.pin_catalog_source(session, "serving", (CodeCatalog,), codes.TABLE) is is_read_only
    family.assert_awaited_once_with(session, "serving", (CodeCatalog,), read_only=is_read_only)
    generation.assert_awaited_once_with(session, "serving", codes.TABLE, required=True, read_only=is_read_only)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "owner", "custody"])
async def test_read_only_binding_authenticates_storage_without_granting_publication(monkeypatch, failure):
    from process.ptg_parts import ptg2_physical_binding as custody

    session = _session()
    session.scalar.return_value = None if failure == "owner" else 90
    monkeypatch.setattr(native, "_schema_oid", AsyncMock(return_value=7))
    publisher = AsyncMock()
    monkeypatch.setattr(native, "protected_publisher_owner", publisher)
    closed = AsyncMock(side_effect=RuntimeError("custody differs") if failure == "custody" else None)
    monkeypatch.setattr(custody, "_require_closed_local_custody", closed)
    pairs = (("code_catalog", 10), (codes.TABLE, 11))
    if failure:
        with pytest.raises(RuntimeError, match="owner|custody"):
            await binding.require_closed_catalog_read_binding(session, "serving", pairs)
    else:
        assert await binding.require_closed_catalog_read_binding(session, "serving", pairs) == 90
        assert closed.await_args.args[1].relation_oids == pairs
        assert closed.await_args.args[1].schema_oid == 7
    publisher.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_read_only_family_and_generation_pins_reject_open_storage(monkeypatch):
    pairs = (("code_catalog", 10),)
    monkeypatch.setattr(native, "_incumbent_pairs", AsyncMock(return_value=pairs))
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=11))
    locks = AsyncMock()
    monkeypatch.setattr(native, "_lock_family", locks)
    monkeypatch.setattr(native, "require_native_read_catalog", AsyncMock())
    monkeypatch.setattr(binding, "_require_model_columns", AsyncMock())
    monkeypatch.setattr(
        binding, "require_closed_catalog_read_binding", AsyncMock(side_effect=RuntimeError("open storage"))
    )
    session = _session()
    with pytest.raises(RuntimeError, match="open storage"):
        await binding.lock_catalog_binding(session, "serving", (CodeCatalog,), read_only=True)
    with pytest.raises(RuntimeError, match="open storage"):
        await binding._lock_generation(session, "serving", codes.TABLE, required=True, read_only=True)
    assert [call.args[3] for call in locks.await_args_list] == ["ACCESS SHARE", "ACCESS SHARE"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing", "changed", "unsafe"])
async def test_catalog_binding_locks_the_complete_native_family(monkeypatch, failure):
    pairs = (("code_catalog", 20), ("code_crosswalk", None if failure == "missing" else 21))
    monkeypatch.setattr(
        native,
        "_incumbent_pairs",
        AsyncMock(
            side_effect=[pairs, (("code_catalog", 22), ("code_crosswalk", 21)) if failure == "changed" else pairs]
        ),
    )
    monkeypatch.setattr(native, "_lock_family", AsyncMock())
    monkeypatch.setattr(
        native,
        "require_native_read_catalog",
        AsyncMock(side_effect=RuntimeError("unsafe native catalog") if failure == "unsafe" else None),
    )
    columns = AsyncMock()
    monkeypatch.setattr(binding, "_require_model_columns", columns)
    session = _session()
    if failure:
        with pytest.raises(RuntimeError, match="incomplete|changed|unsafe"):
            await binding.lock_catalog_binding(session, "serving", (CodeCatalog, CodeCrosswalk))
        columns.assert_not_awaited()
    else:
        assert await binding.lock_catalog_binding(session, "serving", (CodeCatalog, CodeCrosswalk)) == pairs
        native._lock_family.assert_awaited_once_with(
            session, "serving", ("code_catalog", "code_crosswalk"), "SHARE", nowait=True
        )
        assert columns.await_count == 2
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "changed",
    [
        "origin_lineage_id",
        "origin_generation",
        "published_at",
        "row_count",
        "row_sha256",
        "local_lineage_id",
        "local_generation",
    ],
)
def test_code_set_logical_comparison_does_not_discard_source_authority(changed):
    installed = _code_generation()
    value = 0 if changed in {"origin_generation", "row_count", "local_generation"} else None
    assert binding.same_code_sets_origin(replace(installed, code_catalog_oid=20), installed)
    assert not binding.same_code_sets_origin(replace(installed, **{changed: value}), installed)


@pytest.mark.parametrize(
    "changed",
    [
        "origin_generation",
        "local_lineage_id",
        "include_relationships",
        "row_sha256",
        "schema_sha256",
        "sources",
        "content_sha256",
        "local_generation",
    ],
)
def test_ms_drg_logical_comparison_retains_every_semantic_receipt_field(changed):
    installed, observed = _drg_generation(), _drg_generation(20)
    assert binding.same_ms_drg_origin(observed, installed)
    if changed in observed:
        observed[changed] = 0 if changed == "local_generation" else None
    elif changed == "content_sha256":
        observed["receipt"][changed] = "d" * 64
    else:
        observed["receipt"]["tables"][0][changed] = [] if changed == "sources" else "d" * 64
    assert not binding.same_ms_drg_origin(observed, installed)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "logical", "rows", "physical", "schema"])
async def test_code_set_current_binding_resolves_only_exact_source_and_schema(monkeypatch, failure):
    _stub_binding(monkeypatch)
    installed, observed = _code_generation(), _code_generation(20)
    if failure == "logical":
        observed = replace(observed, origin_generation=9)
    if failure == "physical":
        observed = replace(observed, code_catalog_oid=21)
    monkeypatch.setattr(codes, "read_generation", AsyncMock(return_value=observed))
    monkeypatch.setattr(
        codes,
        "scope_receipt",
        AsyncMock(return_value=(4 if failure == "rows" else 3, "a" * 64, observed.code_catalog_oid)),
    )
    monkeypatch.setattr(codes, "_column_signature", AsyncMock(return_value=("shape",)))
    session = _session()
    options_by_field = dict(schema_sha256="bad" if failure == "schema" else codes._schema_digest(("shape",)))
    if failure:
        with pytest.raises(codes.CodeSetsArchiveError, match="current binding"):
            await binding.require_code_sets_binding(session, "serving", installed, **options_by_field)
    else:
        assert await binding.require_code_sets_binding(session, "serving", installed, **options_by_field) == observed
    session.execute.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "logical", "physical"])
async def test_ms_drg_binding_requires_complete_three_table_vector(monkeypatch, failure):
    pairs = tuple((name, 20 + index) for index, (name, _) in enumerate(drg._SCOPES))
    _stub_binding(monkeypatch, pairs)
    installed, observed = _drg_generation(), _drg_generation(20)
    if failure == "logical":
        observed["receipt"]["tables"][1]["row_sha256"] = "d" * 64
    if failure == "physical":
        observed["receipt"]["tables"][2]["relation_oid"] = 99
    monkeypatch.setattr(drg, "read_current_generation", AsyncMock(return_value=observed))
    if failure:
        with pytest.raises(RuntimeError, match="current binding"):
            await binding.require_ms_drg_binding(_session(), "serving", installed)
    else:
        assert await binding.require_ms_drg_binding(_session(), "serving", installed) == observed
    assert binding.lock_catalog_binding.await_args.args[2] == (CodeCatalog, CodeSynonym, CodeRelationship)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "generation", "rows", "late"])
async def test_code_set_rebind_changes_only_physical_receipt(monkeypatch, failure):
    _stub_binding(monkeypatch)
    expected = _code_generation()
    rebound = replace(expected, code_catalog_oid=20)
    monkeypatch.setattr(
        codes,
        "read_generation",
        AsyncMock(
            side_effect=[
                replace(expected, local_generation=4) if failure == "generation" else expected,
                replace(rebound, row_count=9) if failure == "late" else rebound,
            ]
        ),
    )
    monkeypatch.setattr(codes, "scope_receipt", AsyncMock(return_value=(4 if failure == "rows" else 3, "a" * 64, 20)))
    session = _session()
    if failure:
        with pytest.raises(codes.CodeSetsArchiveError, match="generation|source changed"):
            await binding.rebind_code_sets_generation(session, "serving", expected)
    else:
        assert await binding.rebind_code_sets_generation(session, "serving", expected) == rebound
    if failure in {"generation", "rows"}:
        session.execute.assert_not_awaited()
    else:
        query, parameters = session.execute.await_args.args
        assert str(query) == 'UPDATE "serving".code_sets_result_generation SET code_catalog_oid=:oid WHERE id=1'
        assert parameters == {"oid": 20}
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing", "inactive", "rows", "oid"])
async def test_effect_rebinding_requires_current_native_source_authority(monkeypatch, failure):
    _stub_binding(monkeypatch, (("code_catalog", 20), ("code_crosswalk", 21)))
    binding._lock_generation.side_effect = [None, None] if failure == "missing" else [99, None]
    session = _session()
    session.scalar.return_value = failure != "inactive"
    observed = _code_generation(22 if failure == "oid" else 20)
    monkeypatch.setattr(codes, "read_generation", AsyncMock(return_value=observed))
    monkeypatch.setattr(codes, "scope_receipt", AsyncMock(return_value=(4 if failure == "rows" else 3, "a" * 64, 20)))
    if failure:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="authority|content changed"):
            await binding.require_current_catalog_rebinding(session, "serving", 20)
    else:
        await binding.require_current_catalog_rebinding(session, "serving", 20)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch,table,accepted", [(False, 0, True), (True, 0, True), (True, 1, False), (None, 0, False)]
)
async def test_effect_destination_preserves_oid_fence_or_authenticates_catalog(monkeypatch, mismatch, table, accepted):
    proof = AsyncMock()
    monkeypatch.setattr(binding, "require_current_catalog_rebinding", proof)
    session = _session()
    session.scalar.return_value = mismatch
    mirror = dictionary._DRUG_SCOPED_MODELS[table]
    if accepted:
        await dictionary._require_effect_destination(session, mirror, '"candidate"."effects"', "serving", 20)
    else:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="destination changed"):
            await dictionary._require_effect_destination(session, mirror, '"candidate"."effects"', "serving", 20)
    assert proof.await_count == int(accepted and mismatch is True)
    assert "destination_oid IS DISTINCT FROM :oid" in str(session.scalar.await_args.args[0])
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rollback,failure",
    [
        (False, None),
        (False, "binding"),
        (False, "preimage"),
        (True, None),
        (True, "binding"),
        (True, "preimage"),
        (True, "baseline"),
    ],
)
async def test_effect_current_binding_never_replaces_exact_image_checks(monkeypatch, rollback, failure):
    session = _session()
    queries = []

    async def is_rejected(statement, _parameters=None):
        query = str(statement)
        queries.append(query)
        return (failure == "baseline" and "baseline_image IS DISTINCT" in query) or (
            failure == "preimage" and "baseline_image IS DISTINCT" not in query
        )

    session.scalar = is_rejected
    monkeypatch.setattr(dictionary, "_lock_reference_dictionary", AsyncMock(return_value=(20, 21)))
    monkeypatch.setattr(
        dictionary,
        "_require_effect_destination",
        AsyncMock(side_effect=RuntimeError("synthetic binding failure") if failure == "binding" else None),
    )
    writer = AsyncMock()
    monkeypatch.setattr(dictionary, "_write_dictionary_effects", writer)
    options_by_field = dict(
        incoming_schema="candidate", current_schema="current", destination_schema="serving", rollback=rollback
    )
    if failure:
        with pytest.raises(RuntimeError, match="binding failure|destination changed|rollback key changed"):
            await dictionary.apply_reference_dictionary_effects(session, **options_by_field)
        writer.assert_not_awaited()
    else:
        await dictionary.apply_reference_dictionary_effects(session, **options_by_field)
        assert writer.await_count == 2
        assert dictionary._require_effect_destination.await_count == (4 if rollback else 2)
        image = "after_image" if rollback else "before_image"
        assert any(f"effect.{image} IS DISTINCT FROM to_jsonb(live)" in query for query in queries)
        if rollback:
            assert any("effect.baseline_image IS DISTINCT FROM to_jsonb(live)" in query for query in queries)
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("closed", [True, False, None])
async def test_current_binding_requires_real_closed_native_custody(monkeypatch, closed):
    monkeypatch.setattr(native, "protected_publisher_owner", AsyncMock(return_value=90))
    monkeypatch.setattr(native, "_schema_oid", AsyncMock(return_value=80))
    session = _session()
    session.execute.return_value = SimpleNamespace(
        mappings=lambda: SimpleNamespace(one=lambda: {"object_count": 2, "closed": closed})
    )
    pairs = (("code_catalog", 20), ("code_sets_result_generation", 21))
    if closed is True:
        assert await binding.require_closed_catalog_binding(session, "serving", pairs) == 90
    else:
        with pytest.raises(RuntimeError, match="custody is not closed"):
            await binding.require_closed_catalog_binding(session, "serving", pairs)
    query, parameters = session.execute.await_args.args
    assert parameters["heaps"] == [20, 21] and parameters["schema_oid"] == 80
    assert "a.privilege_type<>'SELECT'" in str(query) and "pg_trigger" in str(query)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_relocated_current_receipt_refuses_open_custody(monkeypatch, importer):
    _stub_binding(
        monkeypatch,
        (("code_catalog", 20),)
        if importer == "code-sets"
        else tuple((name, 20 + index) for index, (name, _) in enumerate(drg._SCOPES)),
    )
    binding.require_closed_catalog_binding.side_effect = RuntimeError("synthetic open custody")
    if importer == "code-sets":
        monkeypatch.setattr(codes, "read_generation", AsyncMock(return_value=_code_generation(20)))
        monkeypatch.setattr(codes, "scope_receipt", AsyncMock(return_value=(3, "a" * 64, 20)))
        monkeypatch.setattr(codes, "_column_signature", AsyncMock(return_value=("shape",)))
        operation = binding.require_code_sets_binding(
            _session(), "serving", _code_generation(), schema_sha256=codes._schema_digest(("shape",))
        )
    else:
        monkeypatch.setattr(drg, "read_current_generation", AsyncMock(return_value=_drg_generation(20)))
        operation = binding.require_ms_drg_binding(_session(), "serving", _drg_generation())
    with pytest.raises(RuntimeError, match="open custody"):
        await operation
    assert binding.require_closed_catalog_binding.await_args.args[2][-1][1] == 99


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "text", "extra", "type", "nullable", "generated", "identity", "default"])
async def test_model_schema_check_is_native_and_exact(monkeypatch, change):
    columns = [
        [column.name, str(column.type.compile(dialect=postgresql.dialect())), not column.nullable, "", "", False]
        for column in CodeCatalog.__table__.columns
    ]
    if change == "text":
        columns[3][1] = columns[4][1] = "text"
    elif change == "extra":
        columns.append(["extra", "text", False, "", "", False])
    elif change is not None:
        index = {"type": 1, "nullable": 2, "generated": 3, "identity": 4, "default": 5}[change]
        columns[0][index] = {"type": "integer", "nullable": False, "generated": "s", "identity": "a", "default": True}[
            change
        ]
    session = _session()
    session.execute.return_value = SimpleNamespace(all=lambda: columns)
    primary = AsyncMock()
    monkeypatch.setattr(drg, "_table_shape", primary)
    if change in {None, "text"}:
        await binding._require_model_columns(session, 20, CodeCatalog)
        primary.assert_awaited_once_with(session, 20, CodeCatalog)
    else:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="columns differ"):
            await binding._require_model_columns(session, 20, CodeCatalog)
        primary.assert_not_awaited()


@pytest.mark.asyncio
async def test_text_normalization_changes_only_an_isolated_heap(monkeypatch):
    monkeypatch.setattr(native, "_lock_family", AsyncMock())
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=20))
    monkeypatch.setattr(binding, "_require_model_columns", AsyncMock())
    session = _session()
    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: ["display_name", "short_description"])
    )
    await binding.match_catalog_text_columns(session, "serving", "candidate", "code_catalog")
    queries = [str(arguments.args[0]) for arguments in session.execute.await_args_list]
    assert queries[-2:] == [
        'ALTER TABLE "candidate"."code_catalog" ALTER COLUMN "display_name" TYPE TEXT',
        'ALTER TABLE "candidate"."code_catalog" ALTER COLUMN "short_description" TYPE TEXT',
    ]
    with pytest.raises(native.ReferenceFamilyArchiveError, match="clone scope"):
        await binding.match_catalog_text_columns(session, "serving", "serving", "code_catalog")
