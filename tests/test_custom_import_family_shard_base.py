# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure owner-base contract checks; native pin and seal integration is separate."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from db.models import custom_import as models
from process.custom_import import family_shard_base as base_read
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import ROLE_SPECS, FamilyBlockLayout
from process.custom_import.runner_codec import digest_text, root_key_contract_hash
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_publication import _generation_seal_fixtures


def _request_with_base(**changes):
    return _request(expected_base_generation_id=9, expected_pointer_version=1, **changes)


def _row(key=' {"npi":"1003000126"} ', *, identity=11, children=2):
    return SimpleNamespace(
        root_record_id=identity,
        family_revision_id=identity + 100,
        root_revision_id=identity + 200,
        entity_binding_id=identity + 300,
        family_sha256=b"f" * 32,
        child_count=children,
        logical_key_sha256=digest_text("root-key", key),
        logical_key=key,
        key_bytes=len(key.encode("utf-8")),
    )


def _census(header_rows):
    return SimpleNamespace(
        invalid=False,
        family_count=len(header_rows),
        child_count=sum(header.child_count for header in header_rows),
        maximum_key_bytes=max((header.key_bytes for header in header_rows), default=0),
    )


class _Cursor:
    def __init__(self, rows, *, close_error=None, pause=None):
        self.rows = iter(rows)
        self.close_error, self.pause = close_error, pause
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.pause is not None:
            await self.pause.wait()
        try:
            return next(self.rows)
        except StopIteration:
            raise StopAsyncIteration from None

    async def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def _stream_context(monkeypatch, header_rows, *, snapshot=17, count=None, children=None, close_error=None, pause=None):
    transaction = SimpleNamespace(is_active=True)
    identities = (transaction, None, transaction, None)
    current = AsyncMock(return_value=identities)
    monkeypatch.setattr(base_read, "_read_transaction_identity", current)
    cursor = _Cursor(header_rows, close_error=close_error, pause=pause)
    census = _census(header_rows)
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(one=lambda: census)), stream=AsyncMock(return_value=cursor)
    )
    request = _request_with_base()
    generation = SimpleNamespace(generation_id=9, dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    total = len(header_rows) if count is None else count
    seal = SimpleNamespace(
        root_count=total,
        family_count=total,
        generation_family_count=total,
        family_child_count=sum(header.child_count for header in header_rows) if children is None else children,
    )
    context = base_read.PinnedBase(
        session,
        generation,
        seal,
        request.definition,
        None,
        SimpleNamespace(family_id=snapshot),
        None,
        request,
        identities,
        None,
        request.page_byte_limit,
    )
    return context, cursor, current


@pytest.mark.parametrize("snapshot", [None, 17])
async def test_complete_headers_preserve_exact_legacy_text_and_scope(monkeypatch, snapshot):
    rows = sorted(
        [_row(), _row("opaque original key", identity=91, children=0)], key=lambda row: row.logical_key_sha256
    )
    context, cursor, _ = _stream_context(monkeypatch, rows, snapshot=snapshot)
    headers = [header async for header in context.headers()]
    assert context.scope == (9, snapshot) and context.complete and cursor.closed
    assert [header.typed_key for header in headers] == [(row.logical_key, row.logical_key_sha256) for row in rows]
    assert [header.root_record_id for header in headers] == [row.root_record_id for row in rows]
    assert all((header.base_generation_id, header.base_snapshot_family_id) == context.scope for header in headers)
    assert context.session.execute.await_count == context.session.stream.await_count == 1
    with pytest.raises(FamilyBlockUnavailable, match="one live unread"):
        context.headers()


async def test_empty_base_has_scope_and_requires_real_eof(monkeypatch):
    context, cursor, _ = _stream_context(monkeypatch, [], snapshot=None)
    context._buffer_bytes = 1
    assert context.scope == (9, None) and not context.complete
    assert [header async for header in context.headers()] == []
    assert context.complete and cursor.closed
    assert context.session.stream.call_args.args[0].get_execution_options()["yield_per"] == 1


async def test_header_fetch_budget_uses_observed_unicode_byte_width(monkeypatch):
    rows = sorted([_row("short"), _row("\u00e9\U0001f642" * 19, identity=91)], key=lambda row: row.logical_key_sha256)
    context, cursor, _ = _stream_context(monkeypatch, rows)
    maximum_bytes = max(row.key_bytes for row in rows)
    charge = base_read._HEADER_FIXED_BYTES + base_read._HEADER_KEY_BYTES_MULTIPLIER * maximum_bytes
    context._buffer_bytes = 3 * charge + charge - 1
    assert len([header async for header in context.headers()]) == 2
    statement = context.session.stream.call_args.args[0]
    assert statement.get_execution_options()["yield_per"] == 3
    assert maximum_bytes in statement.compile().params.values()
    assert maximum_bytes > max(len(row.logical_key) for row in rows)
    assert cursor.closed and context.complete
    assert context.session.execute.await_count == context.session.stream.await_count == 1


def test_millions_of_headers_do_not_require_individual_fetches():
    census = SimpleNamespace(invalid=False, family_count=4_000_000, child_count=74_000_000, maximum_key_bytes=64)
    seal = SimpleNamespace(
        root_count=4_000_000, family_count=4_000_000, generation_family_count=4_000_000, family_child_count=74_000_000
    )
    assert base_read._header_fetch_rows(census, seal, 256 * 1024 * 1024, 16 * 1024 * 1024) == 4096


@pytest.mark.parametrize(
    "field,value",
    [
        ("invalid", True),
        ("invalid", None),
        ("invalid", 0),
        ("family_count", True),
        ("family_count", 2),
        ("family_count", -1),
        ("child_count", 1),
        ("child_count", True),
        ("maximum_key_bytes", None),
        ("maximum_key_bytes", -1),
        ("maximum_key_bytes", True),
    ],
)
async def test_invalid_aggregate_refuses_before_header_fetch(monkeypatch, field, value):
    context, cursor, _ = _stream_context(monkeypatch, [_row()])
    setattr(context.session.execute.return_value.one(), field, value)
    with pytest.raises(FamilyBlockUnavailable):
        await anext(context.headers())
    context.session.stream.assert_not_awaited()
    assert not cursor.closed and not context.complete


@pytest.mark.parametrize("damage", ["page", "buffer", "empty_width"])
async def test_header_resource_and_empty_census_refuse_before_fetch(monkeypatch, damage):
    context, cursor, _ = _stream_context(monkeypatch, [] if damage == "empty_width" else [_row()])
    census = context.session.execute.return_value.one()
    if damage == "page":
        census.maximum_key_bytes = context.request.page_byte_limit + 1
    elif damage == "buffer":
        context._buffer_bytes = (
            base_read._HEADER_FIXED_BYTES + base_read._HEADER_KEY_BYTES_MULTIPLIER * census.maximum_key_bytes - 1
        )
    else:
        census.maximum_key_bytes = 1
    refusal = FamilyBlockUnavailable if damage == "empty_width" else FamilyBlockResourceLimit
    with pytest.raises(refusal):
        await anext(context.headers())
    context.session.stream.assert_not_awaited()
    assert not cursor.closed and not context.complete


async def test_aggregate_cannot_cross_transaction_replacement(monkeypatch):
    context, cursor, current = _stream_context(monkeypatch, [_row()])
    current.side_effect = [context._transactions, None]
    with pytest.raises(FamilyBlockUnavailable, match="no longer live"):
        await anext(context.headers())
    context.session.execute.assert_awaited_once()
    context.session.stream.assert_not_awaited()
    assert not cursor.closed and not context.complete


@pytest.mark.parametrize("damage", ["missing", "child_count", "duplicate", "reverse", "relationship", "close"])
async def test_incomplete_or_invalid_headers_never_complete(monkeypatch, damage):
    rows = sorted([_row(), _row("other", identity=91)], key=lambda row: row.logical_key_sha256)
    context, cursor, _ = _stream_context(monkeypatch, rows)
    match damage:
        case "missing":
            cursor.rows = iter(rows[:1])
        case "child_count":
            context.seal.family_child_count += 1
        case "duplicate":
            cursor.rows = iter([rows[0], rows[0]])
        case "reverse":
            cursor.rows = iter(reversed(rows))
        case "relationship":
            context.session.execute.return_value.one().invalid = True
        case "close":
            cursor.close_error = OSError("synthetic close refusal")
    with pytest.raises((FamilyBlockUnavailable, OSError)):
        async for _header in context.headers():
            assert not context.complete
    assert not context.complete
    assert cursor.closed or damage in {"relationship", "child_count"}


async def test_partial_headers_do_not_claim_complete_origin(monkeypatch):
    context, cursor, _ = _stream_context(monkeypatch, [_row(), _row("next", identity=91)])
    headers = context.headers()
    assert (await anext(headers)).root_record_id == 11
    await headers.aclose()
    assert cursor.closed and not context.complete


@pytest.mark.parametrize("changed", ["root", "savepoint", "inactive"])
async def test_header_pin_cannot_cross_transaction_changes(monkeypatch, changed):
    context, cursor, observed = _stream_context(monkeypatch, [_row()])
    identities = list(context._transactions)
    if changed == "inactive":
        identities[0].is_active = False
        observed.return_value = None
    else:
        identities[0 if changed == "root" else 1] = SimpleNamespace(is_active=True)
        observed.return_value = tuple(identities)
    with pytest.raises(FamilyBlockUnavailable, match="no longer live"):
        await anext(context.headers())
    context.session.stream.assert_not_awaited()
    assert not context.complete and not cursor.closed


async def test_cancelled_cursor_closes_without_claiming_eof(monkeypatch):
    context, cursor, _ = _stream_context(monkeypatch, [_row()], pause=asyncio.Event())
    pending = asyncio.create_task(anext(context.headers()))
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert cursor.closed and not context.complete


@pytest.mark.parametrize(
    "field,value",
    [
        ("root_record_id", True),
        ("root_revision_id", 0),
        ("entity_binding_id", -1),
        ("family_revision_id", 2**63),
        ("child_count", -1),
        ("child_count", True),
        ("family_sha256", b"short"),
        ("logical_key_sha256", b"x" * 32),
        ("key_bytes", 1),
        ("logical_key", None),
    ],
)
def test_header_identity_and_exact_byte_checks_refuse_damage(field, value):
    native = _row()
    setattr(native, field, value)
    with pytest.raises(FamilyBlockUnavailable):
        base_read._checked_header(native, (9, 17), 16384)


def test_header_resource_refusal_is_not_an_input_rejection():
    native = _row()
    native.logical_key = None  # The SQL CASE avoids transferring an oversized value.
    with pytest.raises(FamilyBlockResourceLimit, match="page byte budget"):
        base_read._checked_header(native, (9, 17), 1)


@pytest.mark.parametrize("layout", ["canonical", "legacy", "block"])
def test_header_sql_preserves_complete_native_relationships(layout):
    definition = _request().definition
    pin = SimpleNamespace(family_id=None if layout == "canonical" else 17)
    binding = None
    if layout == "block":
        binding = SimpleNamespace(tables_by_role=FamilyBlockLayout(definition, {"details": 7}).tables(17))
    tables_by_role = base_read._header_tables(pin, binding)
    generation = SimpleNamespace(generation_id=9, dataset_id=1, definition_revision_id=92, schema_revision_id=3)
    statement = base_read._header_census(tables_by_role, generation, definition)
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    for name in (
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "root_record_id",
        "entity_binding_id",
        "key_contract_sha256",
        "value_sha256",
    ):
        assert name in sql
    assert sql.count("NOT (EXISTS") == 2 and 'COLLATE "C"' in sql
    assert root_key_contract_hash(definition) in compiled.params.values()
    assert "FOR UPDATE" not in sql
    assert "bool_or(" in sql and "count(*)" in sql and "max(octet_length(" in sql and "sum(" in sql
    assert "LIMIT" not in sql
    assert ("base_revision" in sql) is (layout != "block")
    header = base_read._header_statement(tables_by_role, generation, 4096, 257)
    rendered = str(header.compile(dialect=postgresql.dialect()))
    assert "CASE WHEN" in rendered and "ORDER BY base_root.logical_key_sha256" in rendered
    assert "LIMIT" not in rendered and header.get_execution_options()["yield_per"] == 257
    assert ("ci_snapshot_17" in rendered) is (layout != "canonical")


async def test_original_seal_producer_scope_and_capture_proof_are_required(monkeypatch):
    generation, _, seal, _ = _generation_seal_fixtures()
    generation.root_count = generation.family_count = 2
    seal.root_count = seal.family_count = seal.generation_family_count = 2
    producer = SimpleNamespace(
        state="completed",
        **{
            name: getattr(generation, name)
            for name in (
                "dataset_id",
                "execution_id",
                "definition_revision_id",
                "schema_revision_id",
                "capture_bundle_id",
            )
        },
    )
    model_by_type = {
        models.CustomImportGeneration: generation,
        models.CustomImportGenerationSeal: seal,
        models.CustomImportExecution: producer,
    }
    session = SimpleNamespace(get=AsyncMock(side_effect=lambda model, _identity: model_by_type[model]))
    source_proof = AsyncMock()
    monkeypatch.setattr(base_read.publication, "_validated_source_bundle_digest", source_proof)
    assert await base_read._base_identity(session, _request_with_base(definition_revision_id=92)) == (generation, seal)
    source_proof.assert_awaited_once_with(session, generation)
    producer.state = "running"
    with pytest.raises(base_read.publication.PublicationConflict, match="not completed"):
        await base_read._base_identity(session, _request_with_base())
    producer.state = "completed"
    seal.sealing_token_sha256 = b"x" * 32
    with pytest.raises(base_read.publication.PublicationConflict, match="authority"):
        await base_read._base_identity(session, _request_with_base())


async def test_base_definition_is_verified_under_its_original_revision(monkeypatch):
    definition = _request().definition
    generation = SimpleNamespace(dataset_id=1, definition_revision_id=92, schema_revision_id=3)
    revision = models.CustomImportDefinitionRevision(
        definition_revision_id=92,
        dataset_id=1,
        schema_revision_id=3,
        revision_number=definition.definition_revision,
        contract_version="custom-import/v1",
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    schema = models.CustomImportSchemaRevision(
        schema_revision_id=3,
        dataset_id=1,
        revision_number=definition.schema_revision,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    model_by_type = {models.CustomImportDefinitionRevision: revision, models.CustomImportSchemaRevision: schema}
    session = SimpleNamespace(get=AsyncMock(side_effect=lambda model, _identity: model_by_type[model]))
    registry = object()
    loader = AsyncMock(return_value=registry)
    monkeypatch.setattr(base_read, "load_registry", loader)
    parsed, actual_registry = await base_read._base_definition(session, generation)
    assert parsed == definition and actual_registry is registry
    scope = loader.call_args.args[1]
    assert (scope.dataset_id, scope.definition_revision_id, scope.schema_revision_id) == (1, 92, 3)
    assert scope.definition == definition
    revision.definition_sha256 = b"x" * 32
    with pytest.raises(ValueError, match="definition identity"):
        await base_read._base_definition(session, generation)


@pytest.mark.parametrize("missing", ["generation", "seal", "producer"])
async def test_missing_base_authority_cannot_become_absence(monkeypatch, missing):
    generation, _, seal, _ = _generation_seal_fixtures()
    model_by_type = {
        models.CustomImportGeneration: None if missing == "generation" else generation,
        models.CustomImportGenerationSeal: None if missing == "seal" else seal,
        models.CustomImportExecution: None if missing == "producer" else SimpleNamespace(dataset_id=1),
    }
    session = SimpleNamespace(get=AsyncMock(side_effect=lambda model, _identity: model_by_type[model]))
    digest = AsyncMock()
    monkeypatch.setattr(base_read.publication, "_validated_source_bundle_digest", digest)
    with pytest.raises(FamilyBlockUnavailable):
        await base_read._base_identity(session, _request_with_base())
    digest.assert_not_awaited()


async def test_metadata_load_cannot_cross_transaction_replacement(monkeypatch):
    context, _, observed = _stream_context(monkeypatch, [])
    monkeypatch.setattr(base_read, "_base_identity", AsyncMock(return_value=(context.generation, context.seal)))
    monkeypatch.setattr(base_read, "_base_definition", AsyncMock(return_value=(context.definition, context.registry)))
    monkeypatch.setattr(base_read, "_base_binding", AsyncMock(return_value=(context.pin, context.binding)))
    monkeypatch.setattr(base_read, "validate_sealed_projection", AsyncMock())
    monkeypatch.setattr(base_read, "_verify_leaf_pins", AsyncMock())
    observed.return_value = (SimpleNamespace(is_active=True), None, SimpleNamespace(is_active=True), None)
    with pytest.raises(FamilyBlockUnavailable, match="no longer live"):
        await base_read._open_base(context.session, context.request, None, context._transactions, context._buffer_bytes)


@pytest.mark.parametrize(
    "observed",
    [[(1, 101)], [(1, 101), (2, 101)], [(True, 101), (2, 102)], [(1, None), (2, 102)], [(1, True), (2, 102)]],
)
def test_native_role_oid_census_is_exact(observed):
    with pytest.raises(FamilyBlockUnavailable, match="identity census"):
        base_read._checked_oids(observed, {1, 2})


@pytest.mark.parametrize("origin", [None, 909])
async def test_original_snapshot_ledger_includes_registered_origin(origin):
    generation, _, _, _ = _generation_seal_fixtures()
    names = (
        *base_read._SCOPE,
        "generation_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
    )
    family = SimpleNamespace(
        family_id=17, frozen_at=object(), origin_table_oid=origin, **{name: getattr(generation, name) for name in names}
    )
    relation_rows = [(slot, slot + 100) for slot in range(1, 16)]
    session = SimpleNamespace(
        get=AsyncMock(return_value=family), execute=AsyncMock(return_value=SimpleNamespace(all=lambda: relation_rows))
    )
    pin = SimpleNamespace(family_id=17, legacy_snapshot=True)
    expected_oids = {oid for _slot, oid in relation_rows}
    if origin is not None:
        expected_oids.add(origin)
    assert await base_read._registered_oids(session, generation, pin) == expected_oids
    family.capture_bundle_id += 1
    with pytest.raises(FamilyBlockUnavailable, match="snapshot origin"):
        await base_read._registered_oids(session, generation, pin)


async def test_block_leaf_oids_come_from_the_same_pin_receipt():
    generation, _, _, _ = _generation_seal_fixtures()
    names = (
        *base_read._SCOPE,
        "generation_id",
        "execution_id",
        "capture_bundle_id",
        "producing_fence",
        "producing_token_sha256",
    )
    family = SimpleNamespace(
        family_id=17, frozen_at=object(), origin_table_oid=None, **{name: getattr(generation, name) for name in names}
    )
    by_slot = {slot: index + 100 for index, (slot, _name) in enumerate(ROLE_SPECS.values())}
    pin = SimpleNamespace(family_id=17, legacy_snapshot=False, checked_block_oids=AsyncMock(return_value=by_slot))
    session = SimpleNamespace(get=AsyncMock(return_value=family), execute=AsyncMock())
    assert await base_read._registered_oids(session, generation, pin) == set(by_slot.values())
    pin.checked_block_oids.assert_awaited_once_with(session)
    session.execute.assert_not_awaited()


async def test_explicit_canonical_pin_observes_all_fixed_role_oids(monkeypatch):
    identities = (object(), None, object(), None)
    monkeypatch.setattr(base_read, "_read_transaction_identity", AsyncMock(return_value=identities))
    names = [model.__tablename__ for model in base_read.SNAPSHOT_MODELS]
    resolver = AsyncMock(return_value=names)
    monkeypatch.setattr(base_read, "_canonical_names", resolver)
    relation_rows = [(ordinal, ordinal + 100) for ordinal in range(1, 16)]
    cursor = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [oid for _slot, oid in relation_rows]))
    session = SimpleNamespace(execute=AsyncMock(side_effect=[SimpleNamespace(all=lambda: relation_rows), cursor]))
    await base_read._verify_leaf_pins(session, None, SimpleNamespace(family_id=None), identities)
    assert "to_regclass" in str(session.execute.call_args_list[0].args[0])
    assert session.execute.call_args_list[0].args[1] == {"names": names}
    assert session.execute.call_args.args[1] == {"oids": [oid for _slot, oid in relation_rows]}


@pytest.mark.parametrize(
    "observed,valid", [([101, 202], True), ([101, 202, 303], True), ([101, 303], False), ([101, 101], False)]
)
async def test_actual_leaf_locks_require_oid_containment(monkeypatch, observed, valid):
    identities = (object(), None, object(), None)
    monkeypatch.setattr(base_read, "_read_transaction_identity", AsyncMock(return_value=identities))
    monkeypatch.setattr(base_read, "_registered_oids", AsyncMock(return_value={101, 202}))
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: observed)))
    )
    pin = SimpleNamespace(family_id=17)
    if valid:
        await base_read._verify_leaf_pins(session, None, pin, identities)
    else:
        with pytest.raises(FamilyBlockUnavailable, match="locks are incomplete"):
            await base_read._verify_leaf_pins(session, None, pin, identities)
    sql = str(session.execute.call_args.args[0])
    assert "pg_backend_pid()" in sql and "granted" in sql and "mode='AccessShareLock'" in sql
    assert session.execute.call_args.args[1] == {"oids": [101, 202]}


async def test_lock_census_rechecks_the_same_root_transaction(monkeypatch):
    identities = (object(), None, object(), None)
    monkeypatch.setattr(base_read, "_read_transaction_identity", AsyncMock(side_effect=[identities, None]))
    monkeypatch.setattr(base_read, "_registered_oids", AsyncMock(return_value={101}))
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [101])))
    )
    with pytest.raises(FamilyBlockUnavailable, match="pin transaction"):
        await base_read._verify_leaf_pins(session, None, SimpleNamespace(family_id=17), identities)


@pytest.mark.parametrize("snapshot,legacy", [(None, False), (17, True), (17, False)])
async def test_binding_uses_actual_base_scope_and_exact_layout(monkeypatch, snapshot, legacy):
    generation = SimpleNamespace(generation_id=9, dataset_id=1, definition_revision_id=92, schema_revision_id=3)
    pin = SimpleNamespace(family_id=snapshot, legacy_snapshot=legacy)
    resolver = AsyncMock(return_value=pin)
    canonical = AsyncMock()
    binding = object()
    block = AsyncMock(return_value=binding)
    monkeypatch.setattr(base_read, "resolve_generation_snapshot_binding", resolver)
    monkeypatch.setattr(base_read, "_pin_canonical_tables", canonical)
    monkeypatch.setattr(base_read, "resolve_pinned_family_block_binding", block)
    registry = SimpleNamespace(child_collection_slots={"details": 7})
    actual_pin, actual_binding = await base_read._base_binding(None, generation, _request().definition, registry)
    assert actual_pin is pin
    assert resolver.call_args.args[1].definition_revision_id == 92
    assert canonical.await_count == (1 if snapshot is None else 0)
    assert resolver.await_count == (2 if snapshot is None else 1)
    assert actual_binding is (binding if snapshot is not None and not legacy else None)
    assert block.await_count == (1 if snapshot is not None and not legacy else 0)


async def test_absent_base_never_opens_database_or_invents_scope():
    factory = Mock(side_effect=AssertionError("absent base has no reader"))
    async with base_read.pinned_base(factory, _request()) as context:
        assert context is None
    factory.assert_not_called()


@pytest.mark.parametrize("buffer_bytes", [True, 0, -1, "2048", 1.0])
async def test_invalid_buffer_never_opens_owned_session(buffer_bytes):
    factory = Mock(side_effect=AssertionError("invalid resource budget has no reader"))
    with pytest.raises(ValueError, match="positive byte budget"):
        async with base_read.pinned_base(factory, _request_with_base(), buffer_bytes=buffer_bytes):
            pytest.fail("invalid buffer opened a base")
    factory.assert_not_called()


async def test_context_owns_readonly_transaction_and_closes_active_cursor(monkeypatch):
    context, cursor, current = _stream_context(monkeypatch, [_row()])
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        finally:
            assert cursor.closed
            events.append("transaction_closed")

    @asynccontextmanager
    async def factory():
        events.append("session")
        try:
            yield context.session
        finally:
            events.append("session_closed")

    context.session.begin = transaction
    context.session.in_transaction = lambda: False
    context.session.new = context.session.dirty = context.session.deleted = ()
    context.session.no_autoflush = nullcontext()
    opener = AsyncMock(return_value=context)
    monkeypatch.setattr(base_read, "_open_base", opener)
    async with base_read.pinned_base(factory, _request_with_base()) as borrowed:
        assert borrowed is context
        await anext(borrowed.headers())
        assert not borrowed.complete
    assert events == ["session", "begin", "transaction_closed", "session_closed"]
    statements = [str(call.args[0]) for call in context.session.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    assert all("FOR UPDATE" not in statement for statement in statements)
    assert opener.call_args.args[3] is current.return_value
    assert opener.call_args.args[4] == context.request.page_byte_limit
    with pytest.raises(FamilyBlockUnavailable, match="no longer live"):
        await borrowed.check()
