# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Installed claims readers pin a complete published family before data reads."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from api.reference_family_reads import _claims_dictionary_overlay, claims_dictionary_tables, pin_claims_reader
from db.connection import Database
from process import mrf_address_publication as publication
from process import reference_family_archive as archive
from tests.test_api_reader_sessions import ReaderSession


def _reader(monkeypatch, *, missing=False, drift=False, binding=False, writable=False, families=("claims-pricing",)):
    """Use explicit host-only catalog leaves; native role/CAS proof remains separate."""
    names = sorted(
        name for importer in families for name in archive.reference_family_receive_spec(importer).table_names
    )
    pairs = tuple((name, oid) for oid, name in enumerate(names, 101))
    oid_by_name = dict(pairs)
    oid_by_name.update(code_catalog=201, code_crosswalk=202, current_generation=301, relation=302)
    monkeypatch.setattr(
        archive,
        "_relation_oid",
        AsyncMock(
            side_effect=lambda _session, _schema, name: (
                None if missing and name.startswith("claims_code_") else oid_by_name.get(name)
            )
        ),
    )
    monkeypatch.setattr(publication, "require_native_read_catalog", AsyncMock())
    binding_rows = [("published-generation", name, oid) for name, oid in pairs]
    if binding:
        binding_rows.pop()
    statement_result = SimpleNamespace(all=lambda: binding_rows)

    async def scalar(statement, parameters=None):
        """Resolve the actual locked name and fixed read-only authority predicate."""
        if "to_regclass(:name)" in str(statement):
            return oid_by_name[parameters["name"].split('"')[-2]] + int(drift)
        if "payload" in str(statement) or "left_row" in str(statement):
            return False
        return not writable

    async def execute(statement, parameters=None):
        """Return one actual current binding for each separately scoped native family."""
        if parameters and "importer" in parameters:
            selected_names = archive.reference_family_receive_spec(parameters["importer"]).table_names
            return SimpleNamespace(all=lambda: [row for row in binding_rows if row[1] in selected_names])
        return statement_result

    session = SimpleNamespace(
        info={},
        connection=AsyncMock(),
        rollback=AsyncMock(),
        execute=AsyncMock(side_effect=execute),
        scalar=AsyncMock(side_effect=scalar),
    )
    return session, pairs


@pytest.mark.asyncio
async def test_reader_pins_complete_family_and_shared_row_versions(monkeypatch):
    session, pairs = _reader(monkeypatch)
    assert await pin_claims_reader(session) == pairs
    session.connection.assert_awaited_once_with(execution_options={"isolation_level": "REPEATABLE READ"})
    lock = str(session.execute.await_args_list[0].args[0])
    assert lock.startswith("LOCK TABLE") and lock.endswith("ACCESS SHARE MODE NOWAIT")
    assert all('"' + name + '"' in lock for name, _oid in pairs)
    assert '"code_catalog"' in lock and '"code_crosswalk"' in lock
    binding_sql = " ".join(str(session.execute.await_args_list[1].args[0]).split())
    assert "JOIN hp_snapshot_retention.relation r USING(generation_id)" in binding_sql
    assert "heap.relname::text AS relation_name,r.relation_oid::bigint" in binding_sql
    assert "JOIN pg_catalog.pg_class heap ON heap.oid::bigint=r.relation_oid" in binding_sql
    assert "ORDER BY heap.relname" in binding_sql
    assert "r.relation_name" not in binding_sql and "r.table_name" not in binding_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["drift", "binding", "writable"])
async def test_reader_refuses_identity_publication_or_write_authority_drift(monkeypatch, failure):
    session, _pairs = _reader(monkeypatch, **{failure: True})
    with pytest.raises(RuntimeError):
        await pin_claims_reader(session)


@pytest.mark.asyncio
async def test_reader_uses_pinned_mirrors_without_a_full_shared_dictionary_comparison(monkeypatch):
    session, _pairs = _reader(monkeypatch)
    compare = AsyncMock()
    monkeypatch.setattr(archive, "_is_model_table_equal", compare)
    await pin_claims_reader(session)
    compare.assert_not_awaited()
    catalog, crosswalk = claims_dictionary_tables(session)
    assert "claims_code_catalog" in str(catalog) and "claims_code_crosswalk" in str(crosswalk)


def _seed_reader_catalog(connection):
    """Supply a changed shared key, unrelated key, later owned key and immutable original."""
    from db.models import CodeCatalog

    connection.execute(
        CodeCatalog.__table__.insert(),
        [
            {"code_system": "CPT", "code": "00001", "display_name": "replaced", "source": "other"},
            {"code_system": "CPT", "code": "00002", "display_name": "unrelated", "source": "other"},
            {
                "code_system": "CPT",
                "code": "00003",
                "display_name": "later",
                "source": archive._CLAIMS_DICTIONARY_SOURCE,
            },
        ],
    )

    connection.execute(
        archive.ClaimsCodeCatalog.__table__.insert(),
        {
            "code_system": "CPT",
            "code": "00001",
            "display_name": "pinned",
            "source": archive._CLAIMS_DICTIONARY_SOURCE,
        },
    )


@pytest.mark.asyncio
async def test_actual_codes_and_pricing_queries_use_the_admitted_dictionary_overlay():
    """Consumers use the pinned binding, rather than a registration-only reader fixture."""
    from api.endpoint import codes, pricing

    empty_result = MagicMock()
    empty_result.scalar.return_value = 0
    empty_result.__iter__.return_value = iter(())
    session = SimpleNamespace(
        info={"snapshot_claims_dictionary_tables": _claims_dictionary_overlay()},
        execute=AsyncMock(return_value=empty_result),
    )
    request = SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session))
    await codes.list_codes(request)
    catalog_statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(catalog_statements) == 2 and all("claims_code_catalog" in sql for sql in catalog_statements)
    session.execute.reset_mock()
    assert await pricing._query_crosswalk_edges(session, {("CPT", "00001")}) == []
    assert "claims_code_crosswalk" in str(session.execute.await_args.args[0])


def _seed_reader_crosswalk(connection):
    """Use the real coupled crosswalk key, with one changed and one unrelated edge."""
    from db.models import CodeCrosswalk

    connection.execute(
        CodeCrosswalk.__table__.insert(),
        [
            {
                "from_system": "CPT",
                "from_code": code,
                "to_system": "HCPCS",
                "to_code": code,
                "source": "other",
                "match_type": kind,
            }
            for code, kind in (("00001", "replaced"), ("00002", "unrelated"))
        ],
    )
    connection.execute(
        archive.ClaimsCodeCrosswalk.__table__.insert(),
        {
            "from_system": "CPT",
            "from_code": "00001",
            "to_system": "HCPCS",
            "to_code": "00001",
            "source": archive._CLAIMS_DICTIONARY_SOURCE,
            "match_type": "pinned",
        },
    )


def test_scoped_reader_overlay_keeps_original_rows_and_preserves_unrelated_keys():
    """SQL set semantics are exercised on an ephemeral host database, not PostgreSQL proof."""
    from sqlalchemy import create_engine, select

    from db.models import CodeCatalog, CodeCrosswalk

    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS mrf")
        for model in (CodeCatalog, CodeCrosswalk, *archive._CLAIMS_SCOPED_MODELS):
            model.__table__.create(connection)
        _seed_reader_catalog(connection)
        _seed_reader_crosswalk(connection)
        catalog, crosswalk = _claims_dictionary_overlay()
        assert [
            (entry.code, entry.display_name) for entry in connection.execute(select(catalog).order_by(catalog.c.code))
        ] == [("00001", "pinned"), ("00002", "unrelated")]
        assert [
            (entry.from_code, entry.match_type)
            for entry in connection.execute(select(crosswalk).order_by(crosswalk.c.from_code))
        ] == [("00001", "pinned"), ("00002", "unrelated")]
    engine.dispose()


@pytest.mark.asyncio
async def test_uninstalled_ordinary_reader_keeps_existing_entrypoint(monkeypatch):
    session, _pairs = _reader(monkeypatch, missing=True)
    assert await pin_claims_reader(session) is None
    session.rollback.assert_awaited_once()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("families", (("drug-claims",), ("claims-pricing", "drug-claims")))
async def test_reader_pins_each_complete_installed_family_before_one_dictionary_overlay(monkeypatch, families):
    """One transaction locks and authenticates all installed serving heaps, not just physician data."""
    session, pairs = _reader(monkeypatch, families=families)
    assert await pin_claims_reader(session) == pairs
    lock = str(session.execute.await_args_list[0].args[0])
    assert all('"' + name + '"' in lock for name, _oid in pairs)
    bindings = [call.args[1]["importer"] for call in session.execute.await_args_list if len(call.args) == 2]
    assert bindings == list(families)
    assert "drug_claims_code_catalog" in str(claims_dictionary_tables(session)[0])
    if len(families) == 2:
        overlap_queries = [
            str(call.args[0]) for call in session.scalar.await_args_list if "left_row" in str(call.args[0])
        ]
        assert len(overlap_queries) == 2 and all("IS DISTINCT FROM" in sql for sql in overlap_queries)


@pytest.mark.asyncio
async def test_multifamily_reader_refuses_conflicting_native_dictionary_payloads(monkeypatch):
    from api.reference_family_reads import _require_dictionary_overlap

    session = SimpleNamespace(scalar=AsyncMock(return_value=True))
    with pytest.raises(RuntimeError, match="payloads conflict"):
        await _require_dictionary_overlap(session, ("claims-pricing", "drug-claims"))


@pytest.mark.asyncio
async def test_actual_provider_filter_uses_the_admitted_rx_dictionary(monkeypatch):
    """The provider endpoint's raw crosswalk seam must participate in the same request pin."""
    from api.endpoint import npi

    session = SimpleNamespace(
        info={"snapshot_claims_dictionary_tables": _claims_dictionary_overlay(("claims-pricing", "drug-claims"))},
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [("7",)])),
    )
    monkeypatch.setattr(npi, "_is_table_available", AsyncMock(return_value=True))
    assert await npi._resolve_internal_filter_codes(["00001"], "NDC", "HP_RX_CODE", "rx", session=session) == (
        [7],
        "crosswalk",
    )
    statement, parameters = session.execute.await_args.args
    assert "drug_claims_code_crosswalk" in str(statement) and "claims_code_crosswalk" in str(statement)
    assert parameters["input_codes"] == ["00001"]


def test_multifamily_native_overlay_preserves_shared_keys_and_rx_endpoint_payloads():
    """Exercise actual indexed overlay set semantics locally; native authority remains separate."""
    from sqlalchemy import create_engine, select

    from db.models import CodeCatalog, CodeCrosswalk

    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS mrf")
        for model in (CodeCatalog, CodeCrosswalk, *archive._CLAIMS_SCOPED_MODELS, *archive._DRUG_SCOPED_MODELS):
            model.__table__.create(connection)
        _seed_reader_catalog(connection)
        connection.execute(
            CodeCatalog.__table__.insert(),
            [
                {"code_system": "NDC", "code": "11111", "display_name": "later external payload", "source": "other"},
                {
                    "code_system": "RXNORM",
                    "code": "22222",
                    "display_name": "unrelated external payload",
                    "source": "other",
                },
            ],
        )
        connection.execute(
            archive.DrugClaimsCodeCatalog.__table__.insert(),
            {
                "code_system": "NDC",
                "code": "11111",
                "display_name": "pinned external payload",
                "source": "drug_api_snapshot",
            },
        )
        catalog, _crosswalk = _claims_dictionary_overlay(("claims-pricing", "drug-claims"))
        observed_rows = [
            (entry.code_system, entry.code, entry.display_name)
            for entry in connection.execute(select(catalog).order_by(catalog.c.code_system, catalog.c.code))
        ]
        assert observed_rows == [
            ("CPT", "00001", "pinned"),
            ("CPT", "00002", "unrelated"),
            ("NDC", "11111", "pinned external payload"),
            ("RXNORM", "22222", "unrelated external payload"),
        ]
    engine.dispose()


def _middleware_session(monkeypatch, *, reader_enabled=True):
    """Reuse actual request middleware with a host-only session lifecycle."""
    callbacks_by_name = {}

    class App:
        """Record only the application's existing lifecycle callbacks."""

        def listener(self, _name):
            """Listeners do not run while binding the synthetic request."""
            return lambda callback: callback

        def middleware(self, name):
            """Keep the same registered callback for request and terminal cleanup."""

            def register(callback):
                """Register the actual database request/response implementation."""
                callbacks_by_name[name] = callback
                return callback

            return register

    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", str(reader_enabled))
    session = ReaderSession()
    session.close = AsyncMock(wraps=session.close)
    database = Database()
    database.session_factory = lambda: session
    reader = Database(session_factory=lambda: session)
    reader._reader_login = ("reader_test", "")
    monkeypatch.setattr(database, "_connect_reader", AsyncMock(return_value=reader))
    database.init_app(App())
    return callbacks_by_name, session


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,is_claims_read",
    [
        ("/api/v1/pricing/providers", True),
        ("/api/v1/codes/CPT", True),
        ("/api/v1/npi/near/", True),
        ("/api/v1/coverage/statistics", True),
        ("/api/v1/geo/zip", False),
    ],
)
async def test_request_entrypoint_binds_pinning_only_to_real_pricing_consumers(monkeypatch, path, is_claims_read):
    callbacks, session = _middleware_session(monkeypatch)
    from api import reference_family_reads

    pin = AsyncMock()
    monkeypatch.setattr(reference_family_reads, "pin_claims_reader", pin)
    request = SimpleNamespace(path=path, ctx=SimpleNamespace())
    try:
        await callbacks["request"](request)
        assert request.ctx.sa_session is session
        assert pin.await_count == int(is_claims_read)
    finally:
        await callbacks["response"](request, SimpleNamespace(status=200))
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_request_entrypoint_refuses_unavailable_reader_with_neutral_error(monkeypatch):
    from sanic.exceptions import ServiceUnavailable

    from api import reference_family_reads

    callbacks, session = _middleware_session(monkeypatch)
    monkeypatch.setattr(
        reference_family_reads, "pin_claims_reader", AsyncMock(side_effect=RuntimeError("identity changed"))
    )
    request = SimpleNamespace(path="/api/v1/pricing/providers", ctx=SimpleNamespace())
    try:
        with pytest.raises(ServiceUnavailable, match="temporarily unavailable"):
            await callbacks["request"](request)
    finally:
        await callbacks["response"](request, SimpleNamespace(status=503))
    session.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/api/v1/pricing/providers", "/api/v1/codes/CPT", "/api/v1/npi/near/"])
async def test_writer_rollout_does_not_attempt_reader_pinning(monkeypatch, path):
    from api import reference_family_reads

    callbacks, session = _middleware_session(monkeypatch, reader_enabled=False)
    pin = AsyncMock(side_effect=RuntimeError("Writer is not a Reader"))
    monkeypatch.setattr(reference_family_reads, "pin_claims_reader", pin)
    request = SimpleNamespace(path=path, ctx=SimpleNamespace())
    try:
        await callbacks["request"](request)
        assert request.ctx.sa_session is session
        pin.assert_not_awaited()
    finally:
        await callbacks["response"](request, SimpleNamespace(status=200))
    session.close.assert_awaited_once()
