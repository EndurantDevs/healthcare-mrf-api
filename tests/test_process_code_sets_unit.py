# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, call

import pytest

from process import code_sets_mutable_publication as mutable

code_sets = importlib.import_module("process.code_sets")


POS_HTML = """
<table>
  <tr><th>Place of Service Code(s)</th><th>Place of Service Name</th><th>Place of Service Description</th></tr>
  <tr><th scope="row">23</th><td>Emergency Room - Hospital</td><td>A portion of a hospital where emergency care is provided.</td></tr>
  <tr><th scope="row">28-30</th><td>Unassigned</td><td>N/A</td></tr>
</table>
"""


RC_HTML = """
<table>
  <tr><th>Code</th><th>Display</th></tr>
  <tr><td>0450</td><td>EMERGENCY ROOM - GENERAL CLASSIFICATION</td></tr>
  <tr><td>0981</td><td>PROFESSIONAL FEES - EMERGENCY ROOM SERVICES</td></tr>
</table>
"""


def _pos_html_for_codes(codes: list[str]) -> str:
    body = "".join(f"<tr><td>{code}</td><td>POS {code}</td><td>Description {code}</td></tr>" for code in codes)
    return f"<table>{body}</table>"


def _revenue_html_for_codes(codes: list[str]) -> str:
    body = "".join(f"<tr><td>{code}</td><td>Revenue {code}</td></tr>" for code in codes)
    return f"<table>{body}</table>"


def _stub_import_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    pos_html: str,
    rc_html: str,
) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    ensure_database = AsyncMock()
    ensure_catalog = AsyncMock()
    prepare = AsyncMock(side_effect=lambda _db, _ctx, **options: {**options["metrics"], "status": "prepared"})
    source_html_by_url = {
        code_sets.DEFAULT_POS_URL: pos_html,
        code_sets.DEFAULT_RC_URL: rc_html,
    }

    monkeypatch.setattr(code_sets, "ensure_database", ensure_database)
    monkeypatch.setattr(code_sets, "_ensure_code_catalog", ensure_catalog)
    monkeypatch.setattr(code_sets, "prepare_catalog_handoff", prepare)
    monkeypatch.setattr(code_sets.db, "status", AsyncMock())

    monkeypatch.setattr(
        code_sets,
        "_download_text",
        lambda url: source_html_by_url[url],
    )
    return ensure_database, ensure_catalog, prepare


def test_parse_pos_rows_expand_ranges_and_normalize_html_text():
    source_html = """
    <table>
      <tr>
        <td>7-8</td>
        <td>Office &amp; Clinic<br>After-hours&nbsp;unit</td>
        <td>Professional &amp; technical<br/>services</td>
      </tr>
    </table>
    """

    code_rows = code_sets.parse_pos_code_rows(source_html)

    assert [code_row.code for code_row in code_rows] == ["07", "08"]
    assert code_rows[0].display_name == "Office & Clinic After-hours unit"
    assert code_rows[0].short_description == code_rows[0].display_name
    assert code_rows[0].long_description == "Professional & technical services"
    assert code_rows[0].source == code_sets.SOURCE_POS


def test_parse_pos_rows_skip_malformed_header_blank_code_and_blank_name():
    source_html = """
    <td>outside a row</td>
    <table>
      <tr></tr>
      <tr><td>one cell</td></tr>
      <tr><td>11</td><td>Place of Service Name</td><td>Description</td></tr>
      <tr><td>&nbsp;</td><td>Blank code</td><td>Description</td></tr>
      <tr><td>12</td><td>&nbsp;</td><td>Description</td></tr>
      <tr><td>13</td><td>Valid facility</td><td>&nbsp;</td></tr>
    </table>
    """

    assert code_sets.parse_pos_code_rows(source_html) == [
        code_sets.CodeSetRow(
            code_system="POS",
            code="13",
            display_name="Valid facility",
            short_description="Valid facility",
            long_description=None,
            source=code_sets.SOURCE_POS,
        )
    ]


def test_parse_revenue_rows_normalize_text_and_skip_non_data_rows():
    source_html = """
    <table>
      <tr><td>one cell</td></tr>
      <tr><td>Revenue Code 0001</td><td>Header</td></tr>
      <tr><td>&nbsp;</td><td>Blank code</td></tr>
      <tr><td>0002</td><td>&nbsp;</td></tr>
      <tr><td>0003</td><td>Facility &amp; service<br>line</td></tr>
    </table>
    """

    assert code_sets.parse_revenue_code_rows(source_html) == [
        code_sets.CodeSetRow(
            code_system="RC",
            code="0003",
            display_name="Facility & service line",
            short_description="Facility & service line",
            long_description=None,
            source=code_sets.SOURCE_RC,
        )
    ]


@pytest.mark.parametrize(
    ("raw_code", "width", "expected_codes"),
    [
        ("28 - 30", 2, ["28", "29", "30"]),
        ("30-28", 2, []),
        ("1-102", 2, []),
        ("Code 7", 2, ["07"]),
        ("not assigned", 4, []),
        ("0450", 4, ["0450"]),
    ],
)
def test_expand_code_range_accepts_numeric_codes_and_rejects_invalid_ranges(
    raw_code,
    width,
    expected_codes,
):
    assert code_sets._expand_code_range(raw_code, width) == expected_codes


def test_parse_pos_code_rows_expands_ranges():
    code_rows = code_sets.parse_pos_code_rows(POS_HTML)

    code_row_by_code = {code_row.code: code_row for code_row in code_rows}
    assert code_row_by_code["23"].display_name == "Emergency Room - Hospital"
    assert code_row_by_code["23"].source == code_sets.SOURCE_POS
    assert code_row_by_code["28"].display_name == "Unassigned"
    assert code_row_by_code["30"].display_name == "Unassigned"


def test_parse_revenue_code_rows():
    code_rows = code_sets.parse_revenue_code_rows(RC_HTML)

    code_row_by_code = {code_row.code: code_row for code_row in code_rows}
    assert code_row_by_code["0450"].display_name == "EMERGENCY ROOM - GENERAL CLASSIFICATION"
    assert code_row_by_code["0981"].source == code_sets.SOURCE_RC


def test_modifier_code_rows_include_pricing_api_contract_codes():
    code_rows = code_sets.modifier_code_rows()

    code_row_by_code = {code_row.code: code_row for code_row in code_rows}
    assert set(code_row_by_code) == {"26", "95", "GQ", "NU", "QW", "RR", "TC", "UE"}
    assert code_row_by_code["26"].display_name == "Professional component"
    assert code_row_by_code["TC"].display_name == "Technical component"
    assert code_row_by_code["TC"].source == code_sets.SOURCE_MODIFIER


class _FakeHeaders:
    def __init__(self, charset):
        self.charset = charset

    def get_content_charset(self):
        return self.charset


class _FakeResponse:
    def __init__(self, payload: bytes, charset: str | None):
        self.payload = payload
        self.headers = _FakeHeaders(charset)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


@pytest.mark.parametrize(
    ("payload", "charset", "expected_text"),
    [
        ("café".encode(), None, "café"),
        ("café".encode("latin-1"), "latin-1", "café"),
    ],
)
def test_download_text_honors_response_charset_with_utf8_fallback(
    monkeypatch,
    payload,
    charset,
    expected_text,
):
    observed_requests = []

    def fake_urlopen(request, timeout):
        observed_requests.append((request, timeout))
        return _FakeResponse(payload, charset)

    monkeypatch.setattr(code_sets.urllib.request, "urlopen", fake_urlopen)

    assert code_sets._download_text("https://example.test/codes") == expected_text
    request, timeout = observed_requests[0]
    assert request.full_url == "https://example.test/codes"
    assert request.get_header("User-agent") == "HealthPorta code-set importer"
    assert timeout == 120


@pytest.mark.asyncio
async def test_ensure_code_catalog_does_not_alter_live_shape(monkeypatch):
    create_table = AsyncMock()
    execute_status = AsyncMock()
    monkeypatch.setattr(code_sets.db, "create_table", create_table)
    monkeypatch.setattr(code_sets.db, "status", execute_status)

    await code_sets._ensure_code_catalog("catalog_schema")

    create_table.assert_awaited_once_with(
        code_sets.CodeCatalog.__table__,
        checkfirst=True,
    )
    execute_status.assert_not_awaited()


def test_code_payloads_preserve_first_duplicate_per_system_and_code():
    code_rows = [
        code_sets.CodeSetRow("RC", "0450", "First display", source=code_sets.SOURCE_RC),
        code_sets.CodeSetRow("RC", "0450", "Later display", source=code_sets.SOURCE_RC),
        code_sets.CodeSetRow("POS", "0450", "Different system", source=code_sets.SOURCE_POS),
    ]
    payloads = code_sets._code_payloads(code_rows)
    assert len(payloads) == 2
    first_params, second_params = payloads
    assert first_params["display_name"] == "First display"
    assert first_params["source"] == code_sets.SOURCE_RC
    assert second_params["code_system"] == "POS"
    assert first_params["updated_at"] == second_params["updated_at"]
    assert first_params["is_active"] is True


def test_code_payloads_accept_an_empty_collection():
    assert code_sets._code_payloads([]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_mutable_publication_is_explicit_and_never_falls_back_to_native(monkeypatch, failure):
    from process import code_sets_mutable_publication as mutable

    monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", "legacy-mutable")
    _, ensure_catalog, prepare = _stub_import_dependencies(monkeypatch, pos_html=POS_HTML, rc_html=RC_HTML)
    publisher = AsyncMock(
        side_effect=RuntimeError("mutable refusal") if failure else None, return_value={"status": "succeeded"}
    )
    monkeypatch.setattr(mutable, "publish_mutable_code_sets", publisher)
    captured_by_field = {"storage": {}, "generation": {}}
    monkeypatch.setattr(mutable, "capture_mutable_code_sets", AsyncMock(return_value=(captured_by_field, None)))
    context_by_field = {
        "context": {
            "control_run_id": "synthetic-run",
            "_control_attempt_id": "synthetic-attempt",
            "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
        }
    }
    if failure:
        with pytest.raises(RuntimeError, match="mutable refusal"):
            await code_sets.import_code_sets(_control_context=context_by_field)
    else:
        assert await code_sets.import_code_sets(_control_context=context_by_field) == {"status": "succeeded"}
    prepare.assert_not_awaited()
    ensure_catalog.assert_not_awaited()
    assert publisher.await_args.args == (code_sets.db, context_by_field)
    assert publisher.await_args.kwargs["expected"] == captured_by_field
    assert {code_row["source"] for code_row in publisher.await_args.kwargs["payloads"]} == {
        code_sets.SOURCE_POS,
        code_sets.SOURCE_RC,
        code_sets.SOURCE_MODIFIER,
    }


@pytest.mark.asyncio
async def test_invalid_publication_mode_refuses_before_database_or_download(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", "unknown")
    ensure = AsyncMock()
    monkeypatch.setattr(code_sets, "ensure_database", ensure)
    with pytest.raises(RuntimeError, match="unsupported code-set publication mode"):
        await code_sets.import_code_sets()
    ensure.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["main", "import_code_sets"])
@pytest.mark.parametrize("failure", ["standalone", "incomplete", "test-mode", "schema", "transaction", "reader"])
async def test_invalid_mutable_request_refuses_before_any_database_or_source_io(monkeypatch, entrypoint, failure):
    monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", "legacy-mutable")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "unsafe.schema" if failure == "schema" else "mrf")
    initialize, ensure, download = AsyncMock(), AsyncMock(), Mock()
    monkeypatch.setattr(code_sets, "init_db", initialize)
    monkeypatch.setattr(code_sets, "ensure_database", ensure)
    monkeypatch.setattr(code_sets, "_download_text", download)
    monkeypatch.setattr(code_sets.db, "_transaction_binding", lambda: object() if failure == "transaction" else None)
    monkeypatch.setattr(code_sets.db, "_reader_binding", lambda: object() if failure == "reader" else None)
    context_by_field = {
        "context": {
            "control_run_id": "synthetic-run",
            "_control_attempt_id": "synthetic-attempt",
            "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
        }
    }
    if failure == "incomplete":
        context_by_field["context"].pop("_control_attempt_id")
    with pytest.raises(RuntimeError):
        await getattr(code_sets, entrypoint)(
            test_mode=failure == "test-mode", _control_context=None if failure == "standalone" else context_by_field
        )
    initialize.assert_not_awaited()
    ensure.assert_not_awaited()
    download.assert_not_called()


@pytest.mark.asyncio
async def test_direct_mutable_publisher_revalidates_attempt_before_transaction(monkeypatch):
    from process import code_sets_mutable_publication as mutable

    transaction = Mock()
    monkeypatch.setattr(code_sets.db, "transaction", transaction)
    with pytest.raises(RuntimeError, match="full managed attempt"):
        await mutable.publish_mutable_code_sets(code_sets.db, None, schema="mrf", payloads=[], metrics={})
    transaction.assert_not_called()


def test_mutable_committed_result_uses_the_existing_terminal_projection():
    from process.control_lifecycle import _committed_target_result

    result_by_field = {"status": "succeeded", "publication_mode": "legacy-mutable"}
    context_by_field = {
        "context": {"control_run_terminal_committed": True, "_control_committed_result": result_by_field}
    }
    assert _committed_target_result(context_by_field, target_module="process.code_sets") == result_by_field
    context_by_field["context"]["control_run_terminal_committed"] = False
    assert _committed_target_result(context_by_field, target_module="process.code_sets") is None


@pytest.fixture
def mutable_completion():
    attempt_by_field = {
        "run_id": "synthetic-run",
        "attempt_id": "synthetic-attempt",
        "attempt_started_at": "2026-01-01",
    }
    storage_by_field = {"database_oid": 11, "schema_oid": 12, "owner_oid": 13, "relation_oids": (14, 15, 16)}
    receipt_by_field = mutable._publication_receipt(attempt_by_field, storage_by_field, {"local_generation": 1}, {}, 3)
    row_by_field = {
        "status": "succeeded",
        "phase_detail": "code sets published",
        "error": None,
        "heartbeat_at": datetime(2026, 1, 1, tzinfo=UTC),
        "finished_at": datetime(2026, 1, 1, tzinfo=UTC),
        "metrics": {mutable.METRIC: receipt_by_field},
    }
    return attempt_by_field, storage_by_field, row_by_field


@pytest.mark.parametrize(
    "path,value",
    [
        (("status",), "running"),
        (("phase_detail",), "different phase"),
        (("error",), {"code": "synthetic-error"}),
        (("heartbeat_at",), None),
        (("finished_at",), "2026-01-01"),
        (("metrics",), {}),
        (("metrics", mutable.METRIC), []),
        (("metrics", mutable.METRIC, "contract"), "different-contract"),
        (("metrics", mutable.METRIC, "attempt"), {}),
        (("metrics", mutable.METRIC, "storage"), {}),
        (("metrics", mutable.METRIC, "status"), "running"),
        (("metrics", mutable.METRIC, "publication_mode"), "different-mode"),
        (("metrics", mutable.METRIC, "generation"), {"local_generation": 2}),
    ],
)
def test_mutable_completion_refuses_changed_durable_postimages(mutable_completion, path, value):
    attempt_by_field, storage_by_field, row_by_field = mutable_completion
    expected = deepcopy(row_by_field["metrics"][mutable.METRIC])
    target = row_by_field
    for name in path[:-1]:
        target = target[name]
    target[path[-1]] = value
    with pytest.raises(RuntimeError, match="committed attempt differs"):
        mutable._confirmed(row_by_field, attempt_by_field, storage_by_field, expected)


@pytest.mark.parametrize("bound", ["empty", "rows", "bytes", "source"])
def test_mutable_payload_bounds_refuse_invalid_batches(bound):
    payloads = [{"source": source_name, "code_system": system} for source_name, system in mutable.codes.SOURCES]
    assert mutable._validate_payloads(payloads) is None
    if bound == "empty":
        payloads = []
    elif bound == "rows":
        payloads *= mutable.MAX_ROWS // len(payloads) + 1
    elif bound == "bytes":
        payloads[0]["display_name"] = "x" * mutable.MAX_BYTES
    else:
        payloads[0]["source"] = "different-source"
    with pytest.raises(RuntimeError, match="input scope or bound differs"):
        mutable._validate_payloads(payloads)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,has_receipt,changed_check,expected_error",
    [
        ("succeeded", True, 0, "reconciliation storage changed"),
        ("succeeded", True, 1, "reconciliation storage changed"),
        ("failed", False, None, "commit outcome differs"),
        ("running", True, None, "commit outcome differs"),
        ("running", False, None, None),
        ("canceling", False, None, None),
        ("canceled", False, None, None),
        ("succeeded", True, None, None),
    ],
)
async def test_mutable_reconciliation_requires_stable_storage_and_exact_outcome(
    monkeypatch, mutable_completion, status, has_receipt, changed_check, expected_error
):
    attempt_by_field, storage_by_field, row_by_field = mutable_completion
    expected = deepcopy(row_by_field["metrics"][mutable.METRIC])
    row_by_field["status"] = status
    if not has_receipt:
        row_by_field["metrics"] = {}
    storage_observations = [storage_by_field, storage_by_field]
    if changed_check is not None:
        storage_observations[changed_check] = {**storage_by_field, "database_oid": 99}
    session = Mock(execute=AsyncMock(), scalar=AsyncMock(return_value="pg_catalog,pg_temp"))

    @asynccontextmanager
    async def transaction():
        yield session

    database = Mock(transaction=transaction)
    storage_reader = AsyncMock(side_effect=storage_observations)
    run_reader = AsyncMock(return_value=row_by_field)
    monkeypatch.setattr(mutable, "_mutable_storage", storage_reader)
    monkeypatch.setattr(mutable, "_run", run_reader)
    if expected_error is not None:
        with pytest.raises(RuntimeError, match=expected_error):
            await mutable._reconcile(database, "mrf", attempt_by_field, storage_by_field, expected)
    else:
        assert await mutable._reconcile(database, "mrf", attempt_by_field, storage_by_field, expected) == row_by_field
    assert run_reader.await_count == (0 if changed_check == 0 else 1)
    if run_reader.await_count:
        run_reader.assert_awaited_once_with(session, "mrf", attempt_by_field, lock="SHARE", allow_cancel=True)
    statements = [str(execution.args[0]) for execution in session.execute.await_args_list]
    assert "SET LOCAL lock_timeout='30s'" in statements
    assert "SET LOCAL statement_timeout='30s'" in statements


@pytest.mark.asyncio
async def test_mutable_reconciliation_propagates_a_completed_cancellation(monkeypatch):
    reconcile = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr(mutable, "_reconcile", reconcile)
    with pytest.raises(asyncio.CancelledError):
        await mutable._await_reconciliation(None, "mrf", {}, {}, {})
    reconcile.assert_awaited_once_with(None, "mrf", {}, {}, {})


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["running", "succeeded", "failed", "finished", "wrong-receipt"])
async def test_mutable_acquisition_captures_freshness_or_returns_exact_completion(
    monkeypatch, mutable_completion, outcome
):
    attempt_by_field, storage_by_field, row_by_field = mutable_completion
    if outcome != "succeeded":
        row_by_field = {**row_by_field, "status": "running", "finished_at": None}
    if outcome == "failed":
        row_by_field["status"] = "failed"
    if outcome == "finished":
        row_by_field["finished_at"] = datetime(2026, 1, 1, tzinfo=UTC)
    if outcome == "wrong-receipt":
        row_by_field.update(status="succeeded", finished_at=datetime(2026, 1, 1, tzinfo=UTC))
        row_by_field["metrics"][mutable.METRIC]["contract"] = "different-contract"
    session = Mock(execute=AsyncMock(), scalar=AsyncMock(return_value="pg_catalog,pg_temp"))

    @asynccontextmanager
    async def transaction():
        yield session

    database = Mock(transaction=transaction)
    database._transaction_binding.return_value = None
    database._reader_binding.return_value = None
    monkeypatch.setattr(mutable, "_lock_mutable_storage", AsyncMock(return_value=storage_by_field))
    monkeypatch.setattr(mutable, "_run", AsyncMock(return_value=row_by_field))
    generation = AsyncMock(return_value={"local_generation": 0})
    monkeypatch.setattr(mutable.codes, "read_generation", generation)
    context_by_field = {
        "context": {
            "control_run_id": attempt_by_field["run_id"],
            "_control_attempt_id": attempt_by_field["attempt_id"],
            "_control_attempt_started_at": attempt_by_field["attempt_started_at"],
        }
    }
    if outcome in {"failed", "finished", "wrong-receipt"}:
        with pytest.raises(RuntimeError, match="not running|committed attempt differs"):
            await mutable.capture_mutable_code_sets(database, context_by_field, schema="mrf")
        generation.assert_not_awaited()
    else:
        captured, completed = await mutable.capture_mutable_code_sets(database, context_by_field, schema="mrf")
        if outcome == "running":
            assert captured == {
                "storage": mutable.generation_value(storage_by_field),
                "generation": {"local_generation": 0},
            }
            assert completed is None
            generation.assert_awaited_once_with(session, "mrf", lock=True)
        else:
            assert captured is None and completed == row_by_field["metrics"][mutable.METRIC]
            assert context_by_field["context"]["control_run_terminal_committed"] is True
            generation.assert_not_awaited()


@pytest.mark.asyncio
async def test_mutable_import_returns_completion_before_downloading(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", "legacy-mutable")
    completed_by_field = {"status": "succeeded"}
    monkeypatch.setattr(code_sets, "ensure_database", AsyncMock())
    monkeypatch.setattr(mutable, "capture_mutable_code_sets", AsyncMock(return_value=(None, completed_by_field)))
    download, publish = Mock(), AsyncMock()
    monkeypatch.setattr(code_sets, "_download_text", download)
    monkeypatch.setattr(mutable, "publish_mutable_code_sets", publish)
    context_by_field = {
        "context": {
            "control_run_id": "synthetic-run",
            "_control_attempt_id": "synthetic-attempt",
            "_control_attempt_started_at": "2026-01-01",
        }
    }
    assert await code_sets.import_code_sets(_control_context=context_by_field) == completed_by_field
    download.assert_not_called()
    publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["type", "generated", "identity", "default", "singleton", "id-default"])
async def test_mutable_generation_shape_requires_ordinary_columns_and_one_generation(change):
    columns = [
        ["id", "smallint", True, "", "", False],
        ["local_lineage_id", "uuid", True, "", "", False],
        ["local_generation", "bigint", True, "", "", False],
        ["origin_lineage_id", "uuid", False, "", "", False],
        ["origin_generation", "bigint", False, "", "", False],
        ["published_at", "timestamp with time zone", False, "", "", False],
        ["code_catalog_oid", "bigint", False, "", "", False],
        ["row_count", "bigint", False, "", "", False],
        ["row_sha256", "text", False, "", "", False],
    ]
    changes_by_kind = {"type": (1, "numeric"), "generated": (3, "s"), "identity": (4, "a"), "default": (5, True)}
    if change in changes_by_kind:
        position, value = changes_by_kind[change]
        columns[7][position] = value
    if change == "id-default":
        columns[0][5] = True
    result = Mock()
    result.all.return_value = columns
    session = Mock(execute=AsyncMock(return_value=result), scalar=AsyncMock(return_value=change != "singleton"))
    if change == "id-default":
        assert await mutable._generation_shape(session, "mrf", 15) is None
    else:
        with pytest.raises(RuntimeError, match="generation (model|singleton) differs"):
            await mutable._generation_shape(session, "mrf", 15)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["storage", "closed", "none"])
async def test_mutable_lock_rechecks_storage_before_accepting_mutable_binding(monkeypatch, mutable_completion, failure):
    _, storage_by_field, _ = mutable_completion
    session = Mock(execute=AsyncMock())
    observed = {**storage_by_field, "database_oid": 99} if failure == "storage" else storage_by_field
    monkeypatch.setattr(mutable, "_mutable_storage", AsyncMock(side_effect=[storage_by_field, observed]))
    lock = AsyncMock()
    pin = AsyncMock(return_value=failure == "closed")
    shape = AsyncMock()
    monkeypatch.setattr(mutable.native, "_lock_family", lock)
    monkeypatch.setattr(mutable.binding, "pin_catalog_source", pin)
    monkeypatch.setattr(mutable, "_generation_shape", shape)
    if failure == "none":
        assert await mutable._lock_mutable_storage(session, "mrf") == storage_by_field
        shape.assert_awaited_once_with(session, "mrf", 15)
    else:
        with pytest.raises(RuntimeError, match="storage identity changed|cannot target closed"):
            await mutable._lock_mutable_storage(session, "mrf")
        shape.assert_not_awaited()
    assert lock.await_args_list == [
        call(session, "mrf", ("code_catalog", mutable.codes.TABLE), "SHARE ROW EXCLUSIVE", nowait=True),
        call(session, "mrf", ("import_run",), "ACCESS SHARE", nowait=True),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["foreign", "generation", "storage", "none"])
async def test_mutable_row_application_refuses_changed_ownership_and_generation(
    monkeypatch, mutable_completion, failure
):
    _, storage_by_field, _ = mutable_completion
    session = Mock(execute=AsyncMock(), scalar=AsyncMock(return_value=failure == "foreign"))
    incoming = Mock(ownership=Mock(schema_name="candidate"))
    prior, generation = {"local_generation": 0}, {"local_generation": 1}
    observed = generation if failure == "generation" else prior
    monkeypatch.setattr(mutable.codes, "read_generation", AsyncMock(side_effect=[prior, observed]))
    publish = AsyncMock(return_value=generation)
    cleanup = AsyncMock()
    monkeypatch.setattr(mutable.codes, "publish_local_generation", publish)
    monkeypatch.setattr(mutable.publication, "precreate_catalog_input", AsyncMock(return_value=incoming))
    monkeypatch.setattr(mutable.publication, "copy_catalog_records", AsyncMock())
    monkeypatch.setattr(mutable.publication, "validate_catalog_semantics", AsyncMock())
    monkeypatch.setattr(mutable.native, "cleanup_model_family_stage", cleanup)
    observed_storage = {**storage_by_field, "database_oid": 99} if failure == "storage" else storage_by_field
    monkeypatch.setattr(mutable, "_mutable_storage", AsyncMock(return_value=observed_storage))
    if failure == "none":
        assert await mutable._apply_rows(session, "mrf", [], storage_by_field) == generation
        cleanup.assert_awaited_once_with(session, incoming.spec, incoming.ownership)
    else:
        with pytest.raises(RuntimeError, match="ownership differs|generation changed|storage identity changed"):
            await mutable._apply_rows(session, "mrf", [], storage_by_field)
        cleanup.assert_not_awaited()
    assert publish.await_count == (1 if failure in {"storage", "none"} else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["not-running", "finished", "cas", "source", "storage", "none"])
async def test_mutable_completion_requires_running_attempt_and_returned_cas(monkeypatch, mutable_completion, failure):
    attempt_by_field, storage_by_field, completed = mutable_completion
    running_row_by_field = {**completed, "status": "running", "finished_at": None}
    if failure == "not-running":
        running_row_by_field["status"] = "failed"
    if failure == "finished":
        running_row_by_field["finished_at"] = completed["finished_at"]
    completion_result = Mock()
    completion_result.mappings.return_value.one_or_none.return_value = None if failure == "cas" else completed
    session = Mock(execute=AsyncMock(return_value=completion_result))
    apply = AsyncMock(return_value={"local_generation": 1})
    monkeypatch.setattr(mutable, "_run", AsyncMock(return_value=running_row_by_field))
    monkeypatch.setattr(mutable, "_apply_rows", apply)
    monkeypatch.setattr(mutable.codes, "read_generation", AsyncMock(return_value={"local_generation": 0}))
    expected_by_field = {
        "storage": mutable.generation_value(
            {**storage_by_field, "database_oid": 99} if failure == "storage" else storage_by_field
        ),
        "generation": {"local_generation": 1 if failure == "source" else 0},
    }
    if failure == "none":
        assert (
            await mutable._publish_attempt(
                session, "mrf", attempt_by_field, storage_by_field, [{}, {}, {}], {}, expected_by_field
            )
            == completed
        )
    else:
        with pytest.raises(RuntimeError, match="not running|completion attempt changed|source changed"):
            await mutable._publish_attempt(
                session, "mrf", attempt_by_field, storage_by_field, [{}, {}, {}], {}, expected_by_field
            )
    assert apply.await_count == (1 if failure in {"cas", "none"} else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "canceling", "canceled", "succeeded"])
async def test_mutable_lost_commit_response_obeys_reconciled_terminal_state(monkeypatch, mutable_completion, status):
    attempt_by_field, storage_by_field, completed = mutable_completion
    session = Mock(execute=AsyncMock(), scalar=AsyncMock(return_value="pg_catalog,pg_temp"))

    @asynccontextmanager
    async def transaction():
        yield session
        raise RuntimeError("synthetic lost commit response")

    database = Mock(transaction=transaction)
    database._transaction_binding.return_value = None
    database._reader_binding.return_value = None
    monkeypatch.setattr(mutable, "_lock_mutable_storage", AsyncMock(return_value=storage_by_field))
    monkeypatch.setattr(mutable, "_publish_attempt", AsyncMock(return_value=completed))
    monkeypatch.setattr(mutable, "_await_reconciliation", AsyncMock(return_value={**completed, "status": status}))
    context_by_field = {
        "context": {
            "control_run_id": attempt_by_field["run_id"],
            "_control_attempt_id": attempt_by_field["attempt_id"],
            "_control_attempt_started_at": attempt_by_field["attempt_started_at"],
        }
    }
    payloads = [{"source": source_name, "code_system": system} for source_name, system in mutable.codes.SOURCES]
    if status == "succeeded":
        assert await mutable.publish_mutable_code_sets(
            database, context_by_field, schema="mrf", payloads=payloads, metrics={}
        )
        assert context_by_field["context"]["control_run_terminal_committed"] is True
    else:
        with pytest.raises((RuntimeError, mutable.ImportCancelledError)) as raised:
            await mutable.publish_mutable_code_sets(
                database, context_by_field, schema="mrf", payloads=payloads, metrics={}
            )
        expected = "synthetic lost commit response" if status == "running" else "code-set managed attempt was canceled"
        assert str(raised.value) == expected
        assert context_by_field["context"].get("control_run_terminal_committed") is not True
    assert context_by_field["context"].get("scoped_catalog_commit_unknown") is not True


def test_code_payloads_reject_foreign_source():
    with pytest.raises(RuntimeError, match="source system differs"):
        code_sets._code_payloads([code_sets.CodeSetRow("POS", "23", "Candidate", source="unrelated")])


@pytest.mark.asyncio
async def test_import_code_sets_prepares_all_sources_and_reports_counts(
    monkeypatch,
    capsys,
):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    ensure_database, ensure_catalog, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=POS_HTML,
        rc_html=RC_HTML,
    )

    import_report = await code_sets.import_code_sets()

    ensure_database.assert_awaited_once_with(False)
    ensure_catalog.assert_awaited_once_with("mrf")
    upsert_rows.assert_awaited_once()
    prepared = upsert_rows.await_args.kwargs
    assert prepared["schema"] == "mrf" and prepared["importer"] == "code-sets"
    assert len(prepared["payloads"]["code_catalog"]) == 14
    assert import_report == {
        "pos_rows": 4,
        "rc_rows": 2,
        "modifier_rows": 8,
        "pos_url": code_sets.DEFAULT_POS_URL,
        "rc_url": code_sets.DEFAULT_RC_URL,
        "status": "prepared",
    }
    assert "POS=4 RC=2 MODIFIER=8" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_import_code_sets_test_mode_prefers_representative_codes(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "code_schema")
    _, ensure_catalog, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=_pos_html_for_codes(["01", "21", "22", "23", "24"]),
        rc_html=_revenue_html_for_codes(["0001", "0450", "0981", "0999"]),
    )

    await code_sets.import_code_sets(test_mode=True)

    ensure_catalog.assert_awaited_once_with("code_schema")
    payloads = upsert_rows.await_args.kwargs["payloads"]["code_catalog"]
    assert [entry["code"] for entry in payloads if entry["code_system"] == "POS"] == ["21", "22", "23"]
    assert [entry["code"] for entry in payloads if entry["code_system"] == "RC"] == ["0450", "0981"]


@pytest.mark.asyncio
async def test_import_code_sets_test_mode_falls_back_to_first_ten_rows(
    monkeypatch,
):
    _, _, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=_pos_html_for_codes([f"{number:02}" for number in range(1, 13)]),
        rc_html=_revenue_html_for_codes([f"{number:04}" for number in range(1, 13)]),
    )

    await code_sets.import_code_sets(test_mode=True)

    payloads = upsert_rows.await_args.kwargs["payloads"]["code_catalog"]
    assert [entry["code"] for entry in payloads if entry["code_system"] == "POS"] == [
        f"{number:02}" for number in range(1, 11)
    ]
    assert [entry["code"] for entry in payloads if entry["code_system"] == "RC"] == [
        f"{number:04}" for number in range(1, 11)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pos_html", "rc_html", "expected_message"),
    [
        ("<table></table>", RC_HTML, "CMS POS source produced no code rows"),
        (POS_HTML, "<table></table>", "CMS Blue Button revenue-code source"),
    ],
)
async def test_import_code_sets_rejects_empty_sources_before_catalog_writes(
    monkeypatch,
    pos_html,
    rc_html,
    expected_message,
):
    _, ensure_catalog, upsert_rows = _stub_import_dependencies(
        monkeypatch,
        pos_html=pos_html,
        rc_html=rc_html,
    )

    with pytest.raises(RuntimeError, match=expected_message):
        await code_sets.import_code_sets()

    ensure_catalog.assert_not_awaited()
    upsert_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_main_returns_import_report_and_disconnects(monkeypatch):
    import_report_map = {"pos_rows": 1}
    initialize_database = AsyncMock()
    import_all_code_sets = AsyncMock(return_value=import_report_map)
    disconnect_database = AsyncMock()
    monkeypatch.setattr(code_sets, "init_db", initialize_database)
    monkeypatch.setattr(code_sets, "import_code_sets", import_all_code_sets)
    monkeypatch.setattr(code_sets.db, "disconnect", disconnect_database)

    assert await code_sets.main(test_mode=True) is import_report_map
    initialize_database.assert_awaited_once_with(code_sets.db)
    import_all_code_sets.assert_awaited_once_with(test_mode=True, _control_context=None)
    disconnect_database.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_main_disconnects_when_import_fails(monkeypatch):
    import_failure = RuntimeError("source unavailable")
    initialize_database = AsyncMock()
    import_all_code_sets = AsyncMock(side_effect=import_failure)
    disconnect_database = AsyncMock()
    monkeypatch.setattr(code_sets, "init_db", initialize_database)
    monkeypatch.setattr(code_sets, "import_code_sets", import_all_code_sets)
    monkeypatch.setattr(code_sets.db, "disconnect", disconnect_database)

    with pytest.raises(RuntimeError, match="source unavailable"):
        await code_sets.main()

    assert initialize_database.await_args == call(code_sets.db)
    disconnect_database.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["transaction", "reader", None])
@pytest.mark.parametrize("matched", [False, True])
async def test_queued_code_sets_cancel_reserves_its_own_commit(monkeypatch, binding, matched):
    from types import SimpleNamespace

    from process.control_cancel import is_queued_code_sets_cancel_reserved

    committed_transactions = []

    @asynccontextmanager
    async def transaction():
        yield
        committed_transactions.append(True)

    database = SimpleNamespace(
        _transaction_binding=lambda: object() if binding == "transaction" else None,
        _reader_binding=lambda: object() if binding == "reader" else None,
        transaction=transaction,
        execute=AsyncMock(),
        status=AsyncMock(return_value=int(matched)),
    )
    current_run_by_name = {"run_id": "synthetic-run", "importer": "code-sets", "status": "queued", "progress": {}}
    if binding is not None:
        with pytest.raises(RuntimeError, match="own commit boundary"):
            await is_queued_code_sets_cancel_reserved(database, current_run_by_name, datetime.now(UTC))
        assert not committed_transactions
        database.status.assert_not_awaited()
    else:
        assert await is_queued_code_sets_cancel_reserved(database, current_run_by_name, datetime.now(UTC)) is matched
        assert committed_transactions == [True]
        statement = str(database.status.call_args.args[0])
        assert "finished_at IS NULL" in statement and "status" in statement and "progress" in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["queued", "canceling"])
@pytest.mark.parametrize("matched", [False, True])
async def test_queued_code_sets_cancel_never_signals_a_lost_reservation(monkeypatch, status, matched):
    from api import control_imports

    current_run_by_name = {
        "run_id": "synthetic-run",
        "importer": "code-sets",
        "status": status,
        "metrics": {"enqueue_adapter": "arq_single_job"},
        "progress": {},
    }
    latest_run_by_name = {**current_run_by_name, "status": "running", "progress": {"attempt_id": "claimed"}}
    reservation = AsyncMock(return_value=matched)
    signal = AsyncMock(return_value={"removed": True})
    persist = AsyncMock(return_value={**current_run_by_name, "status": "canceled"})
    monkeypatch.setattr(control_imports, "is_queued_code_sets_cancel_reserved", reservation)
    monkeypatch.setattr(control_imports, "_cancel_signal_for_run", signal)
    monkeypatch.setattr(control_imports, "_persist_cancel_request", persist)
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=latest_run_by_name))
    result = await control_imports._request_worker_cancel(current_run_by_name)
    if matched:
        assert result["status"] == "canceled"
        signal.assert_awaited_once()
        assert persist.call_args.kwargs["current_run"]["status"] == "canceling"
    else:
        assert result == latest_run_by_name
        signal.assert_not_awaited()
        persist.assert_not_awaited()
    reservation.assert_awaited_once()


def test_code_sets_publication_index_matches_its_migration():
    import importlib.util
    from pathlib import Path

    from db.models import ImportRun

    path = Path(__file__).parents[1] / "alembic/versions/20261111010000_code_sets_publication_idempotency.py"
    spec = importlib.util.spec_from_file_location("code_sets_key_schema", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    index = next(item for item in ImportRun.__my_additional_indexes__ if item["name"] == migration.INDEX_NAME)
    assert index == {
        "name": migration.INDEX_NAME,
        "index_elements": ("importer", "idempotency_key"),
        "unique": True,
        "where": migration.INDEX_PREDICATE,
    }
    assert migration.down_revision == "20261108010000_custom_import_family_child_reference_aggregate"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "current_run_by_name",
    [
        {"importer": "other", "status": "queued"},
        {"importer": "code-sets", "status": "running"},
        {"importer": "code-sets", "status": "canceling", "progress": {"attempt_id": "claimed"}},
        {"importer": "code-sets", "status": "canceling", "progress": {"attempt_started_at": "claimed"}},
    ],
)
async def test_code_sets_cancel_reservation_rejects_other_intents(current_run_by_name):
    from types import SimpleNamespace

    from process.control_cancel import is_queued_code_sets_cancel, is_queued_code_sets_cancel_reserved

    database = SimpleNamespace(_transaction_binding=lambda: None, _reader_binding=lambda: None, status=AsyncMock())
    assert not is_queued_code_sets_cancel(current_run_by_name)
    with pytest.raises(RuntimeError, match="observed queued intent"):
        await is_queued_code_sets_cancel_reserved(database, current_run_by_name, datetime.now(UTC))
    database.status.assert_not_awaited()


def test_code_sets_key_migration_offline_sql_is_scoped_and_restores_path(monkeypatch):
    import importlib.util
    from io import StringIO
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    path = Path(__file__).parents[1] / "alembic/versions/20261111010000_code_sets_publication_idempotency.py"
    spec = importlib.util.spec_from_file_location("code_sets_key_offline", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={
            "as_sql": True,
            "literal_binds": True,
            "output_buffer": output,
        },
    )
    monkeypatch.setenv("DB_SCHEMA", "synthetic_schema")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_schema")
    migration.op = Operations(context)
    migration.upgrade()
    migration.downgrade()
    statements = output.getvalue()
    assert "SET LOCAL search_path TO pg_catalog,pg_temp" in statements
    assert (
        "CREATE UNIQUE INDEX import_run_code_sets_publication_idempotency_idx ON synthetic_schema.import_run"
        in statements
    )
    assert "code_sets_mutable_publication" in statements and "set_config('search_path'" in statements
    assert '"synthetic_schema",public' in statements
    assert "DROP INDEX synthetic_schema.import_run_code_sets_publication_idempotency_idx" in statements


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner", [None, {"status": "running"}, {"status": "succeeded", "metrics": {"code_sets_mutable_publication": {}}}]
)
async def test_code_sets_idempotency_reads_active_or_published_owner_once(monkeypatch, owner):
    from types import SimpleNamespace

    from sqlalchemy.dialects import postgresql

    from api import control_imports

    execute = AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: owner))
    fallback = AsyncMock(side_effect=AssertionError("CodeSets ownership uses one snapshot"))
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(execute=execute))
    monkeypatch.setattr(control_imports, "normalize_run", lambda run: run)
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", fallback)
    assert await control_imports._idempotent_import_run("code-sets", "synthetic-key") == owner
    execute.assert_awaited_once()
    fallback.assert_not_awaited()
    statement = str(
        execute.call_args.args[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )
    assert "status IN (" in statement and " OR " in statement and "code_sets_mutable_publication" in statement


@pytest.mark.parametrize("importer", ["nucc", "code-sets"])
@pytest.mark.parametrize("acknowledgment", ["matching", "duplicate", "changed", "missing"])
def test_fixed_enqueue_identity_requires_the_requested_acknowledgment(importer, acknowledgment):
    from types import SimpleNamespace

    from api import control_imports

    job = None if acknowledgment == "duplicate" else SimpleNamespace()
    if acknowledgment in {"matching", "changed"}:
        job.job_id = "synthetic-job" if acknowledgment == "matching" else "foreign-job"
    if acknowledgment in {"changed", "missing"}:
        with pytest.raises(RuntimeError, match="acknowledgement identity changed"):
            control_imports._safe_enqueued_job_id(importer, {}, job, requested_job_id="synthetic-job")
    else:
        assert (
            control_imports._safe_enqueued_job_id(importer, {}, job, requested_job_id="synthetic-job")
            == "synthetic-job"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate", [False, True])
async def test_code_sets_enqueue_uses_its_durable_canonical_job_id(monkeypatch, duplicate):
    from types import SimpleNamespace

    from api import control_imports

    job_id = "code_sets_start_synthetic-run"
    enqueue = AsyncMock(return_value=None if duplicate else SimpleNamespace(job_id=job_id))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=SimpleNamespace(enqueue_job=enqueue)))
    result = await control_imports._enqueue_import_start(
        {"run_id": "synthetic-run", "importer": "code-sets", "params": {}}
    )
    assert result["metrics"]["job_id"] == job_id
    assert enqueue.call_args.kwargs == {"_queue_name": "arq:CodeSets", "_job_id": job_id}
    assert enqueue.call_args.args[1]["run_id"] == "synthetic-run"
    assert enqueue.call_args.args[1]["target_module"] == "process.code_sets"


@pytest.mark.parametrize("persisted", [None, "legacy-job", "code_sets_start_synthetic-run"])
def test_code_sets_cleanup_keeps_legacy_ids_and_derives_new_canonical_ids(persisted):
    from api import control_imports

    identity = control_imports._arq_cleanup_identity(
        {
            "run_id": "synthetic-run",
            "importer": "code-sets",
            "metrics": {"job_id": persisted},
        }
    )
    assert identity[0:2] == ("synthetic-run", "code-sets")
    assert identity[3:] == ("arq:CodeSets", persisted or "code_sets_start_synthetic-run")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["native", "legacy-mutable"])
async def test_code_sets_create_persists_queue_identity_before_dispatch(monkeypatch, mode):
    from types import SimpleNamespace

    from api import control_imports

    if mode == "native":
        monkeypatch.delenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", mode)
    monkeypatch.setattr(
        control_imports, "db", SimpleNamespace(_transaction_binding=lambda: None, _reader_binding=lambda: None)
    )
    monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=None))
    monkeypatch.setattr(control_imports, "_has_isolated_pricing_admission", AsyncMock(return_value=False))
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock())
    admissions = []

    async def admit(importer, run_values_by_name, **_options):
        assert importer == "code-sets"
        admissions.append(run_values_by_name.copy())

    async def enqueue(function, payload, **options):
        assert len(admissions) == 1
        metrics_by_name = admissions[0]["metrics"]
        assert metrics_by_name["job_id"] == options["_job_id"] == "code_sets_start_synthetic-run"
        assert metrics_by_name["queue"] == options["_queue_name"] == "arq:CodeSets"
        assert metrics_by_name["function"] == function == "control_single_job_start"
        assert payload["run_id"] == admissions[0]["run_id"] == "synthetic-run"
        return SimpleNamespace(job_id=options["_job_id"])

    monkeypatch.setattr(control_imports, "_admit_import_row", AsyncMock(side_effect=admit))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=SimpleNamespace(enqueue_job=enqueue)))
    monkeypatch.setattr(
        control_imports, "_persist_enqueue_result", AsyncMock(side_effect=lambda _id, _importer, values: values)
    )
    queued_run, created = await control_imports.create_import_run({"importer": "code-sets", "run_id": "synthetic-run"})
    assert created and queued_run["metrics"]["job_id"] == "code_sets_start_synthetic-run"
    assert queued_run["status"] == "queued" and queued_run["phase_detail"] == "enqueued"
