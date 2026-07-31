# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from arq import Retry
from sqlalchemy.exc import SQLAlchemyError

from process.ext import utils
from tests.ext_utils_import_coverage_support import (
    _Acquire,
    _BrokenChunkStream,
    _BrokenHeaders,
    _Client,
    _CopyDriver,
    _InsertStatement,
    _Request,
    _Response,
    _fake_table,
    _fixed_download_timeout,
)

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_log_fragment"),
    [
        (RuntimeError("stream failed"), "Parallel download error"),
        (utils.ssl.SSLCertVerificationError("certificate failed"), "SSL Error"),
    ],
)
async def test_stream_failures_are_classified_for_retry(
    failure,
    expected_log_fragment,
    tmp_path,
    monkeypatch,
):
    response = _Response(body=b"")
    response.content = _BrokenChunkStream(failure)
    client = _Client(get_requests=[_Request(response)])
    log_error = AsyncMock()

    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(None, False)),
    )
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(utils, "log_error", log_error)

    with pytest.raises(Retry):
        await utils.download_it_and_save(
            "https://example.test/error",
            str(tmp_path / "error.bin"),
            context={"issuer_array": [1], "source": "coverage-test"},
            logger=object(),
        )

    assert expected_log_fragment in log_error.await_args.args[1]


@pytest.mark.asyncio
async def test_response_error_is_logged_before_retry(tmp_path, monkeypatch):
    response = _Response(body=b"")
    response.headers = _BrokenHeaders()
    client = _Client(get_requests=[_Request(response)])
    log_error = AsyncMock()

    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(None, False)),
    )
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(utils, "log_error", log_error)

    with pytest.raises(Retry):
        await utils.download_it_and_save(
            "https://example.test/response-error",
            str(tmp_path / "response-error.bin"),
            context={"issuer_array": [1], "source": "coverage-test"},
            logger=object(),
        )

    assert "Error response 503" in log_error.await_args.args[1]


@pytest.mark.asyncio
async def test_push_objects_empty_single_and_no_copy_paths(monkeypatch):
    slow = AsyncMock(return_value="single")
    monkeypatch.setattr(utils, "push_objects_slow", slow)

    assert await utils.push_objects([], object()) is None
    single_cls = SimpleNamespace(__tablename__="single", __table__=_fake_table())
    assert await utils.push_objects([{"id": 1}], single_cls) == "single"
    slow.assert_awaited_once()

    statements = []

    async def status(statement):
        statements.append(statement)

    conflict_cls = SimpleNamespace(
        __tablename__="no_copy_conflict",
        __table__=_fake_table(),
        __my_index_elements__=["id"],
    )
    plain_cls = SimpleNamespace(
        __tablename__="no_copy_plain",
        __table__=_fake_table(),
    )
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(status),
    )

    await utils.push_objects(
        [{"id": 2, "value": "two"}, {"id": 1, "value": "one"}],
        conflict_cls,
        use_copy=False,
    )
    await utils.push_objects(
        [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
        plain_cls,
        use_copy=False,
    )

    assert statements[0].conflict[0] == "nothing"
    assert statements[1].conflict is None


@pytest.mark.asyncio
async def test_push_objects_conflict_target_and_status_refusals(monkeypatch):
    async def successful_status(_statement):
        return None

    primary_key_cls = SimpleNamespace(
        __tablename__="primary_key_target",
        __table__=_fake_table(),
    )
    initial_index_cls = SimpleNamespace(
        __tablename__="initial_index_target",
        __table__=_fake_table(),
        __my_initial_indexes__=[{}, {"index_elements": ["id"]}],
    )
    monkeypatch.setattr(
        utils,
        "inspect",
        lambda _table: SimpleNamespace(
            primary_key=[SimpleNamespace(name="id")]
        ),
    )
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(successful_status),
    )
    monkeypatch.setenv("HLTHPRT_MAX_INSERT_PARAMETERS", "0")

    await utils.push_objects(
        [{"id": 1, "value": "one"}],
        primary_key_cls,
        rewrite=True,
        use_copy=False,
    )
    await utils.push_objects(
        [{"id": 1, "value": "one"}],
        initial_index_cls,
        rewrite=True,
        use_copy=False,
    )

    async def failed_status(_statement):
        raise SQLAlchemyError("permission denied")

    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(failed_status),
    )
    with pytest.raises(SQLAlchemyError, match="permission denied"):
        await utils.push_objects(
            [{"id": 1}, {"id": 2}],
            primary_key_cls,
            use_copy=False,
        )


@pytest.mark.asyncio
async def test_rewrite_status_missing_table_uses_guarded_retry(monkeypatch):
    original_push_objects = utils.push_objects
    recursive_call = AsyncMock(return_value="retried")

    async def missing_status(_statement):
        raise SQLAlchemyError("relation rewrite_status does not exist")

    fake_cls = SimpleNamespace(
        __tablename__="rewrite_status",
        __table__=_fake_table(),
        __my_index_elements__=["id"],
    )
    monkeypatch.setattr(utils, "push_objects", recursive_call)
    monkeypatch.setattr(utils.db, "create_table", AsyncMock())
    monkeypatch.setattr(utils.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(missing_status),
    )

    assert (
        await original_push_objects(
            [{"id": 1, "value": "value"}],
            fake_cls,
            rewrite=True,
            use_copy=False,
        )
        == "retried"
    )
    recursive_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_copy_schema_dict_and_parallel_success(tmp_path, monkeypatch):
    copy_driver = _CopyDriver()
    fake_cls = SimpleNamespace(
        __tablename__="dict_schema_copy",
        __table__=_fake_table(),
        __table_args__={"schema": "dict_schema"},
        __my_index_elements__=["id"],
    )
    monkeypatch.setattr(utils.db, "acquire", lambda: _Acquire(copy_driver))

    await utils.push_objects(
        [{"id": 1, "value": "value"}],
        fake_cls,
        rewrite=True,
    )
    assert copy_driver.calls[0][1]["schema_name"] == "dict_schema"

    parallel = AsyncMock()
    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(8, True)),
    )
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_THRESHOLD_BYTES", 1)
    monkeypatch.setattr(utils, "PREFER_COMPRESSED_STREAM", False)
    monkeypatch.setattr(utils, "_download_parallel_by_ranges", parallel)
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=_Client()),
    )

    await utils.download_it_and_save(
        "https://example.test/parallel",
        str(tmp_path / "parallel.bin"),
    )
    parallel.assert_awaited_once()


@pytest.mark.asyncio
async def test_resume_refusal_restarts_stream(tmp_path, monkeypatch):
    target = tmp_path / "resume-refused.bin"
    target.write_bytes(b"old")
    client = _Client(
        get_requests=[
            _Request(_Response(status=200, body=b"ignored")),
            _Request(_Response(status=200, body=b"replacement")),
        ]
    )
    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(20, True)),
    )
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "get_http_client", AsyncMock(return_value=client))

    await utils.download_it_and_save(
        "https://example.test/resume-refused",
        str(target),
        prefer_stream=True,
    )
    assert target.read_bytes() == b"replacement"


@pytest.mark.asyncio
async def test_small_helper_terminal_branches(monkeypatch):
    assert utils.return_checksum(["one"], crc=16) != utils.return_checksum(
        ["one"],
        crc=32,
    )

    monkeypatch.setenv(
        "HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS",
        "blocked.test",
    )
    disabled_client = _Client(
        head_requests=[
            _Request(
                _Response(
                    headers={
                        "Content-Length": "invalid",
                        "Accept-Ranges": "bytes",
                    }
                )
            )
        ]
    )
    assert await utils._head_download_info(
        disabled_client,
        "https://blocked.test/file",
    ) == (None, False)

    init_db = AsyncMock()
    monkeypatch.setattr(utils, "init_db", init_db)
    await utils.my_init_db(object())
    init_db.assert_awaited_once()

    connect = AsyncMock()
    monkeypatch.setattr(utils.db, "connect", connect)
    monkeypatch.setattr(utils, "TEST_DATABASE_SUFFIX", None)
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "same")
    utils.db._database_override = None
    await utils.ensure_database(test_mode=True)
    assert utils.db._database_override is None

    utils.err_obj_list.clear()
    utils.err_obj_key.clear()
    flush = AsyncMock()
    monkeypatch.setattr(utils, "flush_error_log", flush)
    for index in range(201):
        await utils.log_error(
            "error",
            f"broken-{index}",
            [index],
            "https://example.test",
            "source",
            "network",
            object(),
        )
    flush.assert_awaited_once()
