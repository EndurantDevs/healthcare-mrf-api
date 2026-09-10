# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import hashlib
import json
import stat
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from multidict import CIMultiDict

from process import tennessee_profile_acquisition as acquisition


def _form(board, submitted=False):
    board_selection = ' selected' if submitted else ''
    location_selection = ' selected' if submitted else ''
    selects = (
        f'<select name="Board.BoardCode"><option value="" selected>Choose</option>'
        f'<option value="{board}"{board_selection}>Board</option></select>'
        '<select name="Profession.ProfessionCode" disabled><option selected>Default to all Professions or select...</option></select>'
        '<select name="Rank.RankName" disabled><option value="" selected>Default to all Ranks or select...</option></select>'
        '<select name="Status.Status"></select>'
        '<select name="State.StateCode"><option value="" selected>Choose</option>'
        f'<option value="100"{location_selection}>All Locations</option></select>'
        '<select name="County.CountyCode" disabled><option selected>Default to all Counties or select...</option></select>'
    )
    checkboxes = ''.join(f'<input type="checkbox" name="{field}" value="true"'
                         + (' checked' if submitted and field != 'PersonFlag' else '') + '>'
                         for field in ('PersonFlag', 'EduFlag', 'PracticeFlag', 'SAQFlag'))
    hidden = '<input type="hidden" name="__RequestVerificationToken" value="synthetic-session-token">'
    hidden += ''.join(f'<input type="hidden" name="{field}" value="false">'
                      for field in ('PersonFlag', 'EduFlag', 'PracticeFlag', 'SAQFlag'))
    download = '<input type="hidden" name="hasFile" value="True"><button id="hidden-button" type="submit" hidden/>' if submitted else ''
    return (f'<form action="/LicensureReports" method="post">{selects}{checkboxes}{hidden}'
            '<button id="submit-button" type="submit">Submit</button></form>'
            f'<form action="/LicensureReports/Home/CreateFile" method="get">{download}</form>').encode()


class SourceResponse:
    def __init__(self, body, content_type, *, chunks=None, status=200, url=None, eof=True):
        self.body, self.status, self.url, self.eof = body, status, url, eof
        self.headers = CIMultiDict({'Content-Type': content_type, 'Content-Length': str(len(body)),
                                   'Set-Cookie': 'session=synthetic-private-cookie'})
        self.chunks = list(chunks if chunks is not None else [body])
        self.content, self.reads = self, 0

    async def read(self, _size):
        self.reads += 1
        chunk = self.chunks.pop(0) if self.chunks else b''
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    def at_eof(self):
        return self.eof and not self.chunks

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        return False


class SourceSession:
    def __init__(self, responses, options):
        self.responses, self.options, self.requests = list(responses), options, []
        self.closed, self._retry_connection = False, True

    def request(self, method, url, **options):
        assert self._retry_connection is False and options['allow_redirects'] is False
        self.requests.append((method, url, options))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        response.url = url if response.url is None else response.url
        return response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception):
        self.closed = True
        return False


def _responses():
    responses = []
    for board, code, label in [('16', 1606, 'Medical Doctor'), ('19', 1907, 'Osteopathic Physician')]:
        responses.extend([
            SourceResponse(_form(board), 'text/html; charset=utf-8'),
            SourceResponse(json.dumps([{'professionCode': code, 'professionName': label}]).encode(), 'application/json'),
            SourceResponse(_form(board, True), 'text/html'),
            SourceResponse(f'Profession Code,Name\r\n{code},Synthetic Example\r\n'.encode(), 'text/csv'),
        ])
    return responses


@pytest.fixture
def source_sessions(monkeypatch):
    responses, sessions = _responses(), []

    def create(**options):
        session = SourceSession(responses[len(sessions) * 4:len(sessions) * 4 + 4], options)
        sessions.append(session)
        return session

    monkeypatch.setattr(acquisition.aiohttp, 'ClientSession', create)
    return responses, sessions


async def test_two_report_unit_retains_exact_bytes_and_token_free_receipts(tmp_path, source_sessions):
    responses, sessions = source_sessions
    progress = AsyncMock()
    manifest = await acquisition.acquire_reports(tmp_path, progress)
    assert manifest == json.loads((tmp_path / 'acquisition.json').read_bytes())
    assert manifest['complete'] and len(manifest['responses']) == 8 and set(manifest['reports']) == {'1606', '1907'}
    assert len(sessions) == 2 and all(session.closed for session in sessions)
    assert sessions[0].options['cookie_jar'] is not sessions[1].options['cookie_jar']
    for session, board, code in zip(sessions, ('16', '19'), ('1606', '1907')):
        assert isinstance(session.options['cookie_jar'], acquisition.aiohttp.CookieJar)
        assert session.options['timeout'].total == 180
        assert session.options['trust_env'] is session.options['auto_decompress'] is False
        assert session.options['headers']['Accept-Encoding'] == 'identity'
        assert [(method, url) for method, url, _ in session.requests] == [
            ('GET', acquisition.BASE_URL), ('POST', acquisition.PROFESSIONS_URL),
            ('POST', acquisition.BASE_URL), ('GET', acquisition.REPORT_URL)]
        assert session.requests[1][2]['data'] == [('id', board)]
        fields = session.requests[2][2]['data']
        assert ('Profession.ProfessionCode', code) in fields and ('State.StateCode', '100') in fields
        assert [field_value for name, field_value in fields if name == 'EduFlag'] == ['true', 'false']
        assert not any(name in {'County.CountyCode', 'Status.Status'} for name, _ in fields)
    for receipt, response in zip(manifest['responses'], responses):
        path = Path(receipt['filepath'])
        assert path.read_bytes() == response.body
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert receipt['complete'] and receipt['eof'] and receipt['content_length_verified']
        assert receipt['content_bytes'] == len(response.body)
        assert receipt['content_sha256'] == hashlib.sha256(response.body).hexdigest()
        assert receipt == json.loads((tmp_path / f"{receipt['stage']}.receipt.json").read_bytes())
    assert 'synthetic-session-token' not in json.dumps(manifest) and 'synthetic-private-cookie' not in json.dumps(manifest)
    assert progress.await_args_list[-1].args == (8, 8)


@pytest.mark.parametrize('change', ['redirect', 'url', 'type', 'encoding', 'length', 'duplicate', 'transfer', 'cap', 'stream_cap', 'eof', 'short', 'disconnect'])
async def test_second_report_failure_preserves_first_without_completion(tmp_path, source_sessions, monkeypatch, change):
    responses, sessions = source_sessions
    response = responses[7]
    if change == 'redirect':
        response.status = 302
    if change == 'url':
        response.url = 'https://example.test/CreateFile'
    if change == 'type':
        response.headers['Content-Type'] = 'text/html'
    if change == 'encoding':
        response.headers['Content-Encoding'] = 'gzip'
    if change == 'length':
        response.headers['Content-Length'] = '+2'
    if change == 'duplicate':
        response.headers.add('Content-Length', response.headers['Content-Length'])
    if change == 'transfer':
        response.headers['Transfer-Encoding'] = 'chunked'
    if change == 'cap':
        response.headers['Content-Length'] = str(acquisition.MAX_REPORT_BYTES + 1)
    if change == 'stream_cap':
        del response.headers['Content-Length']
        monkeypatch.setattr(acquisition, 'MAX_REPORT_BYTES', len(response.body))
        response.chunks = [response.body, b'overflow', b'never read']
    if change == 'eof':
        response.eof = False
    if change == 'short':
        response.chunks = [b'partial']
    if change == 'disconnect':
        response.chunks = [b'partial', ConnectionError('uncertain')]
    with pytest.raises((ValueError, ConnectionError)):
        await acquisition.acquire_reports(tmp_path, AsyncMock())
    assert not (tmp_path / 'acquisition.json').exists()
    assert (tmp_path / 'md-report.csv').read_bytes() == responses[3].body
    receipt = json.loads((tmp_path / 'do-report.receipt.json').read_bytes())
    assert receipt['complete'] is False
    assert len(sessions) == 2 and all(session.closed and len(session.requests) == 4 for session in sessions)
    if change == 'stream_cap':
        assert (tmp_path / 'do-report.csv').read_bytes() == response.body and response.reads == 2


@pytest.mark.parametrize('stage,old,new', [
    (0, b'action="/LicensureReports"', b'action="https://example.test/changed"'),
    (0, b'All Locations', b'Tennessee'),
    (0, b'name="__RequestVerificationToken"', b'name="UnknownFilter"'),
    (0, b'id="submit-button"', b'id="submit-button" formaction="/Other"'),
    (1, b'Medical Doctor', b'Medical Doctor (Special Training)'),
    (1, b'1606', b'1677'),
    (1, b'1606', b'"1606"'),
    (2, b'value="16" selected', b'value="19" selected'),
    (2, b'value="100" selected', b'value="99" selected'),
    (2, b'"EduFlag" value="true" checked', b'"EduFlag" value="true"'),
    (2, b'id="hidden-button"', b'id="different-button"'),
    (2, b'id="hidden-button"', b'id="hidden-button" formmethod="post"'),
    (2, b'Home/CreateFile', b'Home/OtherFile'),
    (2, b'<select name="Status.Status"></select>', b'<select name="Status.Status"><option selected>Active</option></select>'),
])
async def test_form_and_profession_drift_stops_before_next_request(tmp_path, source_sessions, stage, old, new):
    responses, sessions = source_sessions
    response = responses[stage]
    assert old in response.body
    response.body = response.body.replace(old, new)
    response.chunks = [response.body]
    response.headers['Content-Length'] = str(len(response.body))
    with pytest.raises(ValueError):
        await acquisition.acquire_reports(tmp_path, AsyncMock())
    assert len(sessions) == 1 and len(sessions[0].requests) == stage + 1 and sessions[0].closed
    assert not (tmp_path / 'md-report.csv').exists() and not (tmp_path / 'acquisition.json').exists()


@pytest.mark.parametrize('boundary', ['before', 'chunk', 'between', 'final'])
async def test_cancellation_never_marks_unit_complete(tmp_path, source_sessions, boundary):
    responses, sessions = source_sessions
    completed_values = []

    async def progress(completed, total):
        assert total == 8
        if ((boundary == 'before') or (boundary == 'chunk' and responses[0].reads == 1)
                or (boundary == 'between' and completed == 4) or (boundary == 'final' and completed == 8)):
            raise asyncio.CancelledError()
        completed_values.append(completed)

    with pytest.raises(asyncio.CancelledError):
        await acquisition.acquire_reports(tmp_path, progress)
    assert not (tmp_path / 'acquisition.json').exists() and 8 not in completed_values
    assert all(session.closed for session in sessions)
    assert sum(len(session.requests) for session in sessions) == {'before': 0, 'chunk': 1, 'between': 4, 'final': 8}[boundary]
    if boundary == 'chunk':
        assert json.loads((tmp_path / 'md-form.receipt.json').read_bytes())['complete'] is False
        assert (tmp_path / 'md-form.html').read_bytes() == responses[0].body


@pytest.mark.parametrize('kind', ['existing', 'symlink', 'parent_symlink'])
async def test_unknown_paths_are_never_reused_or_removed(tmp_path, source_sessions, kind):
    _, sessions = source_sessions
    directory = tmp_path
    marker = tmp_path / 'md-form.html'
    if kind == 'existing':
        marker.write_bytes(b'incumbent')
    elif kind == 'symlink':
        marker.symlink_to(tmp_path / 'missing')
    else:
        directory = tmp_path / 'linked'
        directory.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises((FileExistsError, ValueError)):
        await acquisition.acquire_reports(directory, AsyncMock())
    assert not any(session.requests for session in sessions)
    assert marker.read_bytes() == b'incumbent' if kind == 'existing' else (marker if kind == 'symlink' else directory).is_symlink()


@pytest.mark.parametrize('deadline', ['RESPONSE_SECONDS', 'RUN_SECONDS'])
async def test_response_and_run_deadlines_cancel_without_retry(tmp_path, source_sessions, monkeypatch, deadline):
    responses, sessions = source_sessions
    entered = asyncio.Event()

    async def stalled_read(_size):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(responses[0], 'read', stalled_read)
    monkeypatch.setattr(acquisition, deadline, 0.01)
    with pytest.raises(TimeoutError):
        await acquisition.acquire_reports(tmp_path, AsyncMock())
    assert entered.is_set() and len(sessions[0].requests) == 1 and sessions[0].closed
    assert json.loads((tmp_path / 'md-form.receipt.json').read_bytes())['complete'] is False
    assert not (tmp_path / 'acquisition.json').exists()


@pytest.mark.parametrize('stage,header,header_value', [
    (0, 'Content-Type', 'application/xhtml+xml'), (1, 'Content-Type', 'text/html'),
    (0, 'Content-Length', str(2 * 1024 * 1024 + 1)), (1, 'Content-Length', '-1'),
])
async def test_non_report_responses_use_typed_bounded_contract(tmp_path, source_sessions, stage, header, header_value):
    responses, sessions = source_sessions
    responses[stage].headers[header] = header_value
    with pytest.raises(ValueError):
        await acquisition.acquire_reports(tmp_path, AsyncMock())
    assert len(sessions[0].requests) == stage + 1 and responses[stage].reads == 0
    assert not (tmp_path / 'acquisition.json').exists()


async def test_chunked_eof_is_valid_without_content_length(tmp_path, source_sessions):
    responses, _ = source_sessions
    for response in responses:
        del response.headers['Content-Length']
        response.headers['Transfer-Encoding'] = 'chunked'
    manifest = await acquisition.acquire_reports(tmp_path, AsyncMock())
    assert all(receipt['eof'] and not receipt['content_length_verified'] for receipt in manifest['responses'])


async def test_retry_control_failure_prevents_any_source_request(tmp_path, source_sessions, monkeypatch):
    responses, sessions = source_sessions
    create = acquisition.aiohttp.ClientSession

    def without_retry_control(**options):
        session = create(**options)
        del session._retry_connection
        return session

    monkeypatch.setattr(acquisition.aiohttp, 'ClientSession', without_retry_control)
    with pytest.raises(ValueError, match='retry_control_unavailable'):
        await acquisition.acquire_reports(tmp_path, AsyncMock())
    assert sessions[0].closed and sessions[0].requests == [] and all(response.reads == 0 for response in responses)
