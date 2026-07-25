# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import io
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def cms_doctors_module():
    return importlib.import_module("process.cms_doctors")


def _zip_bytes_with_csv(csv_name: str, csv_payload: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_payload)
    return buffer.getvalue()


class _FakeContent:
    def __init__(self, payload: bytes):
        self._payload = payload

    async def iter_chunked(self, _chunk_size):
        yield self._payload


class _FakeResponse:
    def __init__(self, payload: bytes):
        self.content = _FakeContent(payload)

    def raise_for_status(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeClient:
    def __init__(self, payload: bytes):
        self._payload = payload

    def get(self, _url, timeout=0):
        return _FakeResponse(self._payload)

    async def close(self):
        return None


class _CatalogResponse:
    def __init__(self, *, payload=None, status=200, enter_error=None):
        self._payload = payload
        self.status = status
        self._enter_error = enter_error

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self, *, content_type=None):
        assert content_type is None
        return self._payload

    async def __aenter__(self):
        if self._enter_error is not None:
            raise self._enter_error
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _CatalogClient:
    def __init__(self, *, get_responses=(), head_responses=()):
        self._get_responses = iter(get_responses)
        self._head_responses = iter(head_responses)
        self.get_urls = []
        self.head_urls = []

    def get(self, url, *, timeout):
        self.get_urls.append((url, timeout))
        return next(self._get_responses)

    def head(self, url, *, allow_redirects, timeout):
        self.head_urls.append((url, allow_redirects, timeout))
        return next(self._head_responses)


def test_cms_doctor_identifiers_and_schema_names_are_bounded(cms_doctors_module):
    """Identifiers remain deterministic, valid, and inside PostgreSQL limits."""

    assert cms_doctors_module._stage_index_name("stage", "primary") == "stage_idx_primary"
    assert cms_doctors_module._normalize_import_id(" run-ABC_123 ") == "runABC123"
    fallback_import_id = cms_doctors_module._normalize_import_id("---")
    missing_import_id = cms_doctors_module._normalize_import_id(None)
    assert len(fallback_import_id) == 8
    assert fallback_import_id.isdigit()
    assert len(missing_import_id) == 8
    assert missing_import_id.isdigit()
    assert cms_doctors_module._archived_identifier("current") == "current_old"
    archived_long_name = cms_doctors_module._archived_identifier("x" * 80)
    assert len(archived_long_name) == cms_doctors_module.POSTGRES_IDENTIFIER_MAX_LENGTH
    assert archived_long_name.endswith("_old")
    assert cms_doctors_module._validate_schema_name("_mrf2") == "_mrf2"


@pytest.mark.parametrize("schema_name", ["", "9mrf", "mrf-stage"])
def test_cms_doctor_schema_name_rejects_unsafe_identifiers(
    cms_doctors_module,
    schema_name,
):
    """DDL helpers reject empty, numeric-leading, and punctuated schemas."""

    with pytest.raises(ValueError, match="Invalid schema name"):
        cms_doctors_module._validate_schema_name(schema_name)


@pytest.mark.asyncio
async def test_cms_doctor_stage_indexes_preserve_declared_shape(
    monkeypatch,
    cms_doctors_module,
):
    """Stage indexes retain primary, method, predicate, and default-name metadata."""

    status = AsyncMock()
    monkeypatch.setattr(cms_doctors_module.db, "status", status)
    stage_cls = SimpleNamespace(
        __tablename__="doctor_stage",
        __my_index_elements__=("npi", "address_checksum"),
        __my_additional_indexes__=(
            {"name": "state_idx", "index_elements": ("state",), "where": "state IS NOT NULL"},
            {"index_elements": ("zip_code",), "using": "btree"},
        ),
    )

    await cms_doctors_module._create_stage_indexes(stage_cls, "mrf")
    sql_statements = [call.args[0] for call in status.await_args_list]

    assert len(sql_statements) == 3
    assert "UNIQUE INDEX" in sql_statements[0]
    assert "(npi, address_checksum)" in sql_statements[0]
    assert "state_idx" in sql_statements[1]
    assert "WHERE state IS NOT NULL" in sql_statements[1]
    assert "doctor_stage_idx_zip_code" in sql_statements[2]
    assert "USING btree" in sql_statements[2]


@pytest.mark.asyncio
async def test_cms_doctor_stage_indexes_allow_an_index_free_stage(
    monkeypatch,
    cms_doctors_module,
):
    """A stage without declared index metadata performs no DDL."""

    status = AsyncMock()
    monkeypatch.setattr(cms_doctors_module.db, "status", status)

    await cms_doctors_module._create_stage_indexes(
        SimpleNamespace(__tablename__="doctor_stage"),
        "mrf",
    )

    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_cms_doctor_schema_creation_tolerates_only_a_proven_existing_schema(
    monkeypatch,
    cms_doctors_module,
):
    """A failed CREATE is recoverable only when PostgreSQL proves the schema exists."""

    status = AsyncMock(side_effect=[None, RuntimeError("race"), RuntimeError("denied")])
    scalar = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(cms_doctors_module.db, "status", status)
    monkeypatch.setattr(cms_doctors_module.db, "scalar", scalar)

    await cms_doctors_module._ensure_schema_exists("mrf")
    await cms_doctors_module._ensure_schema_exists("mrf")
    with pytest.raises(RuntimeError, match="denied"):
        await cms_doctors_module._ensure_schema_exists("mrf")

    assert status.await_count == 3
    assert scalar.await_count == 2


def test_cms_doctor_distribution_urls_keep_only_supported_downloads(
    cms_doctors_module,
):
    """Catalog parsing keeps supported files and the canonical CMS download route."""

    dataset_map = {
        "distribution": [
            {"downloadURL": " https://example.test/a.CSV "},
            {"downloadURL": "https://example.test/b.zip"},
            {"downloadURL": "https://example.test/DAC_NationalDownloadableFile?id=1"},
            {"downloadURL": "https://example.test/readme.txt"},
            {},
        ]
    }

    assert cms_doctors_module._distribution_urls(dataset_map) == [
        "https://example.test/a.CSV",
        "https://example.test/b.zip",
        "https://example.test/DAC_NationalDownloadableFile?id=1",
    ]


@pytest.mark.asyncio
async def test_cms_doctor_probe_skips_errors_and_http_failures(cms_doctors_module):
    """Source probing returns the first reachable candidate after bounded failures."""

    client = _CatalogClient(
        head_responses=(
            _CatalogResponse(enter_error=OSError("offline")),
            _CatalogResponse(status=503),
            _CatalogResponse(status=204),
        )
    )
    urls = ["https://example.test/a", "https://example.test/b", "https://example.test/c"]

    assert await cms_doctors_module._first_reachable_url(client, urls) == urls[2]
    assert client.head_urls == [(url, True, 60) for url in urls]
    assert await cms_doctors_module._first_reachable_url(client, []) is None


@pytest.mark.asyncio
async def test_cms_doctor_metastore_download_is_preferred(cms_doctors_module):
    """The authoritative dataset metastore wins when its file is reachable."""

    download_url = "https://example.test/doctors.csv"
    client = _CatalogClient(
        get_responses=(_CatalogResponse(payload={"distribution": [{"downloadURL": download_url}]}),),
        head_responses=(_CatalogResponse(status=200),),
    )

    assert await cms_doctors_module._fetch_doctors_download_url(client) == download_url
    assert len(client.get_urls) == 1


@pytest.mark.parametrize(
    "dataset_identity",
    [
        {"identifier": "mj5m-pzi6"},
        {"landingPage": "https://data.test/dataset/mj5m-pzi6"},
        {
            "title": "National Downloadable File",
            "description": "Doctors and Clinicians directory",
        },
    ],
)
@pytest.mark.asyncio
async def test_cms_doctor_catalog_fallback_accepts_each_stable_identity(
    cms_doctors_module,
    dataset_identity,
):
    """Fallback discovery recognizes each supported stable dataset identity."""

    download_url = "https://example.test/doctors.zip"
    dataset_map = {**dataset_identity, "distribution": [{"downloadURL": download_url}]}
    client = _CatalogClient(
        get_responses=(
            _CatalogResponse(enter_error=OSError("metastore unavailable")),
            _CatalogResponse(payload={"dataset": [{"identifier": "not-it"}, dataset_map]}),
        ),
        head_responses=(_CatalogResponse(status=200),),
    )

    assert await cms_doctors_module._fetch_doctors_download_url(client) == download_url
    assert len(client.get_urls) == 2


@pytest.mark.asyncio
async def test_cms_doctor_catalog_fallback_fails_closed(cms_doctors_module):
    """Fallback discovery distinguishes missing datasets from unreachable files."""

    missing_dataset_client = _CatalogClient(
        get_responses=(
            _CatalogResponse(payload={"distribution": []}),
            _CatalogResponse(payload={"dataset": []}),
        )
    )
    with pytest.raises(ValueError, match="Could not find CMS Doctors dataset"):
        await cms_doctors_module._fetch_doctors_download_url(missing_dataset_client)

    unavailable_file_client = _CatalogClient(
        get_responses=(
            _CatalogResponse(enter_error=OSError("metastore unavailable")),
            _CatalogResponse(
                payload={
                    "dataset": [
                        {
                            "identifier": "mj5m-pzi6",
                            "distribution": [{"downloadURL": "https://example.test/missing.csv"}],
                        }
                    ]
                }
            ),
        ),
        head_responses=(_CatalogResponse(status=404),),
    )
    with pytest.raises(ValueError, match="Could not find CMS Doctors CSV/ZIP"):
        await cms_doctors_module._fetch_doctors_download_url(unavailable_file_client)


@pytest.mark.asyncio
async def test_process_data_keeps_multiple_addresses_per_npi(monkeypatch, cms_doctors_module):
    csv_payload = (
        "NPI,Line 1 Street Address,Line 2 Street Address,City,State,Zip Code,Primary specialty\n"
        "1111111111,123 Main St,,Chicago,IL,60654,Internal Medicine\n"
        "1111111111,789 Lake St,,Chicago,IL,60610,Nurse Practitioner\n"
        "1111111111,789 Lake St,,Chicago,IL,60610,Nurse Practitioner\n"
    )
    zip_payload = _zip_bytes_with_csv("doctors.csv", csv_payload)

    pushed_rows = []

    async def _fake_push(rows, _cls):
        pushed_rows.extend(rows)

    monkeypatch.setattr(
        cms_doctors_module,
        "_fetch_doctors_download_url",
        AsyncMock(return_value="https://x/y.zip"),
    )
    monkeypatch.setattr(cms_doctors_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors_module, "push_objects", _fake_push)
    monkeypatch.setitem(
        __import__("sys").modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClient(zip_payload)),
    )

    import_context_map = {"import_date": "20260321", "context": {}}
    await cms_doctors_module.process_data(import_context_map, {"test_mode": True})

    assert len(pushed_rows) == 2
    assert len({pushed_doctor_row["address_checksum"] for pushed_doctor_row in pushed_rows}) == 2
    assert {pushed_doctor_row["npi"] for pushed_doctor_row in pushed_rows} == {1111111111}


@pytest.mark.asyncio
async def test_process_data_accepts_current_cms_lowercase_schema(monkeypatch, cms_doctors_module):
    csv_payload = (
        "npi,adr_ln_1,adr_ln_2,citytown,state,zip_code,pri_spec\n"
        "2222222222,456 Oak Ave,Ste 7,Austin,TX,78701,Family Practice\n"
    )
    zip_payload = _zip_bytes_with_csv("doctors.csv", csv_payload)

    pushed_rows = []

    async def _fake_push(rows, _cls):
        pushed_rows.extend(rows)

    monkeypatch.setattr(
        cms_doctors_module,
        "_fetch_doctors_download_url",
        AsyncMock(return_value="https://x/y.zip"),
    )
    monkeypatch.setattr(cms_doctors_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors_module, "push_objects", _fake_push)
    monkeypatch.setitem(
        __import__("sys").modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClient(zip_payload)),
    )

    import_context_map = {"import_date": "20260321", "context": {}}
    await cms_doctors_module.process_data(import_context_map, {"test_mode": True})

    assert pushed_rows == [
        {
            "npi": 2222222222,
            "address_checksum": pushed_rows[0]["address_checksum"],
            "address_line1": "456 Oak Ave",
            "address_line2": "Ste 7",
            "city": "Austin",
            "state": "TX",
            "zip_code": "78701",
            "provider_type": "Family Practice",
            "updated_at": pushed_rows[0]["updated_at"],
        }
    ]
