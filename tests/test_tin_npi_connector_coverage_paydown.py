# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from types import SimpleNamespace

import pytest

anchors = importlib.import_module("process.facility_anchors")


class _Response:
    def __init__(self, *, status=200, body=b"", json_body=None, read_error=None):
        self.status = status
        self._body = body
        self._json_body = json_body
        self._read_error = read_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False

    async def read(self):
        if self._read_error:
            raise self._read_error
        return self._body

    async def json(self, *, content_type=None):
        return self._json_body


class _Client:
    def __init__(self, *responses):
        self.responses = list(responses)

    def get(self, url, *, timeout):
        return self.responses.pop(0)


def _csv_bytes(headers, rows):
    values = [",".join(headers)]
    values.extend(
        ",".join(str(row.get(header, "")) for header in headers) for row in rows
    )
    return "\n".join(values).encode()


def test_facility_identity_normalizers_reject_ambiguous_values():
    assert anchors._normalize_zip("02108-1234") == "02108"
    assert anchors._normalize_zip("123") == ""
    assert anchors._normalize_identity_part(" North Clinic #2 ") == "northclinic2"
    assert anchors._normalize_npi("not-an-npi") is None
    assert anchors._normalize_phone("+44 20 7946 0958") is None
    assert anchors._normalize_medicare_ccn(None) is None
    assert (
        anchors._stage_index_name("facility_stage", "geo") == "facility_stage_idx_geo"
    )


def test_schema_and_archive_identifiers_are_bounded(monkeypatch):
    assert anchors._validate_schema_name("_private_2") == "_private_2"
    for invalid in ("", "2mrf", "mrf-live"):
        with pytest.raises(ValueError, match="Invalid schema name"):
            anchors._validate_schema_name(invalid)

    class _Clock:
        @classmethod
        def now(cls):
            return SimpleNamespace(strftime=lambda _fmt: "20260729")

    monkeypatch.setattr(anchors.datetime, "datetime", _Clock)
    assert anchors._normalize_import_id(" run-2026/07/29 ") == "run20260729"
    assert anchors._normalize_import_id("---") == "20260729"
    assert anchors._archived_identifier("short_name") == "short_name_old"
    archived = anchors._archived_identifier("x" * 80)
    assert archived.endswith("_old")
    assert len(archived) <= anchors.POSTGRES_IDENTIFIER_MAX_LENGTH


@pytest.mark.asyncio
async def test_schema_creation_recovers_only_when_namespace_exists(monkeypatch):
    class _SchemaDb:
        def __init__(self, *, exists):
            self.exists = exists
            self.statements = []

        async def status(self, sql):
            self.statements.append(sql)
            raise RuntimeError("concurrent schema creation")

        async def scalar(self, sql):
            assert "to_regnamespace('mrf')" in sql
            return self.exists

    existing = _SchemaDb(exists=True)
    monkeypatch.setattr(anchors, "db", existing)
    await anchors._ensure_schema_exists("mrf")

    missing = _SchemaDb(exists=False)
    monkeypatch.setattr(anchors, "db", missing)
    with pytest.raises(RuntimeError, match="concurrent schema creation"):
        await anchors._ensure_schema_exists("mrf")


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"Latitude": "bad", "Longitude": "bad", "Location": ""}, (None, None)),
        ({"Location": "41.88, -87.62"}, (41.88, -87.62)),
        ({"Location": "-120.5 40.5"}, (40.5, -120.5)),
        ({"Latitude": "42", "Longitude": "", "Location": "unresolved"}, (42.0, None)),
    ],
)
def test_coordinate_parser_accepts_only_plausible_orderings(row, expected):
    assert anchors._parse_lat_lng(row) == expected


@pytest.mark.asyncio
async def test_stage_indexes_preserve_unique_partial_and_method_contracts(monkeypatch):
    statements = []

    class _Db:
        async def status(self, sql):
            statements.append(" ".join(sql.split()))

    class _Stage:
        __tablename__ = "facility_anchor_stage"
        __my_index_elements__ = ["id"]
        __my_additional_indexes__ = [
            {
                "name": "coordinates",
                "index_elements": ["latitude", "longitude"],
                "using": "gist",
                "where": "latitude IS NOT NULL",
            },
            {"index_elements": ["state"]},
        ]

    monkeypatch.setattr(anchors, "db", _Db())
    await anchors._create_stage_indexes(_Stage, "mrf")
    await anchors._create_stage_indexes(SimpleNamespace(), "mrf")

    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS facility_anchor_stage_idx_primary"
        in statements[0]
    )
    assert (
        "USING gist (latitude, longitude) WHERE latitude IS NOT NULL" in statements[1]
    )
    assert "facility_anchor_stage_idx_state" in statements[2]


def _hrsa_csv(rows):
    headers = [
        "Site Name",
        "Geocode Latitude",
        "Geocode Longitude",
        "Site Address",
        "Site City",
        "Site State Abbreviation",
        "Site Postal Code",
        "Site Telephone Number",
        "FQHC Site NPI Number",
        "FQHC Site Medicare Billing Number",
        "Health Center Number",
        "BHCMIS Organization Identification Number",
        "BPHC Assigned Number",
        "Health Center Name",
        "Health Center Organization Street Address",
        "Health Center Organization City",
        "Health Center Organization State",
        "Health Center Organization ZIP Code",
    ]
    return _csv_bytes(headers, rows)


@pytest.mark.asyncio
async def test_hrsa_parser_filters_bad_rows_and_emits_normalized_anchor(monkeypatch):
    body = _hrsa_csv(
        [
            {"Geocode Latitude": "1", "Geocode Longitude": "2"},
            {
                "Site Name": "Bad coordinate",
                "Geocode Latitude": "x",
                "Geocode Longitude": "2",
            },
            {
                "Site Name": "Zero coordinate",
                "Geocode Latitude": "0",
                "Geocode Longitude": "0",
            },
            {
                "Site Name": "North Clinic",
                "Geocode Latitude": "41.9",
                "Geocode Longitude": "-87.6",
                "Site Address": "10 Main St",
                "Site City": "Chicago",
                "Site State Abbreviation": "IL",
                "Site Postal Code": "60601-1234",
                "Site Telephone Number": "1-312-555-0199",
                "FQHC Site NPI Number": "1234567890",
                "FQHC Site Medicare Billing Number": "12-AB",
                "Health Center Number": "HC-1",
                "BHCMIS Organization Identification Number": "ORG-1",
                "BPHC Assigned Number": "BPHC-1",
                "Health Center Name": "North Health",
                "Health Center Organization Street Address": "20 Main St",
                "Health Center Organization City": "Chicago",
                "Health Center Organization State": "IL",
                "Health Center Organization ZIP Code": "60602-1234",
            },
        ]
    )
    batches = []

    async def push(rows, stage):
        batches.append((list(rows), stage))

    monkeypatch.setattr(anchors, "push_objects", push)
    count = await anchors._fetch_and_parse_hrsa(
        _Client(_Response(body=body)),
        "stage",
        batch_size=1,
        test_mode=True,
        test_limit=1,
    )

    assert count == 1
    hrsa_anchor_row = batches[0][0][0]
    assert hrsa_anchor_row["facility_type"] == "FQHC"
    assert hrsa_anchor_row["telephone_number"] == "3125550199"
    assert hrsa_anchor_row["npi"] == 1234567890
    assert hrsa_anchor_row["medicare_ccn"] == "12AB"


@pytest.mark.asyncio
async def test_hrsa_parser_handles_tail_batch_http_failure_and_read_error(monkeypatch):
    pushed_rows = []

    async def push(rows, _stage):
        pushed_rows.extend(rows)

    monkeypatch.setattr(anchors, "push_objects", push)
    valid_body = _hrsa_csv(
        [{"Site Name": "Clinic", "Geocode Latitude": "40", "Geocode Longitude": "-75"}]
    )
    assert (
        await anchors._fetch_and_parse_hrsa(
            _Client(_Response(body=valid_body)), "stage", 10, False, 500
        )
        == 1
    )
    assert len(pushed_rows) == 1
    assert (
        await anchors._fetch_and_parse_hrsa(
            _Client(_Response(status=503)), "stage", 10, False, 500
        )
        == 0
    )
    assert (
        await anchors._fetch_and_parse_hrsa(
            _Client(_Response(read_error=OSError("truncated"))), "stage", 10, False, 500
        )
        == 0
    )


@pytest.mark.asyncio
async def test_cms_catalog_selects_csv_and_fails_closed_for_missing_metadata():
    catalog_by_field = {
        "dataset": [
            {"identifier": "other", "landingPage": "/dataset/other", "title": "Other"},
            {
                "identifier": "legacy",
                "landingPage": f"/dataset/{anchors.DEFAULT_CMS_HOSPITAL_DATASET_ID}",
                "title": "Hospitals",
                "distribution": [
                    {"downloadURL": "https://example.test/readme.json"},
                    {"downloadURL": "https://example.test/hospitals.CSV"},
                ],
            },
        ]
    }
    url = await anchors._fetch_cms_hospital_csv_url(
        _Client(_Response(json_body=catalog_by_field))
    )
    assert url == "https://example.test/hospitals.CSV"

    with pytest.raises(ValueError, match="Could not find Hospital"):
        await anchors._fetch_cms_hospital_csv_url(
            _Client(_Response(json_body={"dataset": []}))
        )
    with pytest.raises(
        ValueError, match="Could not find Hospital General Information CSV"
    ):
        await anchors._fetch_cms_hospital_csv_url(
            _Client(
                _Response(
                    json_body={
                        "dataset": [
                            {
                                "identifier": anchors.DEFAULT_CMS_HOSPITAL_DATASET_ID,
                                "distribution": [{"downloadURL": "metadata.json"}],
                            }
                        ]
                    }
                )
            )
        )


def _cms_csv(rows):
    return _csv_bytes(
        [
            "Facility ID",
            "Facility Name",
            "Hospital Name",
            "Address",
            "City",
            "City/Town",
            "State",
            "ZIP Code",
            "Latitude",
            "Longitude",
            "Location",
        ],
        rows,
    )


@pytest.mark.asyncio
async def test_cms_parser_filters_bad_rows_and_tracks_coordinate_coverage(monkeypatch):
    body = _cms_csv(
        [
            {"Facility ID": "ignored"},
            {
                "Facility ID": "H-1",
                "Facility Name": "General Hospital",
                "Address": "1 State St",
                "City": "Boston",
                "State": "MA",
                "ZIP Code": "02108-1234",
                "Location": "POINT (-71.0589 42.3601)",
            },
        ]
    )
    batches = []

    async def push(rows, stage):
        batches.append((list(rows), stage))

    monkeypatch.setenv(
        "HLTHPRT_CMS_HOSPITAL_CSV_URL", "https://example.test/hospitals.csv"
    )
    monkeypatch.setattr(anchors, "push_objects", push)
    cms_parse_counts = await anchors._fetch_and_parse_cms_hospitals(
        _Client(_Response(body=body)),
        "stage",
        batch_size=1,
        test_mode=True,
        test_limit=1,
        already_accepted=0,
    )

    assert cms_parse_counts == (1, 1)
    cms_anchor_row = batches[0][0][0]
    assert cms_anchor_row["id"] == "H-1"
    assert cms_anchor_row["zip_code"] == "02108"
    assert cms_anchor_row["medicare_ccn"] == "H1"


@pytest.mark.asyncio
async def test_cms_parser_handles_tail_batch_http_failure_and_read_error(monkeypatch):
    pushed_rows = []

    async def push(rows, _stage):
        pushed_rows.extend(rows)

    monkeypatch.setenv(
        "HLTHPRT_CMS_HOSPITAL_CSV_URL", "https://example.test/hospitals.csv"
    )
    monkeypatch.setattr(anchors, "push_objects", push)
    monkeypatch.setattr(anchors.uuid, "uuid4", lambda: "generated-id")
    body = _cms_csv([{"Hospital Name": "Fallback Hospital", "City/Town": "Reno"}])

    assert await anchors._fetch_and_parse_cms_hospitals(
        _Client(_Response(body=body)), "stage", 10, False, 500, 0
    ) == (1, 0)
    assert pushed_rows[0]["id"] == "generated-id"
    assert (
        await anchors._fetch_and_parse_cms_hospitals(
            _Client(_Response(status=500)), "stage", 10, False, 500, 0
        )
        == 0
    )
    assert await anchors._fetch_and_parse_cms_hospitals(
        _Client(_Response(read_error=OSError("truncated"))), "stage", 10, False, 500, 0
    ) == (0, 0)
