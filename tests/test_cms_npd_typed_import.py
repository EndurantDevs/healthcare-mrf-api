# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure parts of the typed CMS NPD bulk import, with a synthetic release builder shared by the DB test."""

import hashlib
import importlib.util
import json
from compression import zstd
from pathlib import Path
from types import SimpleNamespace

import pytest

from process import cms_npd_source
from process import cms_npd_typed_release as release_sql
from process.cms_npd_source import FileProbe, ObservedRelease, parse_manifest
from process.cms_npd_typed_import import HttpSource
from process.cms_npd_typed_schema import (
    FINISH_STATEMENTS,
    RESOURCE_FILES,
    release_schema_name,
    release_table_statements,
)
from process.cms_npd_typed_stage import CmsNpdTypedImportError, _verified_counts, encoder_threads, file_plan

_META = {"lastUpdated": "2026-01-01T00:00:00Z"}
_ADDRESS = [{"use": "work", "line": ["1 Example St"], "city": "Exampleville", "state": "ZZ", "postalCode": "00001"}]


def _uuid(kind, index):
    return f"0000000{kind}-0000-4000-8000-{index:012d}"


def _resources(index):
    """One synthetic record of every type; the endpoint names an organization that does not exist."""
    organization = f"1{index:09d}"
    location_reference = f"Location/Location-{_uuid(1, index)}"
    networks = [{"reference": "Organization/Organization-net-1"}]
    return {
        "Organization": {
            "id": f"Organization-{organization}",
            "address": _ADDRESS,
            "type": [{"coding": [{"code": "prov"}]}, {"text": "ein"}],
        },
        "Practitioner": {
            "id": f"Practitioner-{organization}",
            "address": _ADDRESS,
            "identifier": [{"system": "http://terminology.hl7.org/NamingSystem/npi", "value": organization}],
        },
        "PractitionerRole": {
            "id": f"PractitionerRole-{_uuid(3, index)}",
            "practitioner": {"reference": f"Practitioner/Practitioner-{organization}"},
            "organization": {"reference": f"Organization/Organization-{organization}"},
            "location": [{"reference": f"Location/Location-{_uuid(1, index)}"}],
            "endpoint": [{"reference": f"Endpoint/Endpoint-{_uuid(2, index)}"}],
            "insurancePlan": [{"reference": f"InsurancePlan/InsurancePlan-{_uuid(5, index)}"}],
            "network": networks,
        },
        "Location": {
            "id": f"Location-{_uuid(1, index)}",
            "managingOrganization": {"reference": f"Organization/Organization-{organization}"},
            "partOf": {"reference": location_reference},
        },
        "Endpoint": {
            "id": f"Endpoint-{_uuid(2, index)}",
            "managingOrganization": {"reference": f"Organization/Organization-9{index:09d}"},
        },
        "HealthcareService": {
            "id": f"HealthcareService-{_uuid(4, index)}",
            "providedBy": {"reference": f"Organization/Organization-{organization}"},
        },
        "InsurancePlan": {
            "id": f"InsurancePlan-{_uuid(5, index)}",
            "network": networks,
            "administeredBy": networks[0],
            "coverageArea": [{"reference": location_reference}],
            "plan": [
                {
                    "network": networks,
                    "coverageArea": [{"reference": location_reference}],
                    "period": {"start": "2026-01-01"},
                }
            ],
            "coverage": [{"network": networks}],
        },
        "OrganizationAffiliation": {
            "id": f"OrganizationAffiliation-{_uuid(6, index)}",
            "organization": {"reference": "Organization/Organization-net-1"},
            "participatingOrganization": {"reference": f"Organization/Organization-{organization}"},
        },
    }


def _ndjson(resource_type, count, bad_line=False):
    records = [_resources(index)[resource_type] for index in range(1, count + 1)]
    if resource_type == "Organization":
        records.append({"id": "Organization-net-1"})
    lines = [json.dumps({"resourceType": resource_type, "meta": _META, **record}) for record in records]
    if bad_line:
        lines.append(json.dumps({"resourceType": resource_type}))  # no id: the encoder rejects it
    return "".join(line + "\n" for line in lines).encode()


def write_release(directory, count, generated_at="2026-01-01", bad_type=None):
    """Write eight zstd files and a manifest; return the observed release (id = manifest digest)."""
    directory.mkdir(parents=True, exist_ok=True)
    files_by_name = {}
    for stem, resource_type, _, _ in RESOURCE_FILES:
        decoded = _ndjson(resource_type, count, bad_line=resource_type == bad_type)
        compressed = zstd.compress(decoded)
        (directory / f"{stem}.ndjson.zst").write_bytes(compressed)
        files_by_name[f"{stem}.ndjson"] = {"compressed_bytes": len(compressed), "original_bytes": len(decoded)}
    totals_by_field = {
        field: sum(entry[field] for entry in files_by_name.values()) for field in ("compressed_bytes", "original_bytes")
    }
    raw = json.dumps(
        {
            "compression_algorithm": "zstd",
            "generated_at": generated_at,
            "files": files_by_name,
            "totals": totals_by_field,
        }
    ).encode()
    (directory / "manifest.json").write_bytes(raw)
    manifest = parse_manifest(raw)
    probes = tuple(FileProbe(f'"etag-{spec.name}"', spec.compressed_bytes) for spec in manifest.files)
    return ObservedRelease(manifest, probes, hashlib.sha256(raw).hexdigest())


class LocalSource:
    """An already downloaded release directory standing in for the download site."""

    def __init__(self, directory, observed):
        self.directory, self.observed, self.kept = directory, observed, None

    def observe(self):
        return self.observed

    def fetch(self, observed):
        assert observed is self.observed
        return self.directory

    def prune(self, keep_release_ids):
        self.kept = set(keep_release_ids)


def test_release_schema_name_uses_a_validated_id_prefix():
    assert release_schema_name("ab" * 32) == "cms_npd_" + "ab" * 8
    with pytest.raises(ValueError):
        release_schema_name("AB" * 32)


def test_release_table_statements_are_qualified_only_with_a_safe_schema():
    assert 'CREATE TABLE IF NOT EXISTS "control_1".cms_npd_release (' in release_table_statements("control_1")[0]
    assert "CREATE TABLE IF NOT EXISTS cms_npd_release (" in release_table_statements()[0]
    with pytest.raises(ValueError):
        release_table_statements('x"; DROP')


def test_migration_creates_the_release_table_in_the_control_schema(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261119090000_cms_npd_typed_release.py"
    specification = importlib.util.spec_from_file_location("cms_npd_typed_release_migration", path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    executed_statements = []
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=executed_statements.append))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "control_1")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.upgrade()
    assert executed_statements == list(release_table_statements("control_1"))
    assert "REFERENCES" not in executed_statements[0]
    with pytest.raises(RuntimeError):
        migration.downgrade()


def test_finish_analyzes_only_release_tables():
    assert "ANALYZE" not in FINISH_STATEMENTS
    assert {"ANALYZE practitioner", "ANALYZE network"} <= set(FINISH_STATEMENTS)


@pytest.mark.parametrize(("cpus", "concurrency", "expected"), [(16, 3, 5), (2, 3, 1), (None, 3, 1), (8, 0, 8)])
def test_encoder_threads_share_the_cpus_between_concurrent_files(cpus, concurrency, expected):
    assert encoder_threads(cpus, concurrency) == expected


def test_shrink_and_empty_checks():
    current_by_field = {"release_id": "a" * 64, "row_counts": {"practitioner": 100, "location": 10, "gone": 5}}
    full_by_table = {table: 1 for table in release_sql.RESOURCE_TABLES}
    assert release_sql.shrunk_tables({"practitioner": 80, "location": 7}, current_by_field["row_counts"]) == [
        "location"
    ]
    assert release_sql.empty_tables({**full_by_table, "endpoint": 0}) == ["endpoint"]
    release_sql.assert_publishable({**full_by_table, "practitioner": 81, "location": 9}, current_by_field)
    release_sql.assert_publishable(full_by_table, None)
    with pytest.raises(CmsNpdTypedImportError) as shrunk:
        release_sql.assert_publishable({**full_by_table, "practitioner": 79, "location": 9}, current_by_field)
    assert shrunk.value.control_error_code == "cms_npd_typed_table_shrunk"
    with pytest.raises(CmsNpdTypedImportError) as empty:
        release_sql.assert_publishable({**full_by_table, "insurance_plan": 0}, None)
    assert empty.value.control_error_code == "cms_npd_typed_table_empty"


def test_file_plan_follows_staging_order_and_manifest_sizes(tmp_path):
    observed = write_release(tmp_path, 2)
    plans = file_plan(tmp_path, observed)
    assert [plan.main_table for plan in plans] == [main for _, _, main, _ in RESOURCE_FILES]
    practitioner = plans[0]
    assert practitioner.tables == ("practitioner", "practitioner_address")
    assert practitioner.path == tmp_path / "06-Practitioner.ndjson.zst"
    assert practitioner.compressed_bytes == practitioner.path.stat().st_size
    assert practitioner.etag == '"etag-06-Practitioner.ndjson"'
    assert plans[1].tables == ("practitioner_role",)


def test_verified_counts_reject_every_mismatch(tmp_path):
    plan = file_plan(tmp_path, write_release(tmp_path, 1))[0]
    summary_by_field = {
        "resource_type": "Practitioner",
        "rows": 1,
        "address_rows": 1,
        "input_bytes": plan.original_bytes,
    }
    good = json.dumps(summary_by_field).encode()
    size = plan.original_bytes
    assert _verified_counts(plan, 0, good, b"", [1, 1], size) == {"practitioner": 1, "practitioner_address": 1}
    cases = [
        ((2, good, b"PTG2_SCANNER_ERROR\tline 3", [1, 1], size), "cms_npd_typed_scanner_failed"),
        ((0, b"", b"", [1, 1], size), "cms_npd_typed_scanner_summary_invalid"),
        ((0, good, b"", [1, 1], size - 1), "cms_npd_typed_decoded_size_mismatch"),
        ((0, good, b"", [1, 0], size), "cms_npd_typed_copy_count_mismatch"),
    ]
    for arguments, code in cases:
        with pytest.raises(CmsNpdTypedImportError) as error:
            _verified_counts(plan, *arguments)
        assert error.value.control_error_code == code


class _RecordingConnection:
    def __init__(self, dropped):
        self.calls, self.dropped = [], dropped

    def transaction(self):
        connection = self

        class _Transaction:
            async def __aenter__(self):
                connection.calls.append("BEGIN")

            async def __aexit__(self, *exc):
                connection.calls.append("COMMIT" if exc[0] is None else "ROLLBACK")

        return _Transaction()

    async def fetchrow(self, sql, *args):
        self.calls.append(" ".join(sql.split()[:1]))
        return self.dropped

    async def execute(self, sql, *args):
        self.calls.append(" ".join(sql.split()[:1]))
        self.args = args


async def test_publish_moves_pointers_in_one_transaction_in_order():
    connection = _RecordingConnection({"release_id": "c" * 64, "schema_name": "cms_npd_" + "c" * 16})
    release_by_field = {
        "release_id": "a" * 64,
        "schema": "cms_npd_" + "a" * 16,
        "generated_at": "2026-01-02",
        "run_id": "run-1",
        "row_counts": {"practitioner": 1},
        "unresolved_counts": {"endpoint.organization": 0},
        "files": {},
    }
    dropped = await release_sql.publish(connection, "control_1", release_by_field)
    assert connection.calls == ["BEGIN", "DELETE", "UPDATE", "INSERT", "COMMIT"]
    assert dropped["release_id"] == "c" * 64
    assert connection.args[2].isoformat() == "2026-01-02"
    assert json.loads(connection.args[4]) == {"practitioner": 1}


def test_download_release_writes_the_manifest_and_downloads_every_file(tmp_path, monkeypatch):
    observed = write_release(tmp_path / "source", 1)
    downloaded_files = []

    def fake_download(client, base_url, directory, spec, probe):
        downloaded_files.append((directory, spec.name, probe.etag))
        return directory / f"{spec.name}.zst"

    monkeypatch.setattr(cms_npd_source, "_download", fake_download)
    directory = cms_npd_source.download_release(tmp_path, observed, base_url="https://example.test", parallel=3)
    assert directory == tmp_path / "cms-npd" / "releases" / observed.vector_sha256
    assert (directory / "manifest.json").read_bytes() == observed.manifest.raw
    assert sorted(name for _, name, _ in downloaded_files) == sorted(spec.name for spec in observed.manifest.files)
    assert {path for path, _, _ in downloaded_files} == {directory}
    (directory / "manifest.json").write_bytes(b"kept")  # a resumed download never rewrites the pinned manifest
    cms_npd_source.download_release(tmp_path, observed, parallel=0)
    assert (directory / "manifest.json").read_bytes() == b"kept"
    with pytest.raises(cms_npd_source.CmsNpdSourceError, match="cms_npd_artifact_root_missing"):
        cms_npd_source.download_release(tmp_path / "missing", observed)


def test_http_source_prune_keeps_only_named_release_directories(tmp_path):
    releases = tmp_path / "cms-npd" / "releases"
    for name in ("a" * 64, "b" * 64, "c" * 64, "not-a-release"):
        (releases / name).mkdir(parents=True)
    HttpSource(tmp_path).prune({"a" * 64, "b" * 64})
    assert sorted(path.name for path in releases.iterdir()) == ["a" * 64, "b" * 64, "not-a-release"]
