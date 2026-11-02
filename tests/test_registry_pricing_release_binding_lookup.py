# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure consumed-phase parity; native metadata SQL acceptance is separate."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from api import plan_release_pricing_projection as projection
from process import registry_pricing_release_binding_lookup as lookup
from process import registry_required_target_coverage as coverage
from process.network_approved_catalog_evidence import ApprovedNetworkCatalogEvidenceRecord
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_plan_release_serving import PLAN_RELEASE_ID, _binding_row
from tests.test_plan_release_supporting_metadata import commitment_page

REFERENCE = {
    "healthporta_plan_id": "hpplan_" + "1" * 26,
    "plan_release_id": PLAN_RELEASE_ID,
    "serving_revision_id": "hpserve_" + "3" * 26,
    "role": "in_network",
    "ordinal": 0,
    "snapshot_id": "ptg2:release-old",
}
REPORT = {
    "targets": [{"mapping_status": "resolved", "network_id": 1, "directory_available": True, "priceable": None}],
    "totals": {"pricing_evidence_count": None, "directory_available": 1},
    "assessment": {"pricing": "not_assessed"},
    "provenance": {"original": "retained"},
}


class Savepoint:
    def __init__(self, driver):
        self.driver = driver

    async def start(self):
        self.driver.lifecycle.append("start")

    async def rollback(self):
        self.driver.lifecycle.append("rollback")
        if self.driver.rollback_error:
            raise self.driver.rollback_error

    async def commit(self):
        self.driver.lifecycle.append("release")
        if self.driver.release_error:
            raise self.driver.release_error


class Driver:
    def __init__(self, rows=None):
        self.rows = [_binding_row()] if rows is None else rows
        self.calls = []
        self.lifecycle = []
        self.tasks = []
        self.failure = None
        self.rollback_error = None
        self.release_error = None
        self.in_transaction = True
        self.relations_available = True

    def is_in_transaction(self):
        return self.in_transaction

    def transaction(self):
        return Savepoint(self)

    async def fetch(self, sql, *parameters):
        self.calls.append((sql, parameters))
        self.tasks.append(asyncio.current_task())
        if "supporting_commitments AS MATERIALIZED" in sql:
            import json

            requested_ids = {entry["plan_release_id"] for entry in json.loads(parameters[0])}
            return commitment_page([binding for binding in self.rows if binding["plan_release_id"] in requested_ids])
        if "AS storage_present" in sql:
            return []
        if self.failure:
            raise self.failure
        if "AS readable" in sql:
            return [{"readable": True} for _ in range(2)]
        if "to_regclass" in sql:
            return [{"relation_name": name, "available": self.relations_available} for name in parameters[0]]
        return self.rows

    async def fetchrow(self, sql, *parameters):
        import json

        self.calls.append((sql, parameters))
        self.tasks.append(asyncio.current_task())
        if self.failure:
            raise self.failure
        encoded = json.dumps(self.rows)
        is_bounded = len(encoded.encode()) <= parameters[2]
        return {"bounded": is_bounded, "rows_json": encoded if is_bounded else None}


async def append(driver, **options):
    return await lookup.append_pricing_binding_metadata(
        driver,
        deepcopy(REPORT),
        object(),
        control_schema="sample_control",
        max_report_bytes=options.get("limit", 16 * 1024 * 1024),
    )


@pytest.fixture
def retained_export(monkeypatch):
    requests = [{"network_id": 1, "pricing_refs": [deepcopy(REFERENCE)], "benefit_refs": None}]
    calls = []

    async def export(driver, approved, **options):
        import json

        calls.append((driver, approved, options, asyncio.current_task()))
        return SimpleNamespace(
            records=tuple(
                ApprovedNetworkCatalogEvidenceRecord(request["network_id"], 3, 2, "retained") for request in requests
            ),
            approved_revision=4,
            approved_map_sha256="a" * 64,
            request_sha256="b" * 64,
            request_bytes=json.dumps({"networks": requests}).encode(),
        )

    monkeypatch.setattr(lookup, "read_approved_network_catalog_evidence", export)

    async def unassessed_rows(*args, **kwargs):
        return {}

    monkeypatch.setattr(lookup, "read_pricing_snapshot_row_checks", unassessed_rows)
    return requests, calls


@pytest.mark.asyncio
async def test_bulk_phase_reuses_one_driver_and_keeps_pricing_unknown(retained_export):
    requests, exports = retained_export
    second_id = "hprelease_" + "4" * 26
    requests[0]["pricing_refs"] += [deepcopy(REFERENCE), {**REFERENCE, "plan_release_id": second_id}]
    driver = Driver([_binding_row(), _binding_row(plan_release_id=second_id)])
    result = await append(driver)
    assert len(exports) == 1 and exports[0][0] is driver
    assert len(driver.calls) == 4 and driver.calls[3][1][0] == [PLAN_RELEASE_ID, second_id]
    assert all(task is asyncio.current_task() for task in driver.tasks)
    assert driver.lifecycle == ["start", "release"] and driver.is_in_transaction()
    assert {key: result[key] for key in REPORT if key != "provenance"} == {
        key: value for key, value in REPORT.items() if key != "provenance"
    }
    assert result["provenance"]["original"] == "retained"
    provenance = result["provenance"]["pricing_binding_metadata"]
    assert provenance["full_readiness"] == "not_assessed"
    references = provenance["networks"][0]["pricing_refs"]
    assert all(item["resolved"] is False and item["binding_metadata_match"] is True for item in references)
    assert references[0]["reference"] == REFERENCE


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "unpinned_sibling", "incomplete", "duplicate", "wrong_header", "moved"])
async def test_all_sibling_metadata_and_exact_reference_tuple_are_required(retained_export, change):
    updates_by_case = {
        "missing": [],
        "unpinned_sibling": [
            {"expected_binding_count": 2},
            {"expected_binding_count": 2, "binding_ordinal": 1, "is_pinned": False},
        ],
        "incomplete": [{"expected_binding_count": 2}],
        "duplicate": [{"expected_binding_count": 2}, {"expected_binding_count": 2}],
        "wrong_header": [{"release_status": "draft"}],
        "moved": [{"serving_revision_id": "hpserve_" + "5" * 26}],
    }
    rows = [_binding_row(**updates) for updates in updates_by_case[change]]
    result = await append(Driver(rows))
    witness = result["provenance"]["pricing_binding_metadata"]["networks"][0]["pricing_refs"][0]
    assert witness["resolved"] is False and witness["binding_metadata_match"] is False
    assert result["targets"][0]["directory_available"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.UndefinedTableError("synthetic"),
        asyncio.CancelledError(),
        ValueError("caller"),
        asyncpg.InsufficientPrivilegeError("synthetic"),
        asyncpg.QueryCanceledError("synthetic"),
    ],
)
async def test_only_recovered_physical_sql_error_is_optional(retained_export, failure):
    driver = Driver()
    driver.failure = failure
    if isinstance(failure, lookup._OPTIONAL_STORAGE_ERRORS):
        result = await append(driver)
        assert result["targets"] == REPORT["targets"]
    else:
        with pytest.raises(type(failure)) as caught:
            await append(driver)
        assert caught.value is failure
    assert driver.lifecycle == ["start", "rollback"] and driver.is_in_transaction()


@pytest.mark.asyncio
async def test_failed_rollback_preserves_original_error(retained_export):
    driver = Driver()
    driver.failure = asyncpg.UndefinedTableError("synthetic")
    driver.rollback_error = asyncio.CancelledError()
    with pytest.raises(asyncpg.UndefinedTableError) as caught:
        await append(driver)
    assert caught.value is driver.failure and "rollback was incomplete" in caught.value.__notes__[0]


@pytest.mark.asyncio
async def test_failed_savepoint_release_is_not_optional(retained_export):
    driver = Driver()
    driver.release_error = asyncpg.PostgresError("synthetic")
    with pytest.raises(asyncpg.PostgresError) as caught:
        await append(driver)
    assert caught.value is driver.release_error


@pytest.mark.asyncio
async def test_additive_absence_and_report_budget_preserve_directory(retained_export):
    driver = Driver()
    driver.relations_available = False
    result = await append(driver)
    assert len(driver.calls) == 1 and result["targets"] == REPORT["targets"]
    assert await append(Driver(), limit=1) == REPORT


@pytest.mark.asyncio
async def test_actual_report_body_consumes_the_metadata_phase_once(monkeypatch):
    driver = Driver()
    manifest = PinnedNetworkServingManifest(
        approved_custom_revision=4,
        source_generations={},
        schema_name="sample_serving",
        generation_id=1,
        candidate_id="00000000-0000-0000-0000-000000000001",
        schema_revision=1,
        manifest_sha256="b" * 64,
        address_table_oid=1,
    )

    async def fetchrow(sql, *parameters):
        if "transaction_isolation" in sql:
            return {"isolation": "repeatable read", "readonly": "on"}
        if sql.startswith("SELECT snapshot.snapshot_id"):
            return {"metadata_valid": True}
        if sql.startswith("SELECT manifest.generation_id"):
            return {"candidate_id": coverage.UUID(manifest.candidate_id), "manifest_status": "current"}
        return {"valid": True, "bounded": True, "native_input": "{}"}

    async def resolve(*args, **kwargs):
        return manifest

    async def pin(*args, **kwargs):
        return SimpleNamespace(generation_id="a" * 64)

    async def bundle(*args):
        return {}

    calls = []
    recipes = Mock()

    async def consumed(connection, report, approved, **options):
        calls.append((connection, report, options))
        return report

    driver.fetchrow = fetchrow
    monkeypatch.setattr(coverage, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(coverage, "pin_retained_approved_membership_source", pin)
    monkeypatch.setattr(coverage, "_read_review_bundle", bundle)
    monkeypatch.setattr(coverage, "_verify_ledger", lambda *args: {})
    monkeypatch.setattr(coverage, "verify_source_recipe_metadata", recipes)
    monkeypatch.setattr(coverage, "_native", lambda *args: {})
    monkeypatch.setattr(coverage, "_coverage_report", lambda *args: REPORT)
    monkeypatch.setattr(coverage, "append_pricing_binding_metadata", consumed)
    assert await coverage._read(driver, coverage.UUID(manifest.candidate_id), None, "sample_control") is REPORT
    recipes.assert_called_once_with(
        {"candidate_id": coverage.UUID(manifest.candidate_id), "manifest_status": "current"}
    )
    assert len(calls) == 1 and calls[0][0] is driver and calls[0][1] is REPORT


@pytest.mark.parametrize("retained", (False, True))
def test_native_set_query_preserves_all_original_singleton_predicates(retained):
    for has_projection in (False, True):
        bulk_sql = projection.plan_release_binding_set_sql(
            "synthetic", include_pricing_projection=has_projection, retained=retained
        )
        restored_sql = bulk_sql.replace(
            "revision.plan_release_id = ANY($1::text[])", "revision.plan_release_id = :plan_release_id"
        ).replace("$2", ":pin_owner_type")
        assert restored_sql == projection.plan_release_serving_sql(
            "synthetic", include_pricing_projection=has_projection, retained=retained
        )
        assert "ptg2_current" not in bulk_sql and "LIMIT" not in bulk_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("release_count", (1, 8, 64))
async def test_release_metadata_query_count_is_fixed(retained_export, release_count):
    requests, exports = retained_export
    requests.clear()
    rows = []
    for index in range(release_count):
        identity = "hprelease_" + f"{index:026d}"
        requests.append(
            {
                "network_id": index + 1,
                "pricing_refs": [{**REFERENCE, "plan_release_id": identity}],
                "benefit_refs": None,
            }
        )
        rows.append(_binding_row(plan_release_id=identity))
    driver = Driver(rows)
    report_by_field = {
        **REPORT,
        "targets": [{**REPORT["targets"][0], "network_id": index + 1} for index in range(release_count)],
    }
    result = await lookup.append_pricing_binding_metadata(
        driver, report_by_field, object(), control_schema="sample_control", max_report_bytes=16 * 1024 * 1024
    )
    assert len(exports) == 1 and len(driver.calls) == 4
    assert len(result["provenance"]["pricing_binding_metadata"]["release_metadata"]) == release_count


@pytest.mark.asyncio
async def test_invalid_reference_identity_and_export_failure_remain_primary(retained_export, monkeypatch):
    requests, exports = retained_export
    requests[0]["pricing_refs"][0]["plan_release_id"] = "invalid"
    driver = Driver()
    with pytest.raises(ValueError, match="identity_invalid"):
        await append(driver)
    assert not driver.calls and not driver.lifecycle
    original = ValueError("retained source invalid")

    async def failed_export(*args, **kwargs):
        raise original

    monkeypatch.setattr(lookup, "read_approved_network_catalog_evidence", failed_export)
    with pytest.raises(ValueError) as caught:
        await append(Driver())
    assert caught.value is original


@pytest.mark.asyncio
async def test_null_and_explicit_empty_refs_remain_distinct(retained_export):
    requests, _ = retained_export
    requests[0]["pricing_refs"] = None
    driver = Driver()
    result = await append(driver)
    assert result["provenance"]["pricing_binding_metadata"]["networks"][0]["pricing_refs"] is None
    assert not driver.calls and not driver.lifecycle
    requests[0]["pricing_refs"] = []
    result = await append(driver)
    assert result["provenance"]["pricing_binding_metadata"]["networks"][0]["pricing_refs"] == []
    assert result["totals"]["pricing_evidence_count"] is None


@pytest.mark.asyncio
async def test_unexpected_release_identity_is_not_optional(retained_export):
    driver = Driver([_binding_row(plan_release_id="hprelease_" + "9" * 26)])
    with pytest.raises(ValueError, match="identity_invalid"):
        await append(driver)
    assert driver.lifecycle == ["start", "release"] and driver.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_resource_only", [True, False])
async def test_optional_export_refusal_is_narrow_and_keeps_directory(monkeypatch, is_resource_only):
    from process.network_approved_catalog_evidence import (
        ApprovedNetworkCatalogEvidenceError,
        ApprovedNetworkCatalogEvidenceResourceLimit,
    )

    failure = (
        ApprovedNetworkCatalogEvidenceResourceLimit if is_resource_only else ApprovedNetworkCatalogEvidenceError
    )()

    async def refuse(*args, **kwargs):
        raise failure

    monkeypatch.setattr(lookup, "read_approved_network_catalog_evidence", refuse)
    driver = Driver()
    if is_resource_only:
        result = await append(driver)
        assert result["targets"] == REPORT["targets"] and result["totals"] == REPORT["totals"]
        assert result["assessment"] == REPORT["assessment"]
        assert result["provenance"]["pricing_binding_metadata"] == {
            "status": "unavailable",
            "full_readiness": "not_assessed",
        }
    else:
        with pytest.raises(ApprovedNetworkCatalogEvidenceError) as caught:
            await append(driver)
        assert caught.value is failure
    assert driver.calls == driver.lifecycle == []


@pytest.mark.asyncio
async def test_bulk_source_legacy_and_retired_metadata_keep_separate_closed_routes(retained_export, monkeypatch):
    import json

    from tests.test_plan_release_source_bindings import header

    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    legacy_id = "hprelease_" + "4" * 26
    retired_id = "hprelease_" + "5" * 26
    requests, _ = retained_export
    requests[0]["pricing_refs"].extend(
        {**REFERENCE, "plan_release_id": identity} for identity in (legacy_id, retired_id)
    )
    custody = AsyncMock()
    monkeypatch.setattr(lookup, "require_source_binding_custody_fetch", custody)

    class MixedDriver(Driver):
        async def fetch(self, sql, *parameters):
            if "AS storage_present" in sql:
                self.calls.append((sql, parameters))
                return [
                    header(),
                    header(plan_release_id=legacy_id, storage_present=False),
                    header(plan_release_id=retired_id, current_header_count=0),
                ]
            return await super().fetch(sql, *parameters)

        async def fetchrow(self, sql, *parameters):
            self.calls.append((sql, parameters))
            rows = [row for row in self.rows if row["plan_release_id"] in parameters[0]]
            return {"bounded": True, "rows_json": json.dumps(rows)}

    driver = MixedDriver(
        [_binding_row(plan_release_id=identity) for identity in (PLAN_RELEASE_ID, legacy_id, retired_id)]
    )
    metadata_report = await append(driver)
    phases = [(sql, params) for sql, params in driver.calls if "bindings AS MATERIALIZED" in sql]
    assert len(phases) == 2
    assert [parameters[0] for _, parameters in phases] == [[PLAN_RELEASE_ID], [legacy_id]]
    legacy_sql = next(sql for sql, params in phases if params[0] == [legacy_id])
    source_sql = next(sql for sql, params in phases if params[0] == [PLAN_RELEASE_ID])
    assert "plan_release_snapshot_binding" in legacy_sql
    assert "plan_release_snapshot_binding" not in source_sql
    assert "plan_release_snapshot_pin" not in source_sql
    assert "hp_plan_release_binding" in source_sql
    custody.assert_awaited_once()
    metadata = metadata_report["provenance"]["pricing_binding_metadata"]["release_metadata"]
    assert metadata[PLAN_RELEASE_ID] is not None and metadata[legacy_id] is not None
    assert metadata[retired_id] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed_marker", [False, True])
async def test_bulk_retired_source_release_never_queries_legacy_bindings(
    retained_export, monkeypatch, malformed_marker
):
    from tests.test_plan_release_source_bindings import header

    custody = AsyncMock()
    monkeypatch.setattr(lookup, "require_source_binding_custody_fetch", custody)

    class RetiredDriver(Driver):
        async def fetch(self, sql, *parameters):
            if "AS storage_present" in sql:
                self.calls.append((sql, parameters))
                retired = header(current_header_count=0)
                if malformed_marker:
                    retired["binding_storage"] = "unknown-storage"
                return [retired]
            return await super().fetch(sql, *parameters)

    driver = RetiredDriver()
    if malformed_marker:
        with pytest.raises(ValueError, match="binding_storage_invalid"):
            await append(driver)
    else:
        result = await append(driver)
        provenance = result["provenance"]["pricing_binding_metadata"]
        assert provenance["release_metadata"][PLAN_RELEASE_ID] is None
        assert provenance["networks"][0]["pricing_refs"][0]["binding_metadata_match"] is False
    custody.assert_not_awaited()
    assert all("bindings AS MATERIALIZED" not in sql for sql, _ in driver.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["source_custody", "source_rows", "legacy_rows"])
@pytest.mark.parametrize("error_type", [asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError])
async def test_missing_claimed_storage_refuses_while_legacy_remains_optional(
    retained_export, monkeypatch, stage, error_type
):
    from tests.test_plan_release_source_bindings import header

    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    failure = error_type("synthetic")
    custody = AsyncMock(side_effect=failure if stage == "source_custody" else None)
    monkeypatch.setattr(lookup, "require_source_binding_custody_fetch", custody)

    class MissingStorageDriver(Driver):
        async def fetch(self, sql, *parameters):
            if "AS storage_present" in sql:
                self.calls.append((sql, parameters))
                return [header(storage_present=stage != "legacy_rows")]
            return await super().fetch(sql, *parameters)

        async def fetchrow(self, sql, *parameters):
            raise failure

    driver = MissingStorageDriver()
    if stage == "legacy_rows":
        assert (await append(driver))["targets"] == REPORT["targets"]
        custody.assert_not_awaited()
    else:
        with pytest.raises(ValueError, match="plan_release_source_binding_storage_missing") as caught:
            await append(driver)
        assert caught.value.__cause__ is failure
        custody.assert_awaited_once()
    assert driver.lifecycle == ["start", "rollback"] and driver.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_storage", ["legacy", "source", "both"])
@pytest.mark.parametrize("error_type", [asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError])
async def test_mixed_storage_errors_preserve_the_canonical_and_optional_legacy_boundary(
    retained_export, monkeypatch, failed_storage, error_type
):
    import json

    from tests.test_plan_release_source_bindings import header

    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    legacy_id = "hprelease_" + "4" * 26
    requests, _ = retained_export
    requests[0]["pricing_refs"].append({**REFERENCE, "plan_release_id": legacy_id})
    custody = AsyncMock()
    monkeypatch.setattr(lookup, "require_source_binding_custody_fetch", custody)
    failure = error_type("synthetic")
    queried_storage_names = []

    class MixedStorageDriver(Driver):
        async def fetch(self, sql, *parameters):
            if "AS storage_present" in sql:
                self.calls.append((sql, parameters))
                return [header(), header(plan_release_id=legacy_id, storage_present=False)]
            return await super().fetch(sql, *parameters)

        async def fetchrow(self, sql, *parameters):
            self.calls.append((sql, parameters))
            storage = "source" if parameters[0] == [PLAN_RELEASE_ID] else "legacy"
            queried_storage_names.append(storage)
            if failed_storage in (storage, "both"):
                raise failure
            rows = [row for row in self.rows if row["plan_release_id"] in parameters[0]]
            return {"bounded": True, "rows_json": json.dumps(rows)}

    driver = MixedStorageDriver([_binding_row(), _binding_row(plan_release_id=legacy_id)])
    if failed_storage == "legacy":
        metadata_report = await append(driver)
        assert metadata_report["targets"] == REPORT["targets"]
        metadata = metadata_report["provenance"]["pricing_binding_metadata"]["release_metadata"]
        assert metadata == {PLAN_RELEASE_ID: None, legacy_id: None}
        assert queried_storage_names == ["source", "legacy"]
    else:
        with pytest.raises(ValueError, match="^plan_release_source_binding_storage_missing$") as caught:
            await append(driver)
        assert caught.value.__cause__ is failure
        assert queried_storage_names == ["source"]
    custody.assert_awaited_once()
    assert driver.lifecycle == ["start", "rollback"] and driver.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,reason",
    [
        ("not_text", "metadata_invalid"),
        ("over_budget", "metadata_invalid"),
        ("not_list", "metadata_invalid"),
        ("unexpected_release", "identity_invalid"),
        ("wrong_revision", "identity_invalid"),
        ("wrong_digest", "identity_invalid"),
        ("missing_rows", "incomplete"),
    ],
)
async def test_malformed_canonical_metadata_rolls_back_without_optional_recovery(
    retained_export, monkeypatch, change, reason
):
    import json

    from tests.test_plan_release_source_bindings import header

    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    custody = AsyncMock()
    monkeypatch.setattr(lookup, "require_source_binding_custody_fetch", custody)
    binding_rows = [_binding_row()]
    if change == "unexpected_release":
        binding_rows[0]["plan_release_id"] = "hprelease_" + "9" * 26
    elif change == "wrong_revision":
        binding_rows[0]["serving_revision_id"] = "hpserve_" + "9" * 26
    elif change == "wrong_digest":
        binding_rows[0]["binding_set_digest"] = "9" * 64
    elif change == "missing_rows":
        binding_rows = []
    encoded = json.dumps(binding_rows)
    if change == "not_text":
        encoded = None
    elif change == "over_budget":
        encoded = " " * (16 * 1024 * 1024 + 1)
    elif change == "not_list":
        encoded = "{}"

    class MalformedDriver(Driver):
        async def fetch(self, sql, *parameters):
            if "AS storage_present" in sql:
                self.calls.append((sql, parameters))
                return [header()]
            return await super().fetch(sql, *parameters)

        async def fetchrow(self, sql, *parameters):
            self.calls.append((sql, parameters))
            return {"bounded": True, "rows_json": encoded}

    driver = MalformedDriver()
    with pytest.raises(ValueError, match=reason):
        await append(driver)
    custody.assert_awaited_once()
    assert driver.lifecycle == ["start", "rollback"] and driver.is_in_transaction()


@pytest.mark.asyncio
async def test_combined_storage_budget_does_not_return_a_partial_binding_set(retained_export, monkeypatch):
    import json

    from tests.test_plan_release_source_bindings import header

    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    legacy_id = "hprelease_" + "4" * 26
    binding_rows = [_binding_row(), _binding_row(plan_release_id=legacy_id)]
    maximum = max(len(json.dumps([binding_row]).encode()) for binding_row in binding_rows) + 1
    custody = AsyncMock()
    monkeypatch.setattr(lookup, "require_source_binding_custody_fetch", custody)
    driver = Driver(binding_rows)
    phases = []

    async def fetchrow(sql, identities, owner_type, limit):
        phases.append(identities)
        encoded = json.dumps(
            [binding_row for binding_row in binding_rows if binding_row["plan_release_id"] in identities]
        )
        assert len(encoded.encode()) <= limit == maximum
        return {"bounded": True, "rows_json": encoded}

    driver.fetchrow = fetchrow
    assert (
        await lookup._fetch_admitted_binding_rows(
            driver,
            [PLAN_RELEASE_ID, legacy_id],
            maximum,
            lookup.storage_admissions([header(), header(plan_release_id=legacy_id, storage_present=False)]),
            retained=True,
            has_projection=False,
        )
        is None
    )
    assert phases == [[PLAN_RELEASE_ID], [legacy_id]]
    custody.assert_awaited_once()
