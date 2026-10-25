# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure DTO and synthetic batch-boundary checks; PostgreSQL proof is separate."""

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_network_catalog_read as catalog
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_serving_read import PinnedNetworkServingManifest


def network_document(network_id=7):
    return {
        "network_id": network_id,
        "display_name": "Sample Network",
        "aliases": ["Sample Alias"],
        "archived": False,
        "revision": 3,
        "directory_available": True,
        "priceable": None,
        "benefit_codes": None,
        "companies": [],
        "source_bindings": [],
    }


class BatchConnection:
    def __init__(self):
        self.calls = []
        self.in_transaction = True
        self.isolation = "repeatable read"
        self.read_only = "on"
        self.report = {
            "membership_rows": 2,
            "distinct_memberships": 2,
            "projected_locations": 1,
            "orphan_bindings": 0,
            "candidate_readiness": {"address_rows": 1},
        }
        self.parity = {"address_rows": 1, "changed_arrays": 0}
        self.page = {"total": 1, "bounded": True, "rows_json": json.dumps([network_document()])}
        self.autocomplete = {"plan_count": 1, "bounded": True, "plans_json": json.dumps([autocomplete_document()])}
        self.legacy_batch = {"complete": True, "network_ids": [7]}
        self.legacy = {"ambiguous": False, "permitted_count": 1, "network_id": 7}

    def is_in_transaction(self):
        return self.in_transaction

    async def fetchrow(self, sql, *parameters):
        self.calls.append((sql, parameters))
        if "current_setting" in sql:
            return {"isolation": self.isolation, "read_only": self.read_only}
        if "validation_json::text" in sql:
            return {"validation_json": json.dumps(self.report)}
        if "AS changed_arrays" in sql:
            return self.parity
        if "AS plan_count" in sql:
            return self.autocomplete
        if "AS complete" in sql:
            return self.legacy_batch
        if "AS permitted_count" in sql:
            return self.legacy
        assert "registry_approved_record" in sql
        return self.page


@pytest.fixture
def batch_boundary(monkeypatch):
    connection = BatchConnection()
    manifest = PinnedNetworkServingManifest(
        9,
        "11111111-1111-4111-8111-111111111111",
        "candidate_sample",
        1,
        {"custom_membership": "a" * 64},
        4,
        "b" * 64,
        123,
    )
    pinned_revisions = []

    async def resolve(_connection, *, generation_id, control_schema):
        assert _connection is connection and control_schema == "sample_control"
        assert generation_id in (None, 9)
        return manifest

    async def retained(_connection, *, approved_revision, control_schema):
        assert _connection is connection and control_schema == "sample_control"
        pinned_revisions.append(approved_revision)
        return SimpleNamespace(generation_id="a" * 64)

    async def accounting(_connection, *relations):
        assert _connection is connection
        assert relations == (
            '"candidate_sample".network_membership',
            '"candidate_sample".provider_location_binding',
            '"candidate_sample".entity_address_unified',
        )
        return {"membership_rows": 2, "distinct_memberships": 2, "projected_locations": 1, "orphan_bindings": 0}

    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(catalog, "pin_retained_approved_membership_source", retained)
    monkeypatch.setattr(catalog, "_membership_accounting", accounting)
    return connection, manifest, pinned_revisions


def source_coordinates(system="aca"):
    return RegistryNetworkSourceCoordinates(system, "source", "sample_source", "dataset", "producer", "edition")


def legacy_selector():
    scope_by_field = {
        "issuer_id": "12345",
        "plan_id": "12345CA1234567",
        "plan_year": 2026,
        "state": "CA",
        "checksum_network": -17,
    }
    return catalog.RegistryNetworkCatalogLegacySelector(
        source_coordinates(),
        "checksum_network",
        "-17",
        json.dumps(scope_by_field).encode(),
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": True},
        {"offset": -1},
        {"offset": 1000001},
        {"generation_id": 0},
        {"generation_id": "9"},
        {"archived": 0},
        {"search": " "},
        {"search": "\ud800"},
        {"source": {}},
    ],
)
def test_closed_query_rejects_invalid_inputs(changes):
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.RegistryNetworkCatalogQuery(**changes)


@pytest.mark.parametrize("network_ids", [[1], (0,), (True,), (2, 1), (1, 1), (2147483648,), tuple(range(1, 50002))])
def test_trusted_exclusions_require_bounded_sorted_unique_int4(network_ids):
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._exclusions(network_ids)


def test_maximum_request_boundaries_and_closed_legacy_namespaces():
    catalog.RegistryNetworkCatalogQuery(limit=100, offset=1000000, generation_id=9223372036854775807)
    assert catalog._exclusions(tuple(range(1, 50001)))[-1] == 50000
    selector = legacy_selector()
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.RegistryNetworkCatalogLegacySelector(selector.source, "network_id", "7", selector.source_scope_json)
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.RegistryNetworkCatalogLegacySelector(
            selector.source, "checksum_network", "-0", selector.source_scope_json
        )


@pytest.mark.parametrize(
    "encoded", [b'{"a":1,"a":2}', b'{"a":{"x":1,"x":2}}', b'{"a":NaN}', b"\xff", bytearray(b"{}"), b" " * 16385]
)
def test_metadata_preflight_duplicate_and_non_json_constant_refusal(encoded):
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._closed_json(encoded, 16384)


@pytest.mark.asyncio
async def test_catalog_pins_old_generation_retained_approval_and_exact_source(batch_boundary):
    connection, _manifest, pinned_revisions = batch_boundary
    query = catalog.RegistryNetworkCatalogQuery(generation_id=9, source=source_coordinates(), search="Alias")
    page = await catalog.read_registry_network_catalog(
        connection, query, excluded_network_ids=(2, 5), control_schema="sample_control"
    )
    assert page["generation"] == "9" and page["approved_custom_revision"] == "4"
    assert pinned_revisions == [4]  # The current approval head is deliberately absent.
    assert page["items"][0]["priceable"] is None and page["items"][0]["benefit_codes"] is None
    sql, parameters = connection.calls[-1]
    assert parameters[:5] == (4, (2, 5), False, "Alias", source_coordinates().sql_parameters)
    assert "registry_revision_control" not in sql and "network_registry_alias" not in sql
    assert sql.index("NOT record_key::integer=ANY") < sql.index("filtered AS") < sql.index("OFFSET $7")
    assert "location.entity_type,location.entity_id" in sql
    assert connection.in_transaction is True


@pytest.mark.asyncio
@pytest.mark.parametrize("total,offset", [(0, 0), (6, 10)])
async def test_empty_and_beyond_end_pages_keep_authorized_total(batch_boundary, total, offset):
    connection, _, _ = batch_boundary
    connection.page.update(total=total, rows_json="[]")
    page = await catalog.read_registry_network_catalog(
        connection,
        catalog.RegistryNetworkCatalogQuery(offset=offset),
        excluded_network_ids=(),
        control_schema="sample_control",
    )
    assert page["total"] == total and page["items"] == ()


@pytest.mark.asyncio
async def test_excluded_detail_refuses_before_any_database_access(batch_boundary):
    connection, _, _ = batch_boundary
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="detail_denied"):
        await catalog.read_registry_network_catalog_detail(
            connection, 7, excluded_network_ids=(7,), control_schema="sample_control"
        )
    assert connection.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transaction,isolation,read_only",
    [(False, "repeatable read", "on"), (True, "read committed", "on"), (True, "repeatable read", "off")],
)
async def test_caller_owned_pinned_read_only_transaction_required(batch_boundary, transaction, isolation, read_only):
    connection, _, pinned_revisions = batch_boundary
    connection.in_transaction, connection.isolation, connection.read_only = transaction, isolation, read_only
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="transaction_required"):
        await catalog.read_registry_network_catalog(
            connection, catalog.RegistryNetworkCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
        )
    assert pinned_revisions == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["membership", "arrays", "address", "source"])
async def test_directory_requires_exact_complete_accounting(batch_boundary, failure):
    connection, manifest, _ = batch_boundary
    if failure == "membership":
        connection.report["membership_rows"] = 1
    elif failure == "arrays":
        connection.parity["changed_arrays"] = 1
    elif failure == "address":
        connection.parity["address_rows"] = 2
    else:
        manifest.source_generations["custom_membership"] = "c" * 64
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="directory_unavailable|source_changed"):
        await catalog.read_registry_network_catalog(
            connection, catalog.RegistryNetworkCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unknown", "priceable", "duplicate", "excluded", "short", "oversized", "bound"])
async def test_returned_closed_metadata_and_aggregate_bounds(batch_boundary, failure):
    connection, _, _ = batch_boundary
    document = network_document()
    changes_by_failure = {
        "unknown": {"extra": 1},
        "priceable": {"priceable": True},
        "duplicate": {"aliases": ["Sample Alias", "Sample Alias"]},
        "excluded": {"network_id": 2},
        "oversized": {"display_name": "a" * catalog.MAX_RESPONSE_BYTES},
    }
    document.update(changes_by_failure.get(failure, {}))
    connection.page["total"] = 2 if failure == "short" else 1
    connection.page["bounded"] = failure != "bound"
    connection.page["rows_json"] = json.dumps([document])
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await catalog.read_registry_network_catalog(
            connection,
            catalog.RegistryNetworkCatalogQuery(),
            excluded_network_ids=(2,),
            control_schema="sample_control",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous,permitted_count", [(True, 1), (True, 2), (False, 0)])
async def test_legacy_hidden_missing_and_global_ambiguity_share_refusal(batch_boundary, ambiguous, permitted_count):
    connection, _, _ = batch_boundary
    connection.legacy.update(ambiguous=ambiguous, permitted_count=permitted_count)
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="detail_denied"):
        await catalog.resolve_registry_network_catalog_legacy(
            connection, legacy_selector(), excluded_network_ids=(2,), control_schema="sample_control"
        )
    assert "AS permitted_count" in connection.calls[-1][0]


@pytest.mark.asyncio
async def test_legacy_exact_approved_scope_then_authorized_detail(batch_boundary):
    connection, _, _ = batch_boundary
    selector = legacy_selector()
    page = await catalog.resolve_registry_network_catalog_legacy(
        connection, selector, excluded_network_ids=(2,), generation_id=9, control_schema="sample_control"
    )
    assert page["items"][0]["network_id"] == 7
    sql, parameters = connection.calls[-2]
    assert "source_scope_json'=$3::jsonb" in sql and "network_registry_alias" not in sql
    assert parameters == (4, selector.source.sql_parameters, selector.source_scope_json.decode(), (2,))


def test_nested_company_and_binding_metadata_is_closed():
    document = network_document()
    company_by_field = {
        "company_id": "22222222-2222-4222-8222-222222222222",
        "display_name": "Sample Company",
        "roles": ["insurer"],
        "revision": 2,
        "link_revision": 3,
        "group_id": None,
    }
    binding_by_field = {
        "namespace": "source_binding",
        "binding_id": "33333333-3333-4333-8333-333333333333",
        "revision": 1,
        **dict(
            zip(
                ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"),
                source_coordinates("ptg").sql_parameters,
                strict=True,
            )
        ),
        "source_key": "cohort",
        "source_scope": {"cohort_id": "cohort", "company_key": "company", "snapshot_id": "snapshot"},
        "evidence_id": "evidence",
        "evidence_sha256": "d" * 64,
    }
    document.update(companies=[company_by_field], source_bindings=[binding_by_field])
    assert catalog._network_document(document) == document
    changed = copy.deepcopy(document)
    changed["source_bindings"][0]["source_scope"]["extra"] = "value"
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._network_document(changed)


@pytest.mark.asyncio
async def test_approved_multibyte_names_aliases_and_search_use_character_limits(batch_boundary):
    connection, _, pinned_revisions = batch_boundary
    document = network_document()
    document.update(display_name="é" * 512, aliases=["漢" * 512])
    document["companies"] = [
        {
            "company_id": "22222222-2222-4222-8222-222222222222",
            "display_name": "界" * 512,
            "roles": ["insurer"],
            "revision": 2,
            "link_revision": 3,
            "group_id": None,
        }
    ]
    connection.page["rows_json"] = json.dumps([document], ensure_ascii=False)
    page = await catalog.read_registry_network_catalog(
        connection,
        catalog.RegistryNetworkCatalogQuery(search="é" * 512),
        excluded_network_ids=(),
        control_schema="sample_control",
    )
    assert page["items"] == (document,) and pinned_revisions == [4]
    assert connection.calls[-1][1][3] == "é" * 512
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.RegistryNetworkCatalogQuery(search="é" * 513)
    document["aliases"] = ["漢" * 513]
    connection.page["rows_json"] = json.dumps([document], ensure_ascii=False)
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await catalog.read_registry_network_catalog(
            connection, catalog.RegistryNetworkCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
        )


@pytest.mark.asyncio
async def test_multibyte_approved_page_still_enforces_total_utf8_bytes(batch_boundary):
    connection, _, _ = batch_boundary
    document = network_document()
    document["aliases"] = ["漢" * 508 + f"{ordinal:04d}" for ordinal in range(100)]
    documents = [document | {"network_id": ordinal + 1} for ordinal in range(7)]
    encoded = json.dumps(documents, ensure_ascii=False)
    assert len(encoded) < catalog.MAX_RESPONSE_BYTES < len(encoded.encode("utf-8"))
    connection.page.update(total=7, rows_json=encoded)
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="document_invalid"):
        await catalog.read_registry_network_catalog(
            connection, catalog.RegistryNetworkCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
        )


@pytest.mark.parametrize("codes", [None, ["medical"], ["dental"], ["vision"], ["medical", "dental", "vision"]])
def test_declared_benefit_codes_preserve_canonical_vocabulary_without_pricing(codes):
    document = network_document()
    document["benefit_codes"] = codes
    assert catalog._network_document(document)["benefit_codes"] == codes
    assert document["priceable"] is None


@pytest.mark.parametrize(
    "codes", [[], ["dental", "medical"], ["medical", "medical"], ["pharmacy"], True, "dental", [None]]
)
def test_declared_benefit_codes_refuse_noncanonical_or_inferred_domains(codes):
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._network_document({**network_document(), "benefit_codes": codes})


@pytest.mark.asyncio
async def test_declared_benefit_filter_is_pinned_authorized_and_before_count_and_page(batch_boundary):
    connection, _, _ = batch_boundary
    connection.page["rows_json"] = json.dumps([{**network_document(), "benefit_codes": ["dental"]}])
    page = await catalog.read_registry_network_catalog(
        connection,
        catalog.RegistryNetworkCatalogQuery(generation_id=9, benefit_code="dental"),
        excluded_network_ids=(2, 5),
        control_schema="sample_control",
    )
    assert page["items"][0]["benefit_codes"] == ["dental"] and page["total"] == 1
    sql, parameters = connection.calls[-1]
    assert parameters[0] == 4 and parameters[1] == (2, 5) and parameters[-1] == "dental"
    assert sql.index("NOT record_key::integer=ANY") < sql.index("declared_assertions AS") < sql.index("filtered AS")
    assert sql.index("$10=ANY") < sql.index("OFFSET $7") < sql.index("count(*) FROM filtered")
    assert "assertion->>'company_id'=links.record_key" in sql
    assert "assertion->>'network_id'=network.network_id::text" in sql
    assert "current_date" not in sql.lower() and "now()" not in sql.lower()
    connection.page["rows_json"] = json.dumps([network_document()])
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await catalog.read_registry_network_catalog(
            connection,
            catalog.RegistryNetworkCatalogQuery(benefit_code="dental"),
            excluded_network_ids=(),
            control_schema="sample_control",
        )


@pytest.mark.parametrize(
    "values",
    [
        (),
        ("42", "42"),
        ("-0",),
        ("+1",),
        ("01",),
        ("2147483648",),
        ("-2147483649",),
        (1,),
        tuple(str(n) for n in range(51)),
    ],
)
def test_legacy_batch_rejects_noncanonical_or_unbounded_values(values):
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", values)


def test_legacy_batch_preserves_signed_namespace_and_uuid_types():
    values = ("-2147483648", "0", "2147483647")
    assert catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", values).values == values
    identifier = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert catalog.RegistryNetworkCatalogLegacyBatchSelector("legacy_fhir_uuid", (identifier,)).values == (identifier,)
    for namespace, values in [
        ("network_id", ("42",)),
        ("legacy_fhir_uuid", (identifier.upper(),)),
        ("legacy_fhir_uuid", ("00000000-0000-0000-0000-000000000000",)),
    ]:
        with pytest.raises(catalog.RegistryNetworkCatalogError):
            catalog.RegistryNetworkCatalogLegacyBatchSelector(namespace, values)


@pytest.mark.asyncio
async def test_legacy_batch_one_reverse_query_deduplicates_before_one_page(batch_boundary):
    connection, _, revisions = batch_boundary
    selector = catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", ("42", "0"))
    page = await catalog.resolve_registry_network_catalog_legacy(
        connection, selector, excluded_network_ids=(2,), generation_id=9, control_schema="sample_control"
    )
    assert page["total"] == page["limit"] == 1 and page["items"][0]["network_id"] == 7
    sql, parameters = connection.calls[-2]
    assert parameters == (4, ("42", "0"), "checksum_network", (2,))
    assert sql.index("count(DISTINCT matches.network_id)") < sql.index("NOT network_id=ANY")
    assert connection.calls[-1][1][5] == (7,) and revisions == [4]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unmapped", "partial_page", "hidden", "oversized"])
async def test_legacy_batch_never_returns_partial_targets(batch_boundary, damage):
    connection, _, _ = batch_boundary
    if damage == "unmapped":
        connection.legacy_batch["complete"] = False
    elif damage == "partial_page":
        connection.legacy_batch["network_ids"] = [7, 8]
    elif damage == "hidden":
        connection.page["rows_json"] = json.dumps([network_document(2)])
    else:
        connection.page["bounded"] = False
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await catalog.resolve_registry_network_catalog_legacy(
            connection,
            catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", ("42", "0")),
            excluded_network_ids=(2,),
            control_schema="sample_control",
        )


def autocomplete_document():
    return {
        "plan_id": "12345CA1234567",
        "marketing_name": "Sample é",
        "state": "CA",
        "year": 2026,
        "network_checksum": {"-17": None, "0": ""},
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"query": ""},
        {"query": " x"},
        {"query": "x "},
        {"query": "x" * 129},
        {"query": "x\u0085y"},
        {"query": "x\ud800"},
        {"query": 1},
        {"limit": True},
        {"limit": 0},
        {"limit": 51},
        {"zip_code": "１２３４５"},
        {"zip_code": "1234"},
        {"zip_code": 12345},
        {"generation_id": True},
        {"generation_id": 0},
        {"generation_id": "9"},
        {"generation_id": 9223372036854775808},
    ],
)
def test_autocomplete_closed_display_query(changes):
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.RegistryNetworkAutocompleteQuery(**({"query": "Sample"} | changes))


def test_autocomplete_query_retains_unicode_wildcards_and_maximums():
    query = catalog.RegistryNetworkAutocompleteQuery("É_%", 50, "00000", 9223372036854775807)
    assert query.query == "É_%" and query.limit == 50


@pytest.mark.asyncio
async def test_autocomplete_uses_one_pin_and_global_mapping_before_visibility(batch_boundary):
    connection, _, pinned = batch_boundary
    result = await catalog.read_registry_network_autocomplete(
        connection,
        catalog.RegistryNetworkAutocompleteQuery("Sample", generation_id=9),
        excluded_network_ids=(2,),
        control_schema="sample_control",
    )
    assert result == {"generation": "9", "approved_custom_revision": "4", "plans": [autocomplete_document()]}
    assert pinned == [4]
    sql, parameters = connection.calls[-1]
    assert parameters == (4, (2,), "%sample%", 10, None, catalog.MAX_RESPONSE_BYTES, False)
    assert sql.index("HAVING count(DISTINCT network_id)=1") < sql.index("NOT unique_checksum.network_id=ANY")
    assert sql.index("NOT unique_checksum.network_id=ANY") < sql.index("LIMIT $4")
    assert "tier.year=plan.year" in sql and "tier.issuer_id=plan.issuer_id" in sql
    assert "plan.state" in sql and "plan_year" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"bounded": False},
        {"plan_count": 2},
        {"plans_json": "{}"},
        {"plans_json": json.dumps([autocomplete_document() | {"unexpected": 1}])},
        {"plans_json": json.dumps([autocomplete_document() | {"network_checksum": {"42": "ok", "-0": None}}])},
        {"plans_json": json.dumps([autocomplete_document() | {"year": "2026"}])},
        {"plans_json": json.dumps([autocomplete_document() | {"network_checksum": {"2147483648": "bad"}}])},
    ],
)
async def test_autocomplete_refuses_unbounded_or_unclosed_result(batch_boundary, change):
    connection, _, _ = batch_boundary
    connection.autocomplete.update(change)
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await catalog.read_registry_network_autocomplete(
            connection,
            catalog.RegistryNetworkAutocompleteQuery("Sample"),
            excluded_network_ids=(),
            control_schema="sample_control",
        )


def test_autocomplete_accepts_valid_aca_plan_variant_without_truncating_labels():
    document = autocomplete_document() | {"plan_id": "12345CA1234567-01", "marketing_name": "é" * 2000}
    assert catalog._autocomplete_document(document) == document


def _sealed_manifest(manifest, marker):
    sealed = replace(manifest)
    object.__setattr__(sealed, "_custody_seal_sha256", marker)
    return sealed


@pytest.mark.asyncio
async def test_verified_seal_keeps_approved_map_pin_without_fact_rescan(batch_boundary, monkeypatch):
    connection, manifest, revisions = batch_boundary
    manifest = _sealed_manifest(manifest, "c" * 64)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    accounting = AsyncMock(side_effect=AssertionError("sealed facts must not be rescanned"))
    monkeypatch.setattr(catalog, "_membership_accounting", accounting)
    assert await catalog._pin(connection, 9, "sample_control") is manifest
    assert revisions == [4]
    accounting.assert_not_awaited()
    assert len(connection.calls) == 1 and "current_setting" in connection.calls[0][0]


@pytest.mark.asyncio
async def test_sealed_catalog_still_refuses_changed_approved_source(batch_boundary, monkeypatch):
    connection, manifest, _ = batch_boundary
    manifest = _sealed_manifest(manifest, "c" * 64)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    monkeypatch.setattr(
        catalog,
        "pin_retained_approved_membership_source",
        AsyncMock(return_value=SimpleNamespace(generation_id="changed")),
    )
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await catalog._pin(connection, 9, "sample_control")
    assert len(connection.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", ["", "c" * 63, "C" * 64, True])
async def test_malformed_seal_marker_is_terminal_not_a_legacy_scan(batch_boundary, monkeypatch, marker):
    connection, manifest, _ = batch_boundary
    monkeypatch.setattr(
        catalog,
        "resolve_network_serving_manifest",
        AsyncMock(return_value=_sealed_manifest(manifest, marker)),
    )
    accounting = AsyncMock()
    monkeypatch.setattr(catalog, "_membership_accounting", accounting)
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        await catalog._pin(connection, 9, "sample_control")
    accounting.assert_not_awaited()


def test_all_four_company_roles_roundtrip_without_requiring_a_group():
    company_by_field = {
        "company_id": "11111111-1111-4111-8111-111111111111",
        "display_name": "Example company",
        "roles": ["insurer", "employer", "network_operator", "benefits_administrator"],
        "revision": 1,
        "link_revision": 1,
        "group_id": None,
    }
    assert catalog._company_document(company_by_field) is None
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._company_document(company_by_field | {"roles": [*company_by_field["roles"], "benefits_administrator"]})
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._company_document(company_by_field | {"roles": ["unknown"]})
