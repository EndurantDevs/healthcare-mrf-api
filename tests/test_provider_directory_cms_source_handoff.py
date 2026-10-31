# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS intake remains complete and private while protected finalization is pending."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_source_handoff as cms_handoff
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_completion import _prepared_detached
from tests.test_provider_directory_generic_source_storage import _isolated_harness, harness


def _identity():
    release_by_field = dict(
        source_id="cms-npd",
        generated_at="2026-01-01",
        manifest_sha256="c" * 64,
        vector_sha256="d" * 64,
        files={
            name: dict(sha256="e" * 64, compressed_bytes=100, original_bytes=200, row_count=3, distinct_count=2)
            for name, _ in cms_handoff.cms_npd_source.RESOURCE_FILES
        },
    )
    resources = sorted(kind for _, kind in cms_handoff.cms_npd_source.RESOURCE_FILES)
    identity_by_field = dict(dataset_id="dataset", endpoint_id="endpoint", acquisition_root_run_id="root")
    identity_by_field["metadata"] = dict(
        source_ids=["cms-npd"],
        selected_resources=resources,
        expected_resources=resources,
        resource_hash_contract=cms_handoff.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=release_by_field["generated_at"],
        source_release=release_by_field,
        network_bindings=cms_handoff.fhir_network_binding_metadata(
            source_id="cms-npd",
            dataset_schema="catalog_schema",
            dataset_id="dataset",
            producer_id="cms-npd-bulk-fhir-v1",
            alias_scope="cms-npd",
        ),
    )
    return identity_by_field


@pytest.mark.asyncio
async def test_protocol_selection_works_before_original_transaction_without_granting_io(harness, monkeypatch):
    from process import provider_directory_generic_source_policy as policy
    from tests.test_provider_directory_generic_source_policy import _isolated_factory

    assert cms._is_isolated_intake() is False
    factory = _isolated_factory()
    async with policy.bind_generic_source_transaction_factory(factory):
        assert cms._is_isolated_intake() is True
        factory._require_current_policy.assert_called_once_with()
        with pytest.raises(GenericCandidateError, match="isolated_transaction_required"):
            policy.current_source_storage_binding()
        factory.producer_identity = ("c" * 40, factory.producer_identity[1])
        with pytest.raises(ValueError, match="source_policy_changed"):
            cms._is_isolated_intake()
    async with _isolated_harness(harness, monkeypatch) as publisher:
        assert cms._is_isolated_intake() is True
        assert policy.current_source_storage_binding() is publisher.storage_binding


@pytest.mark.parametrize(
    "changed",
    [
        "scope",
        "selected",
        "expected",
        "files",
        "source",
        "producer",
        "schema",
        "hash",
        "date",
        "counts",
        "bool",
        "resource_hash",
        "projection_date",
        "file_hash",
    ],
)
def test_cms_requires_the_complete_closed_release_and_exact_producer(changed):
    identity = _identity()
    assert (
        cms_handoff.require_cms_handoff_identity(identity, "catalog_schema") == identity["metadata"]["source_release"]
    )
    metadata = identity["metadata"]
    release = metadata["source_release"]
    first_file = next(iter(release["files"].values()))
    match changed:
        case "scope":
            metadata["source_ids"] = ["example-source"]
        case "selected":
            metadata["selected_resources"].pop()
        case "expected":
            metadata["expected_resources"].pop()
        case "files":
            release["files"].pop(next(iter(release["files"])))
        case "source":
            release["source_id"] = "example-source"
        case "producer":
            metadata["network_bindings"]["producer_id"] = "ordinary-rest"
        case "schema":
            metadata["network_bindings"]["dataset_schema"] = "source_schema"
        case "hash":
            release["vector_sha256"] = "Z" * 64
        case "date":
            release["generated_at"] = metadata["semantic_projection_as_of"] = "2026-02-30"
        case "counts":
            first_file["distinct_count"] = 4
        case "bool":
            first_file["row_count"] = True
        case "resource_hash":
            metadata["resource_hash_contract"] = "legacy"
        case "projection_date":
            metadata["semantic_projection_as_of"] = "2026-01-02"
        case _:
            first_file["sha256"] = "f" * 63
    with pytest.raises(GenericCandidateError, match="isolated_cms_"):
        cms_handoff.require_cms_handoff_identity(identity, "catalog_schema")


async def _cms_detached(harness, monkeypatch):
    """Use the actual detached builder, retaining native IO assertions in its harness."""
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        metadata = binding["identity"]["metadata"]
        metadata.update(_identity()["metadata"])
        binding["contract"] = storage.CMS_ISOLATED_CONTRACT
        harness.header["resource_count"] = 16
        original_fetch = harness.connection.fetch.side_effect

        def fetch(sql, *args):
            if sql.startswith("SELECT resource_type,count(*)"):
                return [dict(resource_type=kind, count=2) for _, kind in cms_handoff.cms_npd_source.RESOURCE_FILES]
            return original_fetch(sql, *args)

        harness.connection.fetch.side_effect = fetch
        handoff = await completion.prepare_generic_source_handoff(publisher, binding)
        return handoff


@pytest.mark.asyncio
async def test_cms_prepare_and_reduce_never_allocate_canonical_auxiliary_stages(harness, monkeypatch):
    from process import provider_directory_generic_source_auxiliary as auxiliary

    async with _isolated_harness(harness, monkeypatch) as publisher:
        harness.header["candidate_storage_json"]["identity"]["metadata"].update(_identity()["metadata"])
        harness.header["candidate_storage_json"] = None
        harness.connection.fetch = AsyncMock(return_value=[])
        harness.connection.fetchval = AsyncMock(
            side_effect=lambda query, *args: "private_ordinal_sequence" if "pg_get_serial_sequence" in query else False
        )
        proof_by_name = {}

        async def capture(connection, policy, names, **options):
            assert connection is harness.connection and policy is harness.policy
            for name in names:
                proof_by_name.setdefault(
                    name, dict(name=name, oid=1000 + len(proof_by_name), owner=100, layout_sha256="e" * 64)
                )
            return {name: proof_by_name[name] for name in names}

        monkeypatch.setattr(storage, "_capture", capture)
        monkeypatch.setattr(completion, "source_owner_transaction", storage.source_owner_transaction)
        reduction = AsyncMock()
        mint = AsyncMock(side_effect=AssertionError("source attempted canonical mint"))
        monkeypatch.setattr(completion, "_complete_payloads", reduction)
        monkeypatch.setattr(auxiliary, "prepare_identity_candidate", mint)
        loading = await storage.prepare_generic_source_candidate(object(), harness.directory, harness.candidate)
        assert loading["contract"] == storage.CMS_ISOLATED_CONTRACT
        assert loading["storage"] == storage.storage_document(publisher)
        prepared = await completion.complete_generic_source_candidate(object(), harness.directory, harness.candidate)
        assert prepared["phase"] == "prepared" and prepared["auxiliary"] is None
        assert prepared["relationship_sha256"] is None
        reduction.assert_awaited_once()
        mint.assert_not_awaited()
    assert sum(sql.startswith("CREATE TABLE") for sql in harness.sql) == 2 + len(storage.RELATIONS)
    assert not any("ATTACH" in sql or '"catalog_schema".' in sql or "GRANT SELECT" in sql for sql in harness.sql)


@pytest.mark.asyncio
async def test_cms_handoff_retains_raw_inputs_without_identity_or_serving_claim(harness, monkeypatch):
    handoff = await _cms_detached(harness, monkeypatch)
    document = handoff.document
    assert document["contract"] == storage.CMS_ISOLATED_CONTRACT
    assert document["phase"] == "ready_for_freeze"
    assert document["auxiliary"] is None and document["relationship_sha256"] is None
    assert document["admission"]["resource_count"] == 16
    assert set(document["tables"]) == set(storage.RELATIONS + storage.HANDOFF_CONTROL_ROOTS)
    storage.validate_handoff_document(document, document["storage"])
    assert not any("ATTACH" in sql or '"catalog_schema".' in sql or "GRANT SELECT" in sql for sql in harness.sql)
    connection = SimpleNamespace(is_in_transaction=lambda: pytest.fail("ordinary publisher inspected CMS IO"))
    with pytest.raises(GenericCandidateError, match="ordinary_handoff_required"):
        await completion.validate_frozen_source_handoff(connection, object(), handoff, protected_owner_role="owner")
    rest_by_field = {**document, "contract": storage.ISOLATED_CONTRACT}
    with pytest.raises(GenericCandidateError, match="isolated_kind_unsupported"):
        storage.validate_handoff_document(rest_by_field, document["storage"])
    rest_by_field["identity"]["metadata"].pop("source_release")
    with pytest.raises(GenericCandidateError, match="isolated_kind_unsupported"):
        storage.validate_handoff_document(rest_by_field, document["storage"])


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["counts", "foreign_dataset", "graph", "outside_transaction"])
async def test_cms_raw_handoff_rejects_missing_family_or_premature_graph(harness, monkeypatch, changed):
    handoff = await _cms_detached(harness, monkeypatch)
    rows = [dict(resource_type=kind, count=2) for _, kind in cms_handoff.cms_npd_source.RESOURCE_FILES]
    if changed == "counts":
        rows.pop()
    connection = SimpleNamespace(
        is_in_transaction=lambda: changed != "outside_transaction",
        fetch=AsyncMock(return_value=rows),
        fetchval=AsyncMock(
            side_effect=lambda query, *args: (
                changed == "foreign_dataset" if "dataset_id<>" in query else changed == "graph"
            )
        ),
    )
    with pytest.raises(GenericCandidateError):
        await cms_handoff.validate_cms_handoff_rows(
            connection, SimpleNamespace(schema="source_schema"), handoff.document
        )


@pytest.mark.asyncio
async def test_cms_frozen_validation_keeps_raw_content_and_protected_custody_checks(harness, monkeypatch):
    handoff = await _cms_detached(harness, monkeypatch)
    frozen = AsyncMock(return_value=dict(dataset_hash="a" * 64, resource_count=16))
    monkeypatch.setattr(completion, "_validate_frozen_source_handoff", frozen)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(
            return_value=[dict(resource_type=kind, count=2) for _, kind in cms_handoff.cms_npd_source.RESOURCE_FILES]
        ),
        fetchval=AsyncMock(return_value=False),
    )
    directory = object()
    result = await completion.validate_frozen_cms_source_handoff(
        connection,
        directory,
        handoff,
        protected_owner_role="protected_owner",
    )
    frozen.assert_awaited_once_with(connection, directory, handoff, protected_owner_role="protected_owner")
    assert result["resource_count"] == 16 and result["source_release"] == _identity()["metadata"]["source_release"]


@pytest.mark.asyncio
async def test_isolated_cms_staging_defers_global_evidence_but_keeps_content_checks(monkeypatch):
    monkeypatch.setattr(cms, "_is_isolated_intake", lambda: True)
    counts_by_family = {kind: 2 for _, kind in cms_handoff.cms_npd_source.RESOURCE_FILES}
    methods_by_name = {}
    for name in (
        "_assert_counts",
        "_verify_or_dispose",
        "_assert_witness_counts",
        "_materialize_identity_evidence",
        "_validate_candidate",
    ):
        methods_by_name[name] = AsyncMock(
            return_value=counts_by_family if name == "_assert_counts" else {"validated": True}
        )
        monkeypatch.setattr(cms, name, methods_by_name[name])
    graph = AsyncMock()
    monkeypatch.setattr(cms.relationships, "materialize", graph)
    candidate, fhir = object(), object()
    result = await cms._validate_staged_source_snapshot(
        fhir, object(), candidate, _identity()["metadata"]["source_release"], {}, {}, {}
    )
    assert result == {"validated": True}
    methods_by_name["_assert_counts"].assert_awaited_once()
    methods_by_name["_assert_witness_counts"].assert_awaited_once()
    assert methods_by_name["_verify_or_dispose"].await_count == 2
    methods_by_name["_validate_candidate"].assert_awaited_once()
    methods_by_name["_materialize_identity_evidence"].assert_not_awaited()
    graph.assert_not_awaited()


@pytest.mark.asyncio
async def test_isolated_cms_result_carries_handoff_and_never_calls_legacy_publication(harness, monkeypatch):
    handoff = await _cms_detached(harness, monkeypatch)
    monkeypatch.setattr(cms, "_is_isolated_intake", lambda: True)
    candidate = SimpleNamespace(dataset_id="dataset", already_validated=False)
    monkeypatch.setattr(cms, "_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(cms, "_verify_release", AsyncMock())
    monkeypatch.setattr(
        cms,
        "_stage_and_validate_candidate",
        AsyncMock(
            return_value={
                "source_candidate_handoff": dict(document=handoff.document, sha256=handoff.document_sha256),
            }
        ),
    )
    forbidden_by_name = {}
    for module, names in (
        (cms.recovery, ("repaired_candidate_selection", "resume_pending_cleanup", "dispose_prior_vectors")),
        (cms, ("_registry_source_admission", "_prepare_serving_candidate")),
    ):
        for name in names:
            forbidden_by_name[name] = AsyncMock(side_effect=AssertionError("legacy publication called"))
            monkeypatch.setattr(module, name, forbidden_by_name[name])
    release = deepcopy(_identity()["metadata"]["source_release"])
    staged_result = await cms._stage_acquired({}, {}, "root", object(), release, None, "endpoint")
    assert staged_result["cms_finalization"] == "pending" and "cms_serving_candidate" not in staged_result
    assert staged_result["source_candidate_handoffs"] == {
        "dataset": dict(document=handoff.document, sha256=handoff.document_sha256)
    }
    assert staged_result["resource_count"] == 16
    for method in forbidden_by_name.values():
        method.assert_not_awaited()
