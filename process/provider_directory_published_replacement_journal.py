# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind ordinary retained serving heaps to their exact old and desired Source headers."""

from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

from process import provider_directory_published_replacement_inputs as serving_inputs
from process import provider_directory_published_source_replacement as replacement
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_output_snapshot_catalog import decoded, encoded

FIELD = "source_replacement"


def generation(publication_pin):
    """Fence older inverse consumers while keeping ordinary recovery identities unchanged."""
    from process.provider_directory_serving_recovery import publication_generation

    publication_generation(publication_pin)
    return str(uuid5(NAMESPACE_URL, "provider-directory-published-replacement/v1:" + encoded(publication_pin)))


def _header(values):
    return {
        name: value.isoformat() if isinstance(value, datetime) else value
        for name, value in zip(replacement._HEADER_FIELDS, values, strict=True)
    }


def _outputs(before):
    return {
        root: {"oid": item["native"]["oid"], "rows": item["rows"], "data_sha256": item["data_sha256"]}
        for root, item in before["roots"].items()
    }


def capture(directory, fence, before):
    """Serialize only the genuine preparation scope and original retained output receipts."""
    scope = replacement.selected(directory, fence)
    if scope is None:
        return None
    from process.provider_directory_generic_source_artifacts import current

    admission = current(directory)
    receipts.require(admission is not None and admission.recovery is not None, "replacement_binding_required")
    snapshot = admission.recovery.get("serving_inputs")
    serving_inputs.validate(snapshot)
    dataset = fence.datasets[0]
    return dict(
        version=2,
        source_id=dataset.source_id,
        incumbent=_header(scope.incumbent),
        publication=_header(scope.publication),
        source_contract=decoded(encoded(dataset.source_verification_contract)),
        verification=dict(
            verification_campaign_id=dataset.verification_campaign_id,
            verification_source_scope_hash=dataset.verification_source_scope_hash,
            source_ids=list(dataset.verification_source_ids),
            completion_proof_cutoff=dataset.completion_proof_cutoff,
        ),
        retained_outputs=_outputs(before),
        serving_inputs=decoded(encoded(snapshot)),
    )


def _validate_header(header):
    """Validate the fixed canonical header, preserving nullable legacy digests."""
    receipts.require(
        type(header) is dict
        and set(header) == set(replacement._HEADER_FIELDS)
        and all(
            type(header[name]) is str and bool(header[name].strip())
            for name in ("dataset_id", "endpoint_id", "acquisition_root_run_id", "published_at")
        )
        and type(header["resource_count"]) is int
        and header["resource_count"] >= 0
        and all(
            header[name] is None or type(header[name]) is str
            for name in (
                "import_run_id",
                "dataset_hash",
                "publication_metadata_sha256",
                "content_proof_admission_sha256",
            )
        ),
        "replacement_header_invalid",
    )


def _validate_contract(binding_by_field, pin):
    """Require the closed Source contract and its original verification coordinates."""
    receipts.require(
        type(binding_by_field) is dict
        and set(binding_by_field)
        == {
            "version",
            "source_id",
            "incumbent",
            "publication",
            "source_contract",
            "verification",
            "retained_outputs",
            "serving_inputs",
        }
        and type(binding_by_field["version"]) is int
        and binding_by_field["version"] == 2
        and binding_by_field["source_id"] == pin["source_id"]
        and type(binding_by_field["source_contract"]) is list
        and bool(binding_by_field["source_contract"])
        and binding_by_field["source_contract"][0] == "provider-directory-artifact-source-contract-v1"
        and type(binding_by_field["verification"]) is dict
        and set(binding_by_field["verification"])
        == {"verification_campaign_id", "verification_source_scope_hash", "source_ids", "completion_proof_cutoff"},
        "replacement_binding_invalid",
    )
    verification = binding_by_field["verification"]
    receipts.require(
        type(verification["source_ids"]) is list
        and all(type(entry) is str and bool(entry.strip()) for entry in verification["source_ids"])
        and all(
            verification[name] is None or type(verification[name]) is str
            for name in ("verification_campaign_id", "verification_source_scope_hash", "completion_proof_cutoff")
        ),
        "replacement_binding_invalid",
    )


def validate(binding_by_field, pin, *, before=None):
    """Keep the optional versioned preparation binding closed without changing journal v1."""
    if binding_by_field is None:
        return
    _validate_contract(binding_by_field, pin)
    for header in (binding_by_field["incumbent"], binding_by_field["publication"]):
        _validate_header(header)
    publication = binding_by_field["publication"]
    serving_inputs.validate(binding_by_field["serving_inputs"], pin)
    receipts.require(
        (
            publication["dataset_id"],
            publication["endpoint_id"],
            publication["acquisition_root_run_id"],
            publication["dataset_hash"],
        )
        == (pin["dataset_id"], pin["endpoint_id"], pin["parent_run_id"], pin["dataset_hash"]),
        "replacement_publication_changed",
    )
    outputs = binding_by_field["retained_outputs"]
    receipts.require(type(outputs) is dict and set(outputs) == set(receipts.ROOTS), "replacement_outputs_invalid")
    for entry in outputs.values():
        receipts.require(
            type(entry) is dict
            and set(entry) == {"oid", "rows", "data_sha256"}
            and type(entry["oid"]) is int
            and 0 < entry["oid"] < 2**32
            and type(entry["rows"]) is int
            and entry["rows"] >= 0
            and type(entry["data_sha256"]) is str
            and receipts._HASH.fullmatch(entry["data_sha256"]) is not None,
            "replacement_outputs_invalid",
        )
    if before is not None:
        receipts.require(outputs == _outputs(before), "replacement_output_binding_changed")


async def check(directory, pin, binding_by_field, *, restored=False):
    """Recheck both immutable headers under original endpoint and same-source writer fences."""
    validate(binding_by_field, pin)
    receipts.require(binding_by_field is not None, "replacement_binding_required")
    await receipts.ordinary(directory.db)
    receipts.require(
        await directory.db.scalar("SHOW transaction_isolation") == "read committed", "replacement_snapshot_changed"
    )
    old, new = binding_by_field["incumbent"], binding_by_field["publication"]
    await serving_inputs.lock_and_check(
        directory, binding_by_field["serving_inputs"], pointer="incumbent" if restored else "publication"
    )
    endpoints = SimpleNamespace(endpoint_ids=sorted({old["endpoint_id"], new["endpoint_id"]}))
    await directory._lock_artifact_fence_endpoint_advisories(endpoints, directory.db)
    await directory._lock_artifact_fence_endpoints(endpoints, directory.db)
    source_ref, _ = replacement._tables(directory)
    source_rows = await directory.db.all(
        f"SELECT to_jsonb(source) AS source_record FROM {source_ref} source WHERE source_id=:source_id FOR UPDATE NOWAIT",
        source_id=pin["source_id"],
    )
    receipts.require(len(source_rows) == 1, "replacement_source_changed")
    source_record = directory._json_object(
        directory._pagination_checkpoint_row_mapping(source_rows[0])["source_record"]
    )
    expected_endpoint = old["endpoint_id"] if restored else new["endpoint_id"]
    receipts.require(source_record["endpoint_id"] == expected_endpoint, "replacement_alias_changed")
    contract = directory._artifact_source_verification_contract(source_record, **binding_by_field["verification"])
    receipts.require(
        decoded(encoded(contract)) == binding_by_field["source_contract"], "replacement_configured_source_changed"
    )
    fence = await directory._resolve_provider_directory_artifact_datasets(
        [pin["source_id"]], allow_published_replacement=True
    )
    receipts.require(len(fence.datasets) == 1, "replacement_publication_changed")
    dataset = fence.datasets[0]
    receipts.require(
        (dataset.dataset_id, dataset.endpoint_id, dataset.evidence_run_id, dataset.dataset_hash)
        == (pin["dataset_id"], pin["endpoint_id"], pin["parent_run_id"], pin["dataset_hash"]),
        "replacement_publication_changed",
    )
    fence = replace(fence, datasets=(replace(dataset, serving_endpoint_id=old["endpoint_id"]),))
    incumbent, publication = await replacement._headers(directory, fence, directory.db)
    receipts.require((_header(incumbent), _header(publication)) == (old, new), "replacement_headers_changed")
    await replacement._assert_newest(directory, fence, publication, directory.db)


async def restore(directory, pin, value):
    """Restore the old serving pointer in the same inverse transaction as its retained heaps."""
    if value is None:
        return
    await check(directory, pin, value)
    source_ref, _ = replacement._tables(directory)
    result = await directory.db.status(
        f"UPDATE {source_ref} SET endpoint_id=:old,updated_at=now() "
        "WHERE source_id=:source_id AND endpoint_id=:new "
        "AND metadata_json::jsonb ->> :configured_key = :new",
        old=value["incumbent"]["endpoint_id"],
        new=pin["endpoint_id"],
        source_id=pin["source_id"],
        configured_key=directory.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY,
    )
    receipts.require(directory._coerce_rowcount(result) == 1, "replacement_alias_changed")
