# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose noncurrent CMS data; protected activation remains a separate boundary.

The trusted caller authenticates the issued profile, exact preparation claim,
canonical writer/callable custody and mutation fence through its outer commit.
This module never issues that authority or binds a SOURCE context.
"""

import json
import re
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import JSON

from db.models import ProviderDirectoryEndpointDataset
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_prepared_allocation import adopt_prepared_cms_allocation
from process.provider_directory_cms_prepared_identity_publication import (
    publish_prepared_cms_identities,
    read_prepared_cms_identity_publication,
)
from process.provider_directory_cms_prepared_identity_stream import _frozen_coordinates
from process.provider_directory_cms_prepared_payload_publication import (
    copy_prepared_cms_payloads,
    verify_prepared_cms_payloads,
)
from process.provider_directory_cms_prepared_relationships import assert_prepared_cms_relationships
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_profile_selection_dataset import _cms_dataset_pair, _cms_incumbent_pair

_HEADER_COLUMNS = tuple(str(column.name) for column in ProviderDirectoryEndpointDataset.__table__.columns)
_JSON_COLUMNS = {
    str(column.name) for column in ProviderDirectoryEndpointDataset.__table__.columns if isinstance(column.type, JSON)
}
_RELATIONSHIP_RECEIPT = "provider_directory_cms_npd_relationship_receipt"


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_candidate_" + code)


def _header(native_row):
    native_row = dict(native_row) if native_row is not None else None
    _require(native_row is not None and set(native_row) == set(_HEADER_COLUMNS), "header_layout_changed")
    return {
        key: json.loads(column_value) if key in _JSON_COLUMNS and isinstance(column_value, str) else column_value
        for key, column_value in dict(native_row).items()
    }


async def _inputs(workspace, handoff):
    await workspace.verify_current()
    await _frozen_coordinates(workspace)
    binding, identity, document = workspace.binding, workspace.identity, handoff.document
    _require(
        binding["indexed"]
        and binding["handoff_sha256"] == handoff.document_sha256
        and document["contract"] == storage.CMS_ISOLATED_CONTRACT
        and document["identity"] == identity,
        "handoff_changed",
    )
    _require(coverage._schema_name() == storage_identifier(workspace.catalog_schema), "canonical_schema_changed")
    frozen = storage_relation(workspace.policy.schema, document["tables"][storage.HEADER]["name"])
    header = _header(await completion._frozen_handoff_header(workspace.connection, workspace.policy, document))
    _require(
        header["published_at"] is None
        and header["superseded_at"] is None
        and type(header["publication_metadata_summary_json"]) is dict
        and header["publication_metadata_summary_json"].get("source_ids") == ["cms-npd"]
        and header["content_proof_resource_types"] == sorted(identity["metadata"]["selected_resources"]),
        "frozen_header_changed",
    )
    _cms_dataset_pair(header, allow_desired=True)
    return frozen, header


async def _allocate_header(connection, catalog_schema, frozen, header):
    canonical = storage_relation(catalog_schema, storage.HEADER)
    _require(
        not await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {canonical} WHERE dataset_id=$1)", header["dataset_id"]
        ),
        "canonical_header_exists",
    )
    override_sql_by_column = {"status": "'acquiring'", "validated_at": "NULL", "candidate_storage_json": "NULL"}
    fields = ",".join(storage_identifier(column) for column in _HEADER_COLUMNS)
    selected = ",".join(
        override_sql_by_column.get(column, "s." + storage_identifier(column)) for column in _HEADER_COLUMNS
    )
    status = await connection.execute(
        f"INSERT INTO {canonical} ({fields}) SELECT {selected} FROM {frozen} s WHERE dataset_id=$1",
        header["dataset_id"],
    )
    _require(status == "INSERT 0 1", "header_copy_incomplete")
    return canonical


async def _relationship_receipt(workspace, graph, *, create=False):
    relation = storage_relation(workspace.catalog_schema, _RELATIONSHIP_RECEIPT)
    expected_by_field = {
        "dataset_id": workspace.identity["dataset_id"],
        "release_id": workspace.identity["metadata"]["source_release"]["vector_sha256"],
        "projection_contract": "cms-npd-reference-ledger-v1",
        "relationship_count": graph["relationship_count"],
    }
    if create:
        status = await workspace.connection.execute(
            f"INSERT INTO {relation}(dataset_id,release_id,projection_contract,relationship_count) VALUES($1,$2,$3,$4)",
            *expected_by_field.values(),
        )
        _require(status == "INSERT 0 1", "relationship_receipt_incomplete")
    native_row = await workspace.connection.fetchrow(
        f"SELECT * FROM {relation} WHERE dataset_id=$1", expected_by_field["dataset_id"]
    )
    _require(native_row is not None and dict(native_row) == expected_by_field, "relationship_receipt_changed")


async def _read_header(workspace, expected_by_field):
    canonical = storage_relation(workspace.catalog_schema, storage.HEADER)
    actual = _header(
        await workspace.connection.fetchrow(
            f"SELECT * FROM {canonical} WHERE dataset_id=$1", expected_by_field["dataset_id"]
        )
    )
    expected_by_field = {**expected_by_field, "candidate_storage_json": None}
    if actual["status"] == "published":
        stamp = actual["published_at"]
        _require(type(stamp) is datetime and stamp.tzinfo is None, "canonical_header_changed")
        expected_by_field.update(status="published", published_at=stamp)
    _require(actual == expected_by_field, "canonical_header_changed")
    return actual


async def _native_component(workspace, handoff, header, payloads, native):
    """Compare native-four data; Control separately authenticates its claim and authority."""
    from process.provider_directory_cms_prepared_allocation import read_cms_generic_copy_tables

    _require(type(native) is dict, "native_publication_required")
    native = json.loads(json.dumps(native, allow_nan=False))
    roots = storage.RELATIONS[:4]
    counts_by_root = {root: payloads["row_counts"][root] for root in roots}
    _require(
        set(native)
        == {
            "contract",
            "dataset_id",
            "endpoint_id",
            "run_id",
            "attempt_id",
            "source_handoff_sha256",
            "frozen_sha256",
            "validation_sha256",
            "tables",
            "counts",
            "completed_at",
        }
        and native.get("contract") == "generic-source-canonical-copy-v1"
        and native.get("dataset_id") == workspace.identity["dataset_id"]
        and native.get("endpoint_id") == workspace.identity["endpoint_id"]
        and native.get("source_handoff_sha256") == handoff.document_sha256
        and native.get("counts") == counts_by_root
        and all(type(count) is int and count >= 0 for count in native["counts"].values())
        and counts_by_root[storage.RESOURCE] == handoff.document["admission"]["resource_count"]
        and all(type(native.get(key)) is str and 0 < len(native[key]) <= 128 for key in ("run_id", "attempt_id"))
        and all(
            type(native.get(key)) is str and re.fullmatch(r"[0-9a-f]{64}", native[key])
            for key in ("frozen_sha256", "validation_sha256")
        )
        and type(native.get("tables")) is dict
        and set(native["tables"]) == set(roots),
        "native_publication_changed",
    )
    try:
        stamp = datetime.fromisoformat(native["completed_at"])
        _require(stamp.utcoffset() is not None, "native_publication_changed")
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("cms_prepared_candidate_native_publication_changed") from error
    _require(
        stamp.astimezone(timezone.utc).replace(tzinfo=None) == header["published_at"], "native_publication_changed"
    )
    _require(await read_cms_generic_copy_tables(workspace) == native["tables"], "native_publication_changed")
    return native


async def _descriptor(workspace, header, coverage_proof_by_field):
    canonical = storage_relation(workspace.catalog_schema, storage.HEADER)
    rows = await workspace.connection.fetch(
        f"SELECT * FROM {canonical} WHERE endpoint_id=$1 AND is_current LIMIT 2", header["endpoint_id"]
    )
    return {
        "version": 1,
        "status": "ready",
        "desired_cms_dataset": _cms_dataset_pair(header, allow_desired=True),
        "expected_cms_incumbent": _cms_incumbent_pair(
            [_header(native_row) for native_row in rows], header["endpoint_id"]
        ),
        "release_id": coverage_proof_by_field["release_id"],
        "proof_version": coverage_proof_by_field["proof_version"],
    }


async def _candidate_report(workspace, handoff, header, identities, payloads):
    """Share the existing graph, coverage and descriptor checks without choosing a payload reader."""
    graph = await assert_prepared_cms_relationships(workspace, handoff)
    await _relationship_receipt(workspace, graph)
    coverage_proof_by_field = {
        "dataset_id": header["dataset_id"],
        "endpoint_id": header["endpoint_id"],
        "release_id": workspace.identity["metadata"]["source_release"]["vector_sha256"],
        "dataset_hash": header["dataset_hash"],
        "proof_version": coverage._PROOF_VERSION,
    }
    await coverage.assert_sealed_cms_candidate_coverage(
        workspace.session, coverage._schema_name(), coverage_proof_by_field
    )
    relation = storage_relation(workspace.catalog_schema, coverage._CANDIDATE_TABLE)
    native_row = await workspace.connection.fetchrow(
        f"SELECT * FROM {relation} WHERE dataset_id=$1 AND release_id=$2 AND proof_version=$3",
        coverage_proof_by_field["dataset_id"],
        coverage_proof_by_field["release_id"],
        coverage_proof_by_field["proof_version"],
    )
    _require(native_row is not None, "coverage_changed")
    await workspace.verify_current()
    return {
        "state": "verified_data",
        "header": header,
        "identities": identities,
        "payloads": payloads,
        "relationships": graph,
        "coverage": dict(native_row),
        "descriptor": await _descriptor(workspace, header, coverage_proof_by_field),
    }


async def read_prepared_cms_candidate(workspace, handoff, *, native_publication=None):
    """Read complete noncurrent data evidence; this does not authenticate authority.

    The caller holds its qualified fence or pinned snapshot and may retain this
    result for exact comparison. The serving descriptor stays its existing shape.
    """
    _, expected_header = await _inputs(workspace, handoff)
    header = await _read_header(workspace, expected_header)
    identities = await read_prepared_cms_identity_publication(workspace, handoff)
    payloads = await verify_prepared_cms_payloads(workspace, handoff)
    if header["status"] == "published":
        payloads["native_publication"] = await _native_component(
            workspace, handoff, header, payloads, native_publication
        )
    return await _candidate_report(workspace, handoff, header, identities, payloads)


async def read_prepared_cms_generic_candidate(workspace, handoff, *, allocation, native_publication):
    """Read the four copied targets while the two closed CMS heaps remain unattached."""
    from process.provider_directory_cms_prepared_payload_publication import verify_allocated_cms_generic_payloads

    _, expected_header = await _inputs(workspace, handoff)
    header = await _read_header(workspace, expected_header)
    _require(
        header["status"] == "published"
        and header["is_current"] is False
        and header["published_at"] is not None
        and header["superseded_at"] is None,
        "published_noncurrent_required",
    )
    identities = await read_prepared_cms_identity_publication(workspace, handoff)
    payloads = await verify_allocated_cms_generic_payloads(workspace, handoff, allocation=allocation)
    payloads["native_publication"] = await _native_component(workspace, handoff, header, payloads, native_publication)
    return await _candidate_report(workspace, handoff, header, identities, payloads)


async def finish_prepared_cms_candidate(workspace, handoff, *, allocation):
    """Copy verified data and return a noncurrent descriptor in one savepoint.

    Existing canonical guards stay enabled. Adopt only the exact authenticated
    allocation from an earlier committed transaction. The real protected finisher
    owns allocation provenance, terminal receipt retry and its writer fence.
    All writes here roll back together on failure/cancel. The caller commits.
    """
    allocation = json.loads(json.dumps(allocation, allow_nan=False))
    frozen, expected_header = await _inputs(workspace, handoff)
    identity, connection = workspace.identity, workspace.connection
    release = identity["metadata"]["source_release"]["vector_sha256"]
    async with _savepoint(connection):
        canonical = await adopt_prepared_cms_allocation(workspace, handoff, allocation)
        await publish_prepared_cms_identities(workspace, handoff)
        identities = await read_prepared_cms_identity_publication(workspace, handoff)
        payloads = await copy_prepared_cms_payloads(workspace, handoff)
        graph = await assert_prepared_cms_relationships(workspace, handoff)
        await _relationship_receipt(workspace, graph, create=True)
        status = await connection.execute(
            f"UPDATE {canonical} c SET status='validated',validated_at=f.validated_at FROM {frozen} f "
            "WHERE c.dataset_id=f.dataset_id AND c.dataset_id=$1 AND c.status='acquiring' AND NOT c.is_current",
            identity["dataset_id"],
        )
        _require(status == "UPDATE 1", "header_seal_incomplete")
        coverage_proof_by_field = await coverage.prepare_cms_candidate_coverage_in_session(
            workspace.session,
            SimpleNamespace(dataset_id=identity["dataset_id"], endpoint_id=identity["endpoint_id"]),
            release,
            expected_header["dataset_hash"],
        )
        candidate_evidence = await read_prepared_cms_candidate(workspace, handoff)
        _require(await _inputs(workspace, handoff) == (frozen, expected_header), "frozen_header_changed")
        _require(candidate_evidence["identities"] == identities, "identities_changed")
        _require(candidate_evidence["payloads"] == payloads, "payloads_changed")
        _require(candidate_evidence["relationships"] == graph, "relationships_changed")
        _require(
            all(
                candidate_evidence["coverage"].get(key) == column_value
                for key, column_value in coverage_proof_by_field.items()
            ),
            "coverage_changed",
        )
        await workspace.verify_current()
    return candidate_evidence["descriptor"]
