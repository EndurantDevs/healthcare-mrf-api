# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project sealed CMS intake through its immutable acquisition retry lineage."""

from sqlalchemy import select, text

from api.provider_directory_cms_generation import RESOURCE_TYPES, accepted_release
from api.provider_directory_source_dataset_selection import _source_local_dataset_statement
from db.models import ImportRun, ProviderDirectoryEndpointDataset, db
from process.provider_directory_cms_npd_recovery import candidate_available_sql
from process.provider_directory_cms_serving_receipt import read_serving_receipt
from process.provider_directory_profile_selection_contract import _validated_pair
from process.provider_directory_profile_selection_dataset import _cms_dataset_pair

_MAX_LINEAGE = 256
_METRIC_FIELDS = {
    "version",
    "status",
    "desired_cms_dataset",
    "expected_cms_incumbent",
    "release_id",
    "proof_version",
}
_PIN_FIELDS = {"source_id", "endpoint_id", "dataset_id", "dataset_hash", "acquisition_root_run_id"}
_NON_ACQUISITION_FLAGS = (
    "probe",
    "publish_artifacts_only",
    "canonical_backfill_only",
    "contact_backfill_only",
    "seed_only",
    "provider_directory_dispatch_aggregate",
)


def _dataset_statement(*, current):
    """Use the existing source endpoint and compact admission seal, bounded to detect ambiguity."""
    model = ProviderDirectoryEndpointDataset
    statement = _source_local_dataset_statement((("cms-npd",),), current_published_only=current)
    return (
        statement.add_columns(model.import_run_id)
        .where(
            model.status == ("published" if current else "validated"),
            model.content_proof_admission_version == 1,
            model.content_proof_admission_kind == "generic",
            model.content_proof_resource_types == sorted(RESOURCE_TYPES),
            text(candidate_available_sql(model.__tablename__, model.__table__.schema)),
        )
        .order_by(None)
        .limit(2)
    )


def _run_statement(*, run_id=None, parent_id=None):
    """Read a primary-key owner or at most two children using the importer-qualified retry index."""
    model = ImportRun
    return (
        select(
            model.run_id,
            model.retry_of_run_id,
            model.importer,
            model.engine,
            model.node_id,
            model.status,
            model.finished_at,
            model.params,
            model.metrics["cms_serving_candidate"].label("candidate"),
        )
        .where(
            model.importer == "provider-directory-fhir",
            model.run_id == run_id if parent_id is None else model.retry_of_run_id == parent_id,
        )
        .limit(2)
    )


def _require_acquisition(run, anchor):
    """Reject non-acquisition or cross-node retry edges rather than borrowing unrelated successes."""
    params = run["params"]
    if (
        not isinstance(params, dict)
        or params.get("source_ids") != ["cms-npd"]
        or params.get("import_resources") is not True
        or any(params.get(flag) for flag in _NON_ACQUISITION_FLAGS)
        or run["engine"] != "healthcare-mrf-api"
        or not run["node_id"]
        or run["node_id"] != anchor["node_id"]
    ):
        raise ValueError("cms_candidate_acquisition_changed")


async def _acquisition_leaf(session, dataset):
    """Follow only bounded exact descendants, retaining the immutable dataset owner on the chain."""
    anchors = (await session.execute(_run_statement(run_id=dataset["acquisition_root_run_id"]))).mappings().all()
    if len(anchors) != 1:
        raise ValueError("cms_candidate_acquisition_missing")
    anchor = run = anchors[0]
    visited_run_ids = set()
    for _depth in range(_MAX_LINEAGE):
        _require_acquisition(run, anchor)
        if run["run_id"] in visited_run_ids or run["finished_at"] is None:
            raise ValueError("cms_candidate_acquisition_not_terminal")
        visited_run_ids.add(run["run_id"])
        children = (await session.execute(_run_statement(parent_id=run["run_id"]))).mappings().all()
        if not children:
            if run["status"] != "succeeded" or dataset["import_run_id"] not in visited_run_ids:
                raise ValueError("cms_candidate_acquisition_owner_missing")
            return run
        if len(children) != 1 or run["status"] not in {"failed", "canceled", "succeeded"}:
            raise ValueError("cms_candidate_acquisition_ambiguous")
        run = children[0]
    raise ValueError("cms_candidate_acquisition_depth_exceeded")


def _descriptor(run, desired, incumbent, release_id):
    """Permit only forward publication progression; refresh the incumbent independently of old metrics."""
    metric = run["candidate"]
    if not isinstance(metric, dict) or set(metric) != _METRIC_FIELDS:
        raise ValueError("cms_candidate_metric_invalid")
    if (
        type(metric["version"]) is not int
        or metric["version"] != 1
        or metric["status"] != "ready"
        or type(metric["proof_version"]) is not int
        or metric["proof_version"] != 2
        or metric["release_id"] != release_id
    ):
        raise ValueError("cms_candidate_metric_invalid")
    emitted = _validated_pair(metric["desired_cms_dataset"], allow_desired=True)
    previous = metric["expected_cms_incumbent"]
    if previous is not None:
        previous = _validated_pair(previous)
        if previous["source_id"] != "cms-npd" or previous["endpoint_id"] != desired["endpoint_id"]:
            raise ValueError("cms_candidate_metric_incumbent_invalid")
    if (
        any(emitted[key] != desired[key] for key in _PIN_FIELDS)
        or (emitted["is_current"] and (not desired["is_current"] or previous != emitted))
        or (not emitted["is_current"] and previous is not None and previous["dataset_id"] == emitted["dataset_id"])
    ):
        raise ValueError("cms_candidate_metric_identity_changed")
    return {
        **metric,
        "desired_cms_dataset": desired,
        "expected_cms_incumbent": incumbent,
        "acquisition_run_id": run["run_id"],
    }


async def _require_coverage(session, dataset, release_id):
    """Compare immutable scalar seals and complete relationship receipts without resource scans or locks."""
    schema = ProviderDirectoryEndpointDataset.__table__.schema
    schema_ref = '"' + schema.replace('"', '""') + '"'
    sealed = await session.scalar(
        text(f"""SELECT EXISTS (
        SELECT 1 FROM {schema_ref}.provider_directory_cms_candidate_coverage c
        JOIN {schema_ref}.provider_directory_cms_npd_relationship_receipt r USING (dataset_id)
        JOIN {schema_ref}.provider_directory_endpoint_dataset d USING (dataset_id)
        WHERE c.dataset_id=:dataset_id AND c.endpoint_id=:endpoint_id AND c.dataset_hash=:dataset_hash
          AND c.release_id=:release_id AND c.proof_version=2
          AND c.admission_sha256=d.content_proof_admission_sha256
          AND c.metadata_sha256=d.publication_metadata_sha256
          AND r.release_id=c.release_id AND r.projection_contract='cms-npd-reference-ledger-v1'
          AND r.relationship_count=c.relationship_count)"""),
        {**dataset, "release_id": release_id},
    )
    if not sealed:
        raise ValueError("cms_candidate_coverage_missing")
    if dataset["is_current"]:
        receipt = await read_serving_receipt(session, schema)
        cms_pin_map = {key: dataset[key] for key in _PIN_FIELDS - {"source_id"}}
        cms_pin_map.update(source_id="cms-npd", release_id=release_id, proof_version=2)
        if receipt is None or receipt["payload"]["cms"] != cms_pin_map:
            raise ValueError("cms_candidate_serving_receipt_missing")


async def cms_serving_candidate(catalog):
    """Expose one authenticated ready intake in a bounded read-only snapshot; never mutate its owner."""
    if not any(
        entry.get("source_ids") == ["cms-npd"]
        and entry.get("runnable") is True
        and entry.get("profile_enabled") is True
        for entry in catalog.get("items", [])
    ):
        return None
    async with db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        await session.execute(text("SET LOCAL statement_timeout='4s'"))
        candidates = (await session.execute(_dataset_statement(current=False))).mappings().all()
        incumbents = (await session.execute(_dataset_statement(current=True))).mappings().all()
        if len(candidates) > 1 or len(incumbents) > 1:
            raise ValueError("cms_candidate_dataset_ambiguous")
        if not candidates and not incumbents:
            return None
        dataset = (candidates or incumbents)[0]
        desired = _cms_dataset_pair(
            {**dataset, "publication_metadata_json": dataset["publication_metadata"]}, allow_desired=True
        )
        incumbent = (
            _cms_dataset_pair({**incumbents[0], "publication_metadata_json": incumbents[0]["publication_metadata"]})
            if incumbents
            else None
        )
        if incumbent is not None and incumbent["endpoint_id"] != desired["endpoint_id"]:
            raise ValueError("cms_candidate_endpoint_changed")
        release_id = accepted_release(dataset)
        await _require_coverage(session, dataset, release_id)
        run = await _acquisition_leaf(session, dataset)
        return _descriptor(run, desired, incumbent, release_id)
