# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Support for one real UHC drug PostgreSQL and retained-CAS proof."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from db.connection import Database
from process import uhc_provider_file_catalog_artifacts as catalog_artifacts
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.uhc_drug_acquisition import (
    acquire_uhc_drug_artifacts,
)
from process.formulary_fhir.uhc_drug_operation import (
    uhc_drug_run_identities,
)
from process.formulary_fhir.uhc_drug_twin import (
    verify_and_record_uhc_drug_twins,
)
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID
from process.formulary_fhir.uhc_source_artifacts import (
    prepare_uhc_source_artifact_registration,
)
from process.provider_directory_retained_blob_store import (
    retained_artifact_blob_components,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore
from tests.uhc_provider_file_catalog_test_data import live_catalog_payloads
from tests.uhc_provider_file_catalog_test_data import raw_catalog_snapshot


SYNTHETIC_PLAN_ID = "SYNTHETIC-PUBLIC-PLAN"
SYNTHETIC_RXNORM_ID = "1234567"


@dataclass(frozen=True, slots=True)
class VerticalProofIdentity:
    """Keep only durable selectors after acquisition objects are discarded."""

    receipt_id: str
    candidate_dataset_id: str
    artifact_sha256: str
    artifact_byte_count: int


class _BodyContent:
    def __init__(self, body: bytes) -> None:
        self._body = body

    async def iter_chunked(self, _chunk_size: int):
        yield self._body


class _BodyResponse:
    def __init__(self, source_url: str, body: bytes) -> None:
        self.url = source_url
        self.status = 200
        self.headers: dict[str, str] = {}
        self.content_length = len(body)
        self.content = _BodyContent(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _BodySession:
    def __init__(self, bodies_by_url: dict[str, bytes]) -> None:
        self._bodies_by_url = bodies_by_url
        self.requested_urls: list[str] = []

    def get(
        self,
        source_url: str,
        *,
        allow_redirects: bool,
        headers: dict[str, str],
    ) -> _BodyResponse:
        assert allow_redirects is False
        assert headers == {"Accept-Encoding": "identity"}
        self.requested_urls.append(source_url)
        return _BodyResponse(source_url, self._bodies_by_url[source_url])


def runtime_database(database_url: Any) -> Database:
    """Create a fresh runtime pool without relying on process-wide DB env."""

    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    return Database(
        engine=engine,
        session_factory=async_sessionmaker(
            engine,
            expire_on_commit=False,
            autoflush=False,
        ),
    )


def private_work_directory(root: Path) -> Path:
    """Create the operator's exact private owned spool parent."""

    work_directory = root / "vertical-work"
    work_directory.mkdir(mode=0o700)
    os.chmod(work_directory, 0o700)
    return work_directory


def _synthetic_drug_body() -> bytes:
    source_by_field = {
        "drug_name": "Synthetic public drug",
        "plans": [
            {
                "drug_tier": "Preferred Brand",
                "plan_id": SYNTHETIC_PLAN_ID,
                "plan_id_type": "HIOS",
                "prior_authorization": False,
                "quantity_limit": True,
                "step_therapy": False,
                "years": [2026],
            }
        ],
        "rxnorm_id": SYNTHETIC_RXNORM_ID,
    }
    return json.dumps(
        [source_by_field],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _catalog_payloads(body: bytes) -> tuple[dict[str, Any], dict[str, bytes]]:
    payloads_by_family = live_catalog_payloads()
    bodies_by_name: dict[str, bytes] = {}
    observed_at = (
        dt.datetime.now(dt.UTC) - dt.timedelta(days=1)
    ).isoformat().replace("+00:00", "Z")
    for family in ("cs", "ifp"):
        for catalog_entry in payloads_by_family[family]["drugs"]:
            catalog_entry["date"] = observed_at
            catalog_entry["size"] = len(body)
            bodies_by_name[catalog_entry["name"]] = body
    return payloads_by_family, bodies_by_name


def _retain_listing_snapshot(
    monkeypatch: Any,
    artifact_root: Path,
    payloads_by_family: dict[str, Any],
) -> dict[str, Any]:
    catalog_root = artifact_root / "catalog"
    monkeypatch.setattr(
        catalog_artifacts,
        "catalog_artifact_root",
        lambda: catalog_root,
    )
    artifact_store = PTG2ArtifactStore(catalog_root)
    proof_documents: list[dict[str, Any]] = []
    for document in raw_catalog_snapshot(
        payloads_by_family=payloads_by_family
    ).documents:
        artifact_path = artifact_store.artifact_path(
            document.raw_sha256,
            kind=catalog_artifacts.CATALOG_ARTIFACT_KIND,
            suffix=".json",
        )
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_bytes(document.raw_bytes)
        proof_documents.append(
            {
                "family": document.family,
                "url": document.url,
                "response_url": document.response_url,
                "raw_sha256": document.raw_sha256,
                "byte_count": len(document.raw_bytes),
                "storage_uri": artifact_store.storage_uri(artifact_path),
            }
        )
    return {
        "raw_set_sha256": (
            catalog_artifacts.raw_set_sha256_from_documents(proof_documents)
        ),
        "documents": proof_documents,
    }


def acquisition_fixture(
    monkeypatch: Any,
    artifact_root: Path,
) -> tuple[dict[str, Any], Any, _BodySession]:
    """Retain exact two-listing bytes and expose 48 in-memory responses."""

    payloads_by_family, bodies_by_name = _catalog_payloads(
        _synthetic_drug_body()
    )
    raw_proof = _retain_listing_snapshot(
        monkeypatch,
        artifact_root,
        payloads_by_family,
    )
    registration = prepare_uhc_source_artifact_registration(
        UHC_FORMULARY_SOURCE_ID,
        raw_proof,
    )
    session = _BodySession(
        {
            identity.source_url: bodies_by_name[identity.file_name]
            for identity in registration.identities
        }
    )

    @asynccontextmanager
    async def session_factory(_timeout: Any):
        yield session

    return raw_proof, session_factory, session


def forbidden_session_factory(_timeout: Any):
    """Fail if a complete durable acquisition replay opens HTTP."""

    raise AssertionError("complete retained replay opened a network session")


async def acquire_recorded_twins(
    database: Database,
    raw_proof: dict[str, Any],
    session_factory: Any,
    work_directory: Path,
) -> VerticalProofIdentity:
    """Run real CAS acquisition, replay, twin builds, admission, and receipt."""

    acquisition = await acquire_uhc_drug_artifacts(
        raw_proof,
        database=database,
        session_factory=session_factory,
    )
    assert acquisition.downloaded_file_count == 48
    replay = await acquire_uhc_drug_artifacts(
        raw_proof,
        database=database,
        session_factory=forbidden_session_factory,
    )
    assert replay.artifacts == acquisition.artifacts
    assert (replay.downloaded_file_count, replay.reused_file_count) == (0, 48)
    cutoff = await database.scalar("SELECT transaction_timestamp();")
    run_ids = uhc_drug_run_identities(
        acquisition.source_observation_sha256,
        acquisition.source_file_set_sha256,
        acquisition.artifact_set_sha256,
        cutoff,
    )
    recorded = await verify_and_record_uhc_drug_twins(
        acquisition=acquisition,
        baseline_run_id=run_ids.baseline_run_id,
        candidate_run_id=run_ids.candidate_run_id,
        cutoff=cutoff,
        work_directory=work_directory,
        database=database,
    )
    _assert_independent_recording(recorded)
    first_artifact = acquisition.artifacts.artifacts[0]
    return VerticalProofIdentity(
        receipt_id=recorded.receipt.receipt_id,
        candidate_dataset_id=(
            recorded.twin_result.candidate.dataset.dataset_id
        ),
        artifact_sha256=first_artifact.artifact_sha256,
        artifact_byte_count=first_artifact.artifact_byte_count,
    )


def _assert_independent_recording(recorded: Any) -> None:
    baseline = recorded.twin_result.baseline
    candidate = recorded.twin_result.candidate
    assert baseline.dataset.dataset_id != candidate.dataset.dataset_id
    assert baseline.dataset.run_id != candidate.dataset.run_id
    assert baseline.evidence == candidate.evidence == recorded.receipt.evidence
    assert baseline.evidence.file_count == 48
    assert baseline.evidence.plan_count == 2
    assert baseline.evidence.medication_membership_count == 2
    assert recorded.receipt.admission == recorded.twin_result.admission


async def assert_durable_prepublication_state(
    database: Database,
    schema_name: str,
    identity: VerticalProofIdentity,
) -> None:
    """Require the exact persisted vertical graph and no legacy relation."""

    expected_count_by_relation = {
        "fhir_formulary_source_artifact": 48,
        "fhir_formulary_dataset": 2,
        "fhir_formulary_twin_attempt": 1,
        "fhir_formulary_twin_admission": 1,
        "fhir_formulary_uhc_admission_receipt": 1,
        "fhir_formulary_current": 0,
    }
    for relation_name, expected_count in expected_count_by_relation.items():
        assert await database.scalar(
            f"SELECT count(*) FROM {table_name(relation_name)};"
        ) == expected_count
    stored_receipt = await database.first(
        f"SELECT receipt_id, candidate_dataset_id FROM "
        f"{table_name('fhir_formulary_uhc_admission_receipt')};"
    )
    assert dict(stored_receipt._mapping) == {
        "receipt_id": identity.receipt_id,
        "candidate_dataset_id": identity.candidate_dataset_id,
    }
    legacy_relation = f'"{schema_name}".plan_drug_raw'
    assert await database.scalar(
        "SELECT to_regclass(:relation_name);",
        relation_name=legacy_relation,
    ) is None
    assert await database.scalar(
        "SELECT count(*) FROM pg_class AS relation JOIN pg_namespace AS "
        "namespace ON namespace.oid = relation.relnamespace WHERE "
        "namespace.nspname = :schema_name AND position("
        "'plan_drug_raw' in lower(relation.relname)) > 0;",
        schema_name=schema_name,
    ) == 0


async def current_pointer(database: Database) -> tuple[str, int]:
    """Read the exact current dataset identity and generation."""

    pointer = await database.first(
        f"SELECT dataset_id, generation FROM "
        f"{table_name('fhir_formulary_current')};"
    )
    return pointer.dataset_id, pointer.generation


def corrupt_retained_blob(
    artifact_root: Path,
    identity: VerticalProofIdentity,
) -> None:
    """Change retained bytes in place while preserving their claimed length."""

    artifact_path = artifact_root.joinpath(
        *retained_artifact_blob_components(identity.artifact_sha256)
    )
    original_body = artifact_path.read_bytes()
    assert len(original_body) == identity.artifact_byte_count
    replacement_prefix = b"{" if original_body[:1] != b"{" else b"["
    artifact_path.chmod(0o600)
    artifact_path.write_bytes(replacement_prefix + original_body[1:])


__all__ = (
    "VerticalProofIdentity",
    "acquire_recorded_twins",
    "acquisition_fixture",
    "assert_durable_prepublication_state",
    "corrupt_retained_blob",
    "current_pointer",
    "private_work_directory",
    "runtime_database",
)
