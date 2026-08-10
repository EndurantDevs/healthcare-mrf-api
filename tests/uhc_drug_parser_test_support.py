# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
from contextlib import contextmanager

from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity
from process.formulary_fhir.source_artifact_contract import VerifiedSourceArtifact
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


FILE_SET = "a" * 64
PROJECTION = "b" * 64
VERIFIED_AT = dt.datetime(2026, 8, 10, 12, tzinfo=dt.UTC)


def source_record(
    *,
    plan_id: str = "PLAN / PUBLIC 01",
    tier: str = "Preferred Brand",
    years: list[int] | None = None,
    rxnorm_id: str = "1234567",
    drug_name: str = "Synthetic public drug",
    **extra_fields,
) -> dict:
    return {
        "drug_name": drug_name,
        "plans": [
            {
                "drug_tier": tier,
                "plan_id": plan_id,
                "plan_id_type": "HIOS",
                "prior_authorization": False,
                "quantity_limit": True,
                "step_therapy": False,
                "years": years or [2026],
                **extra_fields.pop("plan_extension", {}),
            }
        ],
        "rxnorm_id": rxnorm_id,
        **extra_fields,
    }


def artifact_set(
    records_by_index: dict[int, list[dict]] | None = None,
    timestamps_by_index: dict[int, str] | None = None,
):
    records_by_index = records_by_index or {}
    timestamps_by_index = timestamps_by_index or {}
    bodies_by_name: dict[str, bytes] = {}
    artifacts: list[VerifiedSourceArtifact] = []
    for index in range(48):
        family = "cs" if index < 24 else "ifp"
        file_name = f"drug-{family}-{index:02d}.json"
        body = json.dumps(
            records_by_index.get(index, [source_record()]),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        bodies_by_name[file_name] = body
        identity = SourceArtifactIdentity(
            source_id=UHC_FORMULARY_SOURCE_ID,
            source_file_set_sha256=FILE_SET,
            source_file_id=f"{index + 1:064x}",
            raw_listing_projection_sha256=PROJECTION,
            family=family,
            file_name=file_name,
            source_url=f"https://example.invalid/{file_name}",
            catalog_modified_at=timestamps_by_index.get(
                index,
                "2026-08-10T00:00:00Z",
            ),
            catalog_entry_sha256=f"{index + 101:064x}",
            expected_byte_count=len(body),
        )
        artifacts.append(
            VerifiedSourceArtifact(
                identity=identity,
                artifact_sha256=hashlib.sha256(body).hexdigest(),
                artifact_byte_count=len(body),
                verified_at=VERIFIED_AT,
            )
        )
    exact_artifacts = tuple(artifacts)
    return (
        VerifiedSourceArtifactSet(
            source_id=UHC_FORMULARY_SOURCE_ID,
            source_file_set_sha256=FILE_SET,
            raw_listing_projection_sha256=PROJECTION,
            artifacts=exact_artifacts,
            artifact_set_sha256=artifact_set_sha256(exact_artifacts),
        ),
        bodies_by_name,
    )


def install_artifact_reader(monkeypatch, spool_module, bodies_by_name):
    @contextmanager
    def open_artifact(artifact):
        yield io.BytesIO(bodies_by_name[artifact.identity.file_name])

    monkeypatch.setattr(spool_module, "open_verified_source_artifact", open_artifact)
