# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
import zlib

import pytest

from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_CONTRACT_ID,
    SOURCE_SUMMARY_CONTRACT_VERSION,
    SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS,
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
)
from process.provider_directory_proof_store import (
    ensure_dataset_proof_shard_table,
)
from process.uhc_retained_dataset import (
    UHC_RETAINED_CANONICAL_CONTRACT_ID,
    UHC_RETAINED_PUBLICATION_CONTRACT_ID,
    UHC_RETAINED_PUBLICATION_METADATA_KEY,
    UHC_RETAINED_SOURCE_ID,
    UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
    UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY,
    UhcAdmittedFile,
    UhcRetainedDatasetError,
    UhcSealedSemanticFile,
    build_uhc_canonical_stage,
    cleanup_uhc_canonical_stage,
    load_complete_admitted_uhc_catalog_set,
)
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    bind_uhc_canonical_content_proof,
)
from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_CONTRACT_ID,
    UHC_SEMANTIC_CONTRACT_VERSION,
    UhcSemanticBuildIdentity,
)
from tests.test_provider_directory_dataset_serving_relations_db import (
    _dataset_database,
    importer,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _provider_fact(ordinal: int) -> dict[str, object]:
    is_individual = ordinal % 2 == 0
    return {
        "type": "INDIVIDUAL" if is_individual else "FACILITY",
        "npi": str(1003821380 + ordinal),
        "name": (
            {"first": f"Ada{ordinal}", "middle": None, "last": "Lovelace"}
            if is_individual
            else None
        ),
        "facility_name": None if is_individual else f"Clinic {ordinal}",
        "facility_type": None if is_individual else ["Clinic"],
        "gender": "F" if is_individual else None,
        "accepting": "accepting",
        "addresses": [
            {
                "address": f"{ordinal + 1} Main St",
                "city": "Chicago",
                "state": "IL",
                "zip": "60601",
                "phone": f"31255512{ordinal:02d}",
            }
        ],
        "plans": [
            {
                "plan_id_type": "HIOS-PLAN-ID",
                "plan_id": "12345IL0010001",
                "years": [2026],
                "network_tier": "PREFERRED",
            }
        ],
        "specialty": ["Family Medicine"],
        "last_updated_on": "2026-07-01",
    }


def _plan_fact(_ordinal: int) -> dict[str, object]:
    return {
        "plan_id_type": "HIOS-PLAN-ID",
        "plan_id": "12345IL0010001",
        "years": [2026],
        "marketing_name": "Example Group Plan",
        "marketing_url": "https://example.test/plan",
        "summary_url": None,
        "formulary_url": None,
        "plan_contact": "8005551212",
        "network": [{"network_tier": "PREFERRED"}],
        "formulary": [{"drug_tier": "GENERIC", "mail_order": True}],
        "last_updated_on": "2026-07-01",
    }


def _counter_map(collection_kind: str) -> dict[str, int]:
    counter_by_field = {
        field_name: 0
        for field_name in SOURCE_SUMMARY_UHC_OUTCOME_COUNT_FIELDS
    }
    if collection_kind == "provider_membership":
        counter_by_field.update(
            raw_provider_records=4,
            raw_individual_records=2,
            raw_facility_records=2,
            raw_address_rows=4,
            raw_provider_plan_rows=4,
            named_facility_records=2,
            facility_type_values=2,
            dated_records=4,
            accepting_newpt_records=4,
            valid_phone_count=4,
            plan_year_rows=4,
        )
    else:
        counter_by_field.update(
            raw_plan_records=4,
            raw_formulary_entries=4,
            dated_records=4,
            plan_year_rows=4,
        )
    return counter_by_field


async def _create_semantic_fixture_relation(
    connection,
    schema: str,
    relation: str,
) -> None:
    """Create one native semantic-stage fixture relation."""
    await connection.execute(
        f"""
        CREATE TABLE {schema}.{relation} (
            row_kind smallint NOT NULL,
            range_ordinal bigint NOT NULL,
            run_ordinal bigint,
            occurrence_ordinal bigint,
            record_start bigint,
            record_count bigint,
            npi text,
            conflict_signature_pack bytea,
            payload_hash text,
            semantic_hash text,
            payload_bytes bytea
        )
        """
    )


async def _insert_semantic_fixture_rows(
    connection,
    schema: str,
    relation: str,
    collection_kind: str,
) -> list[dict[str, object]]:
    """Insert exact fact and evidence rows and return block descriptors."""
    blocks = []
    for ordinal in range(4):
        fact = (
            _provider_fact(ordinal)
            if collection_kind == "provider_membership"
            else _plan_fact(ordinal)
        )
        raw_payload = json.dumps(
            fact, separators=(",", ":"), sort_keys=True
        ).encode() + b"\n"
        compressed = zlib.compress(raw_payload, level=1)
        compressed_hash = hashlib.sha256(compressed).hexdigest()
        semantic_hash = _digest(f"{collection_kind}:semantic:{ordinal}")
        blocks.append(
            {
                "range_ordinal": ordinal,
                "record_start": ordinal,
                "record_count": 1,
                "fact_count": 1,
                "compressed_payload_sha256": compressed_hash,
                "semantic_block_sha256": semantic_hash,
            }
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.{relation} (
                row_kind, range_ordinal, record_start, record_count,
                payload_hash, semantic_hash, payload_bytes
            ) VALUES (1, $1, $1, 1, $2, $3, $4)
            """,
            ordinal,
            compressed_hash,
            semantic_hash,
            compressed,
        )
        if collection_kind == "provider_membership":
            signature = b"".join(
                hashlib.sha256(f"{ordinal}:{field}".encode()).digest()
                for field in range(9)
            )
            await connection.execute(
                f"""
                INSERT INTO {schema}.{relation} (
                    row_kind, range_ordinal, run_ordinal,
                    occurrence_ordinal, npi, conflict_signature_pack
                ) VALUES (2, $1, 0, $1, $2, $3)
                """,
                ordinal,
                str(1003821380 + ordinal),
                signature,
            )
    return blocks


def _semantic_fixture_identity_and_admission(
    catalog_hash: str,
    collection_kind: str,
) -> tuple[UhcSemanticBuildIdentity, UhcAdmittedFile]:
    """Build exact semantic identity and retained admission fixtures."""
    source_file_id = _digest(collection_kind)
    artifact_sha256 = _digest(collection_kind + ":artifact")
    identity = UhcSemanticBuildIdentity(
        catalog_set_sha256=catalog_hash,
        source_file_id=source_file_id,
        artifact_sha256=artifact_sha256,
        raw_contract_version=2,
        raw_range_count=4,
        collection_kind=collection_kind,
        encoder_sha256=_digest("encoder"),
    )
    admitted = UhcAdmittedFile(
        catalog_set_sha256=catalog_hash,
        source_file_id=source_file_id,
        family="ifp",
        collection_kind=collection_kind,
        file_name=(
            "JSON_Providers_ILIEX.json"
            if collection_kind == "provider_membership"
            else "JSON_PLANS_IL.json"
        ),
        artifact_sha256=artifact_sha256,
        artifact_byte_count=1,
        raw_contract_version=2,
        raw_range_count=4,
        record_count=4,
        range_set_sha256=_digest(collection_kind + ":ranges"),
        manifest_sha256=_digest(collection_kind + ":manifest"),
        raw_path=Path(__file__),
        manifest_path=Path(__file__),
    )
    return identity, admitted


def _semantic_fixture_build_row(
    schema: str,
    relation: str,
    collection_kind: str,
    blocks: list[dict[str, object]],
) -> dict[str, object]:
    """Build exact proof metadata for a sealed semantic fixture."""
    is_provider_file = collection_kind == "provider_membership"
    return {
        "stage_schema": schema,
        "stage_relation": relation,
        "fact_blocks_json": blocks,
        "fact_set_sha256": _digest(collection_kind + ":facts"),
        "record_identity_set_sha256": _digest(collection_kind + ":records"),
        "evidence_identity_set_sha256": _digest(collection_kind + ":evidence"),
        "evidence_layout_set_sha256": _digest(collection_kind + ":layout"),
        "evidence_ranges_json": [
            {
                "range_ordinal": ordinal,
                "evidence_count": int(is_provider_file),
                "run_count": int(is_provider_file),
                "layout_sha256": _digest(
                    f"{collection_kind}:evidence-layout:{ordinal}"
                ),
            }
            for ordinal in range(4)
        ],
        "verifier_sha256": _digest("verifier"),
        "counters_json": _counter_map(collection_kind),
        "evidence_count": 4 if is_provider_file else 0,
    }


async def _semantic_file(
    connection,
    schema: str,
    catalog_hash: str,
    collection_kind: str,
) -> UhcSealedSemanticFile:
    """Install one sealed semantic fixture with exact proof metadata."""

    relation = (
        "uhc_sem_provider"
        if collection_kind == "provider_membership"
        else "uhc_sem_plan"
    )
    await _create_semantic_fixture_relation(connection, schema, relation)
    blocks = await _insert_semantic_fixture_rows(
        connection, schema, relation, collection_kind
    )
    identity, admitted = _semantic_fixture_identity_and_admission(
        catalog_hash,
        collection_kind,
    )
    return UhcSealedSemanticFile(
        admitted=admitted,
        identity=identity,
        build_row=_semantic_fixture_build_row(
            schema, relation, collection_kind, blocks
        ),
    )


_ADMITTED_CATALOG_SCHEMA_SQL = """
        CREATE TABLE __SCHEMA__.provider_directory_uhc_catalog_set (
            catalog_set_sha256 varchar(64) PRIMARY KEY,
            file_count integer NOT NULL,
            provider_file_count integer NOT NULL,
            plan_reference_file_count integer NOT NULL
        );
        CREATE TABLE __SCHEMA__.provider_directory_uhc_catalog_file (
            catalog_set_sha256 varchar(64) NOT NULL,
            file_id varchar(64) NOT NULL,
            family varchar(8) NOT NULL,
            collection_kind varchar(32) NOT NULL,
            file_name varchar(256) NOT NULL,
            availability varchar(32) NOT NULL,
            catalog_support varchar(32) NOT NULL
        );
        CREATE TABLE __SCHEMA__.provider_directory_uhc_source_binding (
            catalog_set_sha256 varchar(64) NOT NULL,
            source_file_id varchar(64) NOT NULL,
            artifact_sha256 varchar(64) NOT NULL,
            released_at timestamptz
        );
        CREATE TABLE __SCHEMA__.provider_directory_uhc_raw_artifact (
            artifact_sha256 varchar(64) PRIMARY KEY,
            byte_count bigint NOT NULL,
            storage_uri text NOT NULL,
            status varchar(16) NOT NULL
        );
        CREATE TABLE __SCHEMA__.provider_directory_uhc_raw_layout (
            artifact_sha256 varchar(64) NOT NULL,
            contract_version integer NOT NULL,
            range_count integer NOT NULL,
            record_count bigint NOT NULL,
            range_set_sha256 varchar(64) NOT NULL,
            manifest_sha256 varchar(64) NOT NULL,
            manifest_storage_uri text NOT NULL,
            status varchar(16) NOT NULL
        );
        CREATE TABLE __SCHEMA__.provider_directory_uhc_raw_range (
            artifact_sha256 varchar(64) NOT NULL,
            contract_version integer NOT NULL,
            range_count integer NOT NULL,
            range_ordinal integer NOT NULL,
            status varchar(16) NOT NULL
        );
        CREATE TABLE __SCHEMA__.provider_directory_uhc_artifact_reference (
            content_sha256 varchar(64) NOT NULL,
            artifact_kind varchar(16) NOT NULL,
            layout_artifact_sha256 varchar(64),
            contract_version integer NOT NULL,
            range_count integer NOT NULL,
            catalog_set_sha256 varchar(64) NOT NULL,
            source_file_id varchar(64) NOT NULL,
            storage_uri text NOT NULL,
            released_at timestamptz
        );
"""


async def _install_admitted_artifact_references(
    connection,
    schema: str,
    catalog_hash: str,
    admitted_file: UhcAdmittedFile,
    raw_path: Path,
    manifest_path: Path,
) -> None:
    """Install retained artifact references and verified raw ranges."""
    reference_rows = (
        ("raw", admitted_file.artifact_sha256, None, 0, 0, raw_path.as_uri()),
        (
            "manifest",
            admitted_file.manifest_sha256,
            admitted_file.artifact_sha256,
            2,
            4,
            manifest_path.as_uri(),
        ),
    )
    for artifact_kind, content_hash, layout_hash, contract, ranges, uri in (
        reference_rows
    ):
        await connection.execute(
            f"INSERT INTO {schema}.provider_directory_uhc_artifact_reference "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL)",
            content_hash,
            artifact_kind,
            layout_hash,
            contract,
            ranges,
            catalog_hash,
            admitted_file.source_file_id,
            uri,
        )
    await connection.executemany(
        f"INSERT INTO {schema}.provider_directory_uhc_raw_range "
        "VALUES ($1, 2, 4, $2, 'verified')",
        [
            (admitted_file.artifact_sha256, ordinal)
            for ordinal in range(4)
        ],
    )


async def _install_admitted_file(
    connection,
    schema: str,
    catalog_hash: str,
    admitted_file: UhcAdmittedFile,
    tmp_path: Path,
) -> None:
    """Install one admitted file and all retained registry evidence."""
    raw_path = tmp_path / f"{admitted_file.collection_kind}.json"
    manifest_path = (
        tmp_path / f"{admitted_file.collection_kind}.manifest.json"
    )
    raw_path.write_text("retained", encoding="utf-8")
    manifest_path.write_text("{}", encoding="utf-8")
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_uhc_catalog_file "
        "VALUES ($1, $2, $3, $4, $5, 'published', 'cataloged')",
        catalog_hash,
        admitted_file.source_file_id,
        admitted_file.family,
        admitted_file.collection_kind,
        admitted_file.file_name,
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_uhc_source_binding "
        "VALUES ($1, $2, $3, NULL)",
        catalog_hash,
        admitted_file.source_file_id,
        admitted_file.artifact_sha256,
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_uhc_raw_artifact "
        "VALUES ($1, $2, $3, 'verified')",
        admitted_file.artifact_sha256,
        len("retained"),
        raw_path.as_uri(),
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_uhc_raw_layout "
        "VALUES ($1, 2, 4, 4, $2, $3, $4, 'verified')",
        admitted_file.artifact_sha256,
        admitted_file.range_set_sha256,
        admitted_file.manifest_sha256,
        manifest_path.as_uri(),
    )
    await _install_admitted_artifact_references(
        connection,
        schema,
        catalog_hash,
        admitted_file,
        raw_path,
        manifest_path,
    )


async def _install_admitted_catalog(
    connection,
    schema: str,
    catalog_hash: str,
    files: tuple[UhcAdmittedFile, ...],
    tmp_path: Path,
) -> None:
    """Install the exact admitted-catalog fixture and immutable files."""

    await connection.execute(
        _ADMITTED_CATALOG_SCHEMA_SQL.replace("__SCHEMA__", schema)
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_uhc_catalog_set "
        "VALUES ($1, 2, 1, 1)",
        catalog_hash,
    )
    for admitted_file in files:
        await _install_admitted_file(
            connection,
            schema,
            catalog_hash,
            admitted_file,
            tmp_path,
        )


def _publication_outcome(
    dataset_id: str,
    endpoint_id: str,
    root_id: str,
    dataset_hash: str,
    resource_counts: dict[str, int],
) -> dict[str, object]:
    """Build the final resource-count outcome fixture."""
    selected_resources = sorted(importer.UHC_SUPPORTED_RESOURCES)
    return {
        "complete": True,
        "version": 1,
        "dataset_id": dataset_id,
        "endpoint_id": endpoint_id,
        "acquisition_root_run_id": root_id,
        "dataset_hash": dataset_hash,
        "resource_count": sum(resource_counts.values()),
        "resource_counts": resource_counts,
        "source_ids": [UHC_RETAINED_SOURCE_ID],
        "selected_resources": selected_resources,
    }


def _publication_source_summary(
    outcome: dict[str, object],
    canonical_proof: dict[str, object],
) -> dict[str, object]:
    """Build the source summary fixture bound to the publication outcome."""
    resource_counts = outcome["resource_counts"]
    return {
        "contract_id": SOURCE_SUMMARY_CONTRACT_ID,
        "contract_version": SOURCE_SUMMARY_CONTRACT_VERSION,
        "complete": True,
        "dataset_id": outcome["dataset_id"],
        "endpoint_id": outcome["endpoint_id"],
        "acquisition_root_run_id": outcome["acquisition_root_run_id"],
        "dataset_hash": outcome["dataset_hash"],
        "total_resources": outcome["resource_count"],
        "resource_counts": resource_counts,
        "resource_hashes": {
            resource_type: canonical_proof["resource_hashes"][resource_type]
            for resource_type in resource_counts
        },
        "source_ids": outcome["source_ids"],
        "selected_resources": outcome["selected_resources"],
        "semantic_contract_id": SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
    }


def _publication_metadata(
    dataset_id: str,
    endpoint_id: str,
    root_id: str,
    dataset_hash: str,
    resource_counts: dict[str, int],
    canonical_proof: dict[str, object],
) -> dict[str, object]:
    """Build immutable UHC publication metadata for one fixture dataset."""

    selected_resources = sorted(importer.UHC_SUPPORTED_RESOURCES)
    input_sha256 = "1" * 64
    outcome = _publication_outcome(
        dataset_id,
        endpoint_id,
        root_id,
        dataset_hash,
        resource_counts,
    )
    return {
        "source_ids": [UHC_RETAINED_SOURCE_ID],
        "selected_resources": selected_resources,
        "expected_resources": selected_resources,
        UHC_RETAINED_PUBLICATION_METADATA_KEY: {
            "contract_id": UHC_RETAINED_PUBLICATION_CONTRACT_ID,
            "complete": True,
            "source_id": UHC_RETAINED_SOURCE_ID,
            "dataset_id": dataset_id,
            "acquisition_root_run_id": root_id,
            "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
            "summary_input_sha256": input_sha256,
        },
        UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY: {
            "contract_id": UHC_RETAINED_SUMMARY_INPUT_CONTRACT_ID,
            "complete": True,
            "source_id": UHC_RETAINED_SOURCE_ID,
            "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
            "input_sha256": input_sha256,
        },
        UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: (
            bind_uhc_canonical_content_proof(
                canonical_proof,
                dataset_id=dataset_id,
                endpoint_id=endpoint_id,
                acquisition_root_run_id=root_id,
            )
        ),
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY: outcome,
        importer.SOURCE_SUMMARY_METADATA_KEY: _publication_source_summary(
            outcome,
            canonical_proof,
        ),
    }


async def _build_canonical_fixture(
    database,
    schema: str,
    tmp_path,
):
    """Build a canonical stage from exact admitted semantic fixtures."""
    catalog_hash = _digest("complete-catalog")
    async with database.acquire_driver() as connection:
        provider_file = await _semantic_file(
            connection, schema, catalog_hash, "provider_membership"
        )
        plan_file = await _semantic_file(
            connection, schema, catalog_hash, "plan_reference"
        )
        await _install_admitted_catalog(
            connection,
            schema,
            catalog_hash,
            (provider_file.admitted, plan_file.admitted),
            tmp_path,
        )
        admitted_set = await load_complete_admitted_uhc_catalog_set(
            connection, catalog_hash
        )
        await connection.execute(
            f"UPDATE {schema}.provider_directory_uhc_source_binding "
            "SET released_at=now() WHERE source_file_id=$1",
            plan_file.admitted.source_file_id,
        )
        with pytest.raises(
            UhcRetainedDatasetError,
            match="inactive or ambiguous binding",
        ):
            await load_complete_admitted_uhc_catalog_set(
                connection, catalog_hash
            )
        await connection.execute(
            f"UPDATE {schema}.provider_directory_uhc_source_binding "
            "SET released_at=NULL WHERE source_file_id=$1",
            plan_file.admitted.source_file_id,
        )
        admitted_by_id = {
            admitted_file.source_file_id: admitted_file
            for admitted_file in admitted_set.files
        }
        provider_file = replace(
            provider_file,
            admitted=admitted_by_id[provider_file.admitted.source_file_id],
        )
        plan_file = replace(
            plan_file,
            admitted=admitted_by_id[plan_file.admitted.source_file_id],
        )
        return await build_uhc_canonical_stage(
            connection,
            admitted_set,
            (provider_file, plan_file),
        )


async def _prepare_candidate_fixture_tables(database, schema: str) -> None:
    """Add candidate timestamps, endpoint identity, and proof shards."""
    await database.status(
        f"ALTER TABLE {schema}.provider_directory_endpoint_dataset "
        "ADD COLUMN created_at timestamptz NOT NULL DEFAULT now(), "
        "ADD COLUMN validated_at timestamptz, "
        "ADD COLUMN published_at timestamptz;"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_api_endpoint ("
        "endpoint_id varchar(64) PRIMARY KEY);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_api_endpoint "
        "VALUES ('endpoint-a');"
    )
    await ensure_dataset_proof_shard_table(database, schema)


async def _assert_candidate_retry_and_validation(database, stage):
    """Assert candidate replacement is replayable and then immutable."""
    source_by_field = {
        "endpoint_id": "endpoint-a",
        "api_base": importer.UHC_PROVIDER_DIRECTORY_BASE,
    }
    candidate = await importer._prepare_uhc_retained_candidate(
        source_by_field,
        run_id="uhc-root",
        summary_input=stage.summary_input,
    )
    first_count = await importer._replace_uhc_candidate_resources(
        candidate, stage
    )
    first_proof = await importer._assert_uhc_candidate_content(
        candidate, stage
    )
    replay_candidate = await importer._prepare_uhc_retained_candidate(
        source_by_field,
        run_id="uhc-root",
        summary_input=stage.summary_input,
    )
    assert replay_candidate.dataset_id == candidate.dataset_id
    assert replay_candidate.reused_from_checkpoint is True
    replay_count = await importer._replace_uhc_candidate_resources(
        replay_candidate, stage
    )
    replay_proof = await importer._assert_uhc_candidate_content(
        replay_candidate, stage
    )
    assert first_count == replay_count == 13
    assert replay_proof == first_proof
    publication_identity = importer._expected_uhc_publication_identity(
        stage.summary_input,
        dataset_id=candidate.dataset_id,
        acquisition_root_run_id=candidate.acquisition_root_run_id,
    )
    async with database.acquire() as connection:
        validation_metadata = (
            await importer._endpoint_dataset_source_summary_metadata(
                connection,
                candidate,
                replay_proof,
                {},
                importer.ENDPOINT_DATASET_VALIDATED,
            )
        )
    assert validation_metadata[
        UHC_RETAINED_PUBLICATION_METADATA_KEY
    ] == publication_identity
    assert validation_metadata[
        UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY
    ] == stage.summary_input
    assert validation_metadata[importer.SOURCE_SUMMARY_METADATA_KEY][
        "semantic_contract_id"
    ] == "healthporta.uhc.semantic-facts.v2"
    return candidate, first_proof


async def _prepare_uhc_source_for_publish_gate(
    database,
    schema: str,
    candidate,
    stage,
) -> list[str]:
    """Finalize the replay candidate and bind the corporate source."""
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET status='validated' WHERE dataset_id=:dataset_id;",
        dataset_id=candidate.dataset_id,
    )
    with pytest.raises(RuntimeError, match="parent_immutable"):
        await importer._replace_uhc_candidate_resources(candidate, stage)
    selected_resources = sorted(importer.UHC_SUPPORTED_RESOURCES)
    await database.status(
        f"UPDATE {schema}.provider_directory_source "
        "SET source_id=:source_id, org_name='UHC', "
        "canonical_api_base=:api_base, metadata_json=CAST(:metadata AS jsonb) "
        "WHERE source_id='source-a';",
        source_id=UHC_RETAINED_SOURCE_ID,
        api_base=importer.UHC_PROVIDER_DIRECTORY_BASE,
        metadata=json.dumps(
            {
                "provider_directory_supported_resources": selected_resources,
                "provider_directory_fully_enumerable_resources": (
                    selected_resources
                ),
            },
            sort_keys=True,
        ),
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET status='failed' WHERE dataset_id=:dataset_id;",
        dataset_id=candidate.dataset_id,
    )
    return selected_resources


async def _insert_publish_gate_candidate(
    database,
    schema: str,
    dataset_id: str,
    previous_dataset_id: str,
    dataset_hash: str,
    metadata: dict[str, object],
) -> None:
    """Insert one validated dataset fixture for the publication gate."""
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id,
            import_run_id, previous_dataset_id, dataset_hash,
            resource_count, status, is_current, validated_at,
            publication_metadata_json
        ) VALUES (
            :dataset_id, 'endpoint-a', 'gate-root', 'gate-root',
            :previous_dataset_id, :dataset_hash, 13, 'validated', false,
            now(), CAST(:metadata AS jsonb)
        )
        """,
        dataset_id=dataset_id,
        previous_dataset_id=previous_dataset_id,
        dataset_hash=dataset_hash,
        metadata=json.dumps(metadata, sort_keys=True),
    )


def _good_gate_artifact_dataset(
    dataset_hash: str,
    selected_resources: list[str],
):
    """Build the artifact dataset expected by the successful gate."""
    return importer.ProviderDirectoryArtifactDataset(
        source_id=UHC_RETAINED_SOURCE_ID,
        endpoint_id="endpoint-a",
        dataset_id="uhc-gate-good",
        evidence_run_id="gate-root",
        selected_resources=tuple(selected_resources),
        expected_resources=tuple(selected_resources),
        recorded_expected_resources=tuple(selected_resources),
        previous_dataset_id="dataset-a",
        expected_incumbent_dataset_id="dataset-a",
        status="validated",
        is_current=False,
        promote_on_cutover=True,
        dataset_hash=dataset_hash,
        resource_count=13,
    )


async def _publish_good_gate_candidate(
    database,
    schema: str,
    first_proof,
    stage,
    selected_resources: list[str],
):
    """Publish the fully bound candidate and return its artifact identity."""
    metadata = _publication_metadata(
        "uhc-gate-good",
        "endpoint-a",
        "gate-root",
        first_proof.dataset_hash,
        stage.resource_counts,
        stage.content_proof,
    )
    await _insert_publish_gate_candidate(
        database,
        schema,
        "uhc-gate-good",
        "dataset-a",
        first_proof.dataset_hash,
        metadata,
    )
    good = _good_gate_artifact_dataset(
        first_proof.dataset_hash,
        selected_resources,
    )
    await importer._publish_validated_uhc_dataset(
        importer.EndpointDatasetCandidate(
            endpoint_id="endpoint-a",
            dataset_id="uhc-gate-good",
            acquisition_root_run_id="gate-root",
            source_ids=(UHC_RETAINED_SOURCE_ID,),
            selected_resources=tuple(selected_resources),
            import_run_id="gate-root",
            previous_dataset_id="dataset-a",
            expected_resources=tuple(selected_resources),
            already_validated=True,
        )
    )
    assert await database.scalar(
        f"SELECT is_current FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id='uhc-gate-good';"
    ) is True
    assert await database.scalar(
        f"SELECT status FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id='dataset-a';"
    ) == "superseded"
    return good


async def _assert_bad_gate_candidate_rejected(
    database,
    schema: str,
    first_proof,
    stage,
    good,
) -> None:
    """Reject a candidate whose source summary contradicts its proof."""
    bad_metadata = _publication_metadata(
        "uhc-gate-bad",
        "endpoint-a",
        "gate-root",
        first_proof.dataset_hash,
        stage.resource_counts,
        stage.content_proof,
    )
    bad_metadata[importer.SOURCE_SUMMARY_METADATA_KEY][
        "resource_counts"
    ] = {"InsurancePlan": 999}
    await _insert_publish_gate_candidate(
        database,
        schema,
        "uhc-gate-bad",
        "uhc-gate-good",
        first_proof.dataset_hash,
        bad_metadata,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="candidate_changed",
    ):
        await importer._publish_validated_artifact_dataset(
            importer.replace(
                good,
                dataset_id="uhc-gate-bad",
                previous_dataset_id="uhc-gate-good",
                expected_incumbent_dataset_id="uhc-gate-good",
            )
        )
    assert await database.scalar(
        f"SELECT status FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id='uhc-gate-bad';"
    ) == "validated"


@pytest.mark.asyncio
async def test_postgres_canonical_stage_retry_idempotency_and_publish_gate(
    monkeypatch,
    tmp_path,
):
    """Prove canonical rows, crash replay, immutable fencing, and final gate."""

    async with _dataset_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        stage = await _build_canonical_fixture(database, schema, tmp_path)
        try:
            assert stage.resource_counts["InsurancePlan"] == 1
            assert stage.summary_input["count_by_field"][
                "raw_provider_records"
            ] == 4
            await _prepare_candidate_fixture_tables(database, schema)
            candidate, first_proof = (
                await _assert_candidate_retry_and_validation(database, stage)
            )
            selected_resources = await _prepare_uhc_source_for_publish_gate(
                database,
                schema,
                candidate,
                stage,
            )
            good = await _publish_good_gate_candidate(
                database,
                schema,
                first_proof,
                stage,
                selected_resources,
            )
            await _assert_bad_gate_candidate_rejected(
                database,
                schema,
                first_proof,
                stage,
                good,
            )
        finally:
            async with database.acquire_driver() as connection:
                await cleanup_uhc_canonical_stage(connection, stage)
