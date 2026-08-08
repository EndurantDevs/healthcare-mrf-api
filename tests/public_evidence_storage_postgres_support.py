# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic helpers for the public-evidence catalog PostgreSQL proof."""

from __future__ import annotations

import importlib.util
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
import re
from types import SimpleNamespace
from typing import Any, AsyncIterator, Mapping
import uuid

import asyncpg
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from public_evidence import evidence_record_token_policy as token_policy
from public_evidence import source_release_contract as release_contract
from tests.public_evidence_source_release_support import release_input


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / ("20260808090000_public_evidence_storage_foundation.py")
)
POSTGRES_DSN_ENV = "HLTHPRT_PUBLIC_EVIDENCE_STORAGE_POSTGRES_DSN"
DISPOSABLE_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)
DISPOSABLE_SCHEMA_RE = re.compile(r"^public_evidence_test_[0-9a-f]{32}$")
PTG_POLICY_ID = "ptg-tin-hmac-sha256-v1:synthetic-v1"
PUBLIC_POLICY_ID = "healthporta-tax-identity-hmac-sha256-v1:synthetic-v1"
EXPECTED_COLUMNS_BY_TABLE = {
    "public_evidence_source_identity": (
        "identity_ref",
        "identity_kind",
        "content_identity_kind",
        "content_sha256",
        "created_at",
    ),
    "public_evidence_source_release": (
        "source_release_ref",
        "contract_sha256",
        "contract",
        "foundation_scope",
        "source_kind",
        "authority_classification",
        "trust_classification",
        "semantic_limits",
        "artifact_identity_ref",
        "artifact_identity_kind",
        "artifact_content_identity_kind",
        "artifact_content_sha256",
        "completeness_mode",
        "completeness_evidence_contract_id",
        "completeness_count_unit",
        "completeness_subject_sha256",
        "expected_record_count",
        "observed_record_count",
        "evidence_root_sha256",
        "rights_classification",
        "rights_proof_sha256",
        "source_binding_contract_id",
        "source_artifact_source_type",
        "source_artifact_identity_kind",
        "source_artifact_sha256",
        "source_binding_sha256",
        "shadow_bundle_binding_sha256",
        "observed_start_at",
        "observed_end_at",
        "effective_start_at",
        "effective_end_at",
        "import_run_ref",
        "lifecycle_state",
        "serving_authority",
        "current_pointer_authority",
        "created_at",
    ),
    "public_evidence_token_policy": (
        "token_policy_contract_id",
        "token_policy_id",
        "token_policy_descriptor_sha256",
        "created_at",
    ),
    "public_evidence_tax_identity": (
        "tax_identity_ref",
        "tin_type",
        "token_policy_contract_id",
        "token_policy_id",
        "token_policy_descriptor_sha256",
        "locator_128",
        "full_hmac_sha256",
        "normalization_contract_id",
        "created_at",
    ),
}
EXPECTED_INDEX_NAMES = {
    "public_evidence_source_identity_pkey",
    "public_evidence_source_identity_content_key",
    "public_evidence_source_identity_owner_key",
    "public_evidence_source_release_pkey",
    "public_evidence_source_release_import_run_key",
    "public_evidence_source_release_contract_key",
    "public_evidence_source_release_owner_key",
    "public_evidence_token_policy_pkey",
    "public_evidence_token_policy_owner_key",
    "public_evidence_tax_identity_pkey",
    "public_evidence_tax_identity_hmac_key",
    "public_evidence_tax_identity_locator_idx",
}


def quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def load_migration() -> Any:
    module_spec = importlib.util.spec_from_file_location(
        "public_evidence_storage_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    parsed_url = make_url(raw_dsn)
    database_name = str(parsed_url.database or "")
    if (
        not parsed_url.drivername.startswith("postgresql")
        or not DISPOSABLE_DATABASE_RE.search(database_name)
        or not parsed_url.host
        or not parsed_url.username
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must identify an explicit PostgreSQL test "
            "database; only a generated disposable schema is modified"
        )
    return parsed_url


async def connect(database: sa.URL) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=str(database.host),
        port=int(database.port or 5432),
        user=str(database.username),
        password=str(database.password or ""),
        database=str(database.database),
    )


async def run_migration_action(
    engine: AsyncEngine,
    migration: Any,
    action: str,
) -> None:
    async with engine.connect() as async_connection:

        def apply_action(sync_connection) -> None:
            context = MigrationContext.configure(sync_connection)
            migration.op = Operations(context)
            with context.begin_transaction():
                getattr(migration, action)()

        await async_connection.run_sync(apply_action)


async def drop_schema(engine: AsyncEngine, schema_name: str) -> None:
    if not DISPOSABLE_SCHEMA_RE.fullmatch(schema_name):
        raise RuntimeError(f"refusing to drop schema {schema_name!r}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"DROP SCHEMA IF EXISTS {quoted(schema_name)} CASCADE"
        )


@asynccontextmanager
async def public_evidence_schema() -> (
    AsyncIterator[tuple[AsyncEngine, sa.URL, str, Any]]
):
    parsed_url = database_url()
    engine = create_async_engine(
        parsed_url.set(drivername="postgresql+asyncpg"),
        pool_pre_ping=True,
    )
    schema_name = f"public_evidence_test_{uuid.uuid4().hex}"
    migration = load_migration()
    migration._schema = lambda: schema_name
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f"CREATE SCHEMA {quoted(schema_name)}")
        await run_migration_action(engine, migration, "upgrade")
        yield engine, parsed_url, schema_name, migration
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()


def canonical_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def release_parameters(source_kind: str) -> dict[str, object]:
    descriptor = release_contract.build_public_evidence_source_release(
        release_input(source_kind)
    )
    artifact = descriptor.artifact_identity
    attestation = descriptor.completeness_attestation
    binding = descriptor.source_binding
    return {
        "source_release_ref": descriptor.source_release_ref,
        "contract_sha256": bytes.fromhex(descriptor.contract_sha256),
        "contract": descriptor.contract,
        "foundation_scope": descriptor.foundation_scope,
        "source_kind": descriptor.source_kind,
        "authority_classification": descriptor.authority_classification,
        "trust_classification": descriptor.trust_classification,
        "semantic_limits": list(descriptor.semantic_limits),
        "artifact_identity_ref": artifact.identity_ref,
        "artifact_identity_kind": artifact.identity_kind,
        "artifact_content_identity_kind": artifact.content_identity_kind,
        "artifact_content_sha256": bytes.fromhex(artifact.content_sha256),
        "completeness_mode": attestation.mode,
        "completeness_evidence_contract_id": attestation.evidence_contract_id,
        "completeness_count_unit": attestation.count_unit,
        "completeness_subject_sha256": bytes.fromhex(attestation.subject_sha256),
        "expected_record_count": attestation.expected_record_count,
        "observed_record_count": attestation.observed_record_count,
        "evidence_root_sha256": bytes.fromhex(attestation.evidence_root_sha256),
        "rights_classification": descriptor.rights_classification,
        "rights_proof_sha256": bytes.fromhex(descriptor.rights_proof_sha256),
        "source_binding_contract_id": None if binding is None else binding.contract_id,
        "source_artifact_source_type": (
            None if binding is None else binding.source_artifact_source_type
        ),
        "source_artifact_identity_kind": (
            None if binding is None else binding.source_artifact_identity_kind
        ),
        "source_artifact_sha256": (
            None if binding is None else bytes.fromhex(binding.source_artifact_sha256)
        ),
        "source_binding_sha256": (
            None if binding is None else bytes.fromhex(binding.source_binding_sha256)
        ),
        "shadow_bundle_binding_sha256": (
            None
            if binding is None
            else bytes.fromhex(binding.shadow_bundle_binding_sha256)
        ),
        "observed_start_at": canonical_timestamp(descriptor.observed_interval.start_at),
        "observed_end_at": canonical_timestamp(descriptor.observed_interval.end_at),
        "effective_start_at": canonical_timestamp(
            descriptor.effective_interval.start_at
        ),
        "effective_end_at": canonical_timestamp(descriptor.effective_interval.end_at),
        "import_run_ref": descriptor.import_run_ref,
        "lifecycle_state": descriptor.lifecycle_state,
        "serving_authority": descriptor.serving_authority,
        "current_pointer_authority": descriptor.current_pointer_authority,
    }


def _recomputed_invalid_release_parameters(
    *,
    omit_required_binding: bool,
    omit_declared_count: bool,
) -> dict[str, object]:
    descriptor = release_contract.build_public_evidence_source_release(
        release_input("tic")
    )
    descriptor_by_field = {
        field_name: getattr(descriptor, field_name)
        for field_name in release_contract._SOURCE_FIELDS
    }
    parameters = release_parameters("tic")
    if omit_required_binding:
        descriptor_by_field["source_binding"] = None
        for field_name in (
            "source_binding_contract_id",
            "source_artifact_source_type",
            "source_artifact_identity_kind",
            "source_artifact_sha256",
            "source_binding_sha256",
            "shadow_bundle_binding_sha256",
        ):
            parameters[field_name] = None
    if omit_declared_count:
        attestation = descriptor.completeness_attestation
        attestation_by_field = {
            field_name: getattr(attestation, field_name)
            for field_name in attestation.__slots__
        }
        attestation_by_field["expected_record_count"] = None
        descriptor_by_field["completeness_attestation"] = SimpleNamespace(
            **attestation_by_field
        )
        parameters["expected_record_count"] = None
    descriptor_by_field["import_run_ref"] = release_contract._derive_import_run_ref(
        descriptor_by_field
    )
    descriptor_by_field["source_release_ref"] = (
        release_contract._derive_source_release_ref(descriptor_by_field)
    )
    parameters.update(
        {
            "import_run_ref": descriptor_by_field["import_run_ref"],
            "source_release_ref": descriptor_by_field["source_release_ref"],
            "contract_sha256": bytes.fromhex(
                release_contract._release_sha256(descriptor_by_field)
            ),
        }
    )
    return parameters


def release_parameters_without_required_binding() -> dict[str, object]:
    """Build exact identifiers for a deliberately invalid unbound TiC release."""

    return _recomputed_invalid_release_parameters(
        omit_required_binding=True,
        omit_declared_count=False,
    )


def release_parameters_without_declared_count() -> dict[str, object]:
    """Build exact identifiers for an invalid declared-complete count omission."""

    return _recomputed_invalid_release_parameters(
        omit_required_binding=False,
        omit_declared_count=True,
    )


async def insert_source_release(
    connection: asyncpg.Connection,
    schema_name: str,
    source_kind: str,
    overrides: Mapping[str, object] | None = None,
) -> dict[str, object]:
    parameters = release_parameters(source_kind)
    if overrides:
        parameters.update(overrides)
    schema = quoted(schema_name)
    await connection.execute(
        f"""INSERT INTO {schema}.public_evidence_source_identity (
            identity_ref, identity_kind, content_identity_kind, content_sha256
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (identity_ref) DO NOTHING""",
        parameters["artifact_identity_ref"],
        parameters["artifact_identity_kind"],
        parameters["artifact_content_identity_kind"],
        parameters["artifact_content_sha256"],
    )
    column_names = tuple(parameters)
    placeholders = ", ".join(
        f"${ordinal}" for ordinal in range(1, len(column_names) + 1)
    )
    await connection.execute(
        f"INSERT INTO {schema}.public_evidence_source_release "
        f"({', '.join(column_names)}) VALUES ({placeholders})",
        *(parameters[column_name] for column_name in column_names),
    )
    return parameters


def token_policy_row(contract_id: str, policy_id: str) -> tuple[str, str, bytes]:
    descriptor = token_policy.token_policy_descriptor_sha256(
        contract_id,
        policy_id,
    )
    return contract_id, policy_id, bytes.fromhex(descriptor)


async def insert_token_policies(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    for contract_id, policy_id in (
        (token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT, PTG_POLICY_ID),
        (token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT, PUBLIC_POLICY_ID),
    ):
        await connection.execute(
            f"""INSERT INTO {schema}.public_evidence_token_policy (
                token_policy_contract_id,
                token_policy_id,
                token_policy_descriptor_sha256
            ) VALUES ($1, $2, $3)""",
            *token_policy_row(contract_id, policy_id),
        )


def tax_identity_parameters(
    contract_id: str,
    policy_id: str,
    tin_type: str,
    full_hmac_hex: str,
) -> dict[str, object]:
    descriptor_hex = token_policy.token_policy_descriptor_sha256(
        contract_id,
        policy_id,
    )
    identity = token_policy.build_opaque_tax_identity(
        {
            "tin_type": tin_type,
            "token_policy_contract_id": contract_id,
            "token_policy_id": policy_id,
            "token_policy_descriptor_sha256": descriptor_hex,
            "locator_128": full_hmac_hex[:32],
            "full_hmac_sha256": full_hmac_hex,
        }
    )
    return {
        "tax_identity_ref": identity.tax_identity_ref,
        "tin_type": identity.tin_type,
        "token_policy_contract_id": identity.token_policy_contract_id,
        "token_policy_id": identity.token_policy_id,
        "token_policy_descriptor_sha256": bytes.fromhex(
            identity.token_policy_descriptor_sha256
        ),
        "locator_128": bytes.fromhex(identity.locator_128),
        "full_hmac_sha256": bytes.fromhex(identity.full_hmac_sha256),
        "normalization_contract_id": identity.normalization_contract_id,
    }


async def insert_tax_identity(
    connection: asyncpg.Connection,
    schema_name: str,
    parameters: Mapping[str, object],
) -> None:
    schema = quoted(schema_name)
    column_names = tuple(parameters)
    placeholders = ", ".join(
        f"${ordinal}" for ordinal in range(1, len(column_names) + 1)
    )
    await connection.execute(
        f"INSERT INTO {schema}.public_evidence_tax_identity "
        f"({', '.join(column_names)}) VALUES ({placeholders})",
        *(parameters[column_name] for column_name in column_names),
    )
