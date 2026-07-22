# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real Rust-CLI parity against the Python Provider Directory oracle."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import os
from pathlib import Path

import pytest

import process.provider_directory_projection_native as native
from process.provider_directory_projection_contract import canonical_row_digest
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_materializer_context import (
    projection_context_for_fixture,
    synthetic_projection_context,
)
from tests.provider_directory_projection_materializer_support import (
    SyntheticFHIRFixture,
)
from tests.provider_directory_projection_native_copy_support import (
    BinaryCopyTransaction,
    decode_native_copy_rows,
    golden_projection_rows,
)
from tests.provider_directory_projection_semantic_cases import (
    semantic_projection_cases,
)


NATIVE_BINARY_ENV = "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_BIN"


def _native_binary() -> str:
    binary_path = os.getenv(NATIVE_BINARY_ENV)
    if not binary_path:
        pytest.skip(f"set {NATIVE_BINARY_ENV} to the built ptg2_scanner")
    resolved_path = Path(binary_path).resolve()
    if not resolved_path.is_file():
        pytest.fail(f"{NATIVE_BINARY_ENV} does not name a file")
    return str(resolved_path)


async def _retained_chunks(payload: bytes):
    split_at = max(1, len(payload) // 3)
    for chunk_start in range(0, len(payload), split_at):
        yield payload[chunk_start : chunk_start + split_at]


async def _actual_projection(context):
    transaction = BinaryCopyTransaction(len(context.fixture.resources))
    outcome = await native.run_native_projection_command(
        context.claim,
        context.child_lease,
        context.stage,
        _retained_chunks(context.fixture.encoded),
        framing=context.fixture.framing,
        transaction=transaction,
        materializer_workers=4,
        native_threads=2,
        executable=_native_binary(),
    )
    return outcome, decode_native_copy_rows(bytes(transaction.copy_bytes))


def _accepted_semantic_resources() -> tuple[dict, ...]:
    return tuple(
        semantic_case.resource
        for semantic_case in semantic_projection_cases()
        if semantic_case.accepted
    )


def test_cross_language_semantic_corpus_has_frozen_55_case_shape() -> None:
    semantic_cases = semantic_projection_cases()

    assert len(semantic_cases) == 55
    assert sum(semantic_case.accepted for semantic_case in semantic_cases) == 49
    assert len({semantic_case.name for semantic_case in semantic_cases}) == 55


@pytest.mark.asyncio
@pytest.mark.parametrize("framing", ("ndjson", "bundle"))
async def test_real_native_cli_matches_all_18_fields_for_semantic_corpus(
    framing,
) -> None:
    context = synthetic_projection_context(
        framing,
        resources=_accepted_semantic_resources(),
        label=f"native-cli-semantic-{framing}",
    )

    outcome, native_row_maps = await _actual_projection(context)
    oracle_row_maps = golden_projection_rows(context)
    oracle_digest, oracle_count = canonical_row_digest(oracle_row_maps)

    assert native_row_maps == oracle_row_maps
    assert outcome.copied_record_count == oracle_count == 49
    assert outcome.shard_proof.canonical_row_sha256 == oracle_digest
    assert outcome.shard_proof.proof["resource_counts"] == {
        resource_type: sum(
            row_map["resource_type"] == resource_type
            for row_map in oracle_row_maps
        )
        for resource_type in context.claim.recipe_lease.recipe.selected_resources
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "semantic_case",
    tuple(
        semantic_case
        for semantic_case in semantic_projection_cases()
        if not semantic_case.accepted
    ),
    ids=lambda semantic_case: semantic_case.name,
)
async def test_real_native_cli_rejects_each_malformed_semantic_case(
    semantic_case,
) -> None:
    context = synthetic_projection_context(
        "ndjson",
        resources=(semantic_case.resource,),
        label=f"native-cli-{semantic_case.name}",
    )
    with pytest.raises(ProviderDirectoryProjectionError):
        await _actual_projection(context)


@pytest.mark.asyncio
@pytest.mark.parametrize(("framing", "range_ordinal"), (("ndjson", 0), ("bundle", None)))
async def test_real_native_cli_binds_range_and_full_artifact_identities(
    framing,
    range_ordinal,
) -> None:
    manifest_digest = hashlib.sha256(b"full-layout-manifest").hexdigest()
    context = synthetic_projection_context(
        framing,
        label=f"native-cli-identity-{framing}",
        retained_range_ordinal=range_ordinal,
        block_content_sha256=(None if range_ordinal is not None else manifest_digest),
    )

    outcome, native_row_maps = await _actual_projection(context)

    assert native_row_maps == golden_projection_rows(context)
    assert outcome.input_sha256 == context.child_lease.input_sha256
    if range_ordinal is None:
        assert outcome.input_sha256 == manifest_digest


@pytest.mark.asyncio
async def test_real_native_cli_preserves_arbitrary_precision_decimal() -> None:
    decimal_token = "12345678901234567890.123456789012345678901234567890"
    encoded = (
        b'{"active":true,"extension":[{"url":"urn:exact-decimal",'
        b'"valueDecimal":'
        + decimal_token.encode("ascii")
        + b'}],"id":"organization-exact-decimal",'
        b'"resourceType":"Organization"}\n'
    )
    fixture = SyntheticFHIRFixture(
        "ndjson",
        (
            {
                "resourceType": "Organization",
                "id": "organization-exact-decimal",
                "active": True,
            },
        ),
        encoded,
    )
    context = projection_context_for_fixture(
        fixture,
        label="native-cli-exact-decimal",
        block_content_sha256=hashlib.sha256(b"full-decimal-manifest").hexdigest(),
    )

    _outcome, (native_row_map,) = await _actual_projection(context)
    (oracle_row_map,) = golden_projection_rows(context)

    assert native_row_map == oracle_row_map
    assert str(native_row_map["payload_json"]["extension"][0]["valueDecimal"]) == (
        decimal_token
    )


@pytest.mark.asyncio
async def test_real_native_cli_orders_competing_versions_deterministically() -> None:
    resources = (
        {"resourceType": "Organization", "id": "org-version", "active": True},
        {"resourceType": "Organization", "id": "org-version", "active": False},
    )
    context = synthetic_projection_context(
        "ndjson",
        resources=resources,
        label="native-cli-competing-versions",
    )

    first_outcome, first_row_maps = await _actual_projection(context)
    second_outcome, second_row_maps = await _actual_projection(context)

    assert first_row_maps == second_row_maps == golden_projection_rows(context)
    assert first_outcome.shard_proof == second_outcome.shard_proof
    assert [row_map["source_rank"] for row_map in first_row_maps] == sorted(
        row_map["source_rank"] for row_map in first_row_maps
    )


__all__ = ("NATIVE_BINARY_ENV",)
