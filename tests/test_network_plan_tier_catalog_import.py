"""Closed typed CLI input uses existing identities and never legacy adoption."""

import hashlib
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_catalog_import as catalog_import
from process.network_catalog_store import NetworkCatalogPreimage
from process.registry_fhir_catalog_identity import ALIAS_SCOPE, FHIRNetworkIdentityInput, _allocation_key
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.test_network_catalog_import import _arguments


def _document():
    key, endpoint = "a" * 64, "b" * 64
    allocation = _allocation_key(
        FHIRNetworkIdentityInput(key, endpoint, (UHC_PROVIDER_FILE_SOURCE_ID,), "proof"), "plan_tier"
    )
    return {
        "expected_preimage": asdict(NetworkCatalogPreimage(0, 0, 0, "c" * 64)),
        "rows": [
            {
                "network_id": 7,
                "allocation_key": str(allocation),
                "endpoint_id": endpoint,
                "network_key_id": key,
                "evidence_id": "d" * 64 + ":UHCPlanTier/" + key,
                "display_name": "Synthetic Tier",
                "aliases": [],
            }
        ],
    }


def _parsed(tmp_path, document):
    path = tmp_path / "typed.json"
    input_bytes = json.dumps(document).encode()
    path.write_bytes(input_bytes)
    arguments = _arguments(path, input_bytes) + ["--identity-kind", "existing-plan-tier"]
    arguments[arguments.index("--source-id") + 1] = UHC_PROVIDER_FILE_SOURCE_ID
    arguments[arguments.index("--alias-scope") + 1] = ALIAS_SCOPE
    return catalog_import._parser().parse_args(arguments)


@pytest.mark.parametrize(
    "invalid",
    [
        "missing_preimage",
        "preimage_field",
        "legacy",
        "wrong_key",
        "duplicate_field",
        "unknown_row_field",
        "digest",
        "source",
        "allocation_type",
        "allocation_uppercase",
        "allocation_compact",
    ],
)
def test_typed_cli_refuses_before_database_access(tmp_path, monkeypatch, capsys, invalid):
    document = _document()
    if invalid == "missing_preimage":
        del document["expected_preimage"]
    if invalid == "preimage_field":
        document["expected_preimage"]["unknown"] = 1
    if invalid == "legacy":
        document["rows"][0]["legacy_uuid"] = "11111111-1111-4111-8111-111111111111"
    if invalid == "wrong_key":
        document["rows"][0]["allocation_key"] = "11111111-1111-4111-8111-111111111111"
    if invalid == "unknown_row_field":
        document["rows"][0]["unknown"] = "synthetic"
    if invalid == "allocation_type":
        document["rows"][0]["allocation_key"] = 7
    if invalid == "allocation_uppercase":
        document["rows"][0]["allocation_key"] = document["rows"][0]["allocation_key"].upper()
    if invalid == "allocation_compact":
        document["rows"][0]["allocation_key"] = document["rows"][0]["allocation_key"].replace("-", "")
    parsed = _parsed(tmp_path, document)
    if invalid == "duplicate_field":
        path = parsed.input_file
        path.write_bytes(path.read_bytes().replace(b'"network_id": 7', b'"network_id": 8, "network_id": 7'))
        parsed.input_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    elif invalid == "digest":
        parsed.input_sha256 = "0" * 64
    elif invalid == "source":
        parsed.source_id = "another-source"
    monkeypatch.setattr(catalog_import, "_parser", lambda: SimpleNamespace(parse_args=lambda _: parsed))
    monkeypatch.setattr(catalog_import, "Database", lambda: pytest.fail("invalid input reached database"))
    assert catalog_import.run_command([]) == 2
    assert capsys.readouterr().err == '{"code":"invalid_catalog_input","status":"error"}\n'


@pytest.mark.asyncio
async def test_typed_import_skips_legacy_adoption_and_uses_same_seed(tmp_path, monkeypatch):
    catalog_rows, actor, context = catalog_import._read_input(_parsed(tmp_path, _document()))
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def transaction():
        yield

    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"id": 1}), execute=AsyncMock())
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value='"example_api"'),
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=connection))
            )
        ),
        begin_nested=transaction,
    )
    monkeypatch.setattr(catalog_import, "assume_registry_source_owner", AsyncMock())
    monkeypatch.setattr(
        catalog_import, "adopt_legacy_network_aliases", AsyncMock(side_effect=AssertionError("typed path cannot adopt"))
    )
    seed = AsyncMock(return_value={"created_records": 1})
    monkeypatch.setattr(catalog_import, "seed_network_catalog_drafts", seed)
    assert await catalog_import.import_network_catalog(
        session,
        catalog_rows,
        actor,
        context,
        api_role="example_api",
        owner_role="example_owner",
        control_schema="example_registry",
    ) == {"created_records": 1}
    seeded = seed.call_args.args[1][0]
    assert seeded.network_id == 7 and str(seeded.allocation_key) == catalog_rows[0]["allocation_key"]
    assert connection.execute.call_args.args == ('SET LOCAL ROLE "example_api"',)


@pytest.mark.parametrize(
    "changes",
    [
        {"draft_revision": True},
        {"draft_revision": -1},
        {"draft_revision": 9223372036854775806},
        {"approved_revision": True},
        {"approved_revision": 2},
        {"serving_generation": False},
        {"serving_generation": -1},
        {"metadata_generation": None},
        {"metadata_generation": "C" * 64},
        {"metadata_generation": "c" * 63},
    ],
)
def test_typed_preimage_rejects_ambiguous_revisions_and_noncanonical_metadata(changes):
    from process.network_catalog_store import NetworkCatalogSeedError

    with pytest.raises(NetworkCatalogSeedError, match="preimage is invalid"):
        replace(NetworkCatalogPreimage(1, 1, 0, "c" * 64), **changes)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "mixed",
        "duplicate",
        "allocation_type",
        "endpoint_type",
        "endpoint_hash",
        "key_type",
        "key_hash",
        "evidence_type",
        "evidence_hash",
        "aliases",
    ],
)
async def test_typed_store_refuses_invalid_rows_before_native_access(tmp_path, invalid):
    from process.network_catalog_store import NetworkCatalogSeedError, seed_network_catalog_drafts

    catalog_rows, actor, context = catalog_import._read_input(_parsed(tmp_path, _document()))
    draft_rows = catalog_import._typed_input(catalog_rows, context)
    if invalid == "mixed":
        draft_rows.append({"network_id": 8})
    elif invalid == "duplicate":
        draft_rows.append(draft_rows[0])
    else:
        changes_by_case = {
            "allocation_type": {"allocation_key": str(draft_rows[0].allocation_key)},
            "endpoint_type": {"endpoint_id": 7},
            "endpoint_hash": {"endpoint_id": "Z" * 64},
            "key_type": {"network_key_id": 7},
            "key_hash": {"network_key_id": "Z" * 64},
            "evidence_type": {"evidence_id": 7},
            "evidence_hash": {"evidence_id": "e" * 64 + ":UHCPlanTier/" + "f" * 64},
            "aliases": {"aliases": "Synthetic Alias"},
        }
        draft_rows[0] = replace(draft_rows[0], **changes_by_case[invalid])
    with pytest.raises(NetworkCatalogSeedError, match="typed|homogeneous"):
        await seed_network_catalog_drafts(
            object(),
            draft_rows,
            actor,
            context,
            api_role="example_api",
            owner_role="example_owner",
            control_schema="example_registry",
        )
