# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published generic snapshots retain logical identity and named read validation."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_generic_source_published_snapshot as snapshot
from process import provider_directory_generic_source_storage as storage
from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE, RetainedFHIRCaptureError
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_registry_generic_accepted_capture import _parse_named_header, named_publication_header


def _named_header():
    header = named_publication_header()
    header.pop("header_oid", None)
    header["candidate_storage_json"]["identity"]["metadata"]["selected_resources"] = header[
        "content_proof_resource_types"
    ]
    return header


def test_published_binding_keeps_named_identity_and_tables():
    header = _named_header()
    assert _parse_named_header(header) == header["candidate_storage_json"]
    assert all(table["schema"] == "payload_one" for table in _parse_named_header(header)["tables"].values())


@pytest.mark.parametrize("change", ["dataset", "endpoint", "root", "source", "phase", "release", "resource", "hash"])
def test_published_binding_refuses_logical_drift(change):
    header = _named_header()
    binding = header["candidate_storage_json"]
    changes_by_name = {
        "dataset": lambda: binding["identity"].update(dataset_id="other"),
        "endpoint": lambda: binding["identity"].update(endpoint_id="other"),
        "root": lambda: binding["identity"].update(acquisition_root_run_id="other"),
        "source": lambda: binding["identity"]["metadata"].update(source_ids=["other"]),
        "phase": lambda: binding.update(phase="loading"),
        "release": lambda: binding["identity"]["metadata"].update(source_release={"vector_sha256": "c" * 64}),
        "resource": lambda: binding["tables"][storage.RESOURCE].update(schema="other_payload"),
        "hash": lambda: binding["admission"].update(dataset_hash="f" * 64),
    }
    changes_by_name[change]()
    with pytest.raises((RetainedFHIRCaptureError, GenericCandidateError)):
        _parse_named_header(header)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_named_snapshot_reads_exact_header_and_relations(changed):
    header = _named_header()
    binding = deepcopy(header["candidate_storage_json"])
    header["metadata_matches"] = not changed
    reader = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=header), fetch=AsyncMock())
    writer = SimpleNamespace(is_in_transaction=lambda: True)
    origin = SimpleNamespace(
        schema_name="payload_one",
        admission_profile=SEMANTIC_PROFILE,
        header_relation={"schema": "catalog_example", "name": storage.HEADER},
        resource_relation=binding["tables"][storage.RESOURCE],
        dataset_id=header["dataset_id"],
        endpoint_id=header["endpoint_id"],
        root_run_id=header["acquisition_root_run_id"],
        dataset_sha256=header["dataset_hash"],
        source_id="source-one",
    )
    policy = GenericSourcePolicy("payload_one", "owner", "publisher", ("reader",))
    if changed:
        with pytest.raises(RetainedFHIRCaptureError):
            await snapshot.require_native_generic_snapshot(reader, writer, origin, policy, binding)
        reader.fetch.assert_not_awaited()
    else:
        references = await snapshot.require_native_generic_snapshot(reader, writer, origin, policy, binding)
        assert references == tuple(
            (root, table["schema"], table["name"]) for root, table in sorted(binding["tables"].items())
        )
        assert all(
            call.args[0].startswith('SELECT * FROM "payload_one".') and call.args[0].endswith(" LIMIT 0")
            for call in reader.fetch.await_args_list
        )
    assert 'FROM "catalog_example"."provider_directory_endpoint_dataset"' in reader.fetchrow.await_args.args[0]
