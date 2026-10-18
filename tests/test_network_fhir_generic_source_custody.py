# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned custody keeps the exact source pin and every native child binding."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_source_custody as custody
from process import provider_directory_generic_source_provision as provision
from process import provider_directory_generic_source_storage as storage
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from tests.test_provider_directory_generic_source_storage import _catalog_row


def test_historical_all_heap_contract_is_preserved_and_partial_cut_refuses():
    names = sorted(custody._PARTITIONED_SOURCE)
    rows = [dict(relname=name, relkind=b"r") for name in names]
    assert custody._is_partitioned_source_shape(rows) is False
    assert custody._is_partitioned_source_shape([dict(relname=name, relkind=b"p") for name in names]) is True
    with pytest.raises(custody.FHIRSourceCustodyError):
        custody._is_partitioned_source_shape([rows[0], rows[1] | {"relkind": b"p"}])


def _published():
    pin = PinnedFHIRMembershipSource(
        "source_schema", "cms-npd", "endpoint", "dataset", "a" * 64, "b" * 64, 1, "scope", "2026-08-10"
    )
    identity_by_field = dict(
        dataset_id="dataset",
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        metadata={
            "source_ids": ["cms-npd"],
            "source_release": {"vector_sha256": pin.release_id},
        },
    )
    relation_rows, tables = [], {}
    for index, root in enumerate(provision.PAYLOAD_ROOTS):
        is_partitioned = root in provision.PARTITIONED_ROOTS
        relation_rows.append(
            {
                **_catalog_row(),
                "nspname": "source_schema",
                "relname": root,
                "oid": index + 1,
                "relkind": "p" if is_partitioned else "r",
                "partition_key": "LIST (dataset_id)" if is_partitioned else None,
            }
        )
    for index, root in enumerate(storage.RELATIONS):
        parent = next(relation_by_field for relation_by_field in relation_rows if relation_by_field["relname"] == root)
        relation_by_field = {
            **_catalog_row(),
            "nspname": "source_schema",
            "relname": "pdgs_" + "a" * 32 + "_" + storage._SUFFIXES[root],
            "oid": 100 + index,
            "relispartition": True,
            "parents": [parent["oid"]],
        }
        relation_rows.append(relation_by_field)
        tables[root] = storage._table_proof(relation_by_field, private=False)
    binding_by_field = dict(
        contract=storage.CONTRACT,
        identity=identity_by_field,
        phase="published",
        ordinal=1,
        resets={},
        tables=tables,
        auxiliary={"phase": "published", "tables": {}, "sealed_sha256": "f" * 64},
        relationship_sha256="e" * 64,
    )
    header_by_field = dict(
        endpoint_id="endpoint", acquisition_root_run_id="root", candidate_storage_json=binding_by_field
    )
    return pin, relation_rows, header_by_field


@pytest.mark.asyncio
async def test_exact_published_binding_and_root_oid_are_required(monkeypatch):
    pin, rows, header = _published()
    monkeypatch.setattr(provision, "verify_generic_source_read_family", AsyncMock(return_value=rows))
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=header))
    result = await custody._generic_partitioned_custody(connection, pin, "source_owner", ("source_reader",))
    assert result["binding"] == header["candidate_storage_json"]
    for change in ("root", "child", "phase", "release"):
        changed_rows, changed_header = deepcopy(rows), deepcopy(header)
        if change == "root":
            next(row for row in changed_rows if row["relname"] == storage.RESOURCE)["oid"] = 999
        elif change == "child":
            changed_rows[-1]["parents"] = [999]
        elif change == "phase":
            changed_header["candidate_storage_json"]["phase"] = "prepared"
        else:
            changed_header["candidate_storage_json"]["identity"]["metadata"]["source_release"]["vector_sha256"] = (
                "c" * 64
            )
        monkeypatch.setattr(provision, "verify_generic_source_read_family", AsyncMock(return_value=changed_rows))
        connection.fetchrow.return_value = changed_header
        with pytest.raises((custody.FHIRSourceCustodyError, storage.GenericCandidateError)):
            await custody._generic_partitioned_custody(connection, pin, "source_owner", ("source_reader",))


def test_native_family_distinguishes_global_maps_from_dataset_partitions():
    _, relation_rows, _ = _published()
    provision._require_reader_family(relation_rows, "source_schema", 100)
    for root in storage.CMS_IDENTITY_ROOTS:
        changed = deepcopy(relation_rows)
        next(row for row in changed if row["relname"] == root)["relkind"] = "p"
        with pytest.raises(storage.GenericCandidateError, match="reader_family_changed"):
            provision._require_reader_family(changed, "source_schema", 100)


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe_field", ("namespace_owner_protected", "namespace_owner_reachable", "namespace_create"))
async def test_separate_namespace_owner_requires_protection_and_no_runtime_reach(unsafe_field):
    owner_by_field = dict(
        oid=100,
        rolname="source_owner",
        rolcanlogin=False,
        elevated=False,
        rolsuper=False,
        rolcreatedb=False,
        rolcreaterole=False,
        rolreplication=False,
        rolbypassrls=False,
    )
    reader_by_field = dict(
        oid=101,
        rolname="source_reader",
        elevated=False,
        owner_set=False,
        namespace_usage=True,
        namespace_owner_protected=True,
        namespace_owner_reachable=False,
        namespace_create=False,
    )
    reader_by_field[unsafe_field] = unsafe_field != "namespace_owner_protected"
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[owner_by_field, reader_by_field]))
    with pytest.raises(storage.GenericCandidateError, match="reader_authority"):
        await provision.verify_generic_source_read_family(
            connection, "source_schema", "source_owner", ("source_reader",)
        )
    connection.fetch.assert_awaited_once()
