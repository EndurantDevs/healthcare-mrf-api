# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Batch authority, immutable native identities and no per-resource SQL."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _case():
    candidate = SimpleNamespace(
        dataset_id="dataset",
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        selected_resources=("Organization",),
        reused_from_checkpoint=False,
    )
    directory = SimpleNamespace(
        _schema=lambda: "source_schema",
        _endpoint_dataset_candidate_metadata=lambda _: {
            "source_ids": ["cms-npd"],
            "selected_resources": ["Organization"],
            "resource_hash_contract": "contract",
        },
    )
    identity = storage.candidate_identity(directory, candidate)
    tables_by_root = {
        key: dict(name="pdgs_" + "a" * 32 + suffix, oid=index + 10, owner=100, layout_sha256="c" * 64)
        for index, (key, suffix) in enumerate((("resource_input", "_ri"), ("witness_input", "_wi")))
    }
    binding_by_field = dict(
        contract=storage.CONTRACT,
        identity=identity,
        phase="loading",
        ordinal=0,
        resets={},
        tables=tables_by_root,
        auxiliary=None,
        relationship_sha256=None,
    )
    header_by_field = dict(
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        status="acquiring",
        is_current=False,
        publication_metadata_json=identity["metadata"],
        candidate_storage_json=binding_by_field,
    )
    return candidate, directory, header_by_field


@pytest.fixture
def harness(monkeypatch):
    candidate, directory, header = _case()
    copies, sql = [], []

    async def execute(query, *args):
        sql.append(query)
        if query.startswith("UPDATE"):
            assert storage._json(args[2]) == header["candidate_storage_json"]
            header["candidate_storage_json"] = storage._json(args[1])
            return "UPDATE 1"
        return "OK"

    async def copy(name, **kwargs):
        copies.append((name, kwargs))
        return f"COPY {len(kwargs['records'])}"

    connection = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=lambda *args: deepcopy(header)),
        fetchval=AsyncMock(side_effect=lambda *args: header["candidate_storage_json"]["ordinal"] + 1),
        execute=AsyncMock(side_effect=execute),
        copy_records_to_table=AsyncMock(side_effect=copy),
    )
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))

    @asynccontextmanager
    async def owner(database):
        before = deepcopy(header)
        try:
            yield SimpleNamespace(connection=connection, policy=policy)
        except BaseException:
            header.clear()
            header.update(before)
            raise

    async def capture(conn, configured, names, **kwargs):
        proofs = header["candidate_storage_json"]["tables"]
        return {table["name"]: deepcopy(table) for table in proofs.values()}

    monkeypatch.setattr(storage, "source_owner_transaction", owner)
    monkeypatch.setattr(storage, "_capture", AsyncMock(side_effect=capture))
    return SimpleNamespace(
        candidate=candidate,
        directory=directory,
        header=header,
        connection=connection,
        sql=sql,
        copies=copies,
        policy=policy,
    )


def _rows(count):
    return [
        dict(
            dataset_id="dataset",
            resource_type="Organization",
            resource_id=str(index),
            payload_hash="b" * 64,
            payload_json={"value": index},
            acquired_resource_sha256=None,
        )
        for index in range(count)
    ]


@pytest.mark.asyncio
async def test_many_singletons_are_one_copy_and_one_header_control(harness):
    h = harness
    count = await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(4096))
    assert count == 4096
    assert h.connection.fetchrow.await_count == 3
    assert h.connection.fetchrow.await_args_list[0].args[0].endswith("FOR KEY SHARE")
    assert h.connection.fetchrow.await_args_list[1].args[0].endswith("FOR KEY SHARE")
    assert h.connection.fetchrow.await_args_list[2].args[0].endswith("FOR NO KEY UPDATE")
    assert len(h.copies) == 1
    assert len(h.copies[0][1]["records"]) == 4096
    assert len(h.sql) == 2 and h.sql[-1].startswith("UPDATE")
    assert "pg_advisory_xact_lock" in h.sql[0]
    assert not any("INSERT" in query or "FOR SHARE" in query for query in h.sql)
    assert {record[-2] for record in h.copies[0][1]["records"]} == {1}
    assert h.header["candidate_storage_json"]["ordinal"] == 1


@pytest.mark.asyncio
async def test_witness_failure_rolls_back_resources_and_header_ordinal(harness):
    harness_case = harness
    original = deepcopy(harness_case.header)
    harness_case.connection.copy_records_to_table.side_effect = ["COPY 1", RuntimeError("witness rejected")]
    witness_by_field = dict(
        dataset_id="dataset",
        source_id="cms-npd",
        release_id="a" * 64,
        resource_type="Organization",
        resource_id="0",
        raw_payload_sha256="c" * 64,
        normalized_payload_hash="b" * 64,
        raw_payload_json={"resourceType": "Organization", "id": "0"},
    )
    with pytest.raises(RuntimeError, match="witness rejected"):
        await storage.append_generic_source_batch(
            object(), harness_case.directory, harness_case.candidate, _rows(1), [witness_by_field]
        )
    assert harness_case.header == original
    assert len(harness_case.sql) == 1 and "pg_advisory_xact_lock" in harness_case.sql[0]


@pytest.mark.asyncio
async def test_replaced_oid_refuses_before_copy(harness, monkeypatch):
    h = harness
    proofs_by_name = {
        table["name"]: {**table, "oid": 999} for table in h.header["candidate_storage_json"]["tables"].values()
    }
    monkeypatch.setattr(storage, "_capture", AsyncMock(return_value=proofs_by_name))
    with pytest.raises(GenericCandidateError, match="storage_binding_changed"):
        await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(1))
    h.connection.copy_records_to_table.assert_not_awaited()
    assert len(h.sql) == 1 and "pg_advisory_xact_lock" in h.sql[0]


@pytest.mark.asyncio
async def test_reset_advances_cutoff_without_payload_update_or_delete(harness):
    h = harness
    await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(1))
    await storage.reset_generic_source_family(object(), h.directory, h.candidate, "Organization")
    assert h.header["candidate_storage_json"]["resets"] == {"Organization": 1}
    assert len(h.copies) == 1
    assert all("pg_advisory_xact_lock" in query or query.startswith("UPDATE") for query in h.sql)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("phase", "ordinal", "schema"))
async def test_closed_overflow_or_other_namespace_never_reaches_copy(harness, change):
    h = harness
    if change == "phase":
        h.header["candidate_storage_json"]["phase"] = "prepared"
    elif change == "ordinal":
        h.header["candidate_storage_json"]["ordinal"] = 2**63 - 2
    else:
        h.directory._schema = lambda: "other_schema"
    with pytest.raises(GenericCandidateError):
        await storage.append_generic_source_batch(object(), h.directory, h.candidate, _rows(1))
    h.connection.copy_records_to_table.assert_not_awaited()


def _catalog_row():
    return dict(
        relname="pdgs_" + "a" * 32 + "_ri",
        oid=1,
        relowner=100,
        relkind="r",
        relpersistence="p",
        relrowsecurity=False,
        relforcerowsecurity=False,
        inert=True,
        native_checks=True,
        direct_acl_closed=True,
        readers_closed=True,
        relispartition=False,
        parents=[],
        columns=[["dataset_id", "character varying(96)", True, None]],
        constraints=[],
        indexes=[],
    )


@pytest.mark.parametrize(
    "field,value",
    (
        ("inert", False),
        ("direct_acl_closed", False),
        ("native_checks", False),
        ("parents", [2]),
        ("relispartition", True),
    ),
)
def test_native_private_heap_custody_refuses_hooks_grants_or_attachment(field, value):
    with pytest.raises(GenericCandidateError, match="storage_custody_changed"):
        storage._table_proof(_catalog_row() | {field: value}, private=True)


def _sequence_row(*, owner=100, schema="source_schema"):
    return dict(
        input_name="pdgs_" + "a" * 32 + "_ri",
        attname="batch_ordinal",
        attidentity="d",
        deptype="i",
        oid=123,
        relname="pdgs_" + "a" * 32 + "_ri_batch_ordinal_seq",
        nspname=schema,
        relowner=owner,
        relkind="S",
        relpersistence="p",
        seqtypid="bigint",
        seqstart=1,
        seqincrement=1,
        seqmax=2**63 - 1,
        seqmin=1,
        seqcache=1,
        seqcycle=False,
        acl_closed=True,
    )


@pytest.mark.parametrize(
    "field,value",
    (
        ("attname", "row_ordinal"),
        ("attidentity", "a"),
        ("deptype", "a"),
        ("nspname", "other"),
        ("relowner", 101),
        ("relkind", "r"),
        ("relpersistence", "u"),
        ("seqtypid", "integer"),
        ("seqstart", 2),
        ("seqincrement", 2),
        ("seqmax", 100),
        ("seqmin", 0),
        ("seqcache", 2),
        ("seqcycle", True),
        ("acl_closed", False),
    ),
)
def test_input_sequence_requires_exact_native_custody(harness, field, value):
    with pytest.raises(GenericCandidateError, match="input_sequence_changed"):
        storage._require_input_sequence(_sequence_row() | {field: value}, harness.policy, 100)


@pytest.mark.asyncio
async def test_late_sibling_batch_preserves_higher_ordinal_and_reset(harness):
    harness.connection.fetchval.return_value = 3
    harness.connection.fetchval.side_effect = None

    async def concurrent_copy(name, **options):
        binding = harness.header["candidate_storage_json"]
        binding.update(ordinal=7, resets={"Location": 6})
        return f"COPY {len(options['records'])}"

    harness.connection.copy_records_to_table.side_effect = concurrent_copy
    await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    assert harness.header["candidate_storage_json"]["ordinal"] == 7
    assert harness.header["candidate_storage_json"]["resets"] == {"Location": 6}


@pytest.mark.asyncio
async def test_failed_copy_can_resume_after_native_sequence_gap(harness):
    harness.connection.fetchval.side_effect = [1, 3]
    harness.connection.copy_records_to_table.side_effect = RuntimeError("COPY rejected")
    with pytest.raises(RuntimeError, match="COPY rejected"):
        await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    assert harness.header["candidate_storage_json"]["ordinal"] == 0
    harness.connection.copy_records_to_table.side_effect = None
    harness.connection.copy_records_to_table.return_value = "COPY 1"
    await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, _rows(1))
    assert harness.header["candidate_storage_json"]["ordinal"] == 3


@pytest.mark.asyncio
async def test_partition_and_resource_rows_share_one_family_lock(harness):
    rows = [*_rows(1), {**_rows(1)[0], "resource_type": "LU:Organization:pass:2"}]
    await storage.append_generic_source_batch(object(), harness.directory, harness.candidate, rows)
    locks = [call for call in harness.connection.execute.await_args_list if "advisory" in call.args[0]]
    assert len(locks) == 1
    assert storage._encoded(["source_schema", "dataset", "Organization"]) in locks[0].args[1]
