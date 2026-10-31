# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared batch semantics; native custody is covered by workspace fixtures."""

import asyncio
import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import provider_directory_cms_identity_candidate as common
from process import provider_directory_cms_prepared_identity as prepared
from process import provider_directory_generic_source_policy as source
from process.provider_directory_cms_prepared_workspace import OUTPUT_ROOTS, CMSPreparedWorkspace
from process.provider_directory_generic_source_storage import _SUFFIXES
from process.provider_directory_resource_identity import source_resource_uuid

STAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Transaction:
    def __init__(self, connection):
        self.connection = connection

    async def start(self):
        self.before = deepcopy(self.connection.rows)
        self.role = self.connection.role
        self.connection.transactions.append(self)

    async def rollback(self):
        self.connection.rows = self.before
        self.connection.role = self.role
        assert self.connection.transactions.pop() is self
        self.connection.outcomes.append("rollback")

    async def commit(self):
        assert self.connection.transactions.pop() is self
        self.connection.outcomes.append("commit")


class Connection:
    def __init__(self):
        self.rows, self.reads, self.copies, self.statements = {}, [], [], []
        self.outcomes, self.transactions = [], []
        self.role, self.closed = "prepared_publisher", False
        self.copy_error, self.copy_status, self.fail_copy = None, None, 0

    def transaction(self):
        return Transaction(self)

    def is_closed(self):
        return self.closed

    async def fetch(self, query, source_id, resource_type, ids, *release):
        assert self.role == "prepared_publisher"
        self.reads.append((query, (source_id, resource_type, ids, *release)))
        schema, name = re.search(r'FROM "([a-z0-9_]+)"\."([a-z0-9_]+)"', query).groups()
        return deepcopy(
            [
                row
                for row in self.rows.get((schema, name), [])
                if row["source_id"] == source_id
                and row["resource_type"] == resource_type
                and row["resource_id"] in ids
                and (not release or row["release_id"] == release[0])
            ]
        )

    async def execute(self, query):
        self.statements.append(query)
        assert query in {'SET LOCAL ROLE "prepared_owner"', "RESET ROLE"}
        self.role = "prepared_owner" if query.startswith("SET") else "prepared_publisher"

    async def copy_records_to_table(self, name, *, schema_name, columns, records):
        assert self.role == "prepared_owner" and schema_name == "private_output" and name.startswith("pdgs_")
        self.copies.append((schema_name, name, tuple(columns), deepcopy(records)))
        self.rows.setdefault((schema_name, name), []).extend(dict(zip(columns, row, strict=True)) for row in records)
        if len(self.copies) == self.fail_copy:
            raise self.copy_error
        return self.copy_status or f"COPY {len(records)}"

    def terminate(self):
        self.rows = self.transactions[0].before
        self.transactions.clear()
        self.closed = True


class Workspace(CMSPreparedWorkspace):
    def __init__(self):
        self._readonly = False
        self.connection, self.session = Connection(), SimpleNamespace(invalidate=AsyncMock())
        self.policy = source.GenericSourcePolicy("private_output", "prepared_owner", "prepared_publisher", ("reader",))
        self.source_binding = SimpleNamespace(catalog_schema="catalog")
        self._binding = {
            "identity": {
                "dataset_id": "candidate-one",
                "metadata": {
                    "source_ids": ["cms-npd"],
                    "source_release": {"source_id": "cms-npd", "vector_sha256": "a" * 64},
                    "network_bindings": {"producer_id": "cms-npd-bulk-fhir-v1", "alias_scope": "cms-npd"},
                },
            }
        }
        self._receipts = {
            root: {
                "name": "pdgs_" + "1" * 32 + "_" + _SUFFIXES[root],
                "oid": n + 1,
                "owner": 5,
                "layout_sha256": "b" * 64,
            }
            for n, root in enumerate(OUTPUT_ROOTS)
        }
        self._indexed, self.refuse, self.verifications = False, False, 0

    async def verify_current(self):
        assert self.connection.role == "prepared_publisher"
        self.verifications += 1
        if self.refuse:
            raise ValueError("workspace_refused")
        return self.receipts

    def rows_for(self, root, *, canonical=False):
        key = ("catalog", root) if canonical else (self.policy.schema, self.receipts[root]["name"])
        return self.connection.rows.setdefault(key, [])


@pytest.fixture
def workspace():
    return Workspace()


def resource(kind, key, **extra):
    return {"resourceType": kind, "id": key, **extra}


def binding_row(kind, key, entity_id, created_at=STAMP):
    return dict(
        source_id="cms-npd",
        resource_type=kind,
        resource_id=key,
        organization_id=entity_id if kind == "Organization" else None,
        site_id=entity_id if kind == "Location" else None,
        created_at=created_at,
    )


def evidence_row(payload, *, observed_at=STAMP):
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return dict(
        source_id="cms-npd",
        resource_type=payload["resourceType"],
        resource_id=payload["id"],
        release_id="a" * 64,
        payload_sha256=hashlib.sha256(text.encode()).hexdigest(),
        payload_json=text,
        observed_at=observed_at,
    )


def resource_row(kind, key):
    return dict(
        source_id="cms-npd",
        resource_type=kind,
        resource_id=key,
        entity_id=source_resource_uuid("cms-npd", kind, key),
        created_at=STAMP,
    )


@pytest.mark.parametrize("kind", ["Organization", "Location"])
@pytest.mark.parametrize("count", [1, 125, 1000])
async def test_entity_batch_order_hashes_and_constant_reads(workspace, kind, count):
    resources = [resource(kind, f"r-{n}", name="Example é", address=[{"city": "Example"}]) for n in range(count)]
    if count < 1000:
        resources.append(resources[0])
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    result = await prepared.stage_prepared_entity_batch(workspace, resources=resources)
    assert result[:count] == [
        row["organization_id" if kind == "Organization" else "site_id"]
        for row in workspace.rows_for(common.CMS_IDENTITY_TABLES[0])
    ]
    assert len(set(result)) == count
    if count < 1000:
        assert result[-1] == result[0]
    assert len(workspace.connection.reads) == 4 and len(workspace.connection.copies) == 2
    assert workspace.rows_for(common.CMS_IDENTITY_TABLES[1]) == [
        {**evidence_row(payload), "observed_at": workspace.rows_for(common.CMS_IDENTITY_TABLES[1])[n]["observed_at"]}
        for n, payload in enumerate(resources[:count])
    ]
    assert all("SELECT" in query for query, _ in workspace.connection.reads)
    assert workspace.connection.role == "prepared_publisher"
    assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient


@pytest.mark.parametrize("kind", ["Organization", "Location"])
@pytest.mark.parametrize("canonical", [False, True])
async def test_entity_reuses_opaque_id_and_exact_timestamps(workspace, kind, canonical):
    payload = resource(kind, "existing", name="Example")
    opaque_id = uuid4()
    workspace.rows_for(common.CMS_IDENTITY_TABLES[0], canonical=canonical).append(
        binding_row(kind, "existing", opaque_id)
    )
    workspace.rows_for(common.CMS_IDENTITY_TABLES[1], canonical=canonical).append(evidence_row(payload))
    first = await prepared.stage_prepared_entity_batch(workspace, resources=[payload, payload])
    before = deepcopy(workspace.connection.rows)
    copies = len(workspace.connection.copies)
    again = await prepared.stage_prepared_entity_batch(workspace, resources=[payload])
    assert first == [opaque_id, opaque_id] and again == [opaque_id]
    assert before == workspace.connection.rows and len(workspace.connection.copies) == copies
    assert workspace.rows_for(common.CMS_IDENTITY_TABLES[0])[0]["created_at"] == STAMP
    assert workspace.rows_for(common.CMS_IDENTITY_TABLES[1])[0]["observed_at"] == STAMP


@pytest.mark.parametrize("canonical", [False, True])
async def test_entity_release_hash_conflict_has_no_partial_writes(workspace, canonical):
    workspace.rows_for(common.CMS_IDENTITY_TABLES[1], canonical=canonical).append(
        evidence_row(resource("Organization", "one", name="old"))
    )
    before = deepcopy(workspace.connection.rows)
    with pytest.raises(common.GenericCandidateError, match="release_payload_conflict"):
        await prepared.stage_prepared_entity_batch(workspace, resources=[resource("Organization", "one", name="new")])
    assert workspace.connection.rows == before and not workspace.connection.copies


async def test_entity_opaque_identity_conflict_requires_whole_reprepare(workspace):
    root = common.CMS_IDENTITY_TABLES[0]
    for canonical in (False, True):
        workspace.rows_for(root, canonical=canonical).append(binding_row("Organization", "one", uuid4()))
    with pytest.raises(common.CMSIdentityReprepareRequired, match="whole_candidate_reprepare"):
        await prepared.stage_prepared_entity_batch(workspace, resources=[resource("Organization", "one")])
    assert not workspace.connection.copies


@pytest.mark.parametrize(
    "root,column", [(common.CMS_IDENTITY_TABLES[0], "created_at"), (common.CMS_IDENTITY_TABLES[1], "observed_at")]
)
async def test_entity_existing_timestamp_drift_requires_reprepare(workspace, root, column):
    payload = resource("Organization", "one")
    row = binding_row("Organization", "one", uuid4()) if column == "created_at" else evidence_row(payload)
    workspace.rows_for(root).append(row)
    workspace.rows_for(root, canonical=True).append({**row, column: datetime(2025, 1, 1, tzinfo=timezone.utc)})
    with pytest.raises(common.CMSIdentityReprepareRequired, match="timestamp_conflict"):
        await prepared.stage_prepared_entity_batch(workspace, resources=[payload])
    assert not workspace.connection.copies


@pytest.mark.parametrize(
    "resources",
    [
        None,
        [],
        (),
        [None],
        ["bad"],
        [resource("Organization", "x")] * 1001,
        [resource("Organization", "one"), resource("Location", "two")],
        [resource("Organization", "same"), resource("Organization", "same", name="changed")],
    ],
)
async def test_invalid_entity_batch_never_reads_payload(workspace, resources):
    with pytest.raises(ValueError):
        await prepared.stage_prepared_entity_batch(workspace, resources=resources)
    assert not workspace.connection.reads and not workspace.connection.copies


async def test_entity_input_bytes_bound(workspace):
    payload = resource("Location", "one", name="é" * (16 * 1024 * 1024))
    with pytest.raises(ValueError, match="batch_bytes_invalid"):
        await prepared.stage_prepared_entity_batch(workspace, resources=[payload])
    assert not workspace.connection.reads and not workspace.connection.copies


@pytest.mark.parametrize("kind", ["InsurancePlan", "PractitionerRole"])
@pytest.mark.parametrize("count", [1, 125, 1000])
async def test_resource_identity_order_replay_and_constant_reads(workspace, kind, count):
    keys = [f"r-{n}" for n in range(count)]
    if count < 1000:
        keys += keys[:1]
    result = await prepared.stage_prepared_resource_identity_batch(workspace, resource_type=kind, resource_ids=keys)
    assert result == [source_resource_uuid("cms-npd", kind, key) for key in keys]
    assert len(workspace.connection.reads) == 2 and len(workspace.connection.copies) == 1
    before = deepcopy(workspace.connection.rows)
    again = await prepared.stage_prepared_resource_identity_batch(workspace, resource_type=kind, resource_ids=keys)
    assert again == result and workspace.connection.rows == before
    assert len(workspace.connection.copies) == 1


@pytest.mark.parametrize("kind", ["InsurancePlan", "PractitionerRole"])
async def test_resource_identity_reuses_canonical_timestamp(workspace, kind):
    root = prepared.RESOURCE_IDENTITY_ROOT
    row = resource_row(kind, "existing")
    workspace.rows_for(root, canonical=True).append(row)
    result = await prepared.stage_prepared_resource_identity_batch(
        workspace, resource_type=kind, resource_ids=["new", "existing", "new"]
    )
    assert result == [source_resource_uuid("cms-npd", kind, key) for key in ["new", "existing", "new"]]
    assert workspace.rows_for(root)[1] == row and workspace.rows_for(root, canonical=True) == [row]


@pytest.mark.parametrize("canonical", [False, True])
async def test_resource_identity_conflict_refuses_before_copy(workspace, canonical):
    row = resource_row("InsurancePlan", "one")
    row["entity_id"] = uuid4()
    workspace.rows_for(prepared.RESOURCE_IDENTITY_ROOT, canonical=canonical).append(row)
    with pytest.raises(common.CMSIdentityReprepareRequired, match="immutable_replay_conflict"):
        await prepared.stage_prepared_resource_identity_batch(
            workspace, resource_type="InsurancePlan", resource_ids=["one"]
        )
    assert not workspace.connection.copies


async def test_resource_timestamp_drift_refuses_instead_of_rewriting(workspace):
    root = prepared.RESOURCE_IDENTITY_ROOT
    workspace.rows_for(root).append(resource_row("PractitionerRole", "one"))
    row = resource_row("PractitionerRole", "one")
    row["created_at"] = datetime(2025, 1, 1, tzinfo=timezone.utc)
    workspace.rows_for(root, canonical=True).append(row)
    before = deepcopy(workspace.connection.rows)
    with pytest.raises(common.CMSIdentityReprepareRequired, match="timestamp_conflict"):
        await prepared.stage_prepared_resource_identity_batch(
            workspace, resource_type="PractitionerRole", resource_ids=["one"]
        )
    assert workspace.connection.rows == before and not workspace.connection.copies


async def test_resource_duplicate_lookup_refuses_ambiguity(workspace):
    row = resource_row("InsurancePlan", "one")
    workspace.rows_for(prepared.RESOURCE_IDENTITY_ROOT).extend([row, row])
    with pytest.raises(common.CMSIdentityReprepareRequired, match="lookup_ambiguous"):
        await prepared.stage_prepared_resource_identity_batch(
            workspace, resource_type="InsurancePlan", resource_ids=["one", "two"]
        )
    assert not workspace.connection.copies


@pytest.mark.parametrize(
    "kind,keys",
    [
        ("Organization", ["one"]),
        ([], ["one"]),
        ("InsurancePlan", None),
        ("InsurancePlan", []),
        ("InsurancePlan", ("one",)),
        ("InsurancePlan", ["one"] * 1001),
        ("InsurancePlan", [None]),
        ("InsurancePlan", [[]]),
        ("InsurancePlan", ["bad_id"]),
        ("InsurancePlan", ["a" * 65]),
    ],
)
async def test_invalid_resource_batch_never_reads_payload(workspace, kind, keys):
    with pytest.raises(ValueError):
        await prepared.stage_prepared_resource_identity_batch(workspace, resource_type=kind, resource_ids=keys)
    assert not workspace.connection.reads and not workspace.connection.copies


@pytest.mark.parametrize("entity", [False, True])
async def test_workspace_refusal_precedes_payload_sql(workspace, entity):
    workspace.refuse = True
    with pytest.raises(ValueError, match="workspace_refused"):
        if entity:
            await prepared.stage_prepared_entity_batch(workspace, resources=[resource("Organization", "one")])
        else:
            await prepared.stage_prepared_resource_identity_batch(
                workspace, resource_type="InsurancePlan", resource_ids=["one"]
            )
    assert not workspace.connection.reads and not workspace.connection.copies
    assert not workspace.connection.transactions


@pytest.mark.parametrize("failure", [RuntimeError("copy failed"), TimeoutError(), asyncio.CancelledError()])
async def test_second_entity_copy_failure_rolls_back_first_and_preserves_prior_rows(workspace, failure):
    await prepared.stage_prepared_entity_batch(workspace, resources=[resource("Location", "retained")])
    before = deepcopy(workspace.connection.rows)
    workspace.connection.fail_copy = len(workspace.connection.copies) + 2
    workspace.connection.copy_error = failure
    with pytest.raises(type(failure)):
        await prepared.stage_prepared_entity_batch(workspace, resources=[resource("Organization", "new")])
    assert workspace.connection.rows == before
    assert not workspace.connection.transactions
    if isinstance(failure, (TimeoutError, asyncio.CancelledError)):
        assert workspace.connection.closed
        workspace.session.invalidate.assert_awaited_once()
    else:
        assert workspace.connection.role == "prepared_publisher"


async def test_incomplete_copy_status_rolls_back(workspace):
    workspace.connection.copy_status = "COPY 0"
    with pytest.raises(common.GenericCandidateError, match="copy_incomplete"):
        await prepared.stage_prepared_resource_identity_batch(
            workspace, resource_type="InsurancePlan", resource_ids=["one"]
        )
    assert not workspace.connection.rows and workspace.connection.role == "prepared_publisher"


async def test_typed_copy_retains_byte_and_row_bounds(workspace, monkeypatch):
    monkeypatch.setattr(common, "COPY_MAX_ROWS", 2)
    monkeypatch.setattr(common, "COPY_MAX_BYTES", 250)
    await prepared.stage_prepared_resource_identity_batch(
        workspace, resource_type="InsurancePlan", resource_ids=[f"r-{n}" for n in range(5)]
    )
    assert [len(copy[3]) for copy in workspace.connection.copies] == [2, 2, 1]
    for _, _, columns, records in workspace.connection.copies:
        assert 21 + sum(2 + sum(4 + common._copy_value_bytes(value) for value in row) for row in records) <= 250
        assert columns == prepared.RESOURCE_IDENTITY_COLUMNS


async def test_single_copy_record_bound_leaves_no_binding(workspace, monkeypatch):
    monkeypatch.setattr(common, "COPY_MAX_BYTES", 220)
    with pytest.raises(common.GenericCandidateError, match="copy_record_too_large"):
        await prepared.stage_prepared_entity_batch(
            workspace, resources=[resource("Organization", "one", name="x" * 200)]
        )
    assert not workspace.connection.rows


async def test_arbitrary_object_is_not_a_prepared_workspace():
    with pytest.raises(ValueError, match="workspace_required"):
        await prepared.stage_prepared_entity_batch(SimpleNamespace(), resources=[resource("Organization", "one")])
