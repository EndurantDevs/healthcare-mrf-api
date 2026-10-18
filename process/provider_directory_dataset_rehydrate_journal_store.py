# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original-session, complete-member checkpoint locks and exact native row CAS."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass

from sqlalchemy import text

from process.provider_directory_dataset_rehydrate_journal import _require, merge_completed_histories, output_journal
from process.provider_directory_dataset_rehydrate_scope import _load_dataset_scope
from process.provider_directory_dataset_rehydrate_types import CHECKPOINT_TABLE, _table_ref
from process.provider_directory_output_snapshot_catalog import decoded, encoded
from process.provider_directory_source_checkpoint_control import (
    require_checkpoint_executor,
    source_checkpoint_transaction,
)


@dataclass(frozen=True)
class CheckpointOutputJournal:
    """Complete existing checkpoint members for one exact pinned SOURCE operation."""

    runtime: object
    request: object
    scope: object
    expected_by_resource: tuple[tuple[str, int], ...]
    verified_retry_of_run_id: str | None = None

    @property
    def members(self):
        """Return the canonical complete resource checkpoint key set."""
        return tuple(resource for resource, _ in self.expected_by_resource)


@dataclass(frozen=True)
class OutputJournalSnapshot:
    """Native member witnesses valid only in their issuing original transaction."""

    rows: tuple
    journal: dict
    session: object
    task: object
    transaction_id: str


def _coordinates(journal):
    _require(
        type(journal) is CheckpointOutputJournal
        and 1 <= len(journal.members) <= 8
        and list(journal.members) == sorted(set(journal.members))
        and set(journal.members) <= set(journal.scope.resource_types)
        and all(type(count) is int and count >= 0 for _, count in journal.expected_by_resource),
        "members_invalid",
    )
    return dict(
        source_id=journal.scope.source_id,
        dataset_id=journal.scope.dataset_id,
        root_run_id=journal.scope.acquisition_root_run_id,
        resources=list(journal.members),
    )


async def _initialize(journal, session):
    table = _table_ref(journal.runtime.schema, CHECKPOINT_TABLE)
    await session.execute(
        text(f"""INSERT INTO {table} (
          source_id,dataset_id,acquisition_root_run_id,resource_type,endpoint_id,dataset_hash,owner_run_id,
          state,expected_input_count,created_at,started_at,updated_at
        ) SELECT :source_id,:dataset_id,:root_run_id,m.resource_type,:endpoint_id,:dataset_hash,:owner_run_id,
          'running',m.expected_count,now(),now(),now()
        FROM jsonb_to_recordset(CAST(:members AS jsonb)) m(resource_type text,expected_count bigint)
        ON CONFLICT(source_id,dataset_id,acquisition_root_run_id,resource_type) DO NOTHING"""),
        {
            **_coordinates(journal),
            "endpoint_id": journal.scope.endpoint_id,
            "dataset_hash": journal.scope.dataset_hash,
            "owner_run_id": journal.request.owner_run_id,
            "members": encoded(
                [dict(resource_type=resource, expected_count=count) for resource, count in journal.expected_by_resource]
            ),
        },
    )


async def _lock_members(journal, session):
    table = _table_ref(journal.runtime.schema, CHECKPOINT_TABLE)
    checkpoint_rows = (
        (
            await session.execute(
                text(f"""SELECT c.resource_type,c.endpoint_id,c.dataset_hash,c.expected_input_count,c.owner_run_id,
                     c.evidence_json,to_jsonb(c)::text AS witness
              FROM {table} c WHERE c.source_id=:source_id AND c.dataset_id=:dataset_id
                AND c.acquisition_root_run_id=:root_run_id AND c.resource_type=ANY(CAST(:resources AS varchar[]))
              ORDER BY c.resource_type COLLATE "C" FOR UPDATE NOWAIT"""),
                _coordinates(journal),
            )
        )
        .mappings()
        .all()
    )
    _require(
        tuple(checkpoint_row["resource_type"] for checkpoint_row in checkpoint_rows) == journal.members,
        "members_changed",
    )
    expected_by_resource = dict(journal.expected_by_resource)
    _require(
        all(
            checkpoint_row["endpoint_id"] == journal.scope.endpoint_id
            and checkpoint_row["dataset_hash"] == journal.scope.dataset_hash
            and checkpoint_row["expected_input_count"] == expected_by_resource[checkpoint_row["resource_type"]]
            for checkpoint_row in checkpoint_rows
        ),
        "checkpoint_scope_changed",
    )
    journals = [output_journal(decoded(checkpoint_row["evidence_json"])) for checkpoint_row in checkpoint_rows]
    merged = merge_completed_histories(journals)
    if merged["active"] is not None:
        owners = {checkpoint_row["owner_run_id"] for checkpoint_row in checkpoint_rows}
        _require(
            len(owners) == 1
            and (
                owners == {journal.request.owner_run_id}
                or journal.verified_retry_of_run_id is not None
                and owners == {journal.verified_retry_of_run_id}
                and journal.verified_retry_of_run_id != journal.request.owner_run_id
            ),
            "retry_owner_changed",
        )
    transaction_id = (await session.execute(text("SELECT pg_current_xact_id()::text"))).scalar_one()
    return OutputJournalSnapshot(
        tuple(dict(checkpoint_row) for checkpoint_row in checkpoint_rows),
        merged,
        session,
        asyncio.current_task(),
        transaction_id,
    )


@asynccontextmanager
async def checkpoint_output_transaction(journal):
    """Nest in the same output transaction, or open one short genuine Factory transaction."""
    _coordinates(journal)
    async with source_checkpoint_transaction(journal.runtime.database, schema=journal.runtime.schema) as session:
        await session.execute(text("SET LOCAL lock_timeout='100ms'"))
        fresh = await _load_dataset_scope(journal.runtime, journal.request, lock=True)
        _require(fresh.fence_identity == journal.scope.fence_identity, "source_changed")
        await _initialize(journal, session)
        yield session, await _lock_members(journal, session)


async def verify_retry_predecessor(runtime, request):
    """Read the admitted control row before opening a pinned SOURCE operation."""
    predecessor = request.retry_of_run_id
    if predecessor is None:
        return None
    from process.ptg_parts.ptg_source_attempt_actions import _TERMINAL_OUTER_STATUSES, _load_outer_run

    _require(
        type(predecessor) is str
        and bool(predecessor)
        and predecessor != request.owner_run_id
        and runtime.database._transaction_binding() is None,
        "retry_lineage_changed",
    )
    async with runtime.database.transaction() as session:
        table = _table_ref(runtime.schema, "import_run")
        child = await _load_outer_run(session, outer_run_table=table, run_id=request.owner_run_id, lock_row=False)
        parent = await _load_outer_run(session, outer_run_table=table, run_id=predecessor, lock_row=False)
        _require(
            child is not None
            and parent is not None
            and child["run_id"] == request.owner_run_id
            and child["importer"] == "provider-directory-fhir"
            and child["status"] == "running"
            and child["retry_of_run_id"] == predecessor
            and parent["run_id"] == predecessor
            and parent["importer"] == child["importer"]
            and parent["status"] in _TERMINAL_OUTER_STATUSES,
            "retry_lineage_changed",
        )
    return predecessor


async def preparation_operation(journal, operation, targets):
    """Retain the stable original operation after exact authorized member ownership checks."""
    async with checkpoint_output_transaction(journal) as (_, snapshot):
        active = snapshot.journal["active"]
        if active is None:
            return operation
        _require(
            {key: value for key, value in operation.items() if key != "run_id"}
            == {key: value for key, value in active["operation"].items() if key != "run_id"}
            and list(targets) == active["targets"]
            and list(journal.members) == active["members"],
            "active_conflict",
        )
        return deepcopy(active["operation"])


def _preserve_history(before, after):
    for digest, generation in before["generations"].items():
        replacement = after["generations"].get(digest)
        _require(replacement is not None, "generation_removed")
        _require(
            all(replacement[key] == generation[key] for key in ("operation", "targets", "members", "prepared"))
            and (generation["published"] is None or replacement == generation),
            "generation_changed",
        )
    if before["active"] is not None and after["active"] is None:
        digest = before["active"]["operation_digest"]
        _require(after["generations"].get(digest, {}).get("published") is not None, "active_removed")
    elif before["active"] is not None:
        old, new = before["active"], after["active"]
        _require(
            all(old[key] == new[key] for key in ("operation_digest", "operation", "targets", "members"))
            and set(old["stages"]) <= set(new["stages"]),
            "active_conflict",
        )
        for target, entry in old["stages"].items():
            replacement = new["stages"][target]
            _require(entry["stage"] == replacement["stage"] and entry["rows"] <= replacement["rows"], "stage_changed")
            _require(entry["receipt"] is None or entry == replacement, "sealed_stage_changed")


def _complete_proof(proof, expected):
    required_by_field = dict(
        input=expected,
        typed=expected,
        typed_extra=0,
        canonical_hash_matched=expected,
        canonical_extra=0,
        source_edges=expected,
        source_edge_extra=0,
    )
    _require(
        type(proof) is dict and proof == required_by_field and all(type(value) is int for value in proof.values()),
        "proof_incomplete",
    )


async def _publication_proofs(journal, snapshot, journal_value, capability, completed):
    published_generations = [
        (digest, generation)
        for digest, generation in journal_value["generations"].items()
        if generation["published"] is not None
        and snapshot.journal["generations"].get(digest, {}).get("published") is None
    ]
    _require(len(published_generations) <= 1, "generation_changed")
    if not published_generations:
        _require(completed is None, "preparation_incomplete")
        return None
    from process.provider_directory_output_snapshot_custody import _require_capability

    await _require_capability(capability)
    digest, generation = published_generations[0]
    _require(
        capability.session is snapshot.session
        and capability.operation_digest == digest
        and capability.targets == tuple(generation["targets"])
        and capability.reference == generation["operation"].get("rollback_of"),
        "publication_changed",
    )
    proofs_by_resource = generation["proofs"]
    _require(set(proofs_by_resource) == set(journal.members), "members_changed")
    _require(completed == proofs_by_resource if capability.reference is None else completed is None, "proof_changed")
    expected_by_resource = dict(journal.expected_by_resource)
    for resource, proof in proofs_by_resource.items():
        keys = {
            "input",
            "typed",
            "typed_extra",
            "canonical_hash_matched",
            "canonical_extra",
            "source_edges",
            "source_edge_extra",
        }
        _require(
            type(proof) is dict
            and set(proof) == keys
            and proof["input"] == expected_by_resource[resource]
            and all(type(count) is int and count >= 0 for count in proof.values()),
            "proof_incomplete",
        )
        if capability.reference is None:
            _complete_proof(proof, expected_by_resource[resource])
    return proofs_by_resource


def _member_changes(snapshot, journal_value, proofs_by_resource):
    changes = []
    for checkpoint_row in snapshot.rows:
        evidence_by_name = {**decoded(checkpoint_row["evidence_json"]), "output_snapshots": journal_value}
        proof = proofs_by_resource[checkpoint_row["resource_type"]] if proofs_by_resource is not None else None
        state = None
        if proof is not None:
            evidence_by_name["proof"] = proof
            state = (
                "complete"
                if (
                    proof["typed"] == proof["canonical_hash_matched"] == proof["source_edges"] == proof["input"]
                    and proof["typed_extra"] == proof["canonical_extra"] == proof["source_edge_extra"] == 0
                )
                else "proof_failed"
            )
        changes.append(
            dict(
                resource_type=checkpoint_row["resource_type"],
                witness=checkpoint_row["witness"],
                evidence=encoded(evidence_by_name),
                state=state,
                input_count=proof["input"] if proof is not None else None,
                mapped_count=proof["typed"] if proof is not None else None,
            )
        )
    return changes


async def write_output_journal(journal, snapshot, journal_value, *, completed=None, capability=None):
    """CAS every original native member row; preserve all nonprotocol evidence."""
    publisher = await require_checkpoint_executor(journal.runtime.database)
    _require(publisher.policy.schema == journal.runtime.schema, "checkpoint_scope_changed")
    journal_value = output_journal({"output_snapshots": journal_value})
    _preserve_history(snapshot.journal, journal_value)
    proofs_by_resource = await _publication_proofs(journal, snapshot, journal_value, capability, completed)
    changes = _member_changes(snapshot, journal_value, proofs_by_resource)
    from process.provider_directory_generic_source_policy import source_copy_session

    session = source_copy_session()
    _require(
        snapshot.session is session
        and snapshot.task is asyncio.current_task()
        and (await session.execute(text("SELECT pg_current_xact_id()::text"))).scalar_one() == snapshot.transaction_id,
        "checkpoint_transaction_changed",
    )
    table = _table_ref(journal.runtime.schema, CHECKPOINT_TABLE)
    updated_resources = (
        (
            await session.execute(
                text(f"""UPDATE {table} c SET evidence_json=CAST(m.evidence AS jsonb),updated_at=now(),
              owner_run_id=:owner_run_id,
              state=COALESCE(m.state,c.state),
              error=CASE WHEN m.state IS NULL THEN c.error ELSE NULL END,
              input_count=COALESCE(m.input_count,c.input_count),mapped_count=COALESCE(m.mapped_count,c.mapped_count),
              rejected_count=CASE WHEN m.input_count IS NULL THEN c.rejected_count ELSE 0 END,
              completed_at=CASE WHEN m.state IS NULL THEN c.completed_at WHEN m.state='complete' THEN now() ELSE NULL END
            FROM jsonb_to_recordset(CAST(:changes AS jsonb))
              m(resource_type text,witness text,evidence text,state text,input_count bigint,mapped_count bigint)
            WHERE c.source_id=:source_id AND c.dataset_id=:dataset_id AND c.acquisition_root_run_id=:root_run_id
              AND c.resource_type=m.resource_type AND to_jsonb(c)=CAST(m.witness AS jsonb)
            RETURNING c.resource_type"""),
                {**_coordinates(journal), "owner_run_id": journal.request.owner_run_id, "changes": encoded(changes)},
            )
        )
        .scalars()
        .all()
    )
    _require(sorted(updated_resources) == list(journal.members), "checkpoint_cas_failed")
