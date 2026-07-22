# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_graph as candidate_graph
from api import ptg2_candidate_audit_reverse as reverse_scope
from api import ptg2_db_sidecars as sidecars
from api import ptg2_shared_blocks as shared_blocks
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError
from process.ptg_parts.ptg2_shared_blocks import shared_block_hash


class _Rows:
    def __init__(self, rows):
        self.rows = tuple(rows)

    def __iter__(self):
        return iter(self.rows)


class _GraphFallbackSession:
    def __init__(self):
        self.calls = []
        self.execute = self.execute_graph_query
        self.payload_by_kind = {
            "graph_npi_groups_v1": (4).to_bytes(4, "little"),
            "graph_provider_set_groups_v1": (4).to_bytes(4, "little"),
        }
        self.hash_by_kind = {
            object_kind: shared_block_hash(
                format_version=2,
                object_kind=object_kind,
                codec="none",
                payload=raw_payload,
            )
            for object_kind, raw_payload in self.payload_by_kind.items()
        }

    async def execute_graph_query(self, statement, params=None):
        statement_text = str(statement)
        params_by_name = dict(params or {})
        self.calls.append((statement_text, params_by_name))
        if "ptg2_v3_graph_owner" in statement_text:
            return _Rows(self._owner_rows(params_by_name))
        if "ptg2_v3_snapshot_block" in statement_text:
            object_kind = params_by_name["object_kind"]
            if object_kind not in self.payload_by_kind:
                raise AssertionError("oversized broad graph reached payload I/O")
            return _Rows(
                (
                    {
                        "object_kind": object_kind,
                        "block_key": 0,
                        "fragment_no": 0,
                        "mapping_entry_count": 1,
                        "block_hash": self.hash_by_kind[object_kind],
                    },
                )
            )
        if "ptg2_v3_block" in statement_text:
            requested_hash = tuple(params_by_name["block_hashes"])[0]
            object_kind = next(
                candidate_kind
                for candidate_kind, block_hash in self.hash_by_kind.items()
                if block_hash == requested_hash
            )
            raw_payload = self.payload_by_kind[object_kind]
            return _Rows(
                (
                    {
                        "block_hash": requested_hash,
                        "object_kind": object_kind,
                        "format_version": 2,
                        "codec": "none",
                        "block_entry_count": 1,
                        "raw_byte_count": len(raw_payload),
                        "stored_byte_count": len(raw_payload),
                        "payload": raw_payload,
                    },
                )
            )
        raise AssertionError(f"unexpected SQL: {statement_text}")

    @staticmethod
    def _owner_rows(params_by_name):
        direction = int(params_by_name["direction"])
        owner_key = int(tuple(params_by_name["owner_keys"])[0])
        selected_count = (
            shared_blocks.PTG2_V3_GRAPH_CHUNK_BYTES // 4 + 1
            if direction == shared_blocks.PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET
            else 1
        )
        return (
            {
                "owner_key": owner_key,
                "first_chunk": 0,
                "member_offset": 0,
                "member_count": selected_count,
                "selected_member_count": selected_count,
            },
        )


def _challenge():
    return AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=1234567890,
        source_artifact_key=0,
        tuple_digest="a" * 64,
        network_name_digests=("b" * 64,),
        multiplicity=1,
    )


def _serving_tables():
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=100,
        price_dictionary_block_bytes=2048,
    )


class _GraphLookupRecorder:
    def __init__(self, budget):
        self.budget = budget
        self.retention_by_direction = {}
        self.errors_by_direction = {}

    async def lookup_graph_members(
        self,
        lookup_session,
        snapshot_key,
        direction,
        owner_keys,
        **options,
    ):
        before_bytes = self.budget.retained_bytes
        try:
            return await sidecars.lookup_shared_graph_members_from_db(
                lookup_session,
                snapshot_key,
                direction,
                owner_keys,
                **options,
            )
        except BaseException as exc:
            self.errors_by_direction[direction] = exc
            raise
        finally:
            self.retention_by_direction[direction] = (
                before_bytes,
                self.budget.retained_bytes,
            )


class _BroadProviderScopeLookup:
    def __init__(self, recorder, schema_name):
        self.recorder = recorder
        self.schema_name = schema_name

    async def __call__(
        self,
        lookup_session,
        serving_tables,
        challenges,
        persisted_occurrences,
        *,
        retention_budget,
    ):
        assert retention_budget is self.recorder.budget
        return await candidate_graph.provider_set_keys_by_npi(
            self.recorder.lookup_graph_members,
            lookup_session,
            serving_tables.shared_snapshot_key,
            self.schema_name,
            challenges,
            persisted_occurrences,
            retention_budget=retention_budget,
        )


async def _load_scope_in_read_once(
    broad_scope_lookup,
    session,
    budget,
    challenge,
    code_index,
    schema_name,
):
    with shared_blocks.shared_block_read_once_scope(
        max_retained_raw_bytes=4
    ) as read_once_scope:
        shared_blocks.bind_shared_block_decoded_retention_budget(budget)
        observed = await reverse_scope.load_candidate_provider_scope(
            broad_scope_lookup,
            session,
            _serving_tables(),
            (challenge,),
            (),
            code_index,
            schema_name=schema_name,
            retention_budget=budget,
        )
        read_once_scope.assert_processed_once()
        return observed, read_once_scope.ledger


def _assert_overflow_translation_and_cleanup(recorder):
    direction = shared_blocks.PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET
    before_bytes, after_bytes = recorder.retention_by_direction[direction]
    assert before_bytes == after_bytes
    translated_error = recorder.errors_by_direction[direction]
    assert isinstance(translated_error, ManifestReadLimitError)
    assert isinstance(
        translated_error.__cause__,
        shared_blocks.SharedGraphReadLimitError,
    )


def _assert_read_once_ledger(session, ledger, schema_name):
    assert ledger["physical_block_reads"] == 2
    assert ledger["physical_block_decodes"] == 2
    assert ledger["repeated_physical_reads"] == 0
    assert ledger["repeated_physical_decodes"] == 0
    relevant_statements = [
        statement_text
        for statement_text, _params_by_name in session.calls
        if "ptg2_v3_" in statement_text
    ]
    assert relevant_statements
    assert all(schema_name in statement_text for statement_text in relevant_statements)


@pytest.mark.asyncio
async def test_real_graph_preflight_overflow_falls_back_in_same_scope(monkeypatch):
    challenge = _challenge()
    schema_name = "candidate_schema"
    session = _GraphFallbackSession()
    budget = CandidateAuditDecodedRetentionBudget()
    recorder = _GraphLookupRecorder(budget)
    broad_scope_lookup = _BroadProviderScopeLookup(recorder, schema_name)
    forward_lookup = AsyncMock(return_value={(7, 5, 0): (10,)})
    monkeypatch.setattr(
        reverse_scope,
        "lookup_forward_price_index_from_db",
        forward_lookup,
    )
    code_index = CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7},)},
        by_key={7: {"code_key": 7}},
    )

    observed, ledger = await _load_scope_in_read_once(
        broad_scope_lookup,
        session,
        budget,
        challenge,
        code_index,
        schema_name,
    )

    assert observed.provider_set_keys_by_npi == {challenge.npi: (5,)}
    assert observed.price_keys_by_occurrence == {(7, 5, 0): (10,)}
    assert forward_lookup.await_args.kwargs["schema_name"] == schema_name
    _assert_overflow_translation_and_cleanup(recorder)
    _assert_read_once_ledger(session, ledger, schema_name)
