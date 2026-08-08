# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Executable PostgreSQL 18/Python parity for the first persistence rows."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from public_evidence import evidence_record_contract as record_contract
from public_evidence import evidence_record_primitives as record_primitives
from public_evidence import record_persistence_candidate_contract as candidate_contract
from public_evidence import (
    record_persistence_candidate_primitives as candidate_primitives,
)
from tests import public_evidence_record_postgres_parity_support as postgres_parity
from tests.public_evidence_record_support import (
    enumeration_input,
    source_release,
)

GENERIC_PAYLOAD = {
    "z_text": 'quote:" backslash:\\ solidus:/ controls:\b\f\n\r\t unit:\x1f',
    "a_scalars": [None, False, True, -17, 0, 42],
    "m_nested": {"z": "omega", "a": "alpha"},
}
GENERIC_CANONICAL = b'{"a_scalars":[null,false,true,-17,0,42],"m_nested":{"a":"alpha","z":"omega"},"z_text":"quote:\\" backslash:\\\\ solidus:/ controls:\\b\\f\\n\\r\\t unit:\\u001f"}'

_FROZEN_NPI_VECTOR_LINES = (
    "normalized record|1707|f6e5c29b456d274f4ee4d6f1a1e14b9b7ba1b147acceeeb4086f97aa70198e97|402e4677009b593cb1fd6b6d550666e39368294ca1402544d03fae52f7d74518|evidence_record_contract|evidence_record|peev1_|peev1_Mj5oeZlNfveFjKpat1G0-VLMfnf47wSFEK50O9zpxJs",
    "typed NPI row|442|3240cd096c3561b928c241ffb97b7ab132a5de66916e33f95f54f636c9e7616d|e1edba22da2d7e715b5cfad3fabf9973f45aa1a43e7e32d5042123eb9521cbce|persistence_candidate_typed_row|||",
    "source-link row|415|fbe4bc880024f9c7258da241f0441fd1a5dbf0f2d3997f5606eda2e713bf170d|7faed7c20ff4da7186aaeb6a80a132b7a7ea5238e064ea9bbc141b57480c9018|persistence_candidate_source_link_row|||",
    "source-link vector|320|ef342fdaa864d3e99c218cf3c5ff3f3970821bc2b11f8204ef4468acd85da9e1|4b27ef8b65682e9cf0e26e3d47eccf34eb61ebf683f37e6e417b3e28a7ff6dc7|persistence_candidate_source_link_vector|||",
    "record authority|652|887100be36b25b3f126f6feec6f81d0972cc883b5e328df28c9361772f0a621a|80e382399459a46abe8ae1fcc50386d1812e465de9f61b7884ae82297bf0c2d2|persistence_candidate_record_authority_state|||",
    "common row|1281|e6160096a6764887b6928f80df63a2c8637deea9f757c72bb2c8fa62d9eb541e|018f379e719c1c9e9289cb062ce8bafd6d85fe869adf06fb4f169cb2349d766e|persistence_candidate_common_row|||",
    "candidate envelope|1659|cda27ce7301d0a1ba0cb4b20765850a62a212d7cb2e3772d0db70b5c2ee9d3d7|de4717e8ac99ea26039cbdf834ae9c4641d18b6bf73c2842168a1b8979afde91|persistence_candidate_contract|persistence_candidate|pepc1_|pepc1_ZNUNv6VH-mOpKEDaJlJWXQt8yqR_BuqL8QCBuddZ-X0",
)


@dataclass(frozen=True)
class FrozenPayload:
    """One released Python payload and its exact expected PostgreSQL result."""

    name: str
    payload: object
    digest_purpose: str
    canonical_length: int
    plain_sha256: str
    framed_digest: str
    reference_purpose: str | None = None
    reference_prefix: str = ""
    reference: str | None = None


GENERIC_FROZEN = FrozenPayload(
    "generic ASCII vector",
    GENERIC_PAYLOAD,
    "persistence_candidate_typed_row",
    152,
    "8adc48a3c0bd0293dcd411997571eabaf2d5414033bc0c28651491351a4f5092",
    "a6ceeb11b8c7012b81e9a64afe20e70ee36db63467ca5c370c5916f92a5967d5",
    "persistence_candidate",
    "pepc1_",
    "pepc1_5ScBPcdGY2uOvS59WAiDoRHgoR44Npk7u1FnS5GCsMg",
)


def _frame(domain: bytes, purpose: str, canonical_json: bytes) -> bytes:
    purpose_bytes = purpose.encode("ascii")
    return (
        domain
        + len(purpose_bytes).to_bytes(2, "big")
        + purpose_bytes
        + len(canonical_json).to_bytes(8, "big")
        + canonical_json
    )


def _record_payload(
    record: record_contract.PublicEvidenceRecord,
) -> dict[str, object]:
    return record_contract._record_payload(
        record.release,
        record.source_records,
        record.observed_at,
        record.effective_interval,
        record.record_type,
        record.evidence,
        record.authority_state,
    )


def _source_link_vector_payload(
    candidate: candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
) -> dict[str, object]:
    return {
        "ordering_contract_id": candidate_primitives.SOURCE_LINK_ORDERING_CONTRACT,
        "source_record_count": len(candidate.source_link_rows),
        "links": [
            {
                "source_record_ordinal": source_link.source_record_ordinal,
                "source_record_ref": source_link.source_record_ref,
                "row_sha256": source_link.row_sha256,
            }
            for source_link in candidate.source_link_rows
        ],
    }


def _npi_candidate() -> tuple[
    record_contract.PublicEvidenceRecord,
    candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
]:
    nppes_release = source_release("nppes_entity_address")
    record = record_contract.build_public_evidence_record(
        nppes_release,
        enumeration_input(nppes_release),
    )
    candidate = candidate_contract.build_public_evidence_record_persistence_candidate(
        record
    )
    return record, candidate


def _frozen_npi_payloads() -> tuple[
    candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
    tuple[FrozenPayload, ...],
]:
    evidence_record, candidate = _npi_candidate()
    canonical_candidates = (
        _record_payload(evidence_record),
        candidate_contract._row_payload(candidate.typed_row),
        candidate_contract._row_payload(candidate.source_link_rows[0]),
        _source_link_vector_payload(candidate),
        evidence_record.authority_state._asdict(),
        candidate_contract._row_payload(candidate.common_row),
        candidate_contract._candidate_payload(
            evidence_record,
            candidate.common_row,
            candidate.authority_state,
        ),
    )
    frozen_payloads = []
    for vector_line, canonical_candidate in zip(
        _FROZEN_NPI_VECTOR_LINES,
        canonical_candidates,
        strict=True,
    ):
        vector_fields = vector_line.split("|")
        name, length, plain, framed, purpose, ref_purpose, prefix, ref = vector_fields
        frozen_payloads.append(
            FrozenPayload(
                name,
                canonical_candidate,
                purpose,
                int(length),
                plain,
                framed,
                ref_purpose or None,
                prefix,
                ref or None,
            )
        )
    return candidate, tuple(frozen_payloads)


def _assert_normalized_error(caught: pytest.ExceptionInfo[BaseException]) -> None:
    assert type(caught.value) is (
        postgres_parity.PublicEvidenceRecordPostgresParityError
    )
    assert str(caught.value) == "public_evidence_record_postgres_parity_invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def _assert_frozen_vector(
    vector: postgres_parity.PostgresFramedVector,
    frozen: FrozenPayload,
    canonical_json: bytes,
) -> None:
    assert vector.canonical_json == canonical_json, frozen.name
    assert len(canonical_json) == frozen.canonical_length, frozen.name
    assert vector.plain_sha256 == frozen.plain_sha256, frozen.name
    assert vector.digest_frame == _frame(
        postgres_parity.DIGEST_DOMAIN,
        frozen.digest_purpose,
        canonical_json,
    ), frozen.name
    assert vector.framed_digest == frozen.framed_digest, frozen.name
    if frozen.reference_purpose is None:
        assert vector.reference_frame is None, frozen.name
        assert vector.reference is None, frozen.name
        return
    assert vector.reference_frame == _frame(
        postgres_parity.REFERENCE_DOMAIN,
        frozen.reference_purpose,
        canonical_json,
    ), frozen.name
    assert vector.reference == frozen.reference, frozen.name


async def _assert_supported_ascii_shapes(connection: object) -> None:
    supported_candidates = (
        {},
        [],
        "synthetic",
        True,
        False,
        -(1 << 63),
        (1 << 63) - 1,
        None,
        {"empty_array": [], "empty_object": {}},
    )
    for supported_candidate in supported_candidates:
        expected = json.dumps(
            supported_candidate,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        actual = await postgres_parity.postgres18_ascii_canonical_json(
            connection,
            supported_candidate,
        )
        assert actual == expected


@pytest.mark.asyncio
async def test_postgres18_ascii_json_and_domain_frames_match_python() -> None:
    python_canonical = json.dumps(
        GENERIC_PAYLOAD,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    assert python_canonical == GENERIC_CANONICAL
    digest_frame = _frame(
        postgres_parity.DIGEST_DOMAIN,
        GENERIC_FROZEN.digest_purpose,
        python_canonical,
    )
    reference_frame = _frame(
        postgres_parity.REFERENCE_DOMAIN,
        GENERIC_FROZEN.reference_purpose or "",
        python_canonical,
    )
    assert hashlib.sha256(digest_frame).hexdigest() == GENERIC_FROZEN.framed_digest
    token = base64.urlsafe_b64encode(hashlib.sha256(reference_frame).digest())
    assert (
        GENERIC_FROZEN.reference_prefix + token.rstrip(b"=").decode("ascii")
        == GENERIC_FROZEN.reference
    )
    async with postgres_parity.postgres18_readonly_connection() as connection:
        vector = await postgres_parity.postgres18_framed_vector(
            connection,
            GENERIC_PAYLOAD,
            digest_purpose=GENERIC_FROZEN.digest_purpose,
            reference_purpose=GENERIC_FROZEN.reference_purpose,
            reference_prefix=GENERIC_FROZEN.reference_prefix,
        )
        await _assert_supported_ascii_shapes(connection)
    _assert_frozen_vector(vector, GENERIC_FROZEN, GENERIC_CANONICAL)


@pytest.mark.asyncio
async def test_postgres18_npi_record_and_candidate_rows_match_python() -> None:
    candidate, frozen_payloads = _frozen_npi_payloads()
    postgres_parity.postgres18_require_supported_record_type(
        candidate.record.record_type
    )
    assert candidate.record.source_records[0].source_record_ref == (
        "pesr1_ypH9o9RxF2DJ-13SlqstfwysAPnpSQq3F9KxJ0SxySk"
    )
    assert candidate.record.evidence_ref == frozen_payloads[0].reference
    assert candidate.candidate_ref == frozen_payloads[-1].reference

    async with postgres_parity.postgres18_readonly_connection() as connection:
        for frozen in frozen_payloads:
            python_canonical = record_primitives._canonical_json(frozen.payload)
            vector = await postgres_parity.postgres18_framed_vector(
                connection,
                frozen.payload,
                digest_purpose=frozen.digest_purpose,
                reference_purpose=frozen.reference_purpose,
                reference_prefix=frozen.reference_prefix,
            )
            _assert_frozen_vector(vector, frozen, python_canonical)

    assert candidate.typed_row.row_sha256 == frozen_payloads[1].framed_digest
    assert candidate.source_link_rows[0].row_sha256 == frozen_payloads[2].framed_digest
    assert candidate.common_row.source_link_vector_sha256 == (
        frozen_payloads[3].framed_digest
    )
    assert (
        candidate.common_row.authority_state_sha256 == frozen_payloads[4].framed_digest
    )
    assert candidate.common_row.row_sha256 == frozen_payloads[5].framed_digest
    assert candidate.contract_sha256 == frozen_payloads[6].framed_digest


@pytest.mark.asyncio
async def test_postgres18_source_link_order_is_exact_and_fail_closed() -> None:
    ordered_references = [f"pesr1_{prefix}{'A' * 42}" for prefix in "-0A_a"]
    ordered_links = [
        {
            "source_record_ordinal": ordinal,
            "source_record_ref": reference,
            "row_sha256": f"{ordinal:064x}",
        }
        for ordinal, reference in enumerate(ordered_references)
    ]
    reversed_links = [
        {
            "source_record_ordinal": ordinal,
            "source_record_ref": reference,
            "row_sha256": f"{ordinal:064x}",
        }
        for ordinal, reference in enumerate(reversed(ordered_references))
    ]
    duplicate_links = [dict(ordered_links[0]), dict(ordered_links[0])]
    duplicate_links[1]["source_record_ordinal"] = 1

    async with postgres_parity.postgres18_readonly_connection() as connection:
        await postgres_parity.postgres18_validate_source_link_order(
            connection,
            ordered_links,
        )
        for invalid_links in (reversed_links, duplicate_links):
            with pytest.raises(
                postgres_parity.PublicEvidenceRecordPostgresParityError
            ) as caught:
                await postgres_parity.postgres18_validate_source_link_order(
                    connection,
                    invalid_links,
                )
            _assert_normalized_error(caught)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_payload",
    (
        {"source_reported_name": "Synthet\N{LATIN SMALL LETTER I WITH ACUTE}c"},
        {"text": "contains\x00nul"},
        {"text": "contains\x7fdel"},
        {"\x00key": "value"},
        {1: "non-string-key"},
        {"integer": 1 << 63},
        {"float": 1.5},
        {"tuple": ("not", "json")},
    ),
)
async def test_postgres18_ascii_boundary_rejects_unproven_values(
    invalid_payload: object,
) -> None:
    async with postgres_parity.postgres18_readonly_connection() as connection:
        with pytest.raises(
            postgres_parity.PublicEvidenceRecordPostgresParityError
        ) as caught:
            await postgres_parity.postgres18_ascii_canonical_json(
                connection,
                invalid_payload,
            )
    _assert_normalized_error(caught)
    assert "Synthet" not in str(caught.value)


def test_tax_name_persistence_parity_remains_unsupported() -> None:
    """Keep ASCII and Unicode tax-name rows behind the record-type gate."""

    with pytest.raises(
        postgres_parity.PublicEvidenceRecordPostgresParityError
    ) as caught:
        postgres_parity.postgres18_require_supported_record_type("tax_identity_name")
    _assert_normalized_error(caught)


@pytest.mark.asyncio
async def test_connection_failure_is_value_free_and_context_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = AsyncMock(side_effect=RuntimeError("PRIVATE-DSN-MARKER"))
    monkeypatch.setenv(
        postgres_parity.POSTGRES_DSN_ENV,
        "postgresql://synthetic@127.0.0.1/public_evidence_parity_test",
    )
    monkeypatch.setattr(postgres_parity.asyncpg, "connect", connect)
    with pytest.raises(
        postgres_parity.PublicEvidenceRecordPostgresParityError
    ) as caught:
        async with postgres_parity.postgres18_readonly_connection():
            pytest.fail("connection unexpectedly succeeded")
    _assert_normalized_error(caught)
    assert "PRIVATE-DSN-MARKER" not in repr(caught.value)
    connect.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe_dsn",
    (
        "postgresql://synthetic@127.0.0.1/production",
        "postgresql+psycopg://synthetic@127.0.0.1/synthetic_test",
        "postgresqlanything://synthetic@127.0.0.1/synthetic_test",
        "postgresql://127.0.0.1/synthetic_test",
    ),
)
async def test_unsafe_dsn_is_rejected_before_connection(
    monkeypatch: pytest.MonkeyPatch,
    unsafe_dsn: str,
) -> None:
    connect = AsyncMock(side_effect=AssertionError("unsafe connection attempt"))
    monkeypatch.setenv(postgres_parity.POSTGRES_DSN_ENV, unsafe_dsn)
    monkeypatch.setattr(postgres_parity.asyncpg, "connect", connect)
    with pytest.raises(
        postgres_parity.PublicEvidenceRecordPostgresParityError
    ) as caught:
        async with postgres_parity.postgres18_readonly_connection():
            pytest.fail("connection unexpectedly succeeded")
    _assert_normalized_error(caught)
    connect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_point",
    ("start", "boundary", "rollback", "close"),
)
async def test_session_failures_are_value_free_and_context_free(
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    private_error = RuntimeError(f"PRIVATE-{failure_point.upper()}-MARKER")
    transaction = SimpleNamespace(
        start=AsyncMock(
            side_effect=private_error if failure_point == "start" else None
        ),
        rollback=AsyncMock(
            side_effect=private_error if failure_point == "rollback" else None
        ),
    )
    boundary_by_field = {
        "postgres18": True,
        "utf8": True,
        "readonly": True,
        "no_temp_schema": True,
    }
    connection = SimpleNamespace(
        transaction=lambda **_kwargs: transaction,
        fetchrow=AsyncMock(
            side_effect=private_error if failure_point == "boundary" else None,
            return_value=boundary_by_field,
        ),
        is_in_transaction=lambda: True,
        close=AsyncMock(
            side_effect=private_error if failure_point == "close" else None
        ),
    )
    monkeypatch.setenv(
        postgres_parity.POSTGRES_DSN_ENV,
        "postgresql://synthetic@127.0.0.1/public_evidence_parity_test",
    )
    monkeypatch.setattr(
        postgres_parity.asyncpg,
        "connect",
        AsyncMock(return_value=connection),
    )
    with pytest.raises(
        postgres_parity.PublicEvidenceRecordPostgresParityError
    ) as caught:
        async with postgres_parity.postgres18_readonly_connection() as yielded:
            assert yielded is connection
    _assert_normalized_error(caught)
    assert "PRIVATE" not in repr(caught.value)


@pytest.mark.asyncio
async def test_user_body_failure_is_not_masked() -> None:
    body_error = RuntimeError("synthetic-user-body-failure")
    with pytest.raises(RuntimeError) as caught:
        async with postgres_parity.postgres18_readonly_connection():
            raise body_error
    assert caught.value is body_error


def test_postgres_parity_support_is_query_only_and_dormant() -> None:
    source = Path(postgres_parity.__file__).read_text(encoding="utf-8")
    upper_source = source.upper()
    for mutating_verb in "CREATE INSERT UPDATE DELETE ALTER DROP TRUNCATE".split():
        assert f"{mutating_verb} " not in upper_source
    assert "pg_my_temp_schema() = 0" in source
    assert "transaction(readonly=True)" in source
    assert "jsonb" not in source.lower()
