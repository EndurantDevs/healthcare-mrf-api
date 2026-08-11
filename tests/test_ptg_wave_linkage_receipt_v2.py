"""RSA linkage receipts over the independently re-derived persisted graph."""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest

from process import ptg_wave_outcomes as outcomes
from process.ptg_wave_outcome_contract import (
    _collection_digest,
    _record_digest,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    LINKAGE_RECEIPT_SCHEMA,
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    RETAINED_PRIVATE_KEY_FILES_ENV,
)
from process.ptg_wave_receipt_contract import (
    admission_receipt_mapping,
    ordinary_cutover_id,
)
from process.ptg_wave_state import canonical_json
from tests.test_control_import_waves import _KEY
from tests.test_ptg_wave_outcomes import (
    _Result,
    _Session,
    _ack,
    _install_transaction,
)
from tests.test_ptg_wave_receipt_authority import FIXED_KEY, _new_key
from tests.test_ptg_wave_v12_pristine_abandonment import _boundary


def _stable_graph():
    wave, intents, _runs, _admission = _boundary()
    records = [
        {
            "ordinal": intent.ordinal,
            "run_id": intent.run_id,
            "job_id": intent.job_id,
            "source_file_import_id": intent.source_file_import_id,
            "content_version": intent.content_version,
            "status": "succeeded",
            "snapshot_id": f"snapshot-{intent.ordinal}",
            "import_id": intent.source_file_import_id,
        }
        for intent in intents
    ]
    stable_nodes = [
        SimpleNamespace(
            **record,
            outcome_digest=_record_digest(record),
        )
        for record in records
    ]
    wave.state = "awaiting_linkage"
    wave.outcomes_digest = _collection_digest(
        "healthporta.ptg-wave.outcomes.v1",
        records,
    )
    return wave, intents, stable_nodes


def _active_keyring(monkeypatch) -> PTGWaveReceiptKeyring:
    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, "receipt-active")
    monkeypatch.setenv(
        ACTIVE_PRIVATE_KEY_FILE_ENV,
        str(FIXED_KEY.resolve()),
    )
    monkeypatch.delenv(RETAINED_PRIVATE_KEY_FILES_ENV, raising=False)
    return PTGWaveReceiptKeyring.from_environment()


async def _record(
    monkeypatch,
    wave,
    intents,
    stable,
    keyring,
    *,
    control_key=_KEY,
    ack_key=None,
):
    session = _Session(
        _Result(scalar=wave),
        _Result(rows=stable),
        _Result(rows=intents),
    )
    _install_transaction(monkeypatch, session)
    ack = _ack(
        wave,
        stable,
        key=control_key if ack_key is None else ack_key,
    )
    receipt = await outcomes.record_linkage_ack(
        wave.wave_id,
        ack,
        key=control_key,
        cutover_id=ordinary_cutover_id(wave.wave_id),
        receipt_key_id=wave.receipt_key_id,
        receipt_keyring=keyring,
        receipt_issued_at="2026-08-10T12:34:56.123456Z",
    )
    return receipt, ack, session


@pytest.mark.asyncio
async def test_control_token_rotation_does_not_wedge_v6_linkage(
    monkeypatch,
):
    wave, intents, stable = _stable_graph()
    original_signature_digest = wave.cohort_signature_digest
    rotated_control_key = "token-b-after-v6-admission"
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", rotated_control_key)

    receipt, ack, _session = await _record(
        monkeypatch,
        wave,
        intents,
        stable,
        _active_keyring(monkeypatch),
        control_key=rotated_control_key,
        ack_key=_KEY,
    )

    assert receipt["payload"]["cohort_signature_digest"] == (
        original_signature_digest
    )

    wave.state = "terminalizing"
    replay_session = _Session(
        _Result(scalar=wave),
        _Result(rows=stable),
        _Result(rows=intents),
    )
    _install_transaction(monkeypatch, replay_session)
    replay = await outcomes.record_linkage_ack(
        wave.wave_id,
        ack,
        key="token-c-after-linkage-persistence",
        cutover_id=ordinary_cutover_id(wave.wave_id),
        receipt_key_id=wave.receipt_key_id,
        receipt_keyring=_active_keyring(monkeypatch),
    )
    assert canonical_json(replay) == canonical_json(receipt)
    assert replay_session.flush_count == 0

    altered_ack = copy.deepcopy(ack)
    altered_ack["signature"] = "0" * 64
    altered_session = _Session(
        _Result(scalar=wave),
        _Result(rows=stable),
    )
    _install_transaction(monkeypatch, altered_session)
    with pytest.raises(
        outcomes.PTGWaveOutcomeConflict,
        match="conflicts with the first receipt",
    ):
        await outcomes.record_linkage_ack(
            wave.wave_id,
            altered_ack,
            key="token-c-after-linkage-persistence",
            cutover_id=ordinary_cutover_id(wave.wave_id),
            receipt_key_id=wave.receipt_key_id,
            receipt_keyring=_active_keyring(monkeypatch),
        )


@pytest.mark.asyncio
async def test_v6_linkage_signs_persists_and_replays_exact_bytes(monkeypatch):
    """Prove first-write linkage signing and byte-exact replay."""
    wave, intents, stable = _stable_graph()
    keyring = _active_keyring(monkeypatch)
    receipt, ack, session = await _record(
        monkeypatch,
        wave,
        intents,
        stable,
        keyring,
    )
    assert receipt["schema"] == LINKAGE_RECEIPT_SCHEMA
    assert receipt["key_id"] == wave.receipt_key_id
    assert receipt["payload"]["operation_id"] == wave.wave_id
    assert receipt["payload"]["cutover_id"] == ordinary_cutover_id(
        wave.wave_id
    )
    assert receipt["payload"]["outcomes_digest"] == wave.outcomes_digest
    assert receipt["payload"]["mapping_digest"] == ack["mapping_digest"]
    assert wave.linkage_receipt == receipt
    assert wave.linkage_receipt_payload_digest == receipt["payload_digest"]
    assert session.flush_count == 1
    first_bytes = canonical_json(receipt)
    wave.state = "terminalizing"
    replay_session = _Session(
        _Result(scalar=wave),
        _Result(rows=stable),
        _Result(rows=intents),
    )
    _install_transaction(monkeypatch, replay_session)
    replay = await outcomes.record_linkage_ack(
        wave.wave_id,
        ack,
        key="token-b-after-linkage-persistence",
        cutover_id=ordinary_cutover_id(wave.wave_id),
        receipt_key_id=wave.receipt_key_id,
        receipt_keyring=keyring,
    )
    assert canonical_json(replay) == first_bytes
    assert replay_session.flush_count == 0
    altered_ack = copy.deepcopy(ack)
    altered_ack["signature"] = "0" * 64
    altered_session = _Session(
        _Result(scalar=wave),
        _Result(rows=stable),
    )
    _install_transaction(monkeypatch, altered_session)
    with pytest.raises(
        outcomes.PTGWaveOutcomeConflict,
        match="conflicts with the first receipt",
    ):
        await outcomes.record_linkage_ack(
            wave.wave_id,
            altered_ack,
            key="token-b-after-linkage-persistence",
            cutover_id=ordinary_cutover_id(wave.wave_id),
            receipt_key_id=wave.receipt_key_id,
            receipt_keyring=keyring,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_binding", ["key", "cutover"])
async def test_v6_linkage_rejects_wrong_pinned_identity(
    monkeypatch,
    invalid_binding,
):
    wave, _intents, stable = _stable_graph()
    keyring = _active_keyring(monkeypatch)
    session = _Session(_Result(scalar=wave), _Result(rows=stable))
    _install_transaction(monkeypatch, session)

    with pytest.raises(
        outcomes.PTGWaveOutcomeConflict,
        match="stored key and cutover",
    ):
        await outcomes.record_linkage_ack(
            wave.wave_id,
            _ack(wave, stable, key=_KEY),
            key=_KEY,
            cutover_id=(
                "f" * 64
                if invalid_binding == "cutover"
                else ordinary_cutover_id(wave.wave_id)
            ),
            receipt_key_id=(
                "wrong-key"
                if invalid_binding == "key"
                else wave.receipt_key_id
            ),
            receipt_keyring=keyring,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper", ["row_digest", "collection_digest"])
async def test_v6_linkage_rederives_the_persisted_outcome_graph(
    monkeypatch,
    tamper,
):
    wave, _intents, stable = _stable_graph()
    if tamper == "row_digest":
        stable[0].outcome_digest = "f" * 64
    else:
        wave.outcomes_digest = "f" * 64
    session = _Session(_Result(scalar=wave), _Result(rows=stable))
    _install_transaction(monkeypatch, session)

    with pytest.raises(
        outcomes.PTGWaveOutcomeConflict,
        match="exact persisted outcome graph",
    ):
        await outcomes.record_linkage_ack(
            wave.wave_id,
            _ack(wave, stable, key=_KEY),
            key=_KEY,
            cutover_id=ordinary_cutover_id(wave.wave_id),
            receipt_key_id=wave.receipt_key_id,
            receipt_keyring=_active_keyring(monkeypatch),
        )


@pytest.mark.asyncio
async def test_v6_linkage_replay_rejects_cross_domain_forgery(monkeypatch):
    wave, intents, stable = _stable_graph()
    keyring = _active_keyring(monkeypatch)
    receipt, ack, _session = await _record(
        monkeypatch,
        wave,
        intents,
        stable,
        keyring,
    )
    wave.state = "terminalizing"
    wave.linkage_receipt = copy.deepcopy(receipt)
    wave.linkage_receipt["schema"] = ABANDONMENT_RECEIPT_SCHEMA
    replay_session = _Session(
        _Result(scalar=wave),
        _Result(rows=stable),
        _Result(rows=intents),
    )
    _install_transaction(monkeypatch, replay_session)

    with pytest.raises(PTGWaveReceiptAuthorityError):
        await outcomes.record_linkage_ack(
            wave.wave_id,
            ack,
            key=_KEY,
            cutover_id=ordinary_cutover_id(wave.wave_id),
            receipt_key_id=wave.receipt_key_id,
            receipt_keyring=keyring,
        )


@pytest.mark.asyncio
async def test_rotation_uses_retained_epoch_for_already_pinned_linkage(
    monkeypatch,
    tmp_path,
):
    wave, intents, stable = _stable_graph()
    new_key = _new_key(tmp_path / "new-active.pem")
    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, "receipt-new")
    monkeypatch.setenv(ACTIVE_PRIVATE_KEY_FILE_ENV, str(new_key.resolve()))
    monkeypatch.setenv(
        RETAINED_PRIVATE_KEY_FILES_ENV,
        json.dumps({"receipt-active": str(FIXED_KEY.resolve())}),
    )
    rotating = PTGWaveReceiptKeyring.from_environment()

    receipt, _ack_value, _session = await _record(
        monkeypatch,
        wave,
        intents,
        stable,
        rotating,
    )

    assert rotating.active_key_id == "receipt-new"
    assert receipt["key_id"] == "receipt-active"
    assert rotating.validate_stored_receipt(
        receipt,
        schema=LINKAGE_RECEIPT_SCHEMA,
        key_id="receipt-active",
        expected_payload=receipt["payload"],
    ) == receipt


def test_v6_linkage_payload_rebuilds_exact_frozen_admission():
    wave, intents, _stable = _stable_graph()
    admission = admission_receipt_mapping(
        wave,
        intents,
    )
    assert admission["receipt_key_id"] == wave.receipt_key_id
    assert admission["wave_id"] == wave.wave_id
