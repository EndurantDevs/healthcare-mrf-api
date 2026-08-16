"""Asymmetric receipt authority and frozen-envelope tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, literal, select
from sqlalchemy.dialects import postgresql

from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    KEY_EPOCHS_SCHEMA,
    LINKAGE_RECEIPT_SCHEMA,
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    PTGWaveReceiptPublicEpoch,
    RETAINED_PRIVATE_KEY_FILES_ENV,
    RETIRED_PUBLIC_EPOCHS_FILE_ENV,
    load_configured_receipt_keyring,
    require_nonterminal_signing_key_coverage,
    require_persisted_receipt_key_coverage,
    signed_receipt_message,
    validate_receipt_envelope,
)
from process.ptg_wave_receipt_key_coverage import (
    assert_nonterminal_receipt_key_coverage,
)
from process.ptg_wave_ordinary_terminal_receipt import (
    ORDINARY_TERMINAL_PAYLOAD_FIELDS,
    validate_ordinary_terminal_request,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v12_pristine_abandonment import (
    proof_signing_bytes,
    validate_v12_pristine_abandonment_proof,
)
from tests.ptg_wave_receipt_test_keys import (
    EPHEMERAL_RECEIPT_PRIVATE_KEY,
    write_ephemeral_receipt_private_key,
)
from tests.ptg_wave_receipt_authority_assertions import (
    assert_shared_receipt_fixture,
    build_rotating_and_historical_keyrings,
    pinned_receipt_epoch_rows,
)
ROOT = Path(__file__).resolve().parents[1]
FIXED_KEY = EPHEMERAL_RECEIPT_PRIVATE_KEY
SHARED_FIXTURE = ROOT / "tests" / "fixtures" / (
    "ptg_wave_receipts_v2.json"
)


def _configure_active(monkeypatch, key_id: str, path: Path) -> None:
    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, key_id)
    monkeypatch.setenv(ACTIVE_PRIVATE_KEY_FILE_ENV, str(path.resolve()))
    monkeypatch.delenv(RETAINED_PRIVATE_KEY_FILES_ENV, raising=False)
    monkeypatch.delenv(RETIRED_PUBLIC_EPOCHS_FILE_ENV, raising=False)


def _new_key(path: Path) -> Path:
    return write_ephemeral_receipt_private_key(path)


def _payload() -> dict[str, object]:
    return {
        "operation_id": "a" * 64,
        "cutover_id": "b" * 64,
        "wave_digest": "c" * 64,
        "intent_count": 2,
    }


def test_signs_exact_rsa_envelope_and_exposes_public_epoch(monkeypatch):
    _configure_active(monkeypatch, "test-epoch-2026-08", FIXED_KEY)
    keyring = PTGWaveReceiptKeyring.from_environment()

    receipt = keyring.sign_receipt(
        schema=LINKAGE_RECEIPT_SCHEMA,
        key_id="test-epoch-2026-08",
        issued_at="2026-08-10T12:34:56.123456Z",
        receipt_payload=_payload(),
    )

    assert set(receipt) == {
        "schema",
        "key_id",
        "issued_at",
        "payload",
        "payload_digest",
        "signature",
    }
    assert len(receipt["signature"]) == 512
    message = signed_receipt_message(
        schema=LINKAGE_RECEIPT_SCHEMA,
        key_id=receipt["key_id"],
        issued_at=receipt["issued_at"],
        payload=receipt["payload"],
    )
    assert receipt["payload_digest"] == sha256_digest(message)
    assert keyring.validate_stored_receipt(
        receipt,
        schema=LINKAGE_RECEIPT_SCHEMA,
        key_id="test-epoch-2026-08",
        expected_payload=_payload(),
    ) == receipt

    public = keyring.public_epochs_mapping()
    assert public["schema_version"] == KEY_EPOCHS_SCHEMA
    assert public["active_key_id"] == "test-epoch-2026-08"
    assert public["epochs"] == sorted(
        public["epochs"],
        key=lambda epoch: epoch["key_id"],
    )
    assert len(public["epochs"][0]["rsa_modulus"]) == 512
    assert public["epochs"][0]["rsa_exponent"] == 65537
    assert public["epochs"][0]["state"] == "active"


@pytest.mark.parametrize(
    "mutation",
    (
        lambda receipt: receipt.update(extra="forbidden"),
        lambda receipt: receipt.update(schema=ABANDONMENT_RECEIPT_SCHEMA),
        lambda receipt: receipt.update(issued_at="2026-08-10T12:34:56Z"),
        lambda receipt: receipt.update(payload_digest="0" * 64),
        lambda receipt: receipt.update(signature="0" * 512),
        lambda receipt: receipt["payload"].update(intent_count=3),
    ),
)
def test_rejects_extra_downgrade_cross_domain_digest_and_forgery(
    monkeypatch,
    mutation,
):
    _configure_active(monkeypatch, "test-epoch-2026-08", FIXED_KEY)
    keyring = PTGWaveReceiptKeyring.from_environment()
    receipt = keyring.sign_receipt(
        schema=LINKAGE_RECEIPT_SCHEMA,
        key_id="test-epoch-2026-08",
        issued_at="2026-08-10T12:34:56.123456Z",
        receipt_payload=_payload(),
    )
    mutated = copy.deepcopy(receipt)
    mutation(mutated)

    with pytest.raises(PTGWaveReceiptAuthorityError):
        validated = validate_receipt_envelope(
            mutated,
            schema=LINKAGE_RECEIPT_SCHEMA,
        )
        keyring.validate_stored_receipt(
            validated,
            schema=LINKAGE_RECEIPT_SCHEMA,
            key_id="test-epoch-2026-08",
            expected_payload=validated["payload"],
        )


def test_rotation_finishes_pinned_epoch_and_public_history_survives(
    monkeypatch,
    tmp_path,
):
    """Prove rotation retains pinned signing and historical verification."""
    _new_key_path, rotating, historical, _old_public = (
        build_rotating_and_historical_keyrings(
            monkeypatch,
            tmp_path,
            FIXED_KEY,
        )
    )
    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="fresh admission must pin the active",
    ):
        rotating.require_active_for_admission("epoch-old")
    old_receipt = rotating.sign_receipt(
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id="epoch-old",
        issued_at="2026-08-10T12:34:56.123456Z",
        receipt_payload=_payload(),
    )
    # Rotation may retire an epoch from fresh admission while retaining its
    # private key for already-pinned operations. Removing that private epoch
    # is rejected until all of those operations are terminal or quarantined.
    require_nonterminal_signing_key_coverage(
        ["epoch-old"],
        keyring=rotating,
    )
    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="nonterminal V12 wave receipt key epoch is unavailable",
    ):
        require_nonterminal_signing_key_coverage(
            ["epoch-old"],
            keyring=historical,
        )
    require_nonterminal_signing_key_coverage([], keyring=historical)

    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="unavailable for signing",
    ):
        historical.sign_receipt(
            schema=ABANDONMENT_RECEIPT_SCHEMA,
            key_id="epoch-old",
            issued_at="2026-08-10T12:34:56.123456Z",
            receipt_payload=_payload(),
        )
    assert historical.validate_stored_receipt(
        old_receipt,
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id="epoch-old",
        expected_payload=_payload(),
    ) == old_receipt


def test_rejects_epoch_alias_for_same_public_key(monkeypatch):
    _configure_active(monkeypatch, "epoch-active", FIXED_KEY)
    monkeypatch.setenv(
        RETAINED_PRIVATE_KEY_FILES_ENV,
        json.dumps({"epoch-alias": str(FIXED_KEY.resolve())}),
    )
    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="public key material must identify one epoch",
    ):
        PTGWaveReceiptKeyring.from_environment()


def test_active_admission_rejects_same_id_with_different_public_material(
    monkeypatch,
):
    _configure_active(monkeypatch, "epoch-active", FIXED_KEY)
    keyring = PTGWaveReceiptKeyring.from_environment()
    active = keyring.public_by_key_id["epoch-active"]

    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="public key material is not active",
    ):
        keyring.require_active_public_material(
            key_id="epoch-active",
            modulus="8" + "0" * 510 + "1",
            exponent=65537,
        )
    assert keyring.require_active_public_material(
        key_id="epoch-active",
        modulus=active.rsa_modulus,
        exponent=active.rsa_exponent,
    ) == active


def test_persisted_receipt_key_coverage_requires_public_history_and_open_signer(
    monkeypatch,
    tmp_path,
):
    """Prove persisted pins require public history and open-wave signers."""
    new_key, rotating, historical, old_public = (
        build_rotating_and_historical_keyrings(
            monkeypatch,
            tmp_path,
            FIXED_KEY,
        )
    )
    pinned_rows = pinned_receipt_epoch_rows(old_public)

    require_persisted_receipt_key_coverage(
        pinned_rows,
        ["epoch-old"],
        keyring=rotating,
    )

    require_persisted_receipt_key_coverage(
        pinned_rows,
        [],
        keyring=historical,
    )
    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="nonterminal V12 wave receipt key epoch is unavailable",
    ):
        require_persisted_receipt_key_coverage(
            pinned_rows,
            ["epoch-old"],
            keyring=historical,
        )

    _configure_active(monkeypatch, "epoch-new", new_key)
    active_only = PTGWaveReceiptKeyring.from_environment()
    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="persisted V12 receipt public key epoch is unavailable",
    ):
        require_persisted_receipt_key_coverage(
            pinned_rows,
            [],
            keyring=active_only,
        )

    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="public key material conflicts",
    ):
        require_persisted_receipt_key_coverage(
            [("epoch-old", "8" + "0" * 510 + "1", 65537)],
            [],
            keyring=historical,
        )


def test_key_configuration_rejects_duplicate_json_fields(
    monkeypatch,
    tmp_path,
):
    _configure_active(monkeypatch, "epoch-active", FIXED_KEY)
    monkeypatch.setenv(
        RETAINED_PRIVATE_KEY_FILES_ENV,
        '{"epoch-old":"/first.pem","epoch-old":"/second.pem"}',
    )
    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="duplicate object fields",
    ):
        PTGWaveReceiptKeyring.from_environment()

    public_file = tmp_path / "retired-public-duplicates.json"
    public_file.write_text(
        '[{"key_id":"epoch-old","key_id":"epoch-other",'
        '"rsa_modulus":"' + ("8" + "0" * 510 + "1") + '",'
        '"rsa_exponent":65537,"state":"retired"}]',
        encoding="utf-8",
    )
    monkeypatch.delenv(RETAINED_PRIVATE_KEY_FILES_ENV, raising=False)
    monkeypatch.setenv(
        RETIRED_PUBLIC_EPOCHS_FILE_ENV,
        str(public_file.resolve()),
    )
    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="duplicate object fields",
    ):
        PTGWaveReceiptKeyring.from_environment()


def test_server_process_pins_key_material_and_partial_config_fails(
    monkeypatch,
    tmp_path,
):
    for name in (
        ACTIVE_KEY_ID_ENV,
        ACTIVE_PRIVATE_KEY_FILE_ENV,
        RETAINED_PRIVATE_KEY_FILES_ENV,
        RETIRED_PUBLIC_EPOCHS_FILE_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    assert load_configured_receipt_keyring() is None

    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, "epoch-pinned")
    with pytest.raises(PTGWaveReceiptAuthorityError):
        load_configured_receipt_keyring()

    _configure_active(monkeypatch, "epoch-pinned", FIXED_KEY)
    pinned = load_configured_receipt_keyring()
    assert pinned is not None
    pinned_modulus = pinned.public_epochs_mapping()["epochs"][0][
        "rsa_modulus"
    ]

    replacement = _new_key(tmp_path / "replacement.pem")
    monkeypatch.setenv(ACTIVE_PRIVATE_KEY_FILE_ENV, str(replacement.resolve()))
    reloaded_modulus = PTGWaveReceiptKeyring.from_environment().public_epochs_mapping()[
        "epochs"
    ][0]["rsa_modulus"]
    assert reloaded_modulus != pinned_modulus
    assert pinned.public_epochs_mapping()["epochs"][0][
        "rsa_modulus"
    ] == pinned_modulus


def test_shared_fixture_is_canonical_and_verifies_exact_signing_bytes(
):
    """Prove the shared public fixture verifies canonical signing bytes."""
    assert_shared_receipt_fixture(ROOT, SHARED_FIXTURE)


class _CoverageRows:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows

    def scalars(self):
        return self


class _CoverageSession:
    def __init__(self, result_rows):
        self.result_rows = list(result_rows)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return _CoverageRows(self.result_rows.pop(0))


class _CoverageSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@pytest.mark.asyncio
async def test_startup_accepts_rows_returned_by_sqlalchemy(monkeypatch):
    from db.models import db

    _configure_active(monkeypatch, "epoch-active", FIXED_KEY)
    signer = PTGWaveReceiptKeyring.from_environment()
    public_epoch = signer.public_by_key_id["epoch-active"]
    with create_engine("sqlite://").connect() as connection:
        row = connection.execute(
            select(
                literal(public_epoch.key_id),
                literal(public_epoch.rsa_modulus),
                literal(public_epoch.rsa_exponent),
            )
        ).one()
    assert not isinstance(row, (list, tuple))
    session = _CoverageSession([[row], [row], []])
    monkeypatch.setattr(
        db,
        "session",
        lambda: _CoverageSessionContext(session),
    )

    await assert_nonterminal_receipt_key_coverage(keyring=signer)


@pytest.mark.asyncio
async def test_startup_retains_signer_for_each_abandoned_member_without_receipt(
    monkeypatch,
):
    from db.models import db

    _configure_active(monkeypatch, "epoch-abandoned", FIXED_KEY)
    signer = PTGWaveReceiptKeyring.from_environment()
    public_epoch = signer.public_by_key_id["epoch-abandoned"]
    pinned_rows = [
        (
            public_epoch.key_id,
            public_epoch.rsa_modulus,
            public_epoch.rsa_exponent,
        )
    ]
    public_only = PTGWaveReceiptKeyring(
        active_key_id=public_epoch.key_id,
        signing_by_key_id={},
        public_by_key_id={public_epoch.key_id: public_epoch},
    )
    blocked_session = _CoverageSession(
        [pinned_rows, [], [public_epoch.key_id]]
    )
    monkeypatch.setattr(
        db,
        "session",
        lambda: _CoverageSessionContext(blocked_session),
    )

    with pytest.raises(
        PTGWaveReceiptAuthorityError,
        match="unavailable for signing",
    ):
        await assert_nonterminal_receipt_key_coverage(keyring=public_only)

    pending_query = str(
        blocked_session.statements[2].compile(dialect=postgresql.dialect())
    )
    assert "ptg_import_wave_intent" in pending_query
    assert "ptg_import_wave_ordinary_terminal_receipt" in pending_query
    assert "import_run" not in pending_query.replace(
        "ptg_import_wave_ordinary_terminal_receipt", ""
    )

    covered_session = _CoverageSession(
        [pinned_rows, [], [public_epoch.key_id]]
    )
    monkeypatch.setattr(
        db,
        "session",
        lambda: _CoverageSessionContext(covered_session),
    )
    await assert_nonterminal_receipt_key_coverage(keyring=signer)
