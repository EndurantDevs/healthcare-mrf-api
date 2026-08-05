# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Input-side contracts for frozen public billing-entity references."""

from __future__ import annotations

import pytest

from api import ptg2_billing_entity_refs as billing_refs

SNAPSHOT_KEY = 37
TIN_FULL_HMAC = bytes(range(32))
TIN_LOCATOR = TIN_FULL_HMAC[:16]
FROZEN_REFERENCE = (
    "be1_AAECAwQFBgcICQoLDA0ODxIr3ljg-uNk13KslT9vSXm4lGO1maZsqjUk0Jf9HUBm"
)


def _encoded_reference(
    *,
    snapshot_key: int = SNAPSHOT_KEY,
    tin_hmac_sha256: bytes = TIN_FULL_HMAC,
) -> str:
    return billing_refs.encode_billing_entity_ref(
        snapshot_key=snapshot_key,
        tin_id_128=tin_hmac_sha256[:16],
        tin_hmac_sha256=tin_hmac_sha256,
    )


def test_frozen_reference_decodes_and_matches_exact_token_scope() -> None:
    """Preserve the be1 wire format and verify its full collision-safe scope."""

    assert _encoded_reference() == FROZEN_REFERENCE

    decoded_reference = billing_refs.decode_billing_entity_ref(FROZEN_REFERENCE)

    assert decoded_reference.tin_id_128 == TIN_LOCATOR
    assert len(decoded_reference.reference_tag) == 32
    assert billing_refs.is_billing_ref_valid_for_token(
        decoded_reference,
        snapshot_key=SNAPSHOT_KEY,
        tin_hmac_sha256=TIN_FULL_HMAC,
    )


def test_decoded_reference_representation_redacts_locator_and_tag() -> None:
    """Keep correlation-sensitive input material out of common diagnostics."""

    decoded_reference = billing_refs.decode_billing_entity_ref(FROZEN_REFERENCE)

    representation = repr(decoded_reference)
    assert representation == "<decoded-billing-entity-ref token=<redacted>>"
    assert TIN_LOCATOR.hex() not in representation
    assert decoded_reference.reference_tag.hex() not in representation


@pytest.mark.parametrize(
    ("snapshot_key", "tin_hmac_sha256"),
    [
        (SNAPSHOT_KEY + 1, TIN_FULL_HMAC),
        (SNAPSHOT_KEY, TIN_LOCATOR + b"\xff" * 16),
        (SNAPSHOT_KEY, bytes(reversed(range(32)))),
    ],
)
def test_reference_match_rejects_wrong_snapshot_or_full_hmac(
    snapshot_key: int,
    tin_hmac_sha256: bytes,
) -> None:
    """A valid locator alone cannot authenticate a different token or snapshot."""

    decoded_reference = billing_refs.decode_billing_entity_ref(FROZEN_REFERENCE)

    assert not billing_refs.is_billing_ref_valid_for_token(
        decoded_reference,
        snapshot_key=snapshot_key,
        tin_hmac_sha256=tin_hmac_sha256,
    )


@pytest.mark.parametrize(
    "tampered_reference",
    [
        FROZEN_REFERENCE[:4] + "B" + FROZEN_REFERENCE[5:],
        FROZEN_REFERENCE[:-1] + "A",
    ],
)
def test_reference_match_rejects_canonical_locator_or_tag_tampering(
    tampered_reference: str,
) -> None:
    """Canonical wire input still needs an exact locator and tag match."""

    decoded_reference = billing_refs.decode_billing_entity_ref(tampered_reference)

    assert not billing_refs.is_billing_ref_valid_for_token(
        decoded_reference,
        snapshot_key=SNAPSHOT_KEY,
        tin_hmac_sha256=TIN_FULL_HMAC,
    )


@pytest.mark.parametrize(
    "reference",
    [
        None,
        b"be1_" + b"A" * 64,
        "",
        "be2_" + "A" * 64,
        "be1_" + "A" * 63,
        "be1_" + "A" * 65,
        "be1_" + "A" * 63 + "=",
        "be1_" + "A" * 63 + "+",
        "be1_" + "A" * 63 + "/",
        "be1_" + "A" * 63 + "é",
    ],
)
def test_decoder_rejects_noncanonical_input_without_echo(reference: object) -> None:
    """Reject malformed shapes with one generic message that cannot echo input."""

    with pytest.raises(
        billing_refs.PTG2BillingAssociationDataError,
        match=r"^billing entity reference is invalid$",
    ) as error_context:
        billing_refs.decode_billing_entity_ref(reference)

    reference_text = str(reference)
    if reference_text:
        assert reference_text not in str(error_context.value)


@pytest.mark.parametrize(
    ("tin_id_128", "reference_tag"),
    [
        (bytearray(16), bytes(32)),
        (bytes(15), bytes(32)),
        (bytes(16), bytearray(32)),
        (bytes(16), bytes(31)),
    ],
)
def test_decoded_value_rejects_invalid_binary_shapes(
    tin_id_128: object,
    reference_tag: object,
) -> None:
    """Prevent callers from bypassing the strict wire decoder with bad values."""

    with pytest.raises(
        billing_refs.PTG2BillingAssociationDataError,
        match=r"^billing entity reference is invalid$",
    ):
        billing_refs.DecodedBillingEntityRef(
            tin_id_128=tin_id_128,
            reference_tag=reference_tag,
        )


@pytest.mark.parametrize(
    ("snapshot_key", "tin_hmac_sha256", "message"),
    [
        (0, TIN_FULL_HMAC, "billing association snapshot key is invalid"),
        (True, TIN_FULL_HMAC, "billing association snapshot key is invalid"),
        (SNAPSHOT_KEY, bytes(31), "billing association tax-identity token is invalid"),
        (
            SNAPSHOT_KEY,
            bytearray(32),
            "billing association tax-identity token is invalid",
        ),
    ],
)
def test_matcher_rejects_invalid_server_scope(
    snapshot_key: object,
    tin_hmac_sha256: object,
    message: str,
) -> None:
    """Fail closed when the caller supplies an invalid authoritative scope."""

    decoded_reference = billing_refs.decode_billing_entity_ref(FROZEN_REFERENCE)

    with pytest.raises(
        billing_refs.PTG2BillingAssociationDataError,
        match=f"^{message}$",
    ):
        billing_refs.is_billing_ref_valid_for_token(
            decoded_reference,
            snapshot_key=snapshot_key,
            tin_hmac_sha256=tin_hmac_sha256,
        )


def test_matcher_rejects_unvalidated_decoded_objects() -> None:
    """Require the strict decoded type before comparing any token material."""

    with pytest.raises(
        billing_refs.PTG2BillingAssociationDataError,
        match=r"^billing entity reference is invalid$",
    ):
        billing_refs.is_billing_ref_valid_for_token(
            object(),
            snapshot_key=SNAPSHOT_KEY,
            tin_hmac_sha256=TIN_FULL_HMAC,
        )
