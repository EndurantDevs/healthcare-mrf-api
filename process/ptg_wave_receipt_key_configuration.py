"""Bounded environment and file loading for receipt-key epochs."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def load_receipt_keyring_from_environment(keyring_type: Any) -> Any:
    """Load one closed active, retained, and public key configuration."""

    from process.ptg_wave_receipt_authority import (
        ACTIVE_KEY_ID_ENV,
        ACTIVE_PRIVATE_KEY_FILE_ENV,
        RETAINED_PRIVATE_KEY_FILES_ENV,
        RETIRED_PUBLIC_EPOCHS_FILE_ENV,
    )

    active_key_id, signing_epoch_by_key_id = _configured_signing_epochs(
        os.getenv(ACTIVE_KEY_ID_ENV),
        os.getenv(ACTIVE_PRIVATE_KEY_FILE_ENV),
        os.getenv(RETAINED_PRIVATE_KEY_FILES_ENV),
    )
    public_epoch_by_key_id = _configured_public_epochs(
        active_key_id,
        signing_epoch_by_key_id,
        os.getenv(RETIRED_PUBLIC_EPOCHS_FILE_ENV),
    )
    return keyring_type(
        active_key_id=active_key_id,
        signing_by_key_id=dict(signing_epoch_by_key_id),
        public_by_key_id=dict(public_epoch_by_key_id),
    )


def _configured_signing_epochs(
    active_key_value: object,
    active_path_value: object,
    retained_value: object,
) -> tuple[str, dict[str, Any]]:
    from process.ptg_wave_receipt_authority import (
        PTGWaveReceiptAuthorityError,
        _key_id,
    )

    active_key_id = _key_id(active_key_value, "active receipt key ID")
    active_path = _absolute_path(
        active_path_value,
        "active receipt private-key file",
    )
    retained_paths = _retained_private_key_paths(retained_value)
    if active_key_id in retained_paths:
        raise PTGWaveReceiptAuthorityError(
            "active receipt key cannot also be retained"
        )
    signing_epoch_by_key_id = {
        active_key_id: _load_signing_epoch(active_key_id, active_path)
    }
    for key_id, private_path in retained_paths.items():
        signing_epoch_by_key_id[key_id] = _load_signing_epoch(key_id, private_path)
    return active_key_id, signing_epoch_by_key_id


def _configured_public_epochs(
    active_key_id: str,
    signing_epoch_by_key_id: Mapping[str, Any],
    retired_path_value: object,
) -> dict[str, Any]:
    from process.ptg_wave_receipt_authority import (
        MAX_RECEIPT_KEY_EPOCHS,
        PTGWaveReceiptAuthorityError,
        PTGWaveReceiptPublicEpoch,
    )

    public_epoch_by_key_id = {
        key_id: epoch.public_epoch
        for key_id, epoch in signing_epoch_by_key_id.items()
    }
    public_epoch_by_key_id[active_key_id] = PTGWaveReceiptPublicEpoch(
        **{
            **public_epoch_by_key_id[active_key_id].as_mapping(),
            "state": "active",
        }
    )
    for epoch in _retired_public_epochs(retired_path_value):
        existing = public_epoch_by_key_id.get(epoch.key_id)
        if existing is not None and existing != epoch:
            raise PTGWaveReceiptAuthorityError(
                "receipt key epoch definitions conflict"
            )
        public_epoch_by_key_id.setdefault(epoch.key_id, epoch)
    if not 1 <= len(public_epoch_by_key_id) <= MAX_RECEIPT_KEY_EPOCHS:
        raise PTGWaveReceiptAuthorityError("receipt key epoch count is invalid")
    public_key_materials = {
        (epoch.rsa_modulus, epoch.rsa_exponent)
        for epoch in public_epoch_by_key_id.values()
    }
    if len(public_key_materials) != len(public_epoch_by_key_id):
        raise PTGWaveReceiptAuthorityError(
            "receipt public key material must identify one epoch"
        )
    return public_epoch_by_key_id


def _absolute_path(value: object, name: str) -> Path:
    from process.ptg_wave_receipt_authority import PTGWaveReceiptAuthorityError

    if not isinstance(value, str) or not value or not os.path.isabs(value):
        raise PTGWaveReceiptAuthorityError(f"{name} is invalid")
    return Path(value)


def _read_bounded(path: Path, limit: int, name: str) -> bytes:
    from process.ptg_wave_receipt_authority import PTGWaveReceiptAuthorityError

    try:
        stat = path.stat()
        if not path.is_file() or stat.st_size < 1 or stat.st_size > limit:
            raise PTGWaveReceiptAuthorityError(f"{name} is invalid")
        return path.read_bytes()
    except PTGWaveReceiptAuthorityError:
        raise
    except OSError as exc:
        raise PTGWaveReceiptAuthorityError(f"{name} is unavailable") from exc


def _load_signing_epoch(key_id: str, path: Path) -> Any:
    from process.ptg_wave_receipt_authority import (
        MAX_PRIVATE_KEY_BYTES,
        PTGWaveReceiptAuthorityError,
        _SigningEpoch,
    )

    raw_key = _read_bounded(path, MAX_PRIVATE_KEY_BYTES, "receipt private-key file")
    try:
        private_key = serialization.load_pem_private_key(raw_key, password=None)
    except (TypeError, ValueError) as exc:
        raise PTGWaveReceiptAuthorityError("receipt private key is invalid") from exc
    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise PTGWaveReceiptAuthorityError("receipt private key must be RSA")
    numbers = private_key.public_key().public_numbers()
    if private_key.key_size != 2048 or numbers.e != 65537:
        raise PTGWaveReceiptAuthorityError(
            "receipt private key must be RSA-2048 with exponent 65537"
        )
    return _SigningEpoch(key_id=key_id, private_key=private_key)


def _retained_private_key_paths(raw_value: object) -> dict[str, Path]:
    from process.ptg_wave_receipt_authority import (
        MAX_PUBLIC_EPOCH_FILE_BYTES,
        MAX_RECEIPT_KEY_EPOCHS,
        PTGWaveReceiptAuthorityError,
        _key_id,
    )

    if raw_value in (None, ""):
        return {}
    if not isinstance(raw_value, str) or len(raw_value) > MAX_PUBLIC_EPOCH_FILE_BYTES:
        raise PTGWaveReceiptAuthorityError(
            "retained receipt private-key configuration is invalid"
        )
    try:
        retained_path_by_key_id = _strict_json_loads(
            raw_value,
            "retained receipt private-key configuration",
        )
    except json.JSONDecodeError as exc:
        raise PTGWaveReceiptAuthorityError(
            "retained receipt private-key configuration is invalid"
        ) from exc
    if (
        not isinstance(retained_path_by_key_id, Mapping)
        or len(retained_path_by_key_id) >= MAX_RECEIPT_KEY_EPOCHS
    ):
        raise PTGWaveReceiptAuthorityError(
            "retained receipt private-key configuration is invalid"
        )
    return {
        _key_id(key_id, "retained receipt key ID"): _absolute_path(
            path,
            "retained receipt private-key file",
        )
        for key_id, path in retained_path_by_key_id.items()
    }


def _retired_public_epochs(raw_path: object) -> tuple[Any, ...]:
    """Load and validate bounded public-only retired epochs."""

    from process.ptg_wave_receipt_authority import (
        MAX_PUBLIC_EPOCH_FILE_BYTES,
        MAX_RECEIPT_KEY_EPOCHS,
        PTGWaveReceiptAuthorityError,
    )

    if raw_path in (None, ""):
        return ()
    path = _absolute_path(raw_path, "retired receipt public-epoch file")
    raw = _read_bounded(
        path,
        MAX_PUBLIC_EPOCH_FILE_BYTES,
        "retired receipt public-epoch file",
    )
    try:
        epoch_entries = _strict_json_loads(raw, "retired receipt public epochs")
    except json.JSONDecodeError as exc:
        raise PTGWaveReceiptAuthorityError(
            "retired receipt public epochs are invalid"
        ) from exc
    if not isinstance(epoch_entries, list) or len(epoch_entries) >= MAX_RECEIPT_KEY_EPOCHS:
        raise PTGWaveReceiptAuthorityError(
            "retired receipt public epochs are invalid"
        )
    epochs = [_validated_retired_epoch(entry) for entry in epoch_entries]
    if len({epoch.key_id for epoch in epochs}) != len(epochs):
        raise PTGWaveReceiptAuthorityError(
            "retired receipt public epoch IDs must be unique"
        )
    return tuple(epochs)


def _validated_retired_epoch(entry_by_field: object) -> Any:
    """Validate one public-only retired receipt epoch."""

    from process.ptg_wave_receipt_authority import (
        PTGWaveReceiptAuthorityError,
        PTGWaveReceiptPublicEpoch,
        _HEX_512,
        _key_id,
    )

    if not isinstance(entry_by_field, Mapping) or set(entry_by_field) != {
        "key_id", "rsa_modulus", "rsa_exponent", "state",
    }:
        raise PTGWaveReceiptAuthorityError(
            "retired receipt public epoch fields are invalid"
        )
    key_id = _key_id(entry_by_field.get("key_id"), "retired receipt key ID")
    modulus = entry_by_field.get("rsa_modulus")
    exponent = entry_by_field.get("rsa_exponent")
    if (
        not isinstance(modulus, str)
        or _HEX_512.fullmatch(modulus) is None
        or modulus[0] not in "89abcdef"
        or modulus[-1] not in "13579bdf"
        or type(exponent) is not int
        or exponent != 65537
        or entry_by_field.get("state") != "retired"
    ):
        raise PTGWaveReceiptAuthorityError(
            "retired receipt public epoch is invalid"
        )
    return PTGWaveReceiptPublicEpoch(
        key_id=key_id,
        rsa_modulus=modulus,
        rsa_exponent=exponent,
        state="retired",
    )


def _strict_json_loads(raw: str | bytes, name: str) -> Any:
    """Parse configuration JSON while rejecting duplicate object fields."""

    from process.ptg_wave_receipt_authority import PTGWaveReceiptAuthorityError

    def object_from_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        """Reject duplicate fields while constructing one JSON object."""
        object_by_key: dict[str, Any] = {}
        for key, value in pairs:
            if key in object_by_key:
                raise PTGWaveReceiptAuthorityError(
                    f"{name} contains duplicate object fields"
                )
            object_by_key[key] = value
        return object_by_key

    return json.loads(raw, object_pairs_hook=object_from_pairs)
