"""Ephemeral RSA keys for receipt-authority behavior tests.

The shared contract fixture contains only public material.  Tests that need to
sign fresh receipts generate an unrelated process-local key under an OS-owned
temporary directory, so no reusable private key is stored in the repository.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


_TEST_KEY_DIRECTORY = tempfile.TemporaryDirectory(
    prefix="healthporta-ptg-receipt-tests-"
)


def write_ephemeral_receipt_private_key(path: Path) -> Path:
    """Generate one RSA-2048 test signer at a caller-owned temporary path."""

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return path


EPHEMERAL_RECEIPT_PRIVATE_KEY = write_ephemeral_receipt_private_key(
    Path(_TEST_KEY_DIRECTORY.name) / "receipt-private-key.pem"
)
_PUBLIC_NUMBERS = serialization.load_pem_private_key(
    EPHEMERAL_RECEIPT_PRIVATE_KEY.read_bytes(),
    password=None,
).public_key().public_numbers()
EPHEMERAL_RECEIPT_PUBLIC_MODULUS = f"{_PUBLIC_NUMBERS.n:0512x}"


__all__ = [
    "EPHEMERAL_RECEIPT_PRIVATE_KEY",
    "EPHEMERAL_RECEIPT_PUBLIC_MODULUS",
    "write_ephemeral_receipt_private_key",
]
