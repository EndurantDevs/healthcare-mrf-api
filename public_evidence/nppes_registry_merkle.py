# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Streaming source-order Merkle root for NPPES registry row evidence."""

from __future__ import annotations

import hashlib
import re

from public_evidence.nppes_registry_primitives import replay_error


_NODE_DOMAIN = b"HEALTHPORTA_NPPES_REGISTRY_NODE_V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise replay_error()
    return value


def derive_nppes_tree_node(left_sha256: object, right_sha256: object) -> str:
    """Derive one ordered, domain-separated RFC6962-shaped tree node."""

    left = bytes.fromhex(_strict_sha256(left_sha256))
    right = bytes.fromhex(_strict_sha256(right_sha256))
    digest = hashlib.sha256()
    digest.update(_NODE_DOMAIN)
    digest.update(b"\x01")
    digest.update(left)
    digest.update(right)
    return digest.hexdigest()


class NppesEvidenceRootAccumulator:
    """Maintain an O(log n) source-order Merkle frontier."""

    __slots__ = ("_count", "_finished", "_frontier")

    def __init__(self) -> None:
        self._count = 0
        self._finished = False
        self._frontier: list[str | None] = []

    @property
    def count(self) -> int:
        """Return the number of accepted source-order leaves."""

        return self._count

    def add(self, leaf_sha256: object) -> None:
        """Append one exact leaf digest to the source-order frontier."""

        if self._finished:
            raise replay_error()
        node = _strict_sha256(leaf_sha256)
        level = 0
        previous_count = self._count
        while previous_count & 1:
            node = derive_nppes_tree_node(self._frontier[level], node)
            self._frontier[level] = None
            previous_count >>= 1
            level += 1
        if level == len(self._frontier):
            self._frontier.append(node)
        else:
            self._frontier[level] = node
        self._count += 1

    def finish(self) -> str:
        """Seal and return the nonempty ordered tree root."""

        if self._finished or self._count == 0:
            raise replay_error()
        root: str | None = None
        for peak in self._frontier:
            if peak is not None:
                root = peak if root is None else derive_nppes_tree_node(peak, root)
        self._finished = True
        return root
