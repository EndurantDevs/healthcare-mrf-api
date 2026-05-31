# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Neutral entrypoint for local PTG2 legacy cleanup."""

from __future__ import annotations

import asyncio

from process.ptg_parts.ptg2_manifest_cleanup import _amain


if __name__ == "__main__":
    asyncio.run(_amain())
