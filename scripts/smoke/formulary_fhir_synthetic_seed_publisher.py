#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Publish the fixed default-off synthetic formulary generation-one seed."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("HLTHPRT_LOG_CFG", str(ROOT / "logging.yaml"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Publish or exactly replay the fixed synthetic FHIR formulary "
            "generation-one seed. No acquisition is permitted."
        )
    )
    parser.add_argument(
        "command",
        choices=("publish-seed",),
        help="Publish the fixed verified seed candidate.",
    )
    return parser


async def _run(command: str) -> str:
    from process.formulary_fhir.synthetic_seed_publisher import (
        publication_result_json,
    )
    from process.formulary_fhir.synthetic_seed_publisher import (
        publish_synthetic_seed,
    )

    if command != "publish-seed":
        raise RuntimeError("synthetic formulary publisher command is invalid")
    publication = await publish_synthetic_seed()
    return publication_result_json(publication)


def run_command() -> int:
    """Run the fixed publication command with only stable JSON output."""

    arguments = _parser().parse_args()
    from process.formulary_fhir.repository_shared import json_text
    from process.formulary_fhir.synthetic_seed_publisher import (
        SyntheticSeedPublicationError,
    )

    try:
        rendered_publication = asyncio.run(_run(arguments.command))
    except SyntheticSeedPublicationError as error:
        print(
            json_text({"status": "error", "code": error.code}),
            file=sys.stderr,
        )
        return 1
    except TimeoutError:
        print(
            json_text({"status": "error", "code": "timeout"}),
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            json_text({"status": "error", "code": "failed"}),
            file=sys.stderr,
        )
        return 1
    print(rendered_publication)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
