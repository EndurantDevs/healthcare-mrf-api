#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Run the fixed default-off synthetic formulary seed-candidate canary."""

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
            "Build the fixed synthetic FHIR formulary seed candidate. "
            "No upstream FHIR network socket or publication is permitted."
        )
    )
    parser.add_argument(
        "command",
        choices=("verify-seed",),
        help="Run the fixed seed-intent verification canary.",
    )
    return parser


async def _run(command: str) -> str:
    from process.formulary_fhir.synthetic_canary import candidate_result_json
    from process.formulary_fhir.synthetic_canary import (
        verify_synthetic_seed_candidate,
    )

    if command != "verify-seed":
        raise RuntimeError("synthetic formulary canary command is invalid")
    result = await verify_synthetic_seed_candidate()
    return candidate_result_json(result)


def main() -> int:
    """Run the fixed smoke command with only stable JSON output."""

    arguments = _parser().parse_args()
    from process.formulary_fhir.repository_shared import json_text
    from process.formulary_fhir.synthetic_canary import SyntheticCanaryError

    try:
        rendered_result = asyncio.run(_run(arguments.command))
    except SyntheticCanaryError as error:
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
    print(rendered_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
