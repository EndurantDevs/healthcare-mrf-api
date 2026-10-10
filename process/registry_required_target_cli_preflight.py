# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate primitive target CLI arguments before importing worker runtimes."""

import argparse
import json
import re
import sys
from pathlib import Path
from uuid import UUID


class _ReceiptArgumentParser(argparse.ArgumentParser):
    def error(self, _message):
        """Keep primitive argument failures in the fixed JSON receipt."""
        self.exit(2, json.dumps({"code": "invalid_arguments", "status": "error"}, separators=(",", ":")) + "\n")


def review_parser(description):
    """Construct the same reviewed-edition flags without runtime imports."""
    parser = _ReceiptArgumentParser(description=description, allow_abbrev=False)
    parser.add_argument("--input-file", type=Path, required=True)
    parser.add_argument("--snapshot-id", type=UUID, required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--ledger-snapshot-id", type=UUID, required=True)
    return parser


def _generation_id(value):
    if re.fullmatch(r"[1-9][0-9]{0,18}", value) is None or int(value) > 9223372036854775807:
        raise ValueError("generation_invalid")
    return int(value)


def coverage_parser(description):
    """Construct the exact ledger and canonical generation selectors."""
    parser = _ReceiptArgumentParser(description=description, allow_abbrev=False)
    parser.add_argument("--ledger-snapshot-id", type=UUID, required=True)
    parser.add_argument("--generation-id", type=_generation_id)
    return parser


def preflight_module_arguments():
    """Leave normal imports alone; reject invalid -m target selectors before runtime imports."""
    if sys.argv[0] != "-m" or "-m" not in sys.orig_argv:
        return
    module_index = sys.orig_argv.index("-m") + 1
    if module_index >= len(sys.orig_argv):
        return
    module_name = sys.orig_argv[module_index]
    if module_name == "process.registry_required_target_review_import":
        parser = review_parser("Retain an explicitly reviewed target edition without approving or publishing it.")
    elif module_name == "process.registry_required_target_coverage_import":
        parser = coverage_parser("Read complete target coverage from one retained ledger and serving generation.")
    else:
        return
    parser.prog = module_name.rsplit(".", 1)[-1] + ".py"
    parsed = parser.parse_args(sys.argv[1:])
    if not parsed.ledger_snapshot_id.int:
        parser.error("invalid ledger identity")
