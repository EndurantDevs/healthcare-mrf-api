# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare a fresh national directory plan from the previous authorized national plan."""

import argparse
import json
import os
from pathlib import Path

from process.delta_dental_directory_worker import decode_finder_plan
from process.nomi_directory_capture import validated_search_windows
from process.provider_directory_zip_inventory import national_zip_codes
from process.vsp_directory_http import strict_json

# These publisher radii cover the Alaska and territory gaps in the national radius plan.
_NOMI_SUPPLEMENT = (("99747", 50), ("00926", 250), ("96913", 250), ("96799", 250))


def nomi_plan(previous):
    """Extend the authorized radius plan without repeating existing coverage."""
    radius_by_zip = dict(validated_search_windows(previous))
    additions = [
        {"zip_code": zip_code, "radius_miles": miles}
        for zip_code, miles in _NOMI_SUPPLEMENT
        if radius_by_zip.get(zip_code, 0) < miles
    ]
    payload = {**previous, "searches": previous["searches"] + additions}
    validated_search_windows(payload)
    return payload


def delta_plan(previous, edition, budget):
    """Keep prior search order and append current USPS codes for a new edition."""
    if not edition or len(edition) > 128 or edition.strip() != edition or edition == previous["data_edition"]:
        raise ValueError("A new Delta directory edition is required")
    if not isinstance(previous.get("zip_inventory"), list) or not previous["zip_inventory"]:
        raise ValueError("Expected a Delta ZIP inventory plan")
    codes = list(dict.fromkeys([*previous["zip_inventory"], *national_zip_codes()]))
    payload = {**previous, "data_edition": edition, "zip_inventory": codes, "max_origin_requests": budget}
    decode_finder_plan(json.dumps(payload, separators=(",", ":")).encode())
    return payload


def main(argv=None):
    """Write a separate, owner-readable plan after validating the complete replacement."""
    parser = argparse.ArgumentParser(description=__doc__)
    sources = parser.add_subparsers(dest="source", required=True)
    for source in ("nomi", "delta"):
        sub = sources.add_parser(source)
        sub.add_argument("previous", type=Path)
        sub.add_argument("output", type=Path)
        if source == "delta":
            sub.add_argument("--edition", required=True)
            sub.add_argument("--max-origin-requests", type=int, required=True)
    args = parser.parse_args(argv)
    previous = strict_json(args.previous.read_bytes())
    if args.source == "nomi":
        plan = nomi_plan(previous)
    else:
        plan = delta_plan(previous, args.edition, args.max_origin_requests)
    encoded = json.dumps(plan, separators=(",", ":")) + "\n"
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(encoded)
    except OSError:
        args.output.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    main()
