#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Discover public ASR Health Benefits MRF group numbers.

This is an explicit maintenance utility. Do not schedule it as the monthly
mrf-source-discovery import path. Monthly imports should read the committed
seed list produced by this tool.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin

import aiohttp

DEFAULT_BASE_URL = "https://www.asrhealthbenefits.com"
DEFAULT_TOC_PATH = "/umbraco/surface/mrfdownload"
DEFAULT_OUTPUT = Path("specs/mrf_seed_lists/asr_health_benefits_groups.csv")
USER_AGENT = "HealthPorta mrf-source-discovery/asr-group-discovery"
SEED_FIELDNAMES = ("group_number", "status", "source_url", "first_seen_at", "last_verified_at", "notes")


def _toc_url(base_url: str, toc_path: str, group_number: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", toc_path.lstrip("/")) + "?" + urlencode(
        {"fileType": "TableOfContents", "groupNumber": group_number}
    )


def _candidate_groups(start: int, end: int) -> list[str]:
    if start < 1 or end > 9999 or start > end:
        raise ValueError("ASR candidate range must be within 1..9999")
    return [f"{value:04d}" for value in range(start, end + 1)]


def _read_seed_rows(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for lineno, row in enumerate(reader, start=2):
            group_number = str(row.get("group_number") or "").strip()
            if not group_number:
                continue
            if not group_number.isdigit() or len(group_number) != 4:
                raise ValueError(f"{path} line {lineno} must contain a 4-digit group_number")
            rows[group_number] = {
                field: str(row.get(field) or "").strip()
                for field in SEED_FIELDNAMES
            }
            rows[group_number]["group_number"] = group_number
    return rows


def _write_seed_rows(
    path: Path,
    group_numbers: list[str],
    *,
    existing_rows: dict[str, dict[str, str]] | None = None,
    source_url: str,
    verified_at: str,
) -> None:
    rows = existing_rows or {}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SEED_FIELDNAMES)
        writer.writeheader()
        for group_number in group_numbers:
            if not group_number.isdigit() or len(group_number) != 4:
                raise ValueError(f"{group_number} is not a 4-digit group number")
            row = dict(rows.get(group_number) or {})
            row["group_number"] = group_number
            row["status"] = row.get("status") or "active"
            row["source_url"] = row.get("source_url") or source_url
            row["first_seen_at"] = row.get("first_seen_at") or verified_at
            row["last_verified_at"] = verified_at
            row["notes"] = row.get("notes") or "confirmed public TOC group"
            writer.writerow({field: row.get(field, "") for field in SEED_FIELDNAMES})


async def _probe_head(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    toc_path: str,
    group_number: str,
) -> dict[str, Any]:
    url = _toc_url(base_url, toc_path, group_number)
    try:
        async with session.head(url, allow_redirects=True) as resp:
            content_length = resp.headers.get("Content-Length")
            return {
                "group_number": group_number,
                "url": str(resp.url),
                "ok": resp.status == 200,
                "http_status": resp.status,
                "content_length": int(content_length) if content_length and content_length.isdigit() else None,
                "content_type": resp.headers.get("Content-Type"),
                "error": None,
            }
    except Exception as exc:
        return {
            "group_number": group_number,
            "url": url,
            "ok": False,
            "http_status": None,
            "content_length": None,
            "content_type": None,
            "error": str(exc),
        }


async def _validate_json(session: aiohttp.ClientSession, url: str, *, max_bytes: int) -> tuple[bool, str | None]:
    try:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status != 200:
                return False, f"GET returned HTTP {resp.status}"
            chunks: list[bytes] = []
            total = 0
            async for chunk in resp.content.iter_chunked(64 * 1024):
                total += len(chunk)
                if total > max_bytes:
                    return False, f"response exceeds {max_bytes} bytes"
                chunks.append(chunk)
        payload = json.loads(b"".join(chunks).decode("utf-8", errors="replace"))
        if not isinstance(payload, dict):
            return False, "TOC response is not a JSON object"
        return True, None
    except Exception as exc:
        return False, str(exc)


async def _discover(args: argparse.Namespace) -> list[dict[str, Any]]:
    groups = _candidate_groups(args.start, args.end)
    timeout = aiohttp.ClientTimeout(total=None, connect=args.connect_timeout, sock_read=args.read_timeout)
    connector = aiohttp.TCPConnector(limit=args.concurrency, ttl_dns_cache=300)
    queue: asyncio.Queue[str] = asyncio.Queue()
    for group_number in groups:
        queue.put_nowait(group_number)
    results: list[dict[str, Any]] = []
    checked = 0

    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
        connector=connector,
        trust_env=False,
    ) as session:

        async def worker() -> None:
            nonlocal checked
            while True:
                try:
                    group_number = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    if args.delay_seconds > 0:
                        await asyncio.sleep(args.delay_seconds)
                    result = await _probe_head(
                        session,
                        base_url=args.base_url,
                        toc_path=args.toc_path,
                        group_number=group_number,
                    )
                    if result["ok"] and not args.skip_json_validation:
                        json_ok, error = await _validate_json(session, str(result["url"]), max_bytes=args.max_json_bytes)
                        result["ok"] = json_ok
                        result["json_validated"] = json_ok
                        result["json_error"] = error
                    results.append(result)
                    checked += 1
                    if args.progress_every and checked % args.progress_every == 0:
                        found = len([item for item in results if item["ok"]])
                        print(f"checked={checked} found={found}", file=sys.stderr)
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(args.concurrency)]
        await queue.join()
        await asyncio.gather(*workers)
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=1, help="First numeric group candidate. Defaults to 1.")
    parser.add_argument("--end", type=int, default=9999, help="Last numeric group candidate. Defaults to 9999.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--toc-path", default=DEFAULT_TOC_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--write", action="store_true", help="Write found groups to the allowlist file.")
    parser.add_argument("--replace", action="store_true", help="Replace the output file instead of merging with it.")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--connect-timeout", type=float, default=10.0)
    parser.add_argument("--read-timeout", type=float, default=20.0)
    parser.add_argument("--max-json-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--skip-json-validation", action="store_true")
    parser.add_argument("--progress-every", type=int, default=250)
    args = parser.parse_args()
    args.concurrency = max(1, args.concurrency)
    return args


async def _async_main() -> int:
    args = _parse_args()
    results = await _discover(args)
    found = sorted({str(item["group_number"]) for item in results if item["ok"]})
    output_groups = found
    if args.write:
        existing_rows = {} if args.replace else _read_seed_rows(args.output)
        output_groups = sorted({*existing_rows.keys(), *found})
        _write_seed_rows(
            args.output,
            output_groups,
            existing_rows=existing_rows,
            source_url=args.base_url.rstrip("/") + "/MRF",
            verified_at=dt.date.today().isoformat(),
        )
    summary = {
        "checked": len(results),
        "found": len(found),
        "groups": found,
        "output": str(args.output) if args.write else None,
        "output_groups": len(output_groups) if args.write else None,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main() -> int:
    """Run the bounded group discovery report from command-line arguments."""

    return asyncio.run(_async_main())


if __name__ == "__main__":
    raise SystemExit(main())
