"""Command-line contract for the PTG V4 dev canary."""

from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    """Build the read-only canary CLI without any import-dispatch command."""

    parser = argparse.ArgumentParser(
        description=(
            "Read-only PTG V4 import monitor and dev acceptance canary. "
            "Run dispatch remains external."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_monitor_parser(subparsers)
    _add_storage_parser(subparsers)
    _add_reference_parser(subparsers)
    _add_cold_parser(subparsers)
    _add_internal_parser(subparsers)
    _add_accept_parser(subparsers)
    return parser


def _add_monitor_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "monitor-progress",
        help="Poll one already-dispatched run and persist its progress timeline.",
    )
    parser.add_argument("--control-base-url", required=True)
    parser.add_argument("--control-run-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    parser.add_argument("--maximum-monitor-seconds", type=float, default=14_400)
    parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--control-header-env", action="append", default=[])
    parser.add_argument("--allow-insecure-http", action="store_true")


def _add_storage_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "capture-storage-baseline",
        help="Capture exact global relation sizes immediately before dispatch.",
    )
    _add_database_arguments(parser, include_snapshot=False)
    parser.add_argument("--output", required=True)


def _add_cold_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "record-cold-sample",
        help="Record the first V4 request from one newly started dedicated API process.",
    )
    _add_http_probe_arguments(parser)
    parser.add_argument("--case-name", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--reference-evidence", required=True)
    parser.add_argument(
        "--component-fallback-mode",
        choices=("required", "forbidden", "allowed"),
        default="allowed",
    )


def _add_accept_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "accept",
        help="Evaluate terminal import, publication, storage, and serving gates.",
    )
    _add_database_arguments(parser, include_snapshot=False)
    _add_http_probe_arguments(parser)
    parser.add_argument("--progress-evidence", required=True)
    parser.add_argument("--storage-baseline", required=True)
    parser.add_argument("--cold-evidence", required=True)
    parser.add_argument("--reference-evidence", required=True)
    parser.add_argument("--rollback-owner-id", required=True)
    parser.add_argument("--internal-owner-evidence", required=True)
    parser.add_argument("--expect-root-count", action="append", default=[])
    parser.add_argument("--expect-relation-count", action="append", default=[])
    parser.add_argument("--maximum-progress-gap-seconds", type=float, default=15.0)
    parser.add_argument("--warmup-samples", type=int, default=2)
    parser.add_argument("--warm-samples", type=int, default=20)
    parser.add_argument("--output")


def _add_reference_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "capture-v3-reference",
        help=(
            "Capture an ordered semantic page digest from the frozen V3 "
            "candidate; no expected digest is accepted from the operator."
        ),
    )
    parser.add_argument("--api-base-url", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--code-system", default="CPT")
    parser.add_argument("--code", default="70553")
    parser.add_argument("--page-limit", type=int, default=25)
    parser.add_argument("--expected-item-count", type=int, default=25)
    parser.add_argument("--api-header-env", action="append", default=[])
    parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--maximum-response-bytes", type=int, default=2 << 20)
    parser.add_argument("--output", required=True)
    parser.add_argument("--allow-insecure-http", action="store_true")


def _add_internal_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "internal-owner-probe",
        help="Run the in-pod production provider-prefix path for the worst admitted set.",
    )
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--prefix-limit", type=int, default=201)
    parser.add_argument("--cold-samples", type=int, default=20)
    parser.add_argument("--warm-samples", type=int, default=20)
    parser.add_argument("--cold-p95-limit-ms", type=float, default=50.0)
    parser.add_argument("--warm-p95-limit-ms", type=float, default=50.0)
    parser.add_argument("--maximum-database-bytes", type=int, required=True)
    parser.add_argument("--maximum-database-blocks", type=int, required=True)
    parser.add_argument("--maximum-logical-lookups", type=int, required=True)


def _add_database_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_snapshot: bool,
) -> None:
    parser.add_argument(
        "--database-url-env",
        default="PTG_V4_CANARY_DATABASE_URL",
        help="Environment variable containing the PostgreSQL URL.",
    )
    parser.add_argument("--database-schema", default="mrf")
    if include_snapshot:
        parser.add_argument("--snapshot-id", required=True)


def _add_http_probe_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--api-base-url", required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--code-system", default="CPT")
    parser.add_argument("--code", default="70553")
    parser.add_argument("--api-param", action="append", default=[])
    parser.add_argument("--page-limit", type=int, default=25)
    parser.add_argument("--expected-item-count", type=int, default=25)
    parser.add_argument("--api-header-env", action="append", default=[])
    parser.add_argument("--metrics-header-env", action="append", default=[])
    parser.add_argument("--request-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--maximum-response-bytes", type=int, default=2 << 20)
    parser.add_argument("--cold-p95-limit-ms", type=float, default=50.0)
    parser.add_argument("--warm-p95-limit-ms", type=float, default=50.0)
    parser.add_argument("--minimum-cold-process-samples", type=int, default=20)
    parser.add_argument("--maximum-database-bytes", type=int, required=True)
    parser.add_argument("--maximum-database-blocks", type=int, required=True)
    parser.add_argument("--maximum-logical-lookups", type=int, required=True)
    parser.add_argument("--allow-insecure-http", action="store_true")
