#!/usr/bin/env python3
"""Public repository hygiene checks.

CI checks tracked files. Release preparation can additionally scan untracked,
non-ignored files with ``--include-untracked``.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path


FORBIDDEN_PATH_PARTS = {
    ".aider",
    ".codex",
    ".cursor",
    ".windsurf",
}

FORBIDDEN_BASENAMES = {
    "AGENTS.md",
    "CLAUDE.md",
    "CODEX.md",
    "GEMINI.md",
    "copilot-instructions.md",
}

CONTENT_PATTERNS = {
    "agentic-development-reference": re.compile(r"\bagentic\b", re.IGNORECASE),
    "private-ovh-hostname": re.compile(r"\bns\d+\.ip-\d+-\d+-\d+\.us\b", re.IGNORECASE),
    "github-token": re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b"),
    "github-fine-grained-token": re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    "openai-token": re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    "database-url-with-password": re.compile(
        r"\bpostgres(?:ql)?://[^:\s/@{}]+:[^@\s{}]+@",
        re.IGNORECASE,
    ),
    "password-assignment": re.compile(
        r"\b(?:password|passwd|secret|token)\s*[:=]\s*['\"][^'\"\s]{8,}['\"]",
        re.IGNORECASE,
    ),
}

PRIVATE_EXAMPLE_FINGERPRINTS = {
    "0d7f9b704b0669e301fce51d326b99648420899282f8591f7f2c25f0139f390d",
    "12e76d8f8d8c492db3d8694b37965678f1f44502a3f99a04a95b2443e54103e9",
    "172590cb4afd42e31874eb15a27f5950aa3ec2585d718addf1fa0e707e030b1a",
    "17e0c215bd50dd6221d1975833cc1c9f3714927dff40317883213bf97e6fc3c2",
    "2bda54ebe2b49c984fb7bbccd8e1076f15cd261db51f1711061c3975794966f1",
    "456ca0e03c0203dfa5e039ac51ae8eaecad496669348d1e35831e355a992e837",
    "621a56ccfeabab6c65b0a68453335664bb20dd280edafc1f167bb55c74ff5c7b",
    "78bb8ecef68ec12e7b326cf5d38fd19baebfdcad9d6dca0f480a660121809418",
    "7ea30da2361ecea73d26f07495a0e642d739387377972872467d57a458451c4c",
    "8da581325f2ef67ad5d670316501495be60f04e39c24ad441cac51b552594b96",
    "a3f6cfd195ad02801803784eb724e2ae3580de1ff68bd4625d2eef3adf49eb50",
    "b996c1e92f49a8b0cf629680fb4a8fafc627639a344678772f98e025b17da30a",
    "c72086ecc3255c4530054a242028242aa1139a0c8ba397ab169c33abedd661b0",
    "d4590607d00cb32b1a7a776c7eb6c77636d2fe8879349c8faa713ef4b624cade",
    "ea48bd692cc5bb613a2c7aae310aa00cf65eadc93ea1b0c036c07717d1eaca4a",
    "f035af007b46c4de201dfbfd377719b1b0d56ae340b7087797b27a220a2bee49",
}

# Fingerprints of normalized private integration names. Keeping only hashes here
# lets the public gate reject separator variants without publishing
# the names it protects against.
PRIVATE_INTEGRATION_FINGERPRINTS = {
    "216c6d1b6b239d96e8c9a75575e4c94392bce811945aee5d1fae7882344aa39e",
    "2ae795d6a6ea0ce8c620243a10273674e034e9f7afe0a9cc368b201e931151f7",
    "47ded72d07eccdc65a1017e29809be824913399e264f3daa156b39ee598614d8",
    "7b714bcacf92d474089f5cf2c0e3c03a403ffc729174b0f3106ca1367f3f72e9",
    "d8205bea56fce6d160026dffc89bbd8a0655c34296a5ff31886d6e5785270b6b",
    "e3ddb5be56a2a9333debc2676dec9a472954b4c9742424ba849fc5fb466ae141",
    "f3d92f97909c326ce25386f309cae51ed94f7ee1d4c20d1ed1d99f35737fdb39",
}
PRIVATE_TEXT_FINGERPRINTS = (
    PRIVATE_EXAMPLE_FINGERPRINTS | PRIVATE_INTEGRATION_FINGERPRINTS
)
TEXT_TOKEN_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)
PRIVATE_TEXT_WINDOW_MAX = 3
INTEGRATION_IDENTIFIER_SEPARATOR_RE = re.compile(r"[-_./:\\]+")

PATTERN_EXEMPT_PATHS = {
    "scripts/ci/public_hygiene.py",
}


def repository_files(*, include_untracked: bool = False) -> list[Path]:
    """List tracked repository files included in hygiene checks."""
    command = ["git", "ls-files", "-z", "--cached"]
    if include_untracked:
        command.extend(["--others", "--exclude-standard"])
    result = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
    )
    return sorted(
        {
            Path(item.decode("utf-8", errors="surrogateescape"))
            for item in result.stdout.split(b"\0")
            if item
        }
    )


def existing_files(paths: list[Path]) -> list[Path]:
    """Ignore tracked paths deleted by the candidate change being checked."""

    return [path for path in paths if path.is_file()]


def is_binary(path: Path) -> bool:
    """Return whether file content appears binary."""
    try:
        chunk = path.read_bytes()[:4096]
    except OSError:
        return True
    return b"\0" in chunk


def check_paths(paths: list[Path]) -> list[str]:
    """Check tracked paths for prohibited public data."""
    errors: list[str] = []
    for path in paths:
        parts = set(path.parts)
        if parts & FORBIDDEN_PATH_PARTS:
            errors.append(f"forbidden path component: {path}")
        if path.name in FORBIDDEN_BASENAMES:
            errors.append(f"forbidden instruction file: {path}")
        if has_private_text_fingerprint(path.as_posix()):
            errors.append(f"private-path-fingerprint: {path}")
    return errors


def has_private_text_fingerprint(text: str) -> bool:
    """Match private examples without publishing their plaintext in this repository."""

    tokens = list(TEXT_TOKEN_RE.finditer(text))
    for start in range(len(tokens)):
        normalized_window = ""
        integration_identifier = True
        for width in range(PRIVATE_TEXT_WINDOW_MAX):
            token_index = start + width
            if token_index >= len(tokens):
                break
            if width:
                separator = text[
                    tokens[token_index - 1].end() : tokens[token_index].start()
                ]
                integration_identifier = bool(
                    integration_identifier
                    and INTEGRATION_IDENTIFIER_SEPARATOR_RE.fullmatch(separator)
                )
            normalized_window += tokens[token_index].group(0).lower()
            fingerprint = hashlib.sha256(normalized_window.encode("utf-8")).hexdigest()
            if fingerprint in PRIVATE_EXAMPLE_FINGERPRINTS:
                return True
            if (
                integration_identifier
                and fingerprint in PRIVATE_INTEGRATION_FINGERPRINTS
            ):
                return True
    return False


def check_content(paths: list[Path]) -> list[str]:
    """Check tracked text content for prohibited public data."""
    errors: list[str] = []
    for path in paths:
        path_str = path.as_posix()
        if is_binary(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if path_str not in PATTERN_EXEMPT_PATHS:
            for label, pattern in CONTENT_PATTERNS.items():
                if pattern.search(text):
                    errors.append(f"{label}: {path}")
        if has_private_text_fingerprint(text):
            errors.append(f"private-example-fingerprint: {path}")
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse public-hygiene command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-untracked",
        action="store_true",
        help="also scan non-ignored untracked files for a local release check",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the public repository hygiene checks."""
    args = parse_args(argv)
    paths = existing_files(repository_files(include_untracked=args.include_untracked))
    errors = check_paths(paths) + check_content(paths)
    if errors:
        print("Public hygiene check failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    scope = "tracked and untracked" if args.include_untracked else "tracked"
    print(f"Public hygiene check passed for {len(paths)} {scope} files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
