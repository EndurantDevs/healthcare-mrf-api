#!/usr/bin/env python3
"""Public repository hygiene checks.

The check is intentionally based on tracked files only so local scratch files do
not make CI nondeterministic. It blocks private agent/operator guidance paths and
obvious secret material from entering the public repository.
"""

from __future__ import annotations

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

PRIVATE_TEXT_FINGERPRINTS = {
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
TEXT_TOKEN_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)
PRIVATE_TEXT_WINDOW_MAX = 3

SELF_PATHS = {
    "scripts/ci/public_hygiene.py",
    ".github/workflows/ci.yml",
}


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return [Path(line) for line in result.stdout.splitlines() if line]


def is_binary(path: Path) -> bool:
    try:
        chunk = path.read_bytes()[:4096]
    except OSError:
        return True
    return b"\0" in chunk


def check_paths(paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in paths:
        parts = set(path.parts)
        if parts & FORBIDDEN_PATH_PARTS:
            errors.append(f"forbidden path component: {path}")
        if path.name in FORBIDDEN_BASENAMES:
            errors.append(f"forbidden instruction file: {path}")
    return errors


def has_private_text_fingerprint(text: str) -> bool:
    """Match private examples without publishing their plaintext in this repository."""

    tokens = [token.lower() for token in TEXT_TOKEN_RE.findall(text)]
    for start in range(len(tokens)):
        normalized_window = ""
        for width in range(PRIVATE_TEXT_WINDOW_MAX):
            token_index = start + width
            if token_index >= len(tokens):
                break
            normalized_window += tokens[token_index]
            fingerprint = hashlib.sha256(normalized_window.encode("utf-8")).hexdigest()
            if fingerprint in PRIVATE_TEXT_FINGERPRINTS:
                return True
    return False


def check_content(paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in paths:
        path_str = path.as_posix()
        if path_str in SELF_PATHS or is_binary(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in CONTENT_PATTERNS.items():
            if pattern.search(text):
                errors.append(f"{label}: {path}")
        if has_private_text_fingerprint(text):
            errors.append(f"private-example-fingerprint: {path}")
    return errors


def main() -> int:
    paths = tracked_files()
    errors = check_paths(paths) + check_content(paths)
    if errors:
        print("Public hygiene check failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"Public hygiene check passed for {len(paths)} tracked files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
