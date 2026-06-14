#!/usr/bin/env python3
"""Public repository hygiene checks.

The check is intentionally based on tracked files only so local scratch files do
not make CI nondeterministic. It blocks private agent/operator guidance paths and
obvious secret material from entering the public repository.
"""

from __future__ import annotations

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
