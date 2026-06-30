#!/usr/bin/env python3
"""Validate commit subjects and pull request titles for readability."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


ALLOWED_TYPES = {
    "build",
    "chore",
    "ci",
    "deploy",
    "docs",
    "feat",
    "fix",
    "ops",
    "perf",
    "refactor",
    "revert",
    "test",
}
AUTOMATION_PREFIXES = (
    "Bump ",
    "Merge ",
    "Revert ",
)
MAX_SUBJECT_LENGTH = 100
VAGUE_SUMMARIES = {
    "change",
    "changes",
    "cleanup",
    "fix",
    "fixes",
    "misc",
    "stuff",
    "update",
    "updates",
    "wip",
    "work",
}
SUBJECT_PATTERN = re.compile(
    r"^(?P<type>[a-z]+)"
    r"(?:\((?P<scope>[a-z0-9][a-z0-9-]*(?:/[a-z0-9][a-z0-9-]*)*)\))?"
    r"(?P<breaking>!)?: (?P<summary>.+)$"
)


def first_line(message: str) -> str:
    """Return the first non-empty line from a commit message."""
    return message.strip().splitlines()[0].strip() if message.strip() else ""


def is_automation_subject(subject: str) -> bool:
    """Return whether GitHub or Dependabot generated the subject."""
    return subject.startswith(AUTOMATION_PREFIXES)


def vague_summary_key(summary: str) -> str:
    """Normalize a summary so low-signal wording can be detected."""
    return summary.strip().lower().rstrip(".!?")


def validate_subject(subject: str) -> list[str]:
    """Return readability problems for one commit subject."""
    problems: list[str] = []
    if not subject:
        return ["subject is empty"]
    if is_automation_subject(subject):
        return problems
    if len(subject) > MAX_SUBJECT_LENGTH:
        problems.append(f"subject is longer than {MAX_SUBJECT_LENGTH} characters")
    if subject.endswith("."):
        problems.append("subject must not end with a period")

    match = SUBJECT_PATTERN.match(subject)
    if not match:
        problems.append("use 'type(scope): imperative summary' or 'type: imperative summary'")
        return problems

    commit_type = match.group("type")
    summary = match.group("summary")
    if commit_type not in ALLOWED_TYPES:
        problems.append(f"unsupported type '{commit_type}'")
    if vague_summary_key(summary) in VAGUE_SUMMARIES:
        problems.append("summary is too vague")
    return problems


def push_subjects(payload: dict[str, Any]) -> list[str]:
    """Return commit subjects from a GitHub push event payload."""
    raw_commits = payload.get("commits")
    commit_list = raw_commits if isinstance(raw_commits, list) else []
    subject_list = [
        first_line(str(commit.get("message", "")))
        for commit in commit_list
        if isinstance(commit, dict)
    ]
    if not subject_list and isinstance(payload.get("head_commit"), dict):
        subject_list.append(first_line(str(payload["head_commit"].get("message", ""))))
    return [subject for subject in subject_list if subject]


def pull_request_subjects(payload: dict[str, Any]) -> list[str]:
    """Return the title from a GitHub pull request event payload."""
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict):
        return []
    title = str(pull_request.get("title", "")).strip()
    return [title] if title else []


def event_subjects(event_path: Path) -> list[str]:
    """Return subjects to validate from a GitHub event JSON file."""
    event_payload = json.loads(event_path.read_text(encoding="utf-8"))
    subject_list = pull_request_subjects(event_payload)
    if subject_list:
        return subject_list
    return push_subjects(event_payload)


def git_subjects(arguments: list[str]) -> list[str]:
    """Return subjects from git log for the given revision arguments."""
    git_command_parts = ["git", "log", "--format=%s", *arguments]
    completed = subprocess.run(git_command_parts, check=True, text=True, capture_output=True)
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", type=Path, help="GitHub event JSON payload")
    parser.add_argument("--message", action="append", default=[], help="Commit subject or full message")
    parser.add_argument("--last", type=int, help="Validate the last N git commits")
    parser.add_argument("--range", dest="commit_range", help="Validate a git revision range")
    return parser.parse_args(argv)


def cli_subjects(args: argparse.Namespace) -> list[str]:
    """Return all subjects requested by command line options."""
    subject_list = [first_line(message) for message in args.message]
    if args.event:
        subject_list.extend(event_subjects(args.event))
    if args.last:
        subject_list.extend(git_subjects([f"-n{args.last}"]))
    if args.commit_range:
        subject_list.extend(git_subjects([args.commit_range]))
    return [subject for subject in subject_list if subject]


def print_problems(problems_by_subject: list[tuple[str, list[str]]]) -> None:
    """Print validation failures in a CI-friendly format."""
    print("Commit message policy failed:")
    for subject, problems in problems_by_subject:
        print(f"  {subject}")
        for problem in problems:
            print(f"    - {problem}")
    print("\nExpected: type(scope): imperative summary")
    print("Allowed types: " + ", ".join(sorted(ALLOWED_TYPES)))


def main(argv: list[str] | None = None) -> int:
    """Run the commit message policy check."""
    args = parse_args(argv or sys.argv[1:])
    subject_list = cli_subjects(args)
    if not subject_list:
        print("No commit subjects found to validate.", file=sys.stderr)
        return 2

    subject_problem_pairs = [
        (subject, problems)
        for subject in subject_list
        if (problems := validate_subject(subject))
    ]
    if subject_problem_pairs:
        print_problems(subject_problem_pairs)
        return 1
    print(f"Commit message policy OK ({len(subject_list)} subject(s)).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
