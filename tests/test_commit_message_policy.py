import importlib.util
import json
from pathlib import Path

import pytest


def load_policy_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "check_commit_messages.py"
    spec = importlib.util.spec_from_file_location("check_commit_messages", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "subject",
    [
        "fix(api): handle upstream timeout",
        "feat(ptg)!: require explicit source routing",
        "docs: explain commit message style",
        "Merge pull request #123 from EndurantDevs/example",
        "Revert \"fix(api): handle upstream timeout\"",
        "Bump actions/checkout from 4 to 5",
    ],
)
def test_accepts_clear_commit_subjects(subject):
    module = load_policy_module()
    assert module.validate_subject(subject) == []


@pytest.mark.parametrize(
    "subject",
    [
        "",
        "fix",
        "fix: fix",
        "update stuff",
        "feature(api): add route",
        "fix(API): handle timeout",
        "fix(api): handle timeout.",
        "fix(api) handle timeout",
    ],
)
def test_rejects_unclear_commit_subjects(subject):
    module = load_policy_module()
    assert module.validate_subject(subject)


def test_reads_push_event_subjects(tmp_path):
    module = load_policy_module()
    event_path = tmp_path / "push.json"
    event_path.write_text(
        json.dumps(
            {
                "commits": [
                    {"message": "fix(api): handle timeout\n\nBody text."},
                    {"message": "docs: explain commit style"},
                ]
            }
        ),
        encoding="utf-8",
    )

    assert module.event_subjects(event_path) == [
        "fix(api): handle timeout",
        "docs: explain commit style",
    ]


def test_reads_pull_request_title(tmp_path):
    module = load_policy_module()
    event_path = tmp_path / "pull_request.json"
    event_path.write_text(
        json.dumps({"pull_request": {"title": "ci(commit): add message gate"}}),
        encoding="utf-8",
    )

    assert module.event_subjects(event_path) == ["ci(commit): add message gate"]


def test_main_accepts_direct_message(capsys):
    module = load_policy_module()
    exit_code = module.main(["--message", "fix(api): handle timeout"])

    assert not exit_code
    assert "policy OK" in capsys.readouterr().out


def test_main_rejects_unclear_message(capsys):
    module = load_policy_module()
    exit_code = module.main(["--message", "update stuff"])

    assert exit_code
    assert "policy failed" in capsys.readouterr().out
