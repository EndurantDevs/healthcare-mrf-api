import hashlib
import importlib.util
import json
import subprocess
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
    output = capsys.readouterr().out
    assert "policy failed" in output
    assert "commit message 1" in output
    assert "update stuff" not in output


def test_event_style_errors_report_trusted_label(tmp_path, capsys):
    module = load_policy_module()
    event_path = tmp_path / "pull_request.json"
    event_path.write_text(json.dumps({"pull_request": {
        "title": "update stuff", "body": "Public details.", "head": {"ref": "fix/public"},
    }}), encoding="utf-8")

    assert module.main(["--event", str(event_path)]) == 1
    output = capsys.readouterr().out
    assert "PR title" in output
    assert "update stuff" not in output


def test_reads_every_subject_after_a_git_range_base(tmp_path, monkeypatch):
    module = load_policy_module()
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=repository, check=True)
    subprocess.run(
        ["git", "config", "user.name", "CI Test"], cwd=repository, check=True
    )
    subprocess.run(
        ["git", "config", "user.email", "ci@example.invalid"],
        cwd=repository,
        check=True,
    )
    for subject in (
        "docs: establish range base",
        "fix(ci): validate protected push subjects",
        "test(ci): cover workflow dispatch subjects",
    ):
        subprocess.run(
            ["git", "commit", "--allow-empty", "--quiet", "-m", subject, "-m", "Public fixture details."],
            cwd=repository,
            check=True,
        )
        if subject == "docs: establish range base":
            base_sha = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repository,
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()

    monkeypatch.chdir(repository)
    assert module.git_subjects([f"{base_sha}..HEAD"]) == [
        "test(ci): cover workflow dispatch subjects",
        "fix(ci): validate protected push subjects",
    ]
    assert all("\n\nPublic fixture details." in message for message in module.git_messages([f"{base_sha}..HEAD"]))


@pytest.mark.parametrize("arguments", [["--message"], ["--last", "1"], ["--range", "HEAD~1..HEAD"]])
def test_full_messages_are_checked_first(arguments, monkeypatch, capsys):
    module = load_policy_module()
    rejected = "synthetic-project"
    fingerprint = hashlib.sha256(rejected.replace("-", "").encode()).hexdigest()
    monkeypatch.setitem(module.check_text.__globals__, "PRIVATE_INTEGRATION_FINGERPRINTS", {fingerprint})
    message = f"fix: preserve public behavior\n\n{rejected}"
    if arguments == ["--message"]:
        arguments = ["--message", message]
    else:
        monkeypatch.setattr(module, "git_messages", lambda _: [message])
    assert module.main(arguments) == 1
    output = capsys.readouterr()
    assert "private-example-fingerprint" in output.out
    assert rejected not in output.out + output.err


def test_sensitive_subject_is_never_echoed(monkeypatch, capsys):
    module = load_policy_module()
    rejected = "synthetic-project"
    fingerprint = hashlib.sha256(rejected.replace("-", "").encode()).hexdigest()
    monkeypatch.setitem(module.check_text.__globals__, "PRIVATE_INTEGRATION_FINGERPRINTS", {fingerprint})
    assert module.main(["--message", rejected]) == 1
    assert rejected not in capsys.readouterr().out


@pytest.mark.parametrize("event_kind", ["pull_request", "push", "malformed"])
def test_cli_events_reject_sensitive_bodies(event_kind, tmp_path, monkeypatch, capsys):
    module = load_policy_module()
    rejected = "synthetic-project"
    fingerprint = hashlib.sha256(rejected.replace("-", "").encode()).hexdigest()
    monkeypatch.setitem(module.check_text.__globals__, "PRIVATE_INTEGRATION_FINGERPRINTS", {fingerprint})
    event_payload_map = {
        "pull_request": {"title": "fix: preserve behavior", "body": rejected, "head": {"ref": "fix/public"}},
    }
    if event_kind == "push":
        event_payload_map = {"ref": "refs/heads/dev", "commits": [{"message": f"fix: preserve behavior\n\n{rejected}"}]}
    elif event_kind == "malformed":
        event_payload_map = {"pull_request": {"title": rejected}}
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event_payload_map), encoding="utf-8")
    assert module.main(["--event", str(event_path)]) == 1
    assert rejected not in capsys.readouterr().out


def test_git_failure_diagnostics_are_redacted(monkeypatch, capsys):
    module = load_policy_module()
    rejected = "synthetic-project"

    def fail_git_read(_):
        raise subprocess.CalledProcessError(1, ["git", rejected], output=rejected, stderr=rejected)

    monkeypatch.setattr(module, "git_messages", fail_git_read)
    assert module.main(["--range", rejected]) == 1
    output = capsys.readouterr()
    assert rejected not in output.out + output.err


@pytest.mark.parametrize("arguments", [["--last", "0"], ["--last", "-1"], ["--range=--format=%s"]])
def test_malformed_git_selection_is_rejected(arguments, monkeypatch, capsys):
    module = load_policy_module()
    monkeypatch.setattr(module, "git_messages", lambda _: pytest.fail("Invalid selection reached Git"))
    assert module.main(arguments) == 1
    assert "Requested" in capsys.readouterr().out
