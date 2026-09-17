from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = REPO_ROOT / "scripts" / "ci" / "public_hygiene.py"
BENCHMARK_PROFILE_PATH = REPO_ROOT / ".agents" / "endurant-harness-benchmarks.json"
SPEC = importlib.util.spec_from_file_location("public_hygiene", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
PUBLIC_HYGIENE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLIC_HYGIENE)


SYNTHETIC_FRAGMENT_GROUPS = (
    ("synthetic", "project"),
    ("synthetic", "service", "config"),
)


def synthetic_example(parts: tuple[str, ...], separator: str) -> str:
    return separator.join(parts)


class PublicHygieneTests(unittest.TestCase):
    def setUp(self) -> None:
        fingerprints = {
            hashlib.sha256("".join(parts).encode()).hexdigest()
            for parts in SYNTHETIC_FRAGMENT_GROUPS
        }
        patcher = mock.patch.object(
            PUBLIC_HYGIENE, "PRIVATE_INTEGRATION_FINGERPRINTS", fingerprints,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_retained_benchmark_uses_ambient_database_credentials(self) -> None:
        profile = json.loads(BENCHMARK_PROFILE_PATH.read_text(encoding="utf-8"))
        environment = profile["benchmarks"][
            "provider-directory-retained-publication"
        ]["command"]["env"]

        self.assertTrue(
            {"HLTHPRT_DB_USER", "HLTHPRT_DB_PASSWORD"}.isdisjoint(environment)
        )

    def test_private_names_match_separator_variants(self) -> None:
        for parts in SYNTHETIC_FRAGMENT_GROUPS:
            for separator in ("", "-", "_", ".", "/", ":", "\\"):
                with self.subTest(parts=parts, separator=separator):
                    value = synthetic_example(parts, separator)
                    self.assertTrue(
                        PUBLIC_HYGIENE.has_private_text_fingerprint(value)
                    )

    def test_private_identifier_words_in_generic_prose_are_allowed(self) -> None:
        for parts in SYNTHETIC_FRAGMENT_GROUPS:
            with self.subTest(parts=parts):
                value = synthetic_example(parts, " ")
                self.assertFalse(
                    PUBLIC_HYGIENE.has_private_text_fingerprint(value)
                )

    def test_private_names_are_detected_in_paths_and_content(self) -> None:
        value = synthetic_example(SYNTHETIC_FRAGMENT_GROUPS[0], "-")
        path_errors = PUBLIC_HYGIENE.check_paths(
            [Path("docs") / f"{value}-contract.md"]
        )
        self.assertEqual(len(path_errors), 1)
        self.assertTrue(path_errors[0].startswith("private-path-fingerprint:"))
        self.assertNotIn(value, str(path_errors))

        with tempfile.TemporaryDirectory() as directory:
            content_path = Path(directory) / "contract.txt"
            content_path.write_text(f"integration: {value}\n", encoding="utf-8")
            content_errors = PUBLIC_HYGIENE.check_content([content_path])
        self.assertEqual(len(content_errors), 1)
        self.assertTrue(
            content_errors[0].startswith("private-example-fingerprint:")
        )

    def test_generic_operator_and_public_source_text_is_allowed(self) -> None:
        allowed = (
            "Authenticated operator API for an independently operated client.\n"
            "Public payer source registry populated from an official CMS URL.\n"
            "https://data.cms.gov/provider-data\n"
        )
        self.assertFalse(PUBLIC_HYGIENE.has_private_text_fingerprint(allowed))

    def test_only_checker_implementation_is_exempt(self) -> None:
        self_category = "agent" + "ic-development-reference"
        self.assertEqual(
            PUBLIC_HYGIENE.PATTERN_EXEMPTIONS,
            {"scripts/ci/public_hygiene.py": {self_category}},
        )
        self.assertNotIn(
            ".github/workflows/ci.yml",
            PUBLIC_HYGIENE.PATTERN_EXEMPTIONS,
        )

    def test_checker_exemption_still_applies_private_fingerprints(self) -> None:
        checker_path = Path("scripts/ci/public_hygiene.py")
        protected_text = synthetic_example(SYNTHETIC_FRAGMENT_GROUPS[0], "_")
        credential_text = (
            "ghp_"
            + "a" * 20
            + "\npostgresql://user:"
            + "credential@example.test/db\n"
            + "pass"
            + "word='credential'"
        )
        with mock.patch.object(
            PUBLIC_HYGIENE,
            "is_binary",
            return_value=False,
        ), mock.patch.object(
            Path,
            "read_text",
            return_value="agent" + f"ic {protected_text}\n{credential_text}",
        ):
            errors = PUBLIC_HYGIENE.check_content([checker_path])

        self.assertEqual(
            errors,
            [
                "github-token: file 1",
                "database-url-with-password: file 1",
                "password-assignment: file 1",
                "private-example-fingerprint: file 1",
            ],
        )

    def test_repository_files_defaults_to_tracked_files(self) -> None:
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=b"z.py\0a\nb.py\0z.py\0",
        )
        with mock.patch.object(
            PUBLIC_HYGIENE.subprocess,
            "run",
            return_value=completed,
        ) as run:
            paths = PUBLIC_HYGIENE.repository_files()

        self.assertEqual(paths, [Path("a\nb.py"), Path("z.py")])
        self.assertEqual(
            run.call_args.args[0],
            ["git", "ls-files", "-z", "--cached"],
        )

    def test_include_untracked_is_explicit(self) -> None:
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=b"tracked.py\0local.py\0",
        )
        with mock.patch.object(
            PUBLIC_HYGIENE.subprocess,
            "run",
            return_value=completed,
        ) as run:
            paths = PUBLIC_HYGIENE.repository_files(include_untracked=True)

        self.assertEqual(paths, [Path("local.py"), Path("tracked.py")])
        self.assertEqual(
            run.call_args.args[0],
            [
                "git",
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ],
        )

    def test_deleted_tracked_paths_are_not_scanned(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            existing = Path(directory) / "present.txt"
            missing = Path(directory) / "deleted-private-contract.md"
            existing.write_text("public", encoding="utf-8")

            self.assertEqual(
                PUBLIC_HYGIENE.existing_files([missing, existing]),
                [existing],
            )

    def test_deleted_tracked_path_names_are_still_checked(self) -> None:
        missing = Path(sorted(PUBLIC_HYGIENE.FORBIDDEN_PATH_PARTS)[0]) / "deleted.txt"
        with mock.patch.object(PUBLIC_HYGIENE, "repository_files", return_value=[missing]), \
                mock.patch.dict(PUBLIC_HYGIENE.os.environ, {}, clear=True):
            self.assertEqual(PUBLIC_HYGIENE.main([]), 1)

    def test_tracked_test_source_passes_the_content_gate(self) -> None:
        path = Path("tests/test_public_hygiene.py")
        with mock.patch.object(PUBLIC_HYGIENE, "PRIVATE_INTEGRATION_FINGERPRINTS", set()):
            self.assertEqual(PUBLIC_HYGIENE.check_content([path]), [])

    def test_body_only_edits_are_checked(self) -> None:
        rejected = synthetic_example(SYNTHETIC_FRAGMENT_GROUPS[0], "-")
        event_payload_map = {
            "action": "edited", "changes": {"body": {"from": "Public behavior."}},
            "pull_request": {
                "title": "docs: describe public behavior", "body": rejected,
                "head": {"ref": "fix/public-behavior"},
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "event.json"
            event_path.write_text(json.dumps(event_payload_map), encoding="utf-8")
            output = io.StringIO()
            with mock.patch.object(PUBLIC_HYGIENE, "repository_files", return_value=[]), \
                    mock.patch.dict(PUBLIC_HYGIENE.os.environ, {
                        "GITHUB_EVENT_PATH": str(event_path), "GITHUB_EVENT_NAME": "pull_request",
                    }), redirect_stdout(output):
                self.assertEqual(PUBLIC_HYGIENE.main([]), 1)
        self.assertIn("PR body", output.getvalue())
        self.assertNotIn(rejected, output.getvalue())

    def test_prepared_text_errors_are_redacted(self) -> None:
        rejected = synthetic_example(SYNTHETIC_FRAGMENT_GROUPS[0], "-")
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            PUBLIC_HYGIENE.os.environ, {}, clear=True,
        ):
            path = Path(directory) / f"{rejected}.txt"
            path.write_text(rejected, encoding="utf-8")
            args = PUBLIC_HYGIENE.parse_args(["--text-file", str(path), "--text-file", str(path)])
            args.event = None
            errors = PUBLIC_HYGIENE.check_metadata(args)
            self.assertEqual(len(errors), 2)
            self.assertNotIn(rejected, str(errors))
            path.write_bytes(b"\xff")
            with self.assertRaisesRegex(ValueError, "file 1 cannot be read"):
                PUBLIC_HYGIENE.check_metadata(args)
            path.write_text(f"public\0{rejected}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "file 1 is malformed"):
                PUBLIC_HYGIENE.check_metadata(args)

    def test_event_fields_use_shared_matcher(self) -> None:
        rejected = synthetic_example(SYNTHETIC_FRAGMENT_GROUPS[0], "_")
        event_payload_map = {
            "ref": "refs/heads/fix/public-behavior",
            "commits": [{"message": f"fix: preserve behavior\n\n{rejected}"}],
        }
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "event.json"
            event_path.write_text(json.dumps(event_payload_map), encoding="utf-8")
            self.assertTrue(PUBLIC_HYGIENE.check_event(event_path))
            event_payload_map = {"pull_request": {
                "title": "docs: explain public package", "body": None, "head": {"ref": rejected},
            }}
            event_path.write_text(json.dumps(event_payload_map), encoding="utf-8")
            self.assertTrue(PUBLIC_HYGIENE.check_event(event_path))
            event_payload_map["pull_request"]["head"]["ref"] = "docs/public-package"
            event_path.write_text(json.dumps(event_payload_map), encoding="utf-8")
            self.assertEqual(PUBLIC_HYGIENE.check_event(event_path), [])

    def test_malformed_events_fail_closed(self) -> None:
        malformed_list = [
            "not JSON", "[]", "{}", '{"pull_request": null}',
            json.dumps({"pull_request": {"title": "fix: retain behavior"}}),
            json.dumps({"commits": [], "ref": "refs/heads/dev"}),
            json.dumps({"commits": [{"message": 7}], "ref": "refs/heads/dev"}),
            json.dumps({"pull_request": {
                "title": "fix: retain behavior", "body": [], "head": {"ref": "fix/behavior"},
            }}),
        ]
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "event.json"
            for text in malformed_list:
                with self.subTest(text=text):
                    event_path.write_text(text, encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "missing or malformed"):
                        PUBLIC_HYGIENE.event_texts(event_path)

    def test_private_dispatch_does_not_default_metadata(self) -> None:
        with mock.patch.dict(PUBLIC_HYGIENE.os.environ, {
            "GITHUB_EVENT_PATH": "/missing-event.json", "GITHUB_EVENT_NAME": "workflow_dispatch",
        }):
            self.assertIsNone(PUBLIC_HYGIENE.parse_args([]).event)
            self.assertEqual(PUBLIC_HYGIENE.parse_args(["--event", "local.json"]).event, Path("local.json"))

    def test_declared_event_requires_a_path(self) -> None:
        for event_name in ("pull_request", "push"):
            with self.subTest(event_name=event_name), mock.patch.dict(
                PUBLIC_HYGIENE.os.environ, {"GITHUB_EVENT_NAME": event_name}, clear=True,
            ):
                with self.assertRaisesRegex(ValueError, "missing or malformed"):
                    PUBLIC_HYGIENE.check_metadata(PUBLIC_HYGIENE.parse_args([]))

    def test_forbidden_paths_have_redacted_diagnostics(self) -> None:
        paths = [
            Path(sorted(PUBLIC_HYGIENE.FORBIDDEN_PATH_PARTS)[0]) / "sample.txt",
            Path(sorted(PUBLIC_HYGIENE.FORBIDDEN_BASENAMES)[0]),
        ]
        errors = PUBLIC_HYGIENE.check_paths(paths)
        self.assertEqual(errors, [
            "forbidden path component: file 1", "forbidden instruction file: file 2",
        ])
        for path in paths:
            self.assertNotIn(str(path), str(errors))
        self.assertEqual(PUBLIC_HYGIENE.check_paths([Path("docs/public.md")]), [])

    def test_secret_pattern_errors_are_redacted(self) -> None:
        synthetic_marker = "github_" + "pat_" + "A" * 24
        errors = PUBLIC_HYGIENE.check_text(synthetic_marker, "PR body")
        self.assertEqual(errors, ["github-fine-grained-token: PR body"])
        self.assertNotIn(synthetic_marker, str(errors))

    def test_push_head_message_is_checked_independently(self) -> None:
        rejected = synthetic_example(SYNTHETIC_FRAGMENT_GROUPS[0], "-")
        event_payload_map = {
            "ref": "refs/heads/dev", "commits": [{"message": "fix: preserve public behavior"}],
            "head_commit": {"message": f"fix: preserve public behavior\n\n{rejected}"},
        }
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "push.json"
            event_path.write_text(json.dumps(event_payload_map), encoding="utf-8")
            errors = PUBLIC_HYGIENE.check_event(event_path)
        self.assertEqual(errors, ["private-example-fingerprint: push head commit"])
        self.assertNotIn(rejected, str(errors))

    def test_metadata_fields_reject_blank_and_multiline(self) -> None:
        for label in ("PR title", "PR head ref", "push ref"):
            for text in ("", " ", "two\nlines", "two\rlines", "nul\0text"):
                with self.subTest(label=label, text=text), self.assertRaises(ValueError):
                    PUBLIC_HYGIENE.validate_event_texts([(label, text)])

    def test_cli_handles_prepared_and_malformed_text(self) -> None:
        rejected = synthetic_example(SYNTHETIC_FRAGMENT_GROUPS[0], "-")
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            PUBLIC_HYGIENE, "repository_files", return_value=[],
        ), mock.patch.dict(PUBLIC_HYGIENE.os.environ, {}, clear=True):
            path = Path(directory) / f"{rejected}.json"
            path.write_text("Public package behavior and validation.", encoding="utf-8")
            output = io.StringIO()
            with redirect_stdout(output):
                status = PUBLIC_HYGIENE.main(["--text-file", str(path), "--text-file", str(path)])
            self.assertEqual(status, 0)
            self.assertIn("Public hygiene check passed", output.getvalue())
            path.write_text(rejected, encoding="utf-8")
            output = io.StringIO()
            with redirect_stdout(output):
                status = PUBLIC_HYGIENE.main(["--event", str(path)])
            self.assertEqual(status, 1)
            self.assertIn("metadata is missing or malformed", output.getvalue())
            self.assertNotIn(rejected, output.getvalue())


if __name__ == "__main__":
    unittest.main()
