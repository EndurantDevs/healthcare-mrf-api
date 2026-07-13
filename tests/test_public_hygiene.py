from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = REPO_ROOT / "scripts" / "ci" / "public_hygiene.py"
SPEC = importlib.util.spec_from_file_location("public_hygiene", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
PUBLIC_HYGIENE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLIC_HYGIENE)


PRIVATE_FRAGMENT_GROUPS = (
    ("ipa", "ppa"),
    ("ipa", "reyal"),
    ("ipa", "pcm"),
    ("tropmi", "ipa"),
    ("tropmi", "lortnoc"),
    ("ph", "tropmi", "lortnoc"),
    ("atrophtlaeh", "pcm"),
)


def private_example(parts: tuple[str, ...], separator: str) -> str:
    return separator.join(part[::-1] for part in parts)


class PublicHygieneTests(unittest.TestCase):
    def test_private_names_match_separator_variants(self) -> None:
        for parts in PRIVATE_FRAGMENT_GROUPS:
            for separator in ("", "-", "_", "."):
                with self.subTest(parts=parts, separator=separator):
                    value = private_example(parts, separator)
                    self.assertTrue(
                        PUBLIC_HYGIENE.has_private_text_fingerprint(value)
                    )

    def test_private_identifier_words_in_generic_prose_are_allowed(self) -> None:
        for parts in PRIVATE_FRAGMENT_GROUPS:
            with self.subTest(parts=parts):
                value = private_example(parts, " ")
                self.assertFalse(
                    PUBLIC_HYGIENE.has_private_text_fingerprint(value)
                )

    def test_private_names_are_detected_in_paths_and_content(self) -> None:
        value = private_example(PRIVATE_FRAGMENT_GROUPS[4], "-")
        path_errors = PUBLIC_HYGIENE.check_paths(
            [Path("docs") / f"{value}-contract.md"]
        )
        self.assertEqual(len(path_errors), 1)
        self.assertTrue(path_errors[0].startswith("private-path-fingerprint:"))

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
        self.assertEqual(
            PUBLIC_HYGIENE.PATTERN_EXEMPT_PATHS,
            {"scripts/ci/public_hygiene.py"},
        )
        self.assertNotIn(
            ".github/workflows/ci.yml",
            PUBLIC_HYGIENE.PATTERN_EXEMPT_PATHS,
        )

    def test_checker_exemption_still_applies_private_fingerprints(self) -> None:
        checker_path = Path("scripts/ci/public_hygiene.py")
        protected_text = private_example(PRIVATE_FRAGMENT_GROUPS[2], "_")
        with mock.patch.object(
            PUBLIC_HYGIENE,
            "is_binary",
            return_value=False,
        ), mock.patch.object(Path, "read_text", return_value=protected_text):
            errors = PUBLIC_HYGIENE.check_content([checker_path])

        self.assertEqual(
            errors,
            [f"private-example-fingerprint: {checker_path}"],
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

    def test_tracked_test_source_passes_the_content_gate(self) -> None:
        path = Path("tests/test_public_hygiene.py")
        self.assertEqual(PUBLIC_HYGIENE.check_content([path]), [])


if __name__ == "__main__":
    unittest.main()
