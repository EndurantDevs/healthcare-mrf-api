# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import provider_references, source_jobs


def test_toc_url_file_reader_handles_absent_empty_and_line_sources(tmp_path):
    missing = tmp_path / "missing.txt"
    empty = tmp_path / "empty.txt"
    lines = tmp_path / "lines.txt"
    empty.write_text(" \n", encoding="utf-8")
    lines.write_text(
        " https://example.test/first.json \n\n"
        "https://example.test/second.json\n"
        "https://example.test/first.json\n",
        encoding="utf-8",
    )

    assert source_jobs._load_toc_urls_from_file(str(missing)) == []
    assert source_jobs._load_toc_urls_from_file(str(empty)) == []
    assert source_jobs._load_toc_urls_from_file(str(lines)) == [
        "https://example.test/first.json",
        "https://example.test/second.json",
    ]


def test_toc_url_file_reader_validates_json_lists(tmp_path):
    valid = tmp_path / "valid.json"
    invalid = tmp_path / "invalid.json"
    valid.write_text(
        '[" https://example.test/one.json ", "", 7, '
        '"https://example.test/one.json", "https://example.test/two.json"]',
        encoding="utf-8",
    )
    invalid.write_text("[not-json", encoding="utf-8")

    assert source_jobs._load_toc_urls_from_file(str(valid)) == [
        "https://example.test/one.json",
        "https://example.test/two.json",
    ]
    assert source_jobs._load_toc_urls_from_file(str(invalid)) == []


def test_toc_url_file_reader_accepts_supported_mapping_values(tmp_path, monkeypatch):
    """Exercise the mapping contract independently of JSON source detection."""

    source = tmp_path / "mapping.json"
    source.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(
        source_jobs.json,
        "loads",
        lambda _text: {
            "single": " https://example.test/one.json ",
            "many": ["https://example.test/two.json", " ", 3],
            "ignored": None,
        },
    )

    assert source_jobs._load_toc_urls_from_file(str(source)) == [
        "https://example.test/one.json",
        "https://example.test/two.json",
        "3",
    ]


def test_source_job_merge_preserves_unique_plans_and_fills_missing_metadata():
    existing_by_field = {
        "plan_info": [{"plan_id": "one"}, "legacy-entry"],
        "description": None,
    }
    incoming_by_field = {
        "plan_info": [
            {"plan_id": "one"},
            "ignored-entry",
            {"plan_id": "two"},
        ],
        "description": "test import",
        "from_index_url": "https://example.test/index.json",
        "meta": {"source": "test"},
    }

    source_jobs._merge_ptg_job(existing_by_field, incoming_by_field)

    assert existing_by_field == {
        "plan_info": [
            {"plan_id": "one"},
            "legacy-entry",
            {"plan_id": "two"},
        ],
        "description": "test import",
        "from_index_url": "https://example.test/index.json",
        "meta": {"source": "test"},
    }

    source_jobs._merge_ptg_job(
        existing_by_field,
        {
            "description": "replacement",
            "from_index_url": "https://example.test/other.json",
            "meta": {"source": "other"},
        },
        known_plan_identities={source_jobs._plan_identity({"plan_id": "two"})},
    )
    assert existing_by_field["description"] == "test import"
    assert existing_by_field["from_index_url"] == "https://example.test/index.json"
    assert existing_by_field["meta"] == {"source": "test"}


def test_source_job_helpers_retain_unkeyed_rows_and_reject_market_mismatch():
    assert source_jobs._dedupe_rows_by(
        [
            {"key": 1, "value": "old"},
            {"key": 1, "value": "new"},
            {"value": "unkeyed"},
        ],
        "key",
    ) == [
        {"key": 1, "value": "new"},
        {"value": "unkeyed"},
    ]
    assert source_jobs._is_plan_matching_filters(
        {"plan_market_type": "group"},
        plan_market_types=["individual"],
    ) is False


class _RecordingProviderCache:
    def __init__(self):
        self.commit_count = 0

    def commit(self):
        self.commit_count += 1


def _install_provider_reader_fakes(monkeypatch, reader_case):
    """Install one explicit provider-reference stream and persistence contract."""

    monkeypatch.setattr(
        provider_references,
        "open_json_artifact_stream",
        lambda _path: nullcontext(object()),
    )
    monkeypatch.setattr(
        provider_references.ijson,
        "items",
        lambda *_args: iter(reader_case.reference_list),
    )
    monkeypatch.setattr(
        provider_references,
        "_normalize_provider_ref",
        lambda value: value,
    )
    monkeypatch.setattr(
        provider_references,
        "_build_provider_set_entry",
        lambda **kwargs: reader_case.build_entry(kwargs["provider_group_ref"]),
    )
    monkeypatch.setattr(
        provider_references,
        "_env_int",
        lambda *_args: reader_case.batch_row_count,
    )
    monkeypatch.setattr(
        provider_references,
        "_push_objects_from_facade",
        reader_case.push_mock,
    )
    monkeypatch.setattr(
        provider_references,
        "_provider_cache_put",
        reader_case.cache_put_mock,
    )
    monkeypatch.setattr(
        provider_references,
        "_ptg_facade",
        lambda: reader_case.facade,
    )


def _batched_provider_reader_case():
    reference_list = [
        {"provider_group_id": "invalid", "provider_groups": []},
        {"provider_group_id": "one", "provider_groups": [{}]},
        {"provider_group_id": "one-again", "provider_groups": [{}]},
        {"provider_group_id": "two", "provider_groups": [{}]},
        {
            "provider_group_id": "three",
            "provider_groups": [{}],
            "network_names": ["test-network"],
        },
    ]
    entry_by_reference = {
        "invalid": (None, None),
        "one": ({"__hash__": 1}, {"provider_group_hash": 1}),
        "one-again": ({"__hash__": 1}, {"provider_group_hash": 1}),
        "two": ({"__hash__": 2}, {"provider_group_hash": 2}),
        "three": ({"__hash__": 3}, {"provider_group_hash": 3}),
    }
    flush_mock = AsyncMock()
    return SimpleNamespace(
        reference_list=reference_list,
        build_entry=lambda reference: entry_by_reference[reference],
        batch_row_count=2,
        push_mock=AsyncMock(),
        cache_put_mock=Mock(),
        cache=_RecordingProviderCache(),
        facade=SimpleNamespace(
            TEST_PROVIDER_GROUPS=100,
            flush_error_log=flush_mock,
        ),
        flush_mock=flush_mock,
    )


def _limited_provider_reader_case():
    return SimpleNamespace(
        reference_list=[
            {"provider_group_id": "one", "provider_groups": [{}]},
            {"provider_group_id": "two", "provider_groups": [{}]},
        ],
        build_entry=lambda reference: (
            {"__hash__": reference},
            {"provider_group_hash": reference},
        ),
        batch_row_count=0,
        push_mock=AsyncMock(),
        cache_put_mock=Mock(),
        facade=SimpleNamespace(
            TEST_PROVIDER_GROUPS=1,
            flush_error_log=AsyncMock(),
        ),
    )


@pytest.mark.asyncio
async def test_provider_reference_reader_batches_unique_rows_and_commits_cache(
    monkeypatch,
):
    """Batch only unique provider rows while committing the bounded cache."""

    reader_case = _batched_provider_reader_case()
    monkeypatch.setattr(
        provider_references,
        "PTG2ProviderReferenceCache",
        _RecordingProviderCache,
    )
    _install_provider_reader_fakes(monkeypatch, reader_case)

    await provider_references._load_provider_references_from_file(
        "provider-references.json",
        7,
        "provider-class",
        reader_case.cache,
        False,
        "log-class",
        "https://example.test/provider-references.json",
    )

    assert [call.args[0] for call in reader_case.push_mock.await_args_list] == [
        [
            {"provider_group_hash": 1},
            {"provider_group_hash": 2},
        ],
        [{"provider_group_hash": 3}],
    ]
    assert reader_case.cache_put_mock.call_count == 4
    assert reader_case.cache.commit_count == 1
    reader_case.flush_mock.assert_awaited_once_with("log-class")


@pytest.mark.asyncio
async def test_provider_reference_reader_honors_test_limit_and_empty_rows(
    monkeypatch,
):
    """Stop at the test limit and avoid writes when every row is invalid."""

    reader_case = _limited_provider_reader_case()
    _install_provider_reader_fakes(monkeypatch, reader_case)

    await provider_references._load_provider_references_from_file(
        "provider-references.json",
        7,
        "provider-class",
        {},
        True,
        "log-class",
        "https://example.test/provider-references.json",
    )

    reader_case.push_mock.assert_awaited_once_with(
        [{"provider_group_hash": "one"}],
        "provider-class",
    )
    reader_case.cache_put_mock.assert_called_once()
    reader_case.facade.flush_error_log.assert_awaited_once_with("log-class")

    monkeypatch.setattr(
        provider_references,
        "_build_provider_set_entry",
        lambda **_kwargs: (None, None),
    )
    reader_case.push_mock.reset_mock()
    await provider_references._load_provider_references_from_file(
        "provider-references.json",
        7,
        "provider-class",
        {},
        False,
        "log-class",
        "https://example.test/provider-references.json",
    )
    reader_case.push_mock.assert_not_awaited()
