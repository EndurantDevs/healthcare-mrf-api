# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint.pagination import parse_bool_alias, parse_pagination


def test_parse_pagination_defaults_to_page_mode():
    params = parse_pagination({}, default_limit=50, max_limit=200, default_page=1)
    assert params.page == 1
    assert params.limit == 50
    assert params.offset == 0
    assert params.source == "page"


def test_parse_pagination_accepts_page_size_alias():
    params = parse_pagination(
        {"page": "2", "page_size": "25"},
        default_limit=50,
        max_limit=200,
        default_page=1,
        allow_page_size=True,
    )
    assert params.page == 2
    assert params.limit == 25
    assert params.offset == 25


def test_parse_pagination_accepts_offset_mode():
    params = parse_pagination(
        {"offset": "40", "limit": "20"},
        default_limit=50,
        max_limit=200,
        default_page=1,
    )
    assert params.offset == 40
    assert params.page == 3
    assert params.source == "offset"


def test_parse_pagination_rejects_limit_page_size_conflict():
    with pytest.raises(InvalidUsage):
        parse_pagination(
            {"limit": "10", "page_size": "20"},
            default_limit=50,
            max_limit=200,
            default_page=1,
            allow_page_size=True,
        )


def test_parse_pagination_rejects_page_offset_conflict():
    with pytest.raises(InvalidUsage):
        parse_pagination(
            {"page": "3", "limit": "25", "offset": "10"},
            default_limit=50,
            max_limit=200,
            default_page=1,
        )


def test_parse_pagination_rejects_negative_values():
    with pytest.raises(InvalidUsage):
        parse_pagination(
            {"offset": "-1"},
            default_limit=50,
            max_limit=200,
            default_page=1,
        )


def test_parse_pagination_rejects_bad_default_config():
    with pytest.raises(ValueError):
        parse_pagination({}, default_limit=0, max_limit=200, default_page=1)


def test_parse_pagination_rejects_offset_start_conflict():
    with pytest.raises(InvalidUsage):
        parse_pagination(
            {"offset": "10", "start": "20"},
            default_limit=50,
            max_limit=200,
            default_page=1,
        )


def test_parse_pagination_rejects_default_page_below_one():
    with pytest.raises(InvalidUsage):
        parse_pagination({}, default_limit=50, max_limit=200, default_page=0)


def test_parse_bool_alias_supports_primary_and_alias():
    assert (
        parse_bool_alias({"include_facets": "false"}, "include_facets", "include_aggregations", default=True)
        is False
    )
    assert (
        parse_bool_alias({"include_aggregations": "0"}, "include_facets", "include_aggregations", default=True)
        is False
    )
    assert (
        parse_bool_alias({}, "include_facets", "include_aggregations", default=True)
        is True
    )
    assert (
        parse_bool_alias({"include_facets": True}, "include_facets", "include_aggregations", default=False)
        is True
    )


def test_parse_bool_alias_rejects_conflicting_values():
    with pytest.raises(InvalidUsage):
        parse_bool_alias(
            {"include_facets": "true", "include_aggregations": "false"},
            "include_facets",
            "include_aggregations",
            default=True,
        )


def test_parse_bool_alias_rejects_invalid_token():
    with pytest.raises(InvalidUsage):
        parse_bool_alias(
            {"include_facets": "definitely"},
            "include_facets",
            "include_aggregations",
            default=True,
        )
