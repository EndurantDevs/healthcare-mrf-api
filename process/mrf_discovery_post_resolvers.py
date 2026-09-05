# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded discovery POST requests and BCBSNC ASO employer search."""

from __future__ import annotations

import json
import posixpath
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlsplit

from process.tin_npi_connector_security import normalize_ein

if TYPE_CHECKING:
    import aiohttp

    from process.mrf_source_discovery import CrawlTarget


async def post_discovery_text(
    url: str,
    request_payload: str,
    *,
    headers: dict[str, str] | None,
    max_bytes: int,
    session: aiohttp.ClientSession | None,
    allow_redirects: bool,
) -> str:
    """Post bounded text while preserving session ownership and redirect policy."""
    from process import mrf_source_discovery as discovery

    await discovery._assert_fetch_url_allowed(url)
    timeout = discovery.aiohttp.ClientTimeout(
        total=discovery.HTTP_TOTAL_TIMEOUT,
        connect=15,
        sock_read=discovery.HTTP_READ_TIMEOUT,
    )
    async with discovery._discovery_http_session(
        existing_session=session, timeout=timeout, connector_limit=0
    ) as request_session:
        async with request_session.post(
            url,
            data=request_payload,
            headers=headers or {},
            allow_redirects=allow_redirects,
            **discovery._request_ssl_kwargs(url),
        ) as response:
            if not allow_redirects and (
                300 <= response.status < 400 or str(response.url) != url
            ):
                raise ValueError("POST redirect response is not allowed")
            body, charset = await discovery._read_text_response(
                response,
                max_bytes=max_bytes,
                expect_json=False,
                allow_browser_fallback=False,
            )
    return discovery._decode_response_body(body, charset=charset)


def _is_bcbsnc_aso_result_for_ein(
    search_result: dict[str, Any], ein_digits: str
) -> bool:
    from process import mrf_source_discovery as discovery

    meta = search_result.get("meta")
    if not isinstance(meta, dict):
        return False
    toc_url = discovery._clean_text(meta.get("url"))
    parsed = urlsplit(toc_url)
    normalized_path = posixpath.normpath(unquote(parsed.path))
    return (
        parsed.scheme.lower() == "https"
        and parsed.netloc.lower() == "mrfmftprod.bcbsnc.com"
        and normalized_path.startswith(
            "/prod/etl/outbound/table-of-contents/aso/"
        )
        and re.findall(
            r"(?<![0-9])[0-9]{9}(?![0-9])",
            posixpath.basename(normalized_path),
        )
        == [ein_digits]
    )


def _bcbsnc_aso_search_endpoint(resolver: dict[str, Any]) -> str:
    endpoint = str(resolver["endpoint"])
    if endpoint != (
        "https://apiservices-ext.bcbsnc.com/bcbsnc/prod/es/mssearch/api/v1/search"
    ):
        raise ValueError("invalid BCBSNC ASO employer search endpoint")
    return endpoint


def _bcbsnc_aso_search_body(
    resolver: dict[str, Any], ein_digits: str
) -> dict[str, Any]:
    return {
        "text": [f"{ein_digits}~1"],
        "size": int(resolver["results_per_page"]),
        "from": 0,
        "shoulds": {},
        "advancedSearch": {
            "sort": {"field": "meta.groupname.keyword", "order": "ASC"}
        },
        "aggs": True,
        "frontEnd": str(resolver["front_end_id"]),
        "datasource": "",
        "datasourceId": "",
        "datasourceType": "",
        "minimumShouldMatch": 1,
        "collections": [str(resolver["collection_id"])],
    }


def _bcbsnc_aso_exact_search_result(
    response_payload: Any, ein_digits: str, page_size: int
) -> dict[str, Any]:
    if not isinstance(response_payload, dict):
        raise ValueError("invalid BCBSNC ASO employer search response")
    search_results = response_payload.get("results")
    total_hits = response_payload.get("totalHits")
    if (
        not isinstance(search_results, list)
        or not isinstance(response_payload.get("keyMatches"), list)
        or not isinstance(total_hits, int)
        or isinstance(total_hits, bool)
        or total_hits > page_size
        or len(search_results) != total_hits
    ):
        raise ValueError("incomplete BCBSNC ASO employer search response")
    exact_search_results = [
        search_result
        for search_result in search_results
        if isinstance(search_result, dict)
        and _is_bcbsnc_aso_result_for_ein(search_result, ein_digits)
    ]
    if len(exact_search_results) != 1:
        outcome = "no exact" if not exact_search_results else "ambiguous"
        raise ValueError(f"{outcome} BCBSNC ASO employer search result")
    return exact_search_results[0]


def _bcbsnc_aso_crawl_target(
    source_row: dict[str, Any],
    resolved_from_url: str,
    resolver: dict[str, Any],
    ein_digits: str,
    matched_search_result: dict[str, Any],
) -> CrawlTarget:
    from process import mrf_source_discovery as discovery

    result_metadata = matched_search_result["meta"]
    toc_url = discovery._clean_text(result_metadata.get("url"))
    toc_crawl_target = discovery._direct_toc_crawl_target(
        source_row,
        toc_url,
        resolver="bcbsnc_aso_employer_search",
        target_max_bytes=int(resolver["toc_max_bytes"]),
    )
    if toc_crawl_target is None:
        raise ValueError("BCBSNC ASO employer search result is not a direct TOC")
    source_context = discovery._source_query_context_metadata(source_row)
    employer_name = (
        discovery._clean_text(result_metadata.get("groupname"))
        or discovery._clean_text(source_context.get("employer_name"))
        or discovery._clean_text(discovery._source_target_payer_query(source_row))
    )
    plan_info_rows = [
        {
            "plan_id": ein_digits,
            "plan_id_type": "ein",
            "plan_market_type": "group",
            "plan_name": employer_name,
            "plan_sponsor_name": employer_name,
            "issuer_name": source_row.get("display_name"),
        }
    ]
    return discovery.CrawlTarget(
        source=source_row,
        url=toc_crawl_target.url,
        label=employer_name,
        resolved_from_url=resolved_from_url,
        metadata={
            **toc_crawl_target.metadata,
            **source_context,
            "resolver": "bcbsnc_aso_employer_search",
            "query_context_match": True,
            "query_context_match_scope": "employer_identity",
            "bcbsnc_search_endpoint": _bcbsnc_aso_search_endpoint(resolver),
            "bcbsnc_search_result_id": discovery._clean_text(matched_search_result.get("id")),
            "bcbsnc_matched_group_name": employer_name,
            "company_name": employer_name,
            "employer_name": employer_name,
            "ein": ein_digits,
            "plan_info": plan_info_rows,
        },
    )


async def _resolve_bcbsnc_aso_employer_search(
    source_row: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    from process import mrf_source_discovery as discovery

    employer_ein = discovery._source_query_context_value(
        source_row, "query_context_employer_ein"
    )
    if employer_ein in (None, ""):
        return []
    try:
        ein_digits = normalize_ein(employer_ein)
    except ValueError:
        raise ValueError(
            "BCBSNC ASO employer search requires a 9-digit EIN"
        ) from None

    endpoint = _bcbsnc_aso_search_endpoint(resolver)
    model = str(resolver["model"])
    page_size = int(resolver["results_per_page"])
    response_payload = discovery._loads_mrf_json_value(
        await discovery._post_text(
            endpoint,
            json.dumps(_bcbsnc_aso_search_body(resolver, ein_digits)),
            headers={"Content-Type": "application/JSON", "model": model},
            max_bytes=int(resolver["max_bytes"]),
            session=session,
            allow_redirects=False,
        )
    )
    matched_search_result = _bcbsnc_aso_exact_search_result(
        response_payload, ein_digits, page_size
    )
    return [
        _bcbsnc_aso_crawl_target(
            source_row,
            url,
            resolver,
            ein_digits,
            matched_search_result,
        )
    ]
