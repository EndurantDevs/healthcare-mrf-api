# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure URL and query semantics for source-neutral rooted graph reads."""

from __future__ import annotations

from dataclasses import dataclass, field
import urllib.parse

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_PAGINATION,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PAGINATION,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE,
)
from process.provider_directory_rooted_graph_identity import (
    canonical_fhir_resource_id,
    provider_directory_rooted_graph_query_id,
)


ROOTED_GRAPH_QUERY_EXACT_SEARCH = "exact_reference_search"
ROOTED_GRAPH_QUERY_DIRECT_READ = "direct_read"
ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS = "full_insurance_plan_census"
_QUERY_KINDS = frozenset(
    {
        ROOTED_GRAPH_QUERY_EXACT_SEARCH,
        ROOTED_GRAPH_QUERY_DIRECT_READ,
        ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
    }
)


class ProviderDirectoryRootedGraphQueryError(ValueError):
    """Expose bounded validation codes without embedding source references."""

    def __init__(self, code: str = "query_invalid") -> None:
        message_by_code = {
            "api_base_invalid": "rooted graph API base is invalid",
            "direct_read_forbidden": "rooted graph direct read is forbidden",
            "insurance_plan_network_query_forbidden": (
                "rooted graph InsurancePlan network query is forbidden"
            ),
            "query_invalid": "rooted graph query is invalid",
            "search_forbidden": "rooted graph search is forbidden",
        }
        self.code = code if code in message_by_code else "query_invalid"
        super().__init__(message_by_code[self.code])


def canonical_provider_directory_api_base(candidate: object) -> str:
    """Require one canonical HTTPS base with no credentials or query state."""

    if (
        type(candidate) is not str
        or not candidate
        or len(candidate) > 2048
        or candidate != candidate.strip()
        or candidate.endswith("/")
    ):
        raise ProviderDirectoryRootedGraphQueryError("api_base_invalid")
    try:
        parsed = urllib.parse.urlsplit(candidate)
        port = parsed.port
    except ValueError:
        raise ProviderDirectoryRootedGraphQueryError("api_base_invalid") from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path in {"", "/"}
        or (port is not None and not 1 <= port <= 65535)
        or urllib.parse.urlunsplit(parsed) != candidate
    ):
        raise ProviderDirectoryRootedGraphQueryError("api_base_invalid")
    return candidate


def _canonical_reference(reference_type: str, resource_id: object) -> str:
    return f"{reference_type}/{canonical_fhir_resource_id(resource_id)}"


def _exact_search_definition(
    resource_type: str,
    search_parameter: str,
):
    return next(
        (
            search
            for search in PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES
            if search.resource_type == resource_type
            and search.search_parameter == search_parameter
        ),
        None,
    )


def _query_url(
    *,
    api_base: str,
    kind: str,
    resource_type: str,
    search_parameter: str | None,
    reference: str | None,
    page_size: int | None,
) -> str:
    resource_url = f"{api_base}/{resource_type}"
    if kind == ROOTED_GRAPH_QUERY_DIRECT_READ:
        return f"{resource_url}/{reference.rsplit('/', 1)[1]}"
    query_pairs: list[tuple[str, str]] = []
    if kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH:
        query_pairs.append((search_parameter, reference))
    query_pairs.append(("_count", str(page_size)))
    return resource_url + "?" + urllib.parse.urlencode(query_pairs)


def _is_valid_exact_query(
    query: ProviderDirectoryRootedGraphQuery,
) -> bool:
    """Validate an exact query against its closed search definition."""

    definition = _exact_search_definition(
        query.resource_type,
        query.search_parameter,
    )
    return bool(
        definition is not None
        and query.reference is not None
        and query.reference.startswith(definition.reference_type + "/")
        and _canonical_reference(
            definition.reference_type,
            query.reference.split("/", 1)[1],
        )
        == query.reference
        and query.page_size == definition.page_size
        and query.pagination == definition.pagination
    )


def _is_valid_direct_query(
    query: ProviderDirectoryRootedGraphQuery,
) -> bool:
    """Validate one direct read against the four allowed families."""

    return bool(
        query.resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES
        and query.search_parameter is None
        and query.reference is not None
        and query.reference.startswith(query.resource_type + "/")
        and _canonical_reference(
            query.resource_type,
            query.reference.split("/", 1)[1],
        )
        == query.reference
        and query.page_size is None
        and query.pagination == PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_PAGINATION
    )


def _is_valid_plan_census_query(
    query: ProviderDirectoryRootedGraphQuery,
) -> bool:
    """Validate the unfiltered, paginated InsurancePlan census start."""

    return bool(
        query.resource_type == "InsurancePlan"
        and query.search_parameter is None
        and query.reference is None
        and query.page_size == PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE
        and query.pagination == PROVIDER_DIRECTORY_ROOTED_GRAPH_PAGINATION
    )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphQuery:
    """Immutable query plan; construction performs no network operation."""

    kind: str
    resource_type: str
    api_base: str = field(repr=False)
    url: str = field(repr=False)
    search_parameter: str | None = None
    reference: str | None = field(default=None, repr=False)
    page_size: int | None = None
    pagination: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_PAGINATION

    def __post_init__(self) -> None:
        """Reject fields or URLs outside the selected closed query shape."""

        api_base = canonical_provider_directory_api_base(self.api_base)
        if self.kind not in _QUERY_KINDS:
            raise ProviderDirectoryRootedGraphQueryError("query_invalid")
        validator_by_kind = {
            ROOTED_GRAPH_QUERY_EXACT_SEARCH: _is_valid_exact_query,
            ROOTED_GRAPH_QUERY_DIRECT_READ: _is_valid_direct_query,
            ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS: (_is_valid_plan_census_query),
        }
        is_valid = validator_by_kind[self.kind](self)
        expected_url = (
            _query_url(
                api_base=api_base,
                kind=self.kind,
                resource_type=self.resource_type,
                search_parameter=self.search_parameter,
                reference=self.reference,
                page_size=self.page_size,
            )
            if is_valid
            else None
        )
        if not is_valid or self.url != expected_url:
            raise ProviderDirectoryRootedGraphQueryError("query_invalid")

    def identity_document(self) -> dict[str, object]:
        """Return the endpoint-neutral identity for this exact query."""

        return {
            "kind": self.kind,
            "page_size": self.page_size,
            "pagination": self.pagination,
            "reference": self.reference,
            "resource_type": self.resource_type,
            "search_parameter": self.search_parameter,
        }

    def query_id(self, scope_id: str) -> str:
        """Bind this query to one immutable rooted-graph scope."""

        return provider_directory_rooted_graph_query_id(
            scope_id,
            self.identity_document(),
        )

    def __repr__(self) -> str:
        return (
            "<provider-directory-rooted-graph-query "
            f"kind={self.kind!r} resource_type={self.resource_type!r}>"
        )


def build_rooted_graph_search_query(
    *,
    api_base: str,
    resource_type: str,
    search_parameter: str,
    referenced_resource_id: object,
) -> ProviderDirectoryRootedGraphQuery:
    """Build one of the two admitted exact reference searches."""

    if resource_type == "InsurancePlan" and search_parameter == "network":
        raise ProviderDirectoryRootedGraphQueryError(
            "insurance_plan_network_query_forbidden"
        )
    definition = _exact_search_definition(resource_type, search_parameter)
    if definition is None:
        raise ProviderDirectoryRootedGraphQueryError("search_forbidden")
    canonical_api_base = canonical_provider_directory_api_base(api_base)
    reference = _canonical_reference(
        definition.reference_type,
        referenced_resource_id,
    )
    query_url = _query_url(
        api_base=canonical_api_base,
        kind=ROOTED_GRAPH_QUERY_EXACT_SEARCH,
        resource_type=resource_type,
        search_parameter=search_parameter,
        reference=reference,
        page_size=definition.page_size,
    )
    return ProviderDirectoryRootedGraphQuery(
        kind=ROOTED_GRAPH_QUERY_EXACT_SEARCH,
        resource_type=resource_type,
        api_base=canonical_api_base,
        url=query_url,
        search_parameter=search_parameter,
        reference=reference,
        page_size=definition.page_size,
        pagination=definition.pagination,
    )


def build_provider_directory_practitioner_role_query(
    api_base: str,
    practitioner_resource_id: object,
) -> ProviderDirectoryRootedGraphQuery:
    """Build the exact PractitionerRole-by-practitioner root query."""

    return build_rooted_graph_search_query(
        api_base=api_base,
        resource_type="PractitionerRole",
        search_parameter="practitioner",
        referenced_resource_id=practitioner_resource_id,
    )


def build_provider_directory_organization_affiliation_query(
    api_base: str,
    organization_resource_id: object,
) -> ProviderDirectoryRootedGraphQuery:
    """Build the exact affiliation-by-participating-organization query."""

    return build_rooted_graph_search_query(
        api_base=api_base,
        resource_type="OrganizationAffiliation",
        search_parameter="participating-organization",
        referenced_resource_id=organization_resource_id,
    )


def build_rooted_graph_direct_read(
    *,
    api_base: str,
    resource_type: str,
    resource_id: object,
) -> ProviderDirectoryRootedGraphQuery:
    """Build one admitted direct read for a referenced graph node."""

    if resource_type not in PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES:
        raise ProviderDirectoryRootedGraphQueryError("direct_read_forbidden")
    canonical_api_base = canonical_provider_directory_api_base(api_base)
    reference = _canonical_reference(resource_type, resource_id)
    query_url = _query_url(
        api_base=canonical_api_base,
        kind=ROOTED_GRAPH_QUERY_DIRECT_READ,
        resource_type=resource_type,
        search_parameter=None,
        reference=reference,
        page_size=None,
    )
    return ProviderDirectoryRootedGraphQuery(
        kind=ROOTED_GRAPH_QUERY_DIRECT_READ,
        resource_type=resource_type,
        api_base=canonical_api_base,
        url=query_url,
        reference=reference,
        pagination=PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_PAGINATION,
    )


def build_insurance_plan_census_query(
    api_base: str,
) -> ProviderDirectoryRootedGraphQuery:
    """Build the unfiltered first page of the required full plan census."""

    canonical_api_base = canonical_provider_directory_api_base(api_base)
    query_url = _query_url(
        api_base=canonical_api_base,
        kind=ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
        resource_type="InsurancePlan",
        search_parameter=None,
        reference=None,
        page_size=PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE,
    )
    return ProviderDirectoryRootedGraphQuery(
        kind=ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
        resource_type="InsurancePlan",
        api_base=canonical_api_base,
        url=query_url,
        page_size=PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE,
    )


__all__ = (
    "build_insurance_plan_census_query",
    "build_provider_directory_organization_affiliation_query",
    "build_provider_directory_practitioner_role_query",
    "build_rooted_graph_direct_read",
    "build_rooted_graph_search_query",
    "canonical_provider_directory_api_base",
    "ProviderDirectoryRootedGraphQuery",
    "ProviderDirectoryRootedGraphQueryError",
    "ROOTED_GRAPH_QUERY_DIRECT_READ",
    "ROOTED_GRAPH_QUERY_EXACT_SEARCH",
    "ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS",
)
