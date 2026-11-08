# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One signed import eligibility and managed detail read on the same Reader snapshot."""

from dataclasses import dataclass

from sanic.exceptions import NotFound

from api import provider_detail_network_read as detail
from api.endpoint import network_catalog, npi
from api.provider_detail_network_contract import ProviderDetailNetworkQuery, parse_managed_detail_query
from api.provider_profile_snapshot import provider_profile_read_snapshot
from process.custom_import.read_contracts import DEFAULT_READ_TIMEOUT_MS, CustomImportReadRequestError
from process.custom_import.read_core import _bounded_read_window
from process.registry_catalog_policy import RegistryOrganizationVisibility

MAX_MANAGED_BODY_BYTES = 1024 * 1024


@dataclass(frozen=True)
class ManagedProviderDetail:
    policy: dict
    query: ProviderDetailNetworkQuery
    exclusions: tuple[int, ...]
    scope_sha256: str
    organization_policy: RegistryOrganizationVisibility


def parse_managed_provider_policy(document):
    """Validate the closed full policy shared by signed detail and collections."""
    fields = {
        "client_id",
        "policy_revision",
        "excluded_network_ids",
        "query",
        "contract_version",
        "organization_visibility_default",
        "excluded_company_ids",
        "excluded_group_ids",
        "access_scope_sha256",
    }
    try:
        network_catalog._object(document, allowed=fields, required=fields)
        policy, exclusions, scope = network_catalog._policy_parameters(document)
        visibility = RegistryOrganizationVisibility(
            tuple(policy["excluded_company_ids"]), tuple(policy["excluded_group_ids"])
        )
        return policy, exclusions, scope, visibility
    except (ValueError, TypeError, KeyError, UnicodeError) as error:
        raise CustomImportReadRequestError("managed provider detail is invalid") from error


def parse_managed_provider_detail(document):
    """Validate the complete signed v2 policy without reusing HTTP authentication."""
    try:
        policy, exclusions, scope, visibility = parse_managed_provider_policy(document)
        query = parse_managed_detail_query(document["query"])
        return ManagedProviderDetail(policy, query, exclusions, scope, visibility)
    except (ValueError, TypeError, KeyError, UnicodeError) as error:
        raise CustomImportReadRequestError("managed provider detail is invalid") from error


async def read_managed_import_detail(request, parsed, verified):
    """Keep native/import eligibility and exact detail composition in one Reader scope."""
    from api.custom_import_provider_batch import _read_batch_payload

    managed = parsed.managed_detail
    async with provider_profile_read_snapshot(npi.db, npi._runtime_db_schema(), include_detail=True) as session:
        async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
            async with detail.managed_provider_detail_read_scope(
                session,
                managed.query,
                excluded_network_ids=managed.exclusions,
                access_scope_sha256=managed.scope_sha256,
                organization_policy=managed.organization_policy,
            ) as manifest:
                batch_payload = await _read_batch_payload(request, session, parsed, verified)
                if len(batch_payload["items"]) != 1 or batch_payload["items"][0]["status"] != 200:
                    raise NotFound("Provider data is unavailable.")
                provider_item = batch_payload["items"][0]
                provider = provider_item["provider"]
                provider_item["provider"] = {"npi": managed.query.npi} | (
                    {"custom_import": provider["custom_import"]} if "custom_import" in provider else {}
                )
                page = await detail.compose_managed_provider_detail(
                    request, managed.query, session, manifest, excluded_network_ids=managed.exclusions
                )
                batch_payload["managed_detail"] = {
                    "client_id": managed.policy["client_id"],
                    "policy_revision": managed.policy["policy_revision"],
                    **page,
                }
                return batch_payload, manifest
