# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public facade for protected Provider Directory logical-scope lookup."""

from process.provider_directory_endpoint_identity import (
    ProviderDirectoryEndpointIdentity,
    ProviderDirectoryEndpointIdentityError,
    build_provider_directory_endpoint_identity,
)
from process.provider_directory_scope_pseudonym import (
    LOOKUP_DECISION_AMBIGUOUS,
    LOOKUP_DECISION_CREATE,
    LOOKUP_DECISION_MATCHED,
    LogicalScopeLookupRequest,
    LogicalScopePseudonymError,
    LogicalScopeRegistryDecision,
    build_scope_lookup_request,
    build_scope_pseudonymizer,
)


__all__ = [
    "LOOKUP_DECISION_AMBIGUOUS",
    "LOOKUP_DECISION_CREATE",
    "LOOKUP_DECISION_MATCHED",
    "LogicalScopeLookupRequest",
    "LogicalScopePseudonymError",
    "LogicalScopeRegistryDecision",
    "ProviderDirectoryEndpointIdentity",
    "ProviderDirectoryEndpointIdentityError",
    "build_provider_directory_endpoint_identity",
    "build_scope_lookup_request",
    "build_scope_pseudonymizer",
]
