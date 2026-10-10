# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed CMS provider identity; source admission and office custody remain separate."""

import hashlib
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class CMSProviderIdentity:
    source_id: str
    resource_type: str
    resource_id: str

    def __post_init__(self):
        if (
            type(self.source_id) is not str
            or self.source_id != "cms-npd"
            or type(self.resource_type) is not str
            or self.resource_type not in {"Practitioner", "Organization"}
            or type(self.resource_id) is not str
            or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", self.resource_id) is None
        ):
            raise ValueError("cms_provider_identity_invalid")

    @property
    def provider_id(self):
        """Return a bounded native provider ID, distinct from public entity UUIDs."""
        encoded = f"cms-provider.v1|{self.source_id}|{self.resource_type}|{self.resource_id}".encode("ascii")
        return "cms_" + hashlib.sha256(encoded).hexdigest()


_NPI_DECLARATION_PATH = (
    '$.identifier[*] ? (@.system like_regex "npi|national provider" flag "i" '
    '|| @.type.text like_regex "npi|national provider" flag "i" '
    '|| exists(@.type.coding[*] ? (@.system like_regex "npi|national provider" flag "i" '
    '|| @.code like_regex "npi|national provider" flag "i" '
    '|| @.display like_regex "npi|national provider" flag "i")))'
)


def cms_provider_sql_fields(source_parameter):
    """Qualify only an exact admitted provider whose raw source declares no NPI."""
    npi = "coalesce(page.payload->>'npi',provider.payload_json::jsonb->>'npi')"
    qualified = (
        "page_witness.raw_payload_sha256 IS NOT NULL AND provider_witness.raw_payload_sha256 IS NOT NULL "
        "AND provider.resource_id=provider.payload_json::jsonb->>'resource_id' "
        "AND (provider.payload_json::jsonb->>'source_id' IS NULL "
        f"OR provider.payload_json::jsonb->>'source_id'=${source_parameter}) "
        f"AND NOT jsonb_path_exists(page_witness.raw_payload_json::jsonb,'{_NPI_DECLARATION_PATH}') "
        f"AND NOT jsonb_path_exists(provider_witness.raw_payload_json::jsonb,'{_NPI_DECLARATION_PATH}')"
    )
    identity = (
        "'cms_'||encode(sha256(convert_to('cms-provider.v1|'||"
        f"${source_parameter}::text||'|'||provider.resource_type||'|'||provider.resource_id,'UTF8')),'hex')"
    )
    return {
        "provider_system": f"CASE WHEN {npi} IS NOT NULL THEN 'npi' ELSE 'provider_directory' END",
        "provider_id": f"coalesce({npi},CASE WHEN {qualified} THEN {identity} END)",
        "provider_evidence": (
            "CASE WHEN provider_system='npi' THEN "
            "jsonb_build_array($1,$5,$6,resource_type,resource_id,payload_hash,network_id,provider_id,site_id) "
            "ELSE jsonb_build_array($1,$5,$6,resource_type,resource_id,payload_hash,network_id,provider_system,"
            "provider_id,provider_resource_type,provider_resource_id,provider_payload_sha256,site_id) END"
        ),
    }
