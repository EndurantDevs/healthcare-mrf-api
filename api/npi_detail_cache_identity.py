"""Stable request identity for NPI detail response caching."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NpiDetailCacheIdentity:
    npi: int
    view: str
    include_chain: bool
    extra_info: bool
    sync_geocode: bool
    lookup_stored_geocode: bool
    include_sources: bool = False
    include_evidence: bool = False
    include_profile: bool = True
    profile_generation: str | None = None
    profile_serving_identity: str | None = None
    address_overlay_serving_identity: str | None = None
    canonical_publication_identity: str | None = None
    address_limit: int | None = None
    address_offset: int = 0
    include_address_total: bool = True
    address_key: str | None = None
    address_site_key: str | None = None
    address_grouping: str = "flat"


def npi_detail_cache_key(
    identity: NpiDetailCacheIdentity,
    *,
    schema: str,
    address_source: str,
) -> str:
    """Render every response-shaping input into one stable cache key."""

    return (
        f"{schema}|{address_source}|{int(identity.npi)}|{identity.view}|"
        f"{'chain' if identity.include_chain else 'default'}|"
        f"extra:{int(identity.extra_info)}|"
        f"{'sync_geo' if identity.sync_geocode else 'stored_geo'}|"
        f"{'archive_geo' if identity.lookup_stored_geocode else 'no_archive_geo'}|"
        f"sources:{int(identity.include_sources)}|evidence:{int(identity.include_evidence)}|"
        f"profile:{int(identity.include_profile)}|pgen:{identity.profile_generation or 'none'}|"
        f"pserve:{identity.profile_serving_identity or 'unknown'}|"
        f"pdaddr:{identity.address_overlay_serving_identity or 'unknown'}|"
        f"npipub:{identity.canonical_publication_identity or 'untracked'}|"
        f"alim:{identity.address_limit if identity.address_limit is not None else 'all'}|"
        f"aoff:{int(identity.address_offset or 0)}|atotal:{int(identity.include_address_total)}|"
        f"akey:{identity.address_key or 'none'}|"
        f"askey:{identity.address_site_key or 'none'}|agroup:{identity.address_grouping}"
    )


__all__ = ["NpiDetailCacheIdentity", "npi_detail_cache_key"]
