# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Load ordinary TOCs or all proven collection members before catalog writes."""

from __future__ import annotations

import importlib
import tempfile

from process.ptg_parts.cigna_toc_collection import is_cigna_collection_url, materialize_cigna_toc_collection


async def load_toc_input(
    toc_url,
    *,
    filters,
    download_options,
    import_run_id,
    raise_on_error,
    targeted_file_import,
    collection_proof=None,
    file_url_contains=None,
):
    """Keep existing artifact/persistence boundaries and every reporter's own parser context."""
    ptg = importlib.import_module("process.ptg")

    with tempfile.TemporaryDirectory(dir=ptg.ptg2_temp_parent()) as temporary:
        is_collection = collection_proof is not None or is_cigna_collection_url(toc_url)
        try:
            if is_collection:
                if collection_proof is None:
                    raise ValueError("Cigna collection requires expected archive proof")
                raw, members = await materialize_cigna_toc_collection(
                    toc_url, temporary, proof=collection_proof, **download_options
                )
            else:
                raw, logical = await ptg.materialize_json_source(toc_url, temporary, **download_options)
        except ptg.PTG2ArtifactStageFreshnessError, ptg.PTG2FullRebuildFreshnessError:
            raise
        except Exception as error:
            ptg.logger.warning("Failed to download table-of-contents from %s: %s", toc_url, error)
            if raise_on_error:
                raise RuntimeError(f"Failed to download table-of-contents from {toc_url}: {error}") from error
            return None
        if is_collection:
            return await _parse_collection_members(toc_url, raw, members, filters, file_url_contains, import_run_id)
        document = ptg._load_table_of_contents_artifact(logical.logical_path)
        if import_run_id:
            await ptg._record_source_version(
                source_type="table-of-contents",
                domain="catalog",
                raw_artifact=raw,
                logical_artifact=logical,
                import_run_id=import_run_id,
            )
        parsed_entries = []
        if not targeted_file_import or not document.get("reporting_structure"):
            parsed_entries = ptg.parse_toc_catalog_entries(document, toc_url, **filters)
        return document, parsed_entries


async def _parse_collection_members(toc_url, raw, members, filters, file_filters, import_run_id):
    """Parse the complete collection and select one physical file before database writes."""
    ptg = importlib.import_module("process.ptg")

    parsed_entries = []
    for member in members:
        document = ptg._load_table_of_contents_artifact(member.logical_path)
        parsed_entries.extend(ptg.parse_toc_catalog_entries(document, toc_url, **filters))
    _require_one_collection_file(parsed_entries, file_filters)
    if import_run_id:
        for member in members:
            await ptg._record_source_version(
                source_type="table-of-contents",
                domain="catalog",
                raw_artifact=raw,
                logical_artifact=member,
                import_run_id=import_run_id,
            )
    return {}, parsed_entries


def _require_one_collection_file(entries, file_filters):
    """A page-size limit cannot substitute for an unambiguous physical selection."""
    from process.ptg_parts.canonical import canonicalize_url
    from process.ptg_parts.domain import PTG2_DOMAIN_ALLOWED_AMOUNT, PTG2_DOMAIN_IN_NETWORK

    if not file_filters or any(not isinstance(value, str) or not value for value in file_filters):
        raise ValueError("Cigna collection file selection is incomplete")
    tokens = [value.lower() for value in file_filters]
    physical_files = {
        (entry.domain, canonicalize_url(entry.original_url))
        for entry in entries
        if entry.domain in {PTG2_DOMAIN_IN_NETWORK, PTG2_DOMAIN_ALLOWED_AMOUNT}
        and any(token in entry.original_url.lower() for token in tokens)
    }
    if len(physical_files) != 1:
        raise ValueError("Cigna collection file selection is absent or ambiguous")
