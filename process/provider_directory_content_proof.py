# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fold ordered content proof pages while keeping their payload checks bounded."""

from __future__ import annotations

import asyncio
import hashlib

from process.provider_directory_resource_hash import is_semantic_resource_hash_contract
from process.uhc_flex_practitioner_async_safety import drain_operation

ROLE_KIND = "PractitionerRole"


def _native_merged_role_encoder():
    try:
        from ptg2_address_canon import verify_provider_directory_merged_role_page_v1
    except ImportError:
        return None
    return verify_provider_directory_merged_role_page_v1


async def _verified_merged_role_page(directory, resource_rows, encoder, resource_hash_contract):
    """Check one bounded page of stored semantic roles using the existing native role hash."""
    native_rows = []
    byte_bound = 0
    for resource_row in resource_rows:
        fields_by_name = getattr(resource_row, "_mapping", resource_row)
        stored_json = fields_by_name.get("payload_json_text")
        role_id = fields_by_name.get("resource_id")
        stored_hash = fields_by_name.get("payload_hash")
        if (
            fields_by_name.get("resource_type") != ROLE_KIND
            or fields_by_name.get("acquired_resource_sha256") is not None
            or not all(isinstance(field, str) for field in (stored_json, role_id, stored_hash))
            or len(role_id) != 64
            or len(stored_hash) != 64
        ):
            return None
        byte_bound += 4 * (len(stored_json) + len(role_id) + len(stored_hash))
        if byte_bound > 64 * 1024 * 1024:
            return None
        native_rows.append((role_id, stored_hash, stored_json))
    page_proof = await drain_operation(asyncio.to_thread(encoder, native_rows), preserve_cancellation=True)
    if page_proof is None:
        return None
    encoded_identities, fallback_indices = page_proof
    for count, index in enumerate(fallback_indices, 1):
        directory._assert_endpoint_dataset_resource_payload_hash(
            resource_rows[index], resource_hash_contract=resource_hash_contract
        )
        if count % 256 == 0:
            await asyncio.sleep(0.001)
    return encoded_identities


class _ContentDigests:
    def __init__(self, resource_types):
        self.dataset_hash = hashlib.sha256()
        self.resource_count = 0
        self.hashers_by_type = {resource_type: hashlib.sha256() for resource_type in resource_types}
        self.counts_by_type = dict.fromkeys(resource_types, 0)

    def add_identities(self, resource_type, encoded_identities, count=1):
        """Append one ordered row or a verified page to both digests."""
        family_hasher = self.hashers_by_type.get(resource_type)
        if family_hasher is None:
            family_hasher = self.hashers_by_type[resource_type] = hashlib.sha256()
        family_count = self.counts_by_type.get(resource_type, 0)
        if self.resource_count:
            self.dataset_hash.update(b"\n")
        if family_count:
            family_hasher.update(b"\n")
        self.dataset_hash.update(encoded_identities)
        family_hasher.update(encoded_identities)
        self.resource_count += count
        self.counts_by_type[resource_type] = family_count + count


async def _fold_content_page(directory, digests, resource_rows, encoder, verify_payload_hashes, resource_hash_contract):
    if encoder is not None:
        encoded_roles = await _verified_merged_role_page(directory, resource_rows, encoder, resource_hash_contract)
        if encoded_roles is not None:
            digests.add_identities(ROLE_KIND, encoded_roles, len(resource_rows))
            return
    for resource_row in resource_rows:
        if verify_payload_hashes:
            directory._assert_endpoint_dataset_resource_payload_hash(
                resource_row, resource_hash_contract=resource_hash_contract
            )
        identity = directory._endpoint_dataset_hash_identity(resource_row)
        digests.add_identities(identity[0], directory._stable_identity_json(identity).encode("utf-8"))
        if digests.resource_count % 256 == 0:
            await asyncio.sleep(0.001)


async def endpoint_dataset_content_proof(
    directory,
    connection,
    dataset_id,
    resource_types,
    *,
    verify_payload_hashes,
    resource_hash_contract,
    resource_relation,
):
    """Hash a complete dataset and each resource family in stable keyset order."""
    digests = _ContentDigests(resource_types)
    after_resource_type = after_resource_id = None
    batch_size = max(1, directory.ENDPOINT_DATASET_HASH_BATCH_SIZE)
    encoder = None
    if verify_payload_hashes:
        batch_size = min(batch_size, 1000)
        if is_semantic_resource_hash_contract(resource_hash_contract):
            encoder = _native_merged_role_encoder()
    while True:
        resource_rows = await directory._endpoint_dataset_content_page(
            connection,
            dataset_id,
            batch_size,
            after_resource_type,
            after_resource_id,
            include_payload_json=verify_payload_hashes,
            **({"resource_relation": resource_relation} if resource_relation is not None else {}),
        )
        if not resource_rows:
            break
        await _fold_content_page(
            directory, digests, resource_rows, encoder, verify_payload_hashes, resource_hash_contract
        )
        after_resource_type, after_resource_id, _ = directory._endpoint_dataset_hash_identity(resource_rows[-1])
        if len(resource_rows) < batch_size:
            break
    return directory._completed_endpoint_dataset_content_proof(
        digests.dataset_hash, digests.resource_count, digests.hashers_by_type, digests.counts_by_type
    )
