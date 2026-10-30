# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate newly written slice content and exact reuse of independently sealed slices."""

import hashlib
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_compact_readiness import _require
from process.network_membership_slices import _DIGEST_DOMAIN, OFFICE_BUCKET_COUNT


async def verify_compact_slice_contents(connection, copy_target, layout):
    """Check new content hashes and exact reference-map reuse after dependency custody."""
    namespace = _identifier(copy_target.schema_name)
    current = UUID(copy_target.candidate_id)
    for artifact in layout.artifacts:
        if artifact.candidate_id == copy_target.candidate_id:
            continue
        retained = _identifier(artifact.schema_name) + ".network_slice_ref"
        mismatch = await connection.fetchval(
            f"""SELECT EXISTS(SELECT 1 FROM {namespace}.network_slice_ref next
            LEFT JOIN {retained} prior USING(network_id,office_bucket,artifact_candidate_id,slice_id,row_count,content_sha256)
            WHERE next.artifact_candidate_id=$1 AND prior.network_id IS NULL)""",
            UUID(artifact.candidate_id),
        )
        _require(mismatch is False)
    _require(
        await connection.fetchval(
            f"""SELECT NOT EXISTS(SELECT 1 FROM {namespace}.network_office_edge edge
              WHERE NOT EXISTS(SELECT 1 FROM {namespace}.network_slice_ref ref
                WHERE ref.artifact_candidate_id=$1 AND ref.slice_id=edge.slice_id))
            AND NOT EXISTS(SELECT 1 FROM {namespace}.network_slice_ref WHERE artifact_candidate_id=$1
              GROUP BY slice_id HAVING count(*)<>1)""",
            current,
        )
    )
    previous, digest, count, expected = None, None, 0, None
    query = f"""SELECT ref.network_id,ref.office_bucket,ref.slice_id,ref.row_count,ref.content_sha256,edge.location_key
        FROM {namespace}.network_slice_ref ref LEFT JOIN {namespace}.network_office_edge edge USING(slice_id)
        WHERE ref.artifact_candidate_id=$1
        ORDER BY ref.network_id,ref.office_bucket,edge.location_key COLLATE "C"
    """
    async for slice_entry in connection.cursor(query, current, prefetch=4096):
        key = (slice_entry["network_id"], slice_entry["office_bucket"])
        if key != previous:
            if previous is not None:
                _require((count, digest.hexdigest()) == expected)
            previous, digest, count = key, hashlib.sha256(_DIGEST_DOMAIN), 0
            expected = slice_entry["row_count"], slice_entry["content_sha256"]
        _require(type(slice_entry["location_key"]) is str and slice_entry["location_key"])
        encoded = slice_entry["location_key"].encode()
        _require(hashlib.sha256(encoded).digest()[0] & (OFFICE_BUCKET_COUNT - 1) == slice_entry["office_bucket"])
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        count += 1
    if previous is not None:
        _require((count, digest.hexdigest()) == expected)
