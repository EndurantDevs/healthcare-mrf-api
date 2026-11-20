# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate exact reuse of retained slices and complete new slice references."""

from uuid import UUID

from process.network_address_projection import _identifier
from process.network_compact_readiness import _require


async def verify_compact_slice_contents(connection, copy_target, layout):
    """Check exact reference reuse, one owning reference per new edge and each new slice's edge count."""
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
              GROUP BY slice_id HAVING count(*)<>1)
            AND NOT EXISTS(SELECT 1 FROM {namespace}.network_slice_ref ref
              LEFT JOIN (SELECT slice_id,count(*) AS edges FROM {namespace}.network_office_edge GROUP BY slice_id) edge
                USING(slice_id)
              WHERE ref.artifact_candidate_id=$1 AND coalesce(edge.edges,0)<>ref.row_count)""",
            current,
        )
    )
