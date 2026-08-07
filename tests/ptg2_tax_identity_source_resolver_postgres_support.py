# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL resolver assertions shared by source-projection proofs."""

from api.ptg2_billing_entity_refs import encode_billing_entity_ref
from api.ptg2_billing_entity_source_resolution import (
    resolve_billing_entity_ref_source_scope,
)


async def assert_source_resolver(
    connection,
    schema_name: str,
    source_publication,
) -> None:
    """Resolve both colliding references to their exact physical sources."""

    first_hmac = bytes.fromhex("44" * 16 + "55" * 16)
    second_hmac = bytes.fromhex("44" * 16 + "66" * 16)
    source_scopes = []
    for full_hmac in (first_hmac, second_hmac):
        source_scopes.append(
            await resolve_billing_entity_ref_source_scope(
                connection,
                schema_name=schema_name,
                snapshot_key=18,
                billing_entity_ref=encode_billing_entity_ref(
                    snapshot_key=18,
                    tin_id_128=full_hmac[:16],
                    tin_hmac_sha256=full_hmac,
                ),
                source_publication=source_publication,
            )
        )
    first_scope, second_scope = source_scopes
    assert first_scope is not None and first_scope.source_keys == (0,)
    assert second_scope is not None and second_scope.source_keys == (1,)
    assert tuple(
        witness.source_record_ordinal for witness in first_scope.witnesses
    ) == (0,)
    assert tuple(
        witness.source_record_ordinal for witness in second_scope.witnesses
    ) == (1,)
    assert len(first_scope.provider_group_refs) == 1
    assert len(second_scope.provider_group_refs) == 1
    assert first_scope.provider_group_refs != second_scope.provider_group_refs
