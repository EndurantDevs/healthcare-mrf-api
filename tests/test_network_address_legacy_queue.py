# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only the closed historical wire shape may reproduce queued address digests."""

import pytest

from process import network_address_artifact as artifact


@pytest.mark.parametrize("wire", [None, [], [1] * 7, [True, 2, 3, 4, 5, 6, 7], [1, 2, 3, 4, 5, 6, 2**32]])
def test_legacy_address_digest_compatibility_rejects_malformed_wire_fields(wire):
    with pytest.raises(artifact.NetworkAddressArtifactError, match="legacy_origin_invalid"):
        artifact._legacy_origin_document({}, wire)
