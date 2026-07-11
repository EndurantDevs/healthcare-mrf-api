# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import api.ptg2_db_serving_v3 as db_serving_v3
from process.ptg_parts.ptg2_serving_binary_v3 import (
    append_uvarint,
    encode_provider_code_set,
)


def _provider_block(provider_code_rows):
    provider_rows = tuple(provider_code_rows)
    block_span = db_serving_v3.PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
    block_key = provider_rows[0][0] // block_span
    payload = bytearray()
    append_uvarint(payload, len(provider_rows))
    for provider_set_key, code_keys in provider_rows:
        assert provider_set_key // block_span == block_key
        code_payload, _stats = encode_provider_code_set(code_keys)
        append_uvarint(payload, provider_set_key % block_span)
        append_uvarint(payload, len(code_payload))
        payload.extend(code_payload)
    return block_key, bytes(payload)


def test_provider_code_block_decodes_only_requested_containers(monkeypatch):
    block_key, provider_payload = _provider_block(
        ((3, (1, 2)), (4, (7, 8)), (5, (9, 10)))
    )
    decoded_payloads = []
    original_decoder = db_serving_v3.decode_provider_code_set

    def tracked_decoder(payload):
        decoded_payloads.append(bytes(payload))
        return original_decoder(payload)

    monkeypatch.setattr(db_serving_v3, "decode_provider_code_set", tracked_decoder)

    assert db_serving_v3._decode_provider_code_block(
        provider_payload,
        block_key=block_key,
        entry_count=3,
        requested_provider_set_keys={4},
    ) == {4: (7, 8)}
    assert len(decoded_payloads) == 1
