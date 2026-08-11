"""Stable execution constants for exact PTGSmall wave admission."""

from api import control_import_wave_direct as direct_wave


QUEUE = "arq:PTGSmall"
WORKER_CLASS = "process.PTGSmall"
RESOURCE_CLASS = "small"
WORKER_LIMIT = 12
MAX_INTENTS = 4096
MAX_INTENT_CANONICAL_BYTES = direct_wave.MAX_INTENT_CANONICAL_BYTES
MAX_ATTESTATION_CANONICAL_BYTES = direct_wave.MAX_ATTESTATION_CANONICAL_BYTES
PROTOCOL_IDENTITY = "healthporta.ptg-small.exact-wave.v1"
SERIALIZER_IDENTITY = "arq-0.28.process-msgpack.v1"
