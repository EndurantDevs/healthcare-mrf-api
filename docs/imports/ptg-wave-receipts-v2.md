# PTG wave asymmetric receipts v2

Fresh ordinary-cutover operations use an RSA receipt authority that is
separate from the control API bearer token. The bearer token authenticates the
request; it is never accepted as proof that the engine persisted linkage or
retired a pristine wave.

## Cryptographic envelope

Both receipt families use RSA-2048, public exponent 65537, PKCS#1 v1.5, and
SHA-256. A signature is exactly 256 bytes encoded as 512 lowercase hexadecimal
characters. A key ID matches `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`.

The JSON envelope has exactly these fields:

```json
{
  "schema": "<versioned domain>",
  "key_id": "<pinned epoch>",
  "issued_at": "YYYY-MM-DDTHH:MM:SS.ffffffZ",
  "payload": {},
  "payload_digest": "<lowercase sha256 hex>",
  "signature": "<512 lowercase hex>"
}
```

Canonical JSON is UTF-8 `json.dumps(sort_keys=True,separators=(',', ':'),
ensure_ascii=True,allow_nan=False)`. The signed material is the exact canonical
JSON object `{key_id,issued_at,payload}`. Signing bytes and `payload_digest` are:

```text
ASCII(schema) || 0x00 || canonical_json(signed_material)
sha256(signing_bytes)
```

The outer `schema` is checked independently before signature verification. No
extra fields, alternate schema, imprecise timestamp, uppercase hex, or other
canonicalization are accepted.

## Linkage receipt

`POST /import-waves/{wave_id}/linkage-ack` accepts the legacy request unchanged.
A fresh v6 operation must instead send exactly:

```json
{"linkage_ack": {}, "cutover_id": "<64hex>", "key_id": "<pinned epoch>"}
```

The engine independently derives
`sha256("ptg-ordinary-cutover-id-v1:" || operation_id)`, rebuilds the signed v6
admission and every persisted intent byte, and re-derives every outcome-row
digest, the all-outcome collection digest, and the linkage mapping before it
signs. The response is the direct
`healthporta.ptg-wave-linkage-receipt.v2` envelope. An exact retry returns the
persisted byte-equivalent envelope, including after the wave advances beyond
linkage wait. For v6, the ACK HMAC is retained as immutable historical bytes,
but it is not proof authority: the engine validates its exact shape and
independently re-derives every bound graph field before RSA-signing its digest.
This lets an ACK durably created under token A make its first authenticated
POST under token B. Once its ACK and RSA receipt are persisted, an exact retry
under token C is matched byte-for-byte to that immutable ACK and its digest;
it is not re-HMACed with a later control token. An altered retry is rejected.
Legacy linkage continues to require its current-token HMAC.

The payload binds operation/cutover/wave identities; request, attestation,
authorization, snapshot, membership, inventory, subscription, entitlement,
physical/imported/reused partition, jobs, manifest, outcome, mapping, and
linkage-ack digests; and all associated counts. The executable exact field set
is `LINKAGE_PAYLOAD_FIELDS` in `process/ptg_wave_receipt_contract.py`.

## Fresh pristine abandonment

`POST /import-waves/{wave_id}/materialized-preclaim-abandonment` retains the
legacy `{cutover_id}` path. A fresh v6 operation sends exactly:

```json
{
  "schema": "healthporta.ptg-wave.v12-pristine-materialized-abandonment-request.v1",
  "key_id": "<pinned epoch>",
  "operation_id": "<wave_id>",
  "cutover_id": "<derived cutover ID>",
  "admission": {}
}
```

This is a separate proof family, not a legacy v4/v5 recovery. The proof schema
is
`healthporta.ptg-wave.v12-pristine-materialized-abandonment-proof.v1`, and its
recovery basis is `v12_pristine_materialized_cutover`. It binds the exact v6
admission, pristine intent and ImportRun state, zero claim/outcome/worker-start
state, the exact terminal failed Kubernetes Job receipt and shape, and empty
Redis release/work sets. It rejects any predecessor, successor, rollback, or
legacy recovery dependency.

The proof digest is:

```text
sha256(ASCII(proof_schema) || 0x00 || canonical_json(proof_without_digest))
```

Database collection digests are:

```text
sha256(ASCII(domain) || 0x00 || canonical_json(records))
```

with domains:

- `healthporta.ptg-wave.v12-pristine-member-rows.v1`
- `healthporta.ptg-wave.v12-pristine-intent-rows.v1`
- `healthporta.ptg-wave.v12-pristine-run-rows.v1`

The response is the direct
`healthporta.ptg-wave-abandonment-receipt.v2` envelope: HTTP 201 on first
persistence and HTTP 200 on exact replay. The response contains no mutable
`created` marker. The signed payload retains the engine's distinct
`recovery_evidence_sha256`; the outer `payload_digest` never substitutes for
that evidence.

## Key epochs and rotation

Fresh v6 admission has mandatory top-level `receipt_key_id`,
`receipt_public_modulus_hex`, and `receipt_public_exponent`. The modulus is
exactly 512 lowercase hexadecimal characters and the exponent is exactly
65537. All three fields are covered by the admission HMAC. The engine accepts
new admissions only when the ID and public material equal its process-pinned
active epoch, then persists all three immutably. Linkage and abandonment
requests must ask for the exact stored epoch.

The control HMAC authenticates only the first admission. The engine persists
the exact canonical v6 envelope, original signature, request/attestation/
signature digests, and its validated projection. After a control-token
rotation, an exact admission retry is compared with that immutable history;
linkage and abandonment rebuild from the same persisted envelope and never
re-HMAC it with the current token. The current bearer token remains transport
authorization, while each final proof is independently RSA signed.

Runtime configuration is:

- `HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_KEY_ID`
- `HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_PRIVATE_KEY_FILE`
- `HLTHPRT_PTG_WAVE_RECEIPT_RETAINED_PRIVATE_KEY_FILES_JSON`
- `HLTHPRT_PTG_WAVE_RECEIPT_RETIRED_PUBLIC_EPOCHS_FILE`

Paths are absolute and bounded. The active private epoch signs new and pinned
operations. A retained private epoch may sign only operations already admitted
under it. Public-only retired epochs verify historical receipts but cannot
sign. At control-server startup, every persisted v6 public pin must still match
an active or retired public epoch, and every unquarantined nonterminal v6 wave
must still have its matching private signer; removal or same-ID material drift
fails closed. The total exposed epoch set is bounded to eight, duplicate public
material under different key IDs is rejected, and duplicate JSON configuration
fields are rejected.

The server loads the complete keyring once before accepting traffic. Projected
Secret-file updates therefore cannot pair replacement private material with an
old in-process key ID. Rotation requires a controlled server rollout; new
processes revalidate all nonterminal pinned epochs before starting the
controller.

`GET /import-wave-receipt-key-epochs` returns exactly:

```json
{
  "schema_version": "healthporta.ptg-wave-receipt-key-epochs.v1",
  "active_key_id": "<key ID>",
  "epochs": [
    {
      "key_id": "<key ID>",
      "rsa_modulus": "<512 lowercase hex>",
      "rsa_exponent": 65537,
      "state": "active"
    }
  ]
}
```

Epochs are sorted by key ID and exactly one is active. This endpoint is for
discovery and audit only. Consumers must use protected deployment-seeded trust;
they must never trust a key dynamically from this endpoint.

## Persistence and fixtures

Migration `20260810110000_ptg_wave_receipt_authority` installs immutable key and
receipt fields, bounded canonical-JSON and RSA-2048 PKCS#1 v1.5/SHA-256
verification, exact linkage and fresh-abandonment first-write triggers, late
child/run/event/truncate fences, per-member ordinary-terminal first-write
verification, effective-owner release, and a downgrade block once v6 authority
has been used. Legacy v1-v5 rows and ordinary-cutover receipts retain their
existing shape.

The cross-service synthetic fixture is
`tests/fixtures/ptg_wave_receipts_v2.json`. Its SHA-256 is
`701b913369f4896b5ea943844d12519a73ffd8da276dd1ad1bd71fd68692a5da`.
It contains linkage, abandonment, and ordinary-terminal public verification
vectors, but no private key. Tests that need fresh signatures generate unrelated
ephemeral RSA keys under OS-owned temporary directories. Deployment private
material must come from the engine-only secret authority and must never be
committed, logged, or exposed through the epoch endpoint.
