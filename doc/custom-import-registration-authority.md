# Custom-import registration authority

These operator control routes register an immutable custom-import definition
and approved Snowflake source binding. They are separate from `/api/v1` reads
and do not import records, activate a generation, or grant extension-read access.
Apply the schema migration before using them. Privileged standalone CLI
registration remains available independently.

## Requests and authentication

The base resource is
`/control/v1/custom-import/registration-authorities/{authority_id}`. An authority
ID is an opaque ASCII identifier of at most 128 characters: its first character
is alphanumeric; subsequent characters may also include `.`, `_`, `:`, and `-`.

| Method | Resource | Authentication | JSON body |
| --- | --- | --- | --- |
| `PUT` | Base resource | Existing control token | Exactly `registration`, `expires_at`, `token_sha256` |
| `GET` | Base resource | Existing control token | No body |
| `POST` | `/revoke` suffix | Existing control token | `{}` |
| `POST` | `/register` suffix | This authority's capability only | Exactly `dataset_key`, `definition`, `source_binding` |

Control authentication uses the existing `HLTHPRT_CONTROL_API_TOKEN` interface
and its supported control-token headers. For `/register`, send
`Authorization: Bearer <capability>`: the capability is a randomly generated
32-byte value encoded as canonical, unpadded base64url, exactly 43 characters.
A broad control bearer or `X-HealthPorta-Control-Token` is not accepted on that
route. Keep capability bytes out of logs, command arguments, and persisted
plaintext. The engine retains only their SHA-256 hash.

`registration` contains the same three fields accepted by `/register`.
Definitions and bindings must satisfy their existing canonical contracts;
unknown fields and arbitrary queries are not supported. `token_sha256` is the
64-character lowercase hexadecimal SHA-256 of the decoded capability bytes.
`expires_at` must include a timezone and is normalized to UTC. Query parameters,
duplicate JSON keys, and additional top-level fields are rejected. Registration
input is bounded to 1 MiB of canonical semantic data; HTTP bodies are also
bounded to 1 MiB plus 256 bytes, and revoke bodies to 64 bytes.

## Immutable intent and reconciliation

Create the authority once, retaining its original ID, input, capability, and
deadline before any remote request. Exact `PUT` retries return the stored state;
they cannot replace pins or extend expiry. Conflicting pins return `409`.
Revoking an unknown ID creates an irreversible tombstone: a later mint cannot
reuse it.

The engine reconstructs and hashes the complete registration document itself:
SHA-256 of UTF-8 JSON with sorted keys, compact separators, `ensure_ascii=False`,
and non-finite numbers forbidden. This semantic digest is distinct from a
checksum of transported JSON bytes, including ASCII-escaped JSON. Compare the
engine's `input_sha256` with independently computed semantic input evidence.

Unfinished registration requires the exact input and capability, an unrevoked
authority, and an unexpired database-clock deadline. The transaction locks the
authority before dataset insertion/locking and checks expiry again after graph
work. Graph changes and the retained result commit atomically; a failed final
authority check rolls the graph work back. This is a transaction fence, not a
claim that physical WAL commit occurs before the deadline.

Successful mutation responses follow commit and readback. Each operation has an
eight-second timeout. A timeout or `503` does not prove that nothing committed:
reconcile the same authority with `GET` or retry the exact original operation.
Do not substitute another ID, capability, input, or deadline after an uncertain
response.

## Responses and retained history

`PUT`, `GET`, and `/revoke` return redacted state: `authority_id`,
`input_sha256`, `token_sha256`, `expires_at`, `created_at`, `revoked_at`, and
`result`. A tombstone has null input/capability/expiry pins. `result` is null
until registration completes. `/register` returns the retained registration
receipt containing dataset/revision identities and canonical digests, never
capability bytes or source credentials.

An exact completed `/register` retry returns the same historical receipt,
including after authority expiry or revocation; it does not register again.
Revocation preserves committed metadata and results. Later preview, execution,
and extension reads still require their own current authorization.

Every success and error response uses `Cache-Control: no-store`. Errors are
bounded JSON objects containing only `error`: `invalid_request` (`400`),
`forbidden` (`403`), `not_found` (`404`, GET only), `identity_conflict` (`409`),
or `registration_unavailable` (`503`). Treat revocation as effective at the
engine only after its committed acknowledgement or verified state readback.

## Database permission boundary

The API needs the metadata-writing privileges used by registration. A
deployment relying on scoped capabilities must deny registrar and import-worker
principals direct writes to registration metadata and authority tables, schema
creation, guard changes, and privileged-role escalation. A Python route guard or
`REVOKE FROM PUBLIC` alone does not prove that boundary for an explicitly granted
database role. Verify permissions under the actual deployed principals before
enabling capability-based registration.
