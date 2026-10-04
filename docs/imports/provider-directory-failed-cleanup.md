# Failed Profile cleanup

Failed Profile stages remain logged until an operator obtains a separately
funded, signed cleanup authorization. The original build lease does not fund
cleanup. The read-only `inspect` action, independent issuer, and explicit
`execute` action retain their existing database, host, runtime, reservation,
freshness, and physical catalog checks.

The supported authorization versions are closed:

- v1 authorizes the exact three stages of a failed verified `source_delta` build.
- v2 requires `variant: legacy_full_swap` and binds genuine legacy owner and
  publication history, without inventing missing historical capacity metadata.
- v3 requires `variant: initial_full_swap` and binds the exact two stages of a
  failed verified initial build. It uses the original validated initial capacity
  geometry, its signed checkpoint digest, and the actual initial target state.

An initial target state may be `empty` or `legacy_as_of_unknown`. Both contain
real canonical Profile and evidence table identities. Unknown historical time
is not a fabricated date, and absent serving metadata does not imply absent
physical tables or rows.

The v3 inspection request includes the complete original `capacity_geometry`
for issuer verification. The signed body omits this duplicate geometry and
contains a closed `initial` object with `target_state`,
`initial_target_state_sha256`, `initial_receipt_oid`, and
`initial_receipt_storage_fingerprint`. The existing signed
`checkpoint.capacity_geometry_hash` binds the full geometry. The issuer verifies
that hash and the proof, plan, target, receipt, and physical bindings before
signing. Core repeats those checks against the retained checkpoint under its
existing locks before spending authority or dropping anything.

Maintenance may change either incumbent relation's physical size without changing
its identity, layout, history, or row count. Inspection and locked execution then
authenticate the original consumed Profile admission at its recorded acceptance
time and recover the complete signed target state. The original target payload
and hash remain in the cleanup manifest; only the two size fields may differ from
current observations. Current physical measurements and fresh cleanup authority
still fund every mutation. Missing or invalid original admission and any other
target change refuse cleanup before a claim or DROP.

The executor rechecks absent initial/common publication receipts and serving
metadata, the unchanged incumbent targets, the immutable initial receipt layout,
and the exact two non-serving logged stage OIDs and fingerprints. Separate
cleanup funding covers actual claim/checkpoint storage, catalog and TOAST
mutations, DROP, and commit WAL. All versions keep the existing claim payload
ceiling, observation window, operation deadline, and authorization expiry.

## Retaining completed verification history

A completed cleanup claim never authorizes a second disposal. `execute` and
`reconcile` replay an exact completed receipt; incomplete spent claims remain
refused. A future admitted build also verifies retained completed checkpoints
before ignoring their absent stages.

Before retiring original cleanup public trust, retain its original typed public
trust document in an independently authorized history file. Core accepts the
following optional configuration only for completed-history verification:

- `HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_HISTORY_FILE`: an absolute,
  current-user-owned regular file with mode `0600`, one link, outside Git and
  temporary roots. The existing protected reader requires stable file identity
  and at most 256 KiB.
  Reading may update access time; identity, ownership, permissions, link count,
  size, and write/change timestamps must remain stable.
- `HLTHPRT_PROVIDER_DIRECTORY_FAILED_PROFILE_CLEANUP_AUTHORIZED_HISTORY_SHA256`:
  the independently configured SHA-256 of those exact file bytes.

The closed JSON document has `contract_id` equal to
`healthporta.provider-directory.failed-profile-cleanup-trust-history.v1` and an
`epochs` array of one to sixteen original
`provider-directory-database-capacity-trust-v2` public trust documents. Each
entry uses the existing strict typed trust validator. Private keys and unknown
fields are refused. Duplicate canonical epochs are refused; an archived
verification must match at least one authentic epoch. Distinct active and
retired epochs may authenticate the same original claim.

Historical verification is selected only after the immutable claim and signed
coordinates, canonical completion receipt, complete checkpoint preimage, actual
database identity, and absence of every exact stage have been checked. Current
trust is tried first; failure to load current trust does not invalidate completed
history. An independently pinned original epoch may then verify the
original Ed25519 signature and original bindings at the claim's authentic
`claimed_at`. A missing or partial file/pin pair, changed pin, malformed archive,
missing epoch, or absence of an authentic matching epoch fails closed.
The operator's completed `execute` and `reconcile` paths use these same checks.
Fresh execution still requires valid current configured trust before spending a
claim or dropping a stage.

The history file is never used for issuance, a fresh disposal, an incomplete
spent claim, or live reservation accounting. Current authority and active
Control reservation verification retain current trust and retired-key
`verify_until` limits. Cleanup ceilings remain fully reserved until signed
`expires_at`, even after an earlier operation deadline or successful disposal.
Configuring historical verification does not activate an issuer, grant database
privileges, or authorize cleanup.
