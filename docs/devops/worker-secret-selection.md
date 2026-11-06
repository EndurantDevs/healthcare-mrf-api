# Importer-scoped worker credentials

The Kubernetes launcher accepts `importers` in an existing
`HLTHPRT_WORKER_JOB_SECRET_ENV_JSON` entry. It is an additional restriction,
not an alternative to `workerClasses` (or `worker_classes`). Entries without
`importers` retain their existing behavior.

For example, these deployment-owned references select only the candidate
audit importer, not the other pricing jobs sharing its worker class:

```json
[
  {
    "name": "HLTHPRT_DB_USER",
    "secretName": "candidate-audit-database",
    "key": "username",
    "workerClasses": ["process.PTGCandidateAudit"],
    "importers": ["ptg-candidate-audit"]
  },
  {
    "name": "HLTHPRT_DB_PASSWORD",
    "secretName": "candidate-audit-database",
    "key": "password",
    "workerClasses": ["process.PTGCandidateAudit"],
    "importers": ["ptg-candidate-audit"]
  }
]
```

Scoped entries require the genuine admitted run and its recorded queue,
function and job ID. The exact single-job target must match that admission;
request fields alone cannot supply it. The existing PTG-family capacity and
run-action locks remain held until launch settles, including cancellation.
Unscoped wave/configuration callers do not receive scoped credentials. This
selection does not change the local process launcher or any database grants.

Do not place privileged credentials in shared `envFrom` sources or class-only
entries. The audit class also serves projection, prewarm and distance jobs.
Configure the independently deployed audit HTTP API with its own read-only
database actor; the audit worker's credentials must not become the API's.
Keep control admission under its ordinary actor. Use an existing qualified
publisher only after its actual database identity, ownership, memberships and
control permissions have passed the native checks. No `SET ROLE` is implied.

This selector does not change audit semantics: LOCAL candidates remain
audit-only, while canonical candidates retain their existing mode and
activation checks. Qualify both paths and the independent API reader with
real roles before enabling these references. Secret selection is not proof
of publisher readiness, a passing audit or successful activation.

## Discovery worker image

Set the optional `HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE` to an immutable
`repository@sha256:digest` image reference for `mrf-source-discovery` Kubernetes
workers. An unset or blank value uses `HLTHPRT_WORKER_JOB_IMAGE`. Authorized
source-worker launch images retain priority. Other importers continue to use
their configured images.

## Protected source-profile maintenance

Set `HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE` to an independently managed,
read-only JSON policy file for the API and source-profile workers. Process
workers inherit the setting; the Kubernetes launcher forwards it to both the
source workers and `process.SourceProfile_finish`. Use the existing read-only
worker volume configuration to make the file available at that configured path.
Do not place it in writable import artifacts or supply its contents through run
parameters, launch requests, or publication receipts.

The policy is limited to 8 KiB and uses the shared namespace-policy fields:
`owner_role`, `migration_role`, `preparation_owner_role`, and `runtime_roles`.
The complete `runtime_roles` list must match the publication policy and contain
1–16 distinct ordinary roles. Existing optional credential, control-role and
schema-creation fields remain accepted. No credentials belong in this file.

Protected handoff refuses a missing or invalid policy before committing the
handoff. Finishing rechecks the configured role closure, actual worker identity,
RLS policies, owners, privileges and installed guard definitions before cleanup.
An unavailable or changed authority leaves the attempt finalizing for safe retry;
it does not authorize grants, metadata repair, or replacement policy from a receipt.
Legacy unprotected imports keep their existing completion path. New NY profile
acquisitions always require this policy and the configured native publisher,
including the first publication: their canonical bundle carries model-valued
witnesses and the ordinary worker writes no source-record or fact payload rows.
The publisher builds isolated model heaps with bounded binary COPY, validates the
complete indexed candidate, and atomically publishes it with retained custody.
The configured bundle and COPY byte limits remain unchanged.

Sealed legacy bundles retain their original contract. A digest-only bundle does
not supply missing canonical preimages or authorize a witness upgrade; replay
must authenticate the original retained captures and their encoding first.

## Release-reading pricing worker image

Set the optional `HLTHPRT_PLAN_RELEASE_WORKER_IMAGE` to an immutable
`repository@sha256:digest` reference for admitted prewarm, code-projection,
E&M distance and E&M attribution jobs. Selection requires the persisted run,
canonical queue/function/job target and matching worker selection. A caller
cannot supply its own image. Existing Jobs must match the complete expected
execution manifest before reuse.

Unset or blank configuration preserves the generic image. Initial pricing
projection, provider-membership projection, code calibration and unrelated
importers keep their existing image selection. This changes no credentials,
native admission, pricing policy, command or target.

## Collection worker image

Set the optional `HLTHPRT_CIGNA_COLLECTION_WORKER_IMAGE` to an immutable
`repository@sha256:digest` reference for admitted Cigna collection PTG jobs.
Selection requires the persisted validated collection proof, source-attempt
identity and exact recorded queue/function/job target. Request fields cannot
provide this capability or override the image. A conflicting caller job,
source, proof or image refuses the scoped launch.

The five existing PTG size workers retain their queues, commands and single-job
targets. Acquisition and internal finalization run in the same PTG image; this
setting does not select candidate-audit or pricing jobs. An unset or blank value
keeps the generic image fallback. Invalid configured pins refuse collection
launches. With the scoped image configured, an existing Job must match its
expected runtime, source and canonical target before reuse. Other importers and
authorized source-worker image priorities remain unchanged. The process launcher and API image/default are unaffected.

This is image selection for an already admitted run. It does not authorize
additional imports, change database permissions, attest audit/pricing children,
or establish that no older untargeted consumer exists on a shared queue.
