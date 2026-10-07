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
