# Rebuild network search

Start the `registry-publication-worker` importer through the ordinary import
dashboard, or submit the same action to Import Control:

```http
POST /v1/runs
Content-Type: application/json
```

```json
{
  "importer": "registry-publication-worker",
  "node_id": "selected-node",
  "params": {}
}
```

Normal operator, client, node and worker admission still applies. The admitted
worker discovers the latest committed inputs using server-owned configuration;
the request cannot choose source readers, credentials, images or mappings.
Running this action requires no code change, CI run or separate approval packet.
The worker runtime and its PostgreSQL permissions must already be installed.

The rebuild combines available imported definitions with manual overrides.
It preserves unavailable sources' retained contributions until a complete
replacement is available. Negotiated pricing sources without a proven network
name appear as source-labelled provider sets. Provider-only evidence does not
assign a provider to every known office, and allowed amounts do not establish
network enrollment.

ACA plans link to existing companies through source-backed HIOS issuer mappings.
Company names and reported group affiliations retain their source reporting
period; a reported affiliation does not assert current parent ownership.
Missing or conflicting company mappings stay unlinked. Approved manual names,
archives and client visibility rules continue to take precedence.

Completed compatible captures are retained for reuse. An initial rebuild may
need to convert older captures to the current format. An unchanged input set
finishes without publishing another generation. A repeated HTTP request reuses
its admitted run; additional explicit requests during active work coalesce into
one durable successor. A new explicit request can retry a terminal failure.

The verified catalogue, membership and search outputs publish atomically.
Existing readers retain their pinned generation, and the previous generation
remains available for rollback. Failed preparation leaves the serving generation
unchanged and keeps completed reusable source captures.

The existing exact internal request selector remains supported. The ordinary
action does not enable a recurring refresh schedule.
