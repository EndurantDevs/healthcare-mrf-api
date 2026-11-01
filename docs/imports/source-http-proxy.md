# Source HTTP proxy

The shared FHIR REST importer supports an explicitly selected proxy pool for an
exact Source and canonical HTTPS endpoint. Sources absent from the policy retain their
existing transport. The common helper in `process/ext/source_http_proxy.py` also
provides policy validation, route selection and cooldown snapshots for other
Source-aware HTTP callers; those callers must bind and use it explicitly.

## Trusted Source policy

Set `HLTHPRT_SOURCE_HTTP_PROXY_POLICY_JSON` in trusted deployment configuration:

```json
{
  "version": 1,
  "revision": "policy-v1",
  "sources": [
    {
      "source_id": "example-directory",
      "endpoint_scope": "https://directory.example/fhir",
      "use_proxy": true
    }
  ]
}
```

The Source ID and canonical endpoint must match exactly. Imported seed metadata
does not grant proxy access. Alias rows sharing one import group must agree on
their policy. Signed current-version census contracts reject this opt-in until
their transport contract explicitly supports it. There is no admin UI setting.
When any Source is enabled, jobs that can probe or acquire FHIR data require a
nonempty canonical `source_ids` list. Unscoped, string and alias-only selectors
are refused before launch. Seed-only, catalog-only and existing local publication
or serving modes keep their ordinary validation and transport.
Bulk export, official provider-file catalogs and the separate GraphQL connector
reject this opt-in before their direct network requests; their transport
contracts need separate support.

## Shared credential pool

Store `HLTHPRT_HTTP_PROXY_POOL_JSON` in an immutable, versioned worker Secret:

```json
{
  "version": 1,
  "revision": "pool-v1",
  "proxies": [
    {"id": "route-1", "url": "https://example-user:example-password@proxy.example:10001"},
    {"id": "route-2", "url": "https://example-user:example-password@proxy.example:10002"},
    {"id": "route-3", "url": "https://example-user:example-password@proxy.example:10003"}
  ]
}
```

The pool supports 1–64 routes. Each route ID must identify one fixed endpoint.
Percent-encode punctuation in
URL user information. Prefer HTTPS to encrypt the proxy connection as well as
the origin connection. Keep the same pool payload and revisions at every
execution location. The API receives only the policy and two nonsecret references:
`HLTHPRT_HTTP_PROXY_POOL_SECRET_NAME` and `HLTHPRT_HTTP_PROXY_POOL_REVISION`.
It does not read pool credentials.
Configure these three server settings only on the API process. Keep them out of
ConfigMaps and environment blocks inherited by workers; the launcher supplies
each selected worker's frozen policy explicitly.

The explicit worker credential selector must name the exact pool Secret and key
`HLTHPRT_HTTP_PROXY_POOL_JSON`, set `sourceProxyPool: true`, and restrict
`purposes`, `importers` and `workerClasses` to the admitted Source worker. Pool
credentials must not enter broad environment inheritance. A proxy-enabled Source
also requires an explicit immutable worker route. Existing Source routes keep
their image and runtime Secret selection.

The launcher supplies the selected worker with a narrowed single-Source policy,
`HLTHPRT_SOURCE_HTTP_PROXY_BINDING_JSON` and
`HLTHPRT_SOURCE_HTTP_PROXY_COOLDOWNS_JSON`. These capture the Source, endpoint,
policy revision, pool revision and native ancestry's cooldown state. They are
server-generated launch fields, not import request parameters. An enabled worker
rejects missing or changed bindings before its first request.

## Requests and quota handling

The importer freezes policy, credentials and pool for one run. It uses persistent
asynchronous HTTP sessions and one cached verified TLS context. Proxy credentials
authenticate the CONNECT hop; origin authentication remains separate. OAuth token
refresh runs outside the event loop. Origin and proxy certificates are verified,
encoded cursor bytes are preserved, redirects are refused, and an enabled Source
cannot silently fall back to direct transport.

Resource cursors prefer their assigned route. A recognized upstream quota
response cools down that destination/route pair and retries the exact same URL
through another eligible fixed route. Existing request slots and pacing still
bound these attempts, and each logical fetch tries each route at most once.
An explicit failure to connect to a proxy can also try another route within that
budget. Origin TLS failures and arbitrary authorization failures do not rotate.
Quota-driven route changes currently recognize the importer's Molina call-volume
response; other upstream errors retain their existing handling.
If no route is eligible, acquisition stops at the saved cursor. Known reset
deadlines determine the earliest safe retry; an unknown deadline requires an
operator decision. A successful request does not erase another route's cooldown.

Nonsecret bindings and cooldowns remain in the run's terminal audit, including
failure and cancellation. Native retry admission validates the failed ancestry
and restores its cooldowns before network access. A retained run cannot silently
switch between direct and proxied transport or to a different pool revision.

A fresh route does not authorize a retained retry. After a failed fresh root
exists, configure its exact observed root and matching admission rules before
requesting continuation. Preserve its image, runtime Secret and captured transport
state. Do not change an active fresh root to retained routing.

Adding fixed routes changes the central pool, not each Source policy. Publish a
new immutable Secret and revision through the normal configuration path, and keep
the old version available for existing runs and rollback. Cursor concurrency is
independent of the number of proxy endpoints. A rotating gateway with hidden exit
IPs cannot be substituted for fixed routes: its reconnect and quota accounting
requires a separately supported transport contract.
