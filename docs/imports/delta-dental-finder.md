# Delta Dental finder observations

The selected-source worker requests JSON from the national finder through the configured authenticated Source proxy. It records explicitly listed office network memberships and any valid provider NPI returned with the office. A search label or a matching dentist name at another address does not establish network membership.

Supply a client-managed data-use scope and directory edition in the worker's private search manifest. Use five-digit strings to preserve leading ZIP zeros. This synthetic plan selects every named national finder product.

```json
{
  "authorization_scope": "example-client-scope",
  "data_edition": "example-edition",
  "zip_inventory": ["01234"],
  "radius_miles": 30,
  "selector_scope": "named"
}
```

The default `primitive` scope searches PPO, Premier, DeltaCare USA, Medicare Advantage and Patient Direct separately. `named` also searches PPO Plus Premier. `named-and-all` adds the combined finder view. For a narrower plan, replace `zip_inventory`, `radius_miles` and `selector_scope` with `searches`, an array of objects containing `selector`, `zip_code` and `radius_miles`.

The private manifest may opt in to parallel capture with `concurrency` and `requests_per_second`. The concurrency must be an integer from 1 through 8. Parallel capture requires an explicit, positive, finite destination-wide rate and one distinct allowed proxy route per worker. The rate spaces request starts across all workers and successive jobs sharing the artifact root. It applies only to origin requests, so cached responses can be replayed during a publisher hold. The default uses one connection and the existing request budget. Verify exit IP diversity separately.

A server-owned Delta worker route can select a dedicated pool with `proxy_pool`, an object containing the pool `revision` and `secret_name`. The worker receives the referenced pool through its scoped secret selector.

The protected `max_origin_requests` defaults to 100,000. A paced plan can set it as high as 200,000. A plan with more unique windows than its budget is rejected before capture. Retries retain the original concurrency, rate and request budget.

Publisher access denials and rate limits stop new requests across all workers. Completed in-flight responses are retained. A 429 with a finite hold can be retried after the hold matures. Unknown outcomes, including interrupted attempts and 503 responses, are retained as gaps for review and are not automatically sent again. An indefinite hold requires operator review before fresh requests can start.

Within one use scope and edition, jobs share the retained JSON response for an identical ZIP, network, and radius. A retry retains its original plan. A new job can extend that plan and reuse overlapping responses. A new directory edition starts a separate cache. Older HTML captures remain readable. An incomplete retained HTML window stays incomplete.

The finder displays at most 100 dentist-at-office cards per search. A capped window stays open as a coverage gap. The national finder's Medicare Advantage selector and regional commercial Advantage directories represent different products. Search counts, results from neighboring ZIPs and the combined finder view do not establish a complete state or national roster.

Export private office observations from a retained edition with the following command.

```sh
python -m process.delta_dental_directory_observations --capture edition.sqlite3 --output offices.ndjson
```

The export includes the search, dentist, office address, reported networks and available publisher identifiers. Each row represents a source observation and may recur across searches. The command writes a new private file and prints aggregate counts. A saved rendered page can also be examined offline with `--rendered-html`.

## Preparing the next national edition

The shared five-digit inventory in `support/zip/usps_zip5_2026_11.txt` contains the 41,515 distinct ZIPs in the November 2026 USPS Area/District ZIP5 file. Each line preserves leading zeroes. For a later vintage, reproduce the list from the last five digits of each PostalPro ZIP5 record and verify its count and content before updating the reference. [USPS Area/District ZIP5](https://postalpro.usps.com/areadist_ZIP5)

Prepare a fresh nationwide Delta manifest from the preceding national manifest. Set the new edition and its authorized request budget. Existing ZIP order is preserved, followed by newly listed codes. The loader validates the budget against ZIPs times selectors. The example uses the single combined `all-only` selector.

```sh
python -m process.provider_directory_national_plan delta previous.json next.json --edition next-edition --max-origin-requests 50000
```

For Nomi, use the preceding national radius plan. The generator preserves its group, request limits and existing searches, then fills the verified Alaska, Caribbean and Pacific gaps. Use a fresh acquisition root and keep the previous plan with its retained root.

```sh
python -m process.provider_directory_national_plan nomi previous.json next.json
```

Exact-postal-code consumers can use `process.provider_directory_zip_inventory.national_zip_codes()`. VSP covers these areas through its global search tiles and publisher count reconciliation.
