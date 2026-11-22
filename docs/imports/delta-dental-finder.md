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

Within one use scope and edition, jobs share the retained JSON response for an identical ZIP, network, and radius. A retry retains its original plan. A new job can extend that plan and reuse overlapping responses. A new directory edition starts a separate cache. Older HTML captures remain readable. An incomplete retained HTML window stays incomplete.

The finder displays at most 100 dentist-at-office cards per search. A capped window stays open as a coverage gap. The national finder's Medicare Advantage selector and regional commercial Advantage directories represent different products. Search counts, results from neighboring ZIPs and the combined finder view do not establish a complete state or national roster.

Export private office observations from a retained edition with the following command.

```sh
python -m process.delta_dental_directory_observations --capture edition.sqlite3 --output offices.ndjson
```

The export includes the search, dentist, office address, reported networks and available publisher identifiers. Each row represents a source observation and may recur across searches. The command writes a new private file and prints aggregate counts. A saved rendered page can also be examined offline with `--rendered-html`.
