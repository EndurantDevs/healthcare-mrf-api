# MRF Payer Source Registry & Storage Design

> **Status:** research + implementation spec. Captures *where* the universe of U.S. payer
> Transparency-in-Coverage (TiC) machine-readable files (MRFs) can be discovered, *what* we should
> store about payers / plans / sources, and the implemented `mrf-source-discovery` importer shape.
>
> **Relationship to existing specs.** This extends the existing MRF/PTG discovery work — see
> [`05-ptg-discovery-and-crawler.md`](import-control-plane/05-ptg-discovery-and-crawler.md)
> (crawl flow, source-status taxonomy), [`data_source_registry.md`](data_source_registry.md)
> (governance: when an importer ships, add entries to `docs/data-sources.md` + `docs/imports/`),
> and [`docs/imports/mrf.md`](../docs/imports/mrf.md) (the marketplace MRF pipeline).
> The deduped payer roster lives in the companion file
> [`mrf_payer_master_list.md`](mrf_payer_master_list.md).

---

## 1. The core fact that shapes everything

There is **no CMS-hosted master index of payer MRF URLs.** CMS owns only the *rule* and the
*schema* (the [price-transparency-guide](https://github.com/CMSgov/price-transparency-guide)).
Each payer self-publishes its own `index.json` Table-of-Contents (TOC) on its own site, and the
rule does not mandate a standard discovery path. **Discovery is decentralized**, so our registry
must crawl and maintain its own URL list — seeded from community indexes and (optionally) enriched
by paid vendors.

Two structural consequences:
- **"BCBS" is ~33–34 separate legal entities** under three umbrellas (Elevance/Anthem = 14 states,
  HCSC = 5 states, Highmark = 4 states) plus ~20 independents. Never collapse them into one payer.
- **One payer → many TOCs.** A single carrier exposes many brand/EIN-specific indexes
  (fully-insured vs self-insured vs <100-employee vs marketplace; Aetna alone has 7+ brand
  variants). Dedup on **parent entity + reporting-entity name**, not on raw URL count.
- **TPA-hosted sources are first-class discovery targets.** Self-funded group plans often publish
  through a third-party administrator or claims/platform vendor. The plan/issuer remains the
  regulated party, but TPA landing pages, metadata indexes, and direct body-file URLs are part of
  the public MRF source layer and must be cataloged separately from issuer-only sources.

---

## 2. Source registry (where to get MRF URLs / payer lists)

Ranked by usefulness to *us* (free + machine-readable first). Vendors are paid aggregators of the
same federally-public MRFs — useful for the **payer universe** and **enrichment/benchmarking**, not
as a free URL feed.

### 2.1 Free / authoritative (use these to ingest)

| Source | URL | Access | What it gives us | Notes |
|---|---|---|---|---|
| **accessmrf.com** (community index) | `https://www.accessmrf.com/api/sources` | **Free public JSON API, no auth** | ~90 sources, each with the **real payer MRF TOC URL** (`humanUrl`), `status` (current/stale/unsupported/archived), `numPlans`, `numFiles`, `numIndices`, `latestIndexDate`, `slug`, UUID | **Best free bootstrap.** Per-source/plan/file endpoints currently 500 — only list endpoint reliable. Independent project (Felix Haba). |
| **CMS price-transparency-guide** | `https://github.com/CMSgov/price-transparency-guide` | Free (GitHub) | Canonical TOC/in-network schema; `[YYYY-MM-DD]_[payer]_index.json` naming. **Schema v2.0 enforced from 2026-02-02.** | Parse/validate against this. Validator: `…/price-transparency-guide-validator`. Discussions thread is where payers report broken indexes. |
| **TPAFS/transparency-data** | `https://github.com/TPAFS/transparency-data` | Free (GitHub) | `price_transparency/insurers/machine_readable_homepages.csv` (landing pages) + `…_links.csv` (scraped MRF links) | Homepages CSV solid; links CSV partial/under development. |
| **BCBS Association roster** | `https://www.bcbs.com/about-us/blue-cross-blue-shield-system/state-health-plan-companies` | Free | Authoritative list of the ~34 independent Blue licensees by state | Needed to expand "BCBS" correctly; exclude non-U.S. (Canada/PR international) licensees. |
| **Carrier-link compliance lists** (e.g. Tetra Tech, octaxcol PDF) | `https://www.tetratech.com/careers/machine-readable-files/` | Free | Employer-compliance lists of carrier MRF landing pages | Convenience seed lists; verify freshness. |

### 2.2 Hosting platforms (predictable URL shapes — high-leverage for crawling)

Many payers don't self-host; they use a few shared platforms with templated URLs. Recognizing the
platform lets us derive/normalize many TOCs at once:

- **HealthSparq** — `https://{plan}.healthsparq.com/app/public/#/one/insurerCode={CODE}_I&brandCode={BRAND}/machine-readable-transparency-in-coverage` (Blue Shield CA, Excellus, Capital Blue, BCBS AZ/NE, Medica, Wellmark, Quartz, MVP, …). The importer derives the public `mrf.healthsparq.com/{tenant}-egress.../latest_metadata.json` map from `insurerCode`/`brandCode` and stores direct body URLs from that map.
- **Sapphire MRF Hub** — `https://{plan}.sapphiremrfhub.com/` (Allied, BCBS Idaho/LA/KC, Premera, Horizon, Fallon, …). The importer extracts server-rendered `/tocs/YYYYMM/*_index.json` links.
- **Aetna Health1** — `https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode={BRAND}/machine-readable-transparency-in-coverage` (Aetna brands, Meritain, First Health). This uses the same HealthSparq public metadata-map resolver.
- **Highmark hmhs** — `https://mrfdata.hmhs.com/`
- **BCBS ASO** — `https://www.{plan}.com/asomrf?EIN={EIN}` (HCSC plans: IL/TX/NM/OK/MT)
- **Change Healthcare** — `https://mrf-download.changehealthcare.com/{plan}/…` (BCBS VT, …)
- **TPA compliance pages / metadata text indexes** — e.g. Collective Health, WebTPA, PBA, BRMS,
  HPI, MedCost, AmeriBen. Some point to carrier/EIN search pages, while others expose small
  `.txt` metadata indexes with direct body-file URLs. Discovery parses those metadata indexes only;
  full body-file import remains PTG.

> Many of these are **JavaScript apps or EIN-gated**, so the landing URL is not directly
> crawlable — the crawler must drive the app or know the `brandCode`/`EIN` parameters.

### 2.3 Commercial vendors (paid — for universe + enrichment, not free URL feed)

| Vendor | Coverage (self-reported) | Access model | Public payer list? | Useful for |
|---|---|---|---|---|
| **Payerset** (`docs.payerset.com`) | "every payer/plan" internally; **183 documented** | Paid: Rate Explorer + Data Lake (API / S3 / Snowflake). Demo-gated, no public pricing. Docs public. | **Yes — 183, full** (also `sitemap.md`, `llms-full.txt`, per-payer `.md`) | Most complete *public* payer roster; per-payer compliance scorecards (1–5★). |
| **Turquoise Health** | **219 payers tracked / 97 scored**; 588 reporting entities | Paid: license + annual subscription. API / Trino / Snowflake / Databricks / S3 / SFTP. Demo-gated. | No (JS-rendered) | Free public tools: Hospital Transparency Tracker, Payer Transparency Scores (`/transparency-scores`), `/mrf_tracker`. |
| **Serif Health** | **200+ payers / 500–600+ networks**, ~90% commercial lives, since 2023 | Paid (~$1k/mo+ entry, regional). API / Signal portal / bulk delivery. Free `/sample`. | No (inventory behind login) | "Zombie-rate" cleaning, claims-validated rates, hosting-pattern intel. |
| **MRF Data Solutions** (`mrfdatasolutions.com/payers`) | **91 payers** (link directory) | **Free directory** of payer landing pages; paid analytics consulting is contact-gated | **Yes — 91, full** (landing-page links only, no `.json` TOCs) | Free seed list of payer transparency landing pages. |
| **PayerBenchmark** (`payerbenchmark.com`) | ~8 live + 2 "coming" (small/niche) | Freemium API: Starter free (1k calls/mo), Growth $299/mo, Enterprise custom | Partial (~8) | Vendor-stated only; low corroboration — treat as low-confidence. |

---

## 3. Recommended ingestion strategy

1. **Seed (free):** ingest `accessmrf.com/api/sources` (real TOC URLs + status) → merge with the
   TPAFS homepages CSV and the MRF Data Solutions landing-page list → normalize against the
   master payer roster (§ companion file) and the BCBS licensee roster.
2. **Classify by hosting platform** (§2.2) to normalize/expand brand/EIN variants.
3. **Crawl** per the existing [PTG discovery/crawler rules](import-control-plane/05-ptg-discovery-and-crawler.md):
   HEAD → ETag/Last-Modified compare → stream-parse TOC only (never download in-network bodies in
   discovery) → upsert source versions/files/plans/aliases → coverage metrics → status. Honor the
   **limited production crawl** caps (≤25 payer indexes or ≤10 GB first).
4. **Validate** parsed TOCs against the CMS schema (v1.x today, **v2.0 from 2026-02-02**).
5. **Enrich (optional, paid):** Payerset/Turquoise compliance scorecards; Serif zombie-rate /
   claims-validated signal — store as scorecard rows, do not treat as source of truth for URLs.
6. **Governance:** when the importer ships, add a source-mapping entry to `docs/data-sources.md`
   plus a runbook under `docs/imports/` per [`data_source_registry.md`](data_source_registry.md).

Implementation note: `mrf-source-discovery` is async Python rather than Rust because this import is
HTTP/discovery and metadata-upsert bound. Full PTG rate-body import remains the Rust-favorable path.

Operational runbook: [`docs/devops/mrf-source-discovery.md`](../docs/devops/mrf-source-discovery.md).
The runbook records the deployment schedules, TPA smoke commands, verification queries, and the
2026-06-05 local evidence for Collective Health, Aetna Signature Administrators, and Meritain.

---

## 4. Storage design — what to check & store

A normalized model that fits the existing PTG crawler (source versions, files, plans, aliases,
plan-file links, coverage metrics, status taxonomy). Suggested entities:

### `mrf_payer` (reporting entity / carrier)
| field | notes |
|---|---|
| `payer_id` | stable internal id |
| `canonical_name` | e.g. "Blue Cross Blue Shield of Michigan" |
| `aliases[]` | observed name variants across sources (dedup key) |
| `parent_group` | UHG · Elevance(Anthem) · CVS(Aetna) · Cigna · Humana · Centene · HCSC · Highmark · Kaiser · Molina · BCBSA-independent · regional · TPA · DTC |
| `entity_type` | national · blue · regional · medicaid_mco · tpa · provider_sponsored · dtc |
| `states[]`, `ein[]` | coverage states; EIN(s) seen in ASO indexes |
| `lifecycle` | active · acquired · defunct (e.g. Bright/Friday) |
| `source_coverage[]` | which registry sources list it (accessmrf, payerset, mrfdatasolutions, serif, turquoise, payerbenchmark, universe) |

### `mrf_source` (a discoverable index endpoint)
| field | notes |
|---|---|
| `source_id`, `payer_id` | |
| `source_type` | payer_self_hosted · hosting_platform · vendor_aggregator · community_index |
| `hosting_platform` | healthsparq · sapphire · aetna_health1 · highmark_hmhs · bcbs_asomrf · change_healthcare · s3 · custom |
| `access_model` | free · api · paid · gated (terms/login/EIN) |
| `index_url`, `human_url` | TOC entry point(s); `brand_code` / `ein` params if applicable |
| `status` | active · stale · broken · needs_review · disabled · duplicate (matches PTG taxonomy) |
| `etag`, `last_modified`, `content_version`, `last_crawled_at` | freshness/skip logic |
| `num_plans`, `num_files`, `num_indices`, `latest_index_date`, `total_compressed_size` | coverage metrics |
| `schema_version` | v1.x · v2.0 |

### `mrf_plan`, `mrf_file`, `mrf_crawl_run`, `mrf_payer_scorecard`
- **`mrf_plan`**: `plan_id` (HIOS/etc.), `plan_id_type`, `plan_name`, `market_type`
  (group/individual/medicare/medicaid), `reporting_entity_name`, `reporting_structure`, `payer_id`.
- **`mrf_file`**: `file_type` (in_network_rates · allowed_amounts · toc · provider_reference),
  `url`, `is_signed_url` (do **not** leak signed/private URLs to public coverage APIs),
  `size_bytes` (in-network files reach ~1 TB — index the TOC, not the body), `plan_links[]`,
  `schema_version`, `last_seen_at`.
- **`mrf_crawl_run`**: `started_at`, `status`, `etag_skipped`, `files_discovered`,
  `plans_discovered`, `errors[]`, `bytes_streamed`.
- **`mrf_payer_scorecard`** (optional enrichment): `source` (payerset/turquoise), `score` (1–5),
  `update_cadence`, `file_accessibility_pct`, `notes`.

### Checks worth running (quality/compliance signals)
- **Reachability:** is the `index_url` HTTP-reachable, or gated (terms/login/EIN/JS)?
- **Schema validity:** TOC validates against CMS schema (flag v1↔v2 transition).
- **Freshness:** index dated within the expected monthly cadence (`latest_index_date`).
- **Dedup/identity:** stable plan identity across URL reorganizations (PTG rule); detect duplicate
  sources (e.g. shared-backend Blue plans with identical plan/file counts).
- **Sanity:** non-zero plan/file counts; flag "indexed but 0 files" (accessmrf `unsupported`).
- **Provenance/licensing:** third-party seed sources require provenance/license review before
  promotion; official payer TOCs are the durable truth.

---

## 5. Caveats (carry these into the importer)

- **No federal master index** — maintain our own; community indexes (accessmrf, TPAFS) + payer
  crawl are the durable layer; vendors are paid enrichment.
- **URLs are volatile** — paths rotate monthly; several known-good URLs drift or 502 (e.g. Humana's
  `developers.humana.com/cost-transparency` is the stable entry, not the `syntheticdata` path).
  Re-validate periodically.
- **JS / EIN gating** — Aetna Health1, HealthSparq, Sapphire, BCBS `asomrf?EIN=` need app-driving or
  known params; some payers block automated access or gate behind "accept terms."
- **File scale** — TOCs are small JSON; referenced in-network files are 100s of GB to ~1–1.5 TB
  (Cigna/UHC warn explicitly). **Index TOC URLs, not rate bodies.**
- **Schema v2.0 (2026-02-02)** — files posted on/after follow v2.0; parser must handle both.
- **Free indexes under-cover** small regional plans, rental/behavioral networks, and many TPAs;
  paid vendors claim 200+ vs accessmrf's ~90 — use the BCBS roster + master list to detect gaps.
- **Vendor numbers are marketing** — PayerBenchmark's counts are internally inconsistent; treat
  vendor-self-reported coverage/pricing as directional.

---

## 6. Source provenance (this spec)

Compiled 2026 from: accessmrf.com (`/api/sources`), docs.payerset.com (`sitemap.md` + 183 payer
pages), mrfdatasolutions.com/payers (91), serifhealth.com, turquoise.health, payerbenchmark.com,
CMS price-transparency-guide, TPAFS/transparency-data, and the BCBS Association roster. Per-source
detail and the full deduped roster are in [`mrf_payer_master_list.md`](mrf_payer_master_list.md).
