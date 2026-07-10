# Provider Directory Endpoint Support

This matrix describes maintained implementation and campaign configuration. It does not claim that a live probe succeeded, that an import ran, or that a dataset is current. Runtime and import status are written locally or on dev by the endpoint-acquisition harness to `reports/provider-directory-endpoint-acquisition/report.json`, or to its selected `--report` path; the report is not tracked.

Catalog inventory was last confirmed in `healthporta-dev` against `mrf.provider_directory_source` at `2026-07-10T09:33:00Z`. This timestamp confirms catalog coverage only; the tracked verification snapshot is the authority for terminal per-endpoint live status.

`None` access means the configuration expects public access, not that the endpoint is currently reachable. `Probe-only` entries have no resource acquisition configured and must not be treated as imported.

| Source | Configured support | Configured access requirement | Method | Resources | Canonical base | Source IDs | Known blocker or limitation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Idaho (`idaho`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint | https://api-idmedicaid.safhir.io/v1/api/provider-directory | pdfhir_b6fdc036a4686d0ab69f6f3a | Accepts api-ida-prd.safhir.io cursor continuations with checkpoints. |
| Molina (`molina`) | Acquisition-configured | None | REST | Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://api.interop.molinahealthcare.com/providerdirectory | pdfhir_0661ff143952c214d680d95d | Accepts molina.sapphirethreesixtyfive.com cursor continuations. |
| Michigan (`michigan`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint | https://api.interopstation.com/mdhhs/fhir | pdfhir_75511676b61b2bddb6f94322 | PractitionerRole pages are capped at 25. |
| Cigna (`cigna`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint | https://fhir.cigna.com/ProviderDirectory/v1 | pdfhir_46bcb068e81b0bc844e327e9 | Sequential REST pagination at _count=100 preserves Plan-Net network extensions; _count=75 returns false-empty search sets for role, organization, location, service, and affiliation collections. No Bulk. Expected nonempty initial searches retry transient HTTP-200 empty search sets with cooldowns. |
| Aetna Commercial/Medicare (`aetna-commercial-medicare`) | Acquisition-configured | OAuth2 client credentials | Bulk | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation | https://apif1.aetna.com/fhir/v1/providerdirectorydata | pdfhir_d68a896335981928bdbbb80e | OAuth2 client credentials and Bulk; Endpoint collection is unavailable. |
| Humana (`humana`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, Practitioner, PractitionerRole | https://fhir.humana.com/api | pdfhir_00a3d35311756763d420b0d6 | Overrides portal or stale paths to the public FHIR base. |
| IEHP (`iehp`) | Acquisition-configured | None | REST | HealthcareService, InsurancePlan, Location, Organization, Practitioner, PractitionerRole | https://fhir.iehp.org/provider-directory | pdfhir_56521fb2d273045f2c3b73dc | Normalizes portal and resource paths to the Provider Directory base. |
| Arkansas (`arkansas`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint | https://fite.ar-prd.gw02.abacusinsights.ai/provider-directory | pdfhir_d6b52e94ddf5ca7b9faafe17 | Uses synthetic _skip pagination with stable _id sorting. |
| HAP (`hap`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, Practitioner, PractitionerRole | https://provider-directory-r4.api.hap.org | pdfhir_eee751f2c17d5c473daf3060 | Rewrites the blocked cursor host and throttles requests to 20 seconds. |
| Washington (`washington`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint | https://wa.fhir.mhbapp.com/pd/api/v1 | pdfhir_ab222634556364c6b0815409 | Location pages are capped at 25. |
| Wyoming (`wyoming`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint | https://wy.fhir.mhbapp.com/pd/api/v1 | pdfhir_72257bf04f7fb816c0ad6fb9 | PractitionerRole pages are capped at 25. |
| Health Partners Plans (`health-partners-plans`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint | https://providerfhirapi.healthpartnersplans.com | pdfhir_384bff82e94cf93d2aec8af2 | Uses the official public Provider Directory FHIR catalog base. |
| AmeriHealth Caritas NH (`amerihealth-nh`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://api-ext.amerihealthcaritas.com/0900/provider-api | pdfhir_3e8f8d73e9f63b41f4f3fca5 | Plan code 0900; full-refresh pages target 250 rows. |
| AmeriHealth Caritas DE (`amerihealth-de`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://api-ext.amerihealthcaritas.com/7100/provider-api | pdfhir_5ec84ed54e6553236e7cda7b | Plan code 7100; full-refresh pages target 250 rows. |
| AmeriHealth Caritas Louisiana (`amerihealth-la`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://api-ext.amerihealthcaritas.com/2100/provider-api | pdfhir_6571cbf9a177e83423c891d3 | Plan code 2100; full-refresh pages target 250 rows. |
| AmeriHealth Caritas North Carolina (`amerihealth-nc`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://api-ext.amerihealthcaritas.com/1200/provider-api | pdfhir_7cc6538c83f3cb72897656ce | Plan code 1200; full-refresh pages target 250 rows. |
| AmeriHealth Caritas DC (`amerihealth-dc`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://api-ext.amerihealthcaritas.com/5400/provider-api | pdfhir_8e9bf902c028e349d6238382 | Plan code 5400; full-refresh pages target 250 rows. |
| AmeriHealth Caritas PA (`amerihealth-pa`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://api-ext.amerihealthcaritas.com/0500/provider-api | pdfhir_c1552ccaac4326f676710337 | Plan code 0500; full-refresh pages target 250 rows. |
| Horizon NJ (`horizon-nj`) | Probe-only | None | Probe | None configured | https://api.interopstation.com/njios/fhir | pdfhir_8a21548b8f8553c4b28a9b83 | Probe-only InteropStation base; no resource acquisition profile. |
| Texas TMHP (`texas-tmhp`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation | https://cmsinterop.tmhp.com/tmhp/fhir/pd/R4 | pdfhir_569c8c60329e96aefd3e5983 | Seven public collections use stable _id sorting and offset pagination; Endpoint is excluded because it is empty. |
| Nebraska (`nebraska`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation | https://dhhs-api.ne.gov/dhhs/trading-partner/api/cmsi/provider/1.0.0 | pdfhir_6843a6194d9793a0a6cc36b4 | Seven public collections use stable _id sorting and offset pagination; Endpoint is excluded because it returns HTTP 404. |
| UHC (`uhc`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://flex.optum.com/fhirpublic/R4 | pdfhir_0b5cfd565c53364a73981dcb | Six public collections use checkpoints, PractitionerRole reverse lookup, and one-row InsurancePlan pages; HealthcareService and Endpoint are excluded. |
| Missouri (`missouri`) | Probe-only | None | Probe | None configured | https://iox.mohealthnet.conduent.com/providerDirectory/api/R4 | pdfhir_99b383723a0de14e57780696 | Probe-only MO HealthNet R4 base; no resource acquisition profile. |
| Maine (`maine`) | Acquisition-configured | None | REST | Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole | https://maineproviderdirectory.verityanalytics.org/fhir | pdfhir_59350503a7be3498a85d8cf2 | Five collections are anonymously readable with ct cursor pagination; InsurancePlan, HealthcareService, and Endpoint are access-gated and excluded. |
| SCAN (`scan`) | Probe-only | None | Probe | None configured | https://providerdirectory.scanhealthplan.com | pdfhir_736ccaa7958218d4daeaf2e6 | Role reverse lookup and 100-page cap exist, but this campaign only probes. |
| Centene (`centene`) | Probe-only | None | Probe | None configured | https://iopc-pd.api.centene.com/iopc/pd/fhir/providerdirectory | pdfhir_00d0fb813f25b5d699a18eaa | Probe only; CloudFront or WAF access can block a runtime probe. |
| Contra Costa (`contra-costa`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation | https://ihyml0v6d9.execute-api.us-east-1.amazonaws.com/hxprod | pdfhir_8ee2865f928f1d67b8a86090 | Seven public collections follow opaque next-link pagination; the official catalog can return 403, so the confirmed fallback base is retained. |
| ALOHR (`alohr`) | Externally supported | Private connector | GraphQL | Practitioner, Organization, Location, PractitionerRole, OrganizationAffiliation | https://fhir.alabamaonehealthrecord.com/csp/healthshare/hsods/fhir/r4 | pdfhir_0f81c146991b27031b1ec366 | FHIR REST reads are auth-gated; the maintained GraphQL connector uses tenant alohr. |

## Known Not Importable

These sources are intentionally retained as blocked catalog evidence. They are not probe-only entries and have no runnable acquisition base.

| Source | Plan | Support | Required access | Registration | Operational state | Reviewed at | Primary evidence | Blocker |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Chorus Community Health Plans (fka Children's Community Health Plan) (`provider-directory-blocked-chorus-community-health-plans`) | Medicaid MCO | Not supported | None | Not required | Unreachable | 2026-07-10 | https://appconnect.chorushealthplans.org/developers/providerapi | Official client code identifies https://cchp.healthlx.com:9091/fhir, but its capability endpoint returned no usable response and the portal proxy returned HTTP 502 at review time. |
| First Medical Health Plan, Inc. (`provider-directory-blocked-first-medical-pr`) | Medicaid MCO | Not supported | User token | Required | Auth Gated | 2026-07-10 | https://devportal.firstmedicalpr.com/ApiLibrary | Official First Medical developer portal lists Practitioner, PractitionerRole, and Location APIs, but registration, client credentials, user-token access, and production approval are required. |
| Territory of Puerto Rico (`provider-directory-blocked-territory-of-puerto-rico`) | Medicaid FFS | Not supported | None | Not required | Not Published | 2026-07-10 | https://raw.githubusercontent.com/CMSgov/SMA-Endpoint-Directory/main/SMAEndpointDirectory.csv | CMS SMA Endpoint Directory lists Puerto Rico Provider Directory status as TBD and provides no production base, capability statement, FHIR version, or resource list. |

## Observed Live Verification

This tracked snapshot is separate from configured support. It records credential-safe terminal proof and the latest observed run state. When a newer active run supersedes older terminal proof, the old proof remains visible as `Superseded` and is not presented as current.

After a terminal campaign, use the report's `verification_update.argv` or run `python scripts/update_provider_directory_verification.py --report <credential-safe-report.json> --environment <environment>`. The updater rejects stale reports, manifest or campaign mismatches, and terminal labels backed by nonterminal runs.

Verification environment: `healthporta-dev`. Campaign: `provider-directory-canonical-acquisition-2026-07-10-v2`. Snapshot checked at `2026-07-10T17:19:26Z`.

| Source | Proof state | Terminal status | Terminal run ID | Current observation | Access verification | Terminal checked at | Terminal evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Idaho (`idaho`) | Current | Succeeded | run_685b1ff3a18c432e8b2333fe349ce69f | Not recorded | Verified | 2026-07-10T17:19:26Z | Not recorded |
| Molina (`molina`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Michigan (`michigan`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Cigna (`cigna`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Aetna Commercial/Medicare (`aetna-commercial-medicare`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Humana (`humana`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| IEHP (`iehp`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Arkansas (`arkansas`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| HAP (`hap`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Washington (`washington`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Wyoming (`wyoming`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Health Partners Plans (`health-partners-plans`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| AmeriHealth Caritas NH (`amerihealth-nh`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| AmeriHealth Caritas DE (`amerihealth-de`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| AmeriHealth Caritas Louisiana (`amerihealth-la`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| AmeriHealth Caritas North Carolina (`amerihealth-nc`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| AmeriHealth Caritas DC (`amerihealth-dc`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| AmeriHealth Caritas PA (`amerihealth-pa`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Horizon NJ (`horizon-nj`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Texas TMHP (`texas-tmhp`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Nebraska (`nebraska`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| UHC (`uhc`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Missouri (`missouri`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Maine (`maine`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| SCAN (`scan`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Centene (`centene`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| Contra Costa (`contra-costa`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |
| ALOHR (`alohr`) | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded | Not recorded |

Generated by `scripts/generate_provider_directory_support_docs.py`; do not edit this file directly.
