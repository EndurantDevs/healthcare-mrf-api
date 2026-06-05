# MRF Payer Master List (deduped)

> Companion to [`mrf_payer_source_registry.md`](mrf_payer_source_registry.md). The deduped union of
> every U.S. payer found across the researched sources, with which source(s) list it and the real
> public MRF Table-of-Contents (TOC) / transparency landing URL where known.
>
> **Source legend** — `M` MRF Data Solutions · `A` accessmrf.com · `P` Payerset docs · `S` Serif
> Health · `T` Turquoise Health · `B` PayerBenchmark · `U` authoritative/universe (CMS guide,
> accessmrf API, BCBS roster, carrier lists).
>
> **accessmrf `status`** noted where relevant: `current` / `stale` / `unsupported` (indexed, 0 files)
> / `archived`.
>
> **Counts:** ~**230+ distinct payers/sources**. Public-roster maxima per source: Payerset **183**,
> MRF Data Solutions **91**, accessmrf **90**; Serif (200+), Turquoise (219) gate their full lists.
> **Machine-readable seeds:** `https://www.accessmrf.com/api/sources` (TOC URLs + status) and
> `https://docs.payerset.com/sitemap.md` (183 payer slugs).

---

## A. National parents + subsidiaries

| Payer (parent) | Sources | Public MRF TOC / landing URL |
|---|---|---|
| **UnitedHealthcare** (UnitedHealth Group) | M A P S T B U | https://transparency-in-coverage.uhc.com/ · IFP: https://providermrf.uhc.com/IFP |
| ↳ Optum | A(unsupported) S U | https://transparency-in-coverage.optum.com/ |
| ↳ UMR · UnitedHealthcare Community Plan · PreferredOne | T U | (UHC-hosted) |
| **Elevance Health / Anthem** (14 Blue states) | M A P S T B(soon) U | https://www.anthem.com/machine-readable-file/search/ |
| **Aetna** (CVS Health) | M A P S T B U | https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICFI/machine-readable-transparency-in-coverage (brands: ALICSI, ALICUNDER100, ASA, AETNACVS, TEXASFI, AJCA) |
| ↳ Meritain Health (TPA) | A P U | https://health1.aetna.com/app/public/#/one/insurerCode=MERITAIN_I&brandCode=MERITAINOVER/machine-readable-transparency-in-coverage |
| ↳ Aetna Signature Administrators (TPA/network) | A U | https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ASA/machine-readable-transparency-in-coverage |
| ↳ First Health · Allina Health Aetna · Aetna Better Health | A P U | https://www.health1.firsthealth.com/app/public/#/one/insurerCode=FIRSTHEALTH_I&brandCode=FIRSTH/… |
| **Cigna** (The Cigna Group) | M A P S T B U | https://www.cigna.com/legal/compliance/machine-readable-files |
| **Humana** | M A(archived) P S T B U | https://developers.humana.com/cost-transparency |
| **Centene** | M A P S T B U | https://www.centene.com/price-transparency-files.html |
| ↳ Ambetter · WellCare · Fidelis Care (NY) · Health Net (CA) · Managed Health Network · QualChoice · Celtic | M A P S U | (Centene-hosted) · Health Net: https://www.healthnet.com/content/healthnet/en_us/transparency-files.html |
| **Kaiser Permanente** | M A(unsupported) P S T B U | https://healthy.kaiserpermanente.org/front-door/machine-readable |
| **Molina Healthcare** | M A P T B U | https://www.molinahealthcare.com/members/common/mrf.aspx |
| **HCSC** → BCBS IL, TX, NM, OK, MT | M A P U | per-plan `https://www.bcbs{il,tx,nm,ok,mt}.com/asomrf?EIN=…` (IL EIN 260241222) |
| **Highmark** → BCBS PA, DE, WV, Western NY, NE NY | M A P U | https://mrfdata.hmhs.com/ |

---

## B. BCBS independent licensees (≈34; "BCBS" ≠ one payer)

Roster: https://www.bcbs.com/about-us/blue-cross-blue-shield-system/state-health-plan-companies

| Blue plan | Sources | Public MRF TOC / landing URL |
|---|---|---|
| BCBS Alabama | M A P U | https://www.bcbsal.org/web/tcr |
| BCBS Arizona | M A(stale) P U | https://bcbsaz.healthsparq.com/app/public/#/one/insurerCode=BCBSAZ_I&brandCode=BCBSAZ/… |
| Arkansas BCBS (USAble) | A(stale) P T U | https://www.arkansasbluecross.com/interoperability/machine-readable-files |
| Blue Shield of California | A P U | https://web.healthsparq.com/app/public/#/one/insurerCode=BSCA_I&brandCode=BSCA/… |
| CareFirst (DC/MD/N. VA) | M A(stale) P S T U | https://individual.carefirst.com/individuals-families/mandates-policies/machine-readable-file.page |
| Florida Blue (GuideWell) | M A P S T U | https://www.floridablue.com/members/tools-resources/transparency/machine-readable-files |
| HMSA (BCBS Hawaii) | A P U | https://www.hmsa.com/help-center/transparency-in-coverage-machine-readable-files/ |
| Blue Cross of Idaho | M A P U | https://bci.sapphiremrfhub.com/ |
| Wellmark (IA/SD) | M A(stale) P U | https://web.healthsparq.com/app/public/#/one/insurerCode=WMRK_I&brandCode=WELLMARK/… |
| BCBS Kansas | M A(stale) P U | https://www.bcbsks.com/mrf |
| BCBS Kansas City (Blue KC) | M A P U | https://bcbskc.sapphiremrfhub.com/ |
| BCBS Louisiana | M A P T U | https://bcbsla.sapphiremrfhub.com/ |
| BCBS Massachusetts (+HMO) | M A P S U | https://transparency-in-coverage.bluecrossma.com/ |
| BCBS Michigan | M A P U | https://www.bcbsm.com/mrf/index/ |
| BCBS Minnesota | M A P S U | https://www.bluecrossmn.com/transparency-coverage-machine-readable-files |
| BCBS Mississippi | A P U | https://www.bcbsms.com/about-us/transparency-in-coverage |
| BCBS Nebraska | M A P U | https://bcbsneweb.healthsparq.com/app/public/#/one/insurerCode=BCBSNE_I&brandCode=BCBSNE/… |
| Horizon BCBS NJ | M A P T U | https://horizonblue.sapphiremrfhub.com/ |
| BCBS North Carolina | M A P S U | https://www.bluecrossnc.com/about-us/policies-and-best-practices/transparency-coverage-mrf |
| BCBS North Dakota | P U | — |
| Capital Blue Cross (PA) | M A P T U | https://capitalbluecross.healthsparq.com/app/public/#/one/insurerCode=CAPBC_I&brandCode=CAPBC/… |
| Independence Blue Cross (PA) | M A(stale) P S U | https://www.ibx.com/cmstic/?brand=qcc |
| BCBS Rhode Island | A P U | https://www.bcbsri.com/developers |
| BCBS South Carolina | A(stale) P U | https://www.southcarolinablues.com/web/public/brands/universal/transparency-in-coverage/ |
| BCBS Tennessee | M A P U | https://www.bcbst.com/tcr |
| BCBS Vermont | M A P S U | https://www.bluecrossvt.org/our-plans/employers-and-groups/machine-readable-files |
| BCBS Wyoming | P U | — |
| Triple-S Salud (PR) | P U | https://salud.grupotriples.com/en/transparency-in-coverage-machine-readable-files/ |
| Excellus BCBS (NY) | M A P S U | https://excellusbcbs.healthsparq.com/app/public/#/one/insurerCode=EXC_I&brandCode=EXC/… |
| Regence / Cambia (OR/UT/WA/ID) | M A P T U | https://www.regence.com/transparency-in-coverage/ |
| ↳ Asuris Northwest Health | A U | https://www.asuris.com/transparency-in-coverage/ |
| Premera Blue Cross (WA/AK) | M A(stale) P T U | https://premera.sapphiremrfhub.com/ |
| Medica (MN) | M A(stale) P U | https://web.healthsparq.com/healthsparq/public/#/one/insurerCode=MEDICA_I&brandCode=MEDICA&productCode=MRF/… |
| Capital Health Plan (FL) | M P | https://capitalhealth.com/transparency-in-coverage |

---

## C. Regional, provider-sponsored, Medicaid-MCO, DTC & TPA payers

(Deduped union of the long tail. Blank URL = not captured in research; pull from accessmrf API or
the payer's transparency page.)

| Payer | Type | Sources | Public TOC / landing URL |
|---|---|---|---|
| UPMC Health Plan | provider | M A(stale) P U | https://www.upmchealthplan.com/transparency-in-coverage/mrf/ |
| Geisinger Health Plan | provider | M A(stale) P U | https://www.geisinger.org/health-plan/no-surprises-act |
| AmeriHealth (NJ/PA) | regional | M A P S U | https://www.amerihealth.com/developer-resources/index.html |
| AmeriHealth Caritas | medicaid_mco | M U | https://www.amerihealthcaritas.com/price-transparency/ |
| EmblemHealth (NY) | regional | M A(stale) P U | https://transparency.emblemhealth.com/ |
| ConnectiCare (Emblem) | regional | M A(stale) P U | https://transparency.connecticare.com/ |
| MetroPlus Health (NY) | medicaid_mco | M P | https://www.metroplus.org/provider/provider-resources/price-transparency |
| MVP Health Care | regional | M A P S U | https://www.mvphealthcare.com/developers/machine-readable-files |
| CDPHP | regional | M A(unsupported) P S U | https://www.cdphp.com/interoperability/machine-readable-pricing-data-files |
| Univera Healthcare (NY) | regional | M S U | (Lifetime/Excellus-hosted) |
| Medical Mutual of Ohio | regional | M A P U | https://www.medmutual.com/Employers/Machine-Readable-Files.aspx |
| Priority Health (Corewell) | provider | M A(unsupported) P S U | https://www.priorityhealth.com/landing/transparency |
| HealthPartners (MN) | provider | M A(stale) P U | https://www.healthpartners.com/hp/legal-notices/disclosures/transparency/index.html |
| Sanford Health Plan | provider | M A(stale) P T U | https://www.sanfordhealthplan.com/transparency-in-coverage-rule |
| Avera Health Plans | provider | M A P U | https://portaldocs.dakotacare.com/MachineReadableFiles/Avera |
| Security Health Plan (WI) | provider | M A P U | https://www.securityhealth.org/insurance-resources/json |
| Dean Health Plan / Prevea360 | provider | M A(stale) P U | https://www.deancare.com/helpful-links/transparency-in-coverage |
| Quartz Benefits (WI) | provider | M A P U | https://quartzfhir.healthsparq.com/app/public/#/one/insurerCode=QUARTZ_I&brandCode=QUARTZ/… |
| Network Health (WI) | regional | M U | https://networkhealth.com/price-transparency |
| Harvard Pilgrim (Point32) | regional | M A(stale) P S U | https://www.harvardpilgrim.org/public/machine-readable-files |
| Tufts Health Plan (Point32) | regional | M A P S U | https://tuftshealthplan.com/visitor/legal-notices/machine-readable-files |
| Mass General Brigham HP / AllWays | provider | A(stale) P S U | https://massgeneralbrighamhealthplan.org/meet-us/transparency-regulations |
| Fallon Health (MA) | regional | M A P U | https://fallon.sapphiremrfhub.com/ |
| Health New England | provider | M P U | https://healthnewengland.org/transparency-coverage |
| Neighborhood Health Plan RI | medicaid_mco | A P U | https://www.nhpri.org/members/commercial-members-individual-family-plans/price-transparency-machine-readable-files/ |
| WellSense Health Plan (BMC) | medicaid_mco | M P S U | https://www.wellsense.org/machine-readable-files |
| Martin's Point (ME) | regional | P U | — |
| Moda Health (OR) | regional | M A(stale) P U | https://www.modahealth.com/privacy-center/machine-readable-files.shtml |
| PacificSource | regional | A P S U | https://pacificsource.com/resources/json-files |
| Providence Health Plan | provider | M A(unsupported) P U | https://mrfhub.providencehealthplan.com/ |
| SelectHealth (Intermountain) | provider | M A(unsupported) P U | https://selecthealth.org/machine-readable-data |
| Presbyterian Health Plan (NM) | provider | M A(unsupported) P U | https://www.phs.org/tools-resources/member/Pages/applicable-rates.aspx |
| Western Health Advantage (CA) | regional | M A(stale) P U | https://www.westernhealth.com/mywha/price-transparency/ |
| Sharp Health Plan (CA) | provider | P U | — |
| Valley Health Plan (CA) | medicaid_mco | P U | — |
| Oscar Health | dtc | M A P S B(soon) U | https://www.hioscar.com/transparency-in-coverage-files/oscar |
| Devoted Health | dtc/MA | P U | — |
| Clover Health | dtc/MA | P U | — |
| Alignment Health · SCAN | MA | U | — |
| Sidecar Health · Gravie | dtc | P U | — |
| Bright HealthCare (exited 2023) | dtc | M U | (historical) |
| CareSource | medicaid_mco | M S U | https://www.caresource.com/about-us/legal/transparency-in-coverage/ |
| Healthfirst (NY) | medicaid_mco | M P T U | https://healthfirst.org/machine-readable-files/ |
| L.A. Care · Inland Empire Health Plan | medicaid_mco | P U | — |
| Alameda Alliance · CalOptima · CalViva · Central CA Alliance · Contra Costa · Partnership HealthPlan · Health Plan of San Mateo · Denver Health · Hennepin Health | medicaid_mco | P U | — |
| Meridian (Centene) | medicaid_mco | U | — |
| Community Health Choice (TX) · Community Health Options (ME) · Community Care Plan (FL) | regional | A(unsupported) P | — |
| CommunityCare of OK | regional | M S P | https://www.ccok.com/transparency-coverage-information |
| Community Care of NC | medicaid_mco | P | — |
| Baylor Scott & White HP | provider | M P U | https://www.swhp.org/en-us/about-us/transparency-in-coverage |
| CHRISTUS Health Plan | provider | M P | https://www.christushealthplan.org/transparency-in-coverage |
| Cook Children's HP · Children's Community HP | provider | M P | — |
| Independent Health (NY) | regional | M P U | https://www.independenthealth.com/about-independent-health/transparency-in-coverage |
| Paramount Health Care (OH) | provider | M P | https://www.paramounthealthcare.com/transparency-in-coverage/ |
| ProMedica · The Health Plan (WV/OH) · Jefferson HP · Ohio Health Choice | provider | P U | — |
| Optima Health / Sentara | provider | M P S U | https://www.optimahealth.com/transparency-in-coverage |
| VIVA Health (AL) | provider | M P U | https://www.vivahealth.com/transparency-in-coverage/ |
| HAP (Health Alliance Plan, MI) | provider | M P S U | https://www.hap.org/transparency |
| McLaren Health Plan · Physicians Health Plan (MI) | provider | M P U | https://www.phpmichigan.com/About-Us/Transparency-in-Coverage |
| Physicians Health Plan of N. Indiana · Indiana University HP | provider | P S U | — |
| Aspirus Health Plan (WI) | provider | P S U | — |
| Group Health Coop (Eau Claire · SC Wisconsin) | regional | M P U | https://ghcscw.com/transparency-in-coverage |
| Common Ground Healthcare · Chorus Community HP · WPS Health (WI) | regional | P S U | — |
| Froedtert Health Plan · Hally/Health Alliance (IL) | provider | P U | — |
| SSM Health Plan · Mercy/MercyCare · OSF HealthCare | provider | P U | — |
| Sutter Health Plan · Scripps Health Plan | provider | P U | — |
| Piedmont Health Plan · Farm Bureau (TN) | regional | P | — |
| UCare (MN) | medicaid_mco | P U | — |
| UHA Health Insurance (HI) | regional | A P S U | https://www.uhahealth.com/important-notices/transparency-in-coverage-and-no-surprises-act-overview |
| HMAA (HI) | regional | A(stale) P S U | https://www.hmaa.com/provider-directory/machine-readable-files/ |
| Chinese Community HP (CCHP) · Culinary Health Fund (NV) · First Medical (PR) · Florida Complete Care | regional | P | — |
| AvMed (FL) | regional | M P U | https://www.avmed.org/transparency-in-coverage |
| Health Plan of Nevada | regional | M S U | https://www.healthplanofnevada.com/transparency-in-coverage |
| First Choice Health · First Choice Next | regional | M P | https://www.fchn.com/machine-readable-files |
| Alliant Health Plans (GA) | regional | M A(unsupported) P | https://allianthealthplans.com/price-transparency/ |
| AultCare/Aultman (OH) | regional | M P | https://aultcare.com/price-transparency |
| BridgeSpan (Cambia) · Coventry (Aetna, hist.) · WEA Trust (wound down) · Capital Health (NJ) | regional | M | (see landing on mrfdatasolutions/payers) |
| Johns Hopkins HealthCare · University of Utah HP · Children's/VNS Health (NY) | provider | M U | https://www.hopkinshealthplans.org/transparency-in-coverage/ |
| GEHA · MultiPlan (rental) · PHCS · HealthLink | network/TPA | S U | — |
| Allied Benefit Systems | TPA | A P S U | https://alliedbenefit.sapphiremrfhub.com/ |
| AmeriBen | TPA | A P U | https://github.com/AmeriBen/MRF |
| BRMS | TPA | P U | https://www.brmsonline.com/solutions/compliance-administration |
| Collective Health | TPA | P S U | https://transparency-in-coverage.collectivehealth.com/index.html |
| Health Plans Inc (HPI) | TPA | P S U | https://hpitpa.com/transparency-in-coverage-machine-readable-files/ |
| MedCost | TPA | P U | https://www.medcost.com/employers/resources/news/transparency-coverage-update-machine-readable-files |
| PBA / Professional Benefit Administrators | TPA | P U | https://www.pbaclaims.com/mrfs/ |
| WebTPA | TPA | P U | https://www.webtpa.com/rights-protections |
| HealthScope Benefits | TPA | P U | — |

---

## D. How to regenerate / extend this list

1. **`GET https://www.accessmrf.com/api/sources`** → 90 sources with `name`, `status`, `humanUrl`
   (real TOC), `numPlans/numFiles`. Best machine-readable seed.
2. **`https://docs.payerset.com/sitemap.md`** (or `llms-full.txt`) → 183 payer slugs; each
   `https://docs.payerset.com/payers/<slug>-price-transparency.md` carries a `mrf.payerset.com/<slug>`
   redirect + the payer's official CMS index.
3. **`https://www.mrfdatasolutions.com/payers`** → 91 payer landing-page links (static HTML).
4. **BCBS roster** + **TPAFS `machine_readable_homepages.csv`** to fill gaps the vendors miss.
5. Normalize names → dedup on **parent group + reporting-entity name**; expand BCBS umbrellas;
   classify each by hosting platform (§2.2 of the registry spec) before crawling.
