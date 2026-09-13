# Pharmacy report publication checks

The Florida MQA Pharmacy/Pharmacist download is a monthly cohort rather than a
complete pharmacy census. The official
[MQA Search Services and Data Download User Guide, section 3.9](https://mqa-internet.doh.state.fl.us/MQASearchServices/Content/HelpFile/MQA%20SearchServices%20and%20DataDownload%20UserGuide.pdf)
describes pharmacies newly licensed or closed during the previous month. The
authenticated `/PharmacyPharmacist` page gives the same scope and identifies the
file as a small subset of licensure data refreshed daily.

Consequently, `pharmacy_pharmacist` row, match, and fact totals are not compared
against the previous publication's totals. A smaller monthly cohort can publish
without enabling a volume-drop override. Required-source, nonempty-input,
schema, header, and quarantine validation still apply to this report. Other
sources retain their per-source volume checks, and overall provider and source
record volume guards still apply before the atomic publication swap.
