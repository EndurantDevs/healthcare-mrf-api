# PTG Import

## Purpose
Imports Transparency in Coverage file structures, including table-of-contents driven discovery and parsing for in-network and allowed-amounts content.

## Source Websites
- Transparency in Coverage files published by payers and referenced from public TOC locations
- CMS transparency ecosystem context: <https://www.cms.gov/healthplan-price-transparency>

## Start Command
```bash
python main.py start ptg
```

## Common Options
```bash
python main.py start ptg --test
python main.py start ptg --toc-url <URL>
python main.py start ptg --toc-list ./toc_urls.txt
python main.py start ptg --in-network-url <URL>
python main.py start ptg --allowed-url <URL>
python main.py start ptg --provider-ref-url <URL>
```

## Main Outputs
- `ptg_file`
- `ptg_in_network_item`
- `ptg_negotiated_rate`
- `ptg_negotiated_price`
- `ptg_allowed_item`
- `ptg_allowed_payment`
- `ptg_allowed_provider_payment`
- `ptg_provider_group`
- `ptg_billing_code`

## Notes
- This pipeline is payer-file oriented and structurally different from CMS Medicare claims imports.
- It is useful when you need raw transparency file ingestion rather than CMS summarized public use files.
