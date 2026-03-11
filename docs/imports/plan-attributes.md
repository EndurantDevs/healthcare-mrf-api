# Plan Attributes Import

## Purpose
Imports normalized ACA marketplace plan attributes, benefits, prices, and rating-area data used for plan browsing and comparisons.

## Source Websites
- CMS Exchange Public Use Files: <https://www.cms.gov/marketplace/resources/data/public-use-files>
- CMS state-based exchange public use files: <https://www.cms.gov/marketplace/resources/data/state-based-public-use-files>

## Start Command
```bash
python main.py start plan-attributes
```

## Test Mode
```bash
python main.py start plan-attributes --test
```

## Worker
```bash
python main.py worker process.Attributes --burst
```

## Main Outputs
- `plan_attributes`
- `plan_prices`
- `plan_rating_areas`
- `plan_benefits`

## Notes
- This import focuses on plan metadata and rating information, not provider benchmarking.
- The worker shutdown phase performs the publish/swap behavior; there is no separate `finish plan-attributes` command.
