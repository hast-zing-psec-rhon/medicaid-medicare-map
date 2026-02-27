# Medicaid / Medicare Map Dashboard (Prototype)

Working prototype for state and facility-level Medicaid/Medicare exposure analysis using authentic public data.

## What this prototype does

- National state map for Medicaid / Medicare / combined public dollars.
- State drilldown to facility table (for-profit, not-for-profit, government filters).
- Chain grouping (keyword-based + manual overrides).
- Facility revenue decomposition:
  - Total revenue
  - Medicare revenue (modeled estimate)
  - Medicaid revenue
  - Federal vs state Medicaid split (FMAP-based)
- Policy stress tool with cut sliders (Medicare, federal Medicaid, state Medicaid).
- CSV export of filtered facilities.

## Data sources (authentic public)

1. CMS Hospital Cost Reports API (dataset ID: `44060663-47d8-4ced-a115-b53b4c270acb`)
2. Federal Register FY 2026 FMAP table

Generated dataset artifacts are saved under:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/processed`

## Important methodology notes

- **Medicaid** uses reported `Net Revenue from Medicaid` from CMS cost reports.
- **Medicare** is modeled in prototype mode:
  - uses Medicare inpatient payment components + inpatient day-share proxy,
  - capped by total net patient revenue,
  - and flagged by `medicare_method` per facility.
- FMAP split is applied at state level to facility Medicaid revenue estimate.

## Run locally

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python etl/build_dataset.py
.venv/bin/uvicorn app.main:app --reload
```

Open: <http://127.0.0.1:8000>

## Tests

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
.venv/bin/pytest -q
```

## Manual chain overrides

Edit:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/manual/chain_overrides.csv`

Columns:
- `provider_ccn,chain_name,notes`

Then re-run dataset build:

```bash
.venv/bin/python etl/build_dataset.py
```

## Current limitations

- Medicare values are estimated (not complete audited all-payer Medicare revenue).
- Source year selected as latest full fiscal year from current CMS feed (currently FY 2023).
- Senior living facilities are deferred (phase 2).
