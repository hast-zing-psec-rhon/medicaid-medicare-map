# Medicaid / Medicare Map Dashboard (Prototype)

Working prototype for state and facility-level Medicaid/Medicare exposure analysis using authentic public data.

## What this prototype does

- National state map for Medicaid / Medicare / combined public dollars.
- Payer-scope toggle:
  - Public-only (default)
  - Comprehensive (public + modeled private insurance)
- State drilldown to facility table (for-profit, not-for-profit, government filters).
- State private-insurer market-share panel (covered lives, premium, claims basis).
- Confidence badges for private-insurer data quality tiers (A/B/C/U).
- Chain grouping (CMS ownership-based system mapping + keyword fallback + manual overrides).
- Facility revenue decomposition:
  - Total revenue
  - Medicare revenue (modeled estimate)
  - Medicaid revenue
  - Private insurance revenue (modeled from residual revenue)
  - Federal vs state Medicaid split (FMAP-based)
- Policy stress tool with cut sliders (Medicare, federal Medicaid, state Medicaid, private), plus optional insurer-specific private cut overrides.
- CSV export of filtered facilities.


## Frontend UX upgrades (implemented from Claude design feedback)

- Dark-first institutional visual system with light-mode toggle and design tokens.
- Top tab navigation (`National`, `Scenarios`) plus breadcrumb drill path.
- Expanded national KPI strip (8 metrics), top-risk mini table, and quantile-based choropleth rendering.
- State drilldown enhancements:
  - Sticky table headers
  - Pinned first and last columns in facility table
  - Density modes (compact/default/comfortable)
  - Conditional formatting for high-dependency rows
  - Search/filter chips and filter undo history (`Ctrl/Cmd+Z`)
  - Chain decomposition chart, concentration bubble chart, and dependency histogram
- Facility detail slide-out with stacked revenue decomposition and provenance badges.
- Scenario page upgrades:
  - Scope selectors (state, chain)
  - Preset save/load
  - Tornado chart for sensitivity impact
- Keyboard shortcuts (focus-aware): `1`, `2`, `/`, `j`, `k`, `Enter`, `e`, `Esc`, `?`.
- Data vintage badges and clearer modeled-data warnings.

## Robust local hosting (macOS launchd)

To keep the app stable on your machine (auto-restart on crash and auto-start on login), use the included launchd service:

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
./scripts/service.sh install
./scripts/service.sh status
```

Service management commands:

```bash
./scripts/service.sh start
./scripts/service.sh stop
./scripts/service.sh restart
./scripts/service.sh status
./scripts/service.sh logs
./scripts/service.sh uninstall
```

The app is served at `http://127.0.0.1:8080`.

Local/API health endpoint:
- `http://127.0.0.1:8080/health`

## Data sources (authentic public)

1. CMS Hospital Cost Reports API (dataset ID: `44060663-47d8-4ced-a115-b53b4c270acb`)
2. Federal Register FY 2026 FMAP table
3. CMS Hospital Enrollments (PECOS)
4. CMS Hospital All Owners (PECOS)
5. CMS Medical Loss Ratio (MLR) public use files

Generated dataset artifacts are saved under:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/processed`

## Important methodology notes

- **Medicaid** uses reported `Net Revenue from Medicaid` from CMS cost reports.
- **Medicare** is modeled in prototype mode:
  - uses Medicare inpatient payment components + inpatient day-share proxy,
  - capped by total net patient revenue,
  - and flagged by `medicare_method` per facility.
- FMAP split is applied at state level to facility Medicaid revenue estimate.
- **Private insurance** is modeled from residual `other_revenue` using ownership-specific priors (for-profit / nonprofit / government), with uninsured residual tracked separately.
- State-level private insurer market share is sourced from CMS MLR PUF (covered lives, premium, and claims basis).
- **Chain / health-system mapping** uses a three-step hierarchy:
  1. CMS PECOS hospital ownership datasets (`Hospital Enrollments` + `Hospital All Owners`) to map CCNs to multi-site owner organizations,
  2. keyword inference from facility names for residual unmatched records,
  3. manual overrides in `data/manual/chain_overrides.csv`.

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

## Deployment architecture

- **Frontend:** Vercel static hosting
- **Backend:** Google Cloud Run (containerized FastAPI)
- **Browser/API connection:** frontend calls the backend via `PUBLIC_API_BASE_URL`

This repo is now set up for that split deployment model:
- `vercel.json` builds a static frontend into `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/dist`
- `Dockerfile` packages the FastAPI backend for Cloud Run
- deployment helpers live in `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/scripts`

## Deployment-sensitive environment variables

Backend env vars:
- `APP_SERVE_FRONTEND=false` for Cloud Run API-only mode
- `APP_ALLOWED_ORIGINS` comma-separated exact origins
- `APP_ALLOWED_ORIGIN_REGEX` regex for preview domains (default deploy script uses `^https://.*\\.vercel\\.app$`)

Frontend build env vars:
- `PUBLIC_API_BASE_URL` — the Cloud Run base URL, e.g. `https://medicaid-medicare-map-api-xxxxx-uc.a.run.app`

## Build frontend locally for Vercel

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
npm run build
```

This writes:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/dist/index.html`
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/dist/static/app.js`
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/dist/static/styles.css`
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/dist/static/config.js`

To point the frontend at a deployed backend during build:

```bash
PUBLIC_API_BASE_URL="https://YOUR-CLOUD-RUN-URL" npm run build
```

## Deploy backend to Google Cloud Run

Prerequisites:
- authenticated `gcloud`
- a target Google Cloud project

Deploy with:

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
./scripts/deploy_cloud_run.sh YOUR_GCP_PROJECT_ID
```

Optional args:

```bash
./scripts/deploy_cloud_run.sh YOUR_GCP_PROJECT_ID medicaid-medicare-map-api us-central1
```

The script:
- launches `gcloud auth login` if needed
- enables required APIs
- deploys from source to Cloud Run
- prints the backend URL for frontend configuration

## Deploy frontend to Vercel

Deploy with a specific backend URL:

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
./scripts/deploy_vercel.sh https://YOUR-CLOUD-RUN-URL
```

The script:
- launches `vercel login` if needed
- deploys with `PUBLIC_API_BASE_URL` set at build time

## One-command full-stack deployment

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
./scripts/deploy_full_stack.sh YOUR_GCP_PROJECT_ID
```

This:
1. deploys the backend to Cloud Run,
2. captures the resulting Cloud Run URL,
3. deploys the frontend to Vercel against that backend.

## Deployment caveats

- `data/processed/emma_cache.db` is intentionally excluded from deployment source; Cloud Run will rebuild cache entries on demand.
- `data/manual/portfolio_holdings.csv` is intentionally excluded from deployment source; portfolio-linkage endpoints will return empty/default results unless you add a production data path later (for example GCS or a managed database).
- `data/manual/emma_issuer_map.csv` is also excluded from Cloud Run upload; runtime behavior is still supported because the deployed processed facility data already carries the mapped EMMA fields baked into `data/processed/facilities.csv`.

## Manual chain overrides

Edit:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/manual/chain_overrides.csv`

Columns:
- `provider_ccn,chain_name,notes`

Then re-run dataset build:

```bash
.venv/bin/python etl/build_dataset.py
```

## EMMA issuer mapping (not-for-profit hospitals)

Mapping file:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/manual/emma_issuer_map.csv`

Generate auto-candidates (hybrid auto + QA workflow):

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
.venv/bin/python etl/generate_emma_mapping_candidates.py --year 2023
```

Validate mapping quality before rebuild:

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
.venv/bin/python etl/validate_emma_mapping.py
```

Rebuild processed data after mapping updates:

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
.venv/bin/python etl/build_dataset.py
```

The facility table/API now includes:
- `emma_mapping_status`
- `emma_issuer_url`
- `emma_issuer_name`
- `emma_match_score`

## Portfolio holdings input (CUSIP CSV)

Holdings file:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/manual/portfolio_holdings.csv`

Expected columns:
- `portfolio_id`
- `cusip9`
- `position_par` (optional)
- `market_value` (optional)
- `as_of_date` (optional)

Linkage APIs:
- `GET /api/v1/portfolio/holdings/summary`
- `GET /api/v1/facilities/{facility_id}/emma-link?fiscal_year=YYYY`
- `GET /api/v1/facilities/{facility_id}/emma-link?fiscal_year=YYYY&include_fallback=true&portfolio_id=default`
- `GET /api/v1/facilities/{facility_id}/portfolio-linkage?fiscal_year=YYYY&portfolio_id=default&force_refresh=false`

When `include_fallback=true`, the EMMA link endpoint attempts an active-security fallback if no issuer profile is mapped:
- runs EMMA QuickSearch with `ExcludeMatured=true` and `ExcludeCompletelyCalled=true`
- attempts to identify a related CUSIP (preferring a CUSIP-9 matched to your portfolio holdings)
- returns `emma_resolved_url` as either issuer URL, CUSIP search URL, or issue URL fallback

`portfolio-linkage` uses an on-demand EMMA scrape with a 24-hour cache in:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/processed/emma_cache.db`

## Bulk EMMA CUSIP fallback pass (with OCR)

Precomputed fallback map file:
- `/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map/data/manual/emma_cusip_fallback_map.csv`

Run bulk fallback pass for latest-year not-for-profit facilities:

```bash
cd "/Users/mv/code/Municipal Bonds/Medicaid - Medicare Map"
.venv/bin/python etl/bulk_emma_cusip_fallback_pass.py --year 2023 --ownership not_for_profit --refresh
```

Re-run only rows that previously errored or were not found:

```bash
.venv/bin/python etl/bulk_emma_cusip_fallback_pass.py --year 2023 --ownership not_for_profit --only-status error,not_found --max-retries 5 --cooldown-sec 45
```

Notes:
- The pass calls EMMA QuickSearch with active/outstanding filters (`ExcludeMatured=true`, `ExcludeCompletelyCalled=true`).
- It resolves security tokens from `GetFinalScaleData` and OCRs CUSIP images from `ImageGenerator.ashx` via Tesseract.
- The API endpoint `GET /api/v1/facilities/{facility_id}/emma-link?...&include_fallback=true` now checks this precomputed map first, then falls back to live lookup.

## Current limitations

- Medicare values are estimated (not complete audited all-payer Medicare revenue).
- Historical coverage includes the latest 10 CMS annual final hospital cost report releases (currently 2014-2023 vintages).
- Ownership-based chain mapping uses current PECOS ownership relationships and is applied across historical years (does not fully reconstruct year-by-year ownership changes).
- Senior living facilities are deferred (phase 2).
