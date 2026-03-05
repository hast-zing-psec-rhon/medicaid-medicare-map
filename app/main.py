from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from app.data_store import (
    available_years,
    get_facility_emma_link,
    get_chain_insurer_exposure,
    get_chain_trend,
    get_chain_detail,
    get_chains,
    get_facility_insurer_exposure,
    get_facility_trend,
    get_facilities,
    get_facility,
    get_national_trend,
    get_state_insurers,
    get_state_trend,
    get_state_summary,
    get_states,
    load_data,
    run_scenario,
)
from app.emma_cache import EmmaCache
from app.emma_client import EmmaClient
from app.emma_fallback_store import EmmaFallbackStore
from app.portfolio_store import PortfolioStore
from app.runtime import get_runtime_settings
from app.schemas import ScenarioRequest

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / "frontend"
RUNTIME = get_runtime_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = load_data()
    app.state.emma_client = EmmaClient()
    app.state.emma_cache = EmmaCache()
    app.state.emma_fallback_store = EmmaFallbackStore.load()
    app.state.portfolio_store = PortfolioStore.load()
    yield


app = FastAPI(
    title="Medicaid-Medicare Hospital Exposure Dashboard API",
    version="0.1.0",
    lifespan=lifespan,
)
app.state.emma_client = EmmaClient()
app.state.emma_cache = EmmaCache()
app.state.portfolio_store = PortfolioStore.load()

app.add_middleware(
    CORSMiddleware,
    allow_origins=RUNTIME.allowed_origins,
    allow_origin_regex=RUNTIME.allowed_origin_regex,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_store():
    if not hasattr(app.state, "store"):
        app.state.store = load_data()
    return app.state.store


def get_emma_client() -> EmmaClient:
    if not hasattr(app.state, "emma_client"):
        app.state.emma_client = EmmaClient()
    return app.state.emma_client


def get_emma_cache() -> EmmaCache:
    if not hasattr(app.state, "emma_cache"):
        app.state.emma_cache = EmmaCache()
    return app.state.emma_cache


def get_portfolio_store() -> PortfolioStore:
    if not hasattr(app.state, "portfolio_store"):
        app.state.portfolio_store = PortfolioStore.load()
    return app.state.portfolio_store


def get_emma_fallback_store() -> EmmaFallbackStore:
    if not hasattr(app.state, "emma_fallback_store"):
        app.state.emma_fallback_store = EmmaFallbackStore.load()
    return app.state.emma_fallback_store


def _portfolio_linkage_payload(facility_link: dict, portfolio_id: str) -> dict:
    if facility_link["emma_mapping_status"] != "mapped" or not facility_link["emma_issuer_id"]:
        return {
            "facility_id": facility_link["facility_id"],
            "facility_name": facility_link["facility_name"],
            "state_code": facility_link["state_code"],
            "ownership_group": facility_link["ownership_group"],
            "portfolio_id": portfolio_id,
            "mapping": facility_link,
            "cache_status": "n/a",
            "refresh_triggered": False,
            "fetched_at_utc": "",
            "ttl_seconds": 86400,
            "owned_securities": [],
            "related_documents": [],
            "issuer_security_count": 0,
            "issuer_document_count": 0,
            "scrape_status": "skipped",
            "scrape_error": "Facility is not mapped to an EMMA issuer",
        }

    holdings_store = get_portfolio_store()
    holdings_store.reload()
    holdings_rows = holdings_store.holdings_for_portfolio(portfolio_id)
    holdings_cusips = {str(r.get("cusip9", "")).strip() for r in holdings_rows if str(r.get("cusip9", "")).strip()}

    if not holdings_cusips:
        return {
            "facility_id": facility_link["facility_id"],
            "facility_name": facility_link["facility_name"],
            "state_code": facility_link["state_code"],
            "ownership_group": facility_link["ownership_group"],
            "portfolio_id": portfolio_id,
            "mapping": facility_link,
            "cache_status": "n/a",
            "refresh_triggered": False,
            "fetched_at_utc": "",
            "ttl_seconds": 86400,
            "owned_securities": [],
            "related_documents": [],
            "issuer_security_count": 0,
            "issuer_document_count": 0,
            "scrape_status": "skipped",
            "scrape_error": "No holdings found for requested portfolio",
        }

    scraped = get_emma_client().fetch_portfolio_linkage(
        issuer_id=facility_link["emma_issuer_id"],
        holdings_cusips=holdings_cusips,
    )
    owned = [r for r in scraped.get("owned_securities", []) if str(r.get("cusip9", "")) in holdings_cusips]
    docs = [
        d
        for d in scraped.get("related_documents", [])
        if (not str(d.get("related_cusip9", "")).strip()) or (str(d.get("related_cusip9", "")) in holdings_cusips)
    ]

    return {
        "facility_id": facility_link["facility_id"],
        "facility_name": facility_link["facility_name"],
        "state_code": facility_link["state_code"],
        "ownership_group": facility_link["ownership_group"],
        "portfolio_id": portfolio_id,
        "mapping": facility_link,
        "cache_status": "miss",
        "refresh_triggered": False,
        "fetched_at_utc": "",
        "ttl_seconds": 86400,
        "owned_securities": owned,
        "related_documents": docs,
        "issuer_security_count": len(owned),
        "issuer_document_count": len(docs),
        "scrape_status": str(scraped.get("scrape_status", "ok")),
        "scrape_error": str(scraped.get("scrape_error", "")),
    }


def _refresh_cache_async(facility_link: dict, portfolio_id: str) -> None:
    def _run() -> None:
        try:
            payload = _portfolio_linkage_payload(facility_link, portfolio_id)
            get_emma_cache().put(
                issuer_id=facility_link["emma_issuer_id"],
                portfolio_id=portfolio_id,
                payload=payload,
                scrape_status=payload.get("scrape_status", "ok"),
                scrape_error=payload.get("scrape_error", ""),
            )
        except Exception:
            return

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()


if RUNTIME.serve_frontend:
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/", response_model=None)
def home() -> FileResponse | dict:
    if not RUNTIME.serve_frontend:
        return {
            "ok": True,
            "service": "medicaid-medicare-map-api",
            "docs_hint": "/health",
        }
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/health")
def healthcheck() -> dict:
    return {"ok": True}


@app.get("/api/v1/meta/years")
def meta_years() -> dict:
    years = available_years(get_store())
    metadata = get_store().metadata
    return {"years": years, "metadata": metadata}


@app.get("/api/v1/states")
def states(
    year: int = Query(..., alias="fiscal_year"),
    metric: str = Query("medicaid_total"),
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> list[dict]:
    return get_states(
        get_store(),
        year=year,
        metric=metric,
        payer_scope=payer_scope,
        taxonomy_view=taxonomy_view,
    )


@app.get("/api/v1/states/{state_code}/summary")
def state_summary(
    state_code: str,
    year: int = Query(..., alias="fiscal_year"),
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> dict:
    summary = get_state_summary(
        get_store(),
        state_code=state_code,
        year=year,
        payer_scope=payer_scope,
        taxonomy_view=taxonomy_view,
    )
    if not summary:
        raise HTTPException(status_code=404, detail="State not found for selected fiscal year")
    return summary


@app.get("/api/v1/states/{state_code}/facilities")
def state_facilities(
    state_code: str,
    year: int = Query(..., alias="fiscal_year"),
    ownership: str = Query("all"),
    chain_name: Optional[str] = Query(None),
    sort: str = Query("medicaid_revenue"),
    descending: bool = Query(True),
    group_by_chain: bool = Query(False),
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> list[dict]:
    return get_facilities(
        get_store(),
        state_code=state_code,
        year=year,
        ownership=ownership,
        chain_name=chain_name,
        sort_field=sort,
        descending=descending,
        group_by_chain=group_by_chain,
        payer_scope=payer_scope,
        taxonomy_view=taxonomy_view,
    )


@app.get("/api/v1/facilities/{facility_id}")
def facility_detail(facility_id: str, year: int = Query(..., alias="fiscal_year")) -> dict:
    detail = get_facility(get_store(), facility_id, year)
    if not detail:
        raise HTTPException(status_code=404, detail="Facility not found")
    return detail


@app.get("/api/v1/states/{state_code}/insurers")
def state_insurers(
    state_code: str,
    year: int = Query(..., alias="fiscal_year"),
    basis: str = Query("covered_lives"),
    top_n: int = Query(25, ge=1, le=500),
) -> list[dict]:
    return get_state_insurers(
        get_store(),
        state_code=state_code,
        year=year,
        basis=basis,
        top_n=top_n,
    )


@app.get("/api/v1/facilities/{facility_id}/emma-link")
def facility_emma_link(
    facility_id: str,
    year: int = Query(..., alias="fiscal_year"),
    include_fallback: bool = Query(False),
    portfolio_id: str = Query("default"),
) -> dict:
    detail = get_facility_emma_link(get_store(), facility_id=facility_id, year=year)
    if not detail:
        raise HTTPException(status_code=404, detail="Facility not found")

    if include_fallback and (not detail.get("emma_issuer_url")):
        precomputed = get_emma_fallback_store().lookup(facility_id=facility_id, fiscal_year=year)
        if precomputed and str(precomputed.get("emma_fallback_url", "")).strip():
            detail.update(precomputed)
            detail["emma_resolved_url"] = str(precomputed.get("emma_fallback_url", ""))
            detail["emma_link_type"] = str(precomputed.get("emma_fallback_type", "issue") or "issue")
            return detail

        pstore = get_portfolio_store()
        pstore.reload()
        holdings = pstore.cusips_for_portfolio(portfolio_id)
        fallback = get_emma_client().find_emma_fallback_link(
            facility_name=str(detail.get("facility_name", "")),
            state_code=str(detail.get("state_code", "")),
            candidate_cusips=holdings,
        )
        detail.update(fallback)
        if str(detail.get("emma_fallback_url", "")).strip():
            detail["emma_resolved_url"] = str(detail["emma_fallback_url"])
            detail["emma_link_type"] = str(detail.get("emma_fallback_type", "issue") or "issue")

    return detail


@app.get("/api/v1/facilities/{facility_id}/insurer-exposure")
def facility_insurer_exposure(
    facility_id: str,
    year: int = Query(..., alias="fiscal_year"),
    basis: str = Query("covered_lives"),
    top_n: int = Query(15, ge=1, le=250),
) -> dict:
    payload = get_facility_insurer_exposure(
        get_store(),
        facility_id=facility_id,
        year=year,
        basis=basis,
        top_n=top_n,
    )
    if not payload:
        raise HTTPException(status_code=404, detail="Facility not found")
    return payload


@app.get("/api/v1/portfolio/holdings/summary")
def portfolio_holdings_summary() -> dict:
    store = get_portfolio_store()
    store.reload()
    return store.summary()


@app.get("/api/v1/facilities/{facility_id}/portfolio-linkage")
def facility_portfolio_linkage(
    facility_id: str,
    year: int = Query(..., alias="fiscal_year"),
    portfolio_id: str = Query("default"),
    force_refresh: bool = Query(False),
) -> dict:
    detail = get_facility_emma_link(get_store(), facility_id=facility_id, year=year)
    if not detail:
        raise HTTPException(status_code=404, detail="Facility not found")

    if detail["emma_mapping_status"] != "mapped" or not detail["emma_issuer_id"]:
        return _portfolio_linkage_payload(detail, portfolio_id)

    cache = get_emma_cache()
    existing = cache.get(detail["emma_issuer_id"], portfolio_id)

    if existing and existing.is_fresh and not force_refresh:
        payload = dict(existing.payload)
        payload["cache_status"] = "hit"
        payload["refresh_triggered"] = False
        payload["fetched_at_utc"] = existing.fetched_at_utc
        payload["ttl_seconds"] = 86400
        return payload

    if existing and not existing.is_fresh and not force_refresh:
        payload = dict(existing.payload)
        payload["cache_status"] = "stale"
        payload["refresh_triggered"] = True
        payload["fetched_at_utc"] = existing.fetched_at_utc
        payload["ttl_seconds"] = 86400
        _refresh_cache_async(detail, portfolio_id)
        return payload

    payload = _portfolio_linkage_payload(detail, portfolio_id)
    written = cache.put(
        issuer_id=detail["emma_issuer_id"],
        portfolio_id=portfolio_id,
        payload=payload,
        scrape_status=payload.get("scrape_status", "ok"),
        scrape_error=payload.get("scrape_error", ""),
    )
    payload["cache_status"] = "miss_refresh" if force_refresh else "miss"
    payload["refresh_triggered"] = bool(force_refresh)
    payload["fetched_at_utc"] = written.fetched_at_utc
    payload["ttl_seconds"] = 86400
    return payload


@app.get("/api/v1/chains")
def chains(
    year: int = Query(..., alias="fiscal_year"),
    state_code: Optional[str] = Query(None),
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> list[dict]:
    return get_chains(
        get_store(),
        year=year,
        state_code=state_code,
        payer_scope=payer_scope,
        taxonomy_view=taxonomy_view,
    )


@app.get("/api/v1/chains/{chain_name}")
def chain_detail(chain_name: str, year: int = Query(..., alias="fiscal_year")) -> dict:
    detail = get_chain_detail(get_store(), chain_name=chain_name, year=year)
    if not detail:
        raise HTTPException(status_code=404, detail="Chain not found")
    return detail


@app.get("/api/v1/exposures/chains/insurers")
def chain_insurer_exposure(
    chain_name: str = Query(...),
    year: int = Query(..., alias="fiscal_year"),
    basis: str = Query("covered_lives"),
    state_code: Optional[str] = Query(None),
    top_n: int = Query(20, ge=1, le=250),
) -> dict:
    payload = get_chain_insurer_exposure(
        get_store(),
        chain_name=chain_name,
        year=year,
        basis=basis,
        state_code=state_code,
        top_n=top_n,
    )
    if not payload:
        raise HTTPException(status_code=404, detail="Chain not found")
    return payload


@app.get("/api/v1/trends/national")
def national_trend(
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> list[dict]:
    return get_national_trend(get_store(), payer_scope=payer_scope, taxonomy_view=taxonomy_view)


@app.get("/api/v1/trends/states/{state_code}")
def state_trend(
    state_code: str,
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> list[dict]:
    rows = get_state_trend(get_store(), state_code, payer_scope=payer_scope, taxonomy_view=taxonomy_view)
    if not rows:
        raise HTTPException(status_code=404, detail="State not found")
    return rows


@app.get("/api/v1/trends/chains")
def chain_trend(
    chain_name: str = Query(...),
    state_code: Optional[str] = Query(None),
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> list[dict]:
    rows = get_chain_trend(
        get_store(),
        chain_name=chain_name,
        state_code=state_code,
        payer_scope=payer_scope,
        taxonomy_view=taxonomy_view,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Chain not found")
    return rows


@app.get("/api/v1/trends/facilities/{facility_id}")
def facility_trend(facility_id: str) -> list[dict]:
    rows = get_facility_trend(get_store(), facility_id=facility_id)
    if not rows:
        raise HTTPException(status_code=404, detail="Facility not found")
    return rows


@app.post("/api/v1/scenarios/run")
def scenario_run(payload: ScenarioRequest) -> dict:
    return run_scenario(
        get_store(),
        year=payload.fiscal_year,
        medicare_cut_pct=payload.medicare_cut_pct,
        federal_medicaid_cut_pct=payload.federal_medicaid_cut_pct,
        state_medicaid_cut_pct=payload.state_medicaid_cut_pct,
        private_cut_pct=payload.private_cut_pct,
        payer_scope=payload.payer_scope,
        taxonomy_view=payload.taxonomy_view,
        market_share_basis=payload.market_share_basis,
        insurer_cut_overrides=payload.insurer_cut_overrides,
        state_code=payload.state_code,
        chain_name=payload.chain_name,
    )


@app.get("/api/v1/exports/facilities.csv", response_class=PlainTextResponse)
def export_facilities_csv(
    year: int = Query(..., alias="fiscal_year"),
    state_code: Optional[str] = Query(None),
    ownership: str = Query("all"),
    chain_name: Optional[str] = Query(None),
    payer_scope: str = Query("public_only"),
    taxonomy_view: str = Query("funding_source"),
) -> PlainTextResponse:
    df = get_store().facilities.copy()
    df = df[df["fiscal_year"].astype(int) == int(year)]

    if state_code:
        df = df[df["state_code"] == state_code.upper()]
    if ownership != "all":
        df = df[df["ownership_group"] == ownership]
    if chain_name:
        df = df[df["chain_name"] == chain_name]

    if taxonomy_view == "carrier_ownership":
        if "private_carrier_administered_revenue" in df.columns:
            df["private_revenue"] = df["private_carrier_administered_revenue"]
        if "private_carrier_administered_dependency" in df.columns:
            df["private_dependency"] = df["private_carrier_administered_dependency"]

    if payer_scope == "public_only":
        for col in ["private_revenue", "private_dependency"]:
            if col in df.columns:
                df[col] = 0.0
        if "comprehensive_total" in df.columns and "public_total" in df.columns:
            df["comprehensive_total"] = df["public_total"]
        if "comprehensive_dependency" in df.columns and "public_dependency" in df.columns:
            df["comprehensive_dependency"] = df["public_dependency"]

    csv_payload = df.to_csv(index=False)
    return PlainTextResponse(
        content=csv_payload,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="facilities_{year}.csv"'},
    )
