from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from app.data_store import (
    available_years,
    get_chain_detail,
    get_chains,
    get_facilities,
    get_facility,
    get_state_summary,
    get_states,
    load_data,
    run_scenario,
)
from app.schemas import ScenarioRequest

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = load_data()
    yield


app = FastAPI(
    title="Medicaid-Medicare Hospital Exposure Dashboard API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_store():
    if not hasattr(app.state, "store"):
        app.state.store = load_data()
    return app.state.store


app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/", response_class=FileResponse)
def home() -> FileResponse:
    return FileResponse(str(FRONTEND_DIR / "index.html"))


@app.get("/healthz")
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
) -> list[dict]:
    return get_states(get_store(), year, metric)


@app.get("/api/v1/states/{state_code}/summary")
def state_summary(state_code: str, year: int = Query(..., alias="fiscal_year")) -> dict:
    summary = get_state_summary(get_store(), state_code, year)
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
    )


@app.get("/api/v1/facilities/{facility_id}")
def facility_detail(facility_id: str, year: int = Query(..., alias="fiscal_year")) -> dict:
    detail = get_facility(get_store(), facility_id, year)
    if not detail:
        raise HTTPException(status_code=404, detail="Facility not found")
    return detail


@app.get("/api/v1/chains")
def chains(
    year: int = Query(..., alias="fiscal_year"),
    state_code: Optional[str] = Query(None),
) -> list[dict]:
    return get_chains(get_store(), year=year, state_code=state_code)


@app.get("/api/v1/chains/{chain_name}")
def chain_detail(chain_name: str, year: int = Query(..., alias="fiscal_year")) -> dict:
    detail = get_chain_detail(get_store(), chain_name=chain_name, year=year)
    if not detail:
        raise HTTPException(status_code=404, detail="Chain not found")
    return detail


@app.post("/api/v1/scenarios/run")
def scenario_run(payload: ScenarioRequest) -> dict:
    return run_scenario(
        get_store(),
        year=payload.fiscal_year,
        medicare_cut_pct=payload.medicare_cut_pct,
        federal_medicaid_cut_pct=payload.federal_medicaid_cut_pct,
        state_medicaid_cut_pct=payload.state_medicaid_cut_pct,
        state_code=payload.state_code,
        chain_name=payload.chain_name,
    )


@app.get("/api/v1/exports/facilities.csv", response_class=PlainTextResponse)
def export_facilities_csv(
    year: int = Query(..., alias="fiscal_year"),
    state_code: Optional[str] = Query(None),
    ownership: str = Query("all"),
    chain_name: Optional[str] = Query(None),
) -> PlainTextResponse:
    df = get_store().facilities.copy()
    df = df[df["fiscal_year"].astype(int) == int(year)]

    if state_code:
        df = df[df["state_code"] == state_code.upper()]
    if ownership != "all":
        df = df[df["ownership_group"] == ownership]
    if chain_name:
        df = df[df["chain_name"] == chain_name]

    csv_payload = df.to_csv(index=False)
    return PlainTextResponse(
        content=csv_payload,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="facilities_{year}.csv"'},
    )
