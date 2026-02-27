from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"


@dataclass
class DataStore:
    facilities: pd.DataFrame
    state_summary: pd.DataFrame
    chain_summary: pd.DataFrame
    metadata: dict[str, Any]


def _ensure_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"Required data file not found: {path}. Run `.venv/bin/python etl/build_dataset.py` first."
        )


def _round_record(rec: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (float, np.floating)):
            out[k] = round(float(v), 6)
        elif isinstance(v, (np.integer,)):
            out[k] = int(v)
        else:
            out[k] = v
    return out


def load_data() -> DataStore:
    facilities_path = PROCESSED_DIR / "facilities.csv"
    states_path = PROCESSED_DIR / "state_summary.csv"
    chains_path = PROCESSED_DIR / "chain_summary.csv"
    metadata_path = PROCESSED_DIR / "metadata.json"

    for p in [facilities_path, states_path, chains_path, metadata_path]:
        _ensure_file(p)

    facilities = pd.read_csv(facilities_path, dtype={"facility_id": str, "state_code": str})
    states = pd.read_csv(states_path, dtype={"state_code": str})
    chains = pd.read_csv(chains_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    return DataStore(
        facilities=facilities,
        state_summary=states,
        chain_summary=chains,
        metadata=metadata,
    )


def available_years(store: DataStore) -> list[int]:
    years = sorted(store.facilities["fiscal_year"].dropna().astype(int).unique().tolist())
    return years


def select_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    return df[df["fiscal_year"].astype(int) == int(year)].copy()


def get_states(store: DataStore, year: int, metric: str) -> list[dict[str, Any]]:
    df = select_year(store.state_summary, year)

    if metric not in df.columns:
        metric = "medicaid_total"

    df = df.sort_values(metric, ascending=False)
    return [_round_record(rec) for rec in df.to_dict(orient="records")]


def get_state_summary(store: DataStore, state_code: str, year: int) -> Optional[dict[str, Any]]:
    df = select_year(store.state_summary, year)
    subset = df[df["state_code"] == state_code.upper()]
    if subset.empty:
        return None
    return _round_record(subset.iloc[0].to_dict())


def get_facilities(
    store: DataStore,
    state_code: str,
    year: int,
    ownership: str,
    chain_name: Optional[str],
    sort_field: str,
    descending: bool,
    group_by_chain: bool,
) -> list[dict[str, Any]]:
    df = select_year(store.facilities, year)
    df = df[df["state_code"] == state_code.upper()].copy()

    if ownership != "all":
        df = df[df["ownership_group"] == ownership]

    if chain_name:
        df = df[df["chain_name"] == chain_name]

    if df.empty:
        return []

    if group_by_chain:
        grouped = df.groupby(["chain_name", "fiscal_year"], as_index=False).agg(
            medicaid_revenue=("medicaid_revenue", "sum"),
            medicare_revenue=("medicare_revenue", "sum"),
            federal_medicaid_revenue=("federal_medicaid_revenue", "sum"),
            state_medicaid_revenue=("state_medicaid_revenue", "sum"),
            total_revenue=("total_revenue", "sum"),
            other_revenue=("other_revenue", "sum"),
            facility_count=("facility_id", "nunique"),
        )
        grouped["medicare_dependency"] = (grouped["medicare_revenue"] / grouped["total_revenue"]).replace([np.inf, -np.inf], 0).fillna(0)
        grouped["medicaid_dependency"] = (grouped["medicaid_revenue"] / grouped["total_revenue"]).replace([np.inf, -np.inf], 0).fillna(0)
        grouped["public_dependency"] = (
            (grouped["medicaid_revenue"] + grouped["medicare_revenue"]) / grouped["total_revenue"]
        ).replace([np.inf, -np.inf], 0).fillna(0)
        grouped["state_code"] = state_code.upper()
        grouped["facility_name"] = grouped["chain_name"]
        grouped["facility_id"] = "CHAIN::" + grouped["chain_name"]
        grouped["city"] = ""
        grouped["ownership_group"] = "mixed"
        grouped["ownership_type"] = "Mixed"
        grouped["facility_type"] = "CHAIN"
        grouped["chain_confidence"] = "aggregated"
        grouped["medicare_method"] = "aggregated"

        df = grouped

    if sort_field not in df.columns:
        sort_field = "medicaid_revenue"

    df = df.sort_values(sort_field, ascending=not descending)
    return [_round_record(rec) for rec in df.to_dict(orient="records")]


def get_facility(store: DataStore, facility_id: str, year: int) -> Optional[dict[str, Any]]:
    df = select_year(store.facilities, year)
    subset = df[df["facility_id"] == str(facility_id)]
    if subset.empty:
        return None
    return _round_record(subset.iloc[0].to_dict())


def get_chains(store: DataStore, year: int, state_code: Optional[str]) -> list[dict[str, Any]]:
    if state_code:
        df = select_year(store.facilities, year)
        df = df[df["state_code"] == state_code.upper()]
        if df.empty:
            return []
        grouped = df.groupby(["chain_name", "fiscal_year"], as_index=False).agg(
            medicaid_total=("medicaid_revenue", "sum"),
            medicare_total=("medicare_revenue", "sum"),
            federal_medicaid_total=("federal_medicaid_revenue", "sum"),
            state_medicaid_total=("state_medicaid_revenue", "sum"),
            total_revenue=("total_revenue", "sum"),
            facility_count=("facility_id", "nunique"),
            state_count=("state_code", "nunique"),
        )
        grouped["public_total"] = grouped["medicaid_total"] + grouped["medicare_total"]
        grouped["public_dependency"] = (grouped["public_total"] / grouped["total_revenue"]).replace([np.inf, -np.inf], 0).fillna(0)
        grouped = grouped.sort_values("public_total", ascending=False)
        return [_round_record(rec) for rec in grouped.to_dict(orient="records")]

    df = select_year(store.chain_summary, year).sort_values("public_total", ascending=False)
    return [_round_record(rec) for rec in df.to_dict(orient="records")]


def get_chain_detail(store: DataStore, chain_name: str, year: int) -> Optional[dict[str, Any]]:
    df = select_year(store.chain_summary, year)
    subset = df[df["chain_name"] == chain_name]
    if subset.empty:
        return None

    chain_row = _round_record(subset.iloc[0].to_dict())
    facilities = select_year(store.facilities, year)
    facilities = facilities[facilities["chain_name"] == chain_name].copy()

    state_breakdown = (
        facilities.groupby("state_code", as_index=False)
        .agg(
            medicaid_total=("medicaid_revenue", "sum"),
            medicare_total=("medicare_revenue", "sum"),
            total_revenue=("total_revenue", "sum"),
            facility_count=("facility_id", "nunique"),
        )
        .sort_values("medicaid_total", ascending=False)
    )

    chain_row["states"] = [_round_record(rec) for rec in state_breakdown.to_dict(orient="records")]
    return chain_row


def apply_scenario(df: pd.DataFrame, medicare_cut_pct: float, federal_cut_pct: float, state_cut_pct: float) -> pd.DataFrame:
    out = df.copy()
    medicare_factor = 1 - (medicare_cut_pct / 100.0)
    federal_factor = 1 - (federal_cut_pct / 100.0)
    state_factor = 1 - (state_cut_pct / 100.0)

    out["shocked_medicare_revenue"] = out["medicare_revenue"] * medicare_factor
    out["shocked_federal_medicaid_revenue"] = out["federal_medicaid_revenue"] * federal_factor
    out["shocked_state_medicaid_revenue"] = out["state_medicaid_revenue"] * state_factor
    out["shocked_medicaid_revenue"] = out["shocked_federal_medicaid_revenue"] + out["shocked_state_medicaid_revenue"]

    out["baseline_total_revenue"] = out["total_revenue"]
    out["shocked_total_revenue"] = (
        out["other_revenue"] + out["shocked_medicare_revenue"] + out["shocked_medicaid_revenue"]
    )
    out["revenue_at_risk_abs"] = out["baseline_total_revenue"] - out["shocked_total_revenue"]
    out["revenue_at_risk_pct"] = np.divide(
        out["revenue_at_risk_abs"],
        out["baseline_total_revenue"],
        out=np.zeros_like(out["revenue_at_risk_abs"], dtype=float),
        where=out["baseline_total_revenue"] > 0,
    )

    return out


def run_scenario(
    store: DataStore,
    year: int,
    medicare_cut_pct: float,
    federal_medicaid_cut_pct: float,
    state_medicaid_cut_pct: float,
    state_code: Optional[str],
    chain_name: Optional[str],
) -> dict[str, Any]:
    df = select_year(store.facilities, year)

    if state_code:
        df = df[df["state_code"] == state_code.upper()]
    if chain_name:
        df = df[df["chain_name"] == chain_name]

    if df.empty:
        return {
            "fiscal_year": year,
            "scope_state_code": state_code,
            "scope_chain_name": chain_name,
            "baseline_total_revenue": 0,
            "shocked_total_revenue": 0,
            "revenue_at_risk_abs": 0,
            "revenue_at_risk_pct": 0,
            "top_impacted_facilities": [],
        }

    sim = apply_scenario(df, medicare_cut_pct, federal_medicaid_cut_pct, state_medicaid_cut_pct)

    baseline_total = float(sim["baseline_total_revenue"].sum())
    shocked_total = float(sim["shocked_total_revenue"].sum())
    risk_abs = baseline_total - shocked_total
    risk_pct = (risk_abs / baseline_total) if baseline_total > 0 else 0

    top = (
        sim[["facility_id", "facility_name", "state_code", "baseline_total_revenue", "shocked_total_revenue", "revenue_at_risk_abs", "revenue_at_risk_pct"]]
        .sort_values("revenue_at_risk_abs", ascending=False)
        .head(25)
    )

    return {
        "fiscal_year": int(year),
        "scope_state_code": state_code.upper() if state_code else None,
        "scope_chain_name": chain_name,
        "baseline_total_revenue": round(baseline_total, 2),
        "shocked_total_revenue": round(shocked_total, 2),
        "revenue_at_risk_abs": round(risk_abs, 2),
        "revenue_at_risk_pct": round(risk_pct, 6),
        "top_impacted_facilities": [_round_record(rec) for rec in top.to_dict(orient="records")],
    }
