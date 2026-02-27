#!/usr/bin/env python3
"""Build prototype dataset for the Medicaid/Medicare hospital dashboard.

Data sources:
- CMS Hospital Cost Reports (data.cms.gov API)
- Federal Register FMAP Table (FY 2026)
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
MANUAL_DIR = ROOT / "data" / "manual"

CMS_DATASET_ID = "44060663-47d8-4ced-a115-b53b4c270acb"
CMS_API_URL = "https://data.cms.gov/data-api/v1/dataset/{dataset}/data?offset={offset}&size={size}"
FMAP_TABLE_URL = "https://www.federalregister.gov/documents/full_text/html/2024/11/29/2024-27910.html"
FMAP_EFFECTIVE_YEAR = 2026

PAGE_SIZE = 5000
MAX_RETRIES = 4

STATE_ABBR = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

VALID_STATE_CODES = set(STATE_ABBR.values())

# Type of Control mapping based on CMS provider control code families.
TYPE_OF_CONTROL_MAP = {
    "1": ("Government Federal", "government"),
    "2": ("Government State", "government"),
    "3": ("Government County", "government"),
    "4": ("Government City", "government"),
    "5": ("Nonprofit Corporation", "not_for_profit"),
    "6": ("Nonprofit Church", "not_for_profit"),
    "7": ("Nonprofit Other", "not_for_profit"),
    "8": ("Individual", "for_profit"),
    "9": ("For-profit Corporation", "for_profit"),
    "10": ("Partnership", "for_profit"),
    "11": ("Other Proprietary", "for_profit"),
    "12": ("Religious Affiliation", "not_for_profit"),
    "13": ("Tribal", "government"),
}

CHAIN_KEYWORDS = [
    ("HCA", "HCA Healthcare"),
    ("TENET", "Tenet Healthcare"),
    ("COMMUNITY HEALTH SYSTEM", "Community Health Systems"),
    ("CHS ", "Community Health Systems"),
    ("LIFEPOINT", "LifePoint Health"),
    ("UNIVERSAL HEALTH", "Universal Health Services"),
    ("ASCENSION", "Ascension"),
    ("COMMONSPIRIT", "CommonSpirit Health"),
    ("DIGNITY HEALTH", "CommonSpirit Health"),
    ("TRINITY HEALTH", "Trinity Health"),
    ("PROVIDENCE", "Providence"),
    ("ADVENTHEALTH", "AdventHealth"),
    ("ADVENTIST", "Adventist Health"),
    ("SUTTER", "Sutter Health"),
    ("KAISER", "Kaiser Permanente"),
    ("MAYO", "Mayo Clinic"),
    ("CLEVELAND CLINIC", "Cleveland Clinic"),
    ("UPMC", "UPMC"),
    ("BAYLOR SCOTT", "Baylor Scott & White Health"),
    ("BON SECOURS", "Bon Secours Mercy Health"),
    ("MERCY", "Mercy"),
    ("MOUNT SINAI", "Mount Sinai Health System"),
    ("NYU LANGONE", "NYU Langone Health"),
    ("INTERMOUNTAIN", "Intermountain Health"),
    ("BANNER", "Banner Health"),
]


@dataclass
class SourceStats:
    total_rows: int
    years: Dict[int, int]
    target_year: int


def fetch_json(url: str) -> List[dict]:
    """Fetch JSON with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                return json.loads(resp.read())
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)
    return []


def fetch_cms_hospital_data() -> pd.DataFrame:
    rows: List[dict] = []
    offset = 0

    while True:
        url = CMS_API_URL.format(dataset=CMS_DATASET_ID, offset=offset, size=PAGE_SIZE)
        page = fetch_json(url)
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not rows:
        raise RuntimeError("No CMS rows downloaded.")

    raw_path = RAW_DIR / "cms_hospital_cost_reports.json"
    raw_path.write_text(json.dumps(rows), encoding="utf-8")

    return pd.DataFrame(rows)


def choose_latest_full_year(df: pd.DataFrame) -> SourceStats:
    fy_end = pd.to_datetime(df["Fiscal Year End Date"], errors="coerce")
    years = fy_end.dt.year.dropna().astype(int)
    counts = years.value_counts().sort_index()
    if counts.empty:
        raise RuntimeError("Could not infer any fiscal year from CMS data.")

    max_count = counts.max()
    threshold = max_count * 0.8
    eligible = counts[counts >= threshold]
    target_year = int(eligible.index.max())

    return SourceStats(
        total_rows=len(df),
        years={int(k): int(v) for k, v in counts.to_dict().items()},
        target_year=target_year,
    )


def clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)


def fetch_fmap_table() -> pd.DataFrame:
    tables = pd.read_html(FMAP_TABLE_URL)
    if not tables:
        raise RuntimeError("Unable to parse FMAP table from Federal Register.")

    fmap = tables[0].copy()
    fmap.columns = [str(c).strip() for c in fmap.columns]

    state_col = next(col for col in fmap.columns if col.lower().startswith("state"))
    fmap_col = next(col for col in fmap.columns if "Federal" in col and "Enhanced" not in col)

    fmap = fmap[[state_col, fmap_col]].rename(columns={state_col: "state_name", fmap_col: "fmap_percent"})
    fmap["state_name"] = fmap["state_name"].astype(str).str.replace(r"\*", "", regex=True).str.strip()
    fmap = fmap[fmap["state_name"].isin(STATE_ABBR.keys())].copy()
    fmap["state_code"] = fmap["state_name"].map(STATE_ABBR)
    fmap["fmap_percent"] = pd.to_numeric(fmap["fmap_percent"], errors="coerce")
    fmap["federal_share"] = fmap["fmap_percent"] / 100.0
    fmap["fiscal_year"] = FMAP_EFFECTIVE_YEAR

    raw_path = RAW_DIR / "fmap_table_fy2026.csv"
    fmap.to_csv(raw_path, index=False)
    return fmap


def ownership_fields(type_of_control: str) -> tuple[str, str]:
    code = str(type_of_control).strip()
    if code in TYPE_OF_CONTROL_MAP:
        return TYPE_OF_CONTROL_MAP[code]
    return ("Unknown", "unknown")


def infer_chain(hospital_name: str) -> tuple[str, str]:
    if not isinstance(hospital_name, str) or not hospital_name.strip():
        return ("Unmapped / Independent", "low")

    upper_name = f" {hospital_name.upper()} "
    for keyword, chain in CHAIN_KEYWORDS:
        if keyword in upper_name:
            return (chain, "keyword")

    return ("Unmapped / Independent", "low")


def apply_chain_overrides(df: pd.DataFrame) -> pd.DataFrame:
    override_path = MANUAL_DIR / "chain_overrides.csv"
    if not override_path.exists():
        template = pd.DataFrame(columns=["provider_ccn", "chain_name", "notes"])
        template.to_csv(override_path, index=False)
        return df

    overrides = pd.read_csv(override_path, dtype=str)
    if overrides.empty or "provider_ccn" not in overrides.columns or "chain_name" not in overrides.columns:
        return df

    overrides = overrides.dropna(subset=["provider_ccn", "chain_name"]).copy()
    override_map = dict(zip(overrides["provider_ccn"].astype(str).str.zfill(6), overrides["chain_name"].str.strip()))

    mask = df["facility_id"].isin(override_map.keys())
    df.loc[mask, "chain_name"] = df.loc[mask, "facility_id"].map(override_map)
    df.loc[mask, "chain_confidence"] = "manual_override"
    return df


def build_facility_table(cms_df: pd.DataFrame, stats: SourceStats, fmap_df: pd.DataFrame) -> pd.DataFrame:
    df = cms_df.copy()
    df["fy_end_date"] = pd.to_datetime(df["Fiscal Year End Date"], errors="coerce")
    df = df[df["fy_end_date"].dt.year == stats.target_year].copy()

    # Numeric fields used in financial decomposition.
    numeric_cols = [
        "Net Patient Revenue",
        "Total Patient Revenue",
        "Total Income",
        "Net Revenue from Medicaid",
        "DRG Amounts Other Than Outlier Payments",
        "Outlier Payments For Discharges",
        "Disproportionate Share Adjustment",
        "Managed Care Simulated Payments",
        "Total IME Payment",
        "Hospital Total Days Title XVIII For Adults & Peds",
        "Hospital Total Days Title XIX For Adults & Peds",
        "Hospital Total Days (V + XVIII + XIX + Unknown) For Adults & Peds",
        "Total Days Title XVIII",
        "Total Days Title XIX",
        "Total Days (V + XVIII + XIX + Unknown)",
    ]

    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = clean_numeric(df[col])

    # Resolve one record per provider CCN (highest net patient revenue preferred).
    df["facility_id"] = df["Provider CCN"].astype(str).str.zfill(6)
    df = df.sort_values(["facility_id", "fy_end_date", "Net Patient Revenue"], ascending=[True, False, False])
    df = df.drop_duplicates(subset=["facility_id"], keep="first").copy()

    net_patient = df["Net Patient Revenue"]
    total_patient = df["Total Patient Revenue"]
    total_income = df["Total Income"]

    total_revenue = np.where(net_patient > 0, net_patient, np.where(total_patient > 0, total_patient, total_income))
    total_revenue = pd.Series(total_revenue, index=df.index).clip(lower=0)

    medicaid_revenue = df["Net Revenue from Medicaid"].clip(lower=0)
    medicaid_revenue = np.minimum(medicaid_revenue, total_revenue)

    medicare_anchor = (
        df["DRG Amounts Other Than Outlier Payments"]
        + df["Outlier Payments For Discharges"]
        + df["Disproportionate Share Adjustment"]
        + df["Managed Care Simulated Payments"]
        + df["Total IME Payment"]
    ).clip(lower=0)

    total_days = df["Hospital Total Days (V + XVIII + XIX + Unknown) For Adults & Peds"]
    total_days = np.where(total_days > 0, total_days, df["Total Days (V + XVIII + XIX + Unknown)"])
    medicare_days = df["Hospital Total Days Title XVIII For Adults & Peds"]
    medicare_days = np.where(medicare_days > 0, medicare_days, df["Total Days Title XVIII"])
    medicaid_days = df["Hospital Total Days Title XIX For Adults & Peds"]
    medicaid_days = np.where(medicaid_days > 0, medicaid_days, df["Total Days Title XIX"])

    day_share = np.divide(medicare_days, total_days, out=np.zeros_like(medicare_days, dtype=float), where=np.array(total_days) > 0)
    day_share = np.clip(day_share, 0, 1)
    medicare_from_days = total_revenue * day_share

    medicare_est = np.maximum(medicare_anchor, medicare_from_days)
    max_medicare = np.maximum(total_revenue - medicaid_revenue, 0)
    medicare_revenue = np.minimum(medicare_est, max_medicare)

    medicare_method = np.where(
        medicare_anchor > 0,
        "anchor_plus_days_share",
        np.where(day_share > 0, "days_share", "residual_after_medicaid"),
    )

    residual_needed = np.where((medicare_revenue == 0) & (total_revenue > medicaid_revenue), total_revenue - medicaid_revenue, medicare_revenue)
    medicare_revenue = np.where((medicare_revenue == 0) & (total_revenue > medicaid_revenue), residual_needed * 0.5, medicare_revenue)
    medicare_method = np.where((medicare_anchor <= 0) & (day_share <= 0), "residual_50pct", medicare_method)

    other_revenue = np.maximum(total_revenue - medicaid_revenue - medicare_revenue, 0)

    ownership_desc, ownership_group = zip(*df["Type of Control"].map(ownership_fields))

    chain_name, chain_confidence = zip(*df["Hospital Name"].map(infer_chain))

    facilities = pd.DataFrame(
        {
            "facility_id": df["facility_id"],
            "facility_name": df["Hospital Name"].fillna("Unknown Facility"),
            "street_address": df["Street Address"].fillna(""),
            "city": df["City"].fillna(""),
            "state_code": df["State Code"].fillna(""),
            "zip_code": df["Zip Code"].fillna(""),
            "county": df["County"].fillna(""),
            "facility_type": df["CCN Facility Type"].fillna(""),
            "provider_type": df["Provider Type"].fillna(""),
            "ownership_type": list(ownership_desc),
            "ownership_group": list(ownership_group),
            "fiscal_year": stats.target_year,
            "source_fy_end_date": df["Fiscal Year End Date"],
            "total_revenue": total_revenue.round(2),
            "medicare_revenue": pd.Series(medicare_revenue, index=df.index).round(2),
            "medicaid_revenue": pd.Series(medicaid_revenue, index=df.index).round(2),
            "other_revenue": pd.Series(other_revenue, index=df.index).round(2),
            "medicare_method": medicare_method,
            "total_days": pd.Series(total_days, index=df.index).round(2),
            "medicare_days": pd.Series(medicare_days, index=df.index).round(2),
            "medicaid_days": pd.Series(medicaid_days, index=df.index).round(2),
            "chain_name": list(chain_name),
            "chain_confidence": list(chain_confidence),
        }
    )

    facilities = facilities[facilities["state_code"].isin(VALID_STATE_CODES)].copy()
    facilities = facilities[facilities["total_revenue"] > 0].copy()

    facilities = apply_chain_overrides(facilities)

    fmap_map = fmap_df.set_index("state_code")["federal_share"].to_dict()
    facilities["federal_medicaid_share"] = facilities["state_code"].map(fmap_map).fillna(0.5)
    facilities["federal_medicaid_revenue"] = (facilities["medicaid_revenue"] * facilities["federal_medicaid_share"]).round(2)
    facilities["state_medicaid_revenue"] = (facilities["medicaid_revenue"] - facilities["federal_medicaid_revenue"]).round(2)

    facilities["medicare_dependency"] = (facilities["medicare_revenue"] / facilities["total_revenue"]).replace([np.inf, -np.inf], 0).fillna(0)
    facilities["medicaid_dependency"] = (facilities["medicaid_revenue"] / facilities["total_revenue"]).replace([np.inf, -np.inf], 0).fillna(0)
    facilities["public_dependency"] = (
        (facilities["medicare_revenue"] + facilities["medicaid_revenue"]) / facilities["total_revenue"]
    ).replace([np.inf, -np.inf], 0).fillna(0)

    for col in ["medicare_dependency", "medicaid_dependency", "public_dependency", "federal_medicaid_share"]:
        facilities[col] = facilities[col].clip(lower=0, upper=1)

    return facilities


def build_state_summary(facilities: pd.DataFrame) -> pd.DataFrame:
    grouped = facilities.groupby(["fiscal_year", "state_code"], as_index=False).agg(
        medicaid_total=("medicaid_revenue", "sum"),
        medicare_total=("medicare_revenue", "sum"),
        federal_medicaid_total=("federal_medicaid_revenue", "sum"),
        state_medicaid_total=("state_medicaid_revenue", "sum"),
        total_revenue=("total_revenue", "sum"),
        facility_count=("facility_id", "nunique"),
        chain_count=("chain_name", "nunique"),
    )

    grouped["public_total"] = grouped["medicaid_total"] + grouped["medicare_total"]
    grouped["public_dependency"] = grouped["public_total"] / grouped["total_revenue"]
    grouped["public_dependency"] = grouped["public_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped = grouped.sort_values("state_code")
    return grouped


def build_chain_summary(facilities: pd.DataFrame) -> pd.DataFrame:
    grouped = facilities.groupby(["fiscal_year", "chain_name"], as_index=False).agg(
        medicaid_total=("medicaid_revenue", "sum"),
        medicare_total=("medicare_revenue", "sum"),
        federal_medicaid_total=("federal_medicaid_revenue", "sum"),
        state_medicaid_total=("state_medicaid_revenue", "sum"),
        total_revenue=("total_revenue", "sum"),
        facility_count=("facility_id", "nunique"),
        state_count=("state_code", "nunique"),
    )
    grouped["public_total"] = grouped["medicaid_total"] + grouped["medicare_total"]
    grouped["public_dependency"] = grouped["public_total"] / grouped["total_revenue"]
    grouped["public_dependency"] = grouped["public_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped = grouped.sort_values("public_total", ascending=False)
    return grouped


def persist_outputs(facilities: pd.DataFrame, states: pd.DataFrame, chains: pd.DataFrame, stats: SourceStats) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    facilities.to_csv(PROCESSED_DIR / "facilities.csv", index=False)
    states.to_csv(PROCESSED_DIR / "state_summary.csv", index=False)
    chains.to_csv(PROCESSED_DIR / "chain_summary.csv", index=False)

    db_path = PROCESSED_DIR / "app.db"
    if db_path.exists():
        db_path.unlink()

    with sqlite3.connect(db_path) as conn:
        facilities.to_sql("facilities", conn, index=False)
        states.to_sql("state_summary", conn, index=False)
        chains.to_sql("chain_summary", conn, index=False)

    metadata = {
        "source_stats": {
            "cms_total_rows": stats.total_rows,
            "cms_year_counts": stats.years,
            "selected_fiscal_year": stats.target_year,
        },
        "methodology": {
            "medicaid": "Net Revenue from Medicaid from CMS hospital cost reports",
            "medicare": (
                "Estimated from Medicare IPPS payment components plus Medicare inpatient day-share proxy, "
                "capped by total net patient revenue"
            ),
            "federal_state_split": "State-level FMAP percentages from Federal Register FY 2026 table",
        },
        "sources": [
            {
                "name": "CMS Hospital Cost Reports",
                "url": f"https://data.cms.gov/dataset/{CMS_DATASET_ID}",
            },
            {
                "name": "Federal Register FMAP FY2026",
                "url": FMAP_TABLE_URL,
            },
        ],
        "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
    }

    (PROCESSED_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)

    print("Downloading CMS hospital cost reports...")
    cms_df = fetch_cms_hospital_data()

    stats = choose_latest_full_year(cms_df)
    print(f"Downloaded {stats.total_rows} rows; selected FY {stats.target_year}")

    print("Downloading/parsing FMAP table...")
    fmap_df = fetch_fmap_table()

    print("Building facility-level analytics table...")
    facilities = build_facility_table(cms_df, stats, fmap_df)
    states = build_state_summary(facilities)
    chains = build_chain_summary(facilities)

    print(f"Facilities in output: {len(facilities)}")
    print(f"States in output: {states['state_code'].nunique()}")

    persist_outputs(facilities, states, chains, stats)
    print(f"Wrote processed data to {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
