#!/usr/bin/env python3
"""Build prototype dataset for the Medicaid/Medicare hospital dashboard.

Data sources:
- CMS Hospital Cost Reports annual final CSV releases (data.cms.gov / catalog.data.gov)
- Federal Register FMAP Table (FY 2026)
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
import io
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
MANUAL_DIR = ROOT / "data" / "manual"

CMS_DATASET_ID = "44060663-47d8-4ced-a115-b53b4c270acb"
CMS_CATALOG_API_URL = "https://catalog.data.gov/api/3/action/package_show?id=hospital-provider-cost-report"
CMS_DATA_API_URL_TEMPLATE = "https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data?size={size}&offset={offset}"
CMS_HOSPITAL_ENROLLMENTS_DATASET_ID = "f6f6505c-e8b0-4d57-b258-e2b94133aaf2"
CMS_HOSPITAL_ALL_OWNERS_DATASET_ID = "029c119f-f79c-49be-9100-344d31d10344"
CMS_DATA_API_PAGE_SIZE = 5000
FMAP_TABLE_URL = "https://www.federalregister.gov/documents/full_text/html/2024/11/29/2024-27910.html"
FMAP_EFFECTIVE_YEAR = 2026
CMS_HISTORY_YEARS = 10
MLR_ZIP_URL_TEMPLATE = "https://www.cms.gov/files/zip/mlr-public-use-file-{year}.zip"
MLR_AVAILABLE_START_YEAR = 2019

MAX_RETRIES = 4
MIN_OWNER_SYSTEM_SIZE = 2

EMMA_MAPPING_COLUMNS = [
    "facility_id",
    "facility_name",
    "state_code",
    "emma_issuer_id",
    "emma_issuer_name",
    "emma_issuer_url",
    "emma_mapping_status",
    "emma_mapping_method",
    "emma_match_score",
    "reviewed_by",
    "reviewed_at_utc",
    "notes",
]

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
    ("ENCOMPASS", "Encompass Health"),
    ("KINDRED", "Kindred / ScionHealth"),
    ("SELECT SPECIALTY", "Select Medical"),
    ("SSH ", "Select Medical"),
    ("KFH ", "Kaiser Permanente"),
    ("OCHSNER", "Ochsner Health"),
    ("SENTARA", "Sentara Health"),
    ("PIEDMONT", "Piedmont Healthcare"),
    ("ATRIUM", "Atrium Health"),
    ("COREWELL", "Corewell Health"),
    ("SANFORD", "Sanford Health"),
    ("AVERA", "Avera Health"),
    ("MCLAREN", "McLaren Health Care"),
    ("ASPIRUS", "Aspirus Health"),
    ("THEDACARE", "ThedaCare"),
    ("INTEGRIS", "INTEGRIS Health"),
    ("GEISINGER", "Geisinger"),
    ("SSM HEALTH", "SSM Health"),
    ("MEDICAL CITY", "HCA Healthcare"),
    ("TEXAS HEALTH", "Texas Health Resources"),
    ("MEMORIAL HERMANN", "Memorial Hermann"),
    ("UT HEALTH", "UT Health"),
    ("IU HEALTH", "IU Health"),
    ("FRANCISCAN HEALTH", "Franciscan Health"),
    ("MUSC HEALTH", "MUSC Health"),
    ("NORTHSIDE HOSPITAL", "Northside Hospital"),
    ("KETTERING HEALTH", "Kettering Health"),
    ("MONUMENT HEALTH", "Monument Health"),
    ("LOGAN HEALTH", "Logan Health"),
    ("MARSHFIELD", "Marshfield Clinic Health System"),
    ("MYMICHIGAN", "MyMichigan Health"),
    ("BROWARD HEALTH", "Broward Health"),
    ("HENRY FORD", "Henry Ford Health"),
    ("MOUNT CARMEL", "Mount Carmel Health System"),
    ("ROBERT WOOD JOHNSON", "RWJBarnabas Health"),
    ("REGENCY HOSPITAL", "Select Medical"),
    ("POST ACUTE MEDICAL", "Post Acute Medical"),
    ("LANDMARK HOSPITAL", "Landmark Hospitals"),
    ("OCEANS BEHAVIORAL", "Oceans Healthcare"),
    ("HAVEN BEHAVIORAL", "Haven Behavioral Healthcare"),
    ("REUNION REHABILITATION", "Reunion Rehabilitation Hospitals"),
    ("CORNERSTONE SPECIALTY", "Cornerstone Specialty Hospitals"),
    ("KPC PROMISE", "KPC Health"),
]

OWNER_ROLE_PRIORITY = {
    "5% OR GREATER DIRECT OWNERSHIP INTEREST": 5.0,
    "5% OR GREATER INDIRECT OWNERSHIP INTEREST": 4.0,
    "OPERATIONAL/MANAGERIAL CONTROL": 3.0,
    "LIMITED PARTNERSHIP INTEREST": 2.5,
    "GENERAL PARTNERSHIP INTEREST": 2.5,
    "OTHER": 1.0,
}

OWNER_CHAIN_PATTERNS = [
    ("HCA", "HCA Healthcare"),
    ("HEALTHSERV ACQUISITION", "HCA Healthcare"),
    ("HTI HOSPITAL HOLDINGS", "HCA Healthcare"),
    ("HOSPITAL CORP LLC", "HCA Healthcare"),
    ("COMMONSPIRIT", "CommonSpirit Health"),
    ("DIGNITY HEALTH", "CommonSpirit Health"),
    ("CHI ", "CommonSpirit Health"),
    ("TRINITY HEALTH", "Trinity Health"),
    ("ASCENSION", "Ascension"),
    ("PROVIDENCE", "Providence"),
    ("ADVENTIST HEALTH SYSTEM SUNBELT", "AdventHealth"),
    ("ADVENTHEALTH", "AdventHealth"),
    ("ADVENTIST HEALTH SYSTEM/WEST", "Adventist Health"),
    ("ADVENTIST HEALTH", "Adventist Health"),
    ("ENCOMPASS", "Encompass Health"),
    ("KINDRED", "Kindred / ScionHealth"),
    ("SCIONHEALTH", "Kindred / ScionHealth"),
    ("SELECT MEDICAL", "Select Medical"),
    ("SELECT SPECIALTY", "Select Medical"),
    ("SSH ", "Select Medical"),
    ("HOSPITAL HOLDINGS CORPORATION", "Select Medical"),
    ("UNIVERSAL HEALTH SERVICES", "Universal Health Services"),
    ("UHS OF DELAWARE", "Universal Health Services"),
    ("LIFEPOINT", "LifePoint Health"),
    ("PRIME HEALTHCARE", "Prime Healthcare"),
    ("KAISER FOUNDATION", "Kaiser Permanente"),
    ("UPMC", "UPMC"),
    ("BANNER HEALTH", "Banner Health"),
    ("IHC HEALTH SERVICES", "Intermountain Health"),
    ("INTERMOUNTAIN", "Intermountain Health"),
    ("TEXAS HEALTH RESOURCES", "Texas Health Resources"),
    ("MEMORIAL HERMANN", "Memorial Hermann"),
    ("MERCY HEALTH NETWORK", "Mercy"),
    ("MERCY HEALTH", "Mercy"),
    ("BON SECOURS MERCY", "Bon Secours Mercy Health"),
    ("CHRISTUS", "CHRISTUS Health"),
    ("NORTHWELL", "Northwell Health"),
    ("SANFORD", "Sanford Health"),
    ("AVERA", "Avera Health"),
    ("MCLAREN", "McLaren Health Care"),
    ("OSF HEALTHCARE", "OSF HealthCare"),
    ("GEISINGER", "Geisinger"),
    ("ATRIUM", "Atrium Health"),
    ("PIEDMONT", "Piedmont Healthcare"),
    ("SENTARA", "Sentara Health"),
    ("SUTTER BAY HOSPITALS", "Sutter Health"),
    ("RWJBARNABAS", "RWJBarnabas Health"),
    ("BJC HEALTH", "BJC HealthCare"),
    ("SSM HEALTH", "SSM Health"),
    ("MAYO CLINIC", "Mayo Clinic"),
    ("OHIOHEALTH", "OhioHealth"),
    ("CLEVELAND CLINIC", "Cleveland Clinic"),
    ("MUSC", "MUSC Health"),
    ("KETTERING", "Kettering Health"),
    ("BROWARD HEALTH", "Broward Health"),
    ("HENRY FORD", "Henry Ford Health"),
    ("OCHSNER", "Ochsner Health"),
    ("ADVOCATE", "Advocate Health"),
    ("UNIVERSITY HOSPITALS HEALTH SYSTEM", "University Hospitals"),
    ("OCEANS ACQUISITION", "Oceans Healthcare"),
    ("SPRINGSTONE HEALTH", "Springstone"),
    ("ERNEST HEALTH", "Ernest Health"),
    ("BAPTIST HEALTH", "Baptist Health"),
    ("BAPTIST MEMORIAL", "Baptist Memorial Health Care"),
]


@dataclass
class SourceStats:
    total_rows: int
    release_year_counts: Dict[int, int]
    fiscal_year_end_counts: Dict[int, int]
    selected_fiscal_year: int
    release_years: List[int]


def fetch_json(url: str) -> Any:
    """Fetch JSON with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                return json.loads(resp.read())
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)
    return {}


def fetch_csv(url: str) -> pd.DataFrame:
    for attempt in range(MAX_RETRIES):
        try:
            return pd.read_csv(url, dtype=str, low_memory=False)
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)
    return pd.DataFrame()


def list_cms_release_csvs() -> List[tuple[int, str]]:
    payload = fetch_json(CMS_CATALOG_API_URL)
    resources = payload.get("result", {}).get("resources", [])
    out: List[tuple[int, str]] = []

    for res in resources:
        if str(res.get("format", "")).upper() != "CSV":
            continue
        url = str(res.get("url", ""))
        match = re.search(r"CostReport_(\d{4})_Final\.csv", url)
        if not match:
            continue
        out.append((int(match.group(1)), url))

    out.sort(key=lambda x: x[0], reverse=True)
    return out


def fetch_cms_hospital_data(history_years: int = CMS_HISTORY_YEARS) -> tuple[pd.DataFrame, SourceStats]:
    releases = list_cms_release_csvs()[:history_years]
    if not releases:
        raise RuntimeError("Unable to discover CMS historical release CSVs.")

    rows: List[dict] = []
    latest_release_rows: List[dict] = []
    release_year_counts: Dict[int, int] = {}
    release_sources: List[dict[str, Any]] = []

    for release_year, url in releases:
        df = fetch_csv(url)
        df["source_release_year"] = release_year
        recs = df.to_dict(orient="records")
        rows.extend(recs)
        if not latest_release_rows:
            latest_release_rows = recs
        release_year_counts[release_year] = len(df)
        release_sources.append({"release_year": release_year, "url": url, "rows": len(df)})

    if not rows:
        raise RuntimeError("No CMS rows downloaded.")

    raw_path = RAW_DIR / "cms_hospital_cost_reports.json"
    raw_path.write_text(json.dumps(latest_release_rows), encoding="utf-8")
    manifest_path = RAW_DIR / "cms_hospital_cost_reports_manifest.json"
    manifest_path.write_text(json.dumps({"releases": release_sources}, indent=2), encoding="utf-8")

    combined = pd.DataFrame(rows)
    fy_end = pd.to_datetime(combined["Fiscal Year End Date"], errors="coerce")
    years = fy_end.dt.year.dropna().astype(int)
    counts = years.value_counts().sort_index()
    if counts.empty:
        raise RuntimeError("Could not infer any fiscal year from CMS data.")

    max_count = counts.max()
    threshold = max_count * 0.8
    eligible = counts[counts >= threshold]
    selected_year = int(eligible.index.max())

    stats = SourceStats(
        total_rows=len(combined),
        release_year_counts=release_year_counts,
        fiscal_year_end_counts={int(k): int(v) for k, v in counts.to_dict().items()},
        selected_fiscal_year=selected_year,
        release_years=sorted(release_year_counts.keys()),
    )
    return combined, stats


def fetch_paginated_data_api_dataset(dataset_id: str, page_size: int = CMS_DATA_API_PAGE_SIZE) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        url = CMS_DATA_API_URL_TEMPLATE.format(dataset_id=dataset_id, size=page_size, offset=offset)
        payload = fetch_json(url)
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected payload for dataset {dataset_id}: expected list, got {type(payload)}")

        rows.extend(payload)
        if len(payload) < page_size:
            break
        offset += page_size

    return pd.DataFrame(rows)


def fetch_hospital_ownership_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    enrollments = fetch_paginated_data_api_dataset(CMS_HOSPITAL_ENROLLMENTS_DATASET_ID)
    owners = fetch_paginated_data_api_dataset(CMS_HOSPITAL_ALL_OWNERS_DATASET_ID)
    return enrollments, owners


def canonical_owner_chain_name(owner_name: str) -> str:
    if not isinstance(owner_name, str) or not owner_name.strip():
        return "Unmapped / Independent"

    normalized = owner_name.upper().strip()
    for keyword, chain in OWNER_CHAIN_PATTERNS:
        if keyword in normalized:
            return chain
    return owner_name.strip()


def build_ownership_chain_map(enrollments: pd.DataFrame, owners: pd.DataFrame) -> pd.DataFrame:
    if enrollments.empty or owners.empty:
        return pd.DataFrame(columns=["facility_id", "chain_name", "chain_confidence"])

    required_enrollment_cols = {"CCN", "ASSOCIATE ID"}
    required_owner_cols = {"ASSOCIATE ID", "ORGANIZATION NAME - OWNER", "TYPE - OWNER", "ROLE TEXT - OWNER"}
    if not required_enrollment_cols.issubset(set(enrollments.columns)):
        return pd.DataFrame(columns=["facility_id", "chain_name", "chain_confidence"])
    if not required_owner_cols.issubset(set(owners.columns)):
        return pd.DataFrame(columns=["facility_id", "chain_name", "chain_confidence"])

    ccn_map = enrollments[["CCN", "ASSOCIATE ID"]].dropna(subset=["CCN", "ASSOCIATE ID"]).copy()
    ccn_map["CCN"] = ccn_map["CCN"].astype(str).str.zfill(6)
    ccn_map["ASSOCIATE ID"] = ccn_map["ASSOCIATE ID"].astype(str)
    ccn_map = ccn_map.drop_duplicates(subset=["CCN"])

    org_owners = owners.copy()
    org_owners = org_owners[(org_owners["TYPE - OWNER"] == "O") & org_owners["ORGANIZATION NAME - OWNER"].fillna("").ne("")]
    org_owners = org_owners[org_owners["ROLE TEXT - OWNER"].isin(OWNER_ROLE_PRIORITY.keys())].copy()
    if org_owners.empty:
        return pd.DataFrame(columns=["facility_id", "chain_name", "chain_confidence"])

    org_owners["ASSOCIATE ID"] = org_owners["ASSOCIATE ID"].astype(str)
    org_owners["role_score"] = org_owners["ROLE TEXT - OWNER"].map(OWNER_ROLE_PRIORITY).astype(float)
    org_owners["pct_ownership"] = pd.to_numeric(org_owners.get("PERCENTAGE OWNERSHIP"), errors="coerce").fillna(0.0)
    org_owners["score"] = org_owners["role_score"] + (org_owners["pct_ownership"] / 100.0)

    for col, weight in [
        ("CHAIN HOME OFFICE - OWNER", 0.5),
        ("HOLDING COMPANY - OWNER", 0.25),
        ("MANAGEMENT SERVICES COMPANY - OWNER", 0.1),
    ]:
        if col in org_owners.columns:
            org_owners["score"] += org_owners[col].fillna("").eq("Y").astype(float) * weight

    org_owners = org_owners.sort_values(["ASSOCIATE ID", "score", "pct_ownership"], ascending=[True, False, False])
    best_owner = org_owners.drop_duplicates(subset=["ASSOCIATE ID"], keep="first")[
        ["ASSOCIATE ID", "ORGANIZATION NAME - OWNER"]
    ].copy()

    ccn_owner = ccn_map.merge(best_owner, on="ASSOCIATE ID", how="left")
    ccn_owner = ccn_owner.dropna(subset=["ORGANIZATION NAME - OWNER"]).copy()
    if ccn_owner.empty:
        return pd.DataFrame(columns=["facility_id", "chain_name", "chain_confidence"])

    owner_system_sizes = ccn_owner.groupby("ORGANIZATION NAME - OWNER")["CCN"].nunique()
    ccn_owner["owner_system_size"] = ccn_owner["ORGANIZATION NAME - OWNER"].map(owner_system_sizes)
    ccn_owner = ccn_owner[ccn_owner["owner_system_size"] >= MIN_OWNER_SYSTEM_SIZE].copy()
    if ccn_owner.empty:
        return pd.DataFrame(columns=["facility_id", "chain_name", "chain_confidence"])

    ccn_owner["chain_name"] = ccn_owner["ORGANIZATION NAME - OWNER"].map(canonical_owner_chain_name)
    ccn_owner["chain_confidence"] = "ownership"
    return ccn_owner.rename(columns={"CCN": "facility_id"})[["facility_id", "chain_name", "chain_confidence"]]


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


def apply_private_revenue_model(facilities: pd.DataFrame) -> pd.DataFrame:
    out = facilities.copy()
    if "other_revenue" not in out.columns:
        out["other_revenue"] = np.maximum(out["total_revenue"] - out["medicare_revenue"] - out["medicaid_revenue"], 0)

    ownership_share_map = {
        "for_profit": 0.92,
        "not_for_profit": 0.88,
        "government": 0.72,
        "unknown": 0.82,
        "mixed": 0.85,
    }
    ownership_share = out["ownership_group"].astype(str).map(ownership_share_map).fillna(0.85)
    out["private_revenue"] = (out["other_revenue"] * ownership_share).clip(lower=0)
    out["private_revenue"] = np.minimum(out["private_revenue"], out["other_revenue"]).round(2)
    out["uninsured_other_revenue"] = np.maximum(out["other_revenue"] - out["private_revenue"], 0).round(2)

    medicare_private_admin_share = 0.55
    medicaid_private_admin_share = 0.75
    out["private_carrier_administered_revenue"] = (
        out["private_revenue"]
        + (out["medicare_revenue"] * medicare_private_admin_share)
        + (out["medicaid_revenue"] * medicaid_private_admin_share)
    )
    out["private_carrier_administered_revenue"] = np.minimum(
        out["private_carrier_administered_revenue"], out["total_revenue"]
    ).clip(lower=0).round(2)

    out["public_total"] = (out["medicare_revenue"] + out["medicaid_revenue"]).round(2)
    out["comprehensive_total"] = (out["public_total"] + out["private_revenue"]).round(2)
    out["private_dependency"] = (
        out["private_revenue"] / out["total_revenue"]
    ).replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    out["private_carrier_administered_dependency"] = (
        out["private_carrier_administered_revenue"] / out["total_revenue"]
    ).replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    out["comprehensive_dependency"] = (
        out["comprehensive_total"] / out["total_revenue"]
    ).replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)

    out["private_data_method"] = "modeled_from_other_revenue"
    out["private_data_confidence"] = "medium"
    out["taxonomy_note"] = "funding_source_default_with_modeled_carrier_ownership_overlay"
    return out


def fetch_bytes(url: str) -> bytes:
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                return resp.read()
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)
    return b""


def _commercial_market_value(df: pd.DataFrame, prefix: str) -> pd.Series:
    total_col = f"{prefix}_total"
    yearly_col = f"{prefix}_yearly"
    q1_col = f"{prefix}_q1"
    deferred_py_col = f"{prefix}_deferred_py"
    deferred_cy_col = f"{prefix}_deferred_cy"

    base = pd.to_numeric(df.get(total_col, np.nan), errors="coerce")
    yearly = pd.to_numeric(df.get(yearly_col, np.nan), errors="coerce")
    q1 = pd.to_numeric(df.get(q1_col, np.nan), errors="coerce")
    deferred_py = pd.to_numeric(df.get(deferred_py_col, 0), errors="coerce").fillna(0.0)
    deferred_cy = pd.to_numeric(df.get(deferred_cy_col, 0), errors="coerce").fillna(0.0)

    selected = base.where(base.notna(), yearly)
    selected = selected.where(selected.notna(), q1)
    selected = selected.fillna(0.0)
    return selected + deferred_py - deferred_cy


def fetch_state_insurer_market_table(min_year: int, max_year: int) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for file_year in range(max(MLR_AVAILABLE_START_YEAR, min_year), max_year + 1):
        url = MLR_ZIP_URL_TEMPLATE.format(year=file_year)
        try:
            blob = fetch_bytes(url)
        except Exception:
            continue

        try:
            zf = zipfile.ZipFile(io.BytesIO(blob))
        except zipfile.BadZipFile:
            continue

        part_name = "Part1_2_Summary_Data_Premium_Claims.csv"
        header_name = "MR_Submission_Template_Header.csv"
        if part_name not in zf.namelist() or header_name not in zf.namelist():
            continue

        with zf.open(part_name) as part_fh:
            part = pd.read_csv(part_fh, dtype=str, low_memory=False)
        with zf.open(header_name) as header_fh:
            header = pd.read_csv(header_fh, dtype=str, low_memory=False)

        part.columns = [str(c).strip().lower() for c in part.columns]
        header.columns = [str(c).strip().lower() for c in header.columns]

        needed_codes = {"TOTAL_DIRECT_PREMIUM_EARNED", "NUMBER_OF_COVERED_LIVES", "TOTAL_INCURRED_CLAIMS_PT1"}
        if "row_lookup_code" not in part.columns:
            continue
        part["row_lookup_code"] = part["row_lookup_code"].astype(str).str.upper()
        part = part[part["row_lookup_code"].isin(needed_codes)].copy()
        if part.empty:
            continue

        merge_cols = ["mr_submission_template_id", "business_state", "company_name", "hios_issuer_id", "naic_company_code", "dba_marketing_name"]
        for col in merge_cols:
            if col not in header.columns:
                header[col] = ""
        header = header[merge_cols].copy()
        merged = part.merge(header, on="mr_submission_template_id", how="left")
        merged["business_state"] = merged["business_state"].fillna("").str.upper()
        merged = merged[merged["business_state"].str.fullmatch(r"[A-Z]{2}")]
        if merged.empty:
            continue

        for prefix in ["cmm_individual", "cmm_small_group", "cmm_large_group"]:
            merged[f"{prefix}_market_value"] = _commercial_market_value(merged, prefix)

        merged["commercial_value"] = (
            merged["cmm_individual_market_value"]
            + merged["cmm_small_group_market_value"]
            + merged["cmm_large_group_market_value"]
        )
        merged["commercial_value"] = pd.to_numeric(merged["commercial_value"], errors="coerce").fillna(0.0)

        merged["insurer_id"] = merged["naic_company_code"].fillna("").str.strip()
        merged["insurer_id"] = np.where(
            merged["insurer_id"] != "",
            merged["insurer_id"],
            merged["hios_issuer_id"].fillna("").str.strip(),
        )
        merged["insurer_id"] = np.where(
            merged["insurer_id"] != "",
            merged["insurer_id"],
            merged["company_name"].fillna("").str.upper().str.replace(r"[^A-Z0-9]+", "_", regex=True).str[:40],
        )

        merged["insurer_name"] = merged["dba_marketing_name"].fillna("").str.strip()
        merged["insurer_name"] = np.where(
            merged["insurer_name"] != "",
            merged["insurer_name"],
            merged["company_name"].fillna("Unknown Insurer").str.strip(),
        )

        aggregated = (
            merged.groupby(["business_state", "insurer_id", "insurer_name", "row_lookup_code"], as_index=False)["commercial_value"]
            .sum()
        )

        pivot = aggregated.pivot_table(
            index=["business_state", "insurer_id", "insurer_name"],
            columns="row_lookup_code",
            values="commercial_value",
            aggfunc="sum",
            fill_value=0.0,
        ).reset_index()

        for col in ["TOTAL_DIRECT_PREMIUM_EARNED", "NUMBER_OF_COVERED_LIVES", "TOTAL_INCURRED_CLAIMS_PT1"]:
            if col not in pivot.columns:
                pivot[col] = 0.0

        pivot = pivot.rename(
            columns={
                "business_state": "state_code",
                "TOTAL_DIRECT_PREMIUM_EARNED": "premium",
                "NUMBER_OF_COVERED_LIVES": "covered_lives",
                "TOTAL_INCURRED_CLAIMS_PT1": "claims",
            }
        )
        pivot["fiscal_year"] = int(file_year)
        pivot["source_file_year"] = int(file_year)
        pivot["confidence_tier"] = "B"
        pivot["data_source"] = f"CMS MLR PUF {file_year}"
        rows.append(pivot)

    if not rows:
        return pd.DataFrame(
            columns=[
                "fiscal_year",
                "state_code",
                "insurer_id",
                "insurer_name",
                "covered_lives",
                "premium",
                "claims",
                "market_share_lives",
                "market_share_premium",
                "market_share_claims",
                "source_file_year",
                "confidence_tier",
                "data_source",
            ]
        )

    out = pd.concat(rows, ignore_index=True)
    out["covered_lives"] = pd.to_numeric(out["covered_lives"], errors="coerce").fillna(0.0).clip(lower=0)
    out["premium"] = pd.to_numeric(out["premium"], errors="coerce").fillna(0.0).clip(lower=0)
    out["claims"] = pd.to_numeric(out["claims"], errors="coerce").fillna(0.0).clip(lower=0)

    totals = out.groupby(["fiscal_year", "state_code"], as_index=False).agg(
        state_lives=("covered_lives", "sum"),
        state_premium=("premium", "sum"),
        state_claims=("claims", "sum"),
    )
    out = out.merge(totals, on=["fiscal_year", "state_code"], how="left")
    out["market_share_lives"] = np.divide(
        out["covered_lives"], out["state_lives"], out=np.zeros(len(out), dtype=float), where=out["state_lives"] > 0
    )
    out["market_share_premium"] = np.divide(
        out["premium"], out["state_premium"], out=np.zeros(len(out), dtype=float), where=out["state_premium"] > 0
    )
    out["market_share_claims"] = np.divide(
        out["claims"], out["state_claims"], out=np.zeros(len(out), dtype=float), where=out["state_claims"] > 0
    )
    out = out.drop(columns=["state_lives", "state_premium", "state_claims"])
    out = out.sort_values(["fiscal_year", "state_code", "market_share_lives"], ascending=[True, True, False])
    return out


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


def _extract_emma_issuer_id(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    return str(qs.get("id", [""])[0]).strip()


def apply_emma_mappings(df: pd.DataFrame) -> pd.DataFrame:
    mapping_path = MANUAL_DIR / "emma_issuer_map.csv"
    if not mapping_path.exists():
        pd.DataFrame(columns=EMMA_MAPPING_COLUMNS).to_csv(mapping_path, index=False)
        df["emma_issuer_id"] = ""
        df["emma_issuer_name"] = ""
        df["emma_issuer_url"] = ""
        df["emma_match_score"] = 0.0
        df["emma_mapping_method"] = "none"
        df["emma_mapping_status"] = np.where(df["ownership_group"] == "not_for_profit", "unmapped", "not_applicable")
        return df

    emma_map = pd.read_csv(mapping_path, dtype=str).fillna("")
    if emma_map.empty or "facility_id" not in emma_map.columns:
        df["emma_issuer_id"] = ""
        df["emma_issuer_name"] = ""
        df["emma_issuer_url"] = ""
        df["emma_match_score"] = 0.0
        df["emma_mapping_method"] = "none"
        df["emma_mapping_status"] = np.where(df["ownership_group"] == "not_for_profit", "unmapped", "not_applicable")
        return df

    # Ensure required columns exist even if legacy/manual file is sparse.
    for col in EMMA_MAPPING_COLUMNS:
        if col not in emma_map.columns:
            emma_map[col] = ""

    emma_map = emma_map[EMMA_MAPPING_COLUMNS].copy()
    emma_map["facility_id"] = emma_map["facility_id"].astype(str).str.zfill(6)
    emma_map["emma_issuer_id"] = emma_map["emma_issuer_id"].where(emma_map["emma_issuer_id"] != "", emma_map["emma_issuer_url"].map(_extract_emma_issuer_id))
    emma_map["emma_issuer_url"] = emma_map.apply(
        lambda r: r["emma_issuer_url"]
        if r["emma_issuer_url"]
        else (
            f"https://emma.msrb.org/IssuerHomePage/Issuer?id={r['emma_issuer_id']}"
            if r["emma_issuer_id"]
            else ""
        ),
        axis=1,
    )

    emma_map["emma_match_score"] = pd.to_numeric(emma_map["emma_match_score"], errors="coerce").fillna(0.0)
    emma_map["emma_mapping_method"] = emma_map["emma_mapping_method"].replace("", "manual")
    emma_map["emma_mapping_status"] = emma_map["emma_mapping_status"].replace("", "mapped")

    map_cols = [
        "facility_id",
        "emma_issuer_id",
        "emma_issuer_name",
        "emma_issuer_url",
        "emma_match_score",
        "emma_mapping_method",
        "emma_mapping_status",
    ]
    merged = df.merge(emma_map[map_cols].drop_duplicates(subset=["facility_id"], keep="last"), on="facility_id", how="left")
    merged["emma_issuer_id"] = merged["emma_issuer_id"].fillna("")
    merged["emma_issuer_name"] = merged["emma_issuer_name"].fillna("")
    merged["emma_issuer_url"] = merged["emma_issuer_url"].fillna("")
    merged["emma_match_score"] = merged["emma_match_score"].fillna(0.0)
    merged["emma_mapping_method"] = merged["emma_mapping_method"].fillna("none")
    merged["emma_mapping_status"] = merged["emma_mapping_status"].fillna("")

    default_status = np.where(merged["ownership_group"] == "not_for_profit", "unmapped", "not_applicable")
    merged["emma_mapping_status"] = np.where(merged["emma_mapping_status"] == "", default_status, merged["emma_mapping_status"])
    return merged


def build_facility_table(
    cms_df: pd.DataFrame,
    stats: SourceStats,
    fmap_df: pd.DataFrame,
    ownership_chain_map: pd.DataFrame | None = None,
) -> pd.DataFrame:
    df = cms_df.copy()
    df["fy_end_date"] = pd.to_datetime(df["Fiscal Year End Date"], errors="coerce")
    df["source_release_year"] = pd.to_numeric(df.get("source_release_year"), errors="coerce")
    df = df[df["source_release_year"].isin(stats.release_years)].copy()
    df["fiscal_year"] = df["source_release_year"].astype(int)

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

    # Resolve one record per provider CCN per annual release (highest net patient revenue preferred).
    df["facility_id"] = df["Provider CCN"].astype(str).str.zfill(6)
    df = df.sort_values(["fiscal_year", "facility_id", "fy_end_date", "Net Patient Revenue"], ascending=[True, True, False, False])
    df = df.drop_duplicates(subset=["fiscal_year", "facility_id"], keep="first").copy()

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
            "fiscal_year": df["fiscal_year"].astype(int),
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

    if ownership_chain_map is not None and not ownership_chain_map.empty:
        ownership_lookup = ownership_chain_map.set_index("facility_id")
        ownership_ids = set(ownership_lookup.index.astype(str))
        ownership_mask = facilities["facility_id"].isin(ownership_ids)
        if ownership_mask.any():
            facilities.loc[ownership_mask, "chain_name"] = facilities.loc[ownership_mask, "facility_id"].map(
                ownership_lookup["chain_name"]
            )
            facilities.loc[ownership_mask, "chain_confidence"] = facilities.loc[ownership_mask, "facility_id"].map(
                ownership_lookup["chain_confidence"]
            )

    facilities = apply_chain_overrides(facilities)
    facilities = apply_emma_mappings(facilities)

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

    facilities = apply_private_revenue_model(facilities)
    return facilities


def build_state_summary(facilities: pd.DataFrame) -> pd.DataFrame:
    grouped = facilities.groupby(["fiscal_year", "state_code"], as_index=False).agg(
        medicaid_total=("medicaid_revenue", "sum"),
        medicare_total=("medicare_revenue", "sum"),
        federal_medicaid_total=("federal_medicaid_revenue", "sum"),
        state_medicaid_total=("state_medicaid_revenue", "sum"),
        private_total=("private_revenue", "sum"),
        private_carrier_administered_total=("private_carrier_administered_revenue", "sum"),
        uninsured_other_total=("uninsured_other_revenue", "sum"),
        total_revenue=("total_revenue", "sum"),
        facility_count=("facility_id", "nunique"),
        chain_count=("chain_name", "nunique"),
    )

    grouped["public_total"] = grouped["medicaid_total"] + grouped["medicare_total"]
    grouped["comprehensive_total"] = grouped["public_total"] + grouped["private_total"]
    grouped["public_dependency"] = grouped["public_total"] / grouped["total_revenue"]
    grouped["private_dependency"] = grouped["private_total"] / grouped["total_revenue"]
    grouped["private_carrier_administered_dependency"] = (
        grouped["private_carrier_administered_total"] / grouped["total_revenue"]
    )
    grouped["comprehensive_dependency"] = grouped["comprehensive_total"] / grouped["total_revenue"]
    grouped["public_dependency"] = grouped["public_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped["private_dependency"] = grouped["private_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped["private_carrier_administered_dependency"] = grouped["private_carrier_administered_dependency"].replace(
        [np.inf, -np.inf], 0
    ).fillna(0).clip(0, 1)
    grouped["comprehensive_dependency"] = grouped["comprehensive_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped = grouped.sort_values("state_code")
    return grouped


def build_chain_summary(facilities: pd.DataFrame) -> pd.DataFrame:
    grouped = facilities.groupby(["fiscal_year", "chain_name"], as_index=False).agg(
        medicaid_total=("medicaid_revenue", "sum"),
        medicare_total=("medicare_revenue", "sum"),
        federal_medicaid_total=("federal_medicaid_revenue", "sum"),
        state_medicaid_total=("state_medicaid_revenue", "sum"),
        private_total=("private_revenue", "sum"),
        private_carrier_administered_total=("private_carrier_administered_revenue", "sum"),
        uninsured_other_total=("uninsured_other_revenue", "sum"),
        total_revenue=("total_revenue", "sum"),
        facility_count=("facility_id", "nunique"),
        state_count=("state_code", "nunique"),
    )
    grouped["public_total"] = grouped["medicaid_total"] + grouped["medicare_total"]
    grouped["comprehensive_total"] = grouped["public_total"] + grouped["private_total"]
    grouped["public_dependency"] = grouped["public_total"] / grouped["total_revenue"]
    grouped["private_dependency"] = grouped["private_total"] / grouped["total_revenue"]
    grouped["private_carrier_administered_dependency"] = (
        grouped["private_carrier_administered_total"] / grouped["total_revenue"]
    )
    grouped["comprehensive_dependency"] = grouped["comprehensive_total"] / grouped["total_revenue"]
    grouped["public_dependency"] = grouped["public_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped["private_dependency"] = grouped["private_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped["private_carrier_administered_dependency"] = grouped["private_carrier_administered_dependency"].replace(
        [np.inf, -np.inf], 0
    ).fillna(0).clip(0, 1)
    grouped["comprehensive_dependency"] = grouped["comprehensive_dependency"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)
    grouped = grouped.sort_values("public_total", ascending=False)
    return grouped


def persist_outputs(
    facilities: pd.DataFrame,
    states: pd.DataFrame,
    chains: pd.DataFrame,
    state_insurer_market: pd.DataFrame,
    stats: SourceStats,
    ownership_stats: dict[str, Any] | None = None,
) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    facilities.to_csv(PROCESSED_DIR / "facilities.csv", index=False)
    states.to_csv(PROCESSED_DIR / "state_summary.csv", index=False)
    chains.to_csv(PROCESSED_DIR / "chain_summary.csv", index=False)
    state_insurer_market.to_csv(PROCESSED_DIR / "state_insurer_market.csv", index=False)

    db_path = PROCESSED_DIR / "app.db"
    if db_path.exists():
        db_path.unlink()

    with sqlite3.connect(db_path) as conn:
        facilities.to_sql("facilities", conn, index=False)
        states.to_sql("state_summary", conn, index=False)
        chains.to_sql("chain_summary", conn, index=False)
        state_insurer_market.to_sql("state_insurer_market", conn, index=False)

    metadata = {
        "source_stats": {
            "cms_total_rows": stats.total_rows,
            "cms_release_year_counts": stats.release_year_counts,
            "cms_fiscal_year_end_counts": stats.fiscal_year_end_counts,
            "selected_fiscal_year": stats.selected_fiscal_year,
            "release_years_loaded": stats.release_years,
        },
        "methodology": {
            "medicaid": "Net Revenue from Medicaid from CMS hospital cost reports",
            "medicare": (
                "Estimated from Medicare IPPS payment components plus Medicare inpatient day-share proxy, "
                "capped by total net patient revenue"
            ),
            "federal_state_split": "State-level FMAP percentages from Federal Register FY 2026 table",
            "chain_grouping": (
                "CMS PECOS hospital ownership mapping (owner organizations with >=2 hospitals), "
                "then facility-name keyword fallback and optional manual overrides"
            ),
            "emma_mapping": "Facility-level not-for-profit issuer mapping sourced from data/manual/emma_issuer_map.csv",
            "private_insurance": (
                "Private revenue modeled from residual other revenue and ownership priors; "
                "state insurer market shares from CMS Medical Loss Ratio public use files"
            ),
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
            {
                "name": "CMS Hospital Enrollments (PECOS)",
                "url": "https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/hospital-enrollments",
            },
            {
                "name": "CMS Hospital All Owners (PECOS)",
                "url": "https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/hospital-all-owners",
            },
            {
                "name": "CMS Medical Loss Ratio Public Use Files",
                "url": "https://www.cms.gov/cciio/resources/data-resources/mlr",
            },
        ],
        "generated_at_utc": pd.Timestamp.utcnow().isoformat(),
    }

    if ownership_stats:
        metadata["source_stats"]["ownership"] = ownership_stats
    metadata["source_stats"]["state_insurer_market_rows"] = int(len(state_insurer_market))

    (PROCESSED_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)
    ownership_chain_map = pd.DataFrame(columns=["facility_id", "chain_name", "chain_confidence"])
    ownership_stats: dict[str, Any] = {}

    print(f"Downloading CMS hospital cost reports ({CMS_HISTORY_YEARS} release years)...")
    cms_df, stats = fetch_cms_hospital_data(history_years=CMS_HISTORY_YEARS)
    print(
        f"Downloaded {stats.total_rows} rows across releases {min(stats.release_years)}-{max(stats.release_years)}; "
        f"selected FY {stats.selected_fiscal_year}"
    )

    print("Downloading CMS PECOS hospital ownership datasets...")
    enrollments_df, owners_df = fetch_hospital_ownership_data()
    ownership_chain_map = build_ownership_chain_map(enrollments_df, owners_df)
    ownership_stats = {
        "hospital_enrollments_rows": int(len(enrollments_df)),
        "hospital_all_owners_rows": int(len(owners_df)),
        "mapped_ccn_count": int(ownership_chain_map["facility_id"].nunique()),
        "min_owner_system_size": MIN_OWNER_SYSTEM_SIZE,
    }
    print(
        "Ownership mapping prepared: "
        f"{ownership_stats['mapped_ccn_count']} CCNs linked to multi-site owner organizations"
    )

    print("Downloading/parsing FMAP table...")
    fmap_df = fetch_fmap_table()

    print("Downloading/parsing CMS MLR private insurer market data...")
    state_insurer_market = fetch_state_insurer_market_table(
        min_year=min(stats.release_years),
        max_year=max(stats.release_years) + 1,
    )
    if state_insurer_market.empty:
        print("Warning: no state insurer market rows found; private insurer views will be unavailable until this source loads.")
    else:
        raw_path = RAW_DIR / "mlr_state_insurer_market.csv"
        state_insurer_market.to_csv(raw_path, index=False)
        print(
            f"State insurer market rows: {len(state_insurer_market)} "
            f"across {state_insurer_market['state_code'].nunique()} states"
        )

    print("Building facility-level analytics table...")
    facilities = build_facility_table(cms_df, stats, fmap_df, ownership_chain_map=ownership_chain_map)
    states = build_state_summary(facilities)
    chains = build_chain_summary(facilities)

    print(f"Facilities in output: {len(facilities)}")
    print(f"States in output: {states['state_code'].nunique()}")

    persist_outputs(
        facilities,
        states,
        chains,
        state_insurer_market=state_insurer_market,
        stats=stats,
        ownership_stats=ownership_stats,
    )
    print(f"Wrote processed data to {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
