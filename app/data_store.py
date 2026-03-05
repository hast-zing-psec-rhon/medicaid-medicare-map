from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"
EMMA_FIELDS = [
    "emma_issuer_id",
    "emma_issuer_name",
    "emma_issuer_url",
    "emma_mapping_status",
    "emma_mapping_method",
    "emma_match_score",
]

PRIVATE_REVENUE_SHARE_BY_OWNERSHIP = {
    "for_profit": 0.92,
    "not_for_profit": 0.88,
    "government": 0.72,
    "unknown": 0.82,
    "mixed": 0.85,
}
DEFAULT_PRIVATE_REVENUE_SHARE = 0.85
DEFAULT_MEDICARE_PRIVATE_ADMIN_SHARE = 0.55
DEFAULT_MEDICAID_PRIVATE_ADMIN_SHARE = 0.75

MARKET_SHARE_BASIS_TO_COLUMN = {
    "covered_lives": "market_share_lives",
    "premium": "market_share_premium",
    "claims": "market_share_claims",
}

STATE_INSURER_MARKET_COLUMNS = [
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


@dataclass
class DataStore:
    facilities: pd.DataFrame
    state_summary: pd.DataFrame
    chain_summary: pd.DataFrame
    metadata: dict[str, Any]
    state_insurer_market: pd.DataFrame


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


def _safe_divide(numerator: pd.Series | np.ndarray, denominator: pd.Series | np.ndarray) -> np.ndarray:
    return np.divide(
        numerator,
        denominator,
        out=np.zeros_like(np.asarray(numerator, dtype=float), dtype=float),
        where=np.asarray(denominator, dtype=float) > 0,
    )


def _apply_private_financial_model(facilities: pd.DataFrame) -> pd.DataFrame:
    df = facilities.copy()

    numeric_cols = [
        "total_revenue",
        "medicare_revenue",
        "medicaid_revenue",
        "federal_medicaid_revenue",
        "state_medicaid_revenue",
        "other_revenue",
        "private_revenue",
        "uninsured_other_revenue",
        "private_carrier_administered_revenue",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "other_revenue" not in df.columns or (df["other_revenue"] == 0).all():
        df["other_revenue"] = np.maximum(df["total_revenue"] - df["medicare_revenue"] - df["medicaid_revenue"], 0)

    if "private_revenue" not in df.columns or df["private_revenue"].isna().all() or (df["private_revenue"] == 0).all():
        ownership_share = (
            df.get("ownership_group", pd.Series("unknown", index=df.index))
            .astype(str)
            .str.lower()
            .map(PRIVATE_REVENUE_SHARE_BY_OWNERSHIP)
            .fillna(DEFAULT_PRIVATE_REVENUE_SHARE)
        )
        modeled_private = (df["other_revenue"] * ownership_share).clip(lower=0)
        df["private_revenue"] = np.minimum(modeled_private, df["other_revenue"]).round(2)

    df["private_revenue"] = np.minimum(df["private_revenue"], df["other_revenue"]).clip(lower=0)

    if "uninsured_other_revenue" not in df.columns or (df["uninsured_other_revenue"] == 0).all():
        df["uninsured_other_revenue"] = np.maximum(df["other_revenue"] - df["private_revenue"], 0).round(2)
    else:
        df["uninsured_other_revenue"] = np.maximum(df["uninsured_other_revenue"], 0)

    if "private_carrier_administered_revenue" not in df.columns or (
        df["private_carrier_administered_revenue"].isna().all() or (df["private_carrier_administered_revenue"] == 0).all()
    ):
        private_carrier = (
            df["private_revenue"]
            + (df["medicare_revenue"] * DEFAULT_MEDICARE_PRIVATE_ADMIN_SHARE)
            + (df["medicaid_revenue"] * DEFAULT_MEDICAID_PRIVATE_ADMIN_SHARE)
        )
        df["private_carrier_administered_revenue"] = np.minimum(private_carrier, df["total_revenue"]).clip(lower=0).round(2)
    else:
        df["private_carrier_administered_revenue"] = np.minimum(
            np.maximum(df["private_carrier_administered_revenue"], 0), df["total_revenue"]
        ).round(2)

    df["public_total"] = (df["medicare_revenue"] + df["medicaid_revenue"]).round(2)
    df["comprehensive_total"] = (df["public_total"] + df["private_revenue"]).round(2)

    df["medicare_dependency"] = _safe_divide(df["medicare_revenue"], df["total_revenue"])
    df["medicaid_dependency"] = _safe_divide(df["medicaid_revenue"], df["total_revenue"])
    df["public_dependency"] = _safe_divide(df["public_total"], df["total_revenue"])
    df["private_dependency"] = _safe_divide(df["private_revenue"], df["total_revenue"])
    df["private_carrier_administered_dependency"] = _safe_divide(
        df["private_carrier_administered_revenue"],
        df["total_revenue"],
    )
    df["comprehensive_dependency"] = _safe_divide(df["comprehensive_total"], df["total_revenue"])

    for dep_col in [
        "medicare_dependency",
        "medicaid_dependency",
        "public_dependency",
        "private_dependency",
        "private_carrier_administered_dependency",
        "comprehensive_dependency",
    ]:
        df[dep_col] = df[dep_col].clip(lower=0, upper=1)

    if "private_data_method" not in df.columns:
        df["private_data_method"] = "modeled_from_other_revenue"
    if "private_data_confidence" not in df.columns:
        df["private_data_confidence"] = "medium"

    return df


def _build_state_summary_from_facilities(facilities: pd.DataFrame) -> pd.DataFrame:
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
    grouped["public_dependency"] = _safe_divide(grouped["public_total"], grouped["total_revenue"])
    grouped["private_dependency"] = _safe_divide(grouped["private_total"], grouped["total_revenue"])
    grouped["private_carrier_administered_dependency"] = _safe_divide(
        grouped["private_carrier_administered_total"], grouped["total_revenue"]
    )
    grouped["comprehensive_dependency"] = _safe_divide(grouped["comprehensive_total"], grouped["total_revenue"])

    for col in ["public_dependency", "private_dependency", "private_carrier_administered_dependency", "comprehensive_dependency"]:
        grouped[col] = grouped[col].clip(0, 1)

    grouped = grouped.sort_values(["fiscal_year", "state_code"])
    return grouped


def _build_chain_summary_from_facilities(facilities: pd.DataFrame) -> pd.DataFrame:
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
    grouped["public_dependency"] = _safe_divide(grouped["public_total"], grouped["total_revenue"])
    grouped["private_dependency"] = _safe_divide(grouped["private_total"], grouped["total_revenue"])
    grouped["private_carrier_administered_dependency"] = _safe_divide(
        grouped["private_carrier_administered_total"], grouped["total_revenue"]
    )
    grouped["comprehensive_dependency"] = _safe_divide(grouped["comprehensive_total"], grouped["total_revenue"])
    for col in ["public_dependency", "private_dependency", "private_carrier_administered_dependency", "comprehensive_dependency"]:
        grouped[col] = grouped[col].clip(0, 1)

    grouped = grouped.sort_values("public_total", ascending=False)
    return grouped


def _load_state_insurer_market() -> pd.DataFrame:
    path = PROCESSED_DIR / "state_insurer_market.csv"
    if not path.exists():
        return pd.DataFrame(columns=STATE_INSURER_MARKET_COLUMNS)

    df = pd.read_csv(path, dtype={"state_code": str, "insurer_id": str, "insurer_name": str})
    for col in STATE_INSURER_MARKET_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col in {"state_code", "insurer_id", "insurer_name", "confidence_tier", "data_source"} else 0.0

    df["state_code"] = df["state_code"].fillna("").astype(str).str.upper()
    df["insurer_id"] = df["insurer_id"].fillna("").astype(str)
    df["insurer_name"] = df["insurer_name"].fillna("").astype(str)

    for col in ["fiscal_year", "source_file_year"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["covered_lives", "premium", "claims", "market_share_lives", "market_share_premium", "market_share_claims"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df[STATE_INSURER_MARKET_COLUMNS].copy()


def load_data() -> DataStore:
    facilities_path = PROCESSED_DIR / "facilities.csv"
    states_path = PROCESSED_DIR / "state_summary.csv"
    chains_path = PROCESSED_DIR / "chain_summary.csv"
    metadata_path = PROCESSED_DIR / "metadata.json"

    for p in [facilities_path, states_path, chains_path, metadata_path]:
        _ensure_file(p)

    facilities = pd.read_csv(facilities_path, dtype={"facility_id": str, "state_code": str})
    for col in EMMA_FIELDS:
        if col not in facilities.columns:
            if col == "emma_match_score":
                facilities[col] = 0.0
            elif col == "emma_mapping_status":
                facilities[col] = np.where(facilities.get("ownership_group", "") == "not_for_profit", "unmapped", "not_applicable")
            elif col == "emma_mapping_method":
                facilities[col] = "none"
            else:
                facilities[col] = ""
    for col in ["emma_issuer_id", "emma_issuer_name", "emma_issuer_url", "emma_mapping_status", "emma_mapping_method"]:
        facilities[col] = facilities[col].fillna("").astype(str)
        facilities[col] = facilities[col].replace({"nan": "", "None": ""})
    facilities["emma_match_score"] = pd.to_numeric(facilities["emma_match_score"], errors="coerce").fillna(0.0)

    facilities = _apply_private_financial_model(facilities)

    # Keep compatibility with persisted files but compute enriched summaries from facilities.
    _ = pd.read_csv(states_path, dtype={"state_code": str})
    _ = pd.read_csv(chains_path)

    states = _build_state_summary_from_facilities(facilities)
    chains = _build_chain_summary_from_facilities(facilities)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    state_insurer_market = _load_state_insurer_market()

    return DataStore(
        facilities=facilities,
        state_summary=states,
        chain_summary=chains,
        metadata=metadata,
        state_insurer_market=state_insurer_market,
    )


def available_years(store: DataStore) -> list[int]:
    years = sorted(store.facilities["fiscal_year"].dropna().astype(int).unique().tolist())
    return years


def select_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    return df[df["fiscal_year"].astype(int) == int(year)].copy()


def _metric_for_taxonomy(metric: str, taxonomy_view: str) -> str:
    if taxonomy_view == "carrier_ownership":
        if metric == "private_total":
            return "private_carrier_administered_total"
        if metric == "private_dependency":
            return "private_carrier_administered_dependency"
    return metric


def _apply_scope_projection(df: pd.DataFrame, payer_scope: str, taxonomy_view: str) -> pd.DataFrame:
    out = df.copy()

    if taxonomy_view == "carrier_ownership":
        if "private_carrier_administered_total" in out.columns:
            out["private_total"] = out["private_carrier_administered_total"]
        if "private_carrier_administered_dependency" in out.columns:
            out["private_dependency"] = out["private_carrier_administered_dependency"]

    if payer_scope == "public_only":
        for col in ["private_total", "private_revenue", "private_dependency"]:
            if col in out.columns:
                out[col] = 0.0
        if "comprehensive_total" in out.columns and "public_total" in out.columns:
            out["comprehensive_total"] = out["public_total"]
        if "comprehensive_dependency" in out.columns and "public_dependency" in out.columns:
            out["comprehensive_dependency"] = out["public_dependency"]

    return out


def get_states(store: DataStore, year: int, metric: str, payer_scope: str = "public_only", taxonomy_view: str = "funding_source") -> list[dict[str, Any]]:
    df = select_year(store.state_summary, year)
    df = _apply_scope_projection(df, payer_scope=payer_scope, taxonomy_view=taxonomy_view)

    metric = _metric_for_taxonomy(metric, taxonomy_view)
    if metric not in df.columns:
        metric = "medicaid_total"

    if payer_scope == "public_only" and metric in {"private_total", "private_carrier_administered_total", "private_dependency", "private_carrier_administered_dependency", "comprehensive_total"}:
        metric = "public_total"

    df = df.sort_values(metric, ascending=False)
    return [_round_record(rec) for rec in df.to_dict(orient="records")]


def get_state_summary(
    store: DataStore,
    state_code: str,
    year: int,
    payer_scope: str = "public_only",
    taxonomy_view: str = "funding_source",
) -> Optional[dict[str, Any]]:
    df = select_year(store.state_summary, year)
    subset = df[df["state_code"] == state_code.upper()]
    if subset.empty:
        return None

    out = _apply_scope_projection(subset, payer_scope=payer_scope, taxonomy_view=taxonomy_view)
    return _round_record(out.iloc[0].to_dict())


def get_facilities(
    store: DataStore,
    state_code: str,
    year: int,
    ownership: str,
    chain_name: Optional[str],
    sort_field: str,
    descending: bool,
    group_by_chain: bool,
    payer_scope: str = "public_only",
    taxonomy_view: str = "funding_source",
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
            private_revenue=("private_revenue", "sum"),
            private_carrier_administered_revenue=("private_carrier_administered_revenue", "sum"),
            uninsured_other_revenue=("uninsured_other_revenue", "sum"),
            total_revenue=("total_revenue", "sum"),
            other_revenue=("other_revenue", "sum"),
            facility_count=("facility_id", "nunique"),
        )
        grouped["public_total"] = grouped["medicaid_revenue"] + grouped["medicare_revenue"]
        grouped["comprehensive_total"] = grouped["public_total"] + grouped["private_revenue"]
        grouped["medicare_dependency"] = _safe_divide(grouped["medicare_revenue"], grouped["total_revenue"])
        grouped["medicaid_dependency"] = _safe_divide(grouped["medicaid_revenue"], grouped["total_revenue"])
        grouped["public_dependency"] = _safe_divide(grouped["public_total"], grouped["total_revenue"])
        grouped["private_dependency"] = _safe_divide(grouped["private_revenue"], grouped["total_revenue"])
        grouped["private_carrier_administered_dependency"] = _safe_divide(
            grouped["private_carrier_administered_revenue"], grouped["total_revenue"]
        )
        grouped["comprehensive_dependency"] = _safe_divide(grouped["comprehensive_total"], grouped["total_revenue"])
        grouped["state_code"] = state_code.upper()
        grouped["facility_name"] = grouped["chain_name"]
        grouped["facility_id"] = "CHAIN::" + grouped["chain_name"]
        grouped["city"] = ""
        grouped["ownership_group"] = "mixed"
        grouped["ownership_type"] = "Mixed"
        grouped["facility_type"] = "CHAIN"
        grouped["chain_confidence"] = "aggregated"
        grouped["medicare_method"] = "aggregated"
        grouped["private_data_method"] = "aggregated"
        grouped["private_data_confidence"] = "medium"
        grouped["emma_issuer_id"] = ""
        grouped["emma_issuer_name"] = ""
        grouped["emma_issuer_url"] = ""
        grouped["emma_mapping_status"] = "not_applicable"
        grouped["emma_mapping_method"] = "aggregated"
        grouped["emma_match_score"] = 0.0

        df = grouped

    df = _apply_scope_projection(df, payer_scope=payer_scope, taxonomy_view=taxonomy_view)

    sort_field = _metric_for_taxonomy(sort_field, taxonomy_view)
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


def get_facility_emma_link(store: DataStore, facility_id: str, year: int) -> Optional[dict[str, Any]]:
    detail = get_facility(store, facility_id=facility_id, year=year)
    if not detail:
        return None

    status = str(detail.get("emma_mapping_status", "") or "")
    if not status:
        status = "unmapped" if detail.get("ownership_group") == "not_for_profit" else "not_applicable"

    issuer_id = str(detail.get("emma_issuer_id", "") or "")
    issuer_name = str(detail.get("emma_issuer_name", "") or "")
    issuer_url = str(detail.get("emma_issuer_url", "") or "")
    if issuer_id.lower() == "nan":
        issuer_id = ""
    if issuer_name.lower() == "nan":
        issuer_name = ""
    if issuer_url.lower() == "nan":
        issuer_url = ""
    if not issuer_url and issuer_id:
        issuer_url = f"https://emma.msrb.org/IssuerHomePage/Issuer?id={issuer_id}"

    link_type = "issuer" if issuer_url else "none"
    resolved_url = issuer_url

    return {
        "facility_id": str(detail.get("facility_id", "")),
        "facility_name": str(detail.get("facility_name", "")),
        "state_code": str(detail.get("state_code", "")),
        "ownership_group": str(detail.get("ownership_group", "")),
        "emma_mapping_status": status,
        "emma_mapping_method": str(detail.get("emma_mapping_method", "none") or "none"),
        "emma_match_score": float(detail.get("emma_match_score", 0.0) or 0.0),
        "emma_issuer_id": issuer_id,
        "emma_issuer_name": issuer_name,
        "emma_issuer_url": issuer_url,
        "emma_primary_url": issuer_url,
        "emma_resolved_url": resolved_url,
        "emma_link_type": link_type,
        "emma_fallback_status": "not_requested",
        "emma_fallback_type": "none",
        "emma_fallback_url": "",
        "emma_fallback_search_term": "",
        "emma_fallback_cusip_query": "",
        "emma_fallback_cusip9": "",
        "emma_fallback_issue_id": "",
        "emma_fallback_issue_desc": "",
        "emma_fallback_issuer_name": "",
        "emma_fallback_match_basis": "",
        "emma_fallback_outstanding_filter_applied": False,
        "emma_fallback_error": "",
    }


def get_chains(
    store: DataStore,
    year: int,
    state_code: Optional[str],
    payer_scope: str = "public_only",
    taxonomy_view: str = "funding_source",
) -> list[dict[str, Any]]:
    if state_code:
        df = select_year(store.facilities, year)
        df = df[df["state_code"] == state_code.upper()]
        if df.empty:
            return []
        grouped = _build_chain_summary_from_facilities(df)
        grouped = _apply_scope_projection(grouped, payer_scope=payer_scope, taxonomy_view=taxonomy_view)
        grouped = grouped.sort_values("public_total", ascending=False)
        return [_round_record(rec) for rec in grouped.to_dict(orient="records")]

    df = _apply_scope_projection(select_year(store.chain_summary, year), payer_scope=payer_scope, taxonomy_view=taxonomy_view)
    df = df.sort_values("public_total", ascending=False)
    return [_round_record(rec) for rec in df.to_dict(orient="records")]


def get_national_trend(store: DataStore, payer_scope: str = "public_only", taxonomy_view: str = "funding_source") -> list[dict[str, Any]]:
    grouped = store.state_summary.groupby("fiscal_year", as_index=False).agg(
        medicaid_total=("medicaid_total", "sum"),
        medicare_total=("medicare_total", "sum"),
        federal_medicaid_total=("federal_medicaid_total", "sum"),
        state_medicaid_total=("state_medicaid_total", "sum"),
        private_total=("private_total", "sum"),
        private_carrier_administered_total=("private_carrier_administered_total", "sum"),
        uninsured_other_total=("uninsured_other_total", "sum"),
        total_revenue=("total_revenue", "sum"),
        facility_count=("facility_count", "sum"),
    )
    grouped["public_total"] = grouped["medicaid_total"] + grouped["medicare_total"]
    grouped["comprehensive_total"] = grouped["public_total"] + grouped["private_total"]
    grouped["public_dependency"] = _safe_divide(grouped["public_total"], grouped["total_revenue"])
    grouped["private_dependency"] = _safe_divide(grouped["private_total"], grouped["total_revenue"])
    grouped["private_carrier_administered_dependency"] = _safe_divide(
        grouped["private_carrier_administered_total"], grouped["total_revenue"]
    )
    grouped["comprehensive_dependency"] = _safe_divide(grouped["comprehensive_total"], grouped["total_revenue"])
    grouped = _apply_scope_projection(grouped, payer_scope=payer_scope, taxonomy_view=taxonomy_view)
    grouped = grouped.sort_values("fiscal_year")
    return [_round_record(rec) for rec in grouped.to_dict(orient="records")]


def get_state_trend(
    store: DataStore,
    state_code: str,
    payer_scope: str = "public_only",
    taxonomy_view: str = "funding_source",
) -> list[dict[str, Any]]:
    df = store.state_summary[store.state_summary["state_code"] == state_code.upper()].copy()
    if df.empty:
        return []
    df = _apply_scope_projection(df, payer_scope=payer_scope, taxonomy_view=taxonomy_view)
    df = df.sort_values("fiscal_year")
    return [_round_record(rec) for rec in df.to_dict(orient="records")]


def get_chain_trend(
    store: DataStore,
    chain_name: str,
    state_code: Optional[str],
    payer_scope: str = "public_only",
    taxonomy_view: str = "funding_source",
) -> list[dict[str, Any]]:
    if state_code:
        df = store.facilities[
            (store.facilities["state_code"] == state_code.upper()) & (store.facilities["chain_name"] == chain_name)
        ].copy()
        if df.empty:
            return []
        grouped = _build_chain_summary_from_facilities(df)
        grouped["chain_name"] = chain_name
        grouped = _apply_scope_projection(grouped, payer_scope=payer_scope, taxonomy_view=taxonomy_view)
        grouped = grouped.sort_values("fiscal_year")
        return [_round_record(rec) for rec in grouped.to_dict(orient="records")]

    df = store.chain_summary[store.chain_summary["chain_name"] == chain_name].copy()
    if df.empty:
        return []
    df = _apply_scope_projection(df, payer_scope=payer_scope, taxonomy_view=taxonomy_view)
    df = df.sort_values("fiscal_year")
    return [_round_record(rec) for rec in df.to_dict(orient="records")]


def get_facility_trend(store: DataStore, facility_id: str) -> list[dict[str, Any]]:
    df = store.facilities[store.facilities["facility_id"] == str(facility_id)].copy()
    if df.empty:
        return []
    keep_cols = [
        "facility_id",
        "facility_name",
        "state_code",
        "chain_name",
        "fiscal_year",
        "total_revenue",
        "medicare_revenue",
        "medicaid_revenue",
        "federal_medicaid_revenue",
        "state_medicaid_revenue",
        "private_revenue",
        "uninsured_other_revenue",
        "other_revenue",
        "medicare_dependency",
        "medicaid_dependency",
        "public_dependency",
        "private_dependency",
        "comprehensive_dependency",
    ]
    out = df[[c for c in keep_cols if c in df.columns]].sort_values("fiscal_year")
    return [_round_record(rec) for rec in out.to_dict(orient="records")]


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
            private_total=("private_revenue", "sum"),
            total_revenue=("total_revenue", "sum"),
            facility_count=("facility_id", "nunique"),
        )
        .sort_values("medicaid_total", ascending=False)
    )

    chain_row["states"] = [_round_record(rec) for rec in state_breakdown.to_dict(orient="records")]
    return chain_row


def _nearest_market_year(df: pd.DataFrame, year: int) -> Optional[int]:
    if df.empty:
        return None
    years = sorted(df["fiscal_year"].dropna().astype(int).unique().tolist())
    if not years:
        return None

    less_or_equal = [y for y in years if y <= int(year)]
    if less_or_equal:
        return max(less_or_equal)
    return min(years)


def get_state_insurers(
    store: DataStore,
    state_code: str,
    year: int,
    basis: str = "covered_lives",
    top_n: int = 25,
) -> list[dict[str, Any]]:
    df = store.state_insurer_market.copy()
    if df.empty:
        return []

    subset = df[df["state_code"] == state_code.upper()].copy()
    if subset.empty:
        return []

    source_year = _nearest_market_year(subset, year)
    if source_year is None:
        return []

    subset = subset[subset["fiscal_year"].astype(int) == int(source_year)].copy()
    share_col = MARKET_SHARE_BASIS_TO_COLUMN.get(basis, "market_share_lives")
    if share_col not in subset.columns:
        share_col = "market_share_lives"

    subset["market_share"] = pd.to_numeric(subset[share_col], errors="coerce").fillna(0.0)
    subset = subset.sort_values("market_share", ascending=False)

    out = subset.head(max(int(top_n), 1)).copy()
    out["requested_fiscal_year"] = int(year)
    out["source_fiscal_year"] = int(source_year)
    out["basis"] = basis

    keep = [
        "state_code",
        "insurer_id",
        "insurer_name",
        "covered_lives",
        "premium",
        "claims",
        "market_share",
        "market_share_lives",
        "market_share_premium",
        "market_share_claims",
        "requested_fiscal_year",
        "source_fiscal_year",
        "source_file_year",
        "confidence_tier",
        "data_source",
        "basis",
    ]

    return [_round_record(rec) for rec in out[keep].to_dict(orient="records")]


def _state_override_weight(store: DataStore, state_code: str, year: int, overrides: dict[str, float], basis: str) -> float:
    if not overrides:
        return 0.0

    state_rows = get_state_insurers(store, state_code=state_code, year=year, basis=basis, top_n=500)
    if not state_rows:
        return 0.0

    share_by_insurer: dict[str, float] = {}
    for row in state_rows:
        key = str(row.get("insurer_id") or "")
        if not key:
            continue
        share_by_insurer[key] = float(row.get("market_share", 0) or 0)

    weighted = 0.0
    for insurer_id, pct in overrides.items():
        pct_num = max(0.0, min(float(pct), 100.0))
        weighted += share_by_insurer.get(str(insurer_id), 0.0) * (pct_num / 100.0)

    return max(0.0, min(weighted, 1.0))


def _allocate_exposure_from_state_shares(
    store: DataStore,
    state_code: str,
    year: int,
    private_amount: float,
    basis: str,
    top_n: int,
) -> list[dict[str, Any]]:
    private_amount = float(private_amount or 0)
    if private_amount <= 0:
        return []

    rows = get_state_insurers(store, state_code=state_code, year=year, basis=basis, top_n=top_n)
    if not rows:
        return [
            {
                "state_code": state_code.upper(),
                "insurer_id": "UNATTRIBUTED",
                "insurer_name": "Unattributed Private",
                "market_share": 1.0,
                "estimated_revenue": round(private_amount, 2),
                "source_fiscal_year": int(year),
                "confidence_tier": "C",
                "basis": basis,
            }
        ]

    exposures = []
    covered_share = 0.0
    for row in rows:
        share = float(row.get("market_share", 0) or 0)
        if share <= 0:
            continue
        amount = private_amount * share
        covered_share += share
        exposures.append(
            {
                "state_code": state_code.upper(),
                "insurer_id": str(row.get("insurer_id", "")),
                "insurer_name": str(row.get("insurer_name", "Unknown")),
                "market_share": share,
                "estimated_revenue": round(amount, 2),
                "source_fiscal_year": int(row.get("source_fiscal_year", year) or year),
                "confidence_tier": str(row.get("confidence_tier", "B") or "B"),
                "basis": basis,
            }
        )

    residual = max(0.0, 1.0 - covered_share)
    if residual > 1e-9:
        exposures.append(
            {
                "state_code": state_code.upper(),
                "insurer_id": "OTHER",
                "insurer_name": "Other / Long Tail",
                "market_share": residual,
                "estimated_revenue": round(private_amount * residual, 2),
                "source_fiscal_year": int(rows[0].get("source_fiscal_year", year) or year),
                "confidence_tier": "C",
                "basis": basis,
            }
        )

    exposures.sort(key=lambda r: float(r.get("estimated_revenue", 0)), reverse=True)
    return exposures


def get_chain_insurer_exposure(
    store: DataStore,
    chain_name: str,
    year: int,
    basis: str = "covered_lives",
    state_code: Optional[str] = None,
    top_n: int = 20,
) -> Optional[dict[str, Any]]:
    df = select_year(store.facilities, year)
    df = df[df["chain_name"] == chain_name].copy()
    if state_code:
        df = df[df["state_code"] == state_code.upper()]

    if df.empty:
        return None

    private_total = float(df["private_revenue"].sum())
    by_state = df.groupby("state_code", as_index=False)["private_revenue"].sum()

    allocations: list[dict[str, Any]] = []
    for _, row in by_state.iterrows():
        allocations.extend(
            _allocate_exposure_from_state_shares(
                store,
                state_code=str(row["state_code"]),
                year=year,
                private_amount=float(row["private_revenue"]),
                basis=basis,
                top_n=top_n,
            )
        )

    if not allocations:
        return {
            "chain_name": chain_name,
            "fiscal_year": int(year),
            "state_code": state_code.upper() if state_code else None,
            "private_total": round(private_total, 2),
            "basis": basis,
            "insurers": [],
        }

    alloc_df = pd.DataFrame(allocations)
    grouped = alloc_df.groupby(["insurer_id", "insurer_name"], as_index=False).agg(
        estimated_revenue=("estimated_revenue", "sum")
    )
    grouped["exposure_pct"] = _safe_divide(grouped["estimated_revenue"], np.array([private_total] * len(grouped)))
    grouped = grouped.sort_values("estimated_revenue", ascending=False)

    insurer_rows = [_round_record(rec) for rec in grouped.to_dict(orient="records")]

    return {
        "chain_name": chain_name,
        "fiscal_year": int(year),
        "state_code": state_code.upper() if state_code else None,
        "private_total": round(private_total, 2),
        "basis": basis,
        "insurers": insurer_rows,
    }


def get_facility_insurer_exposure(
    store: DataStore,
    facility_id: str,
    year: int,
    basis: str = "covered_lives",
    top_n: int = 15,
) -> Optional[dict[str, Any]]:
    df = select_year(store.facilities, year)
    row = df[df["facility_id"] == str(facility_id)]
    if row.empty:
        return None

    rec = row.iloc[0]
    private_amount = float(rec.get("private_revenue", 0.0) or 0.0)
    state_code = str(rec.get("state_code", ""))

    exposures = _allocate_exposure_from_state_shares(
        store,
        state_code=state_code,
        year=year,
        private_amount=private_amount,
        basis=basis,
        top_n=top_n,
    )

    total = sum(float(r.get("estimated_revenue", 0.0)) for r in exposures)
    for item in exposures:
        item["exposure_pct"] = 0.0 if total <= 0 else round(float(item["estimated_revenue"]) / total, 6)

    return {
        "facility_id": str(rec.get("facility_id", "")),
        "facility_name": str(rec.get("facility_name", "")),
        "state_code": state_code,
        "fiscal_year": int(year),
        "private_total": round(private_amount, 2),
        "basis": basis,
        "insurers": [_round_record(r) for r in exposures],
    }


def apply_scenario(
    df: pd.DataFrame,
    medicare_cut_pct: float,
    federal_cut_pct: float,
    state_cut_pct: float,
    private_cut_pct: float,
    payer_scope: str,
    private_state_factors: Optional[dict[str, float]] = None,
) -> pd.DataFrame:
    out = df.copy()
    medicare_factor = 1 - (medicare_cut_pct / 100.0)
    federal_factor = 1 - (federal_cut_pct / 100.0)
    state_factor = 1 - (state_cut_pct / 100.0)
    base_private_factor = 1 - (private_cut_pct / 100.0)

    out["shocked_medicare_revenue"] = out["medicare_revenue"] * medicare_factor
    out["shocked_federal_medicaid_revenue"] = out["federal_medicaid_revenue"] * federal_factor
    out["shocked_state_medicaid_revenue"] = out["state_medicaid_revenue"] * state_factor
    out["shocked_medicaid_revenue"] = out["shocked_federal_medicaid_revenue"] + out["shocked_state_medicaid_revenue"]

    if payer_scope == "comprehensive":
        state_factors = private_state_factors or {}
        per_row_factor = out["state_code"].map(lambda s: state_factors.get(str(s).upper(), base_private_factor))
        out["shocked_private_revenue"] = out["private_revenue"] * per_row_factor
    else:
        out["shocked_private_revenue"] = out["private_revenue"]

    out["baseline_total_revenue"] = out["total_revenue"]
    out["shocked_total_revenue"] = (
        out["uninsured_other_revenue"] + out["shocked_private_revenue"] + out["shocked_medicare_revenue"] + out["shocked_medicaid_revenue"]
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
    private_cut_pct: float,
    payer_scope: str,
    taxonomy_view: str,
    market_share_basis: str,
    insurer_cut_overrides: Optional[dict[str, float]],
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
            "payer_scope": payer_scope,
            "taxonomy_view": taxonomy_view,
            "baseline_total_revenue": 0,
            "shocked_total_revenue": 0,
            "revenue_at_risk_abs": 0,
            "revenue_at_risk_pct": 0,
            "top_impacted_facilities": [],
        }

    state_factors: dict[str, float] = {}
    base_private_factor = 1 - (private_cut_pct / 100.0)
    if payer_scope == "comprehensive":
        states = sorted(df["state_code"].dropna().astype(str).str.upper().unique().tolist())
        for state in states:
            override_weight = _state_override_weight(
                store,
                state_code=state,
                year=year,
                overrides=insurer_cut_overrides or {},
                basis=market_share_basis,
            )
            combined_factor = base_private_factor * (1 - override_weight)
            state_factors[state] = max(0.0, min(combined_factor, 1.0))

    sim = apply_scenario(
        df,
        medicare_cut_pct,
        federal_medicaid_cut_pct,
        state_medicaid_cut_pct,
        private_cut_pct,
        payer_scope,
        private_state_factors=state_factors,
    )

    baseline_total = float(sim["baseline_total_revenue"].sum())
    shocked_total = float(sim["shocked_total_revenue"].sum())
    risk_abs = baseline_total - shocked_total
    risk_pct = (risk_abs / baseline_total) if baseline_total > 0 else 0

    top = (
        sim[[
            "facility_id",
            "facility_name",
            "state_code",
            "baseline_total_revenue",
            "shocked_total_revenue",
            "revenue_at_risk_abs",
            "revenue_at_risk_pct",
        ]]
        .sort_values("revenue_at_risk_abs", ascending=False)
        .head(25)
    )

    return {
        "fiscal_year": int(year),
        "scope_state_code": state_code.upper() if state_code else None,
        "scope_chain_name": chain_name,
        "payer_scope": payer_scope,
        "taxonomy_view": taxonomy_view,
        "market_share_basis": market_share_basis,
        "baseline_total_revenue": round(baseline_total, 2),
        "shocked_total_revenue": round(shocked_total, 2),
        "revenue_at_risk_abs": round(risk_abs, 2),
        "revenue_at_risk_pct": round(risk_pct, 6),
        "top_impacted_facilities": [_round_record(rec) for rec in top.to_dict(orient="records")],
    }
