from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FALLBACK_PATH = ROOT / "data" / "manual" / "emma_cusip_fallback_map.csv"

FALLBACK_COLUMNS = [
    "facility_id",
    "fiscal_year",
    "facility_name",
    "state_code",
    "emma_fallback_status",
    "emma_fallback_type",
    "emma_fallback_url",
    "emma_fallback_search_term",
    "emma_fallback_cusip_query",
    "emma_fallback_cusip9",
    "emma_fallback_issue_id",
    "emma_fallback_issue_desc",
    "emma_fallback_issuer_name",
    "emma_fallback_match_basis",
    "emma_fallback_outstanding_filter_applied",
    "emma_fallback_error",
    "searched_at_utc",
]


@dataclass
class EmmaFallbackStore:
    fallback_df: pd.DataFrame

    @classmethod
    def load(cls, path: Path = FALLBACK_PATH) -> "EmmaFallbackStore":
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(columns=FALLBACK_COLUMNS).to_csv(path, index=False)
            return cls(fallback_df=pd.DataFrame(columns=FALLBACK_COLUMNS))

        df = pd.read_csv(path, dtype=str).fillna("")
        for col in FALLBACK_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        if df.empty:
            return cls(fallback_df=df[FALLBACK_COLUMNS].copy())

        df["facility_id"] = df["facility_id"].astype(str).str.zfill(6)
        df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").fillna(0).astype(int)
        return cls(fallback_df=df[FALLBACK_COLUMNS].copy())

    def reload(self, path: Path = FALLBACK_PATH) -> None:
        self.fallback_df = self.load(path).fallback_df

    def lookup(self, facility_id: str, fiscal_year: int) -> Optional[dict[str, Any]]:
        if self.fallback_df.empty:
            return None
        fid = str(facility_id or "").zfill(6)
        subset = self.fallback_df[self.fallback_df["facility_id"] == fid].copy()
        if subset.empty:
            return None

        subset["year_distance"] = (subset["fiscal_year"] - int(fiscal_year)).abs()
        subset = subset.sort_values(["year_distance", "searched_at_utc"], ascending=[True, False])
        row = subset.iloc[0].to_dict()
        row.pop("year_distance", None)
        row["emma_fallback_outstanding_filter_applied"] = str(row.get("emma_fallback_outstanding_filter_applied", "")).lower() in {
            "1",
            "true",
            "yes",
        }
        return row
