from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO_PATH = ROOT / "data" / "manual" / "portfolio_holdings.csv"


@dataclass
class PortfolioStore:
    holdings_df: pd.DataFrame

    @classmethod
    def load(cls, path: Path = PORTFOLIO_PATH) -> "PortfolioStore":
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(columns=["portfolio_id", "cusip9", "position_par", "market_value", "as_of_date"]).to_csv(path, index=False)
            return cls(holdings_df=pd.DataFrame(columns=["portfolio_id", "cusip9", "position_par", "market_value", "as_of_date"]))

        df = pd.read_csv(path, dtype=str).fillna("")
        if df.empty:
            return cls(holdings_df=pd.DataFrame(columns=["portfolio_id", "cusip9", "position_par", "market_value", "as_of_date"]))

        if "portfolio_id" not in df.columns:
            df["portfolio_id"] = "default"
        if "cusip9" not in df.columns:
            df["cusip9"] = ""

        for col in ["position_par", "market_value", "as_of_date"]:
            if col not in df.columns:
                df[col] = ""

        df["portfolio_id"] = df["portfolio_id"].replace("", "default")
        df["cusip9"] = df["cusip9"].map(_normalize_cusip)
        df = df[df["cusip9"] != ""].copy()
        return cls(holdings_df=df[["portfolio_id", "cusip9", "position_par", "market_value", "as_of_date"]])

    def reload(self, path: Path = PORTFOLIO_PATH) -> None:
        self.holdings_df = self.load(path).holdings_df

    def holdings_for_portfolio(self, portfolio_id: str) -> list[dict[str, Any]]:
        pid = (portfolio_id or "default").strip() or "default"
        subset = self.holdings_df[self.holdings_df["portfolio_id"] == pid].copy()
        if subset.empty and pid != "default":
            subset = self.holdings_df[self.holdings_df["portfolio_id"] == "default"].copy()
        return subset.to_dict(orient="records")

    def cusips_for_portfolio(self, portfolio_id: str) -> set[str]:
        rows = self.holdings_for_portfolio(portfolio_id)
        return {str(r.get("cusip9", "")).strip() for r in rows if str(r.get("cusip9", "")).strip()}

    def summary(self) -> dict[str, Any]:
        if self.holdings_df.empty:
            return {
                "portfolio_count": 0,
                "holding_count": 0,
                "unique_cusip_count": 0,
                "portfolios": [],
            }

        grouped = self.holdings_df.groupby("portfolio_id", as_index=False).agg(
            holdings=("cusip9", "count"),
            unique_cusips=("cusip9", "nunique"),
        )
        grouped = grouped.sort_values(["holdings", "portfolio_id"], ascending=[False, True])
        return {
            "portfolio_count": int(grouped.shape[0]),
            "holding_count": int(self.holdings_df.shape[0]),
            "unique_cusip_count": int(self.holdings_df["cusip9"].nunique()),
            "portfolios": grouped.to_dict(orient="records"),
        }


def _normalize_cusip(value: str) -> str:
    raw = "".join(ch for ch in str(value or "").upper() if ch.isalnum())
    if len(raw) < 6:
        return ""
    return raw[:9]
