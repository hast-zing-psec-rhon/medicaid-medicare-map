#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_PATH = ROOT / "data" / "processed" / "facilities.csv"
OUTPUT_PATH = ROOT / "data" / "manual" / "emma_issuer_map.csv"
SEARCH_AHEAD_URL = "https://emma.msrb.org/QuickSearch/SearchAhead"

OUTPUT_COLUMNS = [
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

HEALTHCARE_TERMS = {
    "hospital",
    "medical",
    "health",
    "healthcare",
    "clinic",
    "university medical",
    "rehabilitation",
    "children",
    "nursing",
}


@dataclass
class Candidate:
    issuer_id: str
    issuer_name: str
    issuer_url: str
    score: float


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]+", " ", str(s or "").upper())).strip()


def _strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", html.unescape(str(s or ""))).strip()


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(a=_normalize(a), b=_normalize(b)).ratio()


def _state_bonus(candidate_name: str, state_code: str) -> float:
    if not state_code:
        return 0.0
    tokens = set(_normalize(candidate_name).split())
    return 1.0 if state_code.upper() in tokens else 0.0


def _healthcare_bonus(candidate_name: str) -> float:
    up = _normalize(candidate_name)
    return 1.0 if any(term.upper() in up for term in HEALTHCARE_TERMS) else 0.0


def _extract_issuer_id(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query)
    return str(qs.get("key", [""])[0]).strip()


def fetch_search_candidates(query: str, timeout: int = 30) -> list[dict]:
    payload = json.dumps({"searchText": query}).encode("utf-8")
    req = urllib.request.Request(
        SEARCH_AHEAD_URL,
        data=payload,
        headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json; charset=utf-8"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return data


def score_candidates(facility_name: str, state_code: str, rows: list[dict]) -> list[Candidate]:
    scored: list[Candidate] = []
    for row in rows:
        if str(row.get("ResultType", "")).lower() != "issuer":
            continue

        raw_name = _strip_tags(str(row.get("Text", "")))
        raw_url = str(row.get("Url", ""))
        issuer_id = _extract_issuer_id(raw_url)
        if not issuer_id:
            continue

        ratio = _similarity(facility_name, raw_name)
        state = _state_bonus(raw_name, state_code)
        health = _healthcare_bonus(raw_name)
        score = (0.55 * ratio) + (0.25 * state) + (0.20 * health)

        issuer_url = f"https://emma.msrb.org/IssuerHomePage/Issuer?id={issuer_id}"
        scored.append(
            Candidate(
                issuer_id=issuer_id,
                issuer_name=raw_name,
                issuer_url=issuer_url,
                score=float(np.clip(score, 0.0, 1.0)),
            )
        )

    scored.sort(key=lambda c: c.score, reverse=True)
    return scored


def derive_mapping_row(facility_id: str, facility_name: str, state_code: str, candidates: list[Candidate]) -> dict[str, str | float]:
    if not candidates:
        return {
            "facility_id": facility_id,
            "facility_name": facility_name,
            "state_code": state_code,
            "emma_issuer_id": "",
            "emma_issuer_name": "",
            "emma_issuer_url": "",
            "emma_mapping_status": "unmapped",
            "emma_mapping_method": "none",
            "emma_match_score": 0.0,
            "reviewed_by": "",
            "reviewed_at_utc": "",
            "notes": "No issuer candidates returned by EMMA search-ahead",
        }

    top = candidates[0]
    second = candidates[1] if len(candidates) > 1 else None
    margin = top.score - second.score if second else top.score

    auto_high_conf = top.score >= 0.88 and margin >= 0.05
    return {
        "facility_id": facility_id,
        "facility_name": facility_name,
        "state_code": state_code,
        "emma_issuer_id": top.issuer_id,
        "emma_issuer_name": top.issuer_name,
        "emma_issuer_url": top.issuer_url,
        "emma_mapping_status": "mapped" if auto_high_conf else "review_required",
        "emma_mapping_method": "auto_high_conf" if auto_high_conf else "auto_review",
        "emma_match_score": round(top.score, 6),
        "reviewed_by": "",
        "reviewed_at_utc": "",
        "notes": "" if auto_high_conf else f"Needs QA. Top-vs-second margin={margin:.4f}",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate EMMA issuer mapping candidates for NFP facilities")
    parser.add_argument("--year", type=int, default=None, help="Fiscal year to map (default: latest)")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of facilities")
    parser.add_argument("--sleep-ms", type=int, default=75, help="Delay between requests in milliseconds")
    args = parser.parse_args()

    if not PROCESSED_PATH.exists():
        raise FileNotFoundError(f"Missing facilities file: {PROCESSED_PATH}. Run ETL first.")

    df = pd.read_csv(PROCESSED_PATH, dtype={"facility_id": str, "state_code": str})
    year = int(args.year) if args.year is not None else int(df["fiscal_year"].max())

    nfp = df[(df["fiscal_year"].astype(int) == year) & (df["ownership_group"] == "not_for_profit")].copy()
    nfp = nfp.sort_values(["state_code", "facility_name"]).drop_duplicates(subset=["facility_id"], keep="first")

    if args.limit:
        nfp = nfp.head(int(args.limit))

    rows: list[dict[str, str | float]] = []
    delay = max(args.sleep_ms, 0) / 1000.0

    print(f"Generating EMMA mapping candidates for FY {year} NFP facilities: {len(nfp)} rows")
    for i, rec in enumerate(nfp.itertuples(index=False), start=1):
        query = str(rec.facility_name or "").strip()
        if not query:
            rows.append(
                {
                    "facility_id": str(rec.facility_id),
                    "facility_name": "",
                    "state_code": str(rec.state_code or ""),
                    "emma_issuer_id": "",
                    "emma_issuer_name": "",
                    "emma_issuer_url": "",
                    "emma_mapping_status": "unmapped",
                    "emma_mapping_method": "none",
                    "emma_match_score": 0.0,
                    "reviewed_by": "",
                    "reviewed_at_utc": "",
                    "notes": "Missing facility name",
                }
            )
            continue

        try:
            search_rows = fetch_search_candidates(query)
            candidates = score_candidates(query, str(rec.state_code or ""), search_rows)
            rows.append(derive_mapping_row(str(rec.facility_id), query, str(rec.state_code or ""), candidates))
        except Exception as exc:  # pragma: no cover - network variability
            rows.append(
                {
                    "facility_id": str(rec.facility_id),
                    "facility_name": query,
                    "state_code": str(rec.state_code or ""),
                    "emma_issuer_id": "",
                    "emma_issuer_name": "",
                    "emma_issuer_url": "",
                    "emma_mapping_status": "review_required",
                    "emma_mapping_method": "auto_review",
                    "emma_match_score": 0.0,
                    "reviewed_by": "",
                    "reviewed_at_utc": "",
                    "notes": f"Search error: {exc}",
                }
            )

        if i % 25 == 0:
            print(f"  processed {i}/{len(nfp)}")
        if delay > 0:
            time.sleep(delay)

    out_df = pd.DataFrame(rows)
    for col in OUTPUT_COLUMNS:
        if col not in out_df.columns:
            out_df[col] = ""
    out_df = out_df[OUTPUT_COLUMNS]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUTPUT_PATH.exists():
        existing = pd.read_csv(OUTPUT_PATH, dtype=str).fillna("")
        if not existing.empty and "facility_id" in existing.columns:
            existing = existing.drop_duplicates(subset=["facility_id"], keep="last")
            out_df = out_df.drop_duplicates(subset=["facility_id"], keep="last")
            combined = pd.concat([existing, out_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["facility_id"], keep="last")
            out_df = combined

    out_df.to_csv(OUTPUT_PATH, index=False)

    mapped = int((out_df["emma_mapping_status"] == "mapped").sum())
    review = int((out_df["emma_mapping_status"] == "review_required").sum())
    unmapped = int((out_df["emma_mapping_status"] == "unmapped").sum())
    print(f"Wrote {len(out_df)} rows to {OUTPUT_PATH}")
    print(f"mapped={mapped}, review_required={review}, unmapped={unmapped}")


if __name__ == "__main__":
    main()
