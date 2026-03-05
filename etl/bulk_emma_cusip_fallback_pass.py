#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys
from time import perf_counter, sleep

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.emma_client import EmmaClient

FACILITIES_PATH = ROOT / "data" / "processed" / "facilities.csv"
OUTPUT_PATH = ROOT / "data" / "manual" / "emma_cusip_fallback_map.csv"

OUTPUT_COLUMNS = [
    "facility_id",
    "fiscal_year",
    "facility_name",
    "state_code",
    "ownership_group",
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
    "elapsed_ms",
]


def _load_facilities(year: int | None, ownership: str) -> pd.DataFrame:
    if not FACILITIES_PATH.exists():
        raise FileNotFoundError(f"Missing facilities dataset: {FACILITIES_PATH}")
    df = pd.read_csv(FACILITIES_PATH, dtype=str).fillna("")
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").fillna(0).astype(int)
    if year is None:
        year = int(df["fiscal_year"].max())
    subset = df[df["fiscal_year"] == int(year)].copy()
    subset["facility_id"] = subset["facility_id"].astype(str).str.zfill(6)
    if ownership and ownership != "all":
        subset = subset[subset["ownership_group"] == ownership].copy()
    subset = subset.sort_values(["state_code", "facility_name"]).drop_duplicates(subset=["facility_id"], keep="first")
    return subset


def _load_existing() -> pd.DataFrame:
    if not OUTPUT_PATH.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    existing = pd.read_csv(OUTPUT_PATH, dtype=str).fillna("")
    for c in OUTPUT_COLUMNS:
        if c not in existing.columns:
            existing[c] = ""
    existing["facility_id"] = existing["facility_id"].astype(str).str.zfill(6)
    existing["fiscal_year"] = pd.to_numeric(existing["fiscal_year"], errors="coerce").fillna(0).astype(int)
    return existing[OUTPUT_COLUMNS].copy()


def _merge_and_write(existing: pd.DataFrame, updates: pd.DataFrame) -> None:
    if updates.empty:
        return
    merged = pd.concat([existing, updates], ignore_index=True)
    merged = merged.drop_duplicates(subset=["facility_id", "fiscal_year"], keep="last")
    merged = merged.sort_values(["fiscal_year", "state_code", "facility_name"])
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged[OUTPUT_COLUMNS].to_csv(OUTPUT_PATH, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk EMMA fallback pass (CUSIP/issue) for facilities")
    parser.add_argument("--year", type=int, default=None, help="Fiscal year (default: latest in facilities.csv)")
    parser.add_argument("--ownership", type=str, default="not_for_profit", help="Ownership filter (default: not_for_profit)")
    parser.add_argument("--limit", type=int, default=None, help="Optional facility limit")
    parser.add_argument("--max-issue-rows", type=int, default=8, help="Max QuickSearch issue rows per facility")
    parser.add_argument("--sleep-ms", type=int, default=80, help="Delay between facilities in ms")
    parser.add_argument("--max-retries", type=int, default=4, help="Retries when EMMA blocks requests (HTTP 403)")
    parser.add_argument("--cooldown-sec", type=int, default=90, help="Cooldown seconds before retry after HTTP 403")
    parser.add_argument("--refresh", action="store_true", help="Refresh even if facility already present in output")
    parser.add_argument(
        "--only-status",
        type=str,
        default="",
        help="Comma-separated existing statuses to refresh (e.g., error,not_found)",
    )
    parser.add_argument("--flush-every", type=int, default=25, help="Write output every N facilities")
    args = parser.parse_args()

    facilities = _load_facilities(year=args.year, ownership=args.ownership)
    if args.limit:
        facilities = facilities.head(int(args.limit))
    if facilities.empty:
        print("No facilities matched selection.")
        return

    existing = _load_existing()
    existing_keys = set(zip(existing["facility_id"].astype(str), existing["fiscal_year"].astype(int)))
    only_status = {s.strip() for s in str(args.only_status or "").split(",") if s.strip()}

    client = EmmaClient(timeout_seconds=30)
    rows: list[dict[str, object]] = []
    started = perf_counter()
    processed = 0

    print(f"Starting bulk EMMA fallback pass for {len(facilities)} facilities")
    for rec in facilities.itertuples(index=False):
        key = (str(rec.facility_id), int(rec.fiscal_year))
        if key in existing_keys:
            if not args.refresh and not only_status:
                continue
            if only_status:
                existing_row = existing[(existing["facility_id"] == key[0]) & (existing["fiscal_year"] == key[1])]
                prev_status = str(existing_row.iloc[0]["emma_fallback_status"]) if not existing_row.empty else ""
                if prev_status not in only_status:
                    continue

        t0 = perf_counter()
        fallback: dict[str, object]
        for attempt in range(max(1, int(args.max_retries)) + 1):
            try:
                fallback = client.find_emma_fallback_link(
                    facility_name=str(rec.facility_name),
                    state_code=str(rec.state_code),
                    candidate_cusips=set(),
                    max_issue_rows=int(args.max_issue_rows),
                )
            except Exception as exc:  # pragma: no cover - network variability
                fallback = {
                    "emma_fallback_status": "error",
                    "emma_fallback_type": "none",
                    "emma_fallback_url": "",
                    "emma_fallback_search_term": "",
                    "emma_fallback_cusip_query": "",
                    "emma_fallback_cusip9": "",
                    "emma_fallback_issue_id": "",
                    "emma_fallback_issue_desc": "",
                    "emma_fallback_issuer_name": "",
                    "emma_fallback_match_basis": "bulk_pass_exception",
                    "emma_fallback_outstanding_filter_applied": True,
                    "emma_fallback_error": str(exc),
                }

            err_msg = str(fallback.get("emma_fallback_error", ""))
            is_403_block = str(fallback.get("emma_fallback_status", "")) == "error" and "403" in err_msg
            if is_403_block and attempt < int(args.max_retries):
                wait_s = int(args.cooldown_sec) * (attempt + 1)
                print(
                    f"  403 block on {rec.facility_id} (attempt {attempt + 1}/{args.max_retries}); "
                    f"cooldown {wait_s}s"
                )
                sleep(wait_s)
                client = EmmaClient(timeout_seconds=30)
                continue
            break

        elapsed_ms = int((perf_counter() - t0) * 1000)
        row = {
            "facility_id": str(rec.facility_id),
            "fiscal_year": int(rec.fiscal_year),
            "facility_name": str(rec.facility_name),
            "state_code": str(rec.state_code),
            "ownership_group": str(rec.ownership_group),
            "emma_fallback_status": str(fallback.get("emma_fallback_status", "")),
            "emma_fallback_type": str(fallback.get("emma_fallback_type", "")),
            "emma_fallback_url": str(fallback.get("emma_fallback_url", "")),
            "emma_fallback_search_term": str(fallback.get("emma_fallback_search_term", "")),
            "emma_fallback_cusip_query": str(fallback.get("emma_fallback_cusip_query", "")),
            "emma_fallback_cusip9": str(fallback.get("emma_fallback_cusip9", "")),
            "emma_fallback_issue_id": str(fallback.get("emma_fallback_issue_id", "")),
            "emma_fallback_issue_desc": str(fallback.get("emma_fallback_issue_desc", "")),
            "emma_fallback_issuer_name": str(fallback.get("emma_fallback_issuer_name", "")),
            "emma_fallback_match_basis": str(fallback.get("emma_fallback_match_basis", "")),
            "emma_fallback_outstanding_filter_applied": bool(fallback.get("emma_fallback_outstanding_filter_applied", True)),
            "emma_fallback_error": str(fallback.get("emma_fallback_error", "")),
            "searched_at_utc": datetime.now(tz=UTC).isoformat(),
            "elapsed_ms": elapsed_ms,
        }
        rows.append(row)
        processed += 1

        if processed % max(1, int(args.flush_every)) == 0:
            _merge_and_write(existing, pd.DataFrame(rows))
            existing = _load_existing()
            rows = []
            matched = int((existing["emma_fallback_status"] == "found").sum())
            print(f"  processed={processed}, written={len(existing)}, found={matched}")

        if args.sleep_ms > 0:
            sleep(args.sleep_ms / 1000.0)

    if rows:
        _merge_and_write(existing, pd.DataFrame(rows))

    final_df = _load_existing()
    total_found = int((final_df["emma_fallback_status"] == "found").sum())
    total_not_found = int((final_df["emma_fallback_status"] == "not_found").sum())
    total_error = int((final_df["emma_fallback_status"] == "error").sum())
    runtime = perf_counter() - started
    print(f"Completed in {runtime:.1f}s")
    print(f"Output: {OUTPUT_PATH}")
    print(f"found={total_found}, not_found={total_not_found}, error={total_error}, total_rows={len(final_df)}")


if __name__ == "__main__":
    main()
