#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT / "data" / "manual" / "emma_issuer_map.csv"

REQUIRED_COLUMNS = {
    "facility_id",
    "facility_name",
    "state_code",
    "emma_issuer_id",
    "emma_issuer_name",
    "emma_issuer_url",
    "emma_mapping_status",
    "emma_mapping_method",
    "emma_match_score",
}
VALID_STATUS = {"mapped", "review_required", "unmapped", "not_applicable"}


def main() -> None:
    if not MAP_PATH.exists():
        raise FileNotFoundError(f"Missing mapping file: {MAP_PATH}")

    df = pd.read_csv(MAP_PATH, dtype=str).fillna("")
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    issues: list[str] = []

    dupes = df[df.duplicated(subset=["facility_id"], keep=False)]
    if not dupes.empty:
        issues.append(f"Duplicate facility_id rows: {dupes['facility_id'].nunique()}")

    bad_status = sorted(set(df.loc[~df["emma_mapping_status"].isin(VALID_STATUS), "emma_mapping_status"].tolist()))
    if bad_status:
        issues.append(f"Invalid mapping statuses: {bad_status}")

    mapped_missing_url = df[(df["emma_mapping_status"] == "mapped") & (df["emma_issuer_url"].str.strip() == "")]
    if not mapped_missing_url.empty:
        issues.append(f"Mapped rows missing URL: {len(mapped_missing_url)}")

    mapped_missing_id = df[(df["emma_mapping_status"] == "mapped") & (df["emma_issuer_id"].str.strip() == "")]
    if not mapped_missing_id.empty:
        issues.append(f"Mapped rows missing issuer id: {len(mapped_missing_id)}")

    if issues:
        print("EMMA mapping validation failed:")
        for issue in issues:
            print(f" - {issue}")
        raise SystemExit(1)

    counts = df["emma_mapping_status"].value_counts(dropna=False)
    print("EMMA mapping validation passed")
    print(counts.to_string())


if __name__ == "__main__":
    main()
