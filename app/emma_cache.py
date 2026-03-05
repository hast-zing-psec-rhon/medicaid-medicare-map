from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = ROOT / "data" / "processed" / "emma_cache.db"


@dataclass
class CacheRecord:
    cache_key: str
    issuer_id: str
    portfolio_id: str
    fetched_at_utc: str
    expires_at_utc: str
    scrape_status: str
    scrape_error: str
    payload: dict[str, Any]

    @property
    def is_fresh(self) -> bool:
        now = datetime.now(tz=UTC)
        try:
            exp = datetime.fromisoformat(self.expires_at_utc)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=UTC)
            return exp > now
        except Exception:
            return False


class EmmaCache:
    def __init__(self, db_path: Path = CACHE_PATH, ttl_hours: int = 24) -> None:
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS issuer_cache (
                    cache_key TEXT PRIMARY KEY,
                    issuer_id TEXT NOT NULL,
                    portfolio_id TEXT NOT NULL,
                    fetched_at_utc TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    scrape_status TEXT NOT NULL,
                    scrape_error TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS issuer_securities (
                    cache_key TEXT NOT NULL,
                    issuer_id TEXT NOT NULL,
                    cusip9 TEXT,
                    issue_description TEXT,
                    maturity_date TEXT,
                    coupon TEXT,
                    security_status TEXT,
                    raw_json TEXT,
                    PRIMARY KEY (cache_key, issuer_id, cusip9)
                );

                CREATE TABLE IF NOT EXISTS issuer_documents (
                    cache_key TEXT NOT NULL,
                    issuer_id TEXT NOT NULL,
                    document_id TEXT,
                    document_type TEXT,
                    posting_date TEXT,
                    title TEXT,
                    related_cusip9 TEXT,
                    document_url TEXT,
                    raw_json TEXT,
                    PRIMARY KEY (cache_key, issuer_id, document_id)
                );
                """
            )

    def make_cache_key(self, issuer_id: str, portfolio_id: str) -> str:
        pid = (portfolio_id or "default").strip() or "default"
        return f"{issuer_id}::{pid}"

    def get(self, issuer_id: str, portfolio_id: str) -> Optional[CacheRecord]:
        cache_key = self.make_cache_key(issuer_id, portfolio_id)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT cache_key, issuer_id, portfolio_id, fetched_at_utc, expires_at_utc,
                       scrape_status, scrape_error, payload_json
                FROM issuer_cache
                WHERE cache_key = ?
                """,
                (cache_key,),
            ).fetchone()

        if not row:
            return None
        payload = json.loads(row["payload_json"])
        return CacheRecord(
            cache_key=row["cache_key"],
            issuer_id=row["issuer_id"],
            portfolio_id=row["portfolio_id"],
            fetched_at_utc=row["fetched_at_utc"],
            expires_at_utc=row["expires_at_utc"],
            scrape_status=row["scrape_status"],
            scrape_error=row["scrape_error"],
            payload=payload,
        )

    def put(
        self,
        issuer_id: str,
        portfolio_id: str,
        payload: dict[str, Any],
        scrape_status: str,
        scrape_error: str = "",
    ) -> CacheRecord:
        now = datetime.now(tz=UTC)
        exp = now + timedelta(hours=self.ttl_hours)
        cache_key = self.make_cache_key(issuer_id, portfolio_id)

        securities = payload.get("owned_securities", []) or []
        documents = payload.get("related_documents", []) or []

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO issuer_cache(cache_key, issuer_id, portfolio_id, fetched_at_utc, expires_at_utc,
                                         payload_json, scrape_status, scrape_error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    issuer_id = excluded.issuer_id,
                    portfolio_id = excluded.portfolio_id,
                    fetched_at_utc = excluded.fetched_at_utc,
                    expires_at_utc = excluded.expires_at_utc,
                    payload_json = excluded.payload_json,
                    scrape_status = excluded.scrape_status,
                    scrape_error = excluded.scrape_error
                """,
                (
                    cache_key,
                    issuer_id,
                    (portfolio_id or "default").strip() or "default",
                    now.isoformat(),
                    exp.isoformat(),
                    json.dumps(payload),
                    scrape_status,
                    scrape_error,
                ),
            )

            conn.execute("DELETE FROM issuer_securities WHERE cache_key = ?", (cache_key,))
            conn.execute("DELETE FROM issuer_documents WHERE cache_key = ?", (cache_key,))

            for sec in securities:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO issuer_securities(
                        cache_key, issuer_id, cusip9, issue_description, maturity_date, coupon, security_status, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cache_key,
                        issuer_id,
                        str(sec.get("cusip9", "")),
                        str(sec.get("issue_description", "")),
                        str(sec.get("maturity_date", "")),
                        str(sec.get("coupon", "")),
                        str(sec.get("security_status", "")),
                        json.dumps(sec),
                    ),
                )

            for doc in documents:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO issuer_documents(
                        cache_key, issuer_id, document_id, document_type, posting_date,
                        title, related_cusip9, document_url, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        cache_key,
                        issuer_id,
                        str(doc.get("document_id", "")),
                        str(doc.get("document_type", "")),
                        str(doc.get("posting_date", "")),
                        str(doc.get("title", "")),
                        str(doc.get("related_cusip9", "")),
                        str(doc.get("document_url", "")),
                        json.dumps(doc),
                    ),
                )

        return CacheRecord(
            cache_key=cache_key,
            issuer_id=issuer_id,
            portfolio_id=(portfolio_id or "default").strip() or "default",
            fetched_at_utc=now.isoformat(),
            expires_at_utc=exp.isoformat(),
            scrape_status=scrape_status,
            scrape_error=scrape_error,
            payload=payload,
        )
