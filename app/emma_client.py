from __future__ import annotations

import html
import io
import json
import re
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from typing import Any

from bs4 import BeautifulSoup

try:  # pragma: no cover - optional OCR dependency
    from PIL import Image, ImageEnhance, ImageOps
except Exception:  # pragma: no cover - optional OCR dependency
    Image = None  # type: ignore[assignment]
    ImageEnhance = None  # type: ignore[assignment]
    ImageOps = None  # type: ignore[assignment]

try:  # pragma: no cover - optional OCR dependency
    import pytesseract
except Exception:  # pragma: no cover - optional OCR dependency
    pytesseract = None  # type: ignore[assignment]


class EmmaClient:
    def __init__(self, timeout_seconds: int = 25) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        self._cusip_token_cache: dict[str, str] = {}

    @staticmethod
    def issuer_profile_url(issuer_id: str) -> str:
        return f"https://emma.msrb.org/IssuerHomePage/Issuer?id={issuer_id}"

    @staticmethod
    def quick_search_transfer_url(search_text: str) -> str:
        encoded = urllib.parse.quote(str(search_text or "").strip())
        return f"https://emma.msrb.org/QuickSearch/Transfer?quickSearchText={encoded}"

    def search_ahead(self, query: str) -> list[dict[str, Any]]:
        query = str(query or "").strip()
        if not query:
            return []

        payload = json.dumps({"searchText": query}).encode("utf-8")
        req = urllib.request.Request(
            "https://emma.msrb.org/QuickSearch/SearchAhead",
            data=payload,
            headers={"User-Agent": self.user_agent, "Content-Type": "application/json; charset=utf-8"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        data = json.loads(body)
        if not isinstance(data, list):
            return []
        return data

    def search_issues(
        self,
        search_terms: str,
        state_code: str = "",
        exclude_matured: bool = True,
        exclude_called: bool = True,
        max_rows: int = 25,
    ) -> list[dict[str, Any]]:
        terms = str(search_terms or "").strip()
        if not terms:
            return []

        payload = {
            "SearchTerms": terms,
            "SearchCategory": "desc",
            "State": str(state_code or "").strip().upper(),
            "DatedDateFrom": "",
            "DatedDateTo": "",
            "MaturityDateFrom": "",
            "MaturityDateTo": "",
            "SourceOfRepayment": "",
            "TaxStatus": "",
            "ExcludeMatured": bool(exclude_matured),
            "ExcludeCompletelyCalled": bool(exclude_called),
        }
        req = urllib.request.Request(
            "https://emma.msrb.org/QuickSearch/Search",
            data=json.dumps(payload).encode("utf-8"),
            headers={"User-Agent": self.user_agent, "Content-Type": "application/json; charset=utf-8"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="ignore")

        data = json.loads(body)
        rows = data.get("Data", []) if isinstance(data, dict) else []
        if not isinstance(rows, list):
            return []

        rows = [r for r in rows if isinstance(r, dict)]
        rows.sort(key=lambda r: (-float(r.get("Score", 0.0) or 0.0), int(r.get("Rank", 10_000) or 10_000)))
        return rows[: max(1, int(max_rows))]

    def _fetch_issue_final_scale_rows(
        self,
        opener: urllib.request.OpenerDirector,
        issue_id: str,
        issue_url: str,
    ) -> list[dict[str, Any]]:
        iid = str(issue_id or "").strip()
        if not iid:
            return []
        url = f"https://emma.msrb.org/IssueView/GetFinalScaleData?id={urllib.parse.quote(iid)}"
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Referer": issue_url or f"https://emma.msrb.org/IssueView/Details/{iid}",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "https://emma.msrb.org",
            },
        )
        with opener.open(req, timeout=self.timeout_seconds) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        try:
            data = json.loads(body)
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        return [r for r in data if isinstance(r, dict)]

    def _decode_cusip_from_token(
        self,
        opener: urllib.request.OpenerDirector,
        token: str,
    ) -> str:
        raw = str(token or "").strip()
        if not raw:
            return ""
        if raw in self._cusip_token_cache:
            return self._cusip_token_cache[raw]

        normalized_raw = "".join(ch for ch in raw.upper() if ch.isalnum())
        if len(normalized_raw) == 9 and _is_valid_cusip9(normalized_raw):
            self._cusip_token_cache[raw] = normalized_raw
            return normalized_raw

        image_url = (
            "https://emma.msrb.org/ImageGenerator.ashx?"
            f"cusip9={urllib.parse.quote(raw)}&rowNum=-3&HeaderTop=false&isLink=false"
        )
        req = urllib.request.Request(image_url, headers={"User-Agent": self.user_agent})
        try:
            with opener.open(req, timeout=self.timeout_seconds) as resp:
                if "image" not in str(resp.headers.get("content-type", "")).lower():
                    self._cusip_token_cache[raw] = ""
                    return ""
                image_bytes = resp.read()
        except Exception:
            self._cusip_token_cache[raw] = ""
            return ""

        decoded = _ocr_cusip_image(image_bytes)
        self._cusip_token_cache[raw] = decoded
        return decoded

    def find_emma_fallback_link(
        self,
        facility_name: str,
        state_code: str = "",
        candidate_cusips: set[str] | None = None,
        max_issue_rows: int = 8,
    ) -> dict[str, Any]:
        normalized_facility = str(facility_name or "").strip()
        normalized_state = str(state_code or "").strip().upper()
        holdings = sorted({_normalize_cusip(c) for c in (candidate_cusips or set()) if _normalize_cusip(c)})

        result: dict[str, Any] = {
            "emma_fallback_status": "not_found",
            "emma_fallback_type": "none",
            "emma_fallback_url": "",
            "emma_fallback_search_term": "",
            "emma_fallback_cusip_query": "",
            "emma_fallback_cusip9": "",
            "emma_fallback_issue_id": "",
            "emma_fallback_issue_desc": "",
            "emma_fallback_issuer_name": "",
            "emma_fallback_match_basis": "",
            "emma_fallback_outstanding_filter_applied": True,
            "emma_fallback_error": "",
        }

        if not normalized_facility:
            result["emma_fallback_match_basis"] = "missing_facility_name"
            return result

        issue_rows: list[dict[str, Any]] = []
        issue_search_terms = _build_issue_search_terms(normalized_facility)
        for term in issue_search_terms:
            try:
                rows = self.search_issues(
                    search_terms=term,
                    state_code=normalized_state,
                    exclude_matured=True,
                    exclude_called=True,
                    max_rows=max_issue_rows,
                )
            except Exception as exc:  # pragma: no cover - network variability
                result["emma_fallback_status"] = "error"
                result["emma_fallback_error"] = f"QuickSearch fallback lookup failed: {exc}"
                return result
            if rows:
                issue_rows = rows
                result["emma_fallback_search_term"] = term
                break

        if not issue_rows:
            result["emma_fallback_match_basis"] = "no_active_issue_candidates"
            return result

        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))
        first_issue_fallback: dict[str, Any] | None = None
        first_cusip6: str = ""

        for row in issue_rows:
            issue_desc = _strip_tags(str(row.get("IssueDesc", "")))
            issuer_name = _strip_tags(str(row.get("IssuerName", "")))
            issue_id = str(row.get("IssueId", "") or "").strip()
            candidate_issue_url = str(row.get("IssueUrl", "") or "").strip().replace("\\u0026", "&")
            issue_url = self._resolve_url(opener, candidate_issue_url) if candidate_issue_url else ""
            issue_id = issue_id or _extract_issue_id(issue_url)

            explicit_cusips = [_normalize_cusip(c) for c in _extract_probable_cusips(issue_desc)]
            explicit_cusips = [c for c in explicit_cusips if c]

            # Best-case: active issue candidate includes a directly parseable CUSIP.
            if explicit_cusips:
                cusip9 = explicit_cusips[0]
                result.update(
                    {
                        "emma_fallback_status": "found",
                        "emma_fallback_type": "cusip",
                        "emma_fallback_url": self.quick_search_transfer_url(cusip9),
                        "emma_fallback_cusip_query": cusip9,
                        "emma_fallback_cusip9": cusip9,
                        "emma_fallback_issue_id": issue_id,
                        "emma_fallback_issue_desc": issue_desc,
                        "emma_fallback_issuer_name": issuer_name,
                        "emma_fallback_match_basis": "active_issue_description_cusip_match",
                    }
                )
                return result

            parsed_cusip6 = ""
            parsed_non_cusip9: list[str] = []
            final_scale_cusips: list[str] = []
            if issue_url:
                try:
                    issue_html = self._fetch_with_disclaimer_accept(opener, issue_url)
                    parsed_cusip6 = _normalize_cusip(_extract_js_function_return(issue_html, "getCusip6"))[:6]
                    parsed_non_cusip9 = _extract_noncusip_security_cusips(issue_html)
                    final_scale_rows = self._fetch_issue_final_scale_rows(opener, issue_id=issue_id, issue_url=issue_url)
                    for sec in final_scale_rows[:24]:
                        token = str(sec.get("Cusip9Enc", "")).strip()
                        if not token:
                            continue
                        decoded = self._decode_cusip_from_token(opener, token)
                        if decoded and decoded not in final_scale_cusips:
                            final_scale_cusips.append(decoded)
                except Exception:
                    parsed_cusip6 = ""
                    parsed_non_cusip9 = []
                    final_scale_cusips = []

            if final_scale_cusips:
                selected = ""
                if holdings:
                    selected = next((c for c in final_scale_cusips if c in holdings), "")
                if (not selected) and parsed_cusip6:
                    selected = next((c for c in final_scale_cusips if c.startswith(parsed_cusip6)), "")
                if not selected:
                    selected = final_scale_cusips[0]
                result.update(
                    {
                        "emma_fallback_status": "found",
                        "emma_fallback_type": "cusip",
                        "emma_fallback_url": self.quick_search_transfer_url(selected),
                        "emma_fallback_cusip_query": selected,
                        "emma_fallback_cusip9": selected,
                        "emma_fallback_issue_id": issue_id,
                        "emma_fallback_issue_desc": issue_desc,
                        "emma_fallback_issuer_name": issuer_name,
                        "emma_fallback_match_basis": "active_issue_final_scale_ocr",
                    }
                )
                return result

            if parsed_non_cusip9:
                cusip9 = parsed_non_cusip9[0]
                result.update(
                    {
                        "emma_fallback_status": "found",
                        "emma_fallback_type": "cusip",
                        "emma_fallback_url": self.quick_search_transfer_url(cusip9),
                        "emma_fallback_cusip_query": cusip9,
                        "emma_fallback_cusip9": cusip9,
                        "emma_fallback_issue_id": issue_id,
                        "emma_fallback_issue_desc": issue_desc,
                        "emma_fallback_issuer_name": issuer_name,
                        "emma_fallback_match_basis": "active_issue_page_noncusip_table",
                    }
                )
                return result

            if parsed_cusip6 and holdings:
                portfolio_match = next((h for h in holdings if h.startswith(parsed_cusip6)), "")
                if portfolio_match:
                    result.update(
                        {
                        "emma_fallback_status": "found",
                        "emma_fallback_type": "cusip",
                        "emma_fallback_url": self.quick_search_transfer_url(portfolio_match),
                        "emma_fallback_cusip_query": portfolio_match,
                        "emma_fallback_cusip9": portfolio_match,
                        "emma_fallback_issue_id": issue_id,
                        "emma_fallback_issue_desc": issue_desc,
                            "emma_fallback_issuer_name": issuer_name,
                            "emma_fallback_match_basis": "active_issue_cusip6_matched_to_portfolio",
                        }
                    )
                    return result

            if not first_cusip6 and parsed_cusip6:
                first_cusip6 = parsed_cusip6

            if not first_issue_fallback and issue_url:
                first_issue_fallback = {
                    "emma_fallback_status": "found",
                    "emma_fallback_type": "issue",
                    "emma_fallback_url": issue_url,
                    "emma_fallback_cusip_query": "",
                    "emma_fallback_cusip9": "",
                    "emma_fallback_issue_id": issue_id,
                    "emma_fallback_issue_desc": issue_desc,
                    "emma_fallback_issuer_name": issuer_name,
                    "emma_fallback_match_basis": "active_issue_candidate_no_decodable_cusip9",
                }

        # Second-best: use CUSIP-6 active issue prefix if no CUSIP-9 was deterministically available.
        if first_cusip6:
            result.update(
                {
                    "emma_fallback_status": "found",
                    "emma_fallback_type": "cusip",
                    "emma_fallback_url": self.quick_search_transfer_url(first_cusip6),
                    "emma_fallback_cusip_query": first_cusip6,
                    "emma_fallback_cusip9": "",
                    "emma_fallback_issue_id": "",
                    "emma_fallback_issue_desc": "",
                    "emma_fallback_issuer_name": "",
                    "emma_fallback_match_basis": "active_issue_cusip6_only",
                }
            )
            return result

        if first_issue_fallback:
            result.update(first_issue_fallback)
            return result

        result["emma_fallback_match_basis"] = "no_fallback_link_from_active_candidates"
        return result

    def fetch_portfolio_linkage(self, issuer_id: str, holdings_cusips: set[str]) -> dict[str, Any]:
        issuer_id = str(issuer_id or "").strip()
        if not issuer_id:
            return {
                "issuer_id": "",
                "issuer_url": "",
                "issuer_name": "",
                "owned_securities": [],
                "related_documents": [],
                "issuer_security_count": 0,
                "issuer_document_count": 0,
                "scrape_status": "error",
                "scrape_error": "Missing issuer_id",
            }

        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(CookieJar()))

        issuer_url = self.issuer_profile_url(issuer_id)
        issuer_html = ""
        issuer_name = ""
        scrape_error = ""
        try:
            issuer_html = self._fetch_with_disclaimer_accept(opener, issuer_url)
            issuer_name = self._extract_issuer_name(issuer_html)
        except Exception as exc:  # pragma: no cover - network variability
            scrape_error = f"Issuer page scrape error: {exc}"

        owned_securities = self._search_owned_securities(opener, issuer_id, holdings_cusips)
        related_documents = self._collect_documents(opener, owned_securities)

        return {
            "issuer_id": issuer_id,
            "issuer_url": issuer_url,
            "issuer_name": issuer_name,
            "owned_securities": owned_securities,
            "related_documents": related_documents,
            "issuer_security_count": len(owned_securities),
            "issuer_document_count": len(related_documents),
            "scrape_status": "ok" if not scrape_error else "partial",
            "scrape_error": scrape_error,
        }

    def _fetch_with_disclaimer_accept(self, opener: urllib.request.OpenerDirector, url: str) -> str:
        req = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        with opener.open(req, timeout=self.timeout_seconds) as resp:
            html_payload = resp.read().decode("utf-8", errors="ignore")
            landing_url = resp.geturl()

        if "disclaimercontent_yesbutton" in html_payload.lower():
            self._accept_disclaimer(opener, landing_url, html_payload)
            with opener.open(req, timeout=self.timeout_seconds) as resp2:
                html_payload = resp2.read().decode("utf-8", errors="ignore")

        return html_payload

    def _accept_disclaimer(self, opener: urllib.request.OpenerDirector, current_url: str, html_payload: str) -> None:
        soup = BeautifulSoup(html_payload, "lxml")
        form = soup.find("form", {"id": "aspnetForm"})
        if not form:
            return

        fields: dict[str, str] = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            typ = (inp.get("type") or "").lower()
            if typ in {"submit", "button", "image"}:
                continue
            fields[name] = inp.get("value", "")

        fields["ctl00$mainContentArea$disclaimerContent$yesButton"] = "Accept"
        action = form.get("action") or current_url
        post_url = urllib.parse.urljoin(current_url, action)
        payload = urllib.parse.urlencode(fields).encode("utf-8")
        req = urllib.request.Request(
            post_url,
            data=payload,
            headers={
                "User-Agent": self.user_agent,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": current_url,
                "Origin": "https://emma.msrb.org",
            },
        )
        with opener.open(req, timeout=self.timeout_seconds):
            pass

    def _extract_issuer_name(self, html_payload: str) -> str:
        if not html_payload:
            return ""
        soup = BeautifulSoup(html_payload, "lxml")
        candidates = [
            soup.select_one("h1"),
            soup.select_one("h2"),
            soup.select_one(".sectionHeader"),
            soup.select_one("#selectedIssuer"),
            soup.select_one("#issuerName"),
        ]
        for node in candidates:
            if node and node.get_text(strip=True):
                txt = node.get_text(" ", strip=True)
                if "Municipal Securities Rulemaking Board" not in txt:
                    return txt
        return ""

    def _search_owned_securities(
        self,
        opener: urllib.request.OpenerDirector,
        issuer_id: str,
        holdings_cusips: set[str],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()

        for cusip in sorted({_normalize_cusip(c) for c in holdings_cusips if _normalize_cusip(c)}):
            queries = [cusip]
            if len(cusip) >= 6:
                queries.append(cusip[:6])

            for query in queries:
                try:
                    matches = self.search_ahead(query)
                except Exception:
                    continue

                for m in matches:
                    label = _strip_tags(str(m.get("Text", "")))
                    candidate_url = str(m.get("Url", "")).replace("\\u0026", "&")
                    result_type = str(m.get("ResultType", ""))

                    final_url = self._resolve_url(opener, candidate_url)
                    haystack = f"{label} {final_url}".upper()

                    if cusip not in haystack:
                        continue

                    issuer_match = issuer_id in final_url
                    key = f"{cusip}|{final_url}"
                    if key in seen:
                        continue
                    seen.add(key)

                    rows.append(
                        {
                            "cusip9": cusip,
                            "issue_description": label,
                            "maturity_date": "",
                            "coupon": "",
                            "security_status": "matched" if issuer_match else "matched_cross_issuer",
                            "result_type": result_type,
                            "security_url": final_url or candidate_url,
                            "issuer_match": issuer_match,
                        }
                    )

                if any(r["cusip9"] == cusip for r in rows):
                    break

        return rows

    def _resolve_url(self, opener: urllib.request.OpenerDirector, url: str) -> str:
        if not url:
            return ""
        req = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        try:
            with opener.open(req, timeout=self.timeout_seconds) as resp:
                return str(resp.geturl())
        except Exception:
            return url

    def _collect_documents(self, opener: urllib.request.OpenerDirector, securities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        docs: list[dict[str, Any]] = []
        seen: set[str] = set()
        max_pages = 12

        for sec in securities[:max_pages]:
            url = str(sec.get("security_url", "")).strip()
            if not url:
                continue

            try:
                html_payload = self._fetch_with_disclaimer_accept(opener, url)
            except Exception:
                continue

            soup = BeautifulSoup(html_payload, "lxml")
            anchors = soup.find_all("a", href=True)
            for a in anchors:
                href = html.unescape(str(a.get("href", ""))).strip()
                text = _strip_tags(a.get_text(" ", strip=True))
                if not href:
                    continue

                full_href = urllib.parse.urljoin(url, href)
                classifier = f"{text} {full_href}".lower()
                if not any(k in classifier for k in ["official", "continuing", "disclosure", "statement", "document"]):
                    continue

                doc_id = _extract_doc_id(full_href) or _extract_doc_id(text) or full_href
                if doc_id in seen:
                    continue
                seen.add(doc_id)

                docs.append(
                    {
                        "document_id": doc_id,
                        "document_type": _classify_document_type(text, full_href),
                        "posting_date": "",
                        "title": text or full_href,
                        "related_cusip9": str(sec.get("cusip9", "")),
                        "document_url": full_href,
                    }
                )

        return docs


def _strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", html.unescape(str(value or ""))).strip()


def _normalize_cusip(value: str) -> str:
    raw = "".join(ch for ch in str(value or "").upper() if ch.isalnum())
    if len(raw) < 6:
        return ""
    return raw[:9]


def _extract_probable_cusips(text: str) -> list[str]:
    raw_hits = re.findall(r"\b[0-9A-Z]{6,9}\b", str(text or "").upper())
    cleaned: list[str] = []
    for token in raw_hits:
        if sum(ch.isdigit() for ch in token) < 4:
            continue
        normalized = _normalize_cusip(token)
        if normalized and normalized not in cleaned:
            cleaned.append(normalized)
    return cleaned


def _extract_issue_id(url: str) -> str:
    path = urllib.parse.urlparse(str(url or "")).path or ""
    if not path:
        return ""
    last = path.rstrip("/").split("/")[-1]
    if re.fullmatch(r"E[A-Z0-9]{4,}", last, flags=re.I):
        return last.upper()
    return ""


def _extract_js_function_return(html_payload: str, function_name: str) -> str:
    if not html_payload or not function_name:
        return ""
    pattern = rf"function\s+{re.escape(function_name)}\s*\(\)\s*\{{\s*return\s*'([^']*)';\s*\}}"
    match = re.search(pattern, html_payload, flags=re.I | re.S)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _extract_noncusip_security_cusips(html_payload: str) -> list[str]:
    if not html_payload:
        return []
    raw = _extract_js_function_return(html_payload, "getNonCusipSecurities")
    if not raw:
        return []
    try:
        decoded = html.unescape(raw)
        data = json.loads(decoded)
    except Exception:
        return []
    if not isinstance(data, list):
        return []

    out: list[str] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        token = _normalize_cusip(str(row.get("Cusip9", "")).strip())
        if token and len(token) == 9 and token not in out:
            out.append(token)
    return out


def _ocr_cusip_image(image_bytes: bytes) -> str:
    if not image_bytes:
        return ""
    if Image is None or pytesseract is None:  # pragma: no cover - dependency guard
        return ""

    try:
        base = Image.open(io.BytesIO(image_bytes))
    except Exception:
        return ""

    variants = []
    try:
        gray = ImageOps.grayscale(base)
        variants.append(gray)
        variants.append(ImageEnhance.Contrast(gray).enhance(2.5))
        variants.append(ImageOps.invert(gray))
    except Exception:
        variants = [base]

    for img in variants:
        try:
            w, h = img.size
            scale = 4
            enlarged = img.resize((max(1, w * scale), max(1, h * scale)))
            text = pytesseract.image_to_string(  # type: ignore[union-attr]
                enlarged,
                config="--psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            )
        except Exception:
            continue
        candidate = "".join(ch for ch in str(text or "").upper() if ch.isalnum())
        if len(candidate) >= 9:
            candidate = candidate[:9]
        if len(candidate) == 9 and _is_valid_cusip9(candidate):
            return candidate
        # keep a weaker fallback if OCR captured a 9-char token but check-digit failed
        if len(candidate) == 9:
            fallback = candidate
            # try one more pass on binarized image
            try:
                bw = enlarged.point(lambda p: 255 if p > 160 else 0)
                text2 = pytesseract.image_to_string(  # type: ignore[union-attr]
                    bw,
                    config="--psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                )
                candidate2 = "".join(ch for ch in str(text2 or "").upper() if ch.isalnum())
                if len(candidate2) >= 9:
                    candidate2 = candidate2[:9]
                if len(candidate2) == 9 and _is_valid_cusip9(candidate2):
                    return candidate2
            except Exception:
                pass
            return fallback
    return ""


def _is_valid_cusip9(cusip: str) -> bool:
    token = "".join(ch for ch in str(cusip or "").upper() if ch.isalnum())
    if len(token) != 9:
        return False

    def _value(ch: str) -> int:
        if ch.isdigit():
            return int(ch)
        # CUSIP letters: A=10 ... Z=35
        return ord(ch) - 55

    try:
        total = 0
        for idx, ch in enumerate(token[:8], start=1):
            val = _value(ch)
            if idx % 2 == 0:
                val *= 2
            total += (val // 10) + (val % 10)
        check = (10 - (total % 10)) % 10
        return check == int(token[8])
    except Exception:
        return False


def _build_issue_search_terms(facility_name: str) -> list[str]:
    base = re.sub(r"\s+", " ", str(facility_name or "").strip())
    if not base:
        return []

    terms: list[str] = []

    def _push(v: str) -> None:
        val = re.sub(r"\s+", " ", str(v or "").strip())
        if val and val not in terms:
            terms.append(val)

    _push(base)

    tokens = re.findall(r"[A-Z0-9]+", base.upper())
    stop = {
        "HOSPITAL",
        "HOSP",
        "MEDICAL",
        "CENTER",
        "CTR",
        "HEALTH",
        "HEALTHCARE",
        "SYSTEM",
        "SYSTEMS",
        "COMMUNITY",
        "REGIONAL",
        "MEMORIAL",
        "REHABILITATION",
        "REHAB",
        "CLINIC",
        "FACILITY",
        "FACILITIES",
        "OF",
        "THE",
        "AND",
        "INC",
        "LLC",
        "CORP",
        "CO",
        "HOLDINGS",
    }
    trimmed_tokens = [t for t in tokens if t not in stop]
    if trimmed_tokens:
        _push(" ".join(trimmed_tokens))
        if len(trimmed_tokens) >= 2:
            _push(" ".join(trimmed_tokens[:2]))

    chain_hint_tokens = {
        "PROVIDENCE",
        "ASCENSION",
        "TRINITY",
        "MERCY",
        "COMMONSPIRIT",
        "ADVENTHEALTH",
        "UPMC",
        "MAYO",
        "CLEVELAND",
        "BANNER",
        "SUTTER",
    }
    for tok in trimmed_tokens:
        if tok in chain_hint_tokens and len(tok) >= 5:
            _push(tok)
            break

    return terms[:5]


def _extract_doc_id(text: str) -> str:
    match = re.search(r"\b(E[A-Z0-9]{5,}|EP\d+|ER\d+)\b", str(text or ""), re.I)
    return match.group(1).upper() if match else ""


def _classify_document_type(text: str, url: str) -> str:
    haystack = f"{text} {url}".lower()
    if "official" in haystack:
        return "Official Statement"
    if "continuing" in haystack:
        return "Continuing Disclosure"
    if "preliminary" in haystack:
        return "Preliminary Official Statement"
    return "Document"
