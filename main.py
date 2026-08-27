"""
opencall-monitor — monitorim i thirrjeve/tenderëve për shërbime ligjore
(GIZ, TED/BE, UNDP, OSBE etj.) relevante për Shqipërinë.

Nis me:  uvicorn main:app --host 0.0.0.0 --port 8000
Dashboard: http://<server>:8000/
"""

import hashlib
import logging
import os
import re
import sqlite3
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
import yaml
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("opencall-monitor")

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "opencalls.db"
CONFIG_PATH = BASE_DIR / "config.yaml"

STATUSES = ["NEW", "REVIEW", "GO", "BID", "WON", "LOST", "DROPPED"]

# Argumente qendrueshmerie per Chromium ne mjedise te kufizuara/kontejnerizuara
# (si Render free tier: 512MB RAM, 0.1 CPU). --disable-dev-shm-usage eshte
# esencial ne Docker sepse /dev/shm parazgjedhur eshte shume i vogel per
# Chromium dhe e ben te crash-oje/ngec ne menyre te herepashershme - kjo eshte
# shkaku me i mundshem i dështimeve te ndermjetme te app_gov/development_aid.
CHROMIUM_LAUNCH_ARGS = [
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--no-sandbox",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ENV_VAR_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")


def load_config() -> dict:
    """Lexon config.yaml dhe zevendeson çdo ${EMRI_VARIABLES} me vleren e
    environment variable-it perkates (p.sh. per kredenciale ne Render, qe s'duhet
    te ruhen ne tekst te thjeshte ne repository). Nese variabli s'eshte vendosur,
    mbetet varg bosh (jo placeholder-i vete) qe te mos rrjedhe gabimisht ne kod."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        raw = f.read()
    substituted = ENV_VAR_RE.sub(lambda m: os.environ.get(m.group(1), ""), raw)
    return yaml.safe_load(substituted)


CONFIG = load_config()


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notices (
                fingerprint TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                external_id TEXT,
                title TEXT NOT NULL,
                buyer TEXT,
                published_date TEXT,
                deadline TEXT,
                url TEXT,
                score INTEGER DEFAULT 0,
                matched_keywords TEXT,
                status TEXT DEFAULT 'NEW',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON notices(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_deadline ON notices(deadline)")


def make_fingerprint(source: str, external_id: str) -> str:
    raw = f"{source}:{external_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


def score_text(text: str) -> tuple[int, list[str]]:
    """Kthen (score, lista e fjalëve-kyçe të gjetura) bazuar në config.yaml"""
    if not text:
        return 0, []
    text_low = text.lower()
    kw_cfg = CONFIG.get("keywords", {})
    weights = {"high": 3, "medium": 2, "low": 1}
    score = 0
    matched = []
    for tier, weight in weights.items():
        for kw in kw_cfg.get(tier, []):
            if kw.lower() in text_low:
                score += weight
                matched.append(kw)
    return score, matched


def _sqlite_safe(value) -> str:
    """Siguron qe vlera te jete tip i mbeshtetur nga sqlite3 (str), edhe nese
    burimi (p.sh. TED) e ka kthyer si liste/dict per shkak te gjuheve te shumta."""
    if isinstance(value, (list, dict)):
        return str(value) if value else ""
    return value if value is not None else ""


def upsert_notice(item: dict):
    """item: dict me source, external_id, title, buyer, published_date, deadline, url"""
    item = {k: _sqlite_safe(v) for k, v in item.items()}
    fingerprint = make_fingerprint(item["source"], item["external_id"])
    score, matched = score_text(f"{item.get('title','')} {item.get('description','')}")
    now = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT fingerprint FROM notices WHERE fingerprint = ?", (fingerprint,)
        ).fetchone()
        if existing:
            return False  # tashmë ekziston, s'është i ri

        try:
            conn.execute(
                """
                INSERT INTO notices
                    (fingerprint, source, external_id, title, buyer, published_date,
                     deadline, url, score, matched_keywords, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fingerprint,
                    item["source"],
                    item.get("external_id", ""),
                    item.get("title", "(pa titull)"),
                    item.get("buyer", ""),
                    item.get("published_date", ""),
                    item.get("deadline", ""),
                    item.get("url", ""),
                    score,
                    ", ".join(matched),
                    "NEW",
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            return False  # nje poll tjeter e shtoi nderkohe (race), s'eshte gabim
        return True  # njoftim i ri, u shtua


# ---------------------------------------------------------------------------
# Connector: TED (EU) — Search API v3, publik, pa autentikim
# ---------------------------------------------------------------------------
def poll_ted() -> dict:
    ted_cfg = CONFIG.get("ted", {})
    if not ted_cfg.get("enabled"):
        return {"source": "ted", "fetched": 0, "new": 0, "error": None}

    cpv_list = " ".join(ted_cfg.get("cpv_codes", []))
    query_parts = [f"classification-cpv IN ({cpv_list})"] if cpv_list else []
    place = ted_cfg.get("place_of_performance")
    if place:
        query_parts.append(f"place-of-performance IN ({place})")
    query = " AND ".join(query_parts) if query_parts else "classification-cpv IN (79100000)"

    payload = {
        "query": query,
        "fields": [
            "publication-number",
            "notice-title",
            "buyer-name",
            "publication-date",
            "deadline-receipt-request",
            "links",
            "description-lot",
        ],
        "limit": ted_cfg.get("limit", 100),
        "scope": ted_cfg.get("scope", "ACTIVE"),
        "checkQuerySyntax": False,
        "paginationMode": "ITERATION",
    }

    fetched, new = 0, 0
    error = None
    try:
        resp = requests.post(ted_cfg["endpoint"], json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        notices = data.get("notices", []) or data.get("results", [])
        fetched = len(notices)

        def to_text(value, default=""):
            """Normalizon fusha te TED qe mund te vijne si string, dict {"eng": "..."},
            liste [{"eng": "..."}], ose te ndertheruara ne disa shtresa (p.sh.
            description-lot vjen si {"eng": ["teksti..."]} - dict qe permban liste)."""
            for _ in range(4):
                if isinstance(value, list):
                    value = value[0] if value else default
                elif isinstance(value, dict):
                    value = value.get("eng") or next(iter(value.values()), default)
                else:
                    break
            return value if isinstance(value, str) else default

        for n in notices:
            ext_id = str(n.get("publication-number", n.get("ND", "")))
            title = to_text(n.get("notice-title", n.get("title", "(pa titull)")), "(pa titull)")
            buyer = to_text(n.get("buyer-name", ""))
            description = to_text(n.get("description-lot", ""))
            links = n.get("links", {})
            url = ""
            if isinstance(links, dict):
                for kind in ("html", "pdf"):
                    section = links.get(kind, {})
                    if isinstance(section, dict):
                        url = section.get("ENG") or next(iter(section.values()), "")
                    if url:
                        break

            item = {
                "source": "TED",
                "external_id": ext_id,
                "title": title,
                "buyer": buyer,
                "published_date": to_text(n.get("publication-date", ""))[:10],
                "deadline": to_text(n.get("deadline-receipt-request", ""))[:10],
                "url": url or f"https://ted.europa.eu/en/notice/-/detail/{ext_id}",
                "description": description,
            }
            if upsert_notice(item):
                new += 1

    except requests.exceptions.RequestException as e:
        error = str(e)
        log.error("TED poll error: %s", e)

    return {"source": "ted", "fetched": fetched, "new": new, "error": error}


# ---------------------------------------------------------------------------
# Connector: UNDP Albania — RSS/RDF feed publik, pa autentikim
# ---------------------------------------------------------------------------
UNDP_NS = {
    "rss": "http://purl.org/rss/1.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "undpprocnot": "http://procurement-notices.undp.org/rss_feed/spec/",
}


def poll_undp() -> dict:
    undp_cfg = CONFIG.get("undp", {})
    if not undp_cfg.get("enabled"):
        return {"source": "undp", "fetched": 0, "new": 0, "error": None}

    fetched, new = 0, 0
    error = None
    try:
        resp = requests.get(undp_cfg["feed_url"], timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = root.findall("rss:item", UNDP_NS)
        fetched = len(items)

        for it in items:
            title_el = it.find("rss:title", UNDP_NS)
            link_el = it.find("rss:link", UNDP_NS)
            date_el = it.find("dc:date", UNDP_NS)
            deadline_el = it.find("undpprocnot:deadline", UNDP_NS)

            title = (title_el.text or "").strip() if title_el is not None else ""
            link = (link_el.text or "").strip() if link_el is not None else ""
            if not title or not link:
                continue
            published_date = (date_el.text or "")[:10] if date_el is not None and date_el.text else ""
            deadline = (deadline_el.text or "")[:10] if deadline_el is not None and deadline_el.text else ""

            item = {
                "source": "UNDP",
                "external_id": link,
                "title": title,
                "buyer": "UNDP Albania",
                "published_date": published_date,
                "deadline": deadline,
                "url": link,
                # feed-i jep vetem titull, jo tekst te plote artikulli - scoring behet mbi titullin
                "description": title,
            }
            if upsert_notice(item):
                new += 1

    except (requests.exceptions.RequestException, ET.ParseError) as e:
        error = str(e)
        log.error("UNDP poll error: %s", e)

    return {"source": "undp", "fetched": fetched, "new": new, "error": error}


# ---------------------------------------------------------------------------
# Connector: Agjencia e Prokurimit Publik (app.gov.al) — tenderat shteterore/
# ministrite/bashkite. Faqja s'ka API; kerkimi ngarkohet me JS (form POST
# ASP.NET), prandaj perdoret Playwright (browser headless), jo requests i thjeshte.
# Rezultatet (Objekti i tenderit / Autoriteti Kontraktues / datat / referenca)
# jane te pranishme direkt ne HTML-in e faqes se rezultateve (jo brenda modal-it),
# prandaj i nxjerrim nga teksti i sheshte i faqes, blloku pas blloku.
# ---------------------------------------------------------------------------
APP_GOV_BLOCK_RE = re.compile(r"(?=Objekti i tenderit:)")
APP_GOV_TITLE_RE = re.compile(r"Objekti i tenderit:\s*(.+)")
APP_GOV_AUTHORITY_RE = re.compile(r"Autoriteti Kontraktues:\s*([^|]+)")
APP_GOV_OPEN_RE = re.compile(r"Data e hapjes:\s*(\d{2}-\d{2}-\d{4})")
APP_GOV_CLOSE_RE = re.compile(r"Data e mbylljes:\s*(\d{2}-\d{2}-\d{4})")
APP_GOV_REF_RE = re.compile(r"Numri i referenc.s:\s*(\S+)")


def _app_gov_date_to_iso(raw: str) -> str:
    """DD-MM-YYYY -> YYYY-MM-DD"""
    if not raw:
        return ""
    try:
        dd, mm, yyyy = raw.split("-")
        return f"{yyyy}-{mm}-{dd}"
    except ValueError:
        return ""


def _app_gov_search(page, search_url: str, term: str) -> list[dict]:
    """Ben nje kerkim te vetem ne app.gov.al dhe kthen listen e rezultateve te
    parsuara (dict me title/buyer/published_date/deadline/external_id)."""
    page.goto(search_url, timeout=60000, wait_until="domcontentloaded")
    page.wait_for_timeout(1000)
    target = None
    for f in page.query_selector_all("form"):
        if f.query_selector('input[name="TenderSubject"]'):
            target = f
            break
    if not target:
        return []
    target.query_selector('input[name="TenderSubject"]').fill(term)
    btn = target.query_selector('input[type="submit"], button[type="submit"]')
    with page.expect_navigation(timeout=30000):
        btn.click()
    page.wait_for_timeout(1500)
    text = page.inner_text("body")

    results = []
    for block in APP_GOV_BLOCK_RE.split(text)[1:]:
        title_m = APP_GOV_TITLE_RE.search(block)
        ref_m = APP_GOV_REF_RE.search(block)
        if not title_m or not ref_m:
            continue
        authority_m = APP_GOV_AUTHORITY_RE.search(block)
        open_m = APP_GOV_OPEN_RE.search(block)
        close_m = APP_GOV_CLOSE_RE.search(block)
        title = title_m.group(1).strip().strip("�“”\"")
        results.append(
            {
                "title": title,
                "buyer": authority_m.group(1).strip() if authority_m else "",
                "published_date": _app_gov_date_to_iso(open_m.group(1)) if open_m else "",
                "deadline": _app_gov_date_to_iso(close_m.group(1)) if close_m else "",
                "external_id": ref_m.group(1).strip(),
            }
        )
    return results


def poll_app_gov() -> dict:
    """Kerkon ne app.gov.al per çdo term shqip te lidhur me legal services
    (config.yaml -> app_gov.search_terms). Faqja s'ka URL te vecante per
    njoftim - te gjitha njoftimet lidhen te faqja e kerkimit vete; numri i
    references (REF-...) i lejon perdoruesit ta gjeje manualisht atje."""
    cfg = CONFIG.get("app_gov", {})
    if not cfg.get("enabled"):
        return {"source": "app_gov", "fetched": 0, "new": 0, "error": None}

    search_url = cfg["search_url"]
    search_terms = cfg.get("search_terms", ["ligjor"])

    fetched, new = 0, 0
    error = None
    seen_refs = set()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_LAUNCH_ARGS)
            page = browser.new_page()
            for term in search_terms:
                try:
                    found = _app_gov_search(page, search_url, term)
                except Exception as e:
                    log.warning("app.gov.al kerkim '%s' deshtoi: %s", term, e)
                    continue
                for r in found:
                    if r["external_id"] in seen_refs:
                        continue
                    seen_refs.add(r["external_id"])
                    fetched += 1
                    item = {
                        "source": "APP (Prokurimi Publik)",
                        "external_id": r["external_id"],
                        "title": r["title"],
                        "buyer": r["buyer"],
                        "published_date": r["published_date"],
                        "deadline": r["deadline"],
                        "url": search_url,
                        "description": f"{r['title']} {r['buyer']}",
                    }
                    if upsert_notice(item):
                        new += 1
            browser.close()
    except Exception as e:
        error = str(e)
        log.error("app.gov.al poll error: %s", e)

    return {"source": "app_gov", "fetched": fetched, "new": new, "error": error}


# ---------------------------------------------------------------------------
# Connector: DevelopmentAid.org — kerkon llogari (membership); mbulon 150+
# donatore (EC, EBRD, WB, GIZ, ADA, UNDP, UN Women, ILO, WHO etj.) per Shqiperi
# ne nje vend te vetem. Perdor Playwright per identifikim dhe kerkim, sepse
# eshte aplikacion Angular pa API publike falas.
# ---------------------------------------------------------------------------
DEV_AID_OPEN_STATUSES = {"open"}


def _dev_aid_login(page) -> bool:
    cfg = CONFIG.get("development_aid", {})
    page.goto("https://www.developmentaid.org/", timeout=45000, wait_until="load")
    page.wait_for_timeout(2500)
    page.evaluate(
        "() => { const el = document.querySelector('da-cookie-police-notification'); if (el) el.remove(); }"
    )
    page.click("text=Sign in", timeout=20000)
    page.wait_for_timeout(1500)
    email_inputs = [
        el for el in page.query_selector_all('input[type="email"], input[name*="email" i]') if el.is_visible()
    ]
    pass_inputs = [el for el in page.query_selector_all('input[type="password"]') if el.is_visible()]
    if not email_inputs or not pass_inputs:
        return False
    email_inputs[0].fill(cfg["email"])
    pass_inputs[0].fill(cfg["password"])
    submit_btns = [
        el for el in page.query_selector_all('button[type="submit"]') if el.is_visible()
    ]
    if not submit_btns:
        return False
    submit_btns[0].click()
    page.wait_for_timeout(2500)
    return page.query_selector("text=Log out") is not None or page.query_selector("text=Sign out") is not None


def _dev_aid_parse_card(card) -> Optional[dict]:
    a = card.query_selector("a.search-card__title")
    if not a:
        return None
    title = (a.get_attribute("title") or "").strip()
    href = a.get_attribute("href") or ""
    if not title or not href:
        return None
    lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
    fields = {}
    i = 0
    while i < len(lines):
        if lines[i].endswith(":"):
            fields[lines[i][:-1]] = lines[i + 1] if i + 1 < len(lines) else ""
            i += 2
        else:
            i += 1
    return {
        "title": title,
        "url": "https://www.developmentaid.org" + href,
        "external_id": href,
        "buyer": fields.get("Funding agency", ""),
        "status": fields.get("Status", ""),
        "deadline_raw": fields.get("Deadline", ""),
        "posted_raw": fields.get("Posted", ""),
    }


def _dev_aid_fetch_detail_text(page, url: str) -> str:
    """Hap faqen e detajuar te tenderit per te marre "Sectors:" dhe pershkrimin
    e lire (jo-anetar) - jep sinjal shume me te fort per scoring se titulli vetem
    (p.sh. "Sectors: Law, Public Sector Governance" per nje tender qe titulli i
    tij vetem s'e permend fjalen "legal")."""
    try:
        page.goto(url, timeout=30000, wait_until="domcontentloaded")
        page.wait_for_timeout(1800)
        page.evaluate(
            "() => { const el = document.querySelector('da-cookie-police-notification'); if (el) el.remove(); }"
        )
        text = page.inner_text("body")
        m = re.search(r"Sectors:\s*\n?(.+)", text)
        sectors = m.group(1).strip() if m else ""
        idx = text.find("Description")
        desc = text[idx : idx + 800] if idx != -1 else ""
        sector_tags = [s.strip().lower() for s in sectors.split(",")]
        law_marker = " __devaid_sector_law__" if "law" in sector_tags else ""
        return f"{sectors} {desc}{law_marker}"
    except Exception as e:
        log.warning("DevelopmentAid detail fetch deshtoi (%s): %s", url, e)
        return ""


def _dev_aid_date_to_iso(raw: str) -> str:
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, "%b %d, %Y").date().isoformat()
    except ValueError:
        return ""


def poll_development_aid() -> dict:
    """Identifikohet ne DevelopmentAid.org me llogarine e konfiguruar dhe kerkon
    per Shqiperi (locations=262) te kombinuar me disa terma legal. Mbahen vetem
    rezultatet me Status=Open (jo Forecast/Formulation/Awarded/Closed/Cancelled)."""
    cfg = CONFIG.get("development_aid", {})
    if not cfg.get("enabled"):
        return {"source": "development_aid", "fetched": 0, "new": 0, "error": None}

    location_id = cfg.get("location_id", 262)
    search_terms = cfg.get("search_terms", ["legal"])

    fetched, new = 0, 0
    error = None
    seen_ids = set()
    open_results = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=CHROMIUM_LAUNCH_ARGS)
            context = browser.new_context()
            page = context.new_page()
            logged_in = False
            for attempt in range(3):
                try:
                    if _dev_aid_login(page):
                        logged_in = True
                        break
                except Exception as e:
                    log.warning("DevelopmentAid login perpjekja %d deshtoi: %s", attempt + 1, e)
                page.wait_for_timeout(2000)
            if not logged_in:
                browser.close()
                return {"source": "development_aid", "fetched": 0, "new": 0, "error": "login deshtoi pas 3 perpjekjeve"}

            for term in search_terms:
                url = (
                    "https://www.developmentaid.org/tenders/search"
                    f"?hiddenAdvancedFilters=0&sort=relevance.desc&locations={location_id}&searchedText={term}"
                )
                try:
                    page.goto(url, timeout=30000, wait_until="domcontentloaded")
                    page.wait_for_timeout(2500)
                    page.evaluate(
                        "() => { const el = document.querySelector('da-cookie-police-notification'); if (el) el.remove(); }"
                    )
                except Exception as e:
                    log.warning("DevelopmentAid kerkim '%s' deshtoi: %s", term, e)
                    continue

                for card in page.query_selector_all("div.search-card__details-container"):
                    parsed = _dev_aid_parse_card(card)
                    if not parsed:
                        continue
                    if parsed["status"].lower() not in DEV_AID_OPEN_STATUSES:
                        continue
                    if parsed["external_id"] in seen_ids:
                        continue
                    seen_ids.add(parsed["external_id"])
                    fetched += 1
                    open_results.append(parsed)

            # Per çdo rezultat te ri "Open", hapim faqen e detajuar per te marre
            # "Sectors:" + pershkrimin e lire - shume me shume sinjal per scoring
            # sesa vetem titulli i shkurter i kartes se kerkimit.
            for parsed in open_results:
                extra_text = _dev_aid_fetch_detail_text(page, parsed["url"])
                item = {
                    "source": "DevelopmentAid",
                    "external_id": parsed["external_id"],
                    "title": parsed["title"],
                    "buyer": parsed["buyer"],
                    "published_date": _dev_aid_date_to_iso(parsed["posted_raw"]),
                    "deadline": _dev_aid_date_to_iso(parsed["deadline_raw"]),
                    "url": parsed["url"],
                    "description": f"{parsed['title']} {parsed['buyer']} {extra_text}",
                }
                if upsert_notice(item):
                    new += 1

            browser.close()
    except Exception as e:
        error = str(e)
        log.error("DevelopmentAid poll error: %s", e)

    return {"source": "development_aid", "fetched": fetched, "new": new, "error": error}


# ---------------------------------------------------------------------------
# Connector: GIZ (via Panorama Online, "Kendi i njoftimeve")
# GIZ Shqiperi s'ka portal publik prokurimi; njoftimet publikohen ne LinkedIn dhe
# rimerren nga Panorama Online. Ky connector kontrollon ate kategori WordPress
# (dhe pagination-in e saj) dhe merr vetem artikujt qe permbajne "GIZ" ne titull.
# ---------------------------------------------------------------------------
PUBLISHED_RE = re.compile(r"([A-Za-z]{3}\s+\d{1,2},\s+\d{4})\s*\|\s*\d{1,2}:\d{2}")


def extract_published_date(text: str) -> str:
    """panorama.com.al vendos daten e publikimit te vete artikullit ne formatin
    'Sep 20, 2025 | 8:38' (me oren bashkangjitur) - ndryshe nga datat e artikujve
    te tjere ne sidebar, qe s'e kane oren. Kjo dallon artikullin nga te tjeret."""
    m = PUBLISHED_RE.search(text)
    if not m:
        return ""
    try:
        return datetime.strptime(m.group(1), "%b %d, %Y").date().isoformat()
    except ValueError:
        return ""


# Data numerike (16.04.2026) OSE tekstuale (24 March 2025 / 04th of April 2025).
# Dy alternativat mbeshteten ne NJE grup te vetem kapjeje, qe te mos ndryshoje
# numeratimin e grupeve ne shabllonet me poshte.
NUMERIC_DATE = r"\d{1,2}[./]\d{1,2}[./]\d{2,4}"
TEXT_DATE = r"\d{1,2}\s*(?:st|nd|rd|th)?\s*(?:of\s+)?[A-Za-z]+\.?,?\s*\d{4}"
DATE_RE = rf"\[?({NUMERIC_DATE}|{TEXT_DATE})\]?"
DEADLINE_PATTERNS = [
    rf"latest by[:\s]+{DATE_RE}",
    rf"latest until[:\s]+{DATE_RE}",
    rf"no later than[:\s]+{DATE_RE}",
    rf"due date[^.\n]*?{DATE_RE}",
    rf"deadline[^.\n]*?{DATE_RE}",
    rf"submitting[^.\n]*?is[:\s]+{DATE_RE}",
    rf"submit[^.\n]*?by[,\s]+{DATE_RE}",
    rf"application period until[:\s]+{DATE_RE}",
]


def parse_date_flexible(raw: str):
    """Parson nje date te kapur nga DATE_RE, qofte numerike (16.04.2026) ose
    tekstuale me emrin e muajit (24 March 2025, 04th of April 2025)."""
    raw = raw.strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(raw.replace("/", "."), fmt).date()
        except ValueError:
            pass
    cleaned = re.sub(r"(\d)\s*(?:st|nd|rd|th)\b", r"\1", raw, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bof\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r",", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            pass
    return None


def extract_deadline_from_text(text: str) -> str:
    """Kerkon nje date afati ne tekstin e artikullit me disa shabllone regjex.
    Kthen date ne format YYYY-MM-DD (string) ose "" nese s'gjendet asgje.

    Disa faqe (p.sh. Panorama Online) fusin hapesira te fshehta brenda vete
    numrave/fjaleve te dates (p.sh. "16.04 .202 6" ose "04 th ... 2026") - ndoshta
    si mbrojtje anti-scraping. Prandaj i mbyllim hapesirat mes shifrave, dhe
    shablloni i dates tekstuale lejon hapesire para "st/nd/rd/th".
    Kur gjenden disa data te mundshme (shpesh ka disa afate te permendura ne
    te njejtin njoftim), merret data me e vonshme, qe zakonisht eshte afati
    perfundimtar real i aplikimit."""
    if not text:
        return ""
    text = re.sub(r"(?<=\d)\s+(?=[\d.])", "", text)

    found = []
    for pattern in DEADLINE_PATTERNS:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            parsed = parse_date_flexible(m.group(1))
            if parsed:
                found.append(parsed)
    if found:
        return max(found).isoformat()
    return ""


GENERIC_GIZ_PROFILE_RE = re.compile(
    r"supports Albania on its path to European integration and development[^.]*\.",
    re.IGNORECASE,
)


def fetch_article(url: str) -> tuple[str, str, str]:
    """Merr artikullin nje here dhe kthen (deadline, published_date, teksti_plote) -
    teksti perdoret edhe per scoring (relevance ndaj legal services), jo vetem per
    gjetjen e afatit/dates.

    Njoftimet pa projekt specifik (p.sh. blerje mobiliesh) perdorin nje "profil"
    fiks te zyres qe permend shume fusha pune te GIZ-it (perfshi "legal reform")
    thjesht si liste e pergjithshme - jo si permbajtje relevante e ketij njoftimi
    konkret. E heqim ate fjali para scoring-ut qe te mos krijoje "false positive"."""
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # Kufizohemi te permbajtja e vete artikullit (jo sidebar/"njoftime te
        # ngjashme"), perndryshe nje "until <date>" nga nje artikull krejt tjeter
        # ne faqe mund te kapet gabimisht si afati i ketij njoftimi.
        header = soup.find(class_="td-post-header")
        content = soup.find(class_="td-post-content")
        article_only = " ".join(
            el.get_text(" ", strip=True) for el in (header, content) if el
        )
        text = article_only or soup.get_text(" ", strip=True)
        # td-post-content perfshin ne fund nje liste "Te lidhura" me tituj artikujsh
        # krejt te palidhur (lajme te tjera te sajtit) - e presim para se te kerkojme
        # data/fjale-kyce, perndryshe nje date apo fjale nga nje lajm tjeter mund te
        # perzihet gabimisht me permbajtjen e njoftimit.
        text = re.split(r"\bTe lidhura\b", text, maxsplit=1, flags=re.IGNORECASE)[0]
        scoring_text = GENERIC_GIZ_PROFILE_RE.sub("", text)
        return extract_deadline_from_text(text), extract_published_date(text), scoring_text
    except requests.exceptions.RequestException as e:
        log.warning("S'u lexua dot artikulli %s: %s", url, e)
        return "", "", ""


REQUESTER_RE = re.compile(r"Requester\s*:\s*([^,\n]+)", re.IGNORECASE)


def match_organization(title: str, article_text: str, organizations: list[dict]) -> str:
    """Percakton emrin e institucionit qe boton njoftimin, ne kete rradhe:
    1) fjale-kyce te njohura (config.yaml -> donors.organizations) ne titull
    2) te njejtat fjale-kyce, por brenda tekstit te plote te artikullit - shume
       njoftime GIZ p.sh. s'e kane "GIZ" ne titull ("CALL FOR TENDER PREQUALIFICATION"
       i thjeshte), por e permendin brenda si "Requester: ... (GIZ)"
    3) rreshti "Requester: <emri>" ne tekst, qe ekziston ne shumicen e ketyre
       njoftimeve pavaresisht botuesit (format standard i tyre)
    4) titulli i ndare tek "/" ose "-" (p.sh. "BANKA E SHQIPERISE/ TENDER...")
    5) etikete gjenerike si rast i fundit, kur asnje nga sa siper s'jep asgje."""
    for text, is_title in ((title, True), (article_text, False)):
        text_low = text.lower()
        for org in organizations:
            if any(kw.lower() in text_low for kw in org.get("match", [])):
                return org["name"]

    m = REQUESTER_RE.search(article_text)
    if m:
        candidate = m.group(1).strip()
        if 2 <= len(candidate) <= 120:
            return candidate

    for sep in ("/", " – ", " - "):
        if sep in title:
            candidate = title.split(sep, 1)[0].strip()
            if 2 <= len(candidate) <= 80:
                return candidate
    return "Panorama - Njoftime"


def poll_giz() -> dict:
    """Merr çdo njoftim nga kategoria 'njoftime' e Panorama Online - pavaresisht
    kush e ka botuar (OJF, donator, biznes privat, banke, gjykate, institucion
    shteteror) - dhe e vlereson sipas permbajtjes se tij (score_text/keywords ne
    config.yaml). Filtri i relevances behet ne /api/notices (score >= min_score_review),
    jo ketu, qe te mos na shpetoje asnje burim vetem sepse s'e njohim emrin e botuesit."""
    donors_cfg = CONFIG.get("donors", {})
    if not donors_cfg.get("enabled"):
        return {"source": "donors", "fetched": 0, "new": 0, "error": None}

    base_url = donors_cfg["base_url"].rstrip("/") + "/"
    max_pages = donors_cfg.get("max_pages", 2)
    organizations = donors_cfg.get("organizations", [{"name": "GIZ", "match": ["giz"]}])
    fetch_deadline = donors_cfg.get("fetch_deadline_from_article", True)

    with get_conn() as conn:
        known_urls = {row["url"] for row in conn.execute("SELECT url FROM notices")}

    fetched, new = 0, 0
    error = None
    seen_urls = set()
    to_fetch = []  # (href, title) - vetem linket e reja, jo te ato tashme ne baze

    try:
        for page in range(1, max_pages + 1):
            page_url = base_url if page == 1 else f"{base_url}page/{page}/"
            resp = requests.get(page_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 404:
                break  # nuk ka me faqe pagination
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Kufizohemi te kolona kryesore e permbajtjes (td-main-content), JO
            # sidebar-i (td-main-sidebar) - sidebar-i permban widget "lajme te
            # fundit"/"me te lexuarat" qe perseritet identik ne çdo faqe pagination
            # dhe s'ka lidhje me njoftimet/tenderat (artikuj lajmesh/opinioni te
            # sajtit ne pergjithesi, jo te kategoria "njoftime").
            main_content = soup.find(class_="td-main-content") or soup

            # Marrim te gjitha linket brenda titujve (h1/h2/h3) qe cojne te nje artikull
            # (jo te nje kategori/tag), gje qe funksionon per shume tema WordPress
            # pa u mbeshtetur ne nje class specifike te panjohur.
            for heading in main_content.find_all(["h1", "h2", "h3"]):
                a = heading.find("a", href=True)
                if not a:
                    continue
                href = a["href"].strip()
                title = a.get_text(strip=True)
                if not href or not title or "/category/" in href or "/tag/" in href:
                    continue
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                fetched += 1
                if href not in known_urls:
                    to_fetch.append((href, title))

        # Artikujt e rinj merren paralelisht (ThreadPoolExecutor) - ne backfill-in
        # e pare (baze bosh) kjo mund te jete qindra artikuj, seri do te zgjaste
        # shume te gjata (1-2 sek secili x qindra = 10-15+ minuta).
        def fetch_one(href_title):
            href, title = href_title
            deadline, published_date, article_text = ("", "", "")
            if fetch_deadline:
                deadline, published_date, article_text = fetch_article(href)
            return href, title, deadline, published_date, article_text

        if to_fetch:
            with ThreadPoolExecutor(max_workers=10) as pool:
                futures = [pool.submit(fetch_one, ht) for ht in to_fetch]
                for future in as_completed(futures):
                    href, title, deadline, published_date, article_text = future.result()
                    org_name = match_organization(title, article_text, organizations)
                    item = {
                        "source": org_name,
                        "external_id": href,
                        "title": title,
                        "buyer": org_name,
                        "published_date": published_date,
                        "deadline": deadline,
                        "url": href,
                        "description": article_text[:4000],
                    }
                    if upsert_notice(item):
                        new += 1

    except requests.exceptions.RequestException as e:
        error = str(e)
        log.error("Donors poll error: %s", e)

    return {"source": "donors", "fetched": fetched, "new": new, "error": error}


def poll_all() -> list[dict]:
    results = []
    results.append(poll_ted())
    results.append(poll_undp())
    results.append(poll_app_gov())
    results.append(poll_development_aid())
    results.append(poll_giz())
    log.info("Poll rezultati: %s", results)
    return results


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="opencall-monitor")
scheduler = BackgroundScheduler()


@app.on_event("startup")
def startup():
    init_db()
    interval = CONFIG.get("poll_interval_hours", 6)
    scheduler.add_job(poll_all, "interval", hours=interval, id="poll_job")
    scheduler.start()
    log.info("Scheduler nisur — kontroll çdo %s orë", interval)
    # kontroll i parë menjëherë në sfond, pa bllokuar startup-in
    scheduler.add_job(poll_all, id="poll_job_initial")


@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown()


@app.post("/api/poll")
def trigger_poll():
    """Nis kontrollin manualisht (p.sh. për testim)"""
    return {"results": poll_all()}


@app.get("/api/notices")
def list_notices(
    status: Optional[str] = Query(None),
    include_expired: bool = Query(False),
    relevant_only: bool = Query(True),
):
    """Si parazgjedhje fshihen njoftimet me deadline te kaluar (per aplikim).
    Njoftimet pa deadline te njohur (fushe bosh - ekstraktimi i dates dështoi per
    ndonje format/faqe te panjohur) NUK mbahen automatikisht si "ende te hapura":
    nese jane botuar me shume se STALE_AFTER_DAYS dite me pare, trajtohen si te
    mbyllura, sepse thirrjet reale te tenderave zgjasin zakonisht 2-6 jave dhe nje
    njoftim mujash/vjet te vjeter pothuajse sigurisht ka perfunduar edhe nese s'e
    gjejme daten e sakte ne tekst. Kjo eshte mbrojtje e pergjithshme, jo specifike
    per nje organizate - mbulon çdo format date qe ende s'e njohim.
    Si parazgjedhje shfaqen vetem njoftimet relevante per legal services (score >=
    min_score_review nga config.yaml) - direkt (legal advisory) ose terthorazi
    (project management/team leader ne projekte qe nderlidhen me legal services)."""
    STALE_AFTER_DAYS = 45
    today = datetime.now(timezone.utc).date().isoformat()
    stale_cutoff = (datetime.now(timezone.utc).date() - timedelta(days=STALE_AFTER_DAYS)).isoformat()
    clauses = []
    params = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if not include_expired:
        clauses.append(
            "((deadline != '' AND deadline >= ?) OR "
            "(deadline = '' AND (published_date = '' OR published_date >= ?)))"
        )
        params.append(today)
        params.append(stale_cutoff)
    if relevant_only:
        clauses.append("score >= ?")
        params.append(CONFIG.get("min_score_review", 2))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM notices {where} ORDER BY score DESC, deadline ASC",
            params,
        ).fetchall()
        return JSONResponse([dict(r) for r in rows])


@app.post("/api/notices/{fingerprint}/status")
def update_status(fingerprint: str, new_status: str = Query(...)):
    if new_status not in STATUSES:
        return JSONResponse({"error": f"status i pavlefshëm: {new_status}"}, status_code=400)
    with get_conn() as conn:
        conn.execute(
            "UPDATE notices SET status = ?, updated_at = ? WHERE fingerprint = ?",
            (new_status, datetime.now(timezone.utc).isoformat(), fingerprint),
        )
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="sq">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>opencall-monitor</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>⚖️</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0b0d;
    --surface: #131519;
    --surface-2: #1a1d23;
    --surface-3: #21252c;
    --border: #262a33;
    --border-soft: #1e222a;
    --text: #eef0f3;
    --text-dim: #9198a3;
    --text-faint: #5c6270;
    --accent: #5b8cff;
    --accent-2: #8b7bff;
    --accent-soft: rgba(91,140,255,0.12);
    --success: #34d399;
    --success-soft: rgba(52,211,153,0.12);
    --warning: #fbbf24;
    --warning-soft: rgba(251,191,36,0.12);
    --danger: #f87171;
    --danger-soft: rgba(248,113,113,0.12);
    --radius: 10px;
    --shadow: 0 1px 2px rgba(0,0,0,0.4), 0 8px 24px -8px rgba(0,0,0,0.5);
  }
  * { box-sizing: border-box; }
  html { scrollbar-color: #2a2f3a transparent; }
  body {
    font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    background: radial-gradient(1200px 600px at 20% -10%, #12141a 0%, var(--bg) 55%);
    color: var(--text);
    margin: 0;
    padding: 24px 32px 60px;
    -webkit-font-smoothing: antialiased;
    min-width: 320px;
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }

  .topbar {
    position: sticky; top: 0; z-index: 20; margin: -24px -32px 0; padding: 16px 32px 14px;
    background: linear-gradient(180deg, var(--bg) 78%, transparent);
    backdrop-filter: blur(6px);
  }
  header { display: flex; align-items: flex-start; justify-content: space-between; flex-wrap: wrap; gap: 16px; margin-bottom: 18px; }
  .brand { display: flex; align-items: center; gap: 11px; }
  .brand .logo {
    width: 34px; height: 34px; border-radius: 9px; flex-shrink: 0;
    background: linear-gradient(135deg, var(--accent), var(--accent-2));
    display: flex; align-items: center; justify-content: center; font-size: 17px;
    box-shadow: 0 4px 14px -4px rgba(91,140,255,0.55);
  }
  .brand h1 { font-size: 19px; font-weight: 800; margin: 0; letter-spacing: -0.01em; line-height: 1.2; }
  .brand .sub { color: var(--text-dim); font-size: 12.5px; margin-top: 1px; }

  .stats { display: flex; gap: 10px; flex-wrap: wrap; }
  .stat {
    background: var(--surface);
    border: 1px solid var(--border-soft);
    border-radius: var(--radius);
    padding: 8px 14px;
    min-width: 78px;
    transition: border-color .15s;
  }
  .stat:hover { border-color: #33394a; }
  .stat .n { font-size: 18px; font-weight: 800; line-height: 1.1; font-variant-numeric: tabular-nums; }
  .stat .l { font-size: 10px; color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.05em; margin-top: 2px; }
  .stat.urgent .n { color: var(--danger); }

  .toolbar {
    display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
    background: var(--surface); border: 1px solid var(--border-soft); border-radius: var(--radius);
    padding: 10px 12px;
  }
  button.primary {
    background: linear-gradient(135deg, var(--accent), var(--accent-2)); color: #0a0b0d; border: none; font-weight: 700;
    padding: 8px 16px; border-radius: 8px; cursor: pointer; font-size: 13px;
    display: inline-flex; align-items: center; gap: 8px; transition: filter .15s, transform .1s;
    white-space: nowrap;
  }
  button.primary:hover { filter: brightness(1.1); }
  button.primary:active { transform: scale(0.97); }
  button.primary:disabled { opacity: .6; cursor: default; }
  .spinner {
    width: 12px; height: 12px; border-radius: 50%;
    border: 2px solid rgba(10,11,13,0.35); border-top-color: #0a0b0d;
    animation: spin .7s linear infinite; display: none;
  }
  button.primary.loading .spinner { display: inline-block; }
  @keyframes spin { to { transform: rotate(360deg); } }
  @keyframes fadeUp { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }

  .search-wrap { position: relative; flex: 1; min-width: 160px; max-width: 260px; }
  .search-wrap svg { position: absolute; left: 9px; top: 50%; transform: translateY(-50%); opacity: .45; pointer-events: none; }
  #searchInput {
    width: 100%; background: var(--surface-3); color: var(--text); border: 1px solid var(--border);
    border-radius: 7px; font-size: 12.5px; padding: 7px 10px 7px 28px; font-family: inherit;
  }
  #searchInput::placeholder { color: var(--text-faint); }
  #searchInput:focus { outline: none; border-color: var(--accent); }
  select#sourceFilter {
    background: var(--surface-3); color: var(--text); border: 1px solid var(--border);
    border-radius: 7px; font-size: 12.5px; padding: 7px 10px; font-family: inherit; cursor: pointer; max-width: 170px;
  }

  .toggle { display: inline-flex; align-items: center; gap: 7px; font-size: 12.5px; color: var(--text-dim); cursor: pointer; user-select: none; white-space: nowrap; }
  .toggle input { accent-color: var(--accent); width: 14px; height: 14px; cursor: pointer; }
  .toolbar-sep { width: 1px; align-self: stretch; background: var(--border-soft); }
  #pollStatus { font-size: 11.5px; color: var(--text-faint); margin-left: auto; white-space: nowrap; }

  .board { display: flex; gap: 14px; overflow-x: auto; padding: 4px 2px 12px; margin-top: 18px; }
  .board::-webkit-scrollbar { height: 8px; }
  .board::-webkit-scrollbar-thumb { background: var(--surface-3); border-radius: 8px; }
  .col { min-width: 282px; max-width: 282px; background: var(--surface); border: 1px solid var(--border-soft); border-radius: 12px; padding: 12px; flex-shrink: 0; }
  .col-head { display: flex; align-items: center; gap: 8px; justify-content: space-between; margin: 2px 4px 12px; }
  .col-head .col-dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
  .col-head h3 { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-dim); margin: 0; flex: 1; }
  .col-head .count { font-size: 11px; color: var(--text-faint); background: var(--surface-3); border-radius: 20px; padding: 1px 8px; font-variant-numeric: tabular-nums; }
  .empty-col { color: var(--text-faint); font-size: 12px; padding: 18px 4px; text-align: center; border: 1px dashed var(--border-soft); border-radius: 8px; }

  .card {
    background: var(--surface-2); border: 1px solid var(--border-soft); border-radius: 10px;
    padding: 12px; margin-bottom: 10px; font-size: 13px; box-shadow: var(--shadow);
    transition: border-color .15s, transform .12s;
    animation: fadeUp .25s ease both;
  }
  .card:hover { border-color: #3a4152; transform: translateY(-1px); }
  .card .badges { display: flex; align-items: center; gap: 6px; margin-bottom: 8px; flex-wrap: wrap; }
  .src-badge { font-size: 10px; font-weight: 700; letter-spacing: .02em; padding: 2px 7px; border-radius: 5px; }
  .score-badge {
    font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 5px;
    background: var(--accent-soft); color: var(--accent); margin-left: auto; cursor: help;
  }
  .card .title { font-weight: 600; line-height: 1.35; margin-bottom: 8px; color: var(--text); }
  .card .meta { color: var(--text-dim); font-size: 11.5px; margin-bottom: 5px; display: flex; align-items: center; gap: 5px; }
  .card .meta .lbl { color: var(--text-faint); min-width: 46px; }
  .deadline { font-size: 10.5px; font-weight: 700; padding: 2px 7px; border-radius: 5px; display: inline-block; }
  .dl-ok { background: var(--success-soft); color: var(--success); }
  .dl-soon { background: var(--warning-soft); color: var(--warning); }
  .dl-urgent { background: var(--danger-soft); color: var(--danger); }
  .kw-row { display: flex; flex-wrap: wrap; gap: 4px; margin: 8px 0 2px; }
  .kw-tag { font-size: 10px; color: var(--text-dim); background: var(--surface-3); border: 1px solid var(--border-soft); border-radius: 4px; padding: 1.5px 6px; }
  .card .link-row { margin: 10px 0 10px; }
  .card .link-row a { font-size: 12px; display: inline-flex; align-items: center; gap: 4px; }
  select.status-select {
    width: 100%; background: var(--surface-3); color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; font-size: 11.5px; padding: 6px 6px; cursor: pointer; font-family: inherit;
  }

  .loading-state, .board-empty { color: var(--text-faint); font-size: 13px; padding: 60px 0; text-align: center; }
  .loading-state .big-spinner {
    width: 26px; height: 26px; border-radius: 50%; margin: 0 auto 12px;
    border: 3px solid var(--surface-3); border-top-color: var(--accent); animation: spin .8s linear infinite;
  }
  .board-empty .emoji { font-size: 26px; margin-bottom: 8px; display: block; }

  @media (max-width: 720px) {
    body { padding: 16px 14px 40px; }
    .topbar { margin: -16px -14px 0; padding: 12px 14px 12px; }
    header { gap: 12px; }
    .stats { width: 100%; }
    .stat { flex: 1; min-width: 0; }
    .toolbar { flex-direction: column; align-items: stretch; }
    .toolbar-sep { display: none; }
    .search-wrap, select#sourceFilter { max-width: none; }
    #pollStatus { margin-left: 0; text-align: center; }
    .board { flex-direction: column; overflow-x: visible; }
    .col { min-width: 0; max-width: none; }
  }
</style>
</head>
<body>
  <div class="topbar">
    <header>
      <div class="brand">
        <div class="logo">⚖️</div>
        <div>
          <h1>opencall-monitor</h1>
          <div class="sub">Thirrje/tenderë për shërbime ligjore — GIZ, TED/BE, UNDP, Prokurimi Publik, DevelopmentAid</div>
        </div>
      </div>
      <div class="stats" id="stats"></div>
    </header>

    <div class="toolbar">
      <button class="primary" id="pollBtn" onclick="pollNow()">
        <span class="spinner"></span><span id="pollBtnLabel">↻ Kontrollo tani</span>
      </button>
      <div class="toolbar-sep"></div>
      <div class="search-wrap">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg>
        <input id="searchInput" type="text" placeholder="Kërko titull ose botues…" oninput="renderBoard()">
      </div>
      <select id="sourceFilter" onchange="renderBoard()"><option value="">Të gjitha burimet</option></select>
      <div class="toolbar-sep"></div>
      <label class="toggle"><input type="checkbox" id="relevantToggle" checked onchange="loadBoard()"> Vetëm relevante</label>
      <label class="toggle"><input type="checkbox" id="expiredToggle" onchange="loadBoard()"> Përfshi të skaduara</label>
      <span id="pollStatus"></span>
    </div>
  </div>

  <div class="board" id="board"><div class="loading-state"><div class="big-spinner"></div>Duke ngarkuar…</div></div>

<script>
const STATUSES = ["NEW","REVIEW","GO","BID","WON","LOST","DROPPED"];
const STATUS_COLORS = { NEW:"#5b8cff", REVIEW:"#fbbf24", GO:"#34d399", BID:"#8b7bff", WON:"#34d399", LOST:"#f87171", DROPPED:"#5c6270" };

const SOURCE_COLORS = {
  "GIZ": { bg: "rgba(91,140,255,0.15)", fg: "#8fb2ff" },
  "TED": { bg: "rgba(52,211,153,0.15)", fg: "#5ee0ae" },
  "UNDP": { bg: "rgba(96,165,250,0.15)", fg: "#7cb8fb" },
  "APP (Prokurimi Publik)": { bg: "rgba(251,191,36,0.15)", fg: "#fbbf24" },
  "DevelopmentAid": { bg: "rgba(232,121,249,0.15)", fg: "#e879f9" },
};
function sourceColor(name) {
  if (SOURCE_COLORS[name]) return SOURCE_COLORS[name];
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) % 360;
  return { bg: `hsla(${h},60%,55%,0.15)`, fg: `hsl(${h},70%,72%)` };
}

let allNotices = [];

function daysLeft(deadline) {
  if (!deadline) return null;
  const d = new Date(deadline);
  if (isNaN(d)) return null;
  return Math.ceil((d - new Date()) / (1000*60*60*24));
}

function deadlineChip(deadline) {
  const days = daysLeft(deadline);
  if (days === null) return '<span class="deadline dl-ok">pa afat</span>';
  let cls = "dl-ok";
  if (days <= 3) cls = "dl-urgent";
  else if (days <= 10) cls = "dl-soon";
  const label = days < 0 ? "MBYLLUR" : (days + "d");
  return `<span class="deadline ${cls}">${label}</span>`;
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}

function renderStats(notices) {
  const sources = new Set(notices.map(n => n.source));
  const urgent = notices.filter(n => { const d = daysLeft(n.deadline); return d !== null && d <= 3 && d >= 0; }).length;
  const stats = document.getElementById('stats');
  stats.innerHTML = [
    { n: notices.length, l: "Njoftime" },
    { n: sources.size, l: "Burime" },
    { n: urgent, l: "Urgjente", cls: urgent > 0 ? "urgent" : "" },
  ].map(s => `<div class="stat ${s.cls||''}"><div class="n">${s.n}</div><div class="l">${s.l}</div></div>`).join('');
}

function populateSourceFilter(notices) {
  const sel = document.getElementById('sourceFilter');
  const current = sel.value;
  const sources = [...new Set(notices.map(n => n.source))].sort();
  sel.innerHTML = '<option value="">Të gjitha burimet</option>' +
    sources.map(s => `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('');
  if (sources.includes(current)) sel.value = current;
}

async function loadBoard() {
  const relevantOnly = document.getElementById('relevantToggle').checked;
  const includeExpired = document.getElementById('expiredToggle').checked;
  const params = new URLSearchParams({ relevant_only: relevantOnly, include_expired: includeExpired });
  const res = await fetch(`/api/notices?${params}`);
  allNotices = await res.json();
  populateSourceFilter(allNotices);
  renderBoard();
}

function renderBoard() {
  const q = document.getElementById('searchInput').value.trim().toLowerCase();
  const sourceFilter = document.getElementById('sourceFilter').value;
  let notices = allNotices;
  if (sourceFilter) notices = notices.filter(n => n.source === sourceFilter);
  if (q) notices = notices.filter(n =>
    (n.title || '').toLowerCase().includes(q) || (n.buyer || '').toLowerCase().includes(q));

  renderStats(notices);

  const board = document.getElementById('board');
  if (notices.length === 0) {
    board.innerHTML = `<div class="board-empty"><span class="emoji">🔍</span>Asnjë njoftim nuk përputhet me filtrat aktualë.</div>`;
    return;
  }
  board.innerHTML = '';
  for (const status of STATUSES) {
    const col = document.createElement('div');
    col.className = 'col';
    const items = notices.filter(n => n.status === status);
    const head = document.createElement('div');
    head.className = 'col-head';
    head.innerHTML = `<span class="col-dot" style="background:${STATUS_COLORS[status]}"></span><h3>${status}</h3><span class="count">${items.length}</span>`;
    col.appendChild(head);
    if (items.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'empty-col';
      empty.textContent = 'Bosh';
      col.appendChild(empty);
    }
    items.forEach((n, i) => {
      const sc = sourceColor(n.source);
      const kws = (n.matched_keywords || '').split(',').map(k => k.trim()).filter(Boolean).slice(0, 4);
      const card = document.createElement('div');
      card.className = 'card';
      card.style.animationDelay = `${Math.min(i, 8) * 25}ms`;
      card.innerHTML = `
        <div class="badges">
          <span class="src-badge" style="background:${sc.bg};color:${sc.fg}">${escapeHtml(n.source)}</span>
          <span class="score-badge" title="Vlerësimi i relevancës">★ ${n.score}</span>
        </div>
        <div class="title">${escapeHtml(n.title)}</div>
        <div class="meta"><span class="lbl">botues:</span> ${escapeHtml(n.buyer || '—')}</div>
        <div class="meta"><span class="lbl">shpallur:</span> ${escapeHtml(n.published_date || '—')}</div>
        <div class="meta"><span class="lbl">afati:</span> ${deadlineChip(n.deadline)} ${escapeHtml(n.deadline || '')}</div>
        ${kws.length ? `<div class="kw-row">${kws.map(k => `<span class="kw-tag">${escapeHtml(k)}</span>`).join('')}</div>` : ''}
        <div class="link-row"><a href="${n.url}" target="_blank" rel="noopener">hap burimin →</a></div>
        <select class="status-select" onchange="setStatus('${n.fingerprint}', this.value)">
          ${STATUSES.map(s => `<option value="${s}" ${s===status?'selected':''}>${s}</option>`).join('')}
        </select>
      `;
      col.appendChild(card);
    });
    board.appendChild(col);
  }
}

async function setStatus(fingerprint, status) {
  await fetch(`/api/notices/${fingerprint}/status?new_status=${status}`, { method: 'POST' });
  loadBoard();
}

async function pollNow() {
  const btn = document.getElementById('pollBtn');
  const label = document.getElementById('pollBtnLabel');
  btn.classList.add('loading');
  btn.disabled = true;
  label.textContent = 'Duke kontrolluar…';
  document.getElementById('pollStatus').textContent = '';
  try {
    const res = await fetch('/api/poll', { method: 'POST' });
    const data = await res.json();
    const total = data.results.reduce((sum, r) => sum + (r.new || 0), 0);
    document.getElementById('pollStatus').textContent = `${total} njoftime të reja · ${new Date().toLocaleTimeString('sq-AL')}`;
  } finally {
    btn.classList.remove('loading');
    btn.disabled = false;
    label.textContent = '↻ Kontrollo tani';
    loadBoard();
  }
}

loadBoard();
setInterval(loadBoard, 30000);
</script>
</body>
</html>
"""
