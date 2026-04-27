#!/usr/bin/env python3
"""
Company Contact Enricher (Hybrid: HTML + Browser + Google Places + Facebook)
===========================================================================
Reads company names from a spreadsheet, finds their website, email,
phone number, and LinkedIn profile, and writes results to a new spreadsheet.

Hybrid approach with fallbacks:
  1. HTML scraping (fast)
  2. Browser automation (JS-heavy sites)
  3. Google Places API (official business data)
  4. Facebook business pages (last resort)

Usage:
    export GOOGLE_PLACES_API_KEY="your_key_here"
    python enrich_companies.py input.xlsx
    python enrich_companies.py "https://docs.google.com/spreadsheets/d/..." --column "Company Name"

To get Google Places API key:
  1. Go to https://console.cloud.google.com/
  2. Create a new project
  3. Enable "Places API"
  4. Create API credentials (API key)
  5. Set environment variable: GOOGLE_PLACES_API_KEY="your_key"

Requirements:
    pip install requests beautifulsoup4 openpyxl pandas tqdm playwright googlemaps facebook-scraper
"""

import argparse
import re
import time
import random
import sys
import os
import logging
from pathlib import Path
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from tqdm import tqdm

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    import googlemaps
    GOOGLE_PLACES_AVAILABLE = True
except ImportError:
    GOOGLE_PLACES_AVAILABLE = False

try:
    from facebook_scraper import get_page
    FACEBOOK_AVAILABLE = True
except ImportError:
    FACEBOOK_AVAILABLE = False

# ─── Configuration ─────────────────────────────────────────────────────────────

DELAY_BETWEEN_REQUESTS = (1.5, 3.5)   # seconds (min, max) — be polite
REQUEST_TIMEOUT        = 10            # seconds per HTTP request
MAX_RETRIES            = 2
MAX_WORKERS            = 4             # parallel workers for enrichment
GOOGLE_PLACES_API_KEY  = os.getenv("GOOGLE_PLACES_API_KEY")  # Load from environment variable
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── HTTP helpers ───────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def _get(url: str, **kwargs) -> requests.Response | None:
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, timeout=REQUEST_TIMEOUT, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                log.debug(f"GET failed for {url}: {e}")
    return None


def _sleep():
    time.sleep(random.uniform(*DELAY_BETWEEN_REQUESTS))


# ── Web search (DuckDuckGo HTML — no API key needed) ──────────────────────────

def search_web(query: str, num_results: int = 5) -> list[str]:
    """Return a list of result URLs from DuckDuckGo for the given query."""
    url = "https://html.duckduckgo.com/html/"
    try:
        r = SESSION.post(
            url,
            data={"q": query, "b": "", "kl": ""},
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        )
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        for a in soup.select("a.result__url"):
            href = a.get("href", "")
            if href and href.startswith("http"):
                links.append(href)
            if len(links) >= num_results:
                break
        return links
    except Exception as e:
        log.debug(f"DuckDuckGo search failed: {e}")
        return []


# ── Website discovery ──────────────────────────────────────────────────────────

SKIP_DOMAINS = {
    "linkedin.com", "facebook.com", "twitter.com", "instagram.com",
    "youtube.com", "wikipedia.org", "yelp.com", "crunchbase.com",
    "bloomberg.com", "reuters.com", "glassdoor.com", "indeed.com",
    "zoominfo.com", "dnb.com", "trustpilot.com",
}


def find_website(company_name: str) -> str:
    """Search for the company's official website."""
    queries = [
        f'"{company_name}" official website',
        f"{company_name} company site",
    ]
    for query in queries:
        results = search_web(query, num_results=6)
        for url in results:
            domain = urlparse(url).netloc.lower().replace("www.", "")
            if not any(skip in domain for skip in SKIP_DOMAINS):
                return url
        _sleep()
    return ""


# ── Contact info extraction from website ──────────────────────────────────────

EMAIL_RE   = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PHONE_RE   = re.compile(
    r"(?:(?:\+|00)44[\s\-]?)?(?:0[\s\-]?)?(?:\d[\s\-]?){9,11}"  # UK
    r"|(?:\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}"  # US
    r"|(?:\+\d{1,3}[\s\-.]?)?\(?\d{2,4}\)?[\s\-.]?\d{3,4}[\s\-.]?\d{3,4}"  # International
)

SPAM_EMAILS = {
    "example.com", "domain.com", "email.com", "yourmail.com",
    "sentry.io", "wixpress.com", "squarespace.com", "wordpress.com",
    "googletagmanager.com", "schema.org", "w3.org",
}

SPAM_EMAIL_PREFIXES = {
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "webmaster", "postmaster", "mailer-daemon",
}


def _clean_phone(raw: str) -> str:
    digits = re.sub(r"[^\d+]", "", raw)
    return raw.strip() if len(digits) >= 7 else ""


def _is_valid_email(email: str) -> bool:
    parts = email.lower().split("@")
    if len(parts) != 2:
        return False
    local, domain = parts
    if domain in SPAM_EMAILS:
        return False
    if local in SPAM_EMAIL_PREFIXES:
        return False
    if email.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
        return False
    # Must have a real TLD
    if "." not in domain:
        return False
    return True


def extract_contacts_from_page(html: str, base_domain: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    emails: list[str] = []
    phones: list[str] = []

    # ── Priority 1: mailto: and tel: href links (most reliable) ──────────────
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("mailto:"):
            email = href[7:].split("?")[0].strip()
            if email and _is_valid_email(email):
                emails.append(email)
        elif href.startswith("tel:"):
            raw = href[4:].strip().replace("%20", " ")
            cleaned = _clean_phone(raw)
            if cleaned:
                phones.append(cleaned)

    # ── Priority 2: Schema.org / JSON-LD structured data ─────────────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json
            data = json.loads(script.string or "")
            # Flatten nested structures
            def _extract_ld(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in ("email", "contactEmail") and isinstance(v, str) and _is_valid_email(v):
                            emails.append(v)
                        elif k in ("telephone", "phone", "faxNumber") and isinstance(v, str):
                            c = _clean_phone(v)
                            if c:
                                phones.append(c)
                        else:
                            _extract_ld(v)
                elif isinstance(obj, list):
                    for item in obj:
                        _extract_ld(item)
            _extract_ld(data)
        except Exception:
            pass

    # ── Priority 3: Plain text scan (fallback) ────────────────────────────────
    # Remove script/style noise before scanning
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)

    for e in EMAIL_RE.findall(text):
        if _is_valid_email(e) and e not in emails:
            emails.append(e)

    for raw_phone in PHONE_RE.findall(text):
        cleaned = _clean_phone(raw_phone)
        if cleaned and cleaned not in phones:
            phones.append(cleaned)

    # ── Pick best email: prefer domain-matching, then any valid ───────────────
    domain_emails = [e for e in emails if base_domain in e.lower()]
    best_email = (domain_emails or emails or [""])[0]

    # ── Pick best phone: prefer longer (more digits = more specific) ──────────
    phones_sorted = sorted(set(phones), key=lambda p: len(re.sub(r"[^\d]", "", p)), reverse=True)
    best_phone = phones_sorted[0] if phones_sorted else ""

    return {"email": best_email, "phone": best_phone}


def _discover_contact_pages(soup: BeautifulSoup, base: str) -> list[str]:
    """Find links to contact/about pages within the site."""
    contact_keywords = re.compile(
        r"contact|about|reach|touch|enquir|inquir|support|help|team|office|location|connect",
        re.IGNORECASE,
    )
    found = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(strip=True).lower()
        # Skip external links, anchors, mailto/tel
        if href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue
        full = urljoin(base, href)
        parsed_full = urlparse(full)
        parsed_base = urlparse(base)
        # Must be same domain
        if parsed_full.netloc and parsed_full.netloc != parsed_base.netloc:
            continue
        # Match on URL path or link text
        if contact_keywords.search(href) or contact_keywords.search(text):
            if full not in found:
                found.append(full)
    return found[:8]  # Limit to avoid crawling whole site


def scrape_website(website_url: str) -> dict:
    """Scrape homepage and contact-related pages for email and phone."""
    result = {"email": "", "phone": ""}
    if not website_url:
        return result

    parsed   = urlparse(website_url)
    base     = f"{parsed.scheme}://{parsed.netloc}"
    base_dom = parsed.netloc.lower().replace("www.", "").split(".")[0]

    # Canonical contact page paths to always try
    contact_paths = [
        "/contact",
        "/contact-us",
        "/contact_us",
        "/contacts",
        "/contactus",
        "/about",
        "/about-us",
        "/about_us",
        "/about/contact",
        "/reach-us",
        "/get-in-touch",
        "/getintouch",
        "/enquire",
        "/enquiries",
        "/support",
        "/help",
        "/team",
        "/our-team",
        "/location",
        "/locations",
        "/offices",
    ]
    pages_to_try = [website_url] + [urljoin(base, p) for p in contact_paths]

    homepage_soup = None
    visited = set()

    for page_url in pages_to_try:
        if page_url in visited:
            continue
        visited.add(page_url)

        r = _get(page_url)
        if not r:
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        # On the homepage, also discover contact-page links dynamically
        if page_url == website_url:
            homepage_soup = soup
            discovered = _discover_contact_pages(soup, base)
            for d in discovered:
                if d not in pages_to_try:
                    pages_to_try.append(d)

        found = extract_contacts_from_page(r.text, base_dom)
        if found["email"] and not result["email"]:
            result["email"] = found["email"]
        if found["phone"] and not result["phone"]:
            result["phone"] = found["phone"]
        if result["email"] and result["phone"]:
            break
        _sleep()

    return result


def scrape_website_browser(website_url: str) -> dict:
    """Scrape website using browser automation (fallback for JS-heavy sites)."""
    if not PLAYWRIGHT_AVAILABLE or not website_url:
        return {"email": "", "phone": ""}
    
    result = {"email": "", "phone": ""}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, timeout=10000)
            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1280, "height": 720},
                ignore_https_errors=True
            )
            page = context.new_page()
            
            parsed = urlparse(website_url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            base_dom = parsed.netloc.lower().replace("www.", "").split(".")[0]

            contact_paths = [
                "/contact", "/contact-us", "/contact_us", "/contacts",
                "/about", "/about-us", "/reach-us", "/get-in-touch",
                "/enquire", "/enquiries", "/support", "/team",
            ]
            pages_to_try = [urljoin(base, p) for p in contact_paths] + [website_url]
            visited = set()
            
            for page_url in pages_to_try:
                if page_url in visited:
                    continue
                visited.add(page_url)
                try:
                    page.goto(page_url, wait_until="domcontentloaded", timeout=8000)
                    time.sleep(0.5)  # Let JS render
                    html = page.content()
                    found = extract_contacts_from_page(html, base_dom)
                    if found["email"] and not result["email"]:
                        result["email"] = found["email"]
                    if found["phone"] and not result["phone"]:
                        result["phone"] = found["phone"]
                    if result["email"] and result["phone"]:
                        break
                except Exception as e:
                    log.debug(f"Browser scrape failed for {page_url}: {e}")
                    continue
            
            browser.close()
    except Exception as e:
        log.debug(f"Browser automation failed for {website_url}: {e}")
    
    return result



# ── Google Places API lookup ────────────────────────────────────────────────────

def lookup_google_places(company_name: str, location: str = "UK") -> dict:
    """Look up company on Google Places API to find phone and other info."""
    if not GOOGLE_PLACES_AVAILABLE or not GOOGLE_PLACES_API_KEY:
        return {"phone": "", "email": ""}
    
    try:
        gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY, timeout=10)
        query = f"{company_name} {location}"
        
        # Search for places matching the company name
        results = gmaps.places(query, type="establishment")
        
        if results["results"]:
            place = results["results"][0]  # Get top result
            phone = place.get("formatted_phone_number", "").replace(" ", "")
            
            # Try to get detailed place info including website
            place_id = place.get("place_id")
            if place_id:
                details = gmaps.place(place_id, fields=[
                    "formatted_phone_number", "website", "formatted_address"
                ])
                if details["result"]:
                    phone = details["result"].get("formatted_phone_number", phone)
                    
            return {"phone": phone, "email": ""}  # Google Places doesn't provide email
    except Exception as e:
        log.debug(f"Google Places lookup failed for {company_name}: {e}")
    
    return {"phone": "", "email": ""}


# ── Facebook business page scraper ────────────────────────────────────────────

def scrape_facebook_business(company_name: str) -> dict:
    """Scrape Facebook business page for contact info."""
    if not FACEBOOK_AVAILABLE:
        return {"phone": "", "email": ""}
    
    try:
        # Search for Facebook page URL first
        search_query = f"{company_name} site:facebook.com"
        results = search_web(search_query, num_results=3)
        
        for fb_url in results:
            if "facebook.com" in fb_url and "/pages/" in fb_url:
                try:
                    # Extract page name from URL
                    page_match = re.search(r'/pages/([^/?]+)', fb_url)
                    if page_match:
                        page_name = page_match.group(1)
                        page = get_page(page_name, timeout=10)
                        
                        phone = page.get("phone", "")
                        email = page.get("email", "")
                        
                        if phone or email:
                            log.debug(f"Facebook found for {company_name}: {phone or email}")
                            return {"phone": phone, "email": email}
                except Exception as e:
                    log.debug(f"Facebook scrape failed for {fb_url}: {e}")
                    continue
    except Exception as e:
        log.debug(f"Facebook lookup failed for {company_name}: {e}")
    
    return {"phone": "", "email": ""}


# ── LinkedIn search ────────────────────────────────────────────────────────────

def find_linkedin(company_name: str) -> str:
    """Return a LinkedIn company page URL if found."""
    query = f"site:linkedin.com/company {company_name}"
    results = search_web(query, num_results=5)
    for url in results:
        if "linkedin.com/company/" in url:
            # Normalise to clean profile URL
            match = re.search(r"(https?://(?:www\.)?linkedin\.com/company/[^/?&#]+)", url)
            if match:
                return match.group(1)
    return ""


# ── Main enrichment function ───────────────────────────────────────────────────

def enrich_company(company_name: str) -> dict:
    """Return a dict of contact info for a single company name."""
    name = company_name.strip()
    log.info(f"Enriching: {name}")

    result = {
        "company_name": name,
        "website":      "",
        "email":        "",
        "phone":        "",
        "linkedin":     "",
        "status":       "ok",
    }

    try:
        # 1. Find website
        website = find_website(name)
        result["website"] = website
        _sleep()

        # 2. Scrape contacts from website (HTML first, browser as fallback)
        if website:
            contacts = scrape_website(website)
            result.update(contacts)
            
            # If no contacts found with HTML, try browser automation
            if not contacts["email"] and not contacts["phone"]:
                log.debug(f"HTML scraping found nothing, trying browser for {name}")
                browser_contacts = scrape_website_browser(website)
                if browser_contacts["email"] or browser_contacts["phone"]:
                    result.update(browser_contacts)
                    log.debug(f"Browser found contacts for {name}")
        
        # 3. If still no phone/email, try Google Places API
        if not result["phone"]:
            log.debug(f"No phone found, trying Google Places for {name}")
            google_info = lookup_google_places(name)
            if google_info["phone"]:
                result["phone"] = google_info["phone"]
                log.debug(f"Google Places found phone for {name}")
        
        # 4. If still no contacts, try Facebook business page
        if not result["phone"] and not result["email"]:
            log.debug(f"No contacts found yet, trying Facebook for {name}")
            fb_info = scrape_facebook_business(name)
            if fb_info["phone"] or fb_info["email"]:
                result.update(fb_info)
                log.debug(f"Facebook found info for {name}")

        # 5. Find LinkedIn
        linkedin = find_linkedin(name)
        result["linkedin"] = linkedin
        _sleep()

    except Exception as e:
        result["status"] = f"error: {e}"
        log.warning(f"Failed for {name}: {e}")

    return result


# ── Spreadsheet I/O ────────────────────────────────────────────────────────────

def google_sheets_to_csv_url(url: str) -> str:
    """Convert a Google Sheets URL to a CSV export URL."""
    # Extract sheet ID from URL like: https://docs.google.com/spreadsheets/d/SHEET_ID/edit?gid=GID
    import re
    sheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
    if not sheet_match:
        raise ValueError("Invalid Google Sheets URL")
    
    sheet_id = sheet_match.group(1)
    
    # Extract gid if present (sheet tab ID)
    gid_match = re.search(r'[?&#]gid=([0-9]+)', url)
    gid = gid_match.group(1) if gid_match else "0"
    
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def read_companies(filepath: str, column: str | None) -> list[str]:
    """Read company names from an xlsx, csv file, or Google Sheets URL."""
    # Handle Google Sheets URLs
    if "docs.google.com/spreadsheets" in filepath:
        log.info("Detected Google Sheets URL — converting to CSV export")
        filepath = google_sheets_to_csv_url(filepath)
        df = pd.read_csv(filepath)
    else:
        path = Path(filepath)
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)

    if column:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found. Available: {list(df.columns)}")
        companies = df[column].dropna().astype(str).tolist()
    else:
        # Auto-detect: pick first column that looks like company names
        for col in df.columns:
            sample = df[col].dropna().head(5).astype(str).tolist()
            if all(len(s) > 1 for s in sample):
                log.info(f"Using column: '{col}'")
                companies = df[col].dropna().astype(str).tolist()
                break
        else:
            companies = df.iloc[:, 0].dropna().astype(str).tolist()

    return [c for c in companies if c.strip()]


def write_results(results: list[dict], output_path: str):
    """Write enriched results to a formatted xlsx file."""
    df = pd.DataFrame(results)
    df.to_excel(output_path, index=False, engine="openpyxl")

    wb = load_workbook(output_path)
    ws = wb.active

    # Header styling
    header_fill = PatternFill("solid", start_color="1E3A5F")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    for cell in ws[1]:
        cell.fill   = header_fill
        cell.font   = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    # Column widths
    col_widths = {
        "A": 30,  # company_name
        "B": 40,  # website
        "C": 35,  # email
        "D": 20,  # phone
        "E": 45,  # linkedin
        "F": 15,  # status
    }
    for col, width in col_widths.items():
        ws.column_dimensions[col].width = width

    # Row height for header
    ws.row_dimensions[1].height = 22

    # Zebra striping and hyperlinks
    light_fill = PatternFill("solid", start_color="EEF2F7")
    for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
        for cell in row:
            if row_idx % 2 == 0:
                cell.fill = light_fill
            # Make website column (B) clickable hyperlinks
            if cell.column == 2 and cell.value and str(cell.value).startswith("http"):
                cell.hyperlink = cell.value
                cell.font = Font(color="0563C1", underline="single")
            # Colour status column
            if cell.column == 6 and cell.value and cell.value != "ok":
                cell.font = Font(color="CC0000")

    # Freeze header row
    ws.freeze_panes = "A2"

    wb.save(output_path)
    log.info(f"Results saved → {output_path}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Enrich company names with contact information."
    )
    parser.add_argument("input", help="Path to input .xlsx/.csv file or Google Sheets URL")
    parser.add_argument(
        "--column", "-c",
        default=None,
        help="Column name containing company names (auto-detected if omitted)",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output file path (default: <input>_enriched.xlsx)",
    )
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Only process the first N companies (useful for testing)",
    )
    parser.add_argument(
        "--resume",
        default=None,
        help="Path to a partially-completed output file — skip already-enriched rows",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show detailed debug output (page URLs tried, what was found at each step)",
    )
    parser.add_argument(
        "--test",
        metavar="COMPANY",
        default=None,
        help="Test enrichment for a single company name and print results (no file needed)",
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.setLevel(logging.DEBUG)

    # ── Quick single-company test mode ────────────────────────────────────────
    if args.test:
        print(f"\nTesting enrichment for: {args.test!r}\n{'─'*50}")
        result = enrich_company(args.test)
        for k, v in result.items():
            print(f"  {k:15s}: {v or '(not found)'}")
        print(f"{'─'*50}\n")
        return

    # Resolve output path
    if args.output:
        output_path = args.output
    elif "docs.google.com/spreadsheets" in args.input:
        # For Google Sheets URLs, use a default name in current directory
        output_path = "enriched_companies.xlsx"
    else:
        input_path = Path(args.input)
        output_path = str(input_path.parent / (input_path.stem + "_enriched.xlsx"))

    # Read companies
    log.info(f"Reading companies from: {args.input}")
    companies = read_companies(args.input, args.column)
    log.info(f"Found {len(companies)} companies")

    if args.limit:
        companies = companies[: args.limit]
        log.info(f"Limiting to first {args.limit}")

    # Resume support: skip already-done rows
    done: set[str] = set()
    results: list[dict] = []
    if args.resume and Path(args.resume).exists():
        existing = pd.read_excel(args.resume)
        done     = set(existing["company_name"].dropna().astype(str).tolist())
        results  = existing.to_dict("records")
        log.info(f"Resuming — {len(done)} already done, {len(companies)-len(done)} remaining")

    # Enrich (with parallel workers)
    to_process = [c for c in companies if c not in done]
    results_lock = {}  # Track which companies are done
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(enrich_company, c): c for c in to_process}
        
        with tqdm(total=len(to_process), desc="Enriching", unit="co") as pbar:
            for future in as_completed(futures):
                row = future.result()
                results.append(row)
                pbar.update(1)
                
                # Write checkpoint every 25 rows
                if len(results) % 25 == 0:
                    write_results(results, output_path)
                    log.info(f"Checkpoint saved ({len(results)} rows)")

    # Final save
    write_results(results, output_path)
    ok    = sum(1 for r in results if r["status"] == "ok")
    found = sum(1 for r in results if r.get("email") or r.get("phone"))
    print(f"\n{'─'*50}")
    print(f"  Done!  {len(results)} companies processed")
    print(f"  ✓  {found} with at least one contact found")
    print(f"  ✗  {len(results)-ok} errors")
    print(f"  Output → {output_path}")
    print(f"{'─'*50}\n")


if __name__ == "__main__":
    main()