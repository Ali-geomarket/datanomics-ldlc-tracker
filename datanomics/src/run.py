#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Datanomics - LDLC tracker (brand-specific)

- Reads a config JSON (datanomics/configs/<brand>.json)
- Scrapes listing pages to get product refs + names + product URLs
- Enriches each product from its product page using JSON-LD:
    price, availability, ratings, identifiers (gtin/mpn/sku)
- Writes/updates an Excel history file (one per brand) with a price column per run (timestamp)

Fixes included:
- Clean product names (remove glued prices like "...659€00")
- Force identifiers (gtin13/mpn/sku) as strings to avoid Excel scientific notation
- Add availability_code (human/analysis-friendly)
- Add price_source to assess reliability
"""

import os
import re
import json
import time
import html
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.ldlc.com"

HEADERS_HTTP = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
        "Gecko/20100101 Firefox/123.0"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_PAGES_SEC = 1.2
SLEEP_BETWEEN_PRODUCTS_SEC = 0.35

# To avoid triggering LDLC protections too often
MAX_PRODUCT_PAGES_PER_RUN = 200  # safety cap

# Monitoring: empty runs
MAX_EMPTY_RUNS = 3


# -------------------------
# Utilities
# -------------------------

def _normalize_spaces(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def clean_product_name(name: str) -> str:
    """
    Remove glued prices from listing anchor text, e.g.:
    "Apple iPhone 15 128 Go Noir659€00" -> "Apple iPhone 15 128 Go Noir"
    """
    if not name:
        return name
    n = _normalize_spaces(name)

    # Remove patterns like 659€00 or 659 € 00
    n = re.sub(r"\d{1,5}\s*€\s*\d{2}\b", "", n)
    # Remove patterns like 659€ (rare)
    n = re.sub(r"\d{1,5}\s*€\b", "", n)

    return _normalize_spaces(n)


def safe_str(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None


def safe_float(x):
    if x is None:
        return None
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def normalize_availability(av: str):
    """
    Normalize schema.org availability URL to a short code for analysis.
    """
    if not av:
        return None
    a = str(av)

    if "InStock" in a:
        return "IN_STOCK"
    if "OutOfStock" in a:
        return "OUT_OF_STOCK"
    if "BackOrder" in a:
        return "BACKORDER"
    if "PreOrder" in a:
        return "PREORDER"
    if "LimitedAvailability" in a:
        return "LIMITED"
    if "SoldOut" in a:
        return "SOLD_OUT"

    return "OTHER"


# -------------------------
# Monitoring: empty runs
# -------------------------

def load_state(state_file: str) -> dict:
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"empty_runs": 0}


def save_state(state_file: str, state: dict) -> None:
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# -------------------------
# Session HTTP + retries
# -------------------------

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS_HTTP)

    retry = Retry(
        total=4,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = build_session()


def get_soup(url: str) -> BeautifulSoup:
    r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        print(f"  !! HTTP {r.status_code} sur {url}")
    r.raise_for_status()
    return BeautifulSoup(html.unescape(r.text), "lxml")


# -------------------------
# Price extraction (robust)
# -------------------------

def _is_installment_context(text: str, span_start: int, span_end: int) -> bool:
    left = max(0, span_start - 18)
    right = min(len(text), span_end + 18)
    ctx = text[left:right].lower()

    if re.search(r"(\b\d+\s*[x×]\s*$)", text[left:span_start].lower()):
        return True
    if "€/mois" in ctx or "par mois" in ctx or "mensuel" in ctx:
        return True
    if re.search(r"\b\d+\s*[x×]\s*\d", ctx):
        return True
    if "payer mensuellement" in ctx or "paiement" in ctx:
        return True
    if "a partir de" in ctx or "à partir de" in ctx:
        return True

    return False


def extract_cash_price(text: str):
    """
    Extract best candidate cash price from a text block.
    Filters monthly/installment contexts.
    """
    if not text:
        return None

    t = _normalize_spaces(text)
    pattern = re.compile(r"(\d[\d\s]*)\s*€\s*(\d{2})?")
    found = []

    for m in pattern.finditer(t):
        euros_raw = (m.group(1) or "").replace(" ", "")
        cents_raw = m.group(2)

        if not euros_raw.isdigit():
            continue

        euros = int(euros_raw)
        cents = int(cents_raw) if (cents_raw and cents_raw.isdigit()) else 0
        val = euros + cents / 100.0
        found.append((m.start(), m.end(), val))

    if not found:
        return None

    candidates = []
    for start, end, val in found:
        if _is_installment_context(t, start, end):
            continue
        candidates.append(val)

    candidates = [v for v in candidates if v >= 30]

    if not candidates:
        return max(v for (_s, _e, v) in found)

    return max(candidates)


# -------------------------
# Listing scraping
# -------------------------

def find_product_container(a_tag):
    node = a_tag
    for _ in range(10):
        if node is None:
            break
        if node.select_one(".price"):
            return node
        node = node.parent
    return None


def scrape_listing_page(url: str):
    soup = get_soup(url)
    links = soup.select('a[href^="/fiche/"]')

    rows = []
    seen_refs = set()

    for a in links:
        href = a.get("href") or ""
        m = re.search(r"^/fiche/(PB[0-9A-Z]+)\.html$", href, re.I)
        if not m:
            continue

        ref = m.group(1)
        if ref in seen_refs:
            continue

        raw_name = a.get_text(strip=True) or ""
        name = clean_product_name(raw_name)

        if not name:
            parent = a.find_parent()
            if parent:
                title = parent.select_one(".title, .title-3, h3, .txt span")
                if title:
                    name = clean_product_name(title.get_text(strip=True))

        if not name:
            continue

        abs_url = urljoin(BASE_URL, href)

        # listing price (best-effort)
        price_eur = None
        container = find_product_container(a)
        if container:
            el_price = container.select_one(".price")
            if el_price:
                txt_price = " ".join(el_price.stripped_strings)
                price_eur = extract_cash_price(txt_price)

        rows.append(
            {
                "reference": ref,
                "nom": name,
                "url_produit": abs_url,
                "prix_listing": price_eur,
            }
        )
        seen_refs.add(ref)

    return rows


# -------------------------
# Product page enrichment (JSON-LD)
# -------------------------

def extract_product_jsonld(soup: BeautifulSoup) -> dict:
    """
    Extract enriched fields from JSON-LD when available.
    Forces IDs to string to avoid Excel scientific notation.
    """
    out = {
        "price_eur": None,
        "availability": None,
        "ratingValue": None,
        "reviewCount": None,
        "gtin13": None,
        "mpn": None,
        "sku": None,
        "brand": None,
    }

    scripts = soup.select('script[type="application/ld+json"]')
    for s in scripts:
        txt = s.string or s.text
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                # Detect product-like node
                if node.get("@type") in ("Product", ["Product"]) or "offers" in node:
                    b = node.get("brand")
                    if isinstance(b, dict):
                        out["brand"] = safe_str(b.get("name")) or out["brand"]
                    elif isinstance(b, str):
                        out["brand"] = safe_str(b) or out["brand"]

                    out["mpn"] = safe_str(node.get("mpn")) or out["mpn"]
                    out["sku"] = safe_str(node.get("sku")) or out["sku"]
                    out["gtin13"] = safe_str(node.get("gtin13")) or out["gtin13"]

                    ar = node.get("aggregateRating")
                    if isinstance(ar, dict):
                        out["ratingValue"] = safe_float(ar.get("ratingValue")) or out["ratingValue"]
                        rc = ar.get("reviewCount") or ar.get("ratingCount")
                        out["reviewCount"] = int(rc) if str(rc).isdigit() else out["reviewCount"]

                    offers = node.get("offers")
                    if isinstance(offers, dict):
                        if out["price_eur"] is None and "price" in offers:
                            out["price_eur"] = safe_float(offers.get("price"))
                        out["availability"] = safe_str(offers.get("availability")) or out["availability"]

                    elif isinstance(offers, list):
                        for of in offers:
                            if not isinstance(of, dict):
                                continue
                            if out["price_eur"] is None and "price" in of:
                                out["price_eur"] = safe_float(of.get("price"))
                            out["availability"] = safe_str(of.get("availability")) or out["availability"]

                stack.extend(node.values())

            elif isinstance(node, list):
                stack.extend(node)

    return out


def extract_availability_label(soup: BeautifulSoup) -> str:
    """
    Best-effort human label from DOM (can change).
    """
    candidates = []
    for sel in [".stock", ".product-stock", ".availability", ".in-stock", ".shipping", ".delivery"]:
        el = soup.select_one(sel)
        if el:
            txt = _normalize_spaces(" ".join(el.stripped_strings))
            if txt and len(txt) <= 140:
                candidates.append(txt)
    return candidates[0] if candidates else None


def enrich_from_product_page(url: str) -> dict:
    soup = get_soup(url)
    enriched = extract_product_jsonld(soup)

    # fallback for price if JSON-LD missing
    if enriched.get("price_eur") is None:
        page_txt = soup.get_text(" ", strip=True)
        enriched["price_eur"] = extract_cash_price(page_txt)
        enriched["price_source"] = "fallback_text"
    else:
        enriched["price_source"] = "jsonld"

    enriched["availability_label"] = extract_availability_label(soup)
    enriched["availability_code"] = normalize_availability(enriched.get("availability"))
    return enriched


# -------------------------
# Excel history
# -------------------------

def update_excel_history(df_run: pd.DataFrame, excel_file: str, sheet_name: str = "Suivi"):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    df_run = df_run.copy()
    df_run[timestamp] = df_run["price_eur"]
    df_run = df_run.set_index("reference")

    fixed_cols = [
        "nom",
        "brand",
        "mpn",
        "sku",
        "gtin13",
        "availability_code",
        "availability",
        "availability_label",
        "ratingValue",
        "reviewCount",
        "price_source",
        "url_produit",
    ]

    keep_cols = [c for c in fixed_cols if c in df_run.columns] + [timestamp]
    df_run = df_run[keep_cols]

    # Load existing
    if os.path.exists(excel_file):
        try:
            df_hist = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl")
            if not df_hist.empty and "reference" in df_hist.columns:
                df_hist = df_hist.set_index("reference")
            else:
                df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))
        except Exception as e:
            print(f"Lecture Excel impossible ({e}). Recréation.")
            df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))
    else:
        df_hist = pd.DataFrame().set_index(pd.Index([], name="reference"))

    # Merge
    df_merged = df_hist.combine_first(df_run)
    df_merged[timestamp] = df_run[timestamp].reindex(df_merged.index)

    # Update fixed fields if new values exist
    for col in fixed_cols:
        if col in df_run.columns:
            if col in df_merged.columns:
                df_merged[col] = df_run[col].combine_first(df_merged[col])
            else:
                df_merged[col] = df_run[col]

    # Final output ordering: fixed columns first + all timestamps at the end
    df_out = df_merged.reset_index()

    # Identify timestamp columns (runs): they match "YYYY-MM-DD HH:MM:SS"
    ts_cols = [c for c in df_out.columns if re.match(r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$", str(c))]
    stable_cols = [c for c in ["reference"] + fixed_cols if c in df_out.columns]
    ordered_cols = stable_cols + sorted(ts_cols)

    # Keep any unexpected columns (just in case) at the end
    remaining = [c for c in df_out.columns if c not in ordered_cols]
    df_out = df_out[ordered_cols + remaining]

    # Sort by latest timestamp price then name
    df_out = df_out.sort_values(by=[timestamp, "nom"], na_position="last")

    os.makedirs(os.path.dirname(excel_file), exist_ok=True)
    with pd.ExcelWriter(excel_file, engine="openpyxl", mode="w") as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Excel mis à jour : {excel_file} | run={timestamp} | produits={len(df_run)}")


# -------------------------
# Main runner
# -------------------------

def run_brand(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    listing_pages = cfg["listing_pages"]
    excel_file = cfg["output_excel"]
    state_file = cfg["state_file"]

    state = load_state(state_file)

    all_rows = []
    seen = set()

    for url in listing_pages:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Listing: {url}")
        try:
            page_rows = scrape_listing_page(url)
        except Exception as e:
            print(f"  !! Erreur listing ({url}) : {e}")
            page_rows = []

        print(f"  -> {len(page_rows)} produits détectés")
        for r in page_rows:
            if r["reference"] not in seen:
                all_rows.append(r)
                seen.add(r["reference"])

        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    if not all_rows:
        state["empty_runs"] = int(state.get("empty_runs", 0)) + 1
        save_state(state_file, state)
        print(f"Aucun produit récupéré. empty_runs={state['empty_runs']} (seuil={MAX_EMPTY_RUNS})")
        if state["empty_runs"] >= MAX_EMPTY_RUNS:
            raise RuntimeError(
                f"Aucun produit récupéré pendant {state['empty_runs']} runs consécutifs."
            )
        return

    state["empty_runs"] = 0
    save_state(state_file, state)

    enriched_rows = []
    for i, r in enumerate(all_rows[:MAX_PRODUCT_PAGES_PER_RUN], start=1):
        print(f"  [{i}/{len(all_rows)}] {r['reference']} - enrich…")
        time.sleep(SLEEP_BETWEEN_PRODUCTS_SEC)

        enriched = {}
        try:
            enriched = enrich_from_product_page(r["url_produit"])
        except Exception as e:
            print(f"    !! enrich KO {r['reference']} : {e}")
            enriched = {
                "price_eur": None,
                "availability": None,
                "availability_label": None,
                "availability_code": None,
                "ratingValue": None,
                "reviewCount": None,
                "gtin13": None,
                "mpn": None,
                "sku": None,
                "brand": None,
                "price_source": None,
            }

        # Decide price + source
        price_eur = enriched.get("price_eur")
        price_source = enriched.get("price_source")

        if price_eur is None and r.get("prix_listing") is not None:
            price_eur = r.get("prix_listing")
            price_source = "listing"

        if price_eur is None:
            price_source = price_source or "missing"

        row = {
            "reference": r["reference"],
            "nom": clean_product_name(r["nom"]),
            "url_produit": r["url_produit"],
            "price_eur": price_eur,
            "price_source": price_source,
            "brand": safe_str(enriched.get("brand")),
            "availability": safe_str(enriched.get("availability")),
            "availability_code": safe_str(enriched.get("availability_code")),
            "availability_label": safe_str(enriched.get("availability_label")),
            "ratingValue": enriched.get("ratingValue"),
            "reviewCount": enriched.get("reviewCount"),
            "gtin13": safe_str(enriched.get("gtin13")),
            "mpn": safe_str(enriched.get("mpn")),
            "sku": safe_str(enriched.get("sku")),
        }
        enriched_rows.append(row)

    df_run = pd.DataFrame(enriched_rows)
    print(df_run.head(10).to_string(index=False))

    update_excel_history(df_run, excel_file=excel_file)


if __name__ == "__main__":
    # In GitHub Actions:
    # python datanomics/src/run.py datanomics/configs/iphone.json
    import sys
    config = sys.argv[1] if len(sys.argv) > 1 else "datanomics/configs/iphone.json"
    run_brand(config)
