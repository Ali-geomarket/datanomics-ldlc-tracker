#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Datanomics - LDLC tracker (brand-specific)

- Reads a config JSON (datanomics/configs/<brand>.json)
- Scrapes listing pages to get product refs + names + product URLs
- Enriches each product from its product page using JSON-LD:
    price, availability, ratings, identifiers (gtin/mpn/sku)
- Writes/updates an Excel history file (one per brand)
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


# -------------------------
# Monitoring: empty runs
# -------------------------

MAX_EMPTY_RUNS = 3


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

def _normalize_spaces(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


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

        name = a.get_text(strip=True) or ""
        if not name:
            parent = a.find_parent()
            if parent:
                title = parent.select_one(".title, .title-3, h3, .txt span")
                if title:
                    name = title.get_text(strip=True)

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
    Returns enriched fields from JSON-LD when available.
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
                # product-like node
                if node.get("@type") in ("Product", ["Product"]) or "offers" in node:
                    if "brand" in node:
                        b = node.get("brand")
                        if isinstance(b, dict):
                            out["brand"] = b.get("name") or out["brand"]
                        elif isinstance(b, str):
                            out["brand"] = b

                    out["mpn"] = node.get("mpn") or out["mpn"]
                    out["sku"] = node.get("sku") or out["sku"]
                    out["gtin13"] = node.get("gtin13") or out["gtin13"]

                    ar = node.get("aggregateRating")
                    if isinstance(ar, dict):
                        out["ratingValue"] = ar.get("ratingValue") or out["ratingValue"]
                        out["reviewCount"] = ar.get("reviewCount") or ar.get("ratingCount") or out["reviewCount"]

                    offers = node.get("offers")
                    if isinstance(offers, dict):
                        if "price" in offers and out["price_eur"] is None:
                            try:
                                out["price_eur"] = float(str(offers["price"]).replace(",", "."))
                            except Exception:
                                pass
                        out["availability"] = offers.get("availability") or out["availability"]

                    if isinstance(offers, list):
                        # take the "best" offer (first with a price)
                        for of in offers:
                            if not isinstance(of, dict):
                                continue
                            if "price" in of and out["price_eur"] is None:
                                try:
                                    out["price_eur"] = float(str(of["price"]).replace(",", "."))
                                except Exception:
                                    pass
                            out["availability"] = of.get("availability") or out["availability"]

                stack.extend(node.values())

            elif isinstance(node, list):
                stack.extend(node)

    return out


def extract_availability_label(soup: BeautifulSoup) -> str:
    """
    Best-effort 'human readable' label (ex: 'En stock', 'Livré après-demain').
    This is optional; DOM can change, so keep it tolerant.
    """
    candidates = []
    for sel in [".stock", ".product-stock", ".availability", ".in-stock", ".shipping", ".delivery"]:
        el = soup.select_one(sel)
        if el:
            txt = " ".join(el.stripped_strings)
            txt = _normalize_spaces(txt)
            if txt and len(txt) <= 120:
                candidates.append(txt)
    return candidates[0] if candidates else None


def enrich_from_product_page(url: str) -> dict:
    soup = get_soup(url)
    enriched = extract_product_jsonld(soup)

    # fallback for price if JSON-LD missing
    if enriched.get("price_eur") is None:
        page_txt = soup.get_text(" ", strip=True)
        enriched["price_eur"] = extract_cash_price(page_txt)

    enriched["availability_label"] = extract_availability_label(soup)
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
        "url_produit",
        "brand",
        "availability",
        "availability_label",
        "ratingValue",
        "reviewCount",
        "gtin13",
        "mpn",
        "sku",
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

    df_out = df_merged.reset_index()
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

    # Enrich from product pages
    enriched_rows = []
    for i, r in enumerate(all_rows[:MAX_PRODUCT_PAGES_PER_RUN], start=1):
        print(f"  [{i}/{len(all_rows)}] {r['reference']} - enrich…")
        time.sleep(SLEEP_BETWEEN_PRODUCTS_SEC)
        try:
            enriched = enrich_from_product_page(r["url_produit"])
        except Exception as e:
            print(f"    !! enrich KO {r['reference']} : {e}")
            enriched = {}

        row = {
            "reference": r["reference"],
            "nom": r["nom"],
            "url_produit": r["url_produit"],
            "price_eur": enriched.get("price_eur") or r.get("prix_listing"),
            "brand": enriched.get("brand"),
            "availability": enriched.get("availability"),
            "availability_label": enriched.get("availability_label"),
            "ratingValue": enriched.get("ratingValue"),
            "reviewCount": enriched.get("reviewCount"),
            "gtin13": enriched.get("gtin13"),
            "mpn": enriched.get("mpn"),
            "sku": enriched.get("sku"),
        }
        enriched_rows.append(row)

    df_run = pd.DataFrame(enriched_rows)
    print(df_run.head(10).to_string(index=False))

    update_excel_history(df_run, excel_file=excel_file)


if __name__ == "__main__":
    # Default: iPhone (simple)
    # In GitHub Actions we will call: python datanomics/src/run.py datanomics/configs/iphone.json
    import sys
    config = sys.argv[1] if len(sys.argv) > 1 else "datanomics/configs/iphone.json"
    run_brand(config)
