import os, json, datetime as dt, sys, time, re, pathlib
import pandas as pd

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# --- Google Sheets ---
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# ================== CONFIG ==================
SPREADSHEET_NAME = os.getenv("SHEET_NAME", "Gold Prices (MMTC-PAMP)")
WORKSHEET_NAME  = os.getenv("SHEET_TAB",  "Daily")
URL = "https://www.mmtcpamp.com/shop/gold"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "90000"))
HEADLESS = os.getenv("HEADLESS", "true").lower() != "false"
OUTDIR = pathlib.Path(".")
DUMP_DIR = OUTDIR / "json_dumps"
HAR_PATH = OUTDIR / "network.har"
# ============================================

def log(msg: str):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------- Google Sheets ----------
def gs_client():
    cred_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not cred_json:
        raise RuntimeError("Secret GOOGLE_SERVICE_ACCOUNT_JSON is missing.")
    creds = Credentials.from_service_account_info(json.loads(cred_json), scopes=SCOPES)
    return gspread.authorize(creds)

def open_or_create_sheet(gc, spreadsheet_name, worksheet_name):
    try:
        sh = gc.open(spreadsheet_name)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(spreadsheet_name)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows="100", cols="26")
    return sh, ws

# ---------- Helpers ----------
COOKIE_TEXTS = ["accept","agree","got it","allow","ok","okay","i agree","accept all","continue","close"]
def try_dismiss_overlays(page):
    for txt in COOKIE_TEXTS:
        sel = f"xpath=//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{txt}')]"
        try:
            for i in range(min(page.locator(sel).count(), 3)):
                try: page.locator(sel).nth(i).click(timeout=1200)
                except: pass
        except: pass

def auto_scroll(page, steps=12, pause=300):
    for _ in range(steps):
        page.mouse.wheel(0, 1600)
        page.wait_for_timeout(pause)

def extract_rows_from_dom(page):
    js = """
    () => {
      const results = [];
      const els = Array.from(document.querySelectorAll('*')).filter(el => el.textContent && el.textContent.includes('₹'));
      for (const p of els) {
        let card = p.closest('div[class*="MuiBox-root"]') || p.closest('div');
        if (!card) continue;
        let nameEl = card.querySelector('p');
        let name = nameEl ? nameEl.textContent.trim() : '';
        let price = p.textContent.trim();
        if (name && price && price.includes('₹')) results.push([name, price]);
      }
      const map = new Map();
      for (const [n, pr] of results) map.set(n, pr);
      return Array.from(map.entries()).map(([n, pr]) => [n, pr]);
    }
    """
    try:
        return page.evaluate(js)
    except:
        return []

def _walk_for_products(obj, found):
    if isinstance(obj, dict):
        lower = {k.lower(): k for k in obj.keys()}
        name_key = next((lower.get(k) for k in ["name","title","productname","product_name","label","sku_name"]), None)
        price_key = next((lower.get(k) for k in ["price","saleprice","sellingprice","amount","value","mrp","offerprice"]), None)
        if name_key and price_key:
            name = str(obj[name_key]).strip()
            price_val = obj[price_key]
            price = f"₹{price_val}" if isinstance(price_val, (int,float)) else str(price_val)
            if name and (("₹" in price) or re.search(r"\d", price)):
                found.append([name, price])
        for v in obj.values():
            _walk_for_products(v, found)
    elif isinstance(obj, list):
        for it in obj:
            _walk_for_products(it, found)

def build_context(p, headless):
    # Ensure dump dir exists
    DUMP_DIR.mkdir(exist_ok=True)
    # Context with HAR capture + Indian headers/geolocation
    browser = p.chromium.launch(
        headless=headless,
        args=["--disable-gpu","--no-sandbox"]
    )
    context = browser.new_context(
        record_har_path=str(HAR_PATH),
        record_har_mode="minimal",
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        geolocation={"longitude":77.2090,"latitude":28.6139},  # New Delhi
        permissions=["geolocation"],
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1440, "height": 1024},
        extra_http_headers={
            "Accept-Language":"en-IN,en;q=0.9",
            "DNT":"1",
            "Sec-Fetch-Site":"same-origin",
            "Sec-Fetch-Mode":"navigate",
            "Sec-Fetch-Dest":"document"
        }
    )
    return browser, context

def scrape_once(timeout_ms=PAGE_TIMEOUT_MS, headless=HEADLESS):
    log(f"Scrape try (headless={headless})…")
    with sync_playwright() as p:
        browser, ctx = build_context(p, headless)
        page = ctx.new_page()

        json_bodies = []
        def on_response(resp):
            ctype = (resp.headers or {}).get("content-type","").lower()
            if "application/json" in ctype:
                try:
                    data = resp.json()
                    json_bodies.append((resp.url, data))
                except: pass
        page.on("response", on_response)

        page.goto(URL, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms//2)
        except PWTimeout:
            pass

        try_dismiss_overlays(page)
        auto_scroll(page)
        try_dismiss_overlays(page)

        # save DOM artifacts early
        try: page.screenshot(path="page.png", full_page=True)
        except: pass
        try: open("page.html","w",encoding="utf-8").write(page.content())
        except: pass

        # save JSON responses for offline analysis
        for idx, (u, blob) in enumerate(json_bodies[:50]):
            try:
                safe = re.sub(r"[^a-zA-Z0-9._-]", "_", u[:100])
                with open(DUMP_DIR / f"{idx:02d}_{safe}.json", "w", encoding="utf-8") as f:
                    json.dump(blob, f, ensure_ascii=False, indent=2)
            except: pass

        # 1) JSON-first parse
        candidates = []
        for _, blob in json_bodies:
            try: _walk_for_products(blob, candidates)
            except: continue

        # 2) DOM fallback parse
        if not candidates:
            try:
                page.wait_for_function(
                    """() => {
                        const body = document.body?.innerText || '';
                        const priceLike = document.querySelector('[class*=price], span:has-text("₹"), p:has-text("₹")');
                        return body.includes('₹') || !!priceLike;
                    }""",
                    timeout=timeout_ms//2
                )
            except PWTimeout:
                log("Timeout waiting for price hints; extracting anyway…")
            rows_dom = extract_rows_from_dom(page)
        else:
            rows_dom = []

        ctx.close(); browser.close()

    # merge
    uniq = {}
    for n, pz in (candidates + rows_dom):
        if n and pz: uniq[n] = pz
    rows = [[n, p] for n, p in uniq.items()]

    df = pd.DataFrame(rows, columns=["Product Name","Price"]).drop_duplicates(subset=["Product Name"])
    if not df.empty:
        df.insert(0, "Date", dt.datetime.now().strftime("%Y-%m-%d"))
    return df

def scrape_with_retry():
    # Try headless first (faster), then headful (some sites block headless)
    df = scrape_once(headless=True)
    if not df.empty: return df
    log("Headless yielded 0 rows; retrying in headful mode…")
    df = scrape_once(headless=False)
    return df

# ---------- Write/Upsert ----------
def upsert_sheet(df_today: pd.DataFrame):
    log("Authorizing Google Sheets…")
    gc = gs_client()
    _, ws = open_or_create_sheet(gc, SPREADSHEET_NAME, WORKSHEET_NAME)

    log("Reading existing sheet (if any)…")
    existing = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")
    if existing.empty:
        log("Empty sheet → writing fresh data.")
        ws.clear()
        set_with_dataframe(ws, df_today, include_index=False, include_column_header=True, resize=True)
        return

    for col in ["Date","Product Name","Price"]:
        if col not in existing.columns: existing[col] = pd.NA
    existing = existing[["Date","Product Name","Price"]]

    combined = pd.concat([existing, df_today], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Date","Product Name"], keep="last")

    log(f"Writing {len(combined)} total rows after merge/dedup…")
    ws.clear()
    set_with_dataframe(ws, combined, include_index=False, include_column_header=True, resize=True)

# ---------- Main ----------
if __name__ == "__main__":
    try:
        df = scrape_with_retry()

        print("\n=== Scraped DataFrame (preview) ===")
        print(df.head(15))
        print("Total rows scraped:", len(df), "\n")

        if df.empty:
            log("No data scraped — failing the job so you inspect artifacts.")
            raise RuntimeError("No data scraped")

        upsert_sheet(df)
        log("Done ✅")
    except Exception as e:
        log(f"ERROR: {e}")
        raise
