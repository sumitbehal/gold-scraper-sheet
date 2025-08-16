import os, json, datetime as dt, sys, time, re
import pandas as pd

# --- Playwright ---
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
PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "90000"))  # 90s
HEADLESS = os.getenv("HEADLESS", "true").lower() != "false"   # set HEADLESS=false to see full browser in CI
# ============================================

def log(msg: str):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------- Google Sheets helpers ----------
def gs_client():
    cred_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not cred_json:
        raise RuntimeError("Secret GOOGLE_SERVICE_ACCOUNT_JSON is missing in GitHub → Settings → Secrets → Actions.")
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

# ---------- Utilities ----------
COOKIE_BUTTON_TEXTS = [
    "accept", "agree", "got it", "allow", "ok", "okay", "i agree", "accept all", "continue", "close"
]

def try_dismiss_overlays(page):
    # Click common cookie/consent buttons if present
    for txt in COOKIE_BUTTON_TEXTS:
        loc = page.locator(
            f"xpath=//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{txt}')]"
        )
        count = 0
        try:
            count = loc.count()
        except Exception:
            pass
        if count:
            for i in range(min(count, 3)):
                try:
                    loc.nth(i).click(timeout=1500)
                except Exception:
                    pass

def auto_scroll(page, steps=10, pause_ms=350):
    # Trigger lazy-load
    for _ in range(steps):
        page.mouse.wheel(0, 1600)
        page.wait_for_timeout(pause_ms)

def extract_rows_from_dom(page):
    # Find rupee text nodes -> climb to nearest card -> first <p> for product name
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
        if (name && price && price.includes('₹')) {
          results.push([name, price]);
        }
      }
      // de-dup by name (keep last)
      const map = new Map();
      for (const [n, pr] of results) map.set(n, pr);
      return Array.from(map.entries()).map(([n, pr]) => [n, pr]);
    }
    """
    try:
        rows = page.evaluate(js)
    except Exception:
        rows = []
    return rows

# --------- Network JSON strategy ----------
def _walk_for_products(obj, found):
    # Heuristic: find objects with likely name+price keys
    if isinstance(obj, dict):
        lower = {k.lower(): k for k in obj.keys()}
        name_key = next((lower.get(k) for k in ["name","title","productname","product_name","label","sku_name"]), None)
        price_key = next((lower.get(k) for k in ["price","saleprice","sellingprice","amount","value","mrp","offerprice"]), None)

        if name_key and price_key:
            name = str(obj[name_key]).strip()
            price_val = obj[price_key]
            if isinstance(price_val, (int, float)):
                price = f"₹{price_val}"
            else:
                price = str(price_val)
            if name and (("₹" in price) or re.search(r"\d", price)):
                found.append([name, price])

        for v in obj.values():
            _walk_for_products(v, found)

    elif isinstance(obj, list):
        for it in obj:
            _walk_for_products(it, found)

def scrape_once_json(timeout_ms=PAGE_TIMEOUT_MS):
    log("Trying network JSON scrape…")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--disable-gpu","--no-sandbox"])
        ctx = browser.new_context(
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 1024},
        )
        page = ctx.new_page()

        json_bodies = []

        def on_response(resp):
            ctype = (resp.headers or {}).get("content-type", "").lower()
            if "application/json" in ctype:
                try:
                    data = resp.json()
                    json_bodies.append(data)
                except Exception:
                    pass

        page.on("response", on_response)

        page.goto(URL, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms//2)
        except PWTimeout:
            pass

        # Also scan embedded JSON (LD+JSON / Next.js)
        embedded = []
        for sel in ['script[type="application/ld+json"]', '#__NEXT_DATA__', 'script[id="__NEXT_DATA__"]']:
            try:
                for h in page.locator(sel).all():
                    text = h.inner_text()
                    embedded.append(json.loads(text))
            except Exception:
                pass

        ctx.close(); browser.close()

    candidates = []
    for blob in json_bodies + embedded:
        try:
            _walk_for_products(blob, candidates)
        except Exception:
            continue

    # De-dup by product name
    uniq = {}
    for name, price in candidates:
        uniq[name] = price
    rows = [[n, p] for n, p in uniq.items()]

    df = pd.DataFrame(rows, columns=["Product Name", "Price"]).drop_duplicates(subset=["Product Name"])
    if not df.empty:
        df.insert(0, "Date", dt.datetime.now().strftime("%Y-%m-%d"))
    log(f"JSON scrape rows: {len(df)}")
    return df

# --------- DOM strategy (fallback) ----------
def scrape_once_dom(timeout_ms=PAGE_TIMEOUT_MS, dump_artifacts_on_empty=True):
    log("Trying DOM scrape…")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--disable-gpu","--no-sandbox"])
        ctx = browser.new_context(
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 1024},
        )
        page = ctx.new_page()

        page.goto(URL, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms//2)
        except PWTimeout:
            pass

        try_dismiss_overlays(page)
        auto_scroll(page, steps=12, pause_ms=300)
        try_dismiss_overlays(page)

        # Wait for prices or typical price-like classes
        try:
            page.wait_for_function(
                """() => {
                    const hasRupee = document.body && document.body.innerText.includes('₹');
                    const priceLike = document.querySelector('span[class*="price"], p[class*="price"], div[class*="price"]');
                    return hasRupee || !!priceLike;
                }""",
                timeout=timeout_ms
            )
        except PWTimeout:
            log("DOM: timeout waiting for price hints; attempting extraction anyway…")

        rows = extract_rows_from_dom(page)

        # Artifacts for debugging if empty
        if dump_artifacts_on_empty and (not rows or len(rows) == 0):
            try:
                page.screenshot(path="page.png", full_page=True)
            except Exception:
                pass
            try:
                html = page.content()
                open("page.html", "w", encoding="utf-8").write(html)
            except Exception:
                pass

        ctx.close(); browser.close()

    df = pd.DataFrame(rows, columns=["Product Name", "Price"]).drop_duplicates(subset=["Product Name"])
    if not df.empty:
        df.insert(0, "Date", dt.datetime.now().strftime("%Y-%m-%d"))
    log(f"DOM scrape rows: {len(df)}")
    return df

def scrape_with_retry():
    # Try JSON first (most robust), then DOM, then DOM once more non-headless if needed.
    df = scrape_once_json()
    if not df.empty:
        return df

    df = scrape_once_dom()
    if not df.empty:
        return df

    # Last resort: run DOM once more with HEADLESS=false (some sites block headless)
    if HEADLESS:
        log("Retrying DOM with headful (HEADLESS=false)…")
        os.environ["HEADLESS"] = "false"
        df = scrape_once_dom(dump_artifacts_on_empty=True)
        os.environ["HEADLESS"] = "true"
        if not df.empty:
            return df

    return pd.DataFrame(columns=["Date","Product Name","Price"])

# ---------- Write/Upsert to Google Sheet ----------
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

    for col in ["Date", "Product Name", "Price"]:
        if col not in existing.columns:
            existing[col] = pd.NA
    existing = existing[["Date", "Product Name", "Price"]]

    combined = pd.concat([existing, df_today], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Date", "Product Name"], keep="last")

    log(f"Writing {len(combined)} total rows after merge/dedup…")
    ws.clear()
    set_with_dataframe(ws, combined, include_index=False, include_column_header=True, resize=True)

# ---------- Main ----------
if __name__ == "__main__":
    try:
        df = scrape_with_retry()
        # Print what we scraped so Actions logs show it
        print("\n=== Scraped DataFrame (head) ===")
        print(df.head(10))
        print("Total rows scraped:", len(df), "\n")

        if df.empty:
            log("No data scraped — writing debug artifacts and failing the job.")
            # If artifacts were created in DOM attempt, they’ll be uploaded by the workflow step on failure
            raise RuntimeError("Scraper found no data; failing to avoid silent green run.")

        upsert_sheet(df)
        log("Done ✅")
    except Exception as e:
        log(f"ERROR: {e}")
        raise
