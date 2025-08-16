import os, json, datetime as dt, sys, time
import pandas as pd

# --- Playwright (no driver mismatch) ---
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
    "accept", "agree", "got it", "allow", "ok", "okay", "i agree", "accept all", "continue",
]
def try_dismiss_overlays(page):
    try:
        # Click common cookie/consent buttons if present
        for txt in COOKIE_BUTTON_TEXTS:
            loc = page.locator(f"xpath=//button[normalize-space()[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{txt}')]]")
            if loc.count():
                for i in range(min(loc.count(), 3)):
                    try:
                        loc.nth(i).click(timeout=1500)
                    except Exception:
                        pass
    except Exception:
        pass

def auto_scroll(page, steps=8, pause=400):
    # Trigger lazy-load
    for _ in range(steps):
        page.mouse.wheel(0, 1500)
        page.wait_for_timeout(pause)

def extract_rows(page):
    # Primary: find any ₹ text nodes, then nearest Material UI card, then first <p> for name
    js = """
    () => {
      const results = [];
      const priceEls = Array.from(document.querySelectorAll('*')).filter(el => el.textContent && el.textContent.includes('₹'));
      for (const p of priceEls) {
        // climb to closest MUI Box card
        let card = p.closest('div[class*="MuiBox-root"]') || p.closest('div');
        if (!card) continue;
        // product name heuristic: first <p> inside the card with non-empty text
        let nameEl = card.querySelector('p');
        let name = nameEl ? nameEl.textContent.trim() : '';
        let price = p.textContent.trim();
        if (name && price.includes('₹')) {
          results.push([name, price]);
        }
      }
      // de-dup by name (keep last)
      const map = new Map();
      for (const [n, pr] of results) map.set(n, pr);
      return Array.from(map.entries()).map(([n, pr]) => [n, pr]);
    }
    """
    rows = page.evaluate(js)
    return rows

def scrape_once(timeout_ms=PAGE_TIMEOUT_MS) -> pd.DataFrame:
    log("Launching Chromium via Playwright…")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-gpu","--no-sandbox"])
        context = browser.new_context(
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 1024},
        )
        page = context.new_page()

        log(f"Opening {URL}")
        page.goto(URL, wait_until="domcontentloaded", timeout=timeout_ms)
        # Let the SPA load data; try network idle but don’t rely solely on it
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms//2)
        except PWTimeout:
            pass

        try_dismiss_overlays(page)
        auto_scroll(page, steps=10, pause=350)
        try_dismiss_overlays(page)

        # Wait for either ₹ or typical price class patterns to appear
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
            log("Timeout waiting for price hints; attempting extraction anyway…")

        rows = extract_rows(page)

        context.close()
        browser.close()

    df = pd.DataFrame(rows, columns=["Product Name", "Price"]).drop_duplicates(subset=["Product Name"])
    if not df.empty:
        df.insert(0, "Date", dt.datetime.now().strftime("%Y-%m-%d"))
    log(f"Scraped {len(df)} rows.")
    return df

def scrape_with_retry() -> pd.DataFrame:
    for attempt in range(2):
        df = scrape_once()
        if not df.empty:
            return df
        log(f"Attempt {attempt+1} yielded 0 rows. Retrying…")
        time.sleep(3)
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
        if df.empty:
            log("No data scraped — not updating sheet.")
            sys.exit(0)
        upsert_sheet(df)
        log("Done ✅")
    except Exception as e:
        log(f"ERROR: {e}")
        raise
