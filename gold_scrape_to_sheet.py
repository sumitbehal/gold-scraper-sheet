import os, json, datetime as dt, sys, time
import pandas as pd

# --- Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- Google Sheets ---
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# ================== CONFIG ==================
SPREADSHEET_NAME = os.getenv("SHEET_NAME", "Gold Prices (MMTC-PAMP)")  # you can override via repo → Actions → Variables
WORKSHEET_NAME  = os.getenv("SHEET_TAB",  "Daily")
URL = "https://www.mmtcpamp.com/shop/gold"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
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

# ---------- Selenium driver ----------
def new_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1440,1024")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    # If setup-chrome provided a custom binary path
    if os.getenv("CHROME_BIN"):
        opts.binary_location = os.getenv("CHROME_BIN")

    # Auto-install a matching chromedriver for the installed Chrome
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

# ---------- Scrape ----------
def scrape(timeout=45) -> pd.DataFrame:
    log("Starting headless Chrome…")
    driver = new_driver()
    try:
        log(f"Opening {URL}")
        driver.get(URL)

        # Wait for content to appear
        WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class,"MuiBox-root")]//p'))
        )

        # Primary card search (tight); fallback (broader) if needed
        cards = driver.find_elements(By.XPATH, '//div[contains(@class,"MuiBox-root") and .//p]')
        if not cards:
            cards = driver.find_elements(By.XPATH, '//div[contains(@class,"MuiBox-root")]')

        rows = []
        for c in cards:
            try:
                name_el = c.find_element(By.XPATH, './/p')
                price_el = c.find_element(By.XPATH, './/*[contains(text(),"₹")]')
                name = name_el.text.strip()
                price = price_el.text.strip()
                if name and price:
                    rows.append([name, price])
            except Exception:
                continue

        df = pd.DataFrame(rows, columns=["Product Name", "Price"]).drop_duplicates(subset=["Product Name"])
        if not df.empty:
            df.insert(0, "Date", dt.datetime.now().strftime("%Y-%m-%d"))
        log(f"Scraped {len(df)} rows.")
        return df
    finally:
        driver.quit()
        log("Closed browser.")

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

    # Ensure the three columns exist and are ordered
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
        df = scrape()
        if df.empty:
            log("No data scraped — not updating sheet.")
            sys.exit(0)
        upsert_sheet(df)
        log("Done ✅")
    except Exception as e:
        log(f"ERROR: {e}")
        raise
