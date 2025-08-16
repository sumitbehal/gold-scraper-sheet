import os, json, datetime as dt
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# --- EDIT THESE IF YOU USED DIFFERENT NAMES ---
SPREADSHEET_NAME = "Gold Prices (MMTC-PAMP)"
WORKSHEET_NAME = "Daily"
URL = "https://www.mmtcpamp.com/shop/gold"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# ------------------------------------------------

def gs_client():
    cred_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not cred_json:
        raise RuntimeError("Add your Google JSON as a GitHub secret named GOOGLE_SERVICE_ACCOUNT_JSON.")
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

def scrape():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1440,1024")
    opts.add_argument("--user-agent=Mozilla/5.0")
    if os.getenv("CHROME_BIN"):
        opts.binary_location = os.getenv("CHROME_BIN")

    driver = webdriver.Chrome(options=opts)  # Selenium Manager handles driver in Actions
    driver.get(URL)

    WebDriverWait(driver, 30).until(
        EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class,"MuiBox-root")]//p'))
    )
    cards = driver.find_elements(By.XPATH, '//div[contains(@class,"MuiBox-root") and .//p]')

    rows = []
    for c in cards:
        try:
            name = c.find_element(By.XPATH, './/p').text.strip()
            price = c.find_element(By.XPATH, './/span[contains(text(),"₹")]').text.strip()
            if name and price:
                rows.append([name, price])
        except Exception:
            continue
    driver.quit()

    df = pd.DataFrame(rows, columns=["Product Name", "Price"]).drop_duplicates(subset=["Product Name"])
    if df.empty:
        return df
    df.insert(0, "Date", dt.datetime.now().strftime("%Y-%m-%d"))
    return df

def upsert_sheet(df: pd.DataFrame):
    gc = gs_client()
    _, ws = open_or_create_sheet(gc, SPREADSHEET_NAME, WORKSHEET_NAME)
    existing = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")

    if existing.empty:
        ws.clear()
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        return

    for col in ["Date", "Product Name", "Price"]:
        if col not in existing.columns:
            existing[col] = pd.NA
    existing = existing[["Date", "Product Name", "Price"]]

    combined = pd.concat([existing, df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Date", "Product Name"], keep="last")

    ws.clear()
    set_with_dataframe(ws, combined, include_index=False, include_column_header=True, resize=True)

if __name__ == "__main__":
    df = scrape()
    if not df.empty:
        upsert_sheet(df)
