import time
import logging
import re
import json
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

# Configuration
PORTAL_URL = os.getenv('PORTAL_URL')
USERNAME = os.getenv('TPUSERNAME')
PASSWORD = os.getenv('PASSWORD')
WEBDRIVER_PATH = os.getenv('WEBDRIVER_PATH')
GCP_CREDS_FILE = 'credentials.json'
G_SHEET_WORKSHEET_NAME = 'scraped_data_25'

# --- Logging Configuration ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler('placement_data.log', mode='w', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def get_gspread_client():
    """Authenticates with Google and returns a gspread client object."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(GCP_CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        logger.info("Successfully authenticated with Google Sheets API.")
        return client
    except Exception as e:
        logger.error("Failed to authenticate with Google Sheets.", exc_info=True)
        return None


def process_job_listings(main_window_handle, worksheet):
    """
    Handles pagination and scrapes detailed job info, consolidating all data for
    one company into a single row with JSON for multi-entry fields.
    """
    page_number = 1
    while True:
        try:
            logger.info(f"Processing page {page_number}...")
            job_table = wait.until(EC.visibility_of_element_located((By.ID, "job-listings")))
            job_rows = job_table.find_elements(By.XPATH, ".//tbody/tr")

            jobs_on_page = []
            for row in job_rows:
                try:
                    company_name = row.find_element(By.XPATH, ".//td[1]").text
                    view_apply_link = row.find_element(By.PARTIAL_LINK_TEXT, "View & Apply").get_attribute('href')
                    updates_link = row.find_element(By.PARTIAL_LINK_TEXT, "Updates").get_attribute('href')
                    jobs_on_page.append(
                        {"name": company_name.strip(), "view_url": view_apply_link, "updates_url": updates_link})
                except NoSuchElementException:
                    logger.warning(f"Skipping a row on page {page_number} (may be a PPO or missing links).")
                    continue

            logger.info(f"Found {len(jobs_on_page)} jobs on page {page_number}. Scraping details...")

            for job in jobs_on_page:
                logger.info(f"--- Processing Company: {job['name']} ---")
                fte_data = []
                intern_data = []
                rounds_data = []
                roles = "Not Found"

                # --- 1. Scrape "View & Apply" Page ---
                driver.execute_script("window.open(arguments[0], '_blank');", job['view_url'])
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(2)

                try:
                    arrived_for_elements = wait.until(
                        EC.presence_of_all_elements_located((By.XPATH, "//h3/following-sibling::div//li")))
                    roles = ', '.join(
                        [elem.text for elem in arrived_for_elements]) if arrived_for_elements else "Not Found"
                    logger.info(f"Arrived For: {roles}")

                    fte_table = driver.find_element(By.XPATH,
                                                    "//b[contains(text(), 'SALARY DETAILS (PER ANNUM) - FTE')]/ancestor::table[1]")
                    salary_rows = fte_table.find_elements(By.XPATH, ".//tbody/tr[.//td[contains(text(),'₹')]]")
                    for s_row in salary_rows:
                        fte_data.append({
                            'programme': s_row.find_element(By.XPATH, ".//td[1]").text.split('\n')[0].strip(),
                            'ctc': s_row.find_element(By.XPATH, ".//td[2]").text.strip()
                        })
                    logger.info(f"FTE Data Found: {fte_data}")
                except (NoSuchElementException, TimeoutException):
                    logger.info("FTE/Role details not found or page structure differs.")

                try:
                    stipend_table = driver.find_element(By.XPATH,
                                                        "//b[contains(text(), 'STIPEND DETAILS - INTERNSHIP')]/ancestor::table[1]")
                    stipend_rows = stipend_table.find_elements(By.XPATH, ".//tbody/tr")
                    for st_row in stipend_rows:
                        cell_text = st_row.find_element(By.XPATH, ".//td[1]").text
                        match = re.search(r"For (UG|PG) ₹ (\d+)", cell_text)
                        if match:
                            intern_data.append({'programme': match.group(1), 'stipend': match.group(2)})
                    logger.info(f"Internship Data Found: {intern_data}")
                except NoSuchElementException:
                    logger.info("Internship Stipend details not found.")

                driver.close()
                driver.switch_to.window(main_window_handle)

                # --- 2. Scrape "Updates" Page ---
                driver.execute_script("window.open(arguments[0], '_blank');", job['updates_url'])
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(2)

                try:
                    result_links_elements = wait.until(
                        EC.presence_of_all_elements_located((By.XPATH, "//div[h6/b[text()='Result']]//li/a")))
                    result_links = [{'name': elem.text, 'url': elem.get_attribute('href')} for elem in
                                    result_links_elements]

                    for link in result_links:
                        driver.execute_script("window.open(arguments[0], '_blank');", link['url'])
                        driver.switch_to.window(driver.window_handles[-1])
                        time.sleep(2)

                        try:
                            student_rows = driver.find_elements(By.XPATH, "//table[thead/tr/th[text()='SL']]/tbody/tr")
                            rounds_data.append({'round': link['name'], 'count': len(student_rows)})
                        except Exception:
                            logger.error(f"Could not count students for round '{link['name']}'.", exc_info=True)

                        driver.close()
                        driver.switch_to.window(driver.window_handles[1])
                    logger.info(f"Rounds Data Found: {rounds_data}")
                except TimeoutException:
                    logger.info("No 'Result' section found on the Updates page.")

                driver.close()
                driver.switch_to.window(main_window_handle)

                # --- 3. Consolidate and Append to Google Sheets ---
                final_row = [
                    job['name'],
                    roles,
                    json.dumps(fte_data) if fte_data else "[]",
                    json.dumps(intern_data) if intern_data else "[]",
                    json.dumps(rounds_data) if rounds_data else "[]",
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                worksheet.append_row(final_row)
                logger.info(f"Successfully appended consolidated row for {job['name']} to Google Sheets.")

            next_button_li = driver.find_element(By.ID, "job-listings_next")
            if "disabled" in next_button_li.get_attribute("class"):
                logger.info("Reached the last page for this year.")
                break
            else:
                logger.info("Clicking the 'Next' button...")
                next_button_a = next_button_li.find_element(By.TAG_NAME, "a")
                driver.execute_script("arguments[0].click();", next_button_a)
                page_number += 1
                time.sleep(3)

        except Exception as e:
            logger.error("An error occurred during the main processing loop:", exc_info=True)
            break


if __name__ == "__main__":
    gspread_client = get_gspread_client()
    if gspread_client:
        try:
            GOOGLE_SHEET_KEY=os.getenv('GOOGLE_SHEET_KEY')
            sheet = gspread_client.open_by_key(GOOGLE_SHEET_KEY).worksheet(G_SHEET_WORKSHEET_NAME)
            logger.info(f"Successfully connected to worksheet '{G_SHEET_WORKSHEET_NAME}'.")
        except Exception:
            logger.error(f"Could not open worksheet. Ensure G_SHEET_ID is correct and sheet is shared.", exc_info=True)
            sheet = None

        if sheet:
            service = webdriver.ChromeService(executable_path=WEBDRIVER_PATH)
            driver = webdriver.Chrome(service=service)
            wait = WebDriverWait(driver, 10)

            try:
                logger.info("Navigating to the placement portal...")
                driver.get(PORTAL_URL)
                driver.maximize_window()

                logger.info("Entering login credentials...")
                username_field = wait.until(EC.presence_of_element_located((By.ID, "identity")))
                password_field = driver.find_element(By.ID, "password")
                login_button = driver.find_element(By.XPATH, "//input[@value='Login']")

                username_field.clear()
                username_field.send_keys(USERNAME)
                password_field.clear()
                password_field.send_keys(PASSWORD)

                login_button.click()
                logger.info("Login successful.")

                wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))
                main_window_handle = driver.current_window_handle
                year_options = ['2025-26']  # Add more years if needed

                for year in year_options:
                    logger.info(f"--- Processing year: {year} ---")
                    select_element = wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))
                    select = Select(select_element)
                    select.select_by_visible_text(year)
                    time.sleep(3)
                    process_job_listings(main_window_handle, sheet)

            except Exception as e:
                logger.error("An unexpected error occurred in the main script:", exc_info=True)
            finally:
                logger.info("Script finished. The browser will close in 15 seconds...")
                time.sleep(15)
                driver.quit()

