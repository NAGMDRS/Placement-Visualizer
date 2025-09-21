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
from selenium.webdriver.chrome.options import Options
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
G_SHEET_WORKSHEET_NAME = 'scraped_data_24'
G_SHEET_PPO_WORKSHEET_NAME = 'ppo_data_24'

# --- Logging Configuration ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler('placement_data_consolidated.log', mode='w', encoding='utf-8')
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


def process_job_listings(main_window_handle, worksheet, ppo_data_list):
    """Handles pagination and scrapes consolidated job info, writing one row per company."""
    page_number = 1
    while True:
        try:
            logger.info(f"Processing page {page_number}...")
            job_table = wait.until(EC.visibility_of_element_located((By.ID, "job-listings")))
            job_rows = job_table.find_elements(By.XPATH, ".//tbody/tr")

            jobs_on_page = []
            for row in job_rows:
                try:
                    company_name = row.find_element(By.XPATH, ".//td[1]").text.strip()
                    date_posted = row.find_element(By.XPATH, ".//td[3]").text.strip()

                    view_apply_link = row.find_element(By.PARTIAL_LINK_TEXT, "View & Apply").get_attribute('href')
                    updates_link = row.find_element(By.PARTIAL_LINK_TEXT, "Updates").get_attribute('href')
                    jobs_on_page.append({
                        "name": company_name, "date_posted": date_posted,
                        "view_url": view_apply_link, "updates_url": updates_link
                    })
                except NoSuchElementException:
                    try:
                        action_cell_text = row.find_element(By.XPATH, ".//td[4]").text
                        if "PPO" in action_cell_text:
                            logger.info(f"Found PPO offering from: {company_name}")
                            ppo_student_count = 0
                            try:
                                updates_link = row.find_element(By.PARTIAL_LINK_TEXT, "Updates").get_attribute('href')
                                driver.execute_script("window.open(arguments[0], '_blank');", updates_link)
                                driver.switch_to.window(driver.window_handles[-1])
                                time.sleep(2)

                                result_links = wait.until(EC.presence_of_all_elements_located(
                                    (By.XPATH, "//div[h6/b[text()='Result']]//li/a")))
                                if result_links:
                                    result_url = result_links[0].get_attribute('href')
                                    driver.get(result_url)
                                    time.sleep(2)
                                    student_rows = driver.find_elements(By.XPATH,
                                                                        "//table[thead/tr/th[text()='SL']]/tbody/tr")
                                    ppo_student_count = len(student_rows)
                            except Exception:
                                logger.warning(
                                    f"Could not automatically determine PPO student count for {company_name}.")
                            finally:
                                driver.close()
                                driver.switch_to.window(main_window_handle)

                            ppo_data_list.append({'name': company_name, 'count': ppo_student_count})
                        else:
                            logger.warning(f"Skipping a row for '{company_name}' (may be missing standard links).")
                    except Exception as e:
                        logger.error(f"Could not process a non-standard row. Error: {e}")
                    continue

            logger.info(f"Found {len(jobs_on_page)} standard jobs on page {page_number}. Scraping details...")

            for job in jobs_on_page:
                company_data = {
                    "company_name": job['name'], "date_posted": job['date_posted'],
                    "arrived_for": "Not Found", "salaries_fte": [],
                    "stipends_internship": [], "rounds_shortlists": []
                }

                # --- 1. Scrape "View & Apply" Page ---
                driver.execute_script("window.open(arguments[0], '_blank');", job['view_url'])
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(2)

                logger.info(f"--- Processing Company: {job['name']} ---")

                try:
                    arrived_for_elements = wait.until(
                        EC.presence_of_all_elements_located((By.XPATH, "//h3/following-sibling::div//li")))
                    company_data['arrived_for'] = ', '.join([elem.text for elem in arrived_for_elements])
                except TimeoutException:
                    logger.warning("Could not determine 'Arrived For' status.")

                try:
                    fte_table = driver.find_element(By.XPATH,
                                                    "//b[contains(text(), 'SALARY DETAILS (PER ANNUM) - FTE')]/ancestor::table[1]")
                    salary_rows = fte_table.find_elements(By.XPATH, ".//tbody/tr[.//td[contains(text(),'₹')]]")
                    for s_row in salary_rows:
                        ctc_raw = s_row.find_element(By.XPATH, ".//td[2]").text
                        ctc = re.sub(r'[₹,]', '', ctc_raw).strip()
                        if ctc and ctc != '0':
                            programme = s_row.find_element(By.XPATH, ".//td[1]").text.split('\n')[0].strip()
                            company_data['salaries_fte'].append({'programme': programme, 'ctc': ctc})
                except NoSuchElementException:
                    logger.info("FTE Salary details not found.")

                try:
                    stipend_table = driver.find_element(By.XPATH,
                                                        "//b[contains(text(), 'STIPEND DETAILS - INTERNSHIP')]/ancestor::table[1]")
                    stipend_rows = stipend_table.find_elements(By.XPATH, ".//tbody/tr")
                    for st_row in stipend_rows:
                        cell_text = st_row.find_element(By.XPATH, ".//td[1]").text
                        match = re.search(r"For (UG|PG) ₹ (\d+)", cell_text)
                        if match and match.group(2) != '0':
                            company_data['stipends_internship'].append(
                                {'programme': match.group(1), 'stipend': match.group(2)})
                except NoSuchElementException:
                    logger.info("Internship Stipend details not found.")

                driver.close()
                driver.switch_to.window(main_window_handle)

                # --- 2. Scrape "Updates" Page ---
                driver.execute_script("window.open(arguments[0], '_blank');", job['updates_url'])
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(2)

                try:
                    result_links = [{'name': elem.text, 'url': elem.get_attribute('href')} for elem in wait.until(
                        EC.presence_of_all_elements_located((By.XPATH, "//div[h6/b[text()='Result']]//li/a")))]
                    for link in result_links:
                        driver.get(link['url'])  # Reuse tab
                        time.sleep(2)
                        student_rows = driver.find_elements(By.XPATH, "//table[thead/tr/th[text()='SL']]/tbody/tr")
                        company_data['rounds_shortlists'].append({'round': link['name'], 'count': len(student_rows)})
                except TimeoutException:
                    logger.info("No 'Result' section found on the Updates page.")

                driver.close()
                driver.switch_to.window(main_window_handle)

                # --- 3. Consolidate and Append Data ---
                logger.info(f"Consolidated Data for {job['name']}: {company_data}")
                row_to_append = [
                    company_data['company_name'], company_data['date_posted'], company_data['arrived_for'],
                    json.dumps(company_data['salaries_fte']), json.dumps(company_data['stipends_internship']),
                    json.dumps(company_data['rounds_shortlists']), datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                worksheet.append_row(row_to_append)

            # --- 4. Pagination Logic ---
            next_button_li = driver.find_element(By.ID, "job-listings_next")
            if "disabled" in next_button_li.get_attribute("class"):
                logger.info("Reached the last page for this year.")
                break
            else:
                next_button_a = next_button_li.find_element(By.TAG_NAME, "a")
                driver.execute_script("arguments[0].click();", next_button_a)
                page_number += 1
                time.sleep(3)
        except Exception as e:
            logger.error(f"An error occurred on page {page_number}:", exc_info=True)
            break


if __name__ == "__main__":
    gspread_client = get_gspread_client()
    sheet, ppo_sheet = None, None
    if gspread_client:
        try:
            GOOGLE_SHEET_KEY = os.getenv('GOOGLE_SHEET_KEY')
            spreadsheet = gspread_client.open_by_key(GOOGLE_SHEET_KEY)
            sheet = spreadsheet.worksheet(G_SHEET_WORKSHEET_NAME)
            logger.info(f"Successfully connected to worksheet '{G_SHEET_WORKSHEET_NAME}'.")
            try:
                ppo_sheet = spreadsheet.worksheet(G_SHEET_PPO_WORKSHEET_NAME)
                logger.info(f"Successfully connected to worksheet '{G_SHEET_PPO_WORKSHEET_NAME}'.")
            except gspread.WorksheetNotFound:
                logger.info(f"Worksheet '{G_SHEET_PPO_WORKSHEET_NAME}' not found. Creating it...")
                ppo_sheet = spreadsheet.add_worksheet(title=G_SHEET_PPO_WORKSHEET_NAME, rows="100", cols="3")
                ppo_sheet.append_row(['Company Name', 'PPO Student Count', 'Scrape Timestamp'])
        except Exception as e:
            logger.error(f"Could not open worksheet. Ensure Sheet ID is correct and shared.", exc_info=True)

    if sheet and ppo_sheet:
        chrome_options = Options()
        prefs = {"credentials_enable_service": False, "profile.password_manager_enabled": False}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--disable-features=Autofill")

        service = webdriver.ChromeService(executable_path=WEBDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 10)

        all_ppo_data = []

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
            year_options = ['2024-25']

            for year in year_options:
                logger.info(f"--- Processing Year: {year} ---")
                select_element = wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))
                Select(select_element).select_by_visible_text(year)
                time.sleep(3)
                process_job_listings(main_window_handle, sheet, all_ppo_data)

        except Exception as e:
            logger.error("An unexpected error occurred in the main script:", exc_info=True)
        finally:
            unique_ppo_data = [dict(t) for t in {tuple(d.items()) for d in all_ppo_data}]
            logger.info("--- SCRIPT COMPLETE ---")
            if unique_ppo_data:
                logger.info(f"Uploading PPO data for {len(unique_ppo_data)} companies to Google Sheets...")
                rows_to_add = [[item['name'], item['count'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')] for item in
                               unique_ppo_data]
                ppo_sheet.append_rows(rows_to_add)
                logger.info("PPO data upload complete.")
            else:
                logger.info("No PPO offerings were found during this run.")

            logger.info("The browser will close in 15 seconds...")
            time.sleep(15)
            driver.quit()

