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
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from datetime import datetime
from urllib.parse import urljoin

load_dotenv()
PORTAL_URL = os.getenv('PORTAL_URL')
USERNAME = os.getenv('TPUSERNAME')
PASSWORD = os.getenv('PASSWORD')
WEBDRIVER_PATH = os.getenv('WEBDRIVER_PATH')
G_SHEET_ID = os.getenv('GOOGLE_SHEET_KEY')
BASE_URL = "https://tp.bitmesra.co.in/"


GCP_CREDS_FILE = 'credentials.json'
G_SHEET_WORKSHEET_NAME = 'scraped_data_25'
G_SHEET_PPO_WORKSHEET_NAME = 'ppo_data_25'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler('placement_data_hybrid.log', mode='w', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def get_gspread_client():
    """Authenticates with Google using service account credentials and returns a gspread client object."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(GCP_CREDS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        logger.info("Successfully authenticated with Google Sheets API.")
        return client
    except Exception:
        logger.error("Failed to authenticate with Google Sheets. Check 'credentials.json'.", exc_info=True)
        return None


# BS4 Parsers
def parse_main_page_with_bs(page_source):
    """Parses the main jobs listing page to get company links and identify PPOs."""
    soup = BeautifulSoup(page_source, 'html.parser')
    jobs = []
    ppos = []
    job_table = soup.find('table', id='job-listings')
    if not job_table or not job_table.find('tbody'):
        return jobs, ppos

    for row in job_table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        if len(cells) < 4:
            continue

        company_name = cells[0].text.strip()
        date_posted = cells[2].text.strip()
        action_cell = cells[3]

        updates_link_tag = action_cell.find('a', string='Updates')
        view_apply_link_tag = action_cell.find('a', string=re.compile(r'View\s*&\s*Apply'))

        if "PPO" in action_cell.text and updates_link_tag:
            ppos.append({
                "name": company_name,
                "updates_url": urljoin(BASE_URL, updates_link_tag['href'])
            })
        elif updates_link_tag and view_apply_link_tag:
            jobs.append({
                "name": company_name, "date_posted": date_posted,
                "view_url": urljoin(BASE_URL, view_apply_link_tag['href']),
                "updates_url": urljoin(BASE_URL, updates_link_tag['href'])
            })
    return jobs, ppos


def parse_view_apply_page_with_bs(page_source):
    """Parses the 'View & Apply' page HTML to extract role, salary, and stipend info."""
    soup = BeautifulSoup(page_source, 'html.parser')
    data = {"arrived_for": "Not Found", "salaries_fte": [], "stipends_internship": []}

    try:
        arrived_for_list = soup.find('h3', string=re.compile(r'.*')).find_next_sibling('div').find_all('li')
        data['arrived_for'] = ', '.join([li.text.strip() for li in arrived_for_list])
    except Exception:
        logger.warning("BS could not find 'Arrived For' section.")

    try:
        fte_header = soup.find('b', string=lambda text: text and 'SALARY DETAILS (PER ANNUM) - FTE' in text)
        if fte_header:
            fte_table = fte_header.find_parent('table')
            for row in fte_table.find('tbody').find_all('tr'):
                cols = row.find_all('td')
                if len(cols) > 1 and ('₹' in cols[1].text or re.search(r'\d', cols[1].text)):
                    programme = cols[0].text.strip().split()[0]
                    ctc = re.sub(r'[₹,]', '', cols[1].text).strip()
                    if ctc and float(ctc) > 0:
                        data['salaries_fte'].append({'programme': programme, 'ctc': ctc})
    except Exception:
        logger.info("BS: FTE Salary details not found or failed to parse.")

    try:
        stipend_header = soup.find('b', string='STIPEND DETAILS - INTERNSHIP')
        if stipend_header:
            stipend_table = stipend_header.find_parent('table')
            for row in stipend_table.find('tbody').find_all('tr'):
                cell_text_html = str(row.find('td'))
                match = re.search(r"For\s+(UG|PG)\s*<b>₹\s*([\d,]+)", cell_text_html)
                if match:
                    stipend = match.group(2).replace(',', '').strip()
                    if stipend and float(stipend) > 0:
                        data['stipends_internship'].append({'programme': match.group(1), 'stipend': stipend})
    except Exception:
        logger.info("BS: Internship Stipend details not found or failed to parse.")

    return data


def parse_updates_page_with_bs(page_source):
    """Parses the 'Updates' page to find links to shortlist/result pages."""
    soup = BeautifulSoup(page_source, 'html.parser')
    round_links = []
    result_div = soup.find('div', style=lambda s: s and 'background-color:#c1fac3' in s)
    if result_div:
        for link_tag in result_div.find_all('a'):
            round_links.append({
                'name': link_tag.text.strip(),
                'url': urljoin(BASE_URL, link_tag['href'])
            })
    return round_links


def parse_shortlist_page_with_bs(page_source):
    """Parses a shortlist page to count the number of students."""
    soup = BeautifulSoup(page_source, 'html.parser')
    student_table = soup.find('table', class_='table-striped')
    if student_table and student_table.find('tbody'):
        return len(student_table.find('tbody').find_all('tr'))
    return 0


def process_pages_hybrid(driver, wait, worksheet, ppo_sheet):
    """Main processing loop using the hybrid Selenium + Beautiful Soup approach."""
    page_number = 1
    all_ppo_data = []
    main_window_handle = driver.current_window_handle

    while True:
        try:
            logger.info(f"--- Processing Page {page_number} ---")
            wait.until(EC.presence_of_element_located((By.ID, "job-listings_info")))
            time.sleep(2)
            page_source = driver.page_source

            jobs_on_page, ppos_on_page = parse_main_page_with_bs(page_source)
            logger.info(f"Found {len(jobs_on_page)} jobs and {len(ppos_on_page)} PPOs on page {page_number}.")

            rows_for_this_page = []

            for ppo in ppos_on_page:
                driver.execute_script("window.open(arguments[0], '_blank');", ppo['updates_url'])
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(2)

                round_links = parse_updates_page_with_bs(driver.page_source)
                ppo_student_count = 0
                if round_links:
                    driver.get(round_links[0]['url'])
                    time.sleep(2)
                    ppo_student_count = parse_shortlist_page_with_bs(driver.page_source)

                all_ppo_data.append({'name': ppo['name'], 'count': ppo_student_count})

                driver.close()
                driver.switch_to.window(main_window_handle)
                time.sleep(1)

            for job in jobs_on_page:
                logger.info(f"Scraping details for: {job['name']}")

                driver.execute_script("window.open(arguments[0], '_blank');", job['view_url'])
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(2)
                view_apply_data = parse_view_apply_page_with_bs(driver.page_source)

                driver.get(job['updates_url'])
                time.sleep(2)
                round_links = parse_updates_page_with_bs(driver.page_source)

                rounds_data = []
                for rd in round_links:
                    driver.get(rd['url'])
                    time.sleep(2)
                    shortlist_count = parse_shortlist_page_with_bs(driver.page_source)
                    rounds_data.append({'round': rd['name'], 'count': shortlist_count})

                driver.close()
                driver.switch_to.window(main_window_handle)
                time.sleep(1)

                row_to_append = [
                    job['name'], job['date_posted'],
                    view_apply_data['arrived_for'],
                    json.dumps(view_apply_data['salaries_fte']),
                    json.dumps(view_apply_data['stipends_internship']),
                    json.dumps(rounds_data),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                rows_for_this_page.append(row_to_append)
                logger.info(f"Data for {job['name']} prepared for batch upload.")

            if rows_for_this_page:
                worksheet.append_rows(rows_for_this_page, value_input_option='USER_ENTERED')
                logger.info(f"Successfully uploaded {len(rows_for_this_page)} rows for page {page_number}.")
            else:
                logger.info(f"No new job data to upload for page {page_number}.")

            try:
                short_wait = WebDriverWait(driver, 3)
                short_wait.until(EC.presence_of_element_located((By.ID, "job-listings_paginate")))

                next_button_lis = driver.find_elements(By.ID, "job-listings_next")

                if not next_button_lis or "disabled" in next_button_lis[0].get_attribute("class"):
                    logger.info("Reached the last page for this year.")
                    break
                else:
                    logger.info(f"Navigating to page {page_number + 1}...")
                    next_button_a = next_button_lis[0].find_element(By.TAG_NAME, "a")
                    driver.execute_script("arguments[0].click();", next_button_a)
                    page_number += 1
            except TimeoutException:
                logger.info("Pagination controls not found. Assuming a single page of results.")
                break

        except Exception:
            logger.error(f"An error occurred on page {page_number}. Stopping pagination.", exc_info=True)
            if len(driver.window_handles) > 1:
                for handle in driver.window_handles:
                    if handle != main_window_handle:
                        driver.switch_to.window(handle)
                        driver.close()
                driver.switch_to.window(main_window_handle)
            break

    return all_ppo_data


if __name__ == "__main__":
    gspread_client = get_gspread_client()
    sheet, ppo_sheet = None, None
    if gspread_client:
        try:
            spreadsheet = gspread_client.open_by_key(G_SHEET_ID)
            sheet = spreadsheet.worksheet(G_SHEET_WORKSHEET_NAME)
            logger.info(f"Successfully connected to worksheet '{G_SHEET_WORKSHEET_NAME}'.")
            try:
                ppo_sheet = spreadsheet.worksheet(G_SHEET_PPO_WORKSHEET_NAME)
            except gspread.WorksheetNotFound:
                logger.info(f"Worksheet '{G_SHEET_PPO_WORKSHEET_NAME}' not found. Creating it...")
                ppo_sheet = spreadsheet.add_worksheet(title=G_SHEET_PPO_WORKSHEET_NAME, rows="100", cols="3")
                ppo_sheet.append_row(['Company Name', 'PPO Student Count', 'Scrape Timestamp'])
        except Exception:
            logger.error("Could not open worksheet. Check Sheet ID and sharing permissions.", exc_info=True)

    if sheet and ppo_sheet:
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {"credentials_enable_service": False,
                                                         "profile.password_manager_enabled": False})
        chrome_options.add_argument("--disable-features=Autofill")

        service = webdriver.ChromeService(executable_path=WEBDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(driver, 15)

        final_ppo_data = []

        try:
            driver.get(PORTAL_URL)
            driver.maximize_window()

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

            year_options = ['2025-26']  # Can be expanded e.g., ['2025-26', '2024-25']
            for year in year_options:
                logger.info(f"--- Processing Year: {year} ---")
                Select(wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))).select_by_visible_text(year)
                time.sleep(3)  # Wait for page to reload after year selection
                final_ppo_data.extend(process_pages_hybrid(driver, wait, sheet, ppo_sheet))

        except Exception:
            logger.error("A critical error occurred in the main script:", exc_info=True)
        finally:
            if final_ppo_data:
                unique_ppo_data = [dict(t) for t in {tuple(d.items()) for d in final_ppo_data}]
                logger.info(f"Uploading PPO data for {len(unique_ppo_data)} companies...")
                rows_to_add = [[item['name'], item['count'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')] for item in
                               unique_ppo_data]
                ppo_sheet.append_rows(rows_to_add)
                logger.info("PPO data upload complete.")
            else:
                logger.info("No new PPO offerings were found.")

            logger.info("--- SCRIPT COMPLETE ---")
            logger.info("The browser will close in 15 seconds...")
            time.sleep(15)
            driver.quit()

