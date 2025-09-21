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
from multiprocessing import Pool, cpu_count

load_dotenv()
PORTAL_URL = os.getenv('PORTAL_URL')
USERNAME = os.getenv('TPUSERNAME')
PASSWORD = os.getenv('PASSWORD')
WEBDRIVER_PATH = os.getenv('WEBDRIVER_PATH')
G_SHEET_ID = os.getenv('GOOGLE_SHEET_KEY')
BASE_URL = "https://tp.bitmesra.co.in/"
GCP_CREDS_FILE = 'credentials.json'
NUM_PROCESSES = int(cpu_count() * 0.75)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler('placement_data_parallel.log', mode='w', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# --- Google Sheets Connection ---
def get_gspread_client():
    """Authenticates with Google using service account credentials."""
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(GCP_CREDS_FILE, scopes=scopes)
        logger.info("MainProcess: Successfully authenticated with Google Sheets API.")
        return gspread.authorize(creds)
    except Exception:
        logger.error("MainProcess: Failed to authenticate with Google Sheets.", exc_info=True)
        return None


# BS4 Parsers
def parse_main_page_with_bs(page_source):
    """Parses the main jobs listing page to get company links and identify PPOs."""
    soup = BeautifulSoup(page_source, 'html.parser')
    jobs, ppos = [], []
    job_table = soup.find('table', id='job-listings')
    if not job_table or not job_table.find('tbody'): return jobs, ppos
    for row in job_table.find('tbody').find_all('tr'):
        cells = row.find_all('td')
        if len(cells) < 4: continue
        company_name, date_posted, action_cell = cells[0].text.strip(), cells[2].text.strip(), cells[3]
        updates_link = action_cell.find('a', string='Updates')
        view_apply_link = action_cell.find('a', string=re.compile(r'View\s*&\s*Apply'))
        if "PPO" in action_cell.text and updates_link:
            ppos.append({"name": company_name, "updates_url": urljoin(BASE_URL, updates_link['href'])})
        elif updates_link and view_apply_link:
            jobs.append({"name": company_name, "date_posted": date_posted,
                         "view_url": urljoin(BASE_URL, view_apply_link['href']),
                         "updates_url": urljoin(BASE_URL, updates_link['href'])})
    return jobs, ppos


def parse_view_apply_page_with_bs(page_source):
    """Parses the 'View & Apply' page HTML to extract role, salary, and stipend info."""
    soup = BeautifulSoup(page_source, 'html.parser')
    data = {"arrived_for": "Not Found", "salaries_fte": [], "stipends_internship": []}
    try:
        arrived_for_list = soup.find('h3', string=re.compile(r'.*')).find_next_sibling('div').find_all('li')
        data['arrived_for'] = ', '.join([li.text.strip() for li in arrived_for_list])
    except Exception:
        pass
    try:
        fte_header = soup.find('b', string=lambda t: t and 'SALARY DETAILS (PER ANNUM) - FTE' in t)
        if fte_header:
            for row in fte_header.find_parent('table').find('tbody').find_all('tr'):
                cols = row.find_all('td')
                if len(cols) > 1 and ('₹' in cols[1].text or re.search(r'\d', cols[1].text)):
                    programme = cols[0].get_text(strip=True).split()[0]
                    ctc = re.sub(r'[₹,]', '', cols[1].text).strip()
                    if ctc and float(ctc) > 0: data['salaries_fte'].append({'programme': programme, 'ctc': ctc})
    except Exception:
        pass
    try:
        stipend_header = soup.find('b', string='STIPEND DETAILS - INTERNSHIP')
        if stipend_header:
            for row in stipend_header.find_parent('table').find_tbody().find_all('tr'):
                match = re.search(r"For\s+(UG|PG)\s*<b>₹\s*([\d,]+)", str(row.find('td')))
                if match:
                    stipend = match.group(2).replace(',', '').strip()
                    if stipend and float(stipend) > 0: data['stipends_internship'].append(
                        {'programme': match.group(1), 'stipend': stipend})
    except Exception:
        pass
    return data


def parse_updates_page_with_bs(page_source):
    """Parses the 'Updates' page to find links to shortlist/result pages."""
    soup = BeautifulSoup(page_source, 'html.parser')
    round_links = []
    result_div = soup.find('div', style=lambda s: s and 'background-color:#c1fac3' in s)
    if result_div:
        for link in result_div.find_all('a'):
            round_links.append({'name': link.text.strip(), 'url': urljoin(BASE_URL, link['href'])})
    return round_links


def parse_shortlist_page_with_bs(page_source):
    """Parses a shortlist page to count the number of students."""
    soup = BeautifulSoup(page_source, 'html.parser')
    student_table = soup.find('table', class_='table-striped')
    return len(student_table.find('tbody').find_all('tr')) if student_table and student_table.find('tbody') else 0


def init_worker_browser(cookies):
    """Initializes a headless browser for a worker and injects login cookies."""
    worker_options = Options()
    worker_options.add_argument("--headless")
    worker_options.add_argument("--disable-gpu")
    worker_options.add_argument("--window-size=1920x1080")
    service = webdriver.ChromeService(executable_path=WEBDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=worker_options)

    driver.get(BASE_URL)
    for name, value in cookies.items():
        driver.add_cookie({'name': name, 'value': value})
    return driver


def scrape_job_worker(job_with_cookies):
    """Worker function to scrape a single regular job posting."""
    job, cookies = job_with_cookies
    driver = init_worker_browser(cookies)
    try:
        driver.get(job['view_url'])
        view_apply_data = parse_view_apply_page_with_bs(driver.page_source)

        driver.get(job['updates_url'])
        round_links = parse_updates_page_with_bs(driver.page_source)

        rounds_data = []
        for rd in round_links:
            driver.get(rd['url'])
            time.sleep(1)  # Small delay for page to render
            count = parse_shortlist_page_with_bs(driver.page_source)
            rounds_data.append({'round': rd['name'], 'count': count})

        return [
            job['name'], job['date_posted'], view_apply_data['arrived_for'],
            json.dumps(view_apply_data['salaries_fte']),
            json.dumps(view_apply_data['stipends_internship']),
            json.dumps(rounds_data), datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ]
    except Exception as e:
        # It's important to log errors from workers
        logging.getLogger(__name__).error(f"Worker for {job['name']} failed: {e}")
        return None
    finally:
        driver.quit()


def scrape_ppo_worker(ppo_with_cookies):
    """Worker function to scrape a single PPO posting."""
    ppo, cookies = ppo_with_cookies
    driver = init_worker_browser(cookies)
    try:
        driver.get(ppo['updates_url'])
        round_links = parse_updates_page_with_bs(driver.page_source)
        count = 0
        if round_links:
            driver.get(round_links[0]['url'])  # Assume first link is the final PPO list
            time.sleep(1)
            count = parse_shortlist_page_with_bs(driver.page_source)
        return {'name': ppo['name'], 'count': count}
    except Exception as e:
        logging.getLogger(__name__).error(f"PPO Worker for {ppo['name']} failed: {e}")
        return None
    finally:
        driver.quit()


# --- MAIN SCRIPT LOGIC ---
if __name__ == "__main__":
    gspread_client = get_gspread_client()
    if not gspread_client: exit()

    main_driver = None
    try:
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", {"credentials_enable_service": False,
                                                         "profile.password_manager_enabled": False})
        service = webdriver.ChromeService(executable_path=WEBDRIVER_PATH)
        main_driver = webdriver.Chrome(service=service, options=chrome_options)
        wait = WebDriverWait(main_driver, 20)

        main_driver.get(PORTAL_URL)
        main_driver.maximize_window()
        wait.until(EC.presence_of_element_located((By.ID, "identity"))).send_keys(USERNAME)
        main_driver.find_element(By.ID, "password").send_keys(PASSWORD)
        main_driver.find_element(By.XPATH, "//input[@value='Login']").click()
        logger.info("MainProcess: Login successful. Extracting session cookies.")
        wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))
        session_cookies = {cookie['name']: cookie['value'] for cookie in main_driver.get_cookies()}

        year_options = ['2025-26']
        for year in year_options:
            logger.info(f"====== STARTING YEAR: {year} ======")
            worksheet_main_name = f"scraped_data_{year.split('-')[0][-2:]}"
            worksheet_ppo_name = f"ppo_data_{year.split('-')[0][-2:]}"

            try:
                spreadsheet = gspread_client.open_by_key(G_SHEET_ID)
                main_sheet = spreadsheet.worksheet(worksheet_main_name)
                try:
                    ppo_sheet = spreadsheet.worksheet(worksheet_ppo_name)
                except gspread.WorksheetNotFound:
                    logger.info(f"MainProcess: Worksheet '{worksheet_ppo_name}' not found. Creating it...")
                    ppo_sheet = spreadsheet.add_worksheet(title=worksheet_ppo_name, rows="100", cols="3")
                    ppo_sheet.append_row(['Company Name', 'PPO Student Count', 'Scrape Timestamp'])
            except Exception as e:
                logger.error(f"MainProcess: Cannot open worksheets for year {year}. Skipping. Error: {e}")
                continue

            Select(wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))).select_by_visible_text(year)
            time.sleep(3)

            page_number = 1
            while True:
                logger.info(f"--- MainProcess: Processing Page {page_number} for year {year} ---")
                wait.until(EC.presence_of_element_located((By.ID, "job-listings_info")))
                time.sleep(2)

                jobs_on_page, ppos_on_page = parse_main_page_with_bs(main_driver.page_source)

                if jobs_on_page:
                    work_items = [(job, session_cookies) for job in jobs_on_page]
                    with Pool(processes=NUM_PROCESSES) as pool:
                        results = pool.map(scrape_job_worker, work_items)
                    successful_results = [res for res in results if res is not None]
                    if successful_results:
                        main_sheet.append_rows(successful_results, value_input_option='USER_ENTERED')
                        logger.info(
                            f"MainProcess: Uploaded {len(successful_results)} job rows from page {page_number}.")

                if ppos_on_page:
                    ppo_work_items = [(ppo, session_cookies) for ppo in ppos_on_page]
                    with Pool(processes=NUM_PROCESSES) as pool:
                        ppo_results = pool.map(scrape_ppo_worker, ppo_work_items)
                    successful_ppos = [res for res in ppo_results if res is not None]
                    if successful_ppos:
                        rows_to_add = [[item['name'], item['count'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')] for
                                       item in successful_ppos]
                        ppo_sheet.append_rows(rows_to_add)
                        logger.info(f"MainProcess: Uploaded {len(successful_ppos)} PPO rows from page {page_number}.")

                try:
                    WebDriverWait(main_driver, 5).until(
                        EC.presence_of_element_located((By.ID, "job-listings_paginate")))
                    next_button = main_driver.find_element(By.ID, "job-listings_next")
                    if "disabled" in next_button.get_attribute("class"):
                        logger.info(f"MainProcess: Reached the last page for year {year}.")
                        break
                    main_driver.execute_script("arguments[0].click();", next_button.find_element(By.TAG_NAME, "a"))
                    page_number += 1
                except (NoSuchElementException, TimeoutException):
                    logger.info(f"MainProcess: No more pages found for year {year}.")
                    break

            logger.info(f"====== COMPLETED YEAR: {year} ======")

    except Exception as e:
        logger.error(f"A critical error occurred in the main script: {e}", exc_info=True)
    finally:
        if main_driver:
            main_driver.quit()
        logger.info("--- SCRIPT COMPLETE ---")
