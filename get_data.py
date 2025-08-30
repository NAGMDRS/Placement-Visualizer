import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
import os

load_dotenv()
PORTAL_URL = os.getenv('PORTAL_URL')
USERNAME = os.getenv('TPUSERNAME')
PASSWORD = os.getenv('PASSWORD')
WEBDRIVER_PATH = os.getenv('WEBDRIVER_PATH')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


service = webdriver.ChromeService(executable_path=WEBDRIVER_PATH)
driver = webdriver.Chrome(service=service)
wait = WebDriverWait(driver, 10)


def open_update_links_for_current_year():
    """
    Handles pagination for the jobs table. It opens 'Updates' links on the current page,
    then clicks 'Next' and repeats until all pages for the current year are processed.
    """
    page_number = 1
    while True:
        try:
            logging.info(f"Processing page {page_number}...")
            # Wait for the table to be visible
            job_table = wait.until(EC.visibility_of_element_located((By.ID, "job-listings")))

            # Find and open all "Updates" links on the current page
            update_links_elements = job_table.find_elements(By.LINK_TEXT, "Updates")

            if not update_links_elements:
                logging.info("No 'Updates' links found on this page.")
            else:
                update_urls = [link.get_attribute('href') for link in update_links_elements]
                logging.info(f"Found {len(update_urls)} links. Opening in new tabs...")
                for url in update_urls:
                    driver.execute_script("window.open(arguments[0], '_blank');", url)
                    time.sleep(1)  # Brief pause to manage tab creation

            next_button_li = driver.find_element(By.ID, "job-listings_next")

            if "disabled" in next_button_li.get_attribute("class"):
                logging.info("Reached the last page of job listings for this year.")
                break
            else:
                logging.info("Clicking the 'Next' button...")
                next_button_a = next_button_li.find_element(By.TAG_NAME, "a")
                driver.execute_script("arguments[0].click();", next_button_a)
                page_number += 1
                time.sleep(2)

        except TimeoutException:
            logging.warning("Timed out waiting for the job table on this page.")
            break
        except Exception as e:
            logging.error("An error occurred during pagination:", exc_info=True)
            break


try:
    logging.info("Navigating to the placement portal...")
    driver.get(PORTAL_URL)
    driver.maximize_window()

    logging.info("Entering login credentials...")
    username_field = wait.until(EC.presence_of_element_located((By.ID, "identity")))
    password_field = driver.find_element(By.ID, "password")
    login_button = driver.find_element(By.XPATH, "//input[@value='Login']")

    username_field.clear()
    username_field.send_keys(USERNAME)
    password_field.clear()
    password_field.send_keys(PASSWORD)

    login_button.click()
    logging.info("Login successful. Navigating to the main dashboard...")

    wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))
    main_window_handle = driver.current_window_handle

    year_options = ['2025-26']

    for year in year_options:
        logging.info(f"Processing year: {year}")
        try:
            current_year_select_element = wait.until(EC.presence_of_element_located((By.ID, "_placeyr")))
            current_year_select = Select(current_year_select_element)

            current_year_select.select_by_visible_text(year)
            logging.info(f"Switched to {year}.")
            time.sleep(3)

            open_update_links_for_current_year()

        except TimeoutException:
            logging.warning(f"Could not find the year dropdown after a refresh. Skipping {year}.")
            continue
        except Exception as e:
            logging.error(f"An error occurred while processing year {year}:", exc_info=True)

    driver.switch_to.window(main_window_handle)

except TimeoutException:
    logging.error("A timeout occurred. An element might not be available or the page took too long to load.",
                  exc_info=True)
except Exception as e:
    logging.error("An unexpected error occurred:", exc_info=True)

finally:
    logging.info("Script finished. All tabs will be closed in 15 seconds...")
    time.sleep(15)
    driver.quit()

