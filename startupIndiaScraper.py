import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import pandas as pd
import random
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

def setup_driver():
    try:
        options = uc.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Add random user agent
        user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
        ]
        options.add_argument(f'user-agent={random.choice(user_agents)}')
        
        driver = uc.Chrome(options=options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        logging.error(f"Error setting up driver: {str(e)}")
        raise

def random_delay():
    """Add random delay between actions to appear more human-like"""
    time.sleep(random.uniform(2, 5))

def scrape_startup_india():
    driver = None
    companies = []
    page = 0
    
    try:
        driver = setup_driver()
        logging.info("Driver setup successful")
        
        while True:
            url = f"https://www.startupindia.gov.in/content/sih/en/search.html?roles=Startup&page={page}#"
            logging.info(f"Accessing page {page + 1}...")
            
            try:
                driver.get(url)
                random_delay()
                
                # Wait for the company cards to load
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "company-card"))
                    )
                except TimeoutException:
                    logging.warning(f"No more companies found on page {page}")
                    break
                
                # Extract company names
                company_elements = driver.find_elements(By.CLASS_NAME, "company-card")
                
                if not company_elements:
                    logging.info("No company elements found on page")
                    break
                
                for element in company_elements:
                    try:
                        company_name = element.find_element(By.CLASS_NAME, "company-name").text
                        if company_name:
                            companies.append(company_name)
                            logging.info(f"Found company: {company_name}")
                    except NoSuchElementException:
                        continue
                
                logging.info(f"Successfully scraped page {page + 1}")
                page += 1
                random_delay()
                
            except Exception as e:
                logging.error(f"Error on page {page}: {str(e)}")
                break
            
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    
    finally:
        if driver:
            driver.quit()
            logging.info("Driver closed")
    
    return companies

def save_to_csv(companies):
    if not companies:
        logging.warning("No companies found to save")
        return
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'startup_india_companies_{timestamp}.csv'
    
    df = pd.DataFrame(companies, columns=['Company Name'])
    df.to_csv(filename, index=False)
    logging.info(f"Saved {len(companies)} companies to {filename}")

if __name__ == "__main__":
    logging.info("Starting to scrape Startup India website...")
    companies = scrape_startup_india()
    save_to_csv(companies)
    logging.info("Scraping completed!")
