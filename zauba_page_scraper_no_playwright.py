import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
import logging
import backoff
import json
import os
import urllib3
import cloudscraper

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZaubaPageScraper:
    def __init__(self):
        # Create a cloudscraper session
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'mobile': False
            }
        )
        
        # Load or create session data
        self.session_file = 'page_session_data.json'
        self.load_session()
        
        # Initialize companies list
        self.companies = []
        
        # Set up headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }

    def load_session(self):
        """Load or create session data."""
        try:
            if os.path.exists(self.session_file):
                with open(self.session_file, 'r') as f:
                    self.session_data = json.load(f)
            else:
                self.session_data = {
                    'last_page_index': 1,  # Start from page 2 (index 1)
                    'companies': []
                }
        except Exception as e:
            logger.error(f"Error loading session: {str(e)}")
            self.session_data = {'last_page_index': 1, 'companies': []}

    def save_session(self):
        """Save current session data."""
        try:
            with open(self.session_file, 'w') as f:
                json.dump(self.session_data, f)
        except Exception as e:
            logger.error(f"Error saving session: {str(e)}")

    @backoff.on_exception(backoff.expo, 
                         Exception,
                         max_tries=3,
                         jitter=backoff.full_jitter)
    def scrape_page(self, page_number):
        """Scrape a specific page of company listings."""
        try:
            # Construct the URL for the page
            url = f'https://www.zaubacorp.com/companies-list/age-A/p-{page_number}-company.html'
            
            logger.info(f"\nScraping page {page_number}: {url}")
            
            # Try with cloudscraper
            try:
                response = self.scraper.get(url, headers=self.headers)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find the table in the container information div
                    container_info = soup.find('div', class_='container information')
                    if container_info:
                        table = container_info.find('table')
                        if table:
                            # Process the table from cloudscraper response
                            rows = table.find_all('tr')
                            if rows:
                                # Skip header row if present
                                start_idx = 1 if len(rows) > 1 else 0
                                
                                for row in rows[start_idx:]:
                                    cols = row.find_all('td')
                                    if len(cols) >= 2:
                                        # Extract CIN and Name from the first two columns
                                        cin_element = cols[0].find('a')
                                        name_element = cols[1].find('a')
                                        
                                        if cin_element and name_element:
                                            cin = cin_element.get_text(strip=True)
                                            name = name_element.get_text(strip=True)
                                            
                                            logger.info(f"Found company: {cin} - {name}")
                                            
                                            self.companies.append({
                                                'CIN': cin,
                                                'Name': name
                                            })
                                
                                # Save after successful page scrape
                                self.save_results()
                                self.save_session()
                                
                                # Random delay between requests (5-8 seconds)
                                delay = random.uniform(5, 8)
                                logger.info(f"Waiting {delay:.1f} seconds before next page...")
                                time.sleep(delay)
                                return True
            except Exception as e:
                logger.warning(f"Cloudscraper attempt failed: {str(e)}")
            
            # If cloudscraper fails, try with regular requests
            try:
                response = requests.get(url, headers=self.headers, verify=False)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find the table in the container information div
                    container_info = soup.find('div', class_='container information')
                    if container_info:
                        table = container_info.find('table')
                        if table:
                            # Process the table
                            rows = table.find_all('tr')
                            if rows:
                                # Skip header row if present
                                start_idx = 1 if len(rows) > 1 else 0
                                
                                for row in rows[start_idx:]:
                                    cols = row.find_all('td')
                                    if len(cols) >= 2:
                                        # Extract CIN and Name from the first two columns
                                        cin_element = cols[0].find('a')
                                        name_element = cols[1].find('a')
                                        
                                        if cin_element and name_element:
                                            cin = cin_element.get_text(strip=True)
                                            name = name_element.get_text(strip=True)
                                            
                                            logger.info(f"Found company: {cin} - {name}")
                                            
                                            self.companies.append({
                                                'CIN': cin,
                                                'Name': name
                                            })
                                
                                # Save after successful page scrape
                                self.save_results()
                                self.save_session()
                                
                                # Random delay between requests (5-8 seconds)
                                delay = random.uniform(5, 8)
                                logger.info(f"Waiting {delay:.1f} seconds before next page...")
                                time.sleep(delay)
                                return True
            except Exception as e:
                logger.warning(f"Requests attempt failed: {str(e)}")
            
            logger.error(f"Failed to scrape page {page_number} with both methods")
            return False
            
        except Exception as e:
            logger.error(f"Error processing page {page_number}: {str(e)}")
            raise

    def save_results(self, output_file='zauba_companies.csv'):
        if self.companies:
            df = pd.DataFrame(self.companies)
            df.to_csv(output_file, index=False)
            logger.info(f"\nSaved {len(self.companies)} companies to {output_file}")
        else:
            logger.warning("No companies to save")

def main():
    scraper = None
    try:
        scraper = ZaubaPageScraper()
        
        # Define the range of pages to scrape (2 to 175)
        start_page = scraper.session_data.get('last_page_index', 1)  # Start from page 2 (index 1)
        end_page = 497
        
        logger.info(f"Starting scraping from page {start_page + 1} to {end_page}")
        
        for page_num in range(start_page, end_page + 1):
            try:
                logger.info(f"\nProcessing page {page_num + 1}/{end_page}")
                success = scraper.scrape_page(page_num + 1)  # +1 because page numbers start at 2
                
                if success:
                    scraper.session_data['last_page_index'] = page_num
                    scraper.save_session()
                else:
                    logger.error(f"Failed to scrape page {page_num + 1}")
                    # Add a longer delay before retrying
                    delay = random.uniform(15, 30)
                    logger.info(f"Waiting {delay:.1f} seconds before retrying...")
                    time.sleep(delay)
                    continue
                
            except Exception as e:
                logger.error(f"Error processing page {page_num + 1}: {str(e)}")
                # Add a longer delay before retrying
                delay = random.uniform(15, 30)
                logger.info(f"Waiting {delay:.1f} seconds before retrying...")
                time.sleep(delay)
                continue
            
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        if scraper:
            logger.info("Scraping completed or interrupted")

if __name__ == "__main__":
    main() 