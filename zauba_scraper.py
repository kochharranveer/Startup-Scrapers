from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
import logging
import backoff
import json
import os
import requests
from fake_useragent import UserAgent
import urllib3
import cloudscraper

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZaubaScraper:
    def __init__(self):
        self.playwright = sync_playwright().start()
        
        # Launch browser with specific options
        self.browser = self.playwright.chromium.launch(
            headless=False,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',
                '--window-size=1920,1080'
            ]
        )
        
        # Create a new context with specific options
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=UserAgent().random,
            ignore_https_errors=True,
            bypass_csp=True
        )
        
        # Create a new page
        self.page = self.context.new_page()
        
        # Create a cloudscraper session
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'mobile': False
            }
        )
        
        # Load or create session data
        self.session_file = 'session_data.json'
        self.load_session()
        
        # Initialize companies list
        self.companies = []
        
        # Set default timeout
        self.page.set_default_timeout(30000)
        
        # Navigate to the website and handle initial setup
        try:
            self.page.goto('https://www.zaubacorp.com')
            time.sleep(random.uniform(3, 5))  # Wait for page to load
            
            # Handle any potential popups or overlays
            try:
                self.page.click('button:has-text("Close")', timeout=5000)
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error during initial setup: {str(e)}")

    def _get_proxy(self):
        """Get a proxy from a proxy service."""
        try:
            # You can replace this with your preferred proxy service
            # For now, return None to use direct connection
            return None
        except Exception as e:
            logger.error(f"Error getting proxy: {str(e)}")
            return None

    def _get_cloudflare_clearance(self):
        """Get Cloudflare clearance using cloudscraper."""
        try:
            response = self.scraper.get('https://www.zaubacorp.com')
            cookies = self.scraper.cookies.get_dict()
            return cookies.get('cf_clearance')
        except Exception as e:
            logger.error(f"Error getting Cloudflare clearance: {str(e)}")
            return None

    def load_session(self):
        """Load or create session data."""
        try:
            if os.path.exists(self.session_file):
                with open(self.session_file, 'r') as f:
                    self.session_data = json.load(f)
            else:
                self.session_data = {
                    'last_company_index': 0,
                    'companies': []
                }
        except Exception as e:
            logger.error(f"Error loading session: {str(e)}")
            self.session_data = {'last_company_index': 0, 'companies': []}

    def save_session(self):
        """Save current session data."""
        try:
            with open(self.session_file, 'w') as f:
                json.dump(self.session_data, f)
        except Exception as e:
            logger.error(f"Error saving session: {str(e)}")

    def simulate_human_behavior(self):
        """Simulate human-like behavior on the page."""
        try:
            # Random scroll
            for _ in range(random.randint(2, 4)):
                scroll_amount = random.randint(100, 300)
                self.page.mouse.wheel(0, scroll_amount)
                time.sleep(random.uniform(0.5, 1.5))
            
            # Random mouse movements
            for _ in range(random.randint(2, 4)):
                x = random.randint(50, 800)
                y = random.randint(50, 600)
                self.page.mouse.move(x, y)
                time.sleep(random.uniform(0.3, 0.7))
            
        except Exception as e:
            logger.warning(f"Error in simulate_human_behavior: {str(e)}")

    @backoff.on_exception(backoff.expo, 
                         Exception,
                         max_tries=3,
                         jitter=backoff.full_jitter)
    def search_companies(self, company_name):
        try:
            # Skip empty or test entries
            if not company_name or company_name.lower() == 'name':
                logger.info(f"Skipping invalid company name: {company_name}")
                return
                
            # Format company name for URL
            formatted_name = company_name.strip().upper().replace(' ', '-')
            url = f'https://www.zaubacorp.com/companysearchresults/{formatted_name}'
            
            logger.info(f"\nSearching for: {company_name}")
            
            # First try with cloudscraper
            try:
                response = self.scraper.get(url)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    table = soup.find('table')
                    if table:
                        # Process the table from cloudscraper response
                        rows = table.find_all('tr')
                        if rows:
                            # Skip header row if present
                            start_idx = 1 if len(rows) > 1 else 0
                            
                            for row in rows[start_idx:]:
                                cols = row.find_all('td')
                                if len(cols) >= 3:
                                    cin = cols[0].get_text(strip=True)
                                    name = cols[1].get_text(strip=True)
                                    address = cols[2].get_text(strip=True)
                                    
                                    # Only add if name matches exactly (case-insensitive)
                                    if name.upper() == company_name.upper():
                                        logger.info(f"Found exact match: {name} (CIN: {cin})")
                                        self.companies.append({
                                            'CIN': cin,
                                            'Name': name,
                                            'Address': address
                                        })
                            
                            # Save after successful search
                            self.save_results()
                            self.save_session()
                            
                            # Random delay between requests (8-12 seconds)
                            delay = random.uniform(8, 12)
                            logger.info(f"Waiting {delay:.1f} seconds before next search...")
                            time.sleep(delay)
                            return
            except Exception as e:
                logger.warning(f"Cloudscraper attempt failed: {str(e)}")
            
            # If cloudscraper fails, try with Playwright
            self.page.goto(url)
            time.sleep(random.uniform(5, 8))
            
            # Simulate human behavior
            self.simulate_human_behavior()
            
            # Wait for table
            table = self.page.wait_for_selector('table')
            if not table:
                logger.error(f"No table found for: '{company_name}'")
                return
            
            # Get table HTML
            table_html = table.inner_html()
            soup = BeautifulSoup(table_html, 'html.parser')
            
            # Process rows
            rows = soup.find_all('tr')
            if not rows:
                logger.error(f"No rows found for: '{company_name}'")
                return
            
            # Skip header row if present
            start_idx = 1 if len(rows) > 1 else 0
            
            for row in rows[start_idx:]:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    cin = cols[0].get_text(strip=True)
                    name = cols[1].get_text(strip=True)
                    address = cols[2].get_text(strip=True)
                    
                    # Only add if name matches exactly (case-insensitive)
                    if name.upper() == company_name.upper():
                        logger.info(f"Found exact match: {name} (CIN: {cin})")
                        self.companies.append({
                            'CIN': cin,
                            'Name': name,
                            'Address': address
                        })
            
            # Save after each successful search
            self.save_results()
            self.save_session()
            
            # Random delay between requests (8-12 seconds)
            delay = random.uniform(8, 12)
            logger.info(f"Waiting {delay:.1f} seconds before next search...")
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Error processing '{company_name}': {str(e)}")
            raise

    def load_company_names(self, filename='AdTech_SI_Names.csv'):
        try:
            with open(filename, 'r') as file:
                company_names = [line.strip() for line in file if line.strip()]
            logger.info(f"\nLoaded {len(company_names)} company names")
            return company_names
        except Exception as e:
            logger.error(f"Error loading company names: {str(e)}")
            return []

    def save_results(self, output_file='company_data.csv'):
        if self.companies:
            df = pd.DataFrame(self.companies)
            df.to_csv(output_file, index=False)
            logger.info(f"\nSaved {len(self.companies)} companies to {output_file}")
        else:
            logger.warning("No companies to save")

    def cleanup_and_recover(self):
        """Clean up browser state and recover from errors."""
        try:
            # Clear cookies and cache
            self.context.clear_cookies()
            
            # Refresh the page
            self.page.reload()
            time.sleep(5)
            
            return True
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            return False

    def close(self):
        """Close the browser and clean up resources."""
        try:
            if hasattr(self, 'browser'):
                self.browser.close()
            if hasattr(self, 'playwright'):
                self.playwright.stop()
        except Exception as e:
            logger.error(f"Error closing browser: {str(e)}")

def main():
    scraper = None
    try:
        scraper = ZaubaScraper()
        company_names = scraper.load_company_names()
        if not company_names:
            logger.error("No company names loaded. Exiting.")
            return

        # Start from last processed company if available
        start_index = scraper.session_data.get('last_company_index', 0)
        logger.info(f"Resuming from company {start_index + 1}")

        for i, company_name in enumerate(company_names[start_index:], start_index + 1):
            try:
                logger.info(f"\nProcessing {i}/{len(company_names)}")
                scraper.search_companies(company_name)
                scraper.session_data['last_company_index'] = i
                scraper.save_session()
            except Exception as e:
                logger.error(f"Error processing company {i}: {str(e)}")
                # Try to recover
                if not scraper.cleanup_and_recover():
                    logger.error("Failed to recover. Exiting.")
                    break
                continue
            
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        if scraper:
            scraper.close()
            logger.info("Browser closed and resources cleaned up")

if __name__ == "__main__":
    main() 