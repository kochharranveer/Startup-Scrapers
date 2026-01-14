import os
import re
import time
import random
import logging
import pandas as pd
import cloudscraper
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import urllib3
import backoff
from datetime import datetime
import brotli
from urllib.parse import urljoin
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from playwright.sync_api import sync_playwright
import json
import sys

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if all required dependencies are installed."""
    required_packages = {
        'beautifulsoup4': 'bs4',
        'pandas': 'pandas',
        'cloudscraper': 'cloudscraper',
        'fake-useragent': 'fake_useragent',
        'urllib3': 'urllib3',
        'backoff': 'backoff'
    }
    
    missing_packages = []
    for package, import_name in required_packages.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error("Missing required packages. Please install them using:")
        logger.error(f"pip install {' '.join(missing_packages)}")
        return False
    return True

class ContactScraper:
    def __init__(self):
        # Check dependencies first
        if not check_dependencies():
            raise ImportError("Missing required dependencies")
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        
        # Initialize cloudscraper session
        self.session = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            },
            delay=10
        )
        
        # Create html_files directory if it doesn't exist
        os.makedirs('html_files', exist_ok=True)
        
        # Initialize contact details list
        self.contact_details = []
        
        # Create a UserAgent instance
        self.ua = UserAgent()
        
        # Create a cloudscraper session with custom browser settings
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'mobile': False
            }
        )
        
        # Set custom headers
        self.scraper.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
        
        # Initialize Playwright
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=['--ignore-certificate-errors', '--ignore-ssl-errors']
        )
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            ignore_https_errors=True
        )

    def _get_random_delay(self):
        """Get a random delay between requests."""
        return random.uniform(3, 6)

    def load_companies(self, filename='company_data.csv'):
        """Load company data from CSV file."""
        try:
            df = pd.read_csv(filename)
            logger.info(f"\nLoaded {len(df)} companies from {filename}")
            return df
        except Exception as e:
            logger.error(f"Error loading companies: {str(e)}")
            return None

    def format_url(self, company_name, cin):
        """Format company name for URL."""
        # Replace spaces and special characters with hyphens
        formatted_name = re.sub(r'[^\w\s-]', '', company_name)
        formatted_name = re.sub(r'\s+', '-', formatted_name.strip())
        formatted_name = formatted_name.upper()
        
        # Create the URL
        if cin:
            url = f"https://www.zaubacorp.com/company/{formatted_name}/{cin}"
        else:
            url = f"https://www.zaubacorp.com/company/{formatted_name}/"
        
        return url

    def save_content(self, filename, content, mode='w', encoding=None):
        try:
            if mode == 'w' and encoding:
                with open(filename, mode, encoding=encoding) as f:
                    f.write(content)
            else:
                with open(filename, mode) as f:
                    f.write(content)
            logger.info(f"Successfully saved content to {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving content to {filename}: {str(e)}")
            return False

    def extract_contact_details(self, soup):
        """Extract contact details from the HTML content."""
        contact_info = {
            'email': 'Not Available',
            'phone': 'Not Available',
            'address': 'Not Available',
            'website': 'Not Available'
        }
        
        # Look for contact details in various sections
        sections = [
            soup.find('div', {'id': 'contact-details'}),
            soup.find('div', {'class': 'col-md-6'}),
            soup.find('div', {'class': 'company-details'}),
            soup.find('div', {'class': 'contact-info'})
        ]
        
        # Combine all sections' text for searching
        all_text = ''
        for section in sections:
            if section:
                all_text += section.get_text() + ' '
        
        # Extract email
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        email_matches = re.findall(email_pattern, all_text)
        if email_matches:
            contact_info['email'] = email_matches[0]
            logger.info(f"Found email: {email_matches[0]}")
        
        # Extract phone
        phone_pattern = r'(?:(?:\+|0{0,2})91(\s*[\-]\s*)?|[0]?)?[789]\d{9}'
        phone_matches = re.findall(phone_pattern, all_text)
        if phone_matches:
            contact_info['phone'] = phone_matches[0]
            logger.info(f"Found phone: {phone_matches[0]}")
        
        # Extract website
        website_pattern = r'(?:https?:\/\/)?(?:www\.)?[a-zA-Z0-9-]+(?:\.[a-zA-Z]{2,})+(?:\/[^\s]*)?'
        website_matches = re.findall(website_pattern, all_text)
        if website_matches:
            contact_info['website'] = website_matches[0]
            logger.info(f"Found website: {website_matches[0]}")
        
        # Extract address
        # Look for address in specific elements
        address_elements = soup.find_all(['p', 'div', 'span'], string=re.compile(r'[A-Za-z0-9\s,\.-]+'))
        for element in address_elements:
            text = element.get_text(strip=True)
            if len(text) > 20 and not re.search(email_pattern, text) and not re.search(website_pattern, text):
                contact_info['address'] = text
                logger.info(f"Found address: {text}")
                break
        
        return contact_info

    def get_contact_details(self, company_name, url, cin):
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logger.info(f"Attempt {retry_count + 1} for {company_name}")
                
                # Add random delay between requests
                time.sleep(random.uniform(5, 10))
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Save raw HTML
                safe_company_name = re.sub(r'[^\w\-_\. ]', '_', company_name)
                raw_file_path = f'html_files/{safe_company_name}_raw.html'
                with open(raw_file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract email from JSON-LD structured data
                email = 'Not Available'
                json_ld = soup.find('script', {'type': 'application/ld+json'})
                if json_ld:
                    try:
                        data = json.loads(json_ld.string)
                        if 'email' in data:
                            email = data['email']
                    except:
                        pass
                
                # If no email found in JSON-LD, try Cloudflare protected email
                if email == 'Not Available':
                    email_elem = soup.find('a', class_='__cf_email__')
                    if email_elem and 'data-cfemail' in email_elem.attrs:
                        encoded_email = email_elem['data-cfemail']
                        email = self.decode_cloudflare_email(encoded_email)
                
                self.logger.info(f"Successfully extracted contact details for {company_name}")
                return {
                    'company_name': company_name,
                    'email': email,
                    'cin': cin
                }
                
            except (requests.exceptions.RequestException, cloudscraper.exceptions.CloudflareChallengeError) as e:
                retry_count += 1
                self.logger.warning(f"Attempt {retry_count} failed for {company_name}: {str(e)}")
                if retry_count < max_retries:
                    wait_time = random.uniform(10, 20)
                    self.logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All attempts failed for {company_name}")
                    return {
                        'company_name': company_name,
                        'email': 'Not Available',
                        'cin': cin
                    }
            except Exception as e:
                self.logger.error(f"Error processing {company_name}: {str(e)}")
                return {
                    'company_name': company_name,
                    'email': 'Not Available',
                    'cin': cin
                }

    def decode_cloudflare_email(self, encoded_email):
        try:
            # Convert hex to bytes
            encoded_bytes = bytes.fromhex(encoded_email)
            
            # First byte is the key to XOR with
            key = encoded_bytes[0]
            
            # XOR each subsequent byte with the key
            decoded = ''
            for b in encoded_bytes[1:]:
                decoded += chr(b ^ key)
            
            return decoded
        except Exception as e:
            self.logger.error(f"Error decoding email: {str(e)}")
            return 'Not Available'

    def scrape_companies(self, companies_df):
        contact_details = []
        total_companies = len(companies_df)
        successful = 0
        failed = 0
        
        try:
            for index, row in companies_df.iterrows():
                company_name = row['Name']
                cin = row['CIN']
                url = self.format_url(company_name, cin)
                
                logger.info(f"\nProcessing {index + 1}/{total_companies} ({((index + 1)/total_companies)*100:.1f}%)")
                logger.info(f"Company: {company_name}")
                logger.info(f"URL: {url}")
                
                contact_info = self.get_contact_details(company_name, url, cin)
                
                if contact_info:
                    contact_details.append(contact_info)
                    successful += 1
                    logger.info(f"Successfully processed {company_name}")
                else:
                    failed += 1
                    contact_details.append({
                        'company_name': company_name,
                        'email': 'Not Available',
                        'cin': cin
                    })
                
                # Random delay between requests
                delay = random.uniform(5, 8)
                logger.info(f"Waiting {delay:.1f} seconds before next request...")
                time.sleep(delay)
        
        except KeyboardInterrupt:
            logger.info("\nScript interrupted by user")
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
        finally:
            # Close browser and Playwright
            try:
                self.context.close()
                self.browser.close()
                self.playwright.stop()
            except:
                pass
            
            logger.info("\nProcessing complete:")
            logger.info(f"Successful: {successful}/{total_companies}")
            logger.info(f"Failed: {failed}/{total_companies}")
            
            # Save results to CSV
            logger.info("Saving partial results...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f'contact_details_{timestamp}.csv'
            df = pd.DataFrame(contact_details)
            df.to_csv(output_file, index=False)
            logger.info(f"\nSaved {len(contact_details)} contact details to {output_file}")
            
            # Print summary statistics
            logger.info("\nSummary Statistics:")
            logger.info(f"Total companies processed: {len(contact_details)}")
            logger.info(f"Companies with email: {sum(1 for c in contact_details if c['email'] != 'Not Available')}")

def main():
    scraper = None
    try:
        # Check if company_data.csv exists
        if not os.path.exists('company_data.csv'):
            logger.error("company_data.csv not found. Please create it with company data.")
            return
        
        scraper = ContactScraper()
        companies_df = scraper.load_companies()
        
        if companies_df is None or companies_df.empty:
            logger.error("No companies loaded. Please check company_data.csv")
            return
        
        scraper.scrape_companies(companies_df)
        
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main() 