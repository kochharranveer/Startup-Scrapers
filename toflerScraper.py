import logging
import sys
import asyncio
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import time
import random
import pandas as pd
import json
import os
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import urllib3
import re
from threading import Lock
import backoff
import nest_asyncio

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tofler_ultra_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Enable nested event loops
nest_asyncio.apply()

class BrowserManager:
    def __init__(self, max_browsers=2):
        self.max_browsers = max_browsers
        self.semaphore = asyncio.Semaphore(max_browsers)
        self.browser_pool = []
        self.playwright = None
        self.lock = asyncio.Lock()
        self.initialized = False
        self.last_request_time = 0
        self.min_request_interval = 5  # Minimum seconds between requests
        self.proxies = [
            # Add your proxies here in the format:
            # {'server': 'http://proxy1.example.com:8080', 'username': 'user1', 'password': 'pass1'},
            # {'server': 'http://proxy2.example.com:8080', 'username': 'user2', 'password': 'pass2'},
        ]
        self.current_proxy_index = 0
        
    def get_next_proxy(self):
        """Get the next proxy from the pool."""
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
        return proxy
        
    async def initialize(self):
        """Initialize the playwright instance."""
        if not self.initialized:
            async with self.lock:
                if not self.initialized:
                    try:
                        self.playwright = await async_playwright().start()
                        self.initialized = True
                        logger.info("Playwright initialized successfully")
                    except Exception as e:
                        logger.error(f"Error initializing Playwright: {str(e)}")
                        raise
        
    async def get_browser(self):
        """Get a browser from the pool or create a new one."""
        await self.initialize()
        
        # Ensure minimum time between requests
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = current_time
        
        async with self.semaphore:
            try:
                if self.browser_pool:
                    browser, context = self.browser_pool.pop()
                    if browser.is_connected():
                        return browser, context
                    else:
                        await self.cleanup_browser(browser, context)
            except Exception as e:
                logger.error(f"Error getting browser from pool: {str(e)}")
            
            # Create new browser
            try:
                proxy = self.get_next_proxy()
                browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-accelerated-2d-canvas',
                        '--disable-gpu',
                        '--window-size=1920,1080'
                    ]
                )
                
                context_options = {
                    'viewport': {'width': 1920, 'height': 1080},
                    'user_agent': UserAgent().random,
                    'ignore_https_errors': True,
                    'bypass_csp': True
                }
                
                if proxy:
                    context_options['proxy'] = {
                        'server': proxy['server'],
                        'username': proxy['username'],
                        'password': proxy['password']
                    }
                
                context = await browser.new_context(**context_options)
                
                # Set extra headers
                await context.set_extra_http_headers({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                })
                
                return browser, context
            except Exception as e:
                logger.error(f"Error creating new browser: {str(e)}")
                raise
            
    async def return_browser(self, browser_tuple):
        """Return a browser to the pool."""
        if not isinstance(browser_tuple, tuple) or len(browser_tuple) != 2:
            logger.error("Invalid browser tuple provided")
            return
            
        browser, context = browser_tuple
        try:
            if browser.is_connected():
                self.browser_pool.append((browser, context))
            else:
                await self.cleanup_browser(browser, context)
        except Exception as e:
            logger.error(f"Error returning browser: {str(e)}")
            await self.cleanup_browser(browser, context)
        finally:
            self.semaphore.release()
            
    async def cleanup_browser(self, browser, context):
        """Clean up browser resources."""
        try:
            if context:
                await context.close()
            if browser:
                await browser.close()
        except Exception as e:
            logger.error(f"Error cleaning up browser: {str(e)}")
            
    async def close_all(self):
        """Close all browser instances and playwright."""
        try:
            while self.browser_pool:
                browser, context = self.browser_pool.pop()
                await self.cleanup_browser(browser, context)
                
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logger.error(f"Error closing all browsers: {str(e)}")

class ToflerUltraScraper:
    def __init__(self, max_workers=3):
        logger.info("Initializing ToflerUltraScraper...")
        self.max_workers = max_workers
        self.output_file = 'tofler_ultra_company_data.csv'
        self.session_file = 'tofler_ultra_session.json'
        self.companies = []
        self.result_queue = asyncio.Queue()
        self.lock = asyncio.Lock()
        self.browser_manager = BrowserManager(max_browsers=2)
        self.processed_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.load_session()
        logger.info(f"Initialized with max_workers={max_workers}")

    def load_session(self):
        """Load session data from file."""
        try:
            if os.path.exists(self.session_file):
                with open(self.session_file, 'r') as f:
                    session_data = json.load(f)
                    self.processed_count = session_data.get('processed_count', 0)
                    self.success_count = session_data.get('success_count', 0)
                    self.failure_count = session_data.get('failure_count', 0)
                logger.info(f"Loaded session data: processed={self.processed_count}, success={self.success_count}, failure={self.failure_count}")
            else:
                logger.info("No session file found, starting new session")
        except Exception as e:
            logger.error(f"Error loading session: {str(e)}")

    async def save_session(self):
        """Save session data to file."""
        try:
            session_data = {
                'processed_count': self.processed_count,
                'success_count': self.success_count,
                'failure_count': self.failure_count,
                'last_update': datetime.now().isoformat()
            }
            async with self.lock:
                with open(self.session_file, 'w') as f:
                    json.dump(session_data, f)
            logger.info(f"Saved session data: processed={self.processed_count}, success={self.success_count}, failure={self.failure_count}")
        except Exception as e:
            logger.error(f"Error saving session: {str(e)}")

    def format_company_name(self, name):
        """Format company name for URL."""
        formatted = re.sub(r'[^\w\s-]', '', name)
        formatted = re.sub(r'\s+', '-', formatted.strip())
        return formatted.lower()

    def generate_tofler_url(self, company_name, cin):
        """Generate Tofler URL from company name and CIN."""
        formatted_name = self.format_company_name(company_name)
        return f"https://www.tofler.in/{formatted_name}/company/{cin}"

    @backoff.on_exception(backoff.expo, 
        (Exception),
        max_tries=3,
        max_time=180,  # 3 minutes max wait
        jitter=backoff.full_jitter)  # Add jitter to avoid thundering herd
    async def scrape_company_details(self, company_name, cin):
        """Scrape company details using a browser from the pool."""
        browser = None
        context = None
        page = None
        start_time = datetime.now()
        
        try:
            browser, context = await self.browser_manager.get_browser()
            page = await context.new_page()
            
            url = self.generate_tofler_url(company_name, cin)
            logger.info(f"Scraping {company_name} (CIN: {cin}) - URL: {url}")
            
            # Add random delay between requests (5-10 seconds)
            await asyncio.sleep(random.uniform(5, 10))
            
            # Navigate to the page with retry logic and longer timeout
            try:
                response = await page.goto(url, timeout=30000, wait_until='networkidle')
                if not response:
                    raise Exception("Failed to get response from page")
                if response.status >= 400:
                    if response.status == 503:
                        # If we get a 503, wait longer before retrying
                        await asyncio.sleep(random.uniform(15, 30))
                    raise Exception(f"HTTP error {response.status}")
            except Exception as e:
                logger.error(f"Navigation error for {url}: {str(e)}")
                raise
            
            # Wait for key elements to load
            try:
                await page.wait_for_selector('section#registered-details-module', timeout=30000)
            except Exception as e:
                logger.warning(f"Timeout waiting for content on {url}: {str(e)}")
            
            # Get page content
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            company_data = self.extract_company_data(soup, company_name, cin)
            logger.info(f"Extracted data for {company_name}: {company_data}")
            
            # Validate extracted data
            if not company_data['name'] or company_data['name'] == 'Not Available':
                raise Exception("Failed to extract company name - possible invalid page or blocking")
            
            # Add to result queue
            await self.result_queue.put(company_data)
            logger.info(f"Added data to queue for {company_name}")
            
            # Update success count
            async with self.lock:
                self.success_count += 1
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Successfully scraped {company_name} in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error processing company {company_name}: {str(e)}")
            async with self.lock:
                self.failure_count += 1
            raise
        finally:
            if page:
                try:
                    await page.close()
                except Exception as e:
                    logger.error(f"Error closing page: {str(e)}")
            if browser and context:
                try:
                    await self.browser_manager.return_browser((browser, context))
                except Exception as e:
                    logger.error(f"Error returning browser: {str(e)}")
                    await self.browser_manager.cleanup_browser(browser, context)

    def extract_company_data(self, soup, original_name, cin):
        """Extract company data from the HTML content."""
        company_data = {
            'original_name': original_name,
            'cin': cin,
            'name': 'Not Available',
            'incorporation_date': 'Not Available',
            'status': 'Not Available',
            'authorized_capital': 'Not Available',
            'paid_up_capital': 'Not Available',
            'registered_address': 'Not Available',
            'email': 'Not Available',
            'pan': 'Not Available',
            'agm': 'Not Available',
            'company_type': 'Not Available',
            'directors': []
        }
        
        try:
            # Extract company name
            name_elem = soup.find('h1', class_='company-name')
            if name_elem:
                company_data['name'] = name_elem.get_text(strip=True)
            
            # Find the registered details section
            registered_section = soup.find('section', id='registered-details-module')
            if registered_section:
                # Extract details from the registered box wrapper
                registered_box = registered_section.find('div', class_='registered_box_wrapper')
                if registered_box:
                    # Extract PAN
                    pan_elem = registered_box.find('h3', string='PAN')
                    if pan_elem:
                        pan_value = pan_elem.find_next('span', class_='text-base')
                        if pan_value:
                            company_data['pan'] = pan_value.get_text(strip=True)
                    
                    # Extract Incorporation date
                    incorp_elem = registered_box.find('h3', string='Incorporation')
                    if incorp_elem:
                        incorp_value = incorp_elem.find_next('span', class_='text-base')
                        if incorp_value:
                            company_data['incorporation_date'] = incorp_value.get_text(strip=True)
                    
                    # Extract Company Email
                    email_elem = registered_box.find('h3', string='Company Email')
                    if email_elem:
                        email_value = email_elem.find_next('span', class_='text-base')
                        if email_value:
                            company_data['email'] = email_value.get_text(strip=True)
                    
                    # Extract Paid up Capital
                    paid_cap_elem = registered_box.find('h3', string='Paid up Capital')
                    if paid_cap_elem:
                        paid_cap_value = paid_cap_elem.find_next('span', class_='text-base')
                        if paid_cap_value:
                            company_data['paid_up_capital'] = paid_cap_value.get_text(strip=True)
                    
                    # Extract Authorised Capital
                    auth_cap_elem = registered_box.find('h3', string='Authorised Capital')
                    if auth_cap_elem:
                        auth_cap_value = auth_cap_elem.find_next('span', class_='text-base')
                        if auth_cap_value:
                            company_data['authorized_capital'] = auth_cap_value.get_text(strip=True)
                    
                    # Extract AGM
                    agm_elem = registered_box.find('h3', string='AGM')
                    if agm_elem:
                        agm_value = agm_elem.find_next('span', class_='text-base')
                        if agm_value:
                            company_data['agm'] = agm_value.get_text(strip=True)
                
                # Extract Company Type
                type_section = registered_section.find('div', class_='flex-col gap-8')
                if type_section:
                    type_badges = type_section.find_all('div', class_='badge')
                    if type_badges:
                        company_types = [badge.get_text(strip=True) for badge in type_badges]
                        company_data['company_type'] = ', '.join(company_types)
            
            # Extract registered address
            address_elem = soup.find('div', class_='registered-address')
            if address_elem:
                company_data['registered_address'] = address_elem.get_text(strip=True)
            
            # Extract directors
            directors_section = soup.find('div', class_='directors-section')
            if directors_section:
                director_elems = directors_section.find_all('div', class_='director-info')
                for elem in director_elems:
                    director_name = elem.find('div', class_='director-name')
                    if director_name:
                        company_data['directors'].append(director_name.get_text(strip=True))
            
            return company_data
            
        except Exception as e:
            logger.error(f"Error extracting company data: {str(e)}")
            return company_data

    async def save_results(self):
        """Save results from the queue to CSV file."""
        try:
            columns = [
                'original_name', 'cin', 'name', 'incorporation_date', 'status',
                'authorized_capital', 'paid_up_capital', 'registered_address',
                'email', 'pan', 'agm', 'company_type', 'directors'
            ]
            
            # Create empty DataFrame with correct columns if file doesn't exist
            if not os.path.exists(self.output_file):
                empty_df = pd.DataFrame(columns=columns)
                empty_df.to_csv(self.output_file, index=False)
                logger.info(f"Created new output file with headers: {self.output_file}")
            
            while True:
                try:
                    logger.info("Waiting for data in result queue...")
                    company_data = await asyncio.wait_for(self.result_queue.get(), timeout=1)
                    logger.info(f"Got data from queue: {company_data}")
                    
                    # Convert directors list to string
                    if 'directors' in company_data:
                        company_data['directors'] = '|'.join(company_data['directors'])
                    
                    # Ensure all columns exist
                    for col in columns:
                        if col not in company_data:
                            company_data[col] = 'Not Available'
                    
                    # Create DataFrame with specific column order
                    df = pd.DataFrame([company_data], columns=columns)
                    logger.info(f"Created DataFrame with columns: {df.columns.tolist()}")
                    
                    # Save to CSV
                    df.to_csv(self.output_file, mode='a', header=False, index=False)
                    logger.info(f"Saved data for company: {company_data.get('name', 'Unknown')}")
                    
                    # Update session and mark task as done
                    await self.save_session()
                    self.result_queue.task_done()
                    
                except asyncio.TimeoutError:
                    logger.info("No more data in queue, save_results finishing")
                    break
                except Exception as e:
                    logger.error(f"Error saving results: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in save_results: {str(e)}")

    async def process_companies(self):
        """Process companies in parallel using asyncio tasks."""
        try:
            tasks = []
            start_time = datetime.now()
            total_companies = len(self.companies)
            
            # Start save_results task
            save_task = asyncio.create_task(self.save_results())
            logger.info("Started save_results task")
            
            for i, (company_name, cin) in enumerate(self.companies[self.processed_count:]):
                task = asyncio.create_task(self.scrape_company_details(company_name, cin))
                tasks.append(task)
                
                if len(tasks) >= self.max_workers:
                    # Wait for some tasks to complete
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    tasks = list(pending)
                
                self.processed_count += 1
                if i % 10 == 0:  # Save session every 10 companies
                    await self.save_session()
                    
                    # Calculate and log detailed progress
                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    companies_per_hour = (self.processed_count / elapsed_time) * 3600 if elapsed_time > 0 else 0
                    success_rate = (self.success_count / (self.processed_count or 1)) * 100
                    remaining_companies = total_companies - self.processed_count
                    estimated_time_remaining = (remaining_companies / companies_per_hour) if companies_per_hour > 0 else 0
                    
                    logger.info(
                        f"\nProgress Update:\n"
                        f"Processed: {self.processed_count}/{total_companies} ({(self.processed_count/total_companies)*100:.1f}%)\n"
                        f"Success Rate: {success_rate:.1f}%\n"
                        f"Speed: {companies_per_hour:.1f} companies/hour\n"
                        f"Estimated Time Remaining: {estimated_time_remaining:.1f} hours\n"
                        f"Failures: {self.failure_count}"
                    )
                
            # Wait for remaining tasks
            if tasks:
                await asyncio.wait(tasks)
            
            # Wait for save_results to finish
            await self.result_queue.join()
            save_task.cancel()
            try:
                await save_task
            except asyncio.CancelledError:
                pass
                
        except asyncio.CancelledError:
            logger.info("Processing cancelled, cleaning up...")
            for task in tasks:
                task.cancel()
            await self.save_session()
            await self.save_results()
            raise
        except Exception as e:
            logger.error(f"Error in process_companies: {str(e)}")
            raise
        finally:
            await self.save_session()
            await self.save_results()

    async def run(self):
        """Main entry point for the scraper."""
        try:
            # Load companies from CSV
            df = pd.read_csv('4000_FTSIDB.csv')
            self.companies = list(zip(df['Name'].tolist(), df['CIN'].tolist()))
            logger.info(f"Loaded {len(self.companies)} companies")
            
            # Start processing
            await self.process_companies()
            
            # Final save
            await self.save_session()
            await self.save_results()
            
            # Calculate final statistics
            total_processed = self.success_count + self.failure_count
            success_rate = (self.success_count / total_processed) * 100 if total_processed > 0 else 0
            logger.info(f"Scraping completed. Total processed: {total_processed}, Success rate: {success_rate:.2f}%")
            
        except Exception as e:
            logger.error(f"Error in run: {str(e)}")
            raise
        finally:
            await self.browser_manager.close_all()

async def main():
    scraper = ToflerUltraScraper(max_workers=3)
    try:
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        await scraper.browser_manager.close_all()

if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main()) 