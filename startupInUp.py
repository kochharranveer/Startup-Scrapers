import requests
from bs4 import BeautifulSoup
import csv
import time

def scrape_companies():
    base_url = "https://www.startinup.up.gov.in/crm/welcome/connect_network/"
    output_file = "startinup_companies.csv"
    
    # Create CSV file with headers
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Company Name', 'Location', 'Industry', 'URL'])
    
    # Loop through pages 1 to 70
    for page in range(70,90 ):
        print(f"Scraping page {page}...")
        url = f"{base_url}{page}"
        
        try:
            # Add headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise exception for bad status codes
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all startup cards
            startup_section = soup.find('div', id='statups_data')
            if startup_section:
                cards = startup_section.find_all('div', class_='col-md-4')
                
                # Append new data to CSV
                with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    
                    for card in cards:
                        try:
                            # Extract company details
                            company_name = card.find('h3').text.strip()
                            location = card.find('p').text.strip()
                            industry = card.find('div', class_='discription').find('p').text.strip()
                            company_url = card.find('a')['href']
                            
                            # Write to CSV
                            writer.writerow([company_name, location, industry, company_url])
                        except Exception as e:
                            print(f"Error processing card: {e}")
                            continue
            
            # Add a small delay to be polite to the server
            time.sleep(1)
            
        except requests.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            continue
    
    print(f"\nScraping complete! Data saved to {output_file}")

if __name__ == "__main__":
    scrape_companies()
