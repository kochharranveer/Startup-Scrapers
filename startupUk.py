import requests
from bs4 import BeautifulSoup
import csv
import time

# Base URL for the startup list
BASE_URL = "https://startuputtarakhand.uk.gov.in/recognised_startups"

# List to store all startup data
data = []

# Function to scrape a single page
def scrape_page(page_num):
    url = f"{BASE_URL}?page={page_num}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the startup list table
        startup_list = soup.find('tbody', id='startuplist')
        if not startup_list:
            return False
            
        # Extract startup data
        for row in startup_list.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) >= 3:  # Ensure we have at least name and email
                startup_name = cells[1].text.strip()
                email = cells[2].text.strip()
                data.append({
                    'Startup Name': startup_name,
                    'Email': email
                })
        return True
    except Exception as e:
        print(f"Error scraping page {page_num}: {str(e)}")
        return False

# Main scraping loop
print("Starting to scrape startup data...")
for page in range(1, 23):  # Pages 1 to 22
    print(f"Scraping page {page}...")
    if not scrape_page(page):
        print(f"Failed to scrape page {page}, stopping...")
        break
    time.sleep(1)  # Add delay to be polite to the server

# Write data to CSV
if data:
    with open('startup_uttarakhand.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Startup Name', 'Email']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Successfully saved {len(data)} startups to startup_uttarakhand.csv")
else:
    print("No data was collected. Check the logs for errors.")
