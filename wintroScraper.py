import csv
import requests
from bs4 import BeautifulSoup
import time
import re
import os

def clean_company_name(name):
    # Convert to uppercase and replace spaces with hyphens
    name = name.upper()
    name = name.replace(" ", "-")
    return name

def extract_email(text):
    # Regular expression for email
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, text)
    return match.group(0) if match else ""

def scrape_company_info(company_name):
    # Clean company name for URL
    clean_name = clean_company_name(company_name)
    url = f"http://wintro.in/company/{clean_name}"
    
    try:
        # Add delay to be respectful to the server
        time.sleep(1)
        print(f"Fetching URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the email in the table
        email = ""
        cin = ""
        
        # Find all table rows
        rows = soup.find_all('tr')
        
        for row in rows:
            # Find cells in the row
            cells = row.find_all('td')
            if len(cells) >= 2:
                header_cell = cells[0].get_text().strip()
                value_cell = cells[1].get_text().strip()
                
                # Check for Email ID
                if 'Email ID' in header_cell:
                    email = value_cell
                # Check for CIN
                elif 'CIN Number' in header_cell:
                    cin = value_cell
        
        # Clean up the values
        email = email.strip()
        cin = cin.strip()
        
        print(f"Found data - Email: {email}, CIN: {cin}")
        
        return {
            'company_name': company_name,
            'cin': cin,
            'email': email
        }
    except Exception as e:
        print(f"Error scraping {company_name}: {str(e)}")
        print(f"Attempted URL: {url}")
        return {
            'company_name': company_name,
            'cin': '',
            'email': ''
        }

def main():
    # Verify FTSIDB.csv exists
    if not os.path.exists('FTSIDB.csv'):
        print("Error: FTSIDB.csv not found!")
        return
        
    # Read company names from FTSIDB.csv
    companies = []
    try:
        with open('FTSIDB.csv', 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            companies = [row[0] for row in csv_reader if row]  # Get first column
        
        print(f"Found {len(companies)} companies in FTSIDB.csv")
        
        if not companies:
            print("No companies found in FTSIDB.csv!")
            return
            
    except Exception as e:
        print(f"Error reading FTSIDB.csv: {str(e)}")
        return
    
    # Create output CSV
    output_file = 'company_emails.csv'
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['company_name', 'cin', 'email'])
            writer.writeheader()
            
            # Process each company
            for i, company in enumerate(companies, 1):
                print(f"\nProcessing company {i} of {len(companies)}: {company}")
                info = scrape_company_info(company)
                writer.writerow(info)
                print(f"Wrote data to {output_file}: {info}")
                print("-" * 50)
                
        print(f"\nFinished! Data written to {output_file}")
        
    except Exception as e:
        print(f"Error writing to {output_file}: {str(e)}")

if __name__ == "__main__":
    main() 