from flask import Flask, render_template, jsonify, Response
import os
import re
import csv
import io
import time
import logging
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
from typing import Optional
import chromedriver_autoinstaller
from selenium.webdriver.chrome.service import Service

# Load environment variables
load_dotenv('config/.env')

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self, url: str):
        self.url = url
        self.last_content = ""

    def fetch_content(self) -> Optional[str]:
        """Fetch content using Selenium and extract product info and prices"""
        try:
            logger.info(f"Fetching content from: {self.url}")
            
            firefox_options = FirefoxOptions()
            firefox_options.add_argument("--headless")
            firefox_options.binary_location = "/usr/bin/firefox"  # Explicit path
            
            # Use the installed geckodriver
            service = FirefoxService('/usr/local/bin/geckodriver')
            driver = webdriver.Firefox(service=service, options=firefox_options)
            
            driver.get(self.url)
            time.sleep(3)  # Wait for content to load
            html = driver.page_source
            driver.quit()
            
            soup = BeautifulSoup(html, "html.parser")
            
            # First try to find the check div, then chekPosition inside it
            check_div = soup.find("div", class_="check")
            if check_div:
                chek_div = check_div.find("div", class_="chekPosition")
            else:
                # Fallback to direct chekPosition search
                chek_div = soup.find("div", class_="chekPosition")
            
            if not chek_div:
                logger.warning("No chekPosition div found")
                return "No product information available"
            
            # Extract all text content including prices from NDS div
            content_lines = []
            
            # First, get all the main <p> tags (UKTZED, barcode, product name)
            for p in chek_div.find_all("p", class_=lambda x: x != "bold"):
                content_lines.append(p.get_text(strip=True))
            
            # Now extract price information from NDS div
            nds_div = chek_div.find("div", class_="NDS")
            if nds_div:
                # Get all paragraphs within NDS div (including the price info)
                for p in nds_div.find_all("p"):
                    content_lines.append(p.get_text(strip=True))
            
            content = "\n".join(content_lines)
            
            if content != self.last_content:
                self.last_content = content
                logger.info(f"New product info extracted: {content}")
                return content
            else:
                logger.info("No new content")
                return None

        except Exception as e:
            logger.error(f"Error fetching content: {e}")
            return None

def get_last_content_from_file():
    """Get the last content from the data file to avoid duplicates"""
    data_file = 'collected_data.txt'
    
    if not os.path.exists(data_file):
        return None
    
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Read the file backwards to find the last content
        content_lines = []
        reading_content = False
        
        for line in reversed(lines):
            line = line.strip()
            
            if line.startswith('=' * 50):
                if reading_content:
                    break  # Found the start of previous entry
                reading_content = True
                continue
            
            if reading_content and line and not line.startswith(('===', 'URL:', 'Content:')):
                content_lines.insert(0, line)  # Add to beginning to maintain order
        
        return '\n'.join(content_lines) if content_lines else None
        
    except Exception as e:
        logger.error(f"Error reading last content from file: {e}")
        return None

def collect_and_save_data():
    """Periodically collect data and save to file, avoiding duplicates"""
    url = os.getenv('TARGET_URL', 'https://example.com')
    collector = DataCollector(url)
    
    # Get the last saved content to avoid duplicates
    last_saved_content = get_last_content_from_file()
    
    while True:
        try:
            content = collector.fetch_content()
            
            if (content and 
                content != "No product information available" and 
                content != last_saved_content):
                
                # Save to file
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                with open('collected_data.txt', 'a', encoding='utf-8') as f:
                    f.write(f"\n{'='*50}\n")
                    f.write(f"=== Data collected at {timestamp} ===\n")
                    f.write(f"URL: {url}\n")
                    f.write("Content:\n")
                    f.write(f"{content}\n")
                    f.write(f"{'='*50}\n")
                
                last_saved_content = content
                logger.info(f"New data saved at {timestamp}")
            else:
                if content == last_saved_content:
                    logger.info("Content unchanged, skipping save")
                else:
                    logger.info("No valid content to save")
            
            # Wait before next check
            time.sleep(60)  # Check every 10 seconds
            
        except Exception as e:
            logger.error(f"Error in data collection loop: {e}")
            time.sleep(60)  # Wait longer on error

def parse_data_file():
    """Parse the collected_data.txt file and return structured sales data"""
    data_file = 'collected_data.txt'
    
    if not os.path.exists(data_file):
        return []
    
    entries = []
    current_entry = {}
    in_content_section = False
    entry_count = 0
    
    with open(data_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.strip()
        
        if line.startswith('=== Data collected at '):
            if current_entry and current_entry.get('content'):
                # Process the previous entry
                current_entry['raw_content'] = '\n'.join(current_entry['content'])
                current_entry['sales_data'] = extract_sales_data(current_entry['raw_content'])
                
                # Only add if we have valid sales data
                if current_entry['sales_data'].get('product_name'):
                    entries.append(current_entry)
                    entry_count += 1
            
            # Start new entry
            current_entry = {
                'timestamp': line.replace('=== Data collected at ', '').replace(' ===', ''),
                'url': '',
                'content': [],
                'raw_content': '',
                'sales_data': {}
            }
            in_content_section = False
            
        elif line.startswith('URL: '):
            current_entry['url'] = line.replace('URL: ', '')
            
        elif line == 'Content:':
            in_content_section = True
            
        elif line.startswith('=' * 50):
            # End of current entry
            in_content_section = False
            
        elif line and in_content_section:
            # Content line
            current_entry['content'].append(line)
    
    # Add the last entry if it exists and has content
    if current_entry and current_entry.get('content'):
        current_entry['raw_content'] = '\n'.join(current_entry['content'])
        current_entry['sales_data'] = extract_sales_data(current_entry['raw_content'])
        if current_entry['sales_data'].get('product_name'):
            entries.append(current_entry)
    
    # Remove duplicates based on timestamp and product name
    unique_entries = remove_duplicate_entries(entries)
    
    return unique_entries[::-1]  # Reverse to show latest first

def remove_duplicate_entries(entries):
    """Remove duplicate entries based on timestamp and product name"""
    seen = set()
    unique_entries = []
    
    for entry in entries:
        sales_data = entry.get('sales_data', {})
        key = (entry.get('timestamp', ''), sales_data.get('product_name', ''))
        
        if key not in seen and sales_data.get('product_name'):
            seen.add(key)
            unique_entries.append(entry)
    
    return unique_entries

def extract_sales_data(content):
    """Extract structured sales information from content including prices"""
    if not content:
        return {}
    
    sales_data = {
        'product_name': '',
        'uktzed': '',
        'barcode': '',
        'quantity': '1',
        'unit_price': '',
        'total_price': '',
        'currency': '₴',
        'price_details': '',
        'price_breakdown': ''
    }
    
    lines = content.split('\n')
    identified_patterns = set()
    
    # First pass: identify all known patterns
    for line in lines:
        line = line.strip()
        
        if 'УКТЗЕД' in line:
            sales_data['uktzed'] = line.replace('УКТЗЕД', '').strip()
            identified_patterns.add(line)
        
        elif 'Штрих-код' in line:
            sales_data['barcode'] = line.replace('Штрих-код', '').strip()
            identified_patterns.add(line)
        
        # Parse unit price and quantity pattern: "163.9 * 1 шт"
        elif '*' in line and any(char.isdigit() for char in line) and 'шт' in line:
            sales_data['price_details'] = line
            identified_patterns.add(line)
            
            # Parse unit price and quantity
            parts = line.split('*')
            if len(parts) >= 2:
                # Extract unit price (first part)
                unit_price_text = parts[0].strip()
                sales_data['unit_price'] = unit_price_text
                
                # Extract quantity (second part)
                quantity_part = parts[1].strip()
                quantity_match = re.search(r'(\d+)\s*шт', quantity_part)
                if quantity_match:
                    sales_data['quantity'] = quantity_match.group(1)
        
        # Parse total price pattern: "163.90 (Б)"
        elif '(Б)' in line and any(char.isdigit() for char in line):
            sales_data['price_breakdown'] = line
            identified_patterns.add(line)
            
            # Extract just the numeric price part
            price_match = re.search(r'^([\d\.,]+)', line)
            if price_match:
                sales_data['total_price'] = price_match.group(1)
    
    # Second pass: find the product name (the line that doesn't match any pattern)
    candidate_lines = []
    for line in lines:
        line = line.strip()
        if line and line not in identified_patterns:
            # Additional checks to avoid false positives
            if not (line.isdigit() or  # Not just numbers
                    re.match(r'^\d+[\.,]\d+$', line) or  # Not just a price
                    len(line) < 5):  # Not too short
                candidate_lines.append(line)
    
    if candidate_lines:
        # Choose the most likely product name (longest line that looks like a product)
        sales_data['product_name'] = max(candidate_lines, key=len)
    
    # Final fallback: if no product name found, use the first non-pattern line
    if not sales_data['product_name']:
        for line in lines:
            line = line.strip()
            if line and line not in identified_patterns:
                sales_data['product_name'] = line
                break
    
    # Clean up prices - ensure they have proper formatting
    if sales_data.get('unit_price'):
        sales_data['unit_price'] = format_price(sales_data['unit_price'])
    
    if sales_data.get('total_price'):
        sales_data['total_price'] = format_price(sales_data['total_price'])
    
    # Calculate unit price if we have total price and quantity but no unit price
    if (not sales_data['unit_price'] and 
        sales_data['total_price'] and 
        sales_data['quantity'] and 
        sales_data['quantity'] != '1'):
        try:
            total = float(sales_data['total_price'].replace(',', '.'))
            quantity = float(sales_data['quantity'])
            unit_price = total / quantity
            sales_data['unit_price'] = f"{unit_price:.2f}"
        except (ValueError, ZeroDivisionError):
            pass
    
    # If we have unit price but no total price, calculate it
    if (sales_data['unit_price'] and 
        not sales_data['total_price'] and 
        sales_data['quantity']):
        try:
            unit = float(sales_data['unit_price'].replace(',', '.'))
            quantity = float(sales_data['quantity'])
            total_price = unit * quantity
            sales_data['total_price'] = f"{total_price:.2f}"
        except (ValueError, ZeroDivisionError):
            pass
    
    return sales_data

def format_price(price_str):
    """Format price string to have 2 decimal places"""
    try:
        # Remove any non-numeric characters except decimal point/comma
        clean_price = re.sub(r'[^\d,.]', '', price_str)
        # Replace comma with dot for conversion
        clean_price = clean_price.replace(',', '.')
        # Convert to float and format with 2 decimal places
        price_float = float(clean_price)
        return f"{price_float:.2f}"
    except (ValueError, TypeError):
        return price_str

def calculate_totals(entries):
    """Calculate total sales statistics"""
    totals = {
        'total_sales': 0.0,
        'total_items': 0,
        'unique_products': set(),
        'sales_by_hour': {}
    }
    
    for entry in entries:
        sales_data = entry.get('sales_data', {})
        
        # Count items
        if sales_data.get('quantity'):
            try:
                totals['total_items'] += int(sales_data.get('quantity', 1))
            except (ValueError, TypeError):
                totals['total_items'] += 1
        
        # Sum sales
        if sales_data.get('total_price'):
            try:
                # Clean the price string before conversion
                price_str = sales_data['total_price'].replace('₴', '').strip()
                price = float(price_str.replace(',', '.'))
                totals['total_sales'] += price
            except (ValueError, TypeError, AttributeError):
                pass
        
        # Count unique products
        if sales_data.get('product_name'):
            totals['unique_products'].add(sales_data['product_name'])
        
        # Sales by hour
        if entry.get('timestamp'):
            try:
                dt = datetime.strptime(entry['timestamp'], '%Y-%m-%d %H:%M:%S')
                hour = dt.strftime('%H:00')
                totals['sales_by_hour'][hour] = totals['sales_by_hour'].get(hour, 0) + 1
            except:
                pass
    
    totals['unique_products_count'] = len(totals['unique_products'])
    totals['total_sales'] = round(totals['total_sales'], 2)
    return totals

@app.route('/')
def index():
    """Main page showing all sales data"""
    entries = parse_data_file()
    totals = calculate_totals(entries)
    
    return render_template('index.html', 
                         entries=entries, 
                         total_entries=len(entries),
                         totals=totals,
                         now=datetime.now())

@app.route('/export/csv')
def export_csv():
    """Export data as CSV with proper UTF-8 encoding"""
    entries = parse_data_file()
    
    # Create CSV in memory with UTF-8 encoding
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow([
        'Timestamp', 'Product', 'UKTZED', 'Barcode', 
        'Quantity', 'Unit Price (UAH)', 'Total Price (UAH)', 'Price Details', 'URL'
    ])
    
    # Write data
    for entry in entries:
        sales_data = entry.get('sales_data', {})
        writer.writerow([
            entry.get('timestamp', ''),
            sales_data.get('product_name', ''),
            sales_data.get('uktzed', ''),
            sales_data.get('barcode', ''),
            sales_data.get('quantity', ''),
            sales_data.get('unit_price', ''),
            sales_data.get('total_price', ''),
            sales_data.get('price_details', ''),
            entry.get('url', '')
        ])
    
    # Create response with proper UTF-8 encoding
    response = Response(
        output.getvalue(),
        mimetype='text/csv; charset=utf-8',
        headers={
            'Content-Disposition': f'attachment; filename=pharmacy-sales-{datetime.now().strftime("%Y-%m-%d")}.csv'
        }
    )
    
    return response

@app.route('/export/excel')
def export_excel():
    """Export data as Excel file (UTF-8 encoded) with proper price formatting"""
    entries = parse_data_file()
    
    # Create CSV with UTF-8 BOM for Excel compatibility
    output = io.StringIO()
    output.write('\ufeff')  # UTF-8 BOM for Excel
    
    writer = csv.writer(output)
    
    # Write header
    writer.writerow([
        'Timestamp', 'Product', 'UKTZED', 'Barcode', 
        'Quantity', 'Unit Price (UAH)', 'Total Price (UAH)', 'Price Details', 'URL'
    ])
    
    # Write data
    for entry in entries:
        sales_data = entry.get('sales_data', {})
        writer.writerow([
            entry.get('timestamp', ''),
            sales_data.get('product_name', ''),
            sales_data.get('uktzed', ''),
            sales_data.get('barcode', ''),
            sales_data.get('quantity', ''),
            sales_data.get('unit_price', ''),
            sales_data.get('total_price', ''),
            sales_data.get('price_details', ''),
            entry.get('url', '')
        ])
    
    response = Response(
        output.getvalue(),
        mimetype='text/csv; charset=utf-8',
        headers={
            'Content-Disposition': f'attachment; filename=pharmacy-sales-{datetime.now().strftime("%Y-%m-%d")}.csv'
        }
    )
    
    return response

@app.route('/api/data')
def api_data():
    """API endpoint for JSON data"""
    entries = parse_data_file()
    return jsonify(entries)

@app.route('/api/totals')
def api_totals():
    """API endpoint for totals"""
    entries = parse_data_file()
    totals = calculate_totals(entries)
    return jsonify(totals)

if __name__ == '__main__':
    # Start data collection in background thread
    data_collector_thread = threading.Thread(target=collect_and_save_data, daemon=True)
    data_collector_thread.start()
    
    # Start Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)