from flask import Flask, render_template, jsonify, Response
import os
import re
import csv
import io
import time
import logging
import threading
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Tuple
import requests
from contextlib import contextmanager
import json

# Load environment variables
load_dotenv('config/.env')

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedDataCollector:
    def __init__(self, url: str):
        self.url = url
        self.last_content = ""
        self.session = requests.Session()
        # Configure session headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    @contextmanager
    def _get_driver(self):
        """Context manager for driver lifecycle"""
        driver = None
        try:
            firefox_options = FirefoxOptions()
            firefox_options.add_argument("--headless")
            firefox_options.add_argument("--no-sandbox")
            firefox_options.add_argument("--disable-dev-shm-usage")
            firefox_options.add_argument("--disable-gpu")
            firefox_options.add_argument("--window-size=1920,1080")
            
            # Performance optimizations
            firefox_options.set_preference("dom.webdriver.enabled", False)
            firefox_options.set_preference("useAutomationExtension", False)
            firefox_options.set_preference("browser.cache.disk.enable", True)
            firefox_options.set_preference("browser.cache.memory.enable", True)
            firefox_options.set_preference("network.http.use-cache", True)
            
            # Reduce resource usage
            firefox_options.set_preference("dom.max_script_run_time", 15)
            firefox_options.set_preference("dom.max_chrome_script_run_time", 15)
            
            # Additional preferences for better compatibility
            firefox_options.set_preference("javascript.enabled", True)
            firefox_options.set_preference("permissions.default.image", 2)  # Don't load images
            
            logger.info("Initializing Firefox driver...")
            service = FirefoxService('/usr/local/bin/geckodriver')
            driver = webdriver.Firefox(service=service, options=firefox_options)
            driver.set_page_load_timeout(45)  # Increased timeout
            driver.implicitly_wait(10)
            
            logger.info("Firefox driver initialized successfully")
            yield driver
        except Exception as e:
            logger.error(f"Driver setup error: {e}")
            raise
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("Firefox driver closed")
                except Exception as e:
                    logger.warning(f"Error quitting driver: {e}")
    
    def _extract_content_selenium(self, driver) -> Optional[str]:
        """Extract content using Selenium with better waiting strategies"""
        try:
            logger.info("Waiting for page to load...")
            
            # Wait for the body to be present first
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for the main check container
            check_container = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.check"))
            )
            
            # Get the HTML of the entire check container
            html = check_container.get_attribute('outerHTML')
            return self._extract_content_from_html(html)
            
        except Exception as e:
            logger.error(f"Error extracting content with Selenium: {e}")
            # Fallback to page source
            try:
                html = driver.page_source
                return self._extract_content_from_html(html)
            except Exception as fallback_error:
                logger.error(f"Fallback extraction also failed: {fallback_error}")
                return None
    
    def _extract_content_from_html(self, html: str) -> Optional[str]:
        """Extract content from HTML - now handles multiple items per check"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Find the main check container
            check_div = soup.select_one('div.check')
            if not check_div:
                logger.warning("No check container found in HTML")
                # Try alternative selectors
                check_div = soup.select_one('div[class*="check"]')
                if not check_div:
                    logger.warning("No check container found with alternative selectors")
                    return None
            
            # Find all item positions within the check
            chek_positions = check_div.select('div.chekPosition')
            logger.info(f"Found {len(chek_positions)} items in the check")
            
            if not chek_positions:
                logger.warning("No items found within the check container")
                return None
            
            all_items_content = []
            
            # Process each item separately
            for i, position in enumerate(chek_positions):
                item_content = self._extract_single_item_content(position, i)
                if item_content:
                    all_items_content.append(item_content)
            
            if all_items_content:
                # Join items with a separator that's easy to parse later
                combined_content = "===ITEM_SEPARATOR===".join(all_items_content)
                logger.info(f"Extracted {len(all_items_content)} items successfully")
                return combined_content
            else:
                logger.warning("No valid content extracted from any items")
                return None
            
        except Exception as e:
            logger.error(f"Content extraction error: {e}")
            return None
    
    def _extract_single_item_content(self, position, item_index: int) -> Optional[str]:
        """Extract content for a single item/position"""
        try:
            content_lines = []
            
            # Extract all text elements from this position
            paragraphs = position.find_all('p')
            
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text:
                    # Skip bold price lines (they'll be extracted separately)
                    if 'bold' not in p.get('class', []) and not re.match(r'^\d+\.\d+', text):
                        content_lines.append(text)
            
            # Add price information if available
            price_section = position.select_one('div.NDS')
            if price_section:
                price_texts = price_section.find_all(text=True, recursive=True)
                for text in price_texts:
                    text = text.strip()
                    if text:
                        content_lines.append(text)
            
            if content_lines:
                item_content = "\n".join(content_lines)
                logger.info(f"Item {item_index + 1}: extracted {len(content_lines)} lines")
                return item_content
            else:
                logger.warning(f"Item {item_index + 1}: no content found")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting item {item_index + 1}: {e}")
            return None
    
    def fetch_content(self) -> Optional[str]:
        """Fetch content with optimized approach"""
        try:
            logger.info(f"Fetching content from: {self.url}")
            
            with self._get_driver() as driver:
                logger.info(f"Navigating to URL: {self.url}")
                driver.get(self.url)
                
                # Extract content using Selenium
                content = self._extract_content_selenium(driver)

            if content:
                logger.info(f"Content extracted successfully ({len(content.split('===ITEM_SEPARATOR==='))} items)")
                if content != self.last_content:
                    self.last_content = content
                    return content
                else:
                    logger.info("Content unchanged from previous fetch")
                    return None
            else:
                logger.warning("No content could be extracted")
                return None

        except Exception as e:
            logger.error(f"Error in fetch_content: {e}")
            return None

class DataManager:
    """Manages data storage and retrieval efficiently"""
    
    def __init__(self, data_file: str = 'collected_data.json'):
        self.data_file = data_file
        self._ensure_data_file()
    
    def _ensure_data_file(self):
        """Ensure data file exists with proper structure"""
        if not os.path.exists(self.data_file):
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            logger.info(f"Created new data file: {self.data_file}")
    
    def get_last_content(self) -> Optional[str]:
        """Get last content efficiently using JSON"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data:
                last_entry = data[-1]
                logger.info(f"Last content timestamp: {last_entry.get('timestamp')}")
                return last_entry.get('raw_content', '')
            logger.info("No previous data found")
            return None
        except Exception as e:
            logger.error(f"Error reading last content: {e}")
            return None
    
    def save_data(self, url: str, content: str) -> bool:
        """Save data efficiently using JSON"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract sales data from all items
            all_sales_data = extract_sales_data(content)
            
            entry = {
                'timestamp': datetime.now().isoformat(),
                'url': url,
                'raw_content': content,
                'sales_data': all_sales_data,
                'item_count': len(all_sales_data) if isinstance(all_sales_data, list) else 1
            }
            
            data.append(entry)
            
            # Keep only last 1000 entries to prevent file bloat
            if len(data) > 1000:
                data = data[-1000:]
                logger.info("Trimmed data to 1000 most recent entries")
            
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Successfully saved data entry with {entry['item_count']} items (total entries: {len(data)})")
            return True
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            return False
    
    def load_data(self) -> List[Dict]:
        """Load all data efficiently"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} entries from data file")
            return data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return []

def extract_sales_data(content: str) -> List[Dict[str, str]]:
    """Extract sales data from content that may contain multiple items"""
    if not content:
        return []
    
    # Split content into individual items
    if '===ITEM_SEPARATOR===' in content:
        items_content = content.split('===ITEM_SEPARATOR===')
    else:
        # Fallback for old format or single items
        items_content = [content]
    
    logger.info(f"Processing {len(items_content)} items for sales data")
    
    all_sales_data = []
    
    for i, item_content in enumerate(items_content):
        sales_data = _extract_single_item_sales_data(item_content.strip(), i)
        if sales_data:
            all_sales_data.append(sales_data)
    
    logger.info(f"Successfully extracted data for {len(all_sales_data)} items")
    return all_sales_data

def _extract_single_item_sales_data(item_content: str, item_index: int) -> Dict[str, str]:
    """Extract sales data for a single item"""
    sales_data = {
        'product_name': '', 'uktzed': '', 'barcode': '', 'quantity': '1',
        'unit_price': '', 'total_price': '', 'currency': 'UAH',
        'price_details': '', 'price_breakdown': '', 'item_index': item_index
    }
    
    lines = [line.strip() for line in item_content.split('\n') if line.strip()]
    logger.info(f"Item {item_index + 1}: Processing {len(lines)} lines")
    
    identified_patterns = set()
    
    # Single pass pattern matching
    for line in lines:
        if 'УКТЗЕД' in line:
            sales_data['uktzed'] = line.replace('УКТЗЕД', '').strip()
            identified_patterns.add(line)
            logger.info(f"Item {item_index + 1}: Found UKTZED: {sales_data['uktzed']}")
        elif 'Штрих-код' in line:
            sales_data['barcode'] = line.replace('Штрих-код', '').strip()
            identified_patterns.add(line)
            logger.info(f"Item {item_index + 1}: Found barcode: {sales_data['barcode']}")
        elif '*' in line and any(char.isdigit() for char in line) and 'шт' in line:
            sales_data['price_details'] = line
            identified_patterns.add(line)
            logger.info(f"Item {item_index + 1}: Found price details: {line}")
            # Parse unit price and quantity
            parts = line.split('*')
            if len(parts) >= 2:
                sales_data['unit_price'] = parts[0].strip()
                quantity_match = re.search(r'(\d+)\s*шт', parts[1])
                if quantity_match:
                    sales_data['quantity'] = quantity_match.group(1)
        elif any(marker in line for marker in ['(А)', '(Б)', '(В)']) and any(char.isdigit() for char in line):
            sales_data['price_breakdown'] = line
            identified_patterns.add(line)
            logger.info(f"Item {item_index + 1}: Found price breakdown: {line}")
            price_match = re.search(r'([\d\.,]+)\s*\([А-Г]\)', line)
            if price_match:
                sales_data['total_price'] = price_match.group(1)
        elif re.match(r'^\d+[\.,]?\d*\s*$', line) and len(line) < 10:
            # This might be a price line
            if not sales_data['total_price']:
                sales_data['total_price'] = line.strip()
    
    # Find product name (longest line not matching patterns)
    candidate_lines = [
        line for line in lines 
        if (line not in identified_patterns and 
            not line.isdigit() and 
            not re.match(r'^\d+[\.,]\d+$', line) and 
            len(line) >= 5 and
            'УКТЗЕД' not in line and
            'Штрих-код' not in line)
    ]
    
    if candidate_lines:
        # Use the longest candidate line as product name
        sales_data['product_name'] = max(candidate_lines, key=len)
        logger.info(f"Item {item_index + 1}: Found product name: {sales_data['product_name']}")
    elif lines:
        # Fallback: first non-pattern line
        for line in lines:
            if line not in identified_patterns:
                sales_data['product_name'] = line
                logger.info(f"Item {item_index + 1}: Using fallback product name: {sales_data['product_name']}")
                break
    
    # Calculate prices if missing
    _calculate_missing_prices(sales_data)
    
    # Only return if we have meaningful data
    if sales_data['product_name'] or sales_data['uktzed'] or sales_data['barcode']:
        return sales_data
    else:
        logger.warning(f"Item {item_index + 1}: No meaningful data found")
        return None

def _calculate_missing_prices(sales_data: Dict):
    """Calculate missing price values for a single item"""
    try:
        # Unit price from total and quantity
        if (not sales_data['unit_price'] and sales_data['total_price'] 
            and sales_data['quantity'] and sales_data['quantity'] != '1'):
            total = float(sales_data['total_price'].replace(',', '.'))
            quantity = float(sales_data['quantity'])
            sales_data['unit_price'] = f"{total / quantity:.2f}"
            logger.info(f"Calculated unit price: {sales_data['unit_price']}")
        
        # Total price from unit price and quantity
        elif (sales_data['unit_price'] and not sales_data['total_price'] 
              and sales_data['quantity']):
            unit = float(sales_data['unit_price'].replace(',', '.'))
            quantity = float(sales_data['quantity'])
            sales_data['total_price'] = f"{unit * quantity:.2f}"
            logger.info(f"Calculated total price: {sales_data['total_price']}")
    except (ValueError, ZeroDivisionError) as e:
        logger.warning(f"Price calculation error: {e}")

def collect_and_save_data():
    """Optimized data collection with configurable intervals"""
    url = os.getenv('TARGET_URL', 'https://help.apteka911.com.ua/receipt/?id=44882428-7730-4B5E-926B-21F2C92FE0CC')
    if not url:
        logger.error("TARGET_URL environment variable not set!")
        return
    
    logger.info(f"Starting data collection for URL: {url}")
    collector = OptimizedDataCollector(url)
    data_manager = DataManager()
    
    # Configurable intervals
    normal_interval = int(os.getenv('CHECK_INTERVAL', '30'))  # seconds
    error_interval = int(os.getenv('ERROR_INTERVAL', '60'))   # seconds
    
    consecutive_errors = 0
    max_consecutive_errors = 3
    
    while True:
        try:
            logger.info(f"=== Collection cycle started ===")
            content = collector.fetch_content()
            
            if content and content != "No product information available":
                last_content = data_manager.get_last_content()
                
                if content != last_content:
                    if data_manager.save_data(url, content):
                        logger.info("✅ New data saved successfully")
                    else:
                        logger.error("❌ Failed to save data")
                        consecutive_errors += 1
                else:
                    logger.info("ℹ️  Content unchanged, skipping save")
            else:
                logger.warning("⚠️  No valid content to save")
                consecutive_errors += 1
            
            # Reset error counter on success
            if content:
                consecutive_errors = 0
            
            sleep_time = normal_interval
            if consecutive_errors >= max_consecutive_errors:
                sleep_time = error_interval * 2
                logger.error(f"🔴 Too many errors, increasing interval to {sleep_time}s")
            elif consecutive_errors > 0:
                sleep_time = error_interval
            
            logger.info(f"💤 Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"🔴 Error in data collection loop: {e}")
            logger.info(f"💤 Sleeping for {error_interval} seconds after error...")
            time.sleep(error_interval)


# Initialize data manager instance
data_manager = DataManager()

@app.route('/')
def index():
    """Main page showing all sales data"""
    entries = data_manager.load_data()
    entries.reverse()  # Show latest first
    
    # Flatten the data for display - each item becomes a separate row
    flattened_entries = []
    for entry in entries:
        sales_data_list = entry.get('sales_data', [])
        if isinstance(sales_data_list, list):
            for item_data in sales_data_list:
                flattened_entries.append({
                    'timestamp': entry.get('timestamp'),
                    'url': entry.get('url'),
                    'sales_data': item_data
                })
        else:
            # Backward compatibility with single-item entries
            flattened_entries.append(entry)
    
    totals = calculate_totals(flattened_entries)
    
    return render_template('index.html', 
                         entries=flattened_entries, 
                         total_entries=len(flattened_entries),
                         totals=totals,
                         now=datetime.now())


@app.route('/export/csv')
def export_csv():
    """Export data as CSV"""
    entries = data_manager.load_data()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        'Timestamp', 'Product', 'UKTZED', 'Barcode', 
        'Quantity', 'Unit Price (UAH)', 'Total Price (UAH)', 'Price Details', 'URL'
    ])
    
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

def parse_data_file():
    """Parse the collected_data.txt file and return structured sales data"""
    data_file = 'collected_data.txt'
    
    if not os.path.exists(data_file):
        return []
    
    entries = []
    current_entry = {}
    in_content_section = False
    
    with open(data_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.strip()
        
        if line.startswith('=== Data collected at '):
            if current_entry:
                # Process the previous entry
                if current_entry.get('content'):
                    current_entry['raw_content'] = '\n'.join(current_entry['content'])
                    current_entry['sales_data'] = extract_sales_data(current_entry['raw_content'])
                entries.append(current_entry)
            
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
            # End of current entry (handled at next entry start)
            in_content_section = False
            
        elif line and in_content_section:
            # Content line
            current_entry['content'].append(line)
    
    # Add the last entry if it exists and has content
    if current_entry and current_entry.get('content'):
        current_entry['raw_content'] = '\n'.join(current_entry['content'])
        current_entry['sales_data'] = extract_sales_data(current_entry['raw_content'])
        entries.append(current_entry)
    
    # Filter out any empty entries
    entries = [entry for entry in entries if entry.get('content')]
    
    return entries[::-1]  # Reverse to show latest first

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
    entries = data_manager.load_data()
    return jsonify(entries)

@app.route('/api/totals')
def api_totals():
    """API endpoint for totals"""
    entries = data_manager.load_data()
    totals = calculate_totals(entries)
    return jsonify(totals)

@app.route('/status')
def status():
    """Status endpoint to check if collection is working"""
    entries = data_manager.load_data()
    last_entry = entries[-1] if entries else None
    
    status_info = {
        'status': 'running',
        'total_entries': len(entries),
        'last_collection': last_entry.get('timestamp') if last_entry else 'Never',
        'last_product': last_entry.get('sales_data', {}).get('product_name') if last_entry else 'None'
    }
    
    return jsonify(status_info)

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
        try:
            totals['total_items'] += int(sales_data.get('quantity', 1))
        except (ValueError, TypeError):
            pass
        
        # Sum sales
        try:
            price = float(sales_data.get('total_price', '0').replace(',', '.'))
            totals['total_sales'] += price
        except (ValueError, TypeError):
            pass
        
        # Count unique products
        if sales_data.get('product_name'):
            totals['unique_products'].add(sales_data['product_name'])
        
        # Sales by hour
        if entry.get('timestamp'):
            try:
                dt = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                hour = dt.strftime('%H:00')
                totals['sales_by_hour'][hour] = totals['sales_by_hour'].get(hour, 0) + 1
            except Exception:
                pass
    
    totals['unique_products_count'] = len(totals['unique_products'])
    totals['total_sales'] = round(totals['total_sales'], 2)
    return totals

if __name__ == '__main__':
    logger.info("Starting Pharmacy Data Collection System")
    
    # Check if required components are available
    try:
        # Start data collection in background thread
        data_collector_thread = threading.Thread(target=collect_and_save_data, daemon=True)
        data_collector_thread.start()
        logger.info("Data collection thread started")
        
        # Start Flask app
        logger.info("Starting Flask application...")
        app.run(debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true', 
                host='0.0.0.0', port=5000, use_reloader=False)
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        