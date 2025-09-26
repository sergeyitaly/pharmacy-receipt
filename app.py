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
from typing import Optional, Dict, List
import requests
from contextlib import contextmanager
import json

# Load environment variables
load_dotenv('config/.env')

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptimizedDataCollector:
    def __init__(self, url: str):
        self.url = url
        self.last_content = ""
        self.driver = None
        self.session = requests.Session()
        # Configure session headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0'
        })
    
    def __del__(self):
        """Ensure driver is properly closed"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
    
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
            firefox_options.set_preference("browser.cache.offline.enable", True)
            firefox_options.set_preference("network.http.use-cache", True)
            
            # Reduce resource usage
            firefox_options.set_preference("dom.max_script_run_time", 10)
            firefox_options.set_preference("dom.max_chrome_script_run_time", 10)
            
            # Use existing service or create new one
            service = FirefoxService('/usr/local/bin/geckodriver')
            driver = webdriver.Firefox(service=service, options=firefox_options)
            driver.set_page_load_timeout(30)  # 30 second timeout
            driver.implicitly_wait(5)  # Reduced implicit wait
            
            yield driver
        except Exception as e:
            logger.error(f"Driver setup error: {e}")
            raise
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    logger.warning(f"Error quitting driver: {e}")
    
    def _try_direct_request(self) -> Optional[str]:
        """Try to fetch content using requests first (lighter than Selenium)"""
        try:
            response = self.session.get(self.url, timeout=10)
            if response.status_code == 200:
                # Check if the content contains our target elements
                if 'chekPosition' in response.text:
                    return response.text
            return None
        except Exception as e:
            logger.debug(f"Direct request failed: {e}")
            return None
    
    def _extract_content(self, html: str) -> Optional[str]:
        """Extract content from HTML"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # More efficient element finding
            chek_div = (soup.find("div", class_="check") and 
                       soup.find("div", class_="check").find("div", class_="chekPosition")) or \
                      soup.find("div", class_="chekPosition")
            
            if not chek_div:
                logger.warning("No chekPosition div found")
                return None
            
            content_lines = []
            
            # Use CSS selectors for better performance
            main_paragraphs = chek_div.select('p:not(.bold)')
            content_lines.extend(p.get_text(strip=True) for p in main_paragraphs if p.get_text(strip=True))
            
            # Extract price information
            nds_div = chek_div.find("div", class_="NDS")
            if nds_div:
                nds_paragraphs = nds_div.find_all("p")
                content_lines.extend(p.get_text(strip=True) for p in nds_paragraphs if p.get_text(strip=True))
            
            content = "\n".join(content_lines)
            return content if content else None
            
        except Exception as e:
            logger.error(f"Content extraction error: {e}")
            return None
    
    def fetch_content(self) -> Optional[str]:
        """Fetch content with optimized approach"""
        try:
            logger.info(f"Fetching content from: {self.url}")
            
            # Try direct HTTP request first (much lighter)
            html = self._try_direct_request()
            
            # Fall back to Selenium if direct request fails or doesn't contain needed content
            if not html or 'chekPosition' not in html:
                logger.info("Falling back to Selenium")
                with self._get_driver() as driver:
                    driver.get(self.url)
                    
                    # Use explicit waits instead of sleep
                    wait = WebDriverWait(driver, 10)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    
                    # Wait specifically for our content if possible
                    try:
                        wait.until(EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "div.chekPosition, div.check")
                        ))
                    except:
                        logger.warning("Target elements not found within timeout")
                    
                    html = driver.page_source
            
            content = self._extract_content(html)
            
            if content and content != self.last_content:
                self.last_content = content
                logger.info("New product info extracted")
                return content
            else:
                logger.info("No new content")
                return None

        except Exception as e:
            logger.error(f"Error fetching content: {e}")
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
    
    def get_last_content(self) -> Optional[str]:
        """Get last content efficiently using JSON"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data:
                return data[-1].get('raw_content', '')
            return None
        except Exception as e:
            logger.error(f"Error reading last content: {e}")
            return None
    
    def save_data(self, url: str, content: str) -> bool:
        """Save data efficiently using JSON"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            entry = {
                'timestamp': datetime.now().isoformat(),
                'url': url,
                'raw_content': content,
                'sales_data': extract_sales_data(content)
            }
            
            data.append(entry)
            
            # Keep only last 1000 entries to prevent file bloat
            if len(data) > 1000:
                data = data[-1000:]
            
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            return False
    
    def load_data(self) -> List[Dict]:
        """Load all data efficiently"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return []

def extract_sales_data(content: str) -> Dict[str, str]:
    """Optimized sales data extraction"""
    if not content:
        return {}
    
    sales_data = {
        'product_name': '', 'uktzed': '', 'barcode': '', 'quantity': '1',
        'unit_price': '', 'total_price': '', 'currency': 'UAH',
        'price_details': '', 'price_breakdown': ''
    }
    
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    identified_patterns = set()
    
    # Single pass pattern matching
    for line in lines:
        if 'УКТЗЕД' in line:
            sales_data['uktzed'] = line.replace('УКТЗЕД', '').strip()
            identified_patterns.add(line)
        elif 'Штрих-код' in line:
            sales_data['barcode'] = line.replace('Штрих-код', '').strip()
            identified_patterns.add(line)
        elif '*' in line and any(char.isdigit() for char in line) and 'шт' in line:
            sales_data['price_details'] = line
            identified_patterns.add(line)
            # Parse unit price and quantity
            parts = line.split('*')
            if len(parts) >= 2:
                sales_data['unit_price'] = parts[0].strip()
                quantity_match = re.search(r'(\d+)\s*шт', parts[1])
                if quantity_match:
                    sales_data['quantity'] = quantity_match.group(1)
        elif '(Б)' in line and any(char.isdigit() for char in line):
            sales_data['price_breakdown'] = line
            identified_patterns.add(line)
            price_match = re.search(r'^([\d\.,]+)', line)
            if price_match:
                sales_data['total_price'] = price_match.group(1)
    
    # Find product name (longest line not matching patterns)
    candidate_lines = [
        line for line in lines 
        if (line not in identified_patterns and 
            not line.isdigit() and 
            not re.match(r'^\d+[\.,]\d+$', line) and 
            len(line) >= 5)
    ]
    
    if candidate_lines:
        sales_data['product_name'] = max(candidate_lines, key=len)
    elif lines:
        # Fallback: first non-pattern line
        for line in lines:
            if line not in identified_patterns:
                sales_data['product_name'] = line
                break
    
    # Calculate prices if missing
    _calculate_missing_prices(sales_data)
    
    return sales_data

def _calculate_missing_prices(sales_data: Dict):
    """Calculate missing price values"""
    try:
        # Unit price from total and quantity
        if (not sales_data['unit_price'] and sales_data['total_price'] 
            and sales_data['quantity'] and sales_data['quantity'] != '1'):
            total = float(sales_data['total_price'].replace(',', '.'))
            quantity = float(sales_data['quantity'])
            sales_data['unit_price'] = f"{total / quantity:.2f}"
        
        # Total price from unit price and quantity
        elif (sales_data['unit_price'] and not sales_data['total_price'] 
              and sales_data['quantity']):
            unit = float(sales_data['unit_price'].replace(',', '.'))
            quantity = float(sales_data['quantity'])
            sales_data['total_price'] = f"{unit * quantity:.2f}"
    except (ValueError, ZeroDivisionError):
        pass

def collect_and_save_data():
    """Optimized data collection with configurable intervals"""
    url = os.getenv('TARGET_URL', 'https://example.com')
    collector = OptimizedDataCollector(url)
    data_manager = DataManager()
    
    # Configurable intervals
    normal_interval = int(os.getenv('CHECK_INTERVAL', '10'))  # seconds
    error_interval = int(os.getenv('ERROR_INTERVAL', '60'))   # seconds
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    while True:
        try:
            content = collector.fetch_content()
            
            if content and content != "No product information available":
                last_content = data_manager.get_last_content()
                
                if content != last_content:
                    if data_manager.save_data(url, content):
                        logger.info("New data saved successfully")
                    else:
                        logger.error("Failed to save data")
                else:
                    logger.info("Content unchanged, skipping save")
            else:
                logger.info("No valid content to save")
            
            consecutive_errors = 0
            time.sleep(normal_interval)
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Error in data collection loop: {e}")
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error("Too many consecutive errors, increasing interval")
                time.sleep(error_interval * 2)  # Double the error interval
            else:
                time.sleep(error_interval)

# Flask routes remain largely the same but use DataManager
data_manager = DataManager()

@app.route('/')
def index():
    """Main page showing all sales data"""
    entries = data_manager.load_data()
    entries.reverse()  # Show latest first
    
    totals = calculate_totals(entries)
    
    return render_template('index.html', 
                         entries=entries, 
                         total_entries=len(entries),
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

# Other routes (export/excel, api/data, api/totals) remain similar
# but should use data_manager.load_data() instead of parse_data_file()

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
    # Start data collection in background thread
    data_collector_thread = threading.Thread(target=collect_and_save_data, daemon=True)
    data_collector_thread.start()
    
    # Start Flask app
    app.run(debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true', 
            host='0.0.0.0', port=5000)