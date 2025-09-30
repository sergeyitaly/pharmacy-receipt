from flask import Flask, render_template, jsonify, Response, request
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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Tuple
import requests
from contextlib import contextmanager
import json
from collections import defaultdict, Counter
import openai
import os

from datetime import datetime, timedelta

class SimpleCache:
    def __init__(self):
        self._cache = {}
    
    def set(self, key, value, timeout=3600):
        self._cache[key] = {
            'value': value,
            'expires': datetime.now() + timedelta(seconds=timeout)
        }
    
    def get(self, key):
        if key in self._cache:
            entry = self._cache[key]
            if datetime.now() < entry['expires']:
                return entry['value']
            else:
                del self._cache[key]
        return None

# Initialize cache
cache = SimpleCache()

# Load environment variables
load_dotenv('config/.env')

app = Flask(__name__)



# Configure OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

@app.route('/ai-analysis', methods=['POST'])
def ai_analysis():
    """Perform AI analysis on top selling products"""
    try:
        data = request.get_json()
        products = data.get('products', [])
        products_hash = data.get('products_hash')
        
        if not products:
            return jsonify({'error': 'No products data provided'}), 400
        
        if not openai.api_key:
            return jsonify({'error': 'OpenAI API key not configured'}), 500
        
        # Check if we have a cached result for this hash
        if products_hash:
            cache_key = f"ai_analysis_{products_hash}"
            cached_result = cache.get(cache_key)
            if cached_result:
                logger.info("Returning cached AI analysis result")
                return jsonify({
                    'analysis': cached_result,
                    'cached': True,
                    'products_analyzed': len(products)
                })
        
        # Prepare prompt for analysis
        prompt = f"""
        Analyze these top 10 pharmacy products and provide insights about potential diseases they might be treating.
        
        Products data:
        {json.dumps(products, indent=2, ensure_ascii=False)}
        
        Please provide a concise analysis (4-5 sentences) focusing on:
        1. 3 most likely diseases/conditions these products treat
        2. Percentage coverage of each disease category
        3. Potential health awareness insights
        
        Format the response in a clear, professional manner suitable for pharmacy business analysis.
        """
        
        # Call OpenAI API
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a pharmaceutical business analyst. Provide concise, professional analysis of pharmacy sales data."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=300,
            temperature=0.7
        )
        
        analysis = response.choices[0].message.content.strip()
        
        # Cache the result for 1 hour
        if products_hash:
            cache_key = f"ai_analysis_{products_hash}"
            cache.set(cache_key, analysis, timeout=3600)  # 1 hour cache
        
        return jsonify({
            'analysis': analysis,
            'cached': False,
            'products_analyzed': len(products)
        })
        
    except Exception as e:
        logger.error(f"AI Analysis error: {e}")
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500
        
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedDataCollector:
    def __init__(self, url: str):
        self.url = url
        self.last_content = ""
        self.driver = None  # Single driver instance
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
    
    def _get_driver(self):
        """Get or create driver instance - REUSE instead of recreate"""
        if self.driver is None:
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
                
                # Reduce resource usage - MORE AGGRESSIVE
                firefox_options.set_preference("dom.max_script_run_time", 10)
                firefox_options.set_preference("dom.max_chrome_script_run_time", 10)
                firefox_options.set_preference("javascript.enabled", True)
                firefox_options.set_preference("permissions.default.image", 2)  # Don't load images
                
                # Additional performance tweaks
                firefox_options.set_preference("browser.tabs.remote.autostart", False)
                firefox_options.set_preference("browser.tabs.remote.autostart.2", False)
                firefox_options.set_preference("browser.sessionstore.resume_from_crash", False)
                firefox_options.set_preference("browser.sessionstore.max_resumed_crashes", 0)
                
                logger.info("Initializing Firefox driver...")
                service = FirefoxService('/usr/local/bin/geckodriver')
                self.driver = webdriver.Firefox(service=service, options=firefox_options)
                self.driver.set_page_load_timeout(30)  # Reduced from 45
                self.driver.implicitly_wait(5)  # Reduced from 10
                
                logger.info("Firefox driver initialized successfully")
            except Exception as e:
                logger.error(f"Driver setup error: {e}")
                raise
        
        return self.driver
    
    def _cleanup_driver(self):
        """Clean up driver if it exists"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                logger.info("Firefox driver closed")
            except Exception as e:
                logger.warning(f"Error quitting driver: {e}")
    
    def _extract_content_selenium(self, driver) -> Optional[str]:
        """Extract content using Selenium with better waiting strategies"""
        try:
            logger.info("Waiting for page to load...")
            
            # Wait for the body to be present first
            WebDriverWait(driver, 20).until(  # Reduced from 30
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for the main check container
            check_container = WebDriverWait(driver, 15).until(  # Reduced from 20
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.check"))
            )
            
            # Get the HTML of the entire check container
            html = check_container.get_attribute('outerHTML')
            return self._extract_content_from_html(html)
            
        except Exception as e:
            logger.error(f"Error extracting content with Selenium: {e}")
            # Cleanup driver on error to force fresh start next time
            self._cleanup_driver()
            
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

            # 🔥 FIX: extract all price/discount blocks (not just one)
            price_sections = position.select('div.NDS')
            for price_section in price_sections:
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
        """Fetch content with optimized approach - REUSES browser"""
        try:
            logger.info(f"Fetching content from: {self.url}")
            
            driver = self._get_driver()  # Get existing or create driver
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
            # Cleanup driver on major error
            self._cleanup_driver()
            return None
    
    def cleanup(self):
        """Public cleanup method"""
        self._cleanup_driver()

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
    
    def get_last_7_days_data(self) -> List[Dict]:
        """Get data from the last 7 days"""
        try:
            data = self.load_data()
            seven_days_ago = datetime.now() - timedelta(days=7)
            
            recent_data = []
            for entry in data:
                try:
                    entry_time = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                    if entry_time >= seven_days_ago:
                        recent_data.append(entry)
                except Exception as e:
                    logger.warning(f"Error parsing timestamp for entry: {e}")
                    continue
            
            logger.info(f"Found {len(recent_data)} entries from last 7 days")
            return recent_data
        except Exception as e:
            logger.error(f"Error getting last 7 days data: {e}")
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

def get_top_selling_products_by_quantity_last_7_days() -> List[Dict]:
    """Get top 10 selling products from last 7 days (by quantity)"""
    try:
        data = data_manager.get_last_7_days_data()
        
        # Aggregate sales by product
        product_sales = defaultdict(lambda: {'quantity': 0, 'revenue': 0.0, 'occurrences': 0})
        
        for entry in data:
            sales_data_list = entry.get('sales_data', [])
            if isinstance(sales_data_list, list):
                for item in sales_data_list:
                    if item and item.get('product_name'):
                        product_name = item['product_name']
                        quantity = int(item.get('quantity', 1))
                        try:
                            revenue = float(item.get('total_price', '0').replace(',', '.'))
                        except (ValueError, TypeError):
                            revenue = 0.0
                        
                        product_sales[product_name]['quantity'] += quantity
                        product_sales[product_name]['revenue'] += revenue
                        product_sales[product_name]['occurrences'] += 1
        
        # Convert to list
        top_products = []
        for product_name, stats in product_sales.items():
            top_products.append({
                'product_name': product_name,
                'total_quantity': stats['quantity'],
                'total_revenue': round(stats['revenue'], 2),
                'occurrences': stats['occurrences'],
                'average_revenue_per_sale': round(
                    stats['revenue'] / stats['occurrences'], 2
                ) if stats['occurrences'] > 0 else 0
            })
        
        # ✅ Sort by total quantity (descending) and take top 10
        top_products.sort(key=lambda x: x['total_quantity'], reverse=True)
        top_10 = top_products[:10]
        
        logger.info(f"Found {len(top_10)} top selling products (by quantity)")
        return top_10
        
    except Exception as e:
        logger.error(f"Error getting top selling products: {e}")
        return []


def get_top_selling_products_by_revenue_last_7_days() -> List[Dict]:
    """Get top 10 selling products from last 7 days"""
    try:
        data = data_manager.get_last_7_days_data()
        
        # Aggregate sales by product
        product_sales = defaultdict(lambda: {'quantity': 0, 'revenue': 0.0, 'occurrences': 0})
        
        for entry in data:
            sales_data_list = entry.get('sales_data', [])
            if isinstance(sales_data_list, list):
                for item in sales_data_list:
                    if item and item.get('product_name'):
                        product_name = item['product_name']
                        quantity = int(item.get('quantity', 1))
                        try:
                            revenue = float(item.get('total_price', '0').replace(',', '.'))
                        except (ValueError, TypeError):
                            revenue = 0.0
                        
                        product_sales[product_name]['quantity'] += quantity
                        product_sales[product_name]['revenue'] += revenue
                        product_sales[product_name]['occurrences'] += 1
        
        # Convert to list and sort by revenue (descending)
        top_products = []
        for product_name, stats in product_sales.items():
            top_products.append({
                'product_name': product_name,
                'total_quantity': stats['quantity'],
                'total_revenue': round(stats['revenue'], 2),
                'occurrences': stats['occurrences'],
                'average_revenue_per_sale': round(stats['revenue'] / stats['occurrences'], 2) if stats['occurrences'] > 0 else 0
            })
        
        # Sort by total revenue descending and take top 10
        top_products.sort(key=lambda x: x['total_revenue'], reverse=True)
        top_10 = top_products[:10]
        
        logger.info(f"Found {len(top_10)} top selling products")
        return top_10
        
    except Exception as e:
        logger.error(f"Error getting top selling products: {e}")
        return []

def collect_and_save_data():
    """Optimized data collection with single browser instance"""
    url = os.getenv('TARGET_URL', 'example.com')
    if not url:
        logger.error("TARGET_URL environment variable not set!")
        return
    
    logger.info(f"Starting data collection for URL: {url}")
    collector = OptimizedDataCollector(url)  # Single instance
    
    # Register cleanup on exit
    import atexit
    atexit.register(collector.cleanup)
    
    # Configurable intervals - INCREASE to reduce load
    normal_interval = int(os.getenv('CHECK_INTERVAL', '60'))  # Increased from 30 to 60
    error_interval = int(os.getenv('ERROR_INTERVAL', '120'))   # Increased from 60 to 120
    
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
                # Cleanup and recreate collector on persistent errors
                collector.cleanup()
                collector = OptimizedDataCollector(url)
            elif consecutive_errors > 0:
                sleep_time = error_interval
            
            logger.info(f"💤 Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"🔴 Error in data collection loop: {e}")
            # Cleanup on error
            collector.cleanup()
            collector = OptimizedDataCollector(url)
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
    
    # Get top selling products for the chart
    top_products = get_top_selling_products_by_quantity_last_7_days()
    
    return render_template('index.html', 
                         entries=flattened_entries, 
                         total_entries=len(flattened_entries),
                         totals=totals,
                         top_products=top_products,
                         now=datetime.now())


@app.route('/export/csv')
def export_csv():
    """Export data as CSV with proper UTF-8 encoding"""
    entries = data_manager.load_data()
    
    # Use BytesIO for binary data with UTF-8 encoding
    output = io.BytesIO()
    
    # Write UTF-8 BOM for Excel compatibility
    output.write(b'\xef\xbb\xbf')
    
    # Create a CSV writer that writes to the BytesIO buffer
    writer = csv.writer(io.TextIOWrapper(output, encoding='utf-8-sig', write_through=True))
    
    writer.writerow([
        'Timestamp', 'Product', 'UKTZED', 'Barcode', 
        'Quantity', 'Unit Price (UAH)', 'Total Price (UAH)', 'Price Details'
    ])
    
    for entry in entries:
        sales_data = entry.get('sales_data', {})
        
        # Handle case where sales_data might be a list
        if isinstance(sales_data, list):
            # If it's a list, take the first item or empty dict
            sales_data = sales_data[0] if sales_data else {}
        
        # Ensure all values are properly encoded as strings
        writer.writerow([
            str(entry.get('timestamp', '')),
            str(sales_data.get('product_name', '')),
            str(sales_data.get('uktzed', '')),
            str(sales_data.get('barcode', '')),
            str(sales_data.get('quantity', '')),
            str(sales_data.get('unit_price', '')),
            str(sales_data.get('total_price', '')),
            str(sales_data.get('price_details', ''))
        ])
    
    # Get the CSV data and create response
    csv_data = output.getvalue()
    
    response = Response(
        csv_data,
        mimetype='text/csv; charset=utf-8',
        headers={
            'Content-Disposition': f'attachment; filename=pharmacy-sales-{datetime.now().strftime("%Y-%m-%d")}.csv'
        }
    )
    
    return response


@app.route('/export/excel')
def export_excel():
    """Export data as Excel-compatible CSV with UTF-8 BOM"""
    entries = data_manager.load_data()
    
    # Create CSV with UTF-8 BOM for Excel compatibility
    output = io.BytesIO()
    output.write(b'\xef\xbb\xbf')  # UTF-8 BOM for Excel
    
    # Write CSV content
    output.write('Timestamp,Product,UKTZED,Barcode,Quantity,Unit Price (UAH),Total Price (UAH),Price Details,URL\n'.encode('utf-8'))
    
    for entry in entries:
        sales_data = entry.get('sales_data', {})
        
        # Handle case where sales_data might be a list
        if isinstance(sales_data, list):
            # If it's a list, take the first item or empty dict
            sales_data = sales_data[0] if sales_data else {}
        
        row = [
            entry.get('timestamp', ''),
            sales_data.get('product_name', ''),
            sales_data.get('uktzed', ''),
            sales_data.get('barcode', ''),
            str(sales_data.get('quantity', '')),
            str(sales_data.get('unit_price', '')),
            str(sales_data.get('total_price', '')),
            sales_data.get('price_details', '')        ]
        # Properly escape CSV fields
        csv_row = ','.join(f'"{field.replace('"', '""')}"' for field in row) + '\n'
        output.write(csv_row.encode('utf-8'))
    
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

@app.route('/api/top-products')
def api_top_products():
    """API endpoint for top selling products"""
    top_products = get_top_selling_products_by_quantity_last_7_days()
    return jsonify(top_products)

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