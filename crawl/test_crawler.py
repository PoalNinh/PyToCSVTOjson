# test_crawler.py - Test crawler với debug chi tiết
import requests
import cloudscraper
from bs4 import BeautifulSoup
import logging
import random
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_batdongsan():
    """Test crawler với debug chi tiết"""
    logger.info("🧪 Testing BatDongSan crawler...")
    
    # Test URL
    url = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm/p1"
    
    # User agents
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Mobile/15E148 Safari/604.1'
    ]
    
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://batdongsan.com.vn/'
    }
    
    # Test methods
    methods = ['cloudscraper', 'mobile', 'requests']
    
    for method in methods:
        logger.info(f"\n🔍 Testing method: {method}")
        
        try:
            if method == 'cloudscraper':
                scraper = cloudscraper.create_scraper()
                response = scraper.get(url, headers=headers, timeout=30)
            elif method == 'mobile':
                headers['User-Agent'] = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Mobile/15E148 Safari/604.1'
                response = requests.get(url, headers=headers, timeout=30)
            else:
                response = requests.get(url, headers=headers, timeout=30)
            
            logger.info(f"📊 Status: {response.status_code}")
            logger.info(f"📏 Content length: {len(response.text)}")
            
            if response.status_code == 200:
                # Save HTML for inspection
                with open(f'test_{method}.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"💾 HTML saved to test_{method}.html")
                
                # Parse HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check title
                title = soup.title.text if soup.title else 'No title'
                logger.info(f"📄 Page title: {title}")
                
                # Look for cards with multiple selectors
                selectors = [
                    'div.js__card.js__card-full-web',
                    'div.re__card-full',
                    'div[class*="js__card"]',
                    'div[class*="re__card"]',
                    'div[class*="card"]',
                    'div[class*="item"]',
                    'article'
                ]
                
                for selector in selectors:
                    cards = soup.select(selector)
                    if cards:
                        logger.info(f"✅ Found {len(cards)} cards with: {selector}")
                        
                        # Show first card info
                        if cards:
                            first_card = cards[0]
                            logger.info(f"   First card classes: {first_card.get('class', 'No class')}")
                            logger.info(f"   First card ID: {first_card.get('id', 'No id')}")
                            
                            # Look for links
                            links = first_card.find_all('a')
                            logger.info(f"   Links found: {len(links)}")
                            for i, link in enumerate(links[:3]):
                                logger.info(f"     Link {i+1}: {link.get('href', 'No href')[:50]}...")
                        
                        break
                else:
                    logger.warning("❌ No cards found with any selector")
                    
                    # Show page structure
                    logger.info("🔍 Page structure analysis:")
                    all_divs = soup.find_all('div')
                    logger.info(f"   Total divs: {len(all_divs)}")
                    
                    # Show first 10 div classes
                    for i, div in enumerate(all_divs[:10]):
                        classes = div.get('class', [])
                        if classes:
                            logger.info(f"   Div {i+1} classes: {classes}")
                
                break  # Success, stop testing other methods
                
            else:
                logger.warning(f"⚠️ HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Error with {method}: {str(e)}")
    
    logger.info("\n🎯 Test completed!")

if __name__ == "__main__":
    test_batdongsan()

