import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from urllib.parse import urljoin, urlparse
import re
import sys
import argparse
import json
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlonhadatMultiCrawler:
    def __init__(self, urls_list=None):
        self.base_url = "https://alonhadat.com.vn"
        self.urls_list = urls_list if urls_list else []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://alonhadat.com.vn/'
        })
        self.all_data = []
        self.url_data = {}  # Dữ liệu theo từng URL

    def get_page_content(self, url, retries=3):
        """Lấy nội dung trang web với retry mechanism"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response
            except requests.RequestException as e:
                logger.error(f"Lỗi khi truy cập {url} (lần thử {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None

    def clean_text(self, text):
        """Làm sạch text"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())

    def get_url_name(self, url):
        """Lấy tên ngắn gọn từ URL"""
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        if path:
            # Lấy phần cuối của path và loại bỏ .htm
            name = path.split('/')[-1].replace('.htm', '')
            # Làm ngắn gọn hơn
            if len(name) > 50:
                name = name[:50] + "..."
            return name
        return "unknown"

    def parse_property_item(self, item, source_url):
        """Parse thông tin từ một item bất động sản"""
        try:
            property_data = {}
            
            # Thêm thông tin source
            property_data['source_url'] = source_url
            property_data['source_name'] = self.get_url_name(source_url)
            property_data['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 1. Tiêu đề và URL
            title_elem = item.find('div', class_='ct_title')
            if title_elem:
                link_elem = title_elem.find('a')
                if link_elem:
                    property_data['title'] = self.clean_text(link_elem.get_text())
                    property_data['detail_url'] = urljoin(self.base_url, link_elem.get('href', ''))
                    property_data['vip_class'] = link_elem.get('class', [])
                else:
                    property_data['title'] = self.clean_text(title_elem.get_text())
                    property_data['detail_url'] = ''
                    property_data['vip_class'] = []
            else:
                property_data['title'] = ''
                property_data['detail_url'] = ''
                property_data['vip_class'] = []

            # 2. Ngày đăng
            date_elem = item.find('div', class_='ct_date')
            property_data['post_date'] = self.clean_text(date_elem.get_text()) if date_elem else ''

            # 3. VIP level
            vip_elem = item.find('div', class_='vipstar')
            if vip_elem:
                vip_classes = vip_elem.get('class', [])
                vip_level = ''
                for cls in vip_classes:
                    if cls.startswith('vip-'):
                        vip_level = cls
                        break
                property_data['vip_level'] = vip_level
            else:
                property_data['vip_level'] = ''

            # 4. Hình ảnh
            img_elem = item.find('div', class_='thumbnail')
            if img_elem:
                img_tag = img_elem.find('img')
                if img_tag:
                    property_data['image_url'] = urljoin(self.base_url, img_tag.get('src', ''))
                    property_data['image_alt'] = img_tag.get('alt', '')
                else:
                    property_data['image_url'] = ''
                    property_data['image_alt'] = ''
            else:
                property_data['image_url'] = ''
                property_data['image_alt'] = ''

            # 5. Mô tả ngắn
            brief_elem = item.find('div', class_='ct_brief')
            if brief_elem:
                for link in brief_elem.find_all('a'):
                    link.decompose()
                property_data['description'] = self.clean_text(brief_elem.get_text())
            else:
                property_data['description'] = ''

            # 6. Diện tích
            area_elem = item.find('div', class_='ct_dt')
            if area_elem:
                area_text = area_elem.get_text()
                area_match = re.search(r'(\d+(?:[.,]\d+)?)', area_text.replace(',', '.'))
                property_data['area'] = area_match.group(1) if area_match else ''
                property_data['area_text'] = self.clean_text(area_text)
            else:
                property_data['area'] = ''
                property_data['area_text'] = ''

            # 7. Kích thước
            size_elem = item.find('div', class_='ct_kt')
            if size_elem:
                size_text = size_elem.get_text()
                size_match = re.search(r'KT:\s*(.+)', size_text)
                property_data['dimensions'] = size_match.group(1).strip() if size_match else self.clean_text(size_text)
            else:
                property_data['dimensions'] = ''

            # 8. Hướng nhà
            direction_elem = item.find('div', class_='ct_direct')
            if direction_elem:
                direction_text = direction_elem.get_text()
                direction_match = re.search(r'Hướng:\s*(.+)', direction_text)
                property_data['direction'] = direction_match.group(1).strip() if direction_match else self.clean_text(direction_text)
            else:
                property_data['direction'] = ''

            # 9. Đường trước nhà
            road_elem = item.find('span', class_='road-width')
            property_data['road_width'] = self.clean_text(road_elem.get_text()) if road_elem else ''

            # 10. Số tầng
            floors_elem = item.find('span', class_='floors')
            property_data['floors'] = self.clean_text(floors_elem.get_text()) if floors_elem else ''

            # 11. Giá
            price_elem = item.find('div', class_='ct_price') or item.find('div', class_='ct*price')
            if price_elem:
                price_text = price_elem.get_text()
                price_match = re.search(r'Giá:\s*(.+)', price_text)
                property_data['price'] = price_match.group(1).strip() if price_match else self.clean_text(price_text)
            else:
                property_data['price'] = ''

            # 12. Địa chỉ
            address_elem = item.find('div', class_='ct_dis')
            if address_elem:
                property_data['address'] = self.clean_text(address_elem.get_text())
            else:
                property_data['address'] = ''

            return property_data

        except Exception as e:
            logger.error(f"Lỗi khi parse item: {e}")
            return None

    def crawl_page(self, url, source_url):
        """Crawl một trang"""
        logger.info(f"Đang crawl: {url}")
        
        response = self.get_page_content(url)
        if not response:
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        items = soup.find_all('div', class_='content-item')
        
        if not items:
            logger.warning(f"Không tìm thấy items nào trong trang {url}")
            return []

        page_data = []
        for i, item in enumerate(items, 1):
            property_data = self.parse_property_item(item, source_url)
            if property_data and property_data.get('title'):
                page_data.append(property_data)

        logger.info(f"Crawl được {len(page_data)} items từ {url}")
        return page_data

    def detect_url_pattern(self, url):
        """Phát hiện pattern URL"""
        parsed_url = urlparse(url)
        path = parsed_url.path
        
        if path.endswith('.htm') and not '/trang--' in path:
            base_pattern = path.replace('.htm', '')
            return f"{parsed_url.scheme}://{parsed_url.netloc}{base_pattern}/trang--{{page}}.htm"
        
        if '/trang--' in path:
            base_pattern = re.sub(r'/trang--\d+\.htm', '', path)
            return f"{parsed_url.scheme}://{parsed_url.netloc}{base_pattern}/trang--{{page}}.htm"
        
        return url.replace('.htm', '/trang--{page}.htm')

    def get_total_pages(self, base_url):
        """Lấy tổng số trang"""
        response = self.get_page_content(base_url)
        if not response:
            return 1

        soup = BeautifulSoup(response.content, 'html.parser')
        pagination = soup.find('div', class_='pagination') or soup.find('ul', class_='pagination')
        if not pagination:
            return 1

        page_links = pagination.find_all('a')
        max_page = 1
        
        for link in page_links:
            try:
                page_text = link.get_text(strip=True)
                if page_text.isdigit():
                    page_num = int(page_text)
                    max_page = max(max_page, page_num)
            except (ValueError, TypeError):
                continue

        return min(max_page, 20)  # Giới hạn 20 trang mỗi URL

    def crawl_single_url(self, crawl_url, max_pages=None):
        """Crawl một URL"""
        logger.info(f"Bắt đầu crawl URL: {crawl_url}")
        
        # Lấy tổng số trang
        if max_pages is None:
            total_pages = self.get_total_pages(crawl_url)
        else:
            total_pages = max_pages

        url_pattern = self.detect_url_pattern(crawl_url)
        url_data = []
        
        for page in range(1, total_pages + 1):
            if page == 1:
                url = crawl_url
            else:
                url = url_pattern.format(page=page)
            
            page_data = self.crawl_page(url, crawl_url)
            url_data.extend(page_data)
            
            if not page_data and page > 1:
                logger.info(f"Không có dữ liệu ở trang {page}, có thể đã hết")
                break
            
            time.sleep(2)  # Delay giữa các trang

        logger.info(f"Hoàn thành crawl {crawl_url}: {len(url_data)} items")
        return url_data

    def crawl_all_urls(self, urls_list=None, max_pages_per_url=None):
        """Crawl tất cả URLs"""
        if urls_list:
            self.urls_list = urls_list
        
        if not self.urls_list:
            logger.error("Không có URL nào để crawl!")
            return []

        logger.info(f"Bắt đầu crawl {len(self.urls_list)} URLs")
        
        all_data = []
        
        for i, url in enumerate(self.urls_list, 1):
            logger.info(f"Crawl URL {i}/{len(self.urls_list)}: {url}")
            
            try:
                url_data = self.crawl_single_url(url, max_pages_per_url)
                
                # Lưu data theo URL
                self.url_data[url] = url_data
                all_data.extend(url_data)
                
                logger.info(f"URL {i} hoàn thành: {len(url_data)} items")
                
                # Delay giữa các URL
                if i < len(self.urls_list):
                    time.sleep(5)
                
            except Exception as e:
                logger.error(f"Lỗi khi crawl {url}: {e}")
                continue

        self.all_data = all_data
        return all_data

    def save_to_excel(self, filename='alonhadat_multi_crawl.xlsx'):
        """Lưu dữ liệu ra file Excel với nhiều sheet"""
        if not self.all_data:
            logger.warning("Không có dữ liệu để lưu")
            return

        # Cột order
        column_order = [
            'source_name', 'title', 'price', 'area', 'area_text', 'dimensions', 
            'direction', 'floors', 'road_width', 'address', 'description', 
            'post_date', 'vip_level', 'detail_url', 'image_url', 'source_url', 'crawl_time'
        ]

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet 1: Tất cả dữ liệu
            df_all = pd.DataFrame(self.all_data)
            available_columns = [col for col in column_order if col in df_all.columns]
            df_all_ordered = df_all[available_columns]
            df_all_ordered.to_excel(writer, sheet_name='All_Data', index=False)
            
            # Sheet 2: Thống kê tổng quan
            stats_data = []
            for url, data in self.url_data.items():
                url_name = self.get_url_name(url)
                stats_data.append({
                    'URL': url_name,
                    'Full_URL': url,
                    'Total_Items': len(data),
                    'Has_Price': len([item for item in data if item.get('price')]),
                    'Has_Area': len([item for item in data if item.get('area')]),
                    'VIP_Items': len([item for item in data if item.get('vip_level')])
                })
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Statistics', index=False)
            
            # Sheet riêng cho từng URL (tối đa 10 sheet)
            for i, (url, data) in enumerate(self.url_data.items()):
                if i >= 10:  # Giới hạn số sheet
                    break
                    
                if data:
                    url_name = self.get_url_name(url)
                    # Tạo tên sheet hợp lệ
                    sheet_name = re.sub(r'[\\/*?:"<>|]', '', url_name)[:31]
                    
                    df_url = pd.DataFrame(data)
                    available_columns = [col for col in column_order if col in df_url.columns]
                    df_url_ordered = df_url[available_columns]
                    df_url_ordered.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Tự động điều chỉnh độ rộng cột cho tất cả sheet
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

        logger.info(f"Đã lưu {len(self.all_data)} items vào {filename}")
        return filename

    def print_summary(self):
        """In thống kê tóm tắt"""
        if not self.all_data:
            logger.info("Không có dữ liệu")
            return

        print(f"\n{'='*80}")
        print(f"THỐNG KÊ CRAWL MULTI URLs")
        print(f"{'='*80}")
        print(f"Tổng số URLs: {len(self.urls_list)}")
        print(f"Tổng số items: {len(self.all_data)}")
        
        print(f"\nThống kê theo từng URL:")
        for url, data in self.url_data.items():
            url_name = self.get_url_name(url)
            print(f"  {url_name}: {len(data)} items")
        
        # Thống kê VIP
        vip_count = len([item for item in self.all_data if item.get('vip_level')])
        print(f"\nTổng tin VIP: {vip_count}/{len(self.all_data)}")
        
        # Thống kê có giá/diện tích
        has_price = len([item for item in self.all_data if item.get('price')])
        has_area = len([item for item in self.all_data if item.get('area')])
        print(f"Có giá: {has_price}/{len(self.all_data)}")
        print(f"Có diện tích: {has_area}/{len(self.all_data)}")

def parse_urls_input(urls_input):
    """Parse input URLs từ nhiều format khác nhau"""
    urls = []
    
    # Nếu là string
    if isinstance(urls_input, str):
        # Kiểm tra xem có phải JSON không
        try:
            urls = json.loads(urls_input)
        except:
            # Tách bằng dấu phẩy hoặc xuống dòng
            urls = [url.strip() for url in re.split(r'[,\n]', urls_input) if url.strip()]
    
    # Nếu là list
    elif isinstance(urls_input, list):
        urls = [str(url).strip() for url in urls_input if str(url).strip()]
    
    # Validate URLs
    valid_urls = []
    for url in urls:
        if 'alonhadat.com.vn' in url:
            valid_urls.append(url)
        else:
            logger.warning(f"URL không hợp lệ (bỏ qua): {url}")
    
    return valid_urls

def main():
    """Hàm main"""
    parser = argparse.ArgumentParser(description='Crawl nhiều URLs từ alonhadat.com.vn')
    parser.add_argument('--urls', '-u', type=str, help='URLs cần crawl (JSON array hoặc cách nhau bởi dấu phẩy)')
    parser.add_argument('--file', '-f', type=str, help='File chứa danh sách URLs (mỗi dòng một URL)')
    parser.add_argument('--pages', '-p', type=int, default=5, help='Số trang tối đa mỗi URL')
    parser.add_argument('--output', '-o', type=str, help='Tên file output')
    
    args = parser.parse_args()
    
    urls_list = []
    
    # Lấy URLs từ arguments
    if args.urls:
        urls_list = parse_urls_input(args.urls)
    
    # Lấy URLs từ file
    elif args.file:
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                file_urls = [line.strip() for line in f.readlines() if line.strip()]
                urls_list = parse_urls_input(file_urls)
        except Exception as e:
            logger.error(f"Lỗi đọc file: {e}")
            sys.exit(1)
    
    # Input interactive
    else:
        print("Nhập danh sách URLs (mỗi dòng một URL, enter trống để kết thúc):")
        urls_input = []
        while True:
            url = input().strip()
            if not url:
                break
            urls_input.append(url)
        urls_list = parse_urls_input(urls_input)
    
    if not urls_list:
        print("Không có URL hợp lệ nào!")
        sys.exit(1)
    
    print(f"Sẽ crawl {len(urls_list)} URLs:")
    for i, url in enumerate(urls_list, 1):
        print(f"  {i}. {url}")
    
    confirm = input(f"\nTiếp tục crawl? (y/n): ").lower()
    if confirm != 'y':
        print("Đã hủy!")
        sys.exit(0)
    
    # Tạo crawler
    crawler = AlonhadatMultiCrawler(urls_list)
    
    try:
        # Crawl dữ liệu
        data = crawler.crawl_all_urls(max_pages_per_url=args.pages)
        
        if data:
            # In thống kê
            crawler.print_summary()
            
            # Lưu dữ liệu
            output_file = args.output or f'alonhadat_multi_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            crawler.save_to_excel(output_file)
            
            print(f"\nHoàn thành! Dữ liệu đã được lưu vào: {output_file}")
        else:
            logger.warning("Không crawl được dữ liệu nào")
            
    except KeyboardInterrupt:
        logger.info("Đã dừng crawl theo yêu cầu người dùng")
        if crawler.all_data:
            output_file = f'partial_multi_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            crawler.save_to_excel(output_file)
            logger.info(f"Đã lưu dữ liệu partial: {output_file}")

if __name__ == "__main__":
    main()