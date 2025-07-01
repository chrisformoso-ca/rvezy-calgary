import csv
import re
import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RVezyDataExtractor:
    def __init__(self, input_file: str, output_db: str):
        self.input_file = Path(input_file)
        self.output_db = Path(output_db)
        self.conn = None
        self.cursor = None
        
    def __enter__(self):
        self.conn = sqlite3.connect(self.output_db)
        self.cursor = self.conn.cursor()
        self.create_tables()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.commit()
            self.conn.close()
            
    def create_tables(self):
        """Create database schema"""
        
        # Hosts table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS hosts (
                host_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                joined_year INTEGER,
                response_rate INTEGER,
                is_superhost BOOLEAN,
                UNIQUE(name, joined_year)
            )
        ''')
        
        # Listings table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS listings (
                listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                title TEXT,
                host_id INTEGER,
                location_city TEXT,
                location_province TEXT,
                rv_type TEXT,
                rv_year INTEGER,
                rv_make TEXT,
                rv_model TEXT,
                length_ft INTEGER,
                sleeps INTEGER,
                num_slide_outs INTEGER,
                weight_lbs INTEGER,
                hitch_size TEXT,
                hitch_weight_lbs INTEGER,
                base_price REAL,
                security_deposit REAL,
                pet_friendly BOOLEAN,
                delivery_available BOOLEAN,
                delivery_max_km INTEGER,
                delivery_price_per_km REAL,
                overall_rating REAL,
                num_reviews INTEGER,
                accuracy_rating REAL,
                value_rating REAL,
                cleanliness_rating REAL,
                communication_rating REAL,
                flexible_pickup BOOLEAN,
                flexible_dropoff BOOLEAN,
                towing_experience_required BOOLEAN,
                FOREIGN KEY (host_id) REFERENCES hosts(host_id)
            )
        ''')
        
        # Pricing table for discounts
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS pricing (
                pricing_id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER,
                discount_type TEXT,
                discount_percent INTEGER,
                discounted_price REAL,
                FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
            )
        ''')
        
        # Amenities table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS amenities (
                amenity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )
        ''')
        
        # Listing amenities junction table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS listing_amenities (
                listing_id INTEGER,
                amenity_id INTEGER,
                PRIMARY KEY (listing_id, amenity_id),
                FOREIGN KEY (listing_id) REFERENCES listings(listing_id),
                FOREIGN KEY (amenity_id) REFERENCES amenities(amenity_id)
            )
        ''')
        
        # Add-ons table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS addons (
                addon_id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER,
                name TEXT,
                price REAL,
                FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
            )
        ''')
        
        # Beds table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS beds (
                bed_id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER,
                bed_type TEXT,
                bed_size TEXT,
                quantity INTEGER,
                FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
            )
        ''')
        
    def extract_host_info(self, content: str) -> Dict:
        """Extract host information from content"""
        host_info = {
            'name': None,
            'joined_year': None,
            'response_rate': None,
            'is_superhost': False
        }
        
        # Extract host name and join year
        host_match = re.search(r'Hosted by ([A-Za-z]+)Joined in (\d{4})', content)
        if host_match:
            host_info['name'] = host_match.group(1)
            host_info['joined_year'] = int(host_match.group(2))
        
        # Extract response rate
        response_match = re.search(r'(\d+)% response rate', content)
        if response_match:
            host_info['response_rate'] = int(response_match.group(1))
        
        # Check if superhost
        if 'Superhost' in content:
            host_info['is_superhost'] = True
            
        return host_info
    
    def extract_rv_specs(self, content: str, title: str) -> Dict:
        """Extract RV specifications"""
        specs = {
            'rv_type': None,
            'rv_year': None,
            'rv_make': None,
            'rv_model': None,
            'length_ft': None,
            'sleeps': None,
            'num_slide_outs': None,
            'weight_lbs': None,
            'hitch_size': None,
            'hitch_weight_lbs': None
        }
        
        # Extract year, make, model from title
        title_match = re.search(r'Rent my (\d{4}) ([A-Za-z\-\s]+?) ([A-Za-z0-9\-\s]+?) from', title)
        if title_match:
            specs['rv_year'] = int(title_match.group(1))
            specs['rv_make'] = title_match.group(2).strip()
            specs['rv_model'] = title_match.group(3).strip()
        
        # Extract RV type from structured field
        rv_type_match = re.search(r'Type of RV\s*([A-Za-z\s]+?)(?:Accommodations|What)', content)
        if rv_type_match:
            specs['rv_type'] = rv_type_match.group(1).strip()
        else:
            # Fallback to searching in content for common types
            rv_types = ['Travel Trailer', 'Class A', 'Class B', 'Class C', 'Fifth Wheel', 
                       'Toy Hauler', 'Campervan', 'Tent Trailer', 'Micro Trailer', 
                       'Hybrid', 'Truck Camper', 'RV Cottage']
            for rv_type in rv_types:
                if rv_type in content:
                    specs['rv_type'] = rv_type
                    break
        
        # Extract specifications
        patterns = {
            'num_slide_outs': r'# of slide outs\s*(\d+)',
            'weight_lbs': r'Weight\s*(\d+)\s*lbs',
            'hitch_weight_lbs': r'Hitch Weight\s*(\d+)\s*lbs',
            'length_ft': r'Length\(ft\)\s*(\d+)\s*ft'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, content)
            if match:
                specs[key] = int(match.group(1))
        
        # Extract sleeps with proper pattern to avoid concatenation with length
        sleeps_match = re.search(r'Sleeps\s*(\d{1,2})(?:\s|$|[^0-9])', content)
        if sleeps_match:
            sleeps_value = int(sleeps_match.group(1))
            # Validate sleeps value is reasonable
            if 1 <= sleeps_value <= 20:
                specs['sleeps'] = sleeps_value
            else:
                print(f"Warning: Invalid sleeps value {sleeps_value} found, skipping")
        
        # Extract hitch size
        hitch_match = re.search(r'Hitch Size\s*([0-9\s/"]+")', content)
        if hitch_match:
            specs['hitch_size'] = hitch_match.group(1).strip()
            
        return specs
    
    def extract_location(self, content: str) -> Tuple[str, str]:
        """Extract city and province"""
        # Pattern for location extraction
        location_match = re.search(r'ft([A-Za-z\-\s]+), ([A-Z]{2})', content)
        if location_match:
            city = location_match.group(1).strip()
            province = location_match.group(2)
            return city, province
        return None, None
    
    def extract_pricing(self, content: str, title: str) -> Dict:
        """Extract pricing information"""
        pricing = {
            'base_price': None,
            'security_deposit': None,
            'discounts': []
        }
        
        # Extract base price from title
        price_match = re.search(r'from \$(\d+)/night', title)
        if price_match:
            pricing['base_price'] = float(price_match.group(1))
        
        # Extract security deposit
        deposit_match = re.search(r'Security Deposit\$(\d+)', content)
        if deposit_match:
            pricing['security_deposit'] = float(deposit_match.group(1))
        
        # Extract discounts
        discount_patterns = [
            (r'Midweek\$(\d+)/Night(\d+)% off', 'midweek'),
            (r'Weekly\$(\d+)/Night(\d+)% off', 'weekly'),
            (r'Monthly\$(\d+)/Night(\d+)% off', 'monthly')
        ]
        
        for pattern, discount_type in discount_patterns:
            match = re.search(pattern, content)
            if match:
                pricing['discounts'].append({
                    'type': discount_type,
                    'price': float(match.group(1)),
                    'percent': int(match.group(2))
                })
                
        return pricing
    
    def extract_amenities(self, content: str) -> List[str]:
        """Extract amenities list"""
        amenities = []
        
        # Common amenities to look for
        amenity_list = [
            'Air conditioner', 'Heater', 'Awning', 'Solar', 'Inverter',
            'Inside shower', 'Outside shower', 'Toilet', 'TV & DVD',
            'Refrigerator', 'Freezer', 'Stove range', 'Microwave', 'Oven',
            'Kitchen sink', 'Dining table', 'Linens provided', 'Camping chairs',
            'Pet friendly', 'Family friendly', 'Backup camera', 'Leveling jacks',
            'Tow hitch', 'On board generator', 'CD player', 'Radio', 'Aux input',
            'USB input', 'Extra storage', 'Full-Winter rental available'
        ]
        
        for amenity in amenity_list:
            if amenity in content:
                amenities.append(amenity)
                
        return amenities
    
    def extract_reviews(self, content: str) -> Dict:
        """Extract review information"""
        reviews = {
            'overall_rating': None,
            'num_reviews': None,
            'accuracy_rating': None,
            'value_rating': None,
            'cleanliness_rating': None,
            'communication_rating': None
        }
        
        # Extract overall rating and number of reviews
        rating_match = re.search(r'(\d+\.\d+)\((\d+) reviews\)', content)
        if rating_match:
            reviews['overall_rating'] = float(rating_match.group(1))
            reviews['num_reviews'] = int(rating_match.group(2))
        
        # Extract individual ratings
        rating_types = {
            'accuracy_rating': r'Accuracy(\d+\.\d+)',
            'value_rating': r'Value(\d+\.\d+)',
            'cleanliness_rating': r'Cleanliness(\d+\.\d+)',
            'communication_rating': r'Communication(\d+\.\d+)'
        }
        
        for key, pattern in rating_types.items():
            match = re.search(pattern, content)
            if match:
                reviews[key] = float(match.group(1))
                
        return reviews
    
    def extract_delivery_info(self, content: str) -> Dict:
        """Extract delivery information"""
        delivery = {
            'available': False,
            'max_km': None,
            'price_per_km': None
        }
        
        if 'No truck no problem' in content or 'delivery' in content.lower():
            delivery['available'] = True
            
            # Extract max delivery distance
            km_match = re.search(r'delivery up to (\d+) km', content)
            if km_match:
                delivery['max_km'] = int(km_match.group(1))
            
            # Extract delivery price
            price_match = re.search(r'Delivery\$(\d+\.?\d*) per km', content)
            if price_match:
                delivery['price_per_km'] = float(price_match.group(1))
                
        return delivery
    
    def extract_rules(self, content: str) -> Dict:
        """Extract RV rules"""
        rules = {
            'flexible_pickup': 'Flexible pickup time' in content,
            'flexible_dropoff': 'Flexible drop-off time' in content,
            'towing_experience_required': 'Towing experience required' in content,
            'pets_allowed': 'Pet friendly' in content and 'No pets' not in content
        }
        
        return rules
    
    def extract_addons(self, content: str) -> List[Dict]:
        """Extract add-ons and their prices"""
        addons = []
        
        # Pattern for add-ons
        addon_pattern = r'([A-Za-z\s]+):\s*\$(\d+)'
        addon_section = re.search(r'Add-ons(.+?)RV rules', content, re.DOTALL)
        
        if addon_section:
            addon_text = addon_section.group(1)
            for match in re.finditer(addon_pattern, addon_text):
                addons.append({
                    'name': match.group(1).strip(),
                    'price': float(match.group(2))
                })
                
        return addons
    
    def extract_beds(self, content: str) -> List[Dict]:
        """Extract bed information"""
        beds = []
        
        # Pattern for beds
        bed_patterns = [
            (r'(\d+) bed([A-Za-z]+)', 'bed'),
            (r'(\d+) dinette bed([A-Za-z]+)', 'dinette bed'),
            (r'(\d+) pullout sofa([A-Za-z]+)', 'pullout sofa'),
            (r'(\d+) bunk bed([A-Za-z]+)', 'bunk bed')
        ]
        
        for pattern, bed_type in bed_patterns:
            matches = re.finditer(pattern, content)
            for match in matches:
                beds.append({
                    'quantity': int(match.group(1)),
                    'bed_type': bed_type,
                    'bed_size': match.group(2)
                })
                
        return beds
    
    def process_listing(self, row: Dict) -> None:
        """Process a single listing and insert into database"""
        try:
            url = row['URL']
            title = row['Title']
            content = row['Content']
            
            # Extract all information
            host_info = self.extract_host_info(content)
            rv_specs = self.extract_rv_specs(content, title)
            city, province = self.extract_location(content)
            pricing = self.extract_pricing(content, title)
            amenities = self.extract_amenities(content)
            reviews = self.extract_reviews(content)
            delivery = self.extract_delivery_info(content)
            rules = self.extract_rules(content)
            addons = self.extract_addons(content)
            beds = self.extract_beds(content)
            
            # Insert host if not exists
            self.cursor.execute('''
                INSERT OR IGNORE INTO hosts (name, joined_year, response_rate, is_superhost)
                VALUES (?, ?, ?, ?)
            ''', (host_info['name'], host_info['joined_year'], 
                  host_info['response_rate'], host_info['is_superhost']))
            
            # Get host_id
            self.cursor.execute('''
                SELECT host_id FROM hosts WHERE name = ? AND joined_year = ?
            ''', (host_info['name'], host_info['joined_year']))
            host_id = self.cursor.fetchone()
            host_id = host_id[0] if host_id else None
            
            # Insert listing
            self.cursor.execute('''
                INSERT OR REPLACE INTO listings (
                    url, title, host_id, location_city, location_province,
                    rv_type, rv_year, rv_make, rv_model, length_ft, sleeps,
                    num_slide_outs, weight_lbs, hitch_size, hitch_weight_lbs,
                    base_price, security_deposit, pet_friendly, delivery_available,
                    delivery_max_km, delivery_price_per_km, overall_rating, num_reviews,
                    accuracy_rating, value_rating, cleanliness_rating, communication_rating,
                    flexible_pickup, flexible_dropoff, towing_experience_required
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                url, title, host_id, city, province,
                rv_specs['rv_type'], rv_specs['rv_year'], rv_specs['rv_make'],
                rv_specs['rv_model'], rv_specs['length_ft'], rv_specs['sleeps'],
                rv_specs['num_slide_outs'], rv_specs['weight_lbs'], rv_specs['hitch_size'],
                rv_specs['hitch_weight_lbs'], pricing['base_price'], pricing['security_deposit'],
                rules['pets_allowed'], delivery['available'], delivery['max_km'],
                delivery['price_per_km'], reviews['overall_rating'], reviews['num_reviews'],
                reviews['accuracy_rating'], reviews['value_rating'], reviews['cleanliness_rating'],
                reviews['communication_rating'], rules['flexible_pickup'], rules['flexible_dropoff'],
                rules['towing_experience_required']
            ))
            
            listing_id = self.cursor.lastrowid
            
            # Insert pricing discounts
            for discount in pricing['discounts']:
                self.cursor.execute('''
                    INSERT INTO pricing (listing_id, discount_type, discount_percent, discounted_price)
                    VALUES (?, ?, ?, ?)
                ''', (listing_id, discount['type'], discount['percent'], discount['price']))
            
            # Insert amenities
            for amenity in amenities:
                self.cursor.execute('INSERT OR IGNORE INTO amenities (name) VALUES (?)', (amenity,))
                self.cursor.execute('SELECT amenity_id FROM amenities WHERE name = ?', (amenity,))
                amenity_id = self.cursor.fetchone()[0]
                self.cursor.execute('''
                    INSERT OR IGNORE INTO listing_amenities (listing_id, amenity_id) 
                    VALUES (?, ?)
                ''', (listing_id, amenity_id))
            
            # Insert add-ons
            for addon in addons:
                self.cursor.execute('''
                    INSERT INTO addons (listing_id, name, price) VALUES (?, ?, ?)
                ''', (listing_id, addon['name'], addon['price']))
            
            # Insert beds
            for bed in beds:
                self.cursor.execute('''
                    INSERT INTO beds (listing_id, bed_type, bed_size, quantity) VALUES (?, ?, ?, ?)
                ''', (listing_id, bed['bed_type'], bed['bed_size'], bed['quantity']))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error processing listing {row.get('URL', 'Unknown')}: {str(e)}")
    
    def process_file(self):
        """Process the entire CSV file"""
        total_processed = 0
        
        with open(self.input_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            
            for row in csv_reader:
                self.process_listing(row)
                total_processed += 1
                
                if total_processed % 50 == 0:
                    logger.info(f"Processed {total_processed} listings...")
        
        logger.info(f"Total listings processed: {total_processed}")
        
        # Print summary statistics
        self.print_summary()
    
    def print_summary(self):
        """Print summary statistics of the extracted data"""
        logger.info("\n=== Data Extraction Summary ===")
        
        # Count listings by RV type
        self.cursor.execute('''
            SELECT rv_type, COUNT(*) as count, AVG(base_price) as avg_price
            FROM listings
            WHERE rv_type IS NOT NULL
            GROUP BY rv_type
            ORDER BY count DESC
        ''')
        
        logger.info("\nListings by RV Type:")
        for row in self.cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]} listings, avg price ${row[2]:.2f}/night")
        
        # Count listings by city
        self.cursor.execute('''
            SELECT location_city, COUNT(*) as count
            FROM listings
            WHERE location_city IS NOT NULL
            GROUP BY location_city
            ORDER BY count DESC
            LIMIT 10
        ''')
        
        logger.info("\nTop 10 Cities:")
        for row in self.cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]} listings")
        
        # Price range
        self.cursor.execute('''
            SELECT MIN(base_price), MAX(base_price), AVG(base_price)
            FROM listings
            WHERE base_price IS NOT NULL
        ''')
        
        min_price, max_price, avg_price = self.cursor.fetchone()
        logger.info(f"\nPrice Range: ${min_price:.2f} - ${max_price:.2f}, Average: ${avg_price:.2f}")


def main():
    input_file = "/home/chris/rvezy/data/raw/RVEzy Listings Text 06302025.csv"
    output_db = "/home/chris/rvezy/data/processed/rvezy_listings.db"
    
    logger.info(f"Starting data extraction from {input_file}")
    logger.info(f"Output database: {output_db}")
    
    with RVezyDataExtractor(input_file, output_db) as extractor:
        extractor.process_file()
    
    logger.info("Data extraction completed successfully!")


if __name__ == "__main__":
    main()