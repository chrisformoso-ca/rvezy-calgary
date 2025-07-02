import sqlite3
import pandas as pd
import json
from pathlib import Path
import numpy as np

def generate_comprehensive_dashboard_data():
    """Generate comprehensive data for the enhanced dashboard"""
    
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    dashboard_data = {}
    
    print("Generating comprehensive dashboard data...")
    
    # 1. Market Overview with all RV types
    print("1. Generating market overview...")
    market_overview = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            ROUND(AVG(base_price) - MIN(base_price), 2) as price_range,
            AVG(num_reviews) as avg_reviews,
            SUM(num_reviews) as total_reviews,
            COUNT(CASE WHEN num_reviews >= 20 THEN 1 END) * 100.0 / COUNT(*) as success_rate,
            AVG(CASE WHEN overall_rating >= 0 AND overall_rating <= 5 THEN overall_rating END) as avg_rating
        FROM listings
        WHERE base_price IS NOT NULL
        GROUP BY rv_type
        ORDER BY count DESC
    """, conn).to_dict('records')
    
    dashboard_data['market_overview'] = market_overview
    
    # 2. All listings for interactive charts
    print("2. Fetching all listings for interactive charts...")
    all_listings = pd.read_sql_query("""
        SELECT 
            l.listing_id,
            l.url,
            l.rv_type,
            l.rv_year,
            l.rv_make,
            l.rv_model,
            l.base_price,
            l.num_reviews,
            CASE WHEN l.overall_rating >= 0 AND l.overall_rating <= 5 THEN l.overall_rating ELSE NULL END as overall_rating,
            l.length_ft,
            CASE WHEN l.weight_lbs >= 1000 AND l.weight_lbs <= 30000 THEN l.weight_lbs ELSE NULL END as weight_lbs,
            l.sleeps,
            l.location_city,
            h.name as host_name,
            h.is_superhost
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.base_price IS NOT NULL
        ORDER BY l.rv_type, l.base_price
    """, conn).to_dict('records')
    
    dashboard_data['all_listings'] = all_listings
    
    # 3. Price distributions by RV type
    print("3. Generating price distributions...")
    price_distributions = {}
    rv_types = pd.read_sql_query("SELECT DISTINCT rv_type FROM listings WHERE rv_type IS NOT NULL", conn)['rv_type'].tolist()
    
    for rv_type in rv_types:
        prices = pd.read_sql_query("""
            SELECT base_price 
            FROM listings 
            WHERE rv_type = ? AND base_price IS NOT NULL
            ORDER BY base_price
        """, conn, params=[rv_type])['base_price'].tolist()
        
        if len(prices) >= 3:
            price_distributions[rv_type] = {
                'prices': prices,
                'count': len(prices),
                'min': min(prices),
                'max': max(prices),
                'mean': np.mean(prices),
                'median': np.median(prices),
                'q1': np.percentile(prices, 25),
                'q3': np.percentile(prices, 75),
                'std': np.std(prices)
            }
    
    dashboard_data['price_distributions'] = price_distributions
    
    # 4. Specifications Analysis
    print("4. Generating specifications analysis...")
    specs_analysis = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(*) as count,
            AVG(length_ft) as avg_length,
            MIN(length_ft) as min_length,
            MAX(length_ft) as max_length,
            AVG(CASE WHEN weight_lbs >= 1000 AND weight_lbs <= 30000 THEN weight_lbs END) as avg_weight,
            MIN(CASE WHEN weight_lbs >= 1000 AND weight_lbs <= 30000 THEN weight_lbs END) as min_weight,
            MAX(CASE WHEN weight_lbs >= 1000 AND weight_lbs <= 30000 THEN weight_lbs END) as max_weight,
            AVG(sleeps) as avg_sleeps,
            MIN(sleeps) as min_sleeps,
            MAX(sleeps) as max_sleeps
        FROM listings
        WHERE rv_type IS NOT NULL
        GROUP BY rv_type
        HAVING COUNT(*) >= 3
        ORDER BY count DESC
    """, conn).to_dict('records')
    
    dashboard_data['specs_analysis'] = specs_analysis
    
    # 5. Price threshold analysis with all listings for slider
    print("5. Generating price threshold analysis...")
    # Get all listings with price for dynamic filtering
    price_threshold_listings = pd.read_sql_query("""
        SELECT 
            l.listing_id,
            l.url,
            l.rv_type,
            l.rv_year,
            l.rv_make,
            l.rv_model,
            l.base_price,
            l.num_reviews,
            CASE WHEN l.overall_rating >= 0 AND l.overall_rating <= 5 THEN l.overall_rating ELSE NULL END as overall_rating,
            l.sleeps,
            h.name as host_name,
            h.is_superhost,
            h.host_id
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.base_price IS NOT NULL
        ORDER BY l.base_price ASC, l.num_reviews DESC
    """, conn).to_dict('records')
    
    # Create price distribution data for insights
    price_ranges = [
        {'min': 0, 'max': 100, 'label': 'Under $100'},
        {'min': 100, 'max': 150, 'label': '$100-150'},
        {'min': 150, 'max': 200, 'label': '$150-200'},
        {'min': 200, 'max': 250, 'label': '$200-250'},
        {'min': 250, 'max': 300, 'label': '$250-300'},
        {'min': 300, 'max': 400, 'label': '$300-400'},
        {'min': 400, 'max': 500, 'label': '$400+'}
    ]
    
    price_distribution = []
    for price_range in price_ranges:
        count = len([l for l in price_threshold_listings 
                    if price_range['min'] < l['base_price'] <= price_range['max']])
        price_distribution.append({
            'range': price_range['label'],
            'count': count,
            'min': price_range['min'],
            'max': price_range['max']
        })
    
    dashboard_data['price_threshold_analysis'] = {
        'all_listings': price_threshold_listings,
        'price_distribution': price_distribution,
        'min_price': min(l['base_price'] for l in price_threshold_listings),
        'max_price': max(l['base_price'] for l in price_threshold_listings)
    }
    
    # 6. Investment opportunities with comprehensive data
    print("6. Generating investment opportunities...")
    investment_opps = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(*) as market_size,
            MIN(base_price) as entry_price,
            MAX(base_price) as max_price,
            AVG(base_price) as avg_price,
            AVG(base_price) - MIN(base_price) as avg_above_entry,
            AVG(num_reviews) as avg_demand,
            COUNT(CASE WHEN num_reviews >= 20 THEN 1 END) * 100.0 / COUNT(*) as success_rate,
            AVG(base_price * 120) as summer_revenue_potential,
            COUNT(CASE WHEN h.is_superhost = 1 THEN 1 END) * 100.0 / COUNT(*) as superhost_rate,
            AVG(CASE WHEN l.overall_rating >= 0 AND l.overall_rating <= 5 THEN l.overall_rating END) as avg_rating
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.base_price IS NOT NULL
        GROUP BY rv_type
        ORDER BY market_size DESC
    """, conn).to_dict('records')
    
    # Add purchase price estimates based on market research
    purchase_prices = {
        'Travel Trailer': {'new': 23000, 'used': 19500},
        'Class C': {'new': 80000, 'used': 50000},
        'Class A': {'new': 150000, 'used': 100000},
        'Class B': {'new': 100000, 'used': 70000},
        'Fifth Wheel': {'new': 45000, 'used': 35000},
        'Tent Trailer': {'new': 15000, 'used': 10000},
        'Toy Hauler': {'new': 35000, 'used': 28000},
        'Campervan': {'new': 60000, 'used': 40000},
        'Micro Trailer': {'new': 18000, 'used': 12000},
        'Hybrid': {'new': 20000, 'used': 15000},
        'Truck Camper': {'new': 25000, 'used': 18000},
        'RV Cottage': {'new': 30000, 'used': 20000}
    }
    
    # Calculate ROI for each RV type
    for opp in investment_opps:
        rv_type = opp['rv_type']
        if rv_type in purchase_prices:
            opp['est_purchase_new'] = purchase_prices[rv_type]['new']
            opp['est_purchase_used'] = purchase_prices[rv_type]['used']
        else:
            # Default estimates for unknown types
            opp['est_purchase_new'] = 30000
            opp['est_purchase_used'] = 20000
        
        # Calculate ROI (assuming 30% operating costs)
        operating_cost_factor = 0.7  # 70% of revenue is profit after costs
        summer_profit = opp['summer_revenue_potential'] * operating_cost_factor
        
        # ROI based on used purchase price
        opp['roi_percentage'] = (summer_profit / opp['est_purchase_used']) * 100
        opp['payback_years'] = opp['est_purchase_used'] / (summer_profit * 2)  # Assuming 2 summer seasons per year
    
    dashboard_data['investment_opportunities'] = investment_opps
    
    # 6b. ROI Calculator Data - Category averages and cost assumptions
    print("6b. Generating ROI calculator data...")
    
    # Get detailed category averages for ROI calculations
    category_averages = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            AVG(CASE WHEN num_reviews >= 20 THEN base_price END) as avg_price_successful,
            AVG(num_reviews) as avg_reviews,
            AVG(CASE WHEN overall_rating >= 0 AND overall_rating <= 5 THEN overall_rating END) as avg_rating,
            AVG(sleeps) as avg_sleeps,
            AVG(length_ft) as avg_length,
            AVG(CASE WHEN rv_year >= 1990 AND rv_year <= 2025 THEN rv_year END) as avg_year
        FROM listings
        WHERE base_price IS NOT NULL
        AND rv_type IN ('Travel Trailer', 'Class C', 'Tent Trailer', 'Campervan', 'Class A')
        GROUP BY rv_type
        HAVING COUNT(*) >= 5
    """, conn).to_dict('records')
    
    # Add estimated purchase prices and summer revenue
    for cat in category_averages:
        # Summer revenue (120 days)
        cat['summer_revenue'] = cat['avg_price'] * 120 if cat['avg_price'] else 0
        cat['summer_revenue_successful'] = cat['avg_price_successful'] * 120 if cat['avg_price_successful'] else 0
        
        # Use existing purchase price estimates
        if cat['rv_type'] == 'Travel Trailer':
            cat['est_purchase_new'] = 35000
            cat['est_purchase_used'] = 25000
        elif cat['rv_type'] == 'Class C':
            cat['est_purchase_new'] = 80000
            cat['est_purchase_used'] = 55000
        elif cat['rv_type'] == 'Tent Trailer':
            cat['est_purchase_new'] = 20000
            cat['est_purchase_used'] = 12000
        elif cat['rv_type'] == 'Campervan':
            cat['est_purchase_new'] = 60000
            cat['est_purchase_used'] = 40000
        elif cat['rv_type'] == 'Class A':
            cat['est_purchase_new'] = 120000
            cat['est_purchase_used'] = 80000
        else:
            cat['est_purchase_new'] = 50000
            cat['est_purchase_used'] = 35000
    
    # Default cost assumptions for calculator
    cost_assumptions = {
        'rvezy_platform_fee': 0.20,  # 20% platform fee
        'insurance_rate': 0.025,  # 2.5% of RV value annually
        'maintenance_rate': 0.05,  # 5% of revenue for maintenance
        'cleaning_per_rental': 0,  # $0 default (user can adjust)
        'supplies_monthly': 0,  # $0 default for supplies
        'storage_monthly': {
            'small': 50,  # Tent Trailer
            'medium': 100,  # Travel Trailer, Campervan
            'large': 150  # Class A, Class C
        },
        'avg_rental_days': {
            'conservative': 84,  # 120 - 30% (summer days minus 30%)
            'moderate': 120,  # Base summer season
            'optimistic': 156  # 120 + 30% (summer days plus 30%)
        }
    }
    
    dashboard_data['roi_calculator'] = {
        'category_averages': category_averages,
        'cost_assumptions': cost_assumptions
    }
    
    # 7. Add-ons comprehensive analysis
    print("7. Generating add-ons analysis...")
    addons_analysis = pd.read_sql_query("""
        SELECT 
            a.name,
            COUNT(DISTINCT a.listing_id) as listings_offering,
            COUNT(DISTINCT a.listing_id) * 100.0 / (SELECT COUNT(*) FROM listings) as adoption_rate,
            MIN(CASE WHEN a.price > 0 AND a.price < 500 THEN a.price END) as min_price,
            AVG(CASE WHEN a.price > 0 AND a.price < 500 THEN a.price END) as avg_price,
            MAX(CASE WHEN a.price < 500 THEN a.price END) as max_price,
            GROUP_CONCAT(DISTINCT l.rv_type) as rv_types_offering
        FROM addons a
        JOIN listings l ON a.listing_id = l.listing_id
        WHERE a.name != '' AND a.name IS NOT NULL
        GROUP BY a.name
        HAVING COUNT(DISTINCT a.listing_id) >= 10
        ORDER BY listings_offering DESC
        LIMIT 20
    """, conn).to_dict('records')
    
    dashboard_data['addons_analysis'] = addons_analysis
    
    # 7b. Top listings with specific add-ons
    print("7b. Generating top listings with add-ons...")
    addon_listings = {}
    top_addons = ['Wifi', 'Portable BBQ', 'Starlink Satellites Internet', 'YYC', 'Fuel Refill Prepayment']
    
    for addon_name in top_addons:
        listings_with_addon = pd.read_sql_query("""
            SELECT DISTINCT
                l.url,
                l.rv_type,
                l.rv_year,
                l.rv_make,
                l.rv_model,
                l.base_price,
                l.num_reviews,
                a.price as addon_price
            FROM listings l
            JOIN addons a ON l.listing_id = a.listing_id
            WHERE a.name = ? AND l.num_reviews > 0
            ORDER BY l.num_reviews DESC
            LIMIT 5
        """, conn, params=[addon_name]).to_dict('records')
        
        addon_listings[addon_name] = listings_with_addon
    
    dashboard_data['addon_listings'] = addon_listings
    
    # 8. Top performers with all details
    print("8. Generating top performers...")
    top_performers = pd.read_sql_query("""
        SELECT 
            l.listing_id,
            l.url,
            l.rv_type,
            l.rv_year,
            l.rv_make,
            l.rv_model,
            l.base_price,
            l.num_reviews,
            CASE WHEN l.overall_rating >= 0 AND l.overall_rating <= 5 THEN l.overall_rating ELSE NULL END as overall_rating,
            l.length_ft,
            l.weight_lbs,
            l.sleeps,
            l.location_city,
            h.name as host_name,
            h.is_superhost,
            l.base_price * 120 as summer_revenue,
            l.base_price * 365 * 0.35 as annual_revenue_35pct
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.num_reviews >= 20
        AND l.base_price IS NOT NULL
        ORDER BY l.num_reviews DESC
        LIMIT 100
    """, conn).to_dict('records')
    
    dashboard_data['top_performers'] = top_performers
    
    # 9. Multi-owner analysis
    print("9. Generating multi-owner analysis...")
    # Use a CTE to ensure proper grouping
    multi_owners = pd.read_sql_query("""
        WITH host_listings AS (
            SELECT 
                h.host_id,
                h.name as host_name,
                h.is_superhost,
                l.listing_id,
                l.rv_type,
                l.base_price,
                l.num_reviews,
                l.overall_rating
            FROM hosts h
            JOIN listings l ON h.host_id = l.host_id
            WHERE l.base_price IS NOT NULL
        )
        SELECT 
            host_id,
            MAX(host_name) as host_name,
            MAX(is_superhost) as is_superhost,
            COUNT(DISTINCT listing_id) as num_rvs,
            GROUP_CONCAT(DISTINCT rv_type) as rv_types,
            COUNT(DISTINCT rv_type) as unique_types,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            SUM(base_price) as total_daily_revenue,
            SUM(num_reviews) as total_reviews,
            AVG(CASE WHEN overall_rating >= 0 AND overall_rating <= 5 THEN overall_rating END) as avg_rating
        FROM host_listings
        GROUP BY host_id
        HAVING COUNT(DISTINCT listing_id) >= 2
        ORDER BY num_rvs DESC, total_reviews DESC
        LIMIT 30
    """, conn).to_dict('records')
    
    # Get portfolios
    for owner in multi_owners:
        portfolio = pd.read_sql_query("""
            SELECT 
                url,
                rv_type,
                rv_year,
                rv_make,
                rv_model,
                base_price,
                num_reviews,
                overall_rating,
                sleeps,
                length_ft,
                weight_lbs
            FROM listings
            WHERE host_id = ?
            ORDER BY base_price DESC
        """, conn, params=[owner['host_id']]).to_dict('records')
        owner['portfolio'] = portfolio
    
    dashboard_data['multi_owners'] = multi_owners
    
    # 10. Your listing analysis
    print("10. Generating your listing analysis...")
    your_listing = pd.read_sql_query("""
        WITH your_price AS (SELECT 97 as price)
        SELECT 
            (SELECT COUNT(*) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price < 97) as cheaper_tt,
            (SELECT COUNT(*) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL) as total_tt,
            (SELECT COUNT(*) FROM listings WHERE base_price < 97) as cheaper_all,
            (SELECT COUNT(*) FROM listings WHERE base_price IS NOT NULL) as total_all,
            (SELECT AVG(base_price) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL) as avg_tt_price,
            (SELECT MIN(base_price) FROM listings WHERE rv_type = 'Tent Trailer' AND base_price IS NOT NULL) as min_tent_price,
            (SELECT AVG(base_price) FROM listings WHERE rv_type = 'Tent Trailer' AND base_price IS NOT NULL) as avg_tent_price,
            (SELECT AVG(num_reviews) FROM listings WHERE rv_type = 'Tent Trailer') as avg_tent_reviews
    """, conn).to_dict('records')[0]
    
    your_listing['your_price'] = 97
    your_listing['tt_percentile'] = (your_listing['cheaper_tt'] / your_listing['total_tt']) * 100
    your_listing['all_percentile'] = (your_listing['cheaper_all'] / your_listing['total_all']) * 100
    
    dashboard_data['your_listing'] = your_listing
    
    # 11. Price by specifications analysis (capacity, age, weight)
    print("11. Generating price by specifications analysis...")
    
    # Price by capacity
    price_by_capacity = pd.read_sql_query("""
        SELECT 
            sleeps,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            AVG(num_reviews) as avg_reviews,
            AVG(CASE WHEN overall_rating >= 0 AND overall_rating <= 5 THEN overall_rating END) as avg_rating
        FROM listings
        WHERE sleeps IS NOT NULL 
        AND base_price IS NOT NULL
        AND sleeps <= 12
        GROUP BY sleeps
        HAVING COUNT(*) >= 5
        ORDER BY sleeps
    """, conn).to_dict('records')
    
    # Price by age
    price_by_age = pd.read_sql_query("""
        SELECT 
            CASE 
                WHEN rv_year >= 2020 THEN '2020s (0-5 years)'
                WHEN rv_year >= 2015 THEN '2015-2019 (5-10 years)'
                WHEN rv_year >= 2010 THEN '2010-2014 (10-15 years)'
                WHEN rv_year >= 2005 THEN '2005-2009 (15-20 years)'
                WHEN rv_year >= 2000 THEN '2000-2004 (20-25 years)'
                ELSE 'Pre-2000 (25+ years)'
            END as age_group,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            AVG(num_reviews) as avg_reviews,
            AVG(CASE WHEN overall_rating >= 0 AND overall_rating <= 5 THEN overall_rating END) as avg_rating,
            MIN(rv_year) as oldest_year,
            MAX(rv_year) as newest_year
        FROM listings
        WHERE rv_year IS NOT NULL 
        AND rv_year >= 1990
        AND rv_year <= 2025
        AND base_price IS NOT NULL
        GROUP BY age_group
        HAVING COUNT(*) >= 5
        ORDER BY newest_year DESC
    """, conn).to_dict('records')
    
    # Price by weight
    price_by_weight = pd.read_sql_query("""
        SELECT 
            CASE 
                WHEN weight_lbs < 3000 THEN 'Ultra-light (<3000 lbs)'
                WHEN weight_lbs < 5000 THEN 'Light (3000-5000 lbs)'
                WHEN weight_lbs < 7000 THEN 'Medium (5000-7000 lbs)'
                WHEN weight_lbs < 10000 THEN 'Heavy (7000-10000 lbs)'
                ELSE 'Very Heavy (10000+ lbs)'
            END as weight_group,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            AVG(num_reviews) as avg_reviews,
            AVG(CASE WHEN overall_rating >= 0 AND overall_rating <= 5 THEN overall_rating END) as avg_rating,
            AVG(weight_lbs) as avg_weight
        FROM listings
        WHERE weight_lbs IS NOT NULL 
        AND weight_lbs >= 1000
        AND weight_lbs <= 30000
        AND base_price IS NOT NULL
        GROUP BY weight_group
        HAVING COUNT(*) >= 5
        ORDER BY avg_weight
    """, conn).to_dict('records')
    
    # Get spec analysis by RV type for filtering
    spec_by_rv_type = {}
    for rv_type in rv_types:
        spec_data = pd.read_sql_query("""
            SELECT 
                rv_type,
                COUNT(*) as count,
                AVG(base_price) as avg_price,
                AVG(CASE WHEN rv_year >= 1990 AND rv_year <= 2025 THEN 2025 - rv_year END) as avg_age,
                AVG(CASE WHEN weight_lbs >= 1000 AND weight_lbs <= 30000 THEN weight_lbs END) as avg_weight,
                AVG(sleeps) as avg_sleeps
            FROM listings
            WHERE rv_type = ?
            AND base_price IS NOT NULL
        """, conn, params=[rv_type]).to_dict('records')
        
        if spec_data and spec_data[0]['count'] > 0:
            spec_by_rv_type[rv_type] = spec_data[0]
    
    dashboard_data['price_by_specifications'] = {
        'by_capacity': price_by_capacity,
        'by_age': price_by_age,
        'by_weight': price_by_weight,
        'by_rv_type': spec_by_rv_type
    }
    
    # Save all data
    output_path = Path("/home/chris/rvezy/output/comprehensive_dashboard_data.json")
    
    # Convert NaN to None for JSON compatibility
    def clean_for_json(obj):
        if isinstance(obj, dict):
            return {k: clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_for_json(item) for item in obj]
        elif isinstance(obj, float) and np.isnan(obj):
            return None
        else:
            return obj
    
    cleaned_data = clean_for_json(dashboard_data)
    
    with open(output_path, 'w') as f:
        json.dump(cleaned_data, f, indent=2, default=str)
    
    print(f"\n✓ Comprehensive dashboard data generated: {output_path}")
    print(f"  - Market overview for all RV types")
    print(f"  - {len(all_listings)} listings with clickable data")
    print(f"  - Price analysis by specifications (capacity, age, weight)")
    print(f"  - Specifications analysis")
    print(f"  - Price threshold analysis with slider")
    print(f"  - Investment opportunities matrix")
    print(f"  - Add-ons analysis")
    print(f"  - Top 100 performers")
    print(f"  - Multi-owner portfolios")
    print(f"  - Your listing analysis")
    
    conn.close()
    return dashboard_data

if __name__ == "__main__":
    generate_comprehensive_dashboard_data()