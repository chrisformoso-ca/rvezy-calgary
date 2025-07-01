import sqlite3
import pandas as pd
import json
from pathlib import Path
import numpy as np

def generate_dashboard_data():
    """Generate comprehensive JSON data for the dashboard"""
    
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    dashboard_data = {}
    
    print("Generating dashboard data...")
    
    # 1. Market Overview
    print("1. Generating market overview...")
    market_overview = pd.read_sql_query("""
        SELECT 
            COUNT(*) as total_listings,
            COUNT(CASE WHEN location_city = 'Calgary' THEN 1 END) as calgary_listings,
            COUNT(DISTINCT host_id) as total_hosts,
            COUNT(DISTINCT CASE WHEN host_id IN (
                SELECT host_id FROM listings GROUP BY host_id HAVING COUNT(*) >= 2
            ) THEN host_id END) as multi_owners,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price
        FROM listings
        WHERE base_price IS NOT NULL
    """, conn).to_dict('records')[0]
    
    dashboard_data['market_overview'] = market_overview
    
    # 2. RV Type Distribution
    print("2. Generating RV type distribution...")
    rv_types = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            AVG(num_reviews) as avg_reviews,
            SUM(num_reviews) as total_reviews
        FROM listings
        WHERE rv_type IS NOT NULL
        AND base_price IS NOT NULL
        GROUP BY rv_type
        ORDER BY count DESC
    """, conn).to_dict('records')
    
    dashboard_data['rv_types'] = rv_types
    
    # 3. Top Listings with URLs
    print("3. Generating top listings...")
    top_listings = pd.read_sql_query("""
        SELECT 
            l.url,
            l.rv_type,
            l.rv_year,
            l.rv_make,
            l.rv_model,
            l.base_price,
            l.num_reviews,
            l.overall_rating,
            l.location_city,
            CASE 
                WHEN l.sleeps > 20 THEN l.sleeps % 100
                ELSE l.sleeps 
            END as sleeps,
            h.name as host_name,
            h.is_superhost,
            ROUND(l.base_price * 120, 0) as summer_revenue_potential
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.num_reviews IS NOT NULL
        AND l.num_reviews > 0
        ORDER BY l.num_reviews DESC
        LIMIT 15
    """, conn).to_dict('records')
    
    dashboard_data['top_listings'] = top_listings
    
    # 4. Multi-Owner Analysis with Portfolio Details
    print("4. Generating multi-owner analysis...")
    multi_owners = pd.read_sql_query("""
        SELECT 
            h.host_id,
            h.name as host_name,
            h.is_superhost,
            h.response_rate,
            COUNT(l.listing_id) as num_rvs,
            GROUP_CONCAT(l.rv_type) as rv_types,
            AVG(l.base_price) as avg_price,
            SUM(l.base_price) as total_daily_revenue,
            AVG(l.overall_rating) as avg_rating,
            SUM(l.num_reviews) as total_reviews,
            AVG(2025 - l.rv_year) as avg_fleet_age,
            MIN(l.rv_year) as oldest_year,
            MAX(l.rv_year) as newest_year,
            COUNT(DISTINCT l.rv_type) as unique_rv_types,
            MIN(l.base_price) as min_price,
            MAX(l.base_price) as max_price
        FROM hosts h
        JOIN listings l ON h.host_id = l.host_id
        WHERE l.base_price IS NOT NULL
        GROUP BY h.host_id
        HAVING COUNT(l.listing_id) >= 2
        ORDER BY num_rvs DESC, total_reviews DESC
        LIMIT 20
    """, conn).to_dict('records')
    
    # Get detailed portfolio for each multi-owner
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
                location_city
            FROM listings
            WHERE host_id = ?
            ORDER BY base_price DESC
        """, conn, params=[owner['host_id']]).to_dict('records')
        owner['portfolio'] = portfolio
        
        # Analyze strategy
        if owner['unique_rv_types'] == 1:
            owner['strategy'] = 'Specialist'
        elif owner['max_price'] - owner['min_price'] < 50:
            owner['strategy'] = 'Standardized Fleet'
        elif owner['avg_price'] < 150:
            owner['strategy'] = 'Budget Focus'
        elif owner['avg_price'] > 250:
            owner['strategy'] = 'Premium Focus'
        else:
            owner['strategy'] = 'Diversified'
    
    dashboard_data['multi_owners'] = multi_owners
    
    # 5. Add-Ons Comprehensive Analysis
    print("5. Generating add-ons analysis...")
    addons = pd.read_sql_query("""
        SELECT 
            a.name,
            COUNT(*) as listings_count,
            MIN(CASE WHEN a.price > 0 AND a.price < 500 THEN a.price END) as min_price,
            AVG(CASE WHEN a.price > 0 AND a.price < 500 THEN a.price END) as avg_price,
            MAX(CASE WHEN a.price < 500 THEN a.price END) as max_price,
            GROUP_CONCAT(DISTINCT l.rv_type) as rv_types_offering
        FROM addons a
        JOIN listings l ON a.listing_id = l.listing_id
        WHERE a.name != '' AND a.name IS NOT NULL
        GROUP BY a.name
        HAVING COUNT(*) >= 5
        ORDER BY listings_count DESC
    """, conn)
    
    # Calculate median prices
    for addon_name in addons['name'].unique():
        prices = pd.read_sql_query("""
            SELECT price 
            FROM addons 
            WHERE name = ? AND price > 0 AND price < 500
        """, conn, params=[addon_name])['price']
        
        median_price = prices.median() if len(prices) > 0 else 0
        addons.loc[addons['name'] == addon_name, 'median_price'] = median_price
    
    dashboard_data['addons'] = addons.to_dict('records')
    
    # 6. Revenue by Sleeping Capacity
    print("6. Generating revenue by sleeping capacity...")
    revenue_by_sleeps = pd.read_sql_query("""
        WITH clean_sleeps AS (
            SELECT 
                CASE 
                    WHEN sleeps > 20 THEN sleeps % 100
                    ELSE sleeps 
                END as sleeps_clean,
                base_price,
                num_reviews
            FROM listings
            WHERE sleeps IS NOT NULL 
            AND base_price IS NOT NULL
        )
        SELECT 
            sleeps_clean as sleeps,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            AVG(COALESCE(num_reviews, 0)) as avg_reviews,
            AVG(base_price * 120) as avg_summer_revenue
        FROM clean_sleeps
        WHERE sleeps_clean BETWEEN 1 AND 20
        GROUP BY sleeps_clean
        ORDER BY sleeps_clean
    """, conn).to_dict('records')
    
    dashboard_data['revenue_by_sleeps'] = revenue_by_sleeps
    
    # 7. Winter-Ready Premium Analysis
    print("7. Generating winter-ready analysis...")
    winter_analysis = pd.read_sql_query("""
        WITH winter_ready AS (
            SELECT DISTINCT l.listing_id, l.rv_type, l.base_price
            FROM listings l
            JOIN listing_amenities la ON l.listing_id = la.listing_id
            JOIN amenities a ON la.amenity_id = a.amenity_id
            WHERE a.name = 'Full-Winter rental available'
        )
        SELECT 
            l.rv_type,
            COUNT(DISTINCT l.listing_id) as total_count,
            COUNT(DISTINCT wr.listing_id) as winter_ready_count,
            AVG(CASE WHEN wr.listing_id IS NOT NULL THEN l.base_price END) as winter_avg_price,
            AVG(CASE WHEN wr.listing_id IS NULL THEN l.base_price END) as regular_avg_price,
            AVG(CASE WHEN wr.listing_id IS NOT NULL THEN l.base_price END) - 
                AVG(CASE WHEN wr.listing_id IS NULL THEN l.base_price END) as dollar_premium
        FROM listings l
        LEFT JOIN winter_ready wr ON l.listing_id = wr.listing_id
        WHERE l.rv_type IS NOT NULL
        AND l.base_price IS NOT NULL
        GROUP BY l.rv_type
    """, conn).to_dict('records')
    
    # Calculate percentage premiums
    for row in winter_analysis:
        if row['regular_avg_price'] and row['winter_avg_price']:
            row['percent_premium'] = ((row['winter_avg_price'] / row['regular_avg_price']) - 1) * 100
        else:
            row['percent_premium'] = 0
    
    dashboard_data['winter_analysis'] = winter_analysis
    
    # 8. Price Tier Requirements
    print("8. Generating price tier requirements...")
    tier_requirements = pd.read_sql_query("""
        WITH price_tiers AS (
            SELECT 
                listing_id,
                CASE 
                    WHEN base_price < 125 THEN 'Budget'
                    WHEN base_price < 175 THEN 'Mid-Range'
                    WHEN base_price < 250 THEN 'Upper-Mid'
                    ELSE 'Premium'
                END as tier
            FROM listings
            WHERE base_price IS NOT NULL
        ),
        essential_amenities AS (
            SELECT 'Refrigerator' as amenity
            UNION SELECT 'Kitchen sink'
            UNION SELECT 'Heater'
            UNION SELECT 'Toilet'
            UNION SELECT 'Air conditioner'
            UNION SELECT 'Inside shower'
            UNION SELECT 'Microwave'
            UNION SELECT 'TV & DVD'
            UNION SELECT 'Backup camera'
            UNION SELECT 'Solar'
        )
        SELECT 
            pt.tier,
            ea.amenity,
            COUNT(DISTINCT CASE WHEN a.name = ea.amenity THEN la.listing_id END) * 100.0 / 
                COUNT(DISTINCT pt.listing_id) as adoption_rate
        FROM price_tiers pt
        CROSS JOIN essential_amenities ea
        LEFT JOIN listing_amenities la ON pt.listing_id = la.listing_id
        LEFT JOIN amenities a ON la.amenity_id = a.amenity_id AND a.name = ea.amenity
        GROUP BY pt.tier, ea.amenity
        HAVING adoption_rate >= 80
        ORDER BY pt.tier, adoption_rate DESC
    """, conn)
    
    # Restructure for dashboard
    tier_req_dict = {}
    for _, row in tier_requirements.iterrows():
        tier = row['tier']
        if tier not in tier_req_dict:
            tier_req_dict[tier] = []
        tier_req_dict[tier].append({
            'amenity': row['amenity'],
            'adoption_rate': round(row['adoption_rate'], 0)
        })
    
    dashboard_data['tier_requirements'] = tier_req_dict
    
    # 9. Top RVs by Revenue Potential
    print("9. Generating top RVs by revenue...")
    top_rvs_by_revenue = pd.read_sql_query("""
        SELECT 
            l.url,
            l.rv_type,
            l.rv_year,
            l.rv_make,
            l.rv_model,
            l.base_price,
            l.num_reviews,
            l.overall_rating,
            l.sleeps,
            h.name as host_name,
            h.is_superhost,
            ROUND(l.base_price * 120, 0) as summer_revenue,
            ROUND(l.base_price * 365 * 0.35, 0) as annual_revenue_35pct
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.base_price IS NOT NULL
        AND l.rv_make IS NOT NULL
        ORDER BY l.base_price DESC
        LIMIT 50
    """, conn).to_dict('records')
    
    dashboard_data['top_rvs_by_revenue'] = top_rvs_by_revenue
    
    # 10. Your Listing Analysis (assuming $97/night Travel Trailer)
    print("10. Generating your listing analysis...")
    your_listing = pd.read_sql_query("""
        WITH your_price AS (SELECT 97 as price)
        SELECT 
            (SELECT COUNT(*) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price < 97) as cheaper_count,
            (SELECT COUNT(*) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL) as total_tt_count,
            (SELECT AVG(base_price) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL) as avg_tt_price,
            (SELECT MIN(base_price) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL) as min_tt_price,
            (SELECT MAX(base_price) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL) as max_tt_price,
            (SELECT COUNT(*) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL AND location_city = 'Calgary') as calgary_tt_count,
            (SELECT AVG(base_price) FROM listings WHERE rv_type = 'Travel Trailer' AND base_price IS NOT NULL AND location_city = 'Calgary') as calgary_avg_price,
            (SELECT COUNT(*) FROM listings WHERE base_price < 97) as cheaper_all_types,
            (SELECT COUNT(*) FROM listings WHERE base_price IS NOT NULL) as total_all_types
    """, conn).to_dict('records')[0]
    
    your_listing['your_price'] = 97
    your_listing['percentile'] = (your_listing['cheaper_count'] / your_listing['total_tt_count']) * 100
    your_listing['percentile_all_rvs'] = (your_listing['cheaper_all_types'] / your_listing['total_all_types']) * 100
    your_listing['revenue_increase_at_avg'] = ((your_listing['avg_tt_price'] / 97) - 1) * 100
    
    dashboard_data['your_listing'] = your_listing
    
    # 11. Budget Segment Analysis
    print("11. Generating budget segment analysis...")
    budget_segment = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(*) as count,
            AVG(base_price) as avg_price,
            MIN(base_price) as min_price,
            MAX(base_price) as max_price,
            AVG(num_reviews) as avg_reviews,
            SUM(num_reviews) as total_reviews,
            AVG(CASE WHEN num_reviews > 0 THEN overall_rating END) as avg_rating
        FROM listings
        WHERE base_price <= 125
        AND base_price IS NOT NULL
        GROUP BY rv_type
        HAVING COUNT(*) >= 3
        ORDER BY avg_reviews DESC
    """, conn).to_dict('records')
    
    dashboard_data['budget_segment'] = budget_segment
    
    # 12. Competitive Intelligence
    print("12. Generating competitive intelligence...")
    
    # Tent Trailer operators
    tent_trailer_operators = pd.read_sql_query("""
        SELECT 
            h.name as host_name,
            h.is_superhost,
            COUNT(l.listing_id) as num_listings,
            AVG(l.base_price) as avg_price,
            SUM(l.num_reviews) as total_reviews,
            AVG(l.overall_rating) as avg_rating
        FROM hosts h
        JOIN listings l ON h.host_id = l.host_id
        WHERE l.rv_type = 'Tent Trailer'
        GROUP BY h.host_id
        ORDER BY total_reviews DESC
        LIMIT 5
    """, conn).to_dict('records')
    
    # Success factors by segment
    success_factors = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(CASE WHEN num_reviews >= 20 THEN 1 END) as high_performers,
            COUNT(*) as total,
            ROUND(COUNT(CASE WHEN num_reviews >= 20 THEN 1 END) * 100.0 / COUNT(*), 1) as success_rate,
            AVG(CASE WHEN num_reviews >= 20 THEN base_price END) as successful_avg_price,
            AVG(base_price) as overall_avg_price
        FROM listings
        WHERE base_price IS NOT NULL
        GROUP BY rv_type
        HAVING COUNT(*) >= 5
        ORDER BY success_rate DESC
    """, conn).to_dict('records')
    
    dashboard_data['competitive_intelligence'] = {
        'tent_trailer_operators': tent_trailer_operators,
        'success_factors': success_factors
    }
    
    # 13. Investment Opportunities
    print("13. Generating investment opportunities...")
    investment_opportunities = pd.read_sql_query("""
        SELECT 
            rv_type,
            COUNT(*) as market_size,
            AVG(base_price) as avg_price,
            AVG(num_reviews) as avg_demand,
            COUNT(CASE WHEN num_reviews >= 20 THEN 1 END) * 100.0 / COUNT(*) as success_rate,
            AVG(base_price * 120) as summer_revenue_potential,
            AVG(base_price * 365 * 0.35) as annual_revenue_35pct,
            MIN(base_price) as entry_price,
            COUNT(CASE WHEN h.is_superhost = 1 THEN 1 END) * 100.0 / COUNT(*) as superhost_rate
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.base_price IS NOT NULL
        GROUP BY rv_type
        HAVING COUNT(*) >= 5
        ORDER BY avg_demand DESC
    """, conn).to_dict('records')
    
    dashboard_data['investment_opportunities'] = investment_opportunities
    
    # Save all data
    output_path = Path("/home/chris/rvezy/output/dashboard_data.json")
    
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
    
    print(f"\n✓ Dashboard data generated: {output_path}")
    print(f"  - Market overview")
    print(f"  - RV type distribution") 
    print(f"  - Top 15 listings with URLs")
    print(f"  - Top 20 multi-owners with strategy analysis")
    print(f"  - Comprehensive add-ons analysis")
    print(f"  - Revenue by sleeping capacity")
    print(f"  - Winter-ready premium analysis")
    print(f"  - Price tier amenity requirements")
    print(f"  - Top 50 RVs by revenue potential")
    print(f"  - Your listing analysis with percentiles")
    print(f"  - Budget segment analysis")
    print(f"  - Competitive intelligence")
    print(f"  - Investment opportunities by segment")
    
    conn.close()
    return dashboard_data

if __name__ == "__main__":
    generate_dashboard_data()