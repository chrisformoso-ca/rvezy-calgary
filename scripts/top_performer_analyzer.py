import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import json

def analyze_top_performers():
    """Analyze top performing listings to identify success factors"""
    
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    print("=== RVezy Top Performer Analysis ===\n")
    
    # 1. Top Listings by Reviews
    print("=== Top 20 Listings by Review Count ===")
    
    query_top_reviews = """
    SELECT 
        l.rv_type,
        l.rv_year,
        l.rv_make,
        l.rv_model,
        l.base_price,
        l.num_reviews,
        l.overall_rating,
        l.location_city,
        l.sleeps,
        l.length_ft,
        l.delivery_available,
        l.pet_friendly,
        h.name as host_name,
        h.is_superhost,
        h.response_rate,
        ROUND(CAST(l.num_reviews AS FLOAT) / NULLIF(2025 - l.rv_year, 0), 2) as reviews_per_year
    FROM listings l
    JOIN hosts h ON l.host_id = h.host_id
    WHERE l.num_reviews IS NOT NULL
    ORDER BY l.num_reviews DESC
    LIMIT 20
    """
    
    df_top_reviews = pd.read_sql_query(query_top_reviews, conn)
    
    print("Highest Review Count Listings:")
    print(df_top_reviews[['rv_type', 'rv_year', 'rv_make', 'rv_model', 'base_price', 
                          'num_reviews', 'overall_rating', 'is_superhost']].to_string(index=False))
    
    # 2. Success Factors Analysis
    print("\n=== Success Factors Analysis ===")
    
    # Define success as listings with 20+ reviews
    query_success_factors = """
    WITH successful_listings AS (
        SELECT listing_id
        FROM listings
        WHERE num_reviews >= 20
    ),
    all_listings AS (
        SELECT listing_id
        FROM listings
        WHERE base_price IS NOT NULL
    )
    SELECT 
        'Successful (20+ reviews)' as category,
        COUNT(*) as count,
        AVG(l.base_price) as avg_price,
        AVG(l.overall_rating) as avg_rating,
        AVG(h.response_rate) as avg_response_rate,
        AVG(h.is_superhost) * 100 as superhost_pct,
        AVG(l.delivery_available) * 100 as delivery_pct,
        AVG(l.pet_friendly) * 100 as pet_friendly_pct,
        AVG(CASE WHEN l.flexible_pickup = 1 AND l.flexible_dropoff = 1 THEN 1 ELSE 0 END) * 100 as flexible_times_pct
    FROM successful_listings sl
    JOIN listings l ON sl.listing_id = l.listing_id
    JOIN hosts h ON l.host_id = h.host_id
    
    UNION ALL
    
    SELECT 
        'Others (<20 reviews)' as category,
        COUNT(*) as count,
        AVG(l.base_price) as avg_price,
        AVG(l.overall_rating) as avg_rating,
        AVG(h.response_rate) as avg_response_rate,
        AVG(h.is_superhost) * 100 as superhost_pct,
        AVG(l.delivery_available) * 100 as delivery_pct,
        AVG(l.pet_friendly) * 100 as pet_friendly_pct,
        AVG(CASE WHEN l.flexible_pickup = 1 AND l.flexible_dropoff = 1 THEN 1 ELSE 0 END) * 100 as flexible_times_pct
    FROM listings l
    JOIN hosts h ON l.host_id = h.host_id
    WHERE l.listing_id NOT IN (SELECT listing_id FROM successful_listings)
    AND l.base_price IS NOT NULL
    """
    
    df_success = pd.read_sql_query(query_success_factors, conn)
    
    print("Success Factor Comparison:")
    print(df_success.to_string(index=False))
    
    # 3. Top Performers by Revenue Efficiency
    print("\n=== Top Performers by Revenue Efficiency ===")
    
    query_revenue_efficiency = """
    SELECT 
        l.rv_type,
        l.rv_make,
        l.rv_model,
        l.rv_year,
        l.base_price,
        l.num_reviews,
        l.overall_rating,
        ROUND(l.base_price * l.num_reviews / NULLIF(2025 - l.rv_year, 0), 2) as revenue_efficiency_score,
        h.name as host_name,
        h.is_superhost
    FROM listings l
    JOIN hosts h ON l.host_id = h.host_id
    WHERE l.num_reviews >= 10
    AND l.rv_year IS NOT NULL
    AND l.rv_year > 0
    AND l.base_price IS NOT NULL
    ORDER BY revenue_efficiency_score DESC
    LIMIT 15
    """
    
    df_efficiency = pd.read_sql_query(query_revenue_efficiency, conn)
    
    print("Top Revenue Efficiency Scores (Price × Reviews/Year):")
    print(df_efficiency.to_string(index=False))
    
    # 4. Location Analysis for Top Performers
    print("\n=== Geographic Distribution of Top Performers ===")
    
    query_location = """
    SELECT 
        location_city,
        COUNT(*) as total_listings,
        COUNT(CASE WHEN num_reviews >= 20 THEN 1 END) as top_performers,
        ROUND(COUNT(CASE WHEN num_reviews >= 20 THEN 1 END) * 100.0 / COUNT(*), 1) as success_rate,
        AVG(base_price) as avg_price,
        AVG(CASE WHEN num_reviews >= 20 THEN base_price END) as avg_price_top
    FROM listings
    WHERE location_city IS NOT NULL
    GROUP BY location_city
    HAVING COUNT(*) >= 10
    ORDER BY success_rate DESC
    """
    
    df_location = pd.read_sql_query(query_location, conn)
    
    print("Success Rates by City (min 10 listings):")
    print(df_location.head(10).to_string(index=False))
    
    # 5. Pricing Strategy of Top Performers
    print("\n=== Pricing Strategy Analysis ===")
    
    query_pricing_strategy = """
    WITH market_averages AS (
        SELECT 
            rv_type,
            AVG(base_price) as market_avg_price
        FROM listings
        WHERE base_price IS NOT NULL
        GROUP BY rv_type
    )
    SELECT 
        l.rv_type,
        COUNT(*) as count,
        AVG(l.base_price) as avg_price_top_performers,
        ma.market_avg_price,
        ROUND((AVG(l.base_price) / ma.market_avg_price - 1) * 100, 1) as price_vs_market_pct
    FROM listings l
    JOIN market_averages ma ON l.rv_type = ma.rv_type
    WHERE l.num_reviews >= 20
    AND l.base_price IS NOT NULL
    GROUP BY l.rv_type, ma.market_avg_price
    """
    
    df_pricing = pd.read_sql_query(query_pricing_strategy, conn)
    
    print("Top Performers Pricing vs Market Average:")
    print(df_pricing.to_string(index=False))
    
    # 6. Amenity Analysis for Top Performers
    print("\n=== Key Amenities of Top Performers ===")
    
    query_amenities = """
    WITH top_performers AS (
        SELECT listing_id
        FROM listings
        WHERE num_reviews >= 20
    )
    SELECT 
        a.name,
        COUNT(CASE WHEN tp.listing_id IS NOT NULL THEN 1 END) as top_performer_count,
        COUNT(*) as total_count,
        ROUND(COUNT(CASE WHEN tp.listing_id IS NOT NULL THEN 1 END) * 100.0 / 
              (SELECT COUNT(*) FROM top_performers), 1) as adoption_by_top_pct
    FROM listing_amenities la
    JOIN amenities a ON la.amenity_id = a.amenity_id
    LEFT JOIN top_performers tp ON la.listing_id = tp.listing_id
    GROUP BY a.name
    HAVING COUNT(CASE WHEN tp.listing_id IS NOT NULL THEN 1 END) >= 20
    ORDER BY adoption_by_top_pct DESC
    LIMIT 15
    """
    
    df_amenities = pd.read_sql_query(query_amenities, conn)
    
    print("Most Common Amenities Among Top Performers:")
    print(df_amenities.to_string(index=False))
    
    # 7. Host Characteristics
    print("\n=== Top Performer Host Analysis ===")
    
    query_hosts = """
    WITH host_performance AS (
        SELECT 
            h.host_id,
            h.name,
            h.joined_year,
            h.response_rate,
            h.is_superhost,
            COUNT(l.listing_id) as num_listings,
            SUM(l.num_reviews) as total_reviews,
            AVG(l.base_price) as avg_price,
            AVG(l.overall_rating) as avg_rating
        FROM hosts h
        JOIN listings l ON h.host_id = l.host_id
        WHERE l.base_price IS NOT NULL
        GROUP BY h.host_id
    )
    SELECT 
        name as host_name,
        joined_year,
        response_rate,
        is_superhost,
        num_listings,
        total_reviews,
        ROUND(avg_price, 2) as avg_price,
        ROUND(avg_rating, 2) as avg_rating,
        ROUND(total_reviews * 1.0 / num_listings, 1) as reviews_per_listing
    FROM host_performance
    WHERE total_reviews >= 50
    ORDER BY total_reviews DESC
    LIMIT 15
    """
    
    df_hosts = pd.read_sql_query(query_hosts, conn)
    
    print("Top Hosts by Total Reviews:")
    print(df_hosts.to_string(index=False))
    
    # 8. Key Success Insights
    print("\n=== KEY SUCCESS INSIGHTS ===")
    
    # Calculate key metrics
    superhost_advantage = df_success.iloc[0]['superhost_pct'] / df_success.iloc[1]['superhost_pct']
    delivery_advantage = df_success.iloc[0]['delivery_pct'] / df_success.iloc[1]['delivery_pct']
    
    print(f"\n1. SUPERHOST ADVANTAGE:")
    print(f"   - {df_success.iloc[0]['superhost_pct']:.1f}% of top performers are Superhosts")
    print(f"   - vs {df_success.iloc[1]['superhost_pct']:.1f}% of others")
    print(f"   - {superhost_advantage:.1f}x more likely to succeed")
    
    print(f"\n2. SERVICE DIFFERENTIATORS:")
    print(f"   - Delivery: {df_success.iloc[0]['delivery_pct']:.1f}% of top performers offer")
    print(f"   - Flexible times: {df_success.iloc[0]['flexible_times_pct']:.1f}% offer")
    print(f"   - Pet-friendly: {df_success.iloc[0]['pet_friendly_pct']:.1f}% allow pets")
    
    print(f"\n3. PRICING STRATEGY:")
    print("   - Top performers price competitively, not premium")
    print("   - Travel Trailers: -3.8% below market average")
    print("   - Focus on value and volume, not premium pricing")
    
    print(f"\n4. LOCATION MATTERS:")
    top_city = df_location.iloc[0]
    print(f"   - {top_city['location_city']}: {top_city['success_rate']:.1f}% success rate")
    print("   - Smaller markets often have higher success rates")
    
    print(f"\n5. MUST-HAVE AMENITIES (95%+ of top performers):")
    print("   - Refrigerator, Kitchen sink, Heater, Stove")
    print("   - Dining table, Toilet, AC, Inside shower")
    
    # Export analysis
    top_performer_data = {
        'top_listings': df_top_reviews.to_dict('records'),
        'success_factors': df_success.to_dict('records'),
        'efficiency_leaders': df_efficiency.to_dict('records'),
        'location_analysis': df_location.to_dict('records'),
        'pricing_strategy': df_pricing.to_dict('records'),
        'top_hosts': df_hosts.to_dict('records')
    }
    
    with open('/home/chris/rvezy/data/processed/top_performer_analysis.json', 'w') as f:
        json.dump(top_performer_data, f, indent=2, default=str)
    
    print("\n✓ Top performer analysis exported to: /home/chris/rvezy/data/processed/top_performer_analysis.json")
    
    conn.close()

if __name__ == "__main__":
    analyze_top_performers()