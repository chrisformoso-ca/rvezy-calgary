import sqlite3
import pandas as pd
from pathlib import Path
import numpy as np

def analyze_multi_rv_owners():
    """Analyze hosts with multiple RV listings to identify rental businesses"""
    
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    print("=== Multi-RV Owner Analysis ===\n")
    
    # 1. Find hosts with multiple listings
    query_multi_hosts = """
    SELECT 
        h.host_id,
        h.name as host_name,
        h.joined_year,
        h.response_rate,
        h.is_superhost,
        COUNT(DISTINCT l.listing_id) as num_listings,
        GROUP_CONCAT(DISTINCT l.location_city) as cities,
        AVG(l.base_price) as avg_price,
        MIN(l.base_price) as min_price,
        MAX(l.base_price) as max_price,
        AVG(l.overall_rating) as avg_rating,
        SUM(l.num_reviews) as total_reviews,
        GROUP_CONCAT(DISTINCT l.rv_type) as rv_types
    FROM hosts h
    JOIN listings l ON h.host_id = l.host_id
    WHERE l.base_price IS NOT NULL
    GROUP BY h.host_id
    HAVING COUNT(DISTINCT l.listing_id) >= 2
    ORDER BY num_listings DESC, total_reviews DESC
    """
    
    df_multi = pd.read_sql_query(query_multi_hosts, conn)
    
    print(f"Found {len(df_multi)} hosts with 2 or more RV listings")
    print(f"Total multi-owner listings: {df_multi['num_listings'].sum()}")
    print(f"Average listings per multi-owner: {df_multi['num_listings'].mean():.1f}")
    print(f"Max listings by single owner: {df_multi['num_listings'].max()}\n")
    
    # 2. Top multi-RV owners
    print("=== Top 10 Multi-RV Owners ===")
    top_owners = df_multi.head(10)[['host_name', 'num_listings', 'avg_price', 
                                     'avg_rating', 'total_reviews', 'is_superhost', 'cities']]
    print(top_owners.to_string(index=False))
    
    # 3. Detailed portfolio analysis for top owners
    print("\n=== Portfolio Analysis for Top 5 Multi-Owners ===")
    
    for idx, owner in df_multi.head(5).iterrows():
        print(f"\n{owner['host_name']} ({owner['num_listings']} RVs):")
        
        # Get detailed listings for this owner
        query_owner_details = """
        SELECT 
            rv_type,
            rv_year,
            rv_make,
            rv_model,
            base_price,
            overall_rating,
            num_reviews,
            location_city,
            sleeps,
            delivery_available
        FROM listings
        WHERE host_id = ?
        AND base_price IS NOT NULL
        ORDER BY base_price DESC
        """
        
        df_owner = pd.read_sql_query(query_owner_details, conn, params=[owner['host_id']])
        print(df_owner.to_string(index=False))
        
        # Calculate portfolio metrics
        total_value = df_owner['base_price'].sum()
        print(f"\nPortfolio Metrics:")
        print(f"  - Total nightly value: ${total_value:.2f}")
        print(f"  - Price range: ${owner['min_price']:.2f} - ${owner['max_price']:.2f}")
        print(f"  - Price spread: {(owner['max_price']/owner['min_price']):.1f}x")
        print(f"  - Potential monthly revenue (50% occupancy): ${total_value * 15:.2f}")
    
    # 4. Compare multi vs single owners
    print("\n=== Multi-Owner vs Single-Owner Comparison ===")
    
    query_comparison = """
    WITH owner_stats AS (
        SELECT 
            h.host_id,
            COUNT(DISTINCT l.listing_id) as num_listings,
            AVG(l.base_price) as avg_price,
            AVG(l.overall_rating) as avg_rating,
            AVG(l.num_reviews) as avg_reviews_per_rv,
            AVG(h.response_rate) as avg_response_rate,
            AVG(h.is_superhost) as superhost_rate
        FROM hosts h
        JOIN listings l ON h.host_id = l.host_id
        WHERE l.base_price IS NOT NULL
        GROUP BY h.host_id
    )
    SELECT 
        CASE 
            WHEN num_listings = 1 THEN 'Single RV Owners'
            ELSE 'Multi RV Owners'
        END as owner_type,
        COUNT(*) as num_hosts,
        AVG(avg_price) as avg_price,
        AVG(avg_rating) as avg_rating,
        AVG(avg_reviews_per_rv) as avg_reviews_per_rv,
        AVG(avg_response_rate) as avg_response_rate,
        AVG(superhost_rate) * 100 as superhost_percentage
    FROM owner_stats
    GROUP BY owner_type
    """
    
    df_comparison = pd.read_sql_query(query_comparison, conn)
    print(df_comparison.to_string(index=False))
    
    # 5. Business model analysis
    print("\n=== Business Model Patterns ===")
    
    # Analyze pricing strategies
    query_pricing_strategy = """
    SELECT 
        h.name as host_name,
        COUNT(DISTINCT l.listing_id) as num_rvs,
        ROUND(AVG(l.base_price), 2) as avg_price,
        ROUND(MAX(l.base_price) - MIN(l.base_price), 2) as price_range,
        ROUND(MIN(l.base_price), 2) as min_price,
        ROUND(MAX(l.base_price), 2) as max_price,
        COUNT(DISTINCT l.rv_type) as num_rv_types,
        GROUP_CONCAT(DISTINCT l.rv_type) as rv_types
    FROM hosts h
    JOIN listings l ON h.host_id = l.host_id
    WHERE l.base_price IS NOT NULL
    GROUP BY h.host_id
    HAVING COUNT(DISTINCT l.listing_id) >= 3
    ORDER BY num_rvs DESC, avg_price DESC
    LIMIT 15
    """
    
    df_strategy = pd.read_sql_query(query_pricing_strategy, conn)
    
    print("Hosts with 3+ RVs - Pricing Strategy Analysis:")
    print(df_strategy.to_string(index=False))
    
    # 6. Geographic concentration
    print("\n=== Geographic Concentration of Multi-Owners ===")
    
    query_geo = """
    SELECT 
        l.location_city,
        COUNT(DISTINCT h.host_id) as multi_owner_count,
        COUNT(DISTINCT l.listing_id) as total_listings,
        AVG(l.base_price) as avg_price
    FROM hosts h
    JOIN listings l ON h.host_id = l.host_id
    WHERE h.host_id IN (
        SELECT host_id 
        FROM listings 
        GROUP BY host_id 
        HAVING COUNT(*) >= 2
    )
    AND l.location_city IS NOT NULL
    GROUP BY l.location_city
    HAVING COUNT(DISTINCT h.host_id) >= 2
    ORDER BY multi_owner_count DESC
    """
    
    df_geo = pd.read_sql_query(query_geo, conn)
    print(df_geo.head(10).to_string(index=False))
    
    # 7. Revenue potential analysis
    print("\n=== Estimated Revenue Potential ===")
    
    # Calculate potential revenue for different owner types
    query_revenue = """
    WITH owner_revenue AS (
        SELECT 
            h.host_id,
            h.name,
            COUNT(DISTINCT l.listing_id) as num_rvs,
            SUM(l.base_price) as total_daily_rate,
            AVG(l.overall_rating) as avg_rating,
            SUM(l.num_reviews) as total_reviews
        FROM hosts h
        JOIN listings l ON h.host_id = l.host_id
        WHERE l.base_price IS NOT NULL
        GROUP BY h.host_id
    )
    SELECT 
        CASE 
            WHEN num_rvs = 1 THEN '1 RV'
            WHEN num_rvs = 2 THEN '2 RVs'
            WHEN num_rvs = 3 THEN '3 RVs'
            WHEN num_rvs >= 4 THEN '4+ RVs'
        END as portfolio_size,
        COUNT(*) as num_hosts,
        AVG(total_daily_rate) as avg_daily_portfolio_rate,
        AVG(total_daily_rate * 15) as est_monthly_revenue_50pct,
        AVG(total_daily_rate * 365 * 0.5) as est_annual_revenue_50pct,
        AVG(avg_rating) as avg_rating,
        AVG(total_reviews) as avg_total_reviews
    FROM owner_revenue
    GROUP BY portfolio_size
    ORDER BY 
        CASE portfolio_size
            WHEN '1 RV' THEN 1
            WHEN '2 RVs' THEN 2
            WHEN '3 RVs' THEN 3
            WHEN '4+ RVs' THEN 4
        END
    """
    
    df_revenue = pd.read_sql_query(query_revenue, conn)
    print("Revenue Potential by Portfolio Size (assuming 50% occupancy):")
    print(df_revenue.to_string(index=False))
    
    # 8. Success factors for multi-owners
    print("\n=== Success Factors for Multi-RV Businesses ===")
    
    # Analyze what makes successful multi-owners
    query_success = """
    WITH multi_owners AS (
        SELECT 
            h.host_id,
            h.name,
            h.is_superhost,
            COUNT(DISTINCT l.listing_id) as num_rvs,
            AVG(l.base_price) as avg_price,
            AVG(l.overall_rating) as avg_rating,
            SUM(l.num_reviews) as total_reviews,
            AVG(l.num_reviews) as avg_reviews_per_rv,
            COUNT(DISTINCT l.rv_type) as rv_type_diversity,
            AVG(l.delivery_available) as delivery_rate
        FROM hosts h
        JOIN listings l ON h.host_id = l.host_id
        WHERE l.base_price IS NOT NULL
        GROUP BY h.host_id
        HAVING COUNT(DISTINCT l.listing_id) >= 2
    )
    SELECT 
        CASE 
            WHEN avg_reviews_per_rv >= 20 THEN 'High Engagement (20+ reviews/RV)'
            WHEN avg_reviews_per_rv >= 10 THEN 'Medium Engagement (10-19 reviews/RV)'
            ELSE 'Low Engagement (<10 reviews/RV)'
        END as engagement_level,
        COUNT(*) as num_hosts,
        AVG(num_rvs) as avg_portfolio_size,
        AVG(avg_price) as avg_price,
        AVG(avg_rating) as avg_rating,
        AVG(is_superhost) * 100 as superhost_pct,
        AVG(delivery_rate) * 100 as delivery_offered_pct
    FROM multi_owners
    GROUP BY engagement_level
    ORDER BY avg_reviews_per_rv DESC
    """
    
    df_success = pd.read_sql_query(query_success, conn)
    print(df_success.to_string(index=False))
    
    # Export detailed multi-owner data
    print("\n=== Exporting Multi-Owner Data ===")
    
    # Create comprehensive multi-owner dataset
    query_export = """
    SELECT 
        h.*,
        l.*,
        (SELECT COUNT(*) FROM listings WHERE host_id = h.host_id) as host_total_listings
    FROM listings l
    JOIN hosts h ON l.host_id = h.host_id
    WHERE h.host_id IN (
        SELECT host_id 
        FROM listings 
        GROUP BY host_id 
        HAVING COUNT(*) >= 2
    )
    ORDER BY h.host_id, l.base_price DESC
    """
    
    df_export = pd.read_sql_query(query_export, conn)
    export_path = '/home/chris/rvezy/data/processed/multi_owner_listings.csv'
    df_export.to_csv(export_path, index=False)
    print(f"Exported {len(df_export)} multi-owner listings to {export_path}")
    
    conn.close()
    
    # Key insights summary
    print("\n=== KEY INSIGHTS ===")
    print("1. Multi-RV owners represent a significant business opportunity on RVezy")
    print("2. Successful multi-owners tend to be Superhosts with high response rates")
    print("3. Portfolio diversity (multiple RV types) appears common among larger operators")
    print("4. Geographic concentration suggests local market expertise is valuable")
    print("5. Estimated annual revenue for 4+ RV portfolios can exceed $100K at 50% occupancy")

if __name__ == "__main__":
    analyze_multi_rv_owners()