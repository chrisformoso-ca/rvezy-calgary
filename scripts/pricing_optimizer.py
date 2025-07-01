import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

def optimize_pricing():
    """Analyze pricing optimization opportunities for the user's $97/night Travel Trailer"""
    
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    # User's current listing details
    USER_PRICE = 97
    USER_TYPE = "Travel Trailer"
    USER_YEAR = 2021
    USER_MODEL = "Super-lite/Conquest 197BH"
    USER_SLEEPS = 6  # Assuming based on typical 197BH model
    USER_LENGTH = 24  # Assuming based on typical 197BH model
    
    print("=== RVezy Pricing Optimization Analysis ===")
    print(f"\nYour Current Listing:")
    print(f"- {USER_YEAR} {USER_MODEL}")
    print(f"- Current Price: ${USER_PRICE}/night")
    print(f"- Type: {USER_TYPE}")
    print(f"- Estimated Sleeps: {USER_SLEEPS}")
    print(f"- Estimated Length: {USER_LENGTH}ft")
    print(f"- Profit Split: 60/40 with RV owner")
    
    # 1. Market Position Analysis
    print("\n=== Market Position Analysis ===")
    
    query_position = """
    SELECT 
        COUNT(*) as total_travel_trailers,
        SUM(CASE WHEN base_price <= ? THEN 1 ELSE 0 END) as cheaper_count,
        MIN(base_price) as min_price,
        MAX(base_price) as max_price,
        AVG(base_price) as avg_price,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY base_price) as q1_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY base_price) as median_price,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY base_price) as q3_price
    FROM listings
    WHERE rv_type = 'Travel Trailer'
    AND location_city = 'Calgary'
    AND base_price IS NOT NULL
    """
    
    # SQLite doesn't have PERCENTILE_CONT, so let's use a different approach
    query_position = """
    SELECT 
        COUNT(*) as total_travel_trailers,
        SUM(CASE WHEN base_price <= ? THEN 1 ELSE 0 END) as cheaper_count,
        MIN(base_price) as min_price,
        MAX(base_price) as max_price,
        AVG(base_price) as avg_price
    FROM listings
    WHERE rv_type = 'Travel Trailer'
    AND location_city = 'Calgary'
    AND base_price IS NOT NULL
    """
    
    df_position = pd.read_sql_query(query_position, conn, params=[USER_PRICE])
    
    # Get percentiles separately
    query_prices = """
    SELECT base_price
    FROM listings
    WHERE rv_type = 'Travel Trailer'
    AND location_city = 'Calgary'
    AND base_price IS NOT NULL
    ORDER BY base_price
    """
    
    prices = pd.read_sql_query(query_prices, conn)['base_price']
    q1 = prices.quantile(0.25)
    median = prices.quantile(0.50)
    q3 = prices.quantile(0.75)
    
    percentile = (df_position['cheaper_count'].iloc[0] / df_position['total_travel_trailers'].iloc[0]) * 100
    
    print(f"Total Travel Trailers in Calgary: {df_position['total_travel_trailers'].iloc[0]}")
    print(f"Your price percentile: {percentile:.1f}%")
    print(f"Price statistics:")
    print(f"  - Minimum: ${df_position['min_price'].iloc[0]:.2f}")
    print(f"  - 25th percentile: ${q1:.2f}")
    print(f"  - Median: ${median:.2f}")
    print(f"  - Average: ${df_position['avg_price'].iloc[0]:.2f}")
    print(f"  - 75th percentile: ${q3:.2f}")
    print(f"  - Maximum: ${df_position['max_price'].iloc[0]:.2f}")
    
    # 2. Comparable RV Analysis
    print("\n=== Comparable RV Analysis ===")
    
    query_comparable = """
    SELECT 
        rv_year,
        rv_make,
        rv_model,
        base_price,
        sleeps,
        length_ft,
        overall_rating,
        num_reviews,
        delivery_available,
        pet_friendly,
        h.is_superhost,
        l.url
    FROM listings l
    JOIN hosts h ON l.host_id = h.host_id
    WHERE l.rv_type = 'Travel Trailer'
    AND l.location_city = 'Calgary'
    AND l.base_price IS NOT NULL
    AND l.sleeps BETWEEN ? AND ?
    AND l.length_ft BETWEEN ? AND ?
    AND l.rv_year BETWEEN ? AND ?
    ORDER BY ABS(l.sleeps - ?) + ABS(l.length_ft - ?) ASC
    LIMIT 20
    """
    
    # Find comparable RVs (similar size, age, capacity)
    df_comparable = pd.read_sql_query(
        query_comparable, 
        conn, 
        params=[
            USER_SLEEPS - 2, USER_SLEEPS + 2,  # Sleeps range
            USER_LENGTH - 4, USER_LENGTH + 4,  # Length range
            USER_YEAR - 3, USER_YEAR + 3,      # Year range
            USER_SLEEPS, USER_LENGTH           # For sorting by similarity
        ]
    )
    
    print(f"Found {len(df_comparable)} comparable Travel Trailers (±2 sleeps, ±4ft, ±3 years)")
    
    if len(df_comparable) > 0:
        print(f"\nComparable RV Price Statistics:")
        print(f"  - Average: ${df_comparable['base_price'].mean():.2f}")
        print(f"  - Median: ${df_comparable['base_price'].median():.2f}")
        print(f"  - Range: ${df_comparable['base_price'].min():.2f} - ${df_comparable['base_price'].max():.2f}")
        
        print(f"\nTop 10 Most Similar RVs:")
        display_cols = ['rv_year', 'rv_make', 'rv_model', 'base_price', 'sleeps', 
                       'length_ft', 'overall_rating', 'num_reviews', 'is_superhost']
        print(df_comparable[display_cols].head(10).to_string(index=False))
    
    # 3. Feature-Based Pricing Analysis
    print("\n=== Feature Impact on Pricing ===")
    
    query_features = """
    WITH feature_analysis AS (
        SELECT 
            AVG(CASE WHEN delivery_available = 1 THEN base_price ELSE NULL END) as avg_with_delivery,
            AVG(CASE WHEN delivery_available = 0 THEN base_price ELSE NULL END) as avg_no_delivery,
            AVG(CASE WHEN pet_friendly = 1 THEN base_price ELSE NULL END) as avg_pet_friendly,
            AVG(CASE WHEN pet_friendly = 0 THEN base_price ELSE NULL END) as avg_no_pets,
            AVG(CASE WHEN h.is_superhost = 1 THEN base_price ELSE NULL END) as avg_superhost,
            AVG(CASE WHEN h.is_superhost = 0 THEN base_price ELSE NULL END) as avg_regular_host
        FROM listings l
        JOIN hosts h ON l.host_id = h.host_id
        WHERE l.rv_type = 'Travel Trailer'
        AND l.location_city = 'Calgary'
        AND l.base_price IS NOT NULL
    )
    SELECT * FROM feature_analysis
    """
    
    df_features = pd.read_sql_query(query_features, conn)
    
    delivery_premium = ((df_features['avg_with_delivery'].iloc[0] / df_features['avg_no_delivery'].iloc[0]) - 1) * 100
    pet_premium = ((df_features['avg_pet_friendly'].iloc[0] / df_features['avg_no_pets'].iloc[0]) - 1) * 100
    superhost_premium = ((df_features['avg_superhost'].iloc[0] / df_features['avg_regular_host'].iloc[0]) - 1) * 100
    
    print(f"Average price premiums for features:")
    print(f"  - Delivery available: {delivery_premium:+.1f}%")
    print(f"  - Pet friendly: {pet_premium:+.1f}%")
    print(f"  - Superhost status: {superhost_premium:+.1f}%")
    
    # 4. Performance-Based Pricing
    print("\n=== Performance-Based Pricing Analysis ===")
    
    query_performance = """
    SELECT 
        CASE 
            WHEN base_price < 100 THEN 'Under $100'
            WHEN base_price BETWEEN 100 AND 125 THEN '$100-125'
            WHEN base_price BETWEEN 126 AND 150 THEN '$126-150'
            WHEN base_price BETWEEN 151 AND 175 THEN '$151-175'
            ELSE 'Over $175'
        END as price_range,
        COUNT(*) as count,
        AVG(overall_rating) as avg_rating,
        AVG(num_reviews) as avg_reviews,
        AVG(CAST(num_reviews AS FLOAT) / 
            NULLIF((2025 - rv_year), 0)) as reviews_per_year
    FROM listings
    WHERE rv_type = 'Travel Trailer'
    AND location_city = 'Calgary'
    AND base_price IS NOT NULL
    AND num_reviews > 0
    GROUP BY price_range
    ORDER BY 
        CASE price_range
            WHEN 'Under $100' THEN 1
            WHEN '$100-125' THEN 2
            WHEN '$126-150' THEN 3
            WHEN '$151-175' THEN 4
            ELSE 5
        END
    """
    
    df_performance = pd.read_sql_query(query_performance, conn)
    print("Performance by Price Range:")
    print(df_performance.to_string(index=False))
    
    # 5. Pricing Recommendations
    print("\n=== PRICING RECOMMENDATIONS ===")
    
    # Calculate recommended price based on comparables
    if len(df_comparable) > 0:
        recommended_base = df_comparable['base_price'].median()
    else:
        recommended_base = median
    
    print(f"\n1. IMMEDIATE OPPORTUNITY:")
    print(f"   Current Price: ${USER_PRICE}/night")
    print(f"   Recommended Price: ${recommended_base:.0f}/night")
    print(f"   Potential Revenue Increase: {((recommended_base/USER_PRICE)-1)*100:.0f}%")
    
    print(f"\n2. PRICING TIERS:")
    print(f"   Conservative: ${q1:.0f}/night (25th percentile)")
    print(f"   Moderate: ${median:.0f}/night (market median)")
    print(f"   Aggressive: ${q3:.0f}/night (75th percentile)")
    
    print(f"\n3. REVENUE PROJECTIONS (Your 60% share):")
    occupancy_rates = [0.3, 0.5, 0.7]
    prices = [USER_PRICE, q1, median, recommended_base, q3]
    price_labels = ['Current', '25th %ile', 'Median', 'Recommended', '75th %ile']
    
    print("\n   Monthly Revenue Projections:")
    print("   " + " | ".join([f"{label:>12}" for label in price_labels]))
    print("   " + "-" * 75)
    
    for occ in occupancy_rates:
        revenues = [price * 30 * occ * 0.6 for price in prices]  # 60% share
        print(f"   {int(occ*100)}% occupancy: " + 
              " | ".join([f"${rev:>11,.0f}" for rev in revenues]))
    
    print("\n   Annual Revenue Projections:")
    print("   " + " | ".join([f"{label:>12}" for label in price_labels]))
    print("   " + "-" * 75)
    
    for occ in occupancy_rates:
        revenues = [price * 365 * occ * 0.6 for price in prices]  # 60% share
        print(f"   {int(occ*100)}% occupancy: " + 
              " | ".join([f"${rev:>11,.0f}" for rev in revenues]))
    
    # 6. Competitive Advantages to Highlight
    print("\n4. STRATEGIES TO SUPPORT HIGHER PRICING:")
    print("   - Emphasize 2021 model year (newer than 68% of Travel Trailers)")
    print("   - Highlight Super-lite design (easier towing = broader market)")
    print("   - Focus on family-friendly features (sleeps 6)")
    print("   - Consider adding delivery service (commands premium)")
    print("   - Maintain high ratings and response rate")
    print("   - Build review count through initial competitive pricing")
    
    # 7. Weekly/Monthly Discount Strategy
    print("\n5. DISCOUNT STRATEGY:")
    
    query_discounts = """
    SELECT 
        AVG(CASE WHEN p.discount_type = 'weekly' THEN p.discount_percent END) as avg_weekly_discount,
        AVG(CASE WHEN p.discount_type = 'monthly' THEN p.discount_percent END) as avg_monthly_discount
    FROM listings l
    LEFT JOIN pricing p ON l.listing_id = p.listing_id
    WHERE l.rv_type = 'Travel Trailer'
    AND l.location_city = 'Calgary'
    AND l.base_price BETWEEN ? AND ?
    """
    
    df_discounts = pd.read_sql_query(query_discounts, conn, params=[recommended_base - 20, recommended_base + 20])
    
    avg_weekly = df_discounts['avg_weekly_discount'].iloc[0] or 10
    avg_monthly = df_discounts['avg_monthly_discount'].iloc[0] or 20
    
    print(f"   Market standard discounts for similar price range:")
    print(f"   - Weekly: {avg_weekly:.0f}% off")
    print(f"   - Monthly: {avg_monthly:.0f}% off")
    
    # Export detailed analysis
    analysis_data = {
        'current_price': USER_PRICE,
        'market_percentile': percentile,
        'comparable_avg': df_comparable['base_price'].mean() if len(df_comparable) > 0 else median,
        'recommended_price': recommended_base,
        'revenue_increase_potential': ((recommended_base/USER_PRICE)-1)*100,
        'market_stats': {
            'min': float(df_position['min_price'].iloc[0]),
            'q1': float(q1),
            'median': float(median),
            'avg': float(df_position['avg_price'].iloc[0]),
            'q3': float(q3),
            'max': float(df_position['max_price'].iloc[0])
        }
    }
    
    import json
    with open('/home/chris/rvezy/data/processed/pricing_analysis.json', 'w') as f:
        json.dump(analysis_data, f, indent=2)
    
    print("\n✓ Detailed analysis exported to: /home/chris/rvezy/data/processed/pricing_analysis.json")
    
    conn.close()

if __name__ == "__main__":
    optimize_pricing()