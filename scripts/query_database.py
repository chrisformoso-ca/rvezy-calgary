import sqlite3
import pandas as pd
from pathlib import Path

def query_database():
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    # Query 1: Travel Trailers in Calgary similar to user's listing
    print("=== Travel Trailers in Calgary (similar to your listing) ===")
    query1 = """
    SELECT 
        l.rv_year,
        l.rv_make,
        l.rv_model,
        l.sleeps,
        l.length_ft,
        l.base_price,
        l.overall_rating,
        l.num_reviews,
        p_weekly.discount_percent as weekly_discount,
        p_monthly.discount_percent as monthly_discount
    FROM listings l
    LEFT JOIN pricing p_weekly ON l.listing_id = p_weekly.listing_id AND p_weekly.discount_type = 'weekly'
    LEFT JOIN pricing p_monthly ON l.listing_id = p_monthly.listing_id AND p_monthly.discount_type = 'monthly'
    WHERE l.rv_type = 'Travel Trailer' 
    AND l.location_city = 'Calgary'
    AND l.base_price IS NOT NULL
    ORDER BY l.base_price
    """
    
    df1 = pd.read_sql_query(query1, conn)
    print(f"Found {len(df1)} Travel Trailers in Calgary")
    print(f"Price range: ${df1['base_price'].min():.2f} - ${df1['base_price'].max():.2f}")
    print(f"Average price: ${df1['base_price'].mean():.2f}")
    print(f"Your price of $97/night is at the {(df1['base_price'] <= 97).sum() / len(df1) * 100:.1f}th percentile")
    
    # Query 2: Price comparison by RV type
    print("\n=== Average Prices by RV Type in Calgary ===")
    query2 = """
    SELECT 
        rv_type,
        COUNT(*) as count,
        AVG(base_price) as avg_price,
        MIN(base_price) as min_price,
        MAX(base_price) as max_price,
        AVG(overall_rating) as avg_rating
    FROM listings
    WHERE location_city = 'Calgary'
    AND rv_type IS NOT NULL
    AND base_price IS NOT NULL
    GROUP BY rv_type
    ORDER BY avg_price DESC
    """
    
    df2 = pd.read_sql_query(query2, conn)
    print(df2.to_string(index=False))
    
    # Query 3: Most common amenities
    print("\n=== Most Common Amenities ===")
    query3 = """
    SELECT 
        a.name,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM listings), 1) as percentage
    FROM listing_amenities la
    JOIN amenities a ON la.amenity_id = a.amenity_id
    GROUP BY a.name
    ORDER BY count DESC
    LIMIT 15
    """
    
    df3 = pd.read_sql_query(query3, conn)
    print(df3.to_string(index=False))
    
    # Query 4: Delivery services
    print("\n=== Delivery Service Analysis ===")
    query4 = """
    SELECT 
        rv_type,
        COUNT(*) as total_listings,
        SUM(delivery_available) as with_delivery,
        ROUND(AVG(delivery_available) * 100, 1) as delivery_percentage,
        AVG(delivery_price_per_km) as avg_delivery_price_per_km
    FROM listings
    WHERE location_city = 'Calgary'
    AND rv_type IS NOT NULL
    GROUP BY rv_type
    """
    
    df4 = pd.read_sql_query(query4, conn)
    print(df4.to_string(index=False))
    
    # Query 5: High performing listings
    print("\n=== Top Performing Listings (by reviews) ===")
    query5 = """
    SELECT 
        rv_type,
        rv_year,
        rv_make,
        rv_model,
        base_price,
        overall_rating,
        num_reviews,
        location_city
    FROM listings
    WHERE num_reviews >= 10
    AND overall_rating >= 4.8
    ORDER BY num_reviews DESC
    LIMIT 10
    """
    
    df5 = pd.read_sql_query(query5, conn)
    print(df5.to_string(index=False))
    
    # Export key data to CSV for further analysis
    print("\n=== Exporting data to CSV files ===")
    
    # Export all Calgary listings
    query_export = """
    SELECT 
        l.*,
        h.name as host_name,
        h.response_rate,
        h.is_superhost
    FROM listings l
    LEFT JOIN hosts h ON l.host_id = h.host_id
    WHERE l.location_city = 'Calgary'
    """
    
    df_export = pd.read_sql_query(query_export, conn)
    df_export.to_csv('/home/chris/rvezy/data/processed/calgary_listings.csv', index=False)
    print(f"Exported {len(df_export)} Calgary listings to calgary_listings.csv")
    
    conn.close()

if __name__ == "__main__":
    query_database()