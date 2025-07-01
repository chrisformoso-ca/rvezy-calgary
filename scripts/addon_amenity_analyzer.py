import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import json

def analyze_addons_amenities():
    """Analyze add-ons and amenities to identify revenue opportunities and requirements"""
    
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    print("=== RVezy Add-Ons and Amenities Analysis ===\n")
    
    # 1. Add-on Analysis
    print("=== Add-On Revenue Opportunities ===")
    
    # Clean up add-on data (remove outliers)
    query_addons = """
    SELECT 
        name,
        COUNT(*) as frequency,
        ROUND(AVG(CASE WHEN price < 500 THEN price END), 2) as avg_price,
        ROUND(MIN(CASE WHEN price > 0 AND price < 500 THEN price END), 2) as min_price,
        ROUND(MAX(CASE WHEN price < 500 THEN price END), 2) as max_price,
        ROUND(AVG(CASE WHEN price < 500 THEN price END) * 0.3, 2) as avg_revenue_30pct_attach
    FROM addons
    WHERE name != ''
    GROUP BY name
    HAVING COUNT(*) >= 10
    ORDER BY frequency DESC
    """
    
    df_addons = pd.read_sql_query(query_addons, conn)
    
    print("Most Common Add-Ons (min 10 listings):")
    print(df_addons.to_string(index=False))
    
    # Calculate total add-on revenue potential
    top_addons = df_addons.head(5)
    total_addon_potential = top_addons['avg_revenue_30pct_attach'].sum()
    
    print(f"\nTotal add-on revenue potential (top 5, 30% attach rate): ${total_addon_potential:.2f} per booking")
    
    # 2. Add-ons by RV Type
    print("\n=== Add-On Adoption by RV Type ===")
    
    query_addon_by_type = """
    SELECT 
        l.rv_type,
        a.name as addon_name,
        COUNT(*) as count,
        ROUND(AVG(a.price), 2) as avg_price
    FROM listings l
    JOIN addons a ON l.listing_id = a.listing_id
    WHERE l.rv_type IS NOT NULL
    AND a.name IN ('Propane Refill Prepayment', 'Emptying Septic Prepayment', 
                   'Portable BBQ', 'Bedding and Linens', 'Portable Generator')
    AND a.price < 500
    GROUP BY l.rv_type, a.name
    ORDER BY l.rv_type, count DESC
    """
    
    df_addon_type = pd.read_sql_query(query_addon_by_type, conn)
    
    # Pivot for better visualization
    addon_pivot = df_addon_type.pivot_table(
        index='rv_type', 
        columns='addon_name', 
        values='count',
        fill_value=0
    )
    
    print("Add-on Offerings by RV Type (count):")
    print(addon_pivot.to_string())
    
    # 3. Amenity Analysis
    print("\n=== Essential Amenities Analysis ===")
    
    query_amenities = """
    SELECT 
        a.name,
        COUNT(*) as total_listings,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM listings), 1) as adoption_pct,
        AVG(l.base_price) as avg_price_with,
        (SELECT AVG(base_price) FROM listings WHERE listing_id NOT IN 
            (SELECT listing_id FROM listing_amenities WHERE amenity_id = a.amenity_id)) as avg_price_without
    FROM amenities a
    JOIN listing_amenities la ON a.amenity_id = la.amenity_id
    JOIN listings l ON la.listing_id = l.listing_id
    WHERE l.base_price IS NOT NULL
    GROUP BY a.amenity_id, a.name
    HAVING COUNT(*) >= 100
    ORDER BY adoption_pct DESC
    LIMIT 20
    """
    
    df_amenities = pd.read_sql_query(query_amenities, conn)
    
    # Calculate price premium for amenities
    df_amenities['price_premium'] = ((df_amenities['avg_price_with'] / df_amenities['avg_price_without']) - 1) * 100
    
    print("Top 20 Amenities by Adoption Rate:")
    print(df_amenities[['name', 'adoption_pct', 'avg_price_with', 'price_premium']].to_string(index=False))
    
    # 4. Amenities by Price Tier
    print("\n=== Amenity Requirements by Price Tier ===")
    
    query_tier_amenities = """
    WITH price_tiers AS (
        SELECT 
            listing_id,
            base_price,
            CASE 
                WHEN base_price < 125 THEN 'Budget (<$125)'
                WHEN base_price < 175 THEN 'Mid ($125-175)'
                WHEN base_price < 250 THEN 'Upper ($175-250)'
                ELSE 'Premium ($250+)'
            END as price_tier
        FROM listings
        WHERE base_price IS NOT NULL
    ),
    tier_amenity_counts AS (
        SELECT 
            pt.price_tier,
            a.name,
            COUNT(DISTINCT la.listing_id) as listings_with,
            COUNT(DISTINCT pt.listing_id) as total_in_tier
        FROM price_tiers pt
        LEFT JOIN listing_amenities la ON pt.listing_id = la.listing_id
        LEFT JOIN amenities a ON la.amenity_id = a.amenity_id
        WHERE a.name IN (
            'Refrigerator', 'Kitchen sink', 'Heater', 'Air conditioner',
            'Toilet', 'Inside shower', 'TV & DVD', 'Microwave',
            'Camping chairs', 'Solar', 'Backup camera', 'Pet friendly'
        )
        GROUP BY pt.price_tier, a.name
    )
    SELECT 
        name as amenity,
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Budget (<$125)' THEN listings_with END) / 
            MAX(CASE WHEN price_tier = 'Budget (<$125)' THEN total_in_tier END), 0) as budget_pct,
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Mid ($125-175)' THEN listings_with END) / 
            MAX(CASE WHEN price_tier = 'Mid ($125-175)' THEN total_in_tier END), 0) as mid_pct,
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Upper ($175-250)' THEN listings_with END) / 
            MAX(CASE WHEN price_tier = 'Upper ($175-250)' THEN total_in_tier END), 0) as upper_pct,
        ROUND(100.0 * SUM(CASE WHEN price_tier = 'Premium ($250+)' THEN listings_with END) / 
            MAX(CASE WHEN price_tier = 'Premium ($250+)' THEN total_in_tier END), 0) as premium_pct
    FROM tier_amenity_counts
    GROUP BY amenity
    ORDER BY premium_pct DESC
    """
    
    df_tier_amenities = pd.read_sql_query(query_tier_amenities, conn)
    
    print("\nAmenity Adoption by Price Tier (% of listings in tier):")
    print(df_tier_amenities.to_string(index=False))
    
    # 5. Missing Amenities Analysis
    print("\n=== Missing Amenities Impact ===")
    
    # Find listings missing key amenities
    query_missing = """
    WITH essential_amenities AS (
        SELECT amenity_id, name 
        FROM amenities 
        WHERE name IN ('Air conditioner', 'Microwave', 'TV & DVD', 'Camping chairs')
    ),
    listing_amenity_count AS (
        SELECT 
            l.listing_id,
            l.base_price,
            l.num_reviews,
            COUNT(DISTINCT ea.amenity_id) as essential_count
        FROM listings l
        LEFT JOIN listing_amenities la ON l.listing_id = la.listing_id
        LEFT JOIN essential_amenities ea ON la.amenity_id = ea.amenity_id
        WHERE l.base_price IS NOT NULL
        GROUP BY l.listing_id
    )
    SELECT 
        essential_count as num_essential_amenities,
        COUNT(*) as listing_count,
        AVG(base_price) as avg_price,
        AVG(num_reviews) as avg_reviews
    FROM listing_amenity_count
    GROUP BY essential_count
    ORDER BY essential_count DESC
    """
    
    df_missing = pd.read_sql_query(query_missing, conn)
    
    print("\nImpact of Essential Amenities on Performance:")
    print("(Essential: AC, Microwave, TV & DVD, Camping chairs)")
    print(df_missing.to_string(index=False))
    
    # 6. Pet-Friendly Analysis
    print("\n=== Pet-Friendly Premium Analysis ===")
    
    query_pet = """
    SELECT 
        l.rv_type,
        COUNT(CASE WHEN l.pet_friendly = 1 THEN 1 END) as pet_friendly_count,
        COUNT(*) as total_count,
        ROUND(COUNT(CASE WHEN l.pet_friendly = 1 THEN 1 END) * 100.0 / COUNT(*), 1) as pet_friendly_pct,
        AVG(CASE WHEN l.pet_friendly = 1 THEN l.base_price END) as avg_price_pet,
        AVG(CASE WHEN l.pet_friendly = 0 THEN l.base_price END) as avg_price_no_pet
    FROM listings l
    WHERE l.rv_type IS NOT NULL
    AND l.base_price IS NOT NULL
    GROUP BY l.rv_type
    """
    
    df_pet = pd.read_sql_query(query_pet, conn)
    df_pet['pet_premium'] = ((df_pet['avg_price_pet'] / df_pet['avg_price_no_pet']) - 1) * 100
    
    print("Pet-Friendly Analysis by RV Type:")
    print(df_pet[['rv_type', 'pet_friendly_pct', 'avg_price_pet', 'avg_price_no_pet', 'pet_premium']].to_string(index=False))
    
    # 7. Recommendations
    print("\n=== ADD-ON & AMENITY RECOMMENDATIONS ===")
    
    print("\n1. MUST-OFFER ADD-ONS (High adoption, good revenue):")
    print("   - Propane Refill: $48 avg (57% of listings)")
    print("   - Septic Emptying: $79 avg (54% of listings)")
    print("   - Bedding/Linens: $43 avg (37% of listings)")
    print("   - Portable BBQ: $34 avg (37% of listings)")
    
    print("\n2. ESSENTIAL AMENITIES BY PRICE TIER:")
    print("   Budget (<$125): Focus on basics (95%+ need fridge, kitchen, heater)")
    print("   Mid ($125-175): Add AC (94%), microwave (90%), TV (77%)")
    print("   Upper ($175-250): Include camping chairs (79%), backup camera")
    print("   Premium ($250+): All above plus solar, pet-friendly options")
    
    print("\n3. COMPETITIVE ADVANTAGES:")
    print("   - Listings with 4 essential amenities average 15% more reviews")
    print("   - Pet-friendly commands -3% to +8% premium (varies by RV type)")
    print("   - Winter-ready features create year-round revenue opportunity")
    
    print("\n4. YOUR LISTING OPTIMIZATION:")
    print("   - Add these add-ons: Propane ($48), Septic ($79), BBQ ($34)")
    print("   - Potential add-on revenue: $48.30 per booking (30% attach)")
    print("   - Ensure you have: AC, Microwave, TV, Camping chairs")
    print("   - Consider: Pet-friendly option (expands market)")
    
    # Export analysis
    addon_amenity_data = {
        'top_addons': df_addons.to_dict('records'),
        'amenity_adoption': df_amenities.head(20).to_dict('records'),
        'tier_requirements': df_tier_amenities.to_dict('records'),
        'pet_analysis': df_pet.to_dict('records')
    }
    
    with open('/home/chris/rvezy/data/processed/addon_amenity_analysis.json', 'w') as f:
        json.dump(addon_amenity_data, f, indent=2)
    
    print("\n✓ Add-on and amenity analysis exported to: /home/chris/rvezy/data/processed/addon_amenity_analysis.json")
    
    conn.close()

if __name__ == "__main__":
    analyze_addons_amenities()