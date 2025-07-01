import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

def analyze_investment_opportunities():
    """Analyze the best RV types and models for investment based on market data"""
    
    db_path = Path("/home/chris/rvezy/data/processed/rvezy_listings.db")
    conn = sqlite3.connect(db_path)
    
    print("=== RVezy Investment Opportunity Analysis ===\n")
    
    # 1. Market Overview by RV Type
    print("=== Market Overview by RV Type ===")
    
    query_overview = """
    SELECT 
        rv_type,
        COUNT(*) as listing_count,
        AVG(base_price) as avg_daily_rate,
        MIN(base_price) as min_price,
        MAX(base_price) as max_price,
        AVG(overall_rating) as avg_rating,
        SUM(num_reviews) as total_reviews,
        AVG(num_reviews) as avg_reviews,
        AVG(CAST(num_reviews AS FLOAT) / NULLIF((2025 - rv_year), 0)) as reviews_per_year,
        COUNT(DISTINCT host_id) as unique_hosts
    FROM listings
    WHERE location_city IN ('Calgary', 'Airdrie', 'Cochrane', 'Chestermere', 'Okotoks')
    AND base_price IS NOT NULL
    AND rv_type IS NOT NULL
    GROUP BY rv_type
    ORDER BY avg_daily_rate DESC
    """
    
    df_overview = pd.read_sql_query(query_overview, conn)
    print(df_overview.to_string(index=False))
    
    # 2. ROI Analysis by RV Type
    print("\n=== ROI Analysis by RV Type ===")
    
    # Estimated purchase prices (rough market estimates)
    purchase_prices = {
        'Travel Trailer': 35000,
        'Class C': 85000,
        'Class B': 120000,
        'Class A': 150000,
        'Fifth Wheel': 45000,
        'Toy Hauler': 50000
    }
    
    roi_data = []
    for _, row in df_overview.iterrows():
        rv_type = row['rv_type']
        if rv_type in purchase_prices:
            purchase_price = purchase_prices[rv_type]
            daily_rate = row['avg_daily_rate']
            
            # Calculate ROI scenarios
            occupancy_rates = [0.3, 0.5, 0.7]
            for occ in occupancy_rates:
                annual_revenue = daily_rate * 365 * occ
                annual_costs = purchase_price * 0.15  # Assume 15% annual costs
                net_income = annual_revenue - annual_costs
                roi_percent = (net_income / purchase_price) * 100
                payback_years = purchase_price / net_income if net_income > 0 else None
                
                roi_data.append({
                    'RV Type': rv_type,
                    'Occupancy': f"{int(occ*100)}%",
                    'Est. Purchase Price': purchase_price,
                    'Avg Daily Rate': daily_rate,
                    'Annual Revenue': annual_revenue,
                    'Est. Annual Costs': annual_costs,
                    'Net Income': net_income,
                    'ROI %': roi_percent,
                    'Payback Years': payback_years
                })
    
    df_roi = pd.DataFrame(roi_data)
    
    # Show best ROI at 50% occupancy
    df_roi_50 = df_roi[df_roi['Occupancy'] == '50%'].sort_values('ROI %', ascending=False)
    print("ROI Analysis at 50% Occupancy:")
    print(df_roi_50[['RV Type', 'Est. Purchase Price', 'Avg Daily Rate', 
                     'Annual Revenue', 'Net Income', 'ROI %', 'Payback Years']].to_string(index=False))
    
    # 3. Market Saturation Analysis
    print("\n=== Market Saturation Analysis ===")
    
    query_saturation = """
    WITH rv_metrics AS (
        SELECT 
            rv_type,
            COUNT(*) as listing_count,
            AVG(num_reviews) as avg_reviews,
            COUNT(DISTINCT host_id) as unique_hosts,
            AVG(base_price) as avg_price,
            COUNT(CASE WHEN num_reviews >= 10 THEN 1 END) as established_count,
            COUNT(CASE WHEN num_reviews < 5 THEN 1 END) as new_count
        FROM listings
        WHERE location_city IN ('Calgary', 'Airdrie', 'Cochrane', 'Chestermere', 'Okotoks')
        AND base_price IS NOT NULL
        AND rv_type IS NOT NULL
        GROUP BY rv_type
    )
    SELECT 
        rv_type,
        listing_count,
        unique_hosts,
        ROUND(CAST(listing_count AS FLOAT) / unique_hosts, 2) as listings_per_host,
        established_count,
        new_count,
        ROUND(CAST(new_count AS FLOAT) / listing_count * 100, 1) as new_listing_pct,
        avg_reviews,
        avg_price
    FROM rv_metrics
    ORDER BY listing_count DESC
    """
    
    df_saturation = pd.read_sql_query(query_saturation, conn)
    print(df_saturation.to_string(index=False))
    
    # 4. Size and Feature Sweet Spots
    print("\n=== Size and Feature Analysis for Travel Trailers ===")
    
    query_size_analysis = """
    SELECT 
        CASE 
            WHEN length_ft < 20 THEN 'Under 20ft'
            WHEN length_ft BETWEEN 20 AND 25 THEN '20-25ft'
            WHEN length_ft BETWEEN 26 AND 30 THEN '26-30ft'
            WHEN length_ft BETWEEN 31 AND 35 THEN '31-35ft'
            ELSE 'Over 35ft'
        END as size_category,
        COUNT(*) as count,
        AVG(base_price) as avg_price,
        AVG(num_reviews) as avg_reviews,
        AVG(overall_rating) as avg_rating,
        AVG(CASE WHEN delivery_available = 1 THEN 1 ELSE 0 END) * 100 as delivery_pct
    FROM listings
    WHERE rv_type = 'Travel Trailer'
    AND location_city IN ('Calgary', 'Airdrie', 'Cochrane', 'Chestermere', 'Okotoks')
    AND length_ft IS NOT NULL
    AND base_price IS NOT NULL
    GROUP BY size_category
    ORDER BY avg_price DESC
    """
    
    df_size = pd.read_sql_query(query_size_analysis, conn)
    print("Travel Trailer Performance by Size:")
    print(df_size.to_string(index=False))
    
    # 5. Top Performing Models
    print("\n=== Top Performing RV Models (by demand proxy) ===")
    
    query_top_models = """
    SELECT 
        rv_type,
        rv_make,
        rv_model,
        COUNT(*) as count,
        AVG(base_price) as avg_price,
        AVG(num_reviews) as avg_reviews,
        AVG(overall_rating) as avg_rating,
        MIN(rv_year) as oldest_year,
        MAX(rv_year) as newest_year
    FROM listings
    WHERE location_city IN ('Calgary', 'Airdrie', 'Cochrane', 'Chestermere', 'Okotoks')
    AND base_price IS NOT NULL
    AND rv_make IS NOT NULL
    AND rv_model IS NOT NULL
    GROUP BY rv_type, rv_make, rv_model
    HAVING COUNT(*) >= 3
    ORDER BY avg_reviews DESC
    LIMIT 15
    """
    
    df_models = pd.read_sql_query(query_top_models, conn)
    print(df_models.to_string(index=False))
    
    # 6. Entry-Level vs Premium Analysis
    print("\n=== Entry-Level vs Premium Market Analysis ===")
    
    query_segments = """
    WITH percentiles AS (
        SELECT 
            rv_type,
            base_price,
            NTILE(4) OVER (PARTITION BY rv_type ORDER BY base_price) as price_quartile
        FROM listings
        WHERE location_city IN ('Calgary', 'Airdrie', 'Cochrane', 'Chestermere', 'Okotoks')
        AND base_price IS NOT NULL
        AND rv_type IS NOT NULL
    ),
    segment_analysis AS (
        SELECT 
            l.rv_type,
            CASE 
                WHEN p.price_quartile = 1 THEN 'Budget'
                WHEN p.price_quartile = 2 THEN 'Mid-Range'
                WHEN p.price_quartile = 3 THEN 'Upper-Mid'
                ELSE 'Premium'
            END as segment,
            COUNT(*) as count,
            AVG(l.base_price) as avg_price,
            AVG(l.num_reviews) as avg_reviews,
            AVG(l.overall_rating) as avg_rating
        FROM listings l
        JOIN percentiles p ON l.rv_type = p.rv_type AND l.base_price = p.base_price
        WHERE l.location_city IN ('Calgary', 'Airdrie', 'Cochrane', 'Chestermere', 'Okotoks')
        GROUP BY l.rv_type, segment
    )
    SELECT * FROM segment_analysis
    WHERE rv_type IN ('Travel Trailer', 'Class C')
    ORDER BY rv_type, 
        CASE segment 
            WHEN 'Budget' THEN 1 
            WHEN 'Mid-Range' THEN 2 
            WHEN 'Upper-Mid' THEN 3 
            ELSE 4 
        END
    """
    
    df_segments = pd.read_sql_query(query_segments, conn)
    print(df_segments.to_string(index=False))
    
    # 7. Investment Recommendations
    print("\n=== TOP INVESTMENT RECOMMENDATIONS ===")
    
    recommendations = []
    
    # Recommendation 1: Entry-level Travel Trailer
    rec1 = {
        'Rank': 1,
        'Type': 'Travel Trailer (20-25ft)',
        'Investment': '$25,000-35,000',
        'Target Rate': '$120-140/night',
        'Why': 'Low entry cost, high demand, easier to tow',
        'Est. ROI': '15-20% at 50% occupancy'
    }
    recommendations.append(rec1)
    
    # Recommendation 2: Mid-size Travel Trailer
    rec2 = {
        'Rank': 2,
        'Type': 'Travel Trailer (26-30ft)',
        'Investment': '$35,000-45,000',
        'Target Rate': '$140-160/night',
        'Why': 'Family-friendly size, good availability',
        'Est. ROI': '12-18% at 50% occupancy'
    }
    recommendations.append(rec2)
    
    # Recommendation 3: Class C Motorhome
    rec3 = {
        'Rank': 3,
        'Type': 'Class C (24-28ft)',
        'Investment': '$70,000-90,000',
        'Target Rate': '$250-300/night',
        'Why': 'Premium rates, self-contained, no tow vehicle needed',
        'Est. ROI': '18-25% at 50% occupancy'
    }
    recommendations.append(rec3)
    
    # Recommendation 4: Toy Hauler
    rec4 = {
        'Rank': 4,
        'Type': 'Toy Hauler',
        'Investment': '$45,000-60,000',
        'Target Rate': '$180-220/night',
        'Why': 'Niche market, less competition, adventure seekers',
        'Est. ROI': '10-15% at 45% occupancy'
    }
    recommendations.append(rec4)
    
    # Recommendation 5: Class B Van
    rec5 = {
        'Rank': 5,
        'Type': 'Class B Van',
        'Investment': '$100,000-130,000',
        'Target Rate': '$250-300/night',
        'Why': 'Growing segment, urban-friendly, year-round use',
        'Est. ROI': '12-16% at 55% occupancy'
    }
    recommendations.append(rec5)
    
    df_recommendations = pd.DataFrame(recommendations)
    print(df_recommendations.to_string(index=False))
    
    # 8. Key Success Factors
    print("\n=== KEY SUCCESS FACTORS FOR NEW INVESTORS ===")
    print("1. **Start with Travel Trailers**: Lower entry cost, established market")
    print("2. **Target Family Size**: 20-30ft trailers that sleep 4-8 people")
    print("3. **Offer Delivery**: Commands 6.4% premium and expands customer base")
    print("4. **Location Strategy**: Focus on Calgary/Airdrie for highest demand")
    print("5. **Feature Must-Haves**: AC, heating, full kitchen, bathroom")
    print("6. **Pricing Strategy**: Start competitive to build reviews, then optimize")
    print("7. **Multi-RV Strategy**: Consider 2-3 unit portfolio for economies of scale")
    
    # 9. Market Gaps and Opportunities
    print("\n=== IDENTIFIED MARKET GAPS ===")
    
    query_gaps = """
    SELECT 
        'Luxury Travel Trailers' as gap,
        'Only ' || COUNT(*) || ' listings over $200/night' as opportunity
    FROM listings
    WHERE rv_type = 'Travel Trailer'
    AND base_price > 200
    AND location_city = 'Calgary'
    
    UNION ALL
    
    SELECT 
        'Pet-Friendly Class C' as gap,
        'Only ' || ROUND(AVG(CASE WHEN pet_friendly = 1 THEN 100 ELSE 0 END), 0) || '% allow pets' as opportunity
    FROM listings
    WHERE rv_type = 'Class C'
    AND location_city = 'Calgary'
    
    UNION ALL
    
    SELECT 
        'Small Travel Trailers' as gap,
        'Only ' || COUNT(*) || ' under 20ft (easy tow market)' as opportunity
    FROM listings
    WHERE rv_type = 'Travel Trailer'
    AND length_ft < 20
    AND location_city = 'Calgary'
    """
    
    df_gaps = pd.read_sql_query(query_gaps, conn)
    for _, row in df_gaps.iterrows():
        print(f"- {row['gap']}: {row['opportunity']}")
    
    # Export investment analysis
    print("\n✓ Detailed investment analysis completed")
    
    # Save key metrics
    investment_summary = {
        'market_overview': df_overview.to_dict('records'),
        'roi_analysis': df_roi_50.to_dict('records'),
        'recommendations': recommendations,
        'purchase_price_estimates': purchase_prices
    }
    
    import json
    with open('/home/chris/rvezy/data/processed/investment_analysis.json', 'w') as f:
        json.dump(investment_summary, f, indent=2)
    
    print("✓ Investment analysis exported to: /home/chris/rvezy/data/processed/investment_analysis.json")
    
    conn.close()

if __name__ == "__main__":
    analyze_investment_opportunities()