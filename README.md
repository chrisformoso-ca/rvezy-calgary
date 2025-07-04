# RVezy Calgary Market Analysis

A comprehensive market analysis system for RV rental listings on RVezy in the Calgary area. This project extracts data from PandaScraper CSV exports and provides competitive intelligence, pricing optimization, and investment recommendations through an interactive dashboard.

## Features

- **Data Extraction**: Automated ETL from unstructured CSV data to normalized SQLite database
- **Market Analysis**: Comprehensive overview of 489+ RV listings with pricing trends
- **Interactive Dashboard**: 7-tab visualization system with real-time filtering
- **Investment Analysis**: ROI calculations and opportunity identification
- **Multi-Owner Intelligence**: Fleet analysis and revenue optimization strategies
- **Price Optimization**: Competitive positioning and pricing recommendations

## Quick Start

```bash
# 1. Extract data from raw CSV
python3 scripts/extract_rvezy_data.py

# 2. Generate dashboard
python3 scripts/generate_comprehensive_dashboard.py

# 3. Serve dashboard locally
cd output && python3 -m http.server 8080

# 4. Open in browser
# http://localhost:8080/dashboard_comprehensive.html
```

## Project Structure

```
rvezy-calgary/
   data/
      raw/              # Original PandaScraper CSV exports
      processed/        # SQLite database (rvezy_listings.db)
   scripts/
      extract_rvezy_data.py         # Main ETL script
      generate_comprehensive_dashboard.py  # Dashboard generator
      pricing_optimizer.py          # Pricing recommendations
      investment_analyzer.py        # ROI analysis
      ...                          # Other analysis scripts
   output/
      dashboard_comprehensive.html  # Interactive dashboard
      comprehensive_dashboard_data.json  # Dashboard data
   CLAUDE.md            # AI assistant instructions
```

## Dashboard Overview

### Market Overview Tab
- RV type distribution and market share
- Key metrics: average price, reviews, ratings
- Your listing position analysis

### Price Analysis Tab
- Interactive price threshold slider ($75-$400)
- Price vs demand bubble chart
- Capacity-based pricing analysis

### Investment Analysis Tab
- ROI calculations for each RV type
- Purchase price estimates
- Success rate metrics
- Investment opportunity matrix

### Host Intelligence Tab
- Multi-owner portfolio analysis
- Summer revenue projections (120 days)
- Fleet strategy identification

## Key Insights

- **Market Size**: 489 listings with pricing data
- **Dominant Type**: Travel Trailers (334 listings, 68%)
- **Price Range**: $75-$489/night
- **Your Position**: $97/night (25th percentile for Travel Trailers)
- **Competition**: 8 Tent Trailers averaging $94/night with 36 reviews

## Business Context

- Current listing: 2021 Travel Trailer at $97/night
- Profit split: 60/40 with RV owner
- Market opportunity: Only 85 Travel Trailers priced below yours
- Recommended strategy: Consider slight price increase to $105-110

## Requirements

- Python 3.7+
- pandas
- sqlite3
- numpy
- plotly (for dashboard visualizations)

## Database Schema

The SQLite database contains 7 normalized tables:
- `listings` - RV details and pricing
- `hosts` - Owner information
- `pricing` - Discount structures
- `amenities` - Features and equipment
- `listing_amenities` - Junction table
- `addons` - Optional services
- `beds` - Sleeping configurations

## Contributing

This project is designed for iterative analysis. To add new analysis scripts:
1. Query the SQLite database in `data/processed/`
2. Output results to console and/or JSON
3. Update the dashboard if adding visualizations

## License

Private project for RVezy market analysis.