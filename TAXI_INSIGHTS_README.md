# NYC Taxi Data Insights Analysis

This project provides comprehensive analysis of NYC taxi trip data to generate meaningful business insights and patterns. The analysis covers revenue trends, geographic patterns, trip characteristics, payment methods, and seasonal variations.

## 🚕 What This Analysis Provides

### Key Insights Generated:
1. **Revenue Analysis**: Peak hours, daily/weekly patterns, seasonal trends
2. **Geographic Patterns**: Most popular pickup/dropoff locations, profitable routes
3. **Trip Characteristics**: Distance, duration, fare correlations
4. **Payment Analysis**: Payment method preferences, tip patterns
5. **Seasonal Trends**: Monthly variations, weekend vs weekday patterns
6. **Business Recommendations**: Actionable insights for taxi operations

### Sample Insights You'll Discover:
- Peak revenue hours and days
- Most profitable pickup/dropoff locations
- Average fare and tip percentages
- Distance-fare correlations
- Payment method preferences
- Weekend vs weekday patterns

## 📊 Analysis Components

### 1. Data Quality Assessment
- Null value analysis
- Basic statistics
- Data completeness check

### 2. Revenue Analysis
- Hourly revenue patterns (24-hour cycle)
- Daily revenue patterns (7-day cycle)
- Monthly revenue trends
- Weekend vs weekday comparison

### 3. Geographic Analysis
- Top 10 pickup locations
- Top 10 dropoff locations
- Most profitable routes
- Location-based fare analysis

### 4. Trip Analysis
- Trip distance statistics
- Trip duration analysis
- Fare per mile calculations
- Distance-fare correlation

### 5. Payment Method Analysis
- Payment type distribution
- Tip analysis by payment method
- Average tip percentages

### 6. Seasonal Analysis
- Monthly revenue patterns
- Weekend vs weekday performance
- Seasonal trend identification

## 🚀 How to Run the Analysis

### Option 1: Run the Python Script
```bash
# Navigate to the src directory
cd src

# Run the analysis script
python taxi_insights.py
```

### Option 2: Use the Runner Script
```bash
# Navigate to the src directory
cd src

# Run using the runner script
python run_taxi_analysis.py
```

### Option 3: Run in Databricks
1. Upload `taxi_insights.py` to your Databricks workspace
2. Create a new notebook
3. Import and run the analysis:
```python
%run /path/to/taxi_insights
```

## 📋 Prerequisites

### For Local Development:
- Python 3.7+
- PySpark
- Databricks Connect (for connecting to Databricks)

### For Databricks:
- Access to Databricks workspace
- Access to `samples.nyctaxi.trips` table

## 📈 Sample Output

The analysis will generate output similar to:

```
=== KEY INSIGHTS SUMMARY ===

1. Peak Revenue Hours:
+----+-------------+
|hour|total_revenue|
+----+-------------+
|  18|   1234567.89|
|  19|   1187654.32|
|  17|   1154321.45|
+----+-------------+

2. Most Profitable Day: Day 6 with $8,765,432.10 revenue

3. Average Fare: $12.45

4. Most Popular Pickup Location: ID 132 with 15,432 trips

5. Most Popular Payment Method: Credit card with 45,678 trips

6. Average Tip Percentage: 18.5%

7. Distance-Fare Correlation: 0.892

8. Weekend vs Weekday Average Fare: $13.25 vs $12.15
```

## 🎯 Business Applications

### For Taxi Companies:
- **Driver Allocation**: Optimize driver deployment during peak hours
- **Pricing Strategy**: Implement dynamic pricing based on demand patterns
- **Route Planning**: Focus on high-profit routes and locations
- **Customer Service**: Improve service quality to maintain tip percentages

### For Drivers:
- **Schedule Optimization**: Work during peak revenue hours
- **Location Strategy**: Position at high-traffic pickup locations
- **Payment Preferences**: Encourage preferred payment methods

### For City Planning:
- **Transportation Infrastructure**: Identify high-demand areas
- **Public Transit**: Understand taxi usage patterns
- **Economic Analysis**: Monitor transportation sector performance

## 🔧 Customization

You can modify the analysis by:

1. **Adding New Metrics**: Extend the analysis functions to include additional KPIs
2. **Filtering Data**: Add date ranges or location filters
3. **Visualization**: Add plotting capabilities using matplotlib or plotly
4. **Export Results**: Save analysis results to CSV or database

## 📁 File Structure

```
src/
├── taxi_insights.py          # Main analysis script
├── run_taxi_analysis.py      # Simple runner script
└── dlt_pipeline.ipynb        # Existing DLT pipeline
```

## 🤝 Contributing

To extend the analysis:

1. Add new analysis functions to `taxi_insights.py`
2. Update the `main()` function to include new analyses
3. Add corresponding insights to `generate_insights()`
4. Update business recommendations as needed

## 📞 Support

For questions or issues:
1. Check the data source availability (`samples.nyctaxi.trips`)
2. Verify Databricks Connect configuration
3. Ensure all required dependencies are installed

---

**Note**: This analysis uses the NYC taxi trip data available in Databricks samples. The actual insights will depend on the specific dataset and time period available in your environment.
