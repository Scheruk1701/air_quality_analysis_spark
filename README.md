# Air Quality Monitoring & Forecasting Project

## Overview
This project simulates a near-real-time air quality monitoring pipeline using Apache Spark. It processes PM2.5, temperature, and humidity sensor data from ingestion to machine learning predictions and dashboard visualizations.

The pipeline includes:
- TCP ingestion of sensor data  
- Outlier handling and data cleaning  
- SQL analytics  
- Predictive modeling using Spark MLlib  
- Final dashboard and visualization of results

---

##  Technologies Used
- Python, PySpark
- Apache Spark (Structured Streaming, SQL, MLlib)
- Pandas, NumPy
- Plotly, Seaborn, Matplotlib
- Kaleido (for exporting visualizations)

---

##  Pipeline Sections

###  Section 1: Data Ingestion
- Ingests historical sensor data (PM2.5, temperature, humidity) over TCP socket.
- Uses Spark Structured Streaming with watermarking.
- Merges metrics into one row per timestamp-region.
- Output: Raw ingested batches and a combined CSV file.

**Script**: `section1_ingestion.py`

---

###  Section 2: Data Aggregation & Transformation
- Handles outliers and missing values.
- Applies feature engineering (rolling averages, lag features).
- Performs hourly and daily grouping by timestamp & region.
- Output: Cleaned data stored in `output_task2/cleaned_data/`

**Script**: `section2_Aggregations.py`

---

###  Section 3: SQL Analysis
- Uses Spark SQL to analyze trends and patterns.
- Window functions to calculate moving averages.
- User-defined function (UDF) to classify AQI category.
- Output: SQL result CSVs in `output_task3/sql_outputs/`

**Script**: `section3_Explorations.py`

---

###  Section 4: Machine Learning with Spark MLlib
- Trains and tunes a Random Forest Regressor to predict PM2.5.
- Splits data into train/test sets.
- Evaluates with RMSE and R² metrics.
- Stores predictions and optionally saves the model.

**Script**: `section4_Spark_MLlib.py`  
**Output**: `output_task4/model_predictions/`

---

###  Section 5: Pipeline Integration & Dashboard Visualization
- Combines all steps into one unified script.
- Generates dashboards:
  - Actual vs predicted PM2.5 (line chart)
  - PM2.5 spikes (scatter plot)
  - AQI breakdown (pie chart)
  - Correlation plot (heatmap)
- Saves both HTML and PNG formats.

**Script**: `section5_pipeline_and_visualization.py`  
**Output**: `output_task5/final_output/`

---

##  Folder Structure
```
air_quality_project/
├── section1_ingestion.py
├── section2_transformations.py
├── section3_sql_queries.py
├── section4_Spark_MLlib.py
├── section5_pipeline_dashboard.py
├── output_task2/cleaned_data/
├── output_task3/sql_outputs/
├── output_task4/model_predictions/
├── output_task5/final_output/
└── README.md
```

---

##  Final Outcome
A complete, end-to-end system featuring:
- Real-time ingestion
- Cleaned and transformed data
- Analytical SQL insights
- Machine learning forecasts
- Interactive visual dashboards

> Ready for demonstration, stakeholder reports, and real-time monitoring.

---

##  How to Run

1. Install dependencies:
```bash
pip install pandas numpy plotly kaleido seaborn matplotlib
```

2. Start TCP ingestion:
```bash
python tcp_log_file_streaming_server.py
```

3. Run each section:
```bash
spark-submit section1_ingestion.py
spark-submit section2_Aggregations.py
spark-submit section3_Exploration.py
spark-submit section4_Spark_MLlib.py
spark-submit section5_pipeline_and_visualization.py
```

---

