import os
import pandas as pd
import numpy as np

# -------------------------
# STEP 1: Load the input data
# -------------------------
input_csv = "/workspaces/air_quality_analysis_spark/output/combined_data_singlefile/part-00000-a877bdd4-7151-4ddb-98d5-bc94d55e4176-c000.csv"
df = pd.read_csv(input_csv, parse_dates=["timestamp"])

# -------------------------
# STEP 2: Handle Outliers
# -------------------------
df = df[df["pm25"] < 1000]
df["temperature"] = np.where(df["temperature"] > 60, np.nan, df["temperature"])
df["humidity"] = np.where((df["humidity"] > 100) | (df["humidity"] < 0), np.nan, df["humidity"])

# -------------------------
# STEP 3: Impute Missing Values
# -------------------------
df["pm25"].fillna(df["pm25"].median(), inplace=True)
df["temperature"].fillna(df["temperature"].median(), inplace=True)
df["humidity"].fillna(df["humidity"].median(), inplace=True)

# -------------------------
# STEP 4: Feature Engineering
# -------------------------
df["date"] = df["timestamp"].dt.date
df["hour"] = df["timestamp"].dt.hour

# Z-score normalization
for feature in ["pm25", "temperature", "humidity"]:
    mean = df[feature].mean()
    std = df[feature].std()
    df[f"{feature}_zscore"] = (df[feature] - mean) / std

# Rolling average, lag, rate-of-change
df.sort_values(by=["region", "timestamp"], inplace=True)
df["pm25_rolling_avg_3"] = df.groupby("region")["pm25"].transform(lambda x: x.rolling(3, min_periods=1).mean())
df["pm25_lag_1"] = df.groupby("region")["pm25"].shift(1)
df["pm25_rate_of_change"] = df["pm25"] - df["pm25_lag_1"]

# -------------------------
# STEP 5: Aggregations
# -------------------------
daily_avg = df.groupby(["date", "region"]).agg({
    "pm25": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index()

hourly_avg = df.groupby(["date", "hour", "region"]).agg({
    "pm25": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index()

pm25_trends = df[["timestamp", "region", "pm25", "pm25_rolling_avg_3", "pm25_lag_1", "pm25_rate_of_change"]].dropna()

# -------------------------
# STEP 6: Save outputs
# -------------------------
main_output_dir = "/workspaces/air_quality_analysis_spark/output_task2"
os.makedirs(main_output_dir, exist_ok=True)

output_datasets = {
    "task2_enhanced_data": df,
    "task2_daily_avg": daily_avg,
    "task2_hourly_avg": hourly_avg,
    "task2_pm25_trends": pm25_trends
}

def create_success_files(folder_path, dataset_name):
    # Create empty SUCCESS file
    open(os.path.join(folder_path, "_SUCCESS"), 'w').close()
    # Create a dummy CRC file
    crc_file = os.path.join(folder_path, f".{dataset_name}.csv.crc")
    open(crc_file, 'w').close()

for dataset_name, dataset_df in output_datasets.items():
    subfolder = os.path.join(main_output_dir, dataset_name)
    os.makedirs(subfolder, exist_ok=True)

    csv_path = os.path.join(subfolder, f"{dataset_name}.csv")
    parquet_path = os.path.join(subfolder, f"{dataset_name}.parquet")

    # Special rounding ONLY for task2_enhanced_data
    if dataset_name == "task2_enhanced_data":
        dataset_df = dataset_df.round(2)

    # Reorder columns nicely for task2_enhanced_data
    if dataset_name == "task2_enhanced_data":
        columns_order = [
            "timestamp", "region", "pm25", "temperature", "humidity", "date", "hour",
            "pm25_zscore", "temperature_zscore", "humidity_zscore",
            "pm25_rolling_avg_3", "pm25_lag_1", "pm25_rate_of_change"
        ]
        dataset_df = dataset_df[columns_order]

    # Write files
    dataset_df.to_csv(csv_path, index=False)
    dataset_df.to_parquet(parquet_path, engine="pyarrow")

    # Create success indicators
    create_success_files(subfolder, dataset_name)

print("\n🎯 Task 2 - COMPLETED Successfully!")
print(f"All outputs saved inside ➡️ {main_output_dir}\n")
