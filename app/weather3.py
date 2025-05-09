import requests
import pandas as pd
import os
from datetime import datetime
from google.cloud import bigquery
import tempfile
import time

# Set your local service account credentials path (used only during local testing)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\BI PROJECT-CAT2\\PROJECT\\weatherpulseanalytics-a0c980d0f70a.json"

def fetch_existing_datetimes_from_bigquery(client, table_id):
    """Fetch existing datetime values from BigQuery table."""
    try:
        query = f"SELECT Datetime FROM `{table_id}`"
        query_job = client.query(query)
        results = query_job.result()
        return set(row.Datetime.strftime("%Y-%m-%d %H:%M:%S") for row in results)
    except Exception as e:
        print(f"⚠️ Warning: Failed to fetch existing datetimes: {e}")
        return set()  # Return empty set to continue with all data

def fetch_weather_with_retry(url, max_retries=5, base_timeout=10):
    """Fetch weather data with exponential backoff retry logic."""
    for attempt in range(max_retries):
        try:
            # Increase timeout with each retry
            timeout = base_timeout * (2 ** attempt)
            print(f"🔄 Attempt {attempt+1}/{max_retries} with timeout {timeout}s...")
            
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            print(f"✅ Successfully retrieved weather data on attempt {attempt+1}")
            return response.json()
            
        except requests.exceptions.Timeout:
            print(f"⏱️ Attempt {attempt+1} timed out after {timeout} seconds")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"⏳ Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                print("❌ All retry attempts exhausted")
                raise
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed on attempt {attempt+1}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"⏳ Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                print("❌ All retry attempts exhausted")
                raise

def fetch_and_process_weather_data():
    """Fetch, process and upload weather data to BigQuery"""
    
    # Coordinates and API
    lat = 11.016844
    lon = 76.955833
    api_key = "a18fbfa3b0ae5d4d9d7853c29bb6b5b6"
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    
    # BigQuery info
    project_id = "weatherpulseanalytics"
    dataset_id = f"{project_id}.weather_data"
    table_id = f"{dataset_id}.coimbatore_forecast"
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        existing_datetimes = fetch_existing_datetimes_from_bigquery(client, table_id)
        
        # Fetch weather data with retry logic
        data = fetch_weather_with_retry(url)
        
        # Process into DataFrame
        weather_list = []
        for item in data['list']:
            dt_text = item["dt_txt"]
            if dt_text in existing_datetimes:
                continue  # Skip duplicates
    
            dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S")
            weather = {
                "Datetime": dt_text,
                "Date": dt.date(),
                "Time": dt.time(),
                "Day": dt.strftime("%A"),
                "Temperature": round(item["main"]["temp"], 1),
                "Feels_Like": round(item["main"]["feels_like"], 1),
                "Temp_Min": round(item["main"]["temp_min"], 1),
                "Temp_Max": round(item["main"]["temp_max"], 1),
                "Humidity": item["main"]["humidity"],
                "Pressure": item["main"]["pressure"],
                "Wind_Speed": item["wind"]["speed"],
                "Wind_Direction": item["wind"]["deg"],
                "Weather": item["weather"][0]["main"],
                "Description": item["weather"][0]["description"],
                "Cloudiness": item["clouds"]["all"],
                "Rain_3h": item.get("rain", {}).get("3h", 0),
                "Snow_3h": item.get("snow", {}).get("3h", 0)
            }
            weather_list.append(weather)
        
        if not weather_list:
            print("✅ No new data to upload. Everything is already up-to-date.")
            return "✅ No new data to upload."
    
        df = pd.DataFrame(weather_list)
        df = df.fillna(0)
    
        # Add extra info
        df["Temp_Category"] = df["Temperature"].apply(lambda temp: "Cool" if temp < 20 else "Moderate" if temp < 30 else "Hot")
        df["Hour"] = df["Time"].apply(lambda x: x.hour)
        df["Time_Of_Day"] = df["Hour"].apply(lambda hour: (
            "Morning" if 5 <= hour < 12 else
            "Afternoon" if 12 <= hour < 17 else
            "Evening" if 17 <= hour < 21 else
            "Night"
        ))
    
        # Save to CSV temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp:
            temp_file_path = temp.name
            df.to_csv(temp_file_path, index=False)
    
        try:
            upload_to_bigquery(temp_file_path, client, table_id)
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    
        return "✅ New weather data uploaded to BigQuery successfully."
        
    except Exception as e:
        error_message = f"❌ Error: {str(e)}"
        print(error_message)
        return error_message

def upload_to_bigquery(csv_file, client, table_id):
    """Upload new rows from CSV to BigQuery."""
    
    schema = [
        bigquery.SchemaField("Datetime", "TIMESTAMP"),
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Time", "TIME"),
        bigquery.SchemaField("Day", "STRING"),
        bigquery.SchemaField("Temperature", "FLOAT"),
        bigquery.SchemaField("Feels_Like", "FLOAT"),
        bigquery.SchemaField("Temp_Min", "FLOAT"),
        bigquery.SchemaField("Temp_Max", "FLOAT"),
        bigquery.SchemaField("Humidity", "INTEGER"),
        bigquery.SchemaField("Pressure", "INTEGER"),
        bigquery.SchemaField("Wind_Speed", "FLOAT"),
        bigquery.SchemaField("Wind_Direction", "INTEGER"),
        bigquery.SchemaField("Weather", "STRING"),
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("Cloudiness", "INTEGER"),
        bigquery.SchemaField("Rain_3h", "FLOAT"),
        bigquery.SchemaField("Snow_3h", "FLOAT"),
        bigquery.SchemaField("Temp_Category", "STRING"),
        bigquery.SchemaField("Hour", "INTEGER"),
        bigquery.SchemaField("Time_Of_Day", "STRING")
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    try:
        with open(csv_file, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        
        job.result()  # Wait for the job to complete
        print(f"✅ Uploaded {job.output_rows} new rows to {table_id}")
    except Exception as e:
        print(f"❌ BigQuery upload failed: {e}")
        raise

# For Cloud Function
def cloud_function_entry(request):
    return fetch_and_process_weather_data()

# For local run
if __name__ == "__main__":
    result = fetch_and_process_weather_data()
    print(result)