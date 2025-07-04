import os
import requests
import psycopg2
from psycopg2 import sql, extras
import time
from datetime import datetime, timedelta

# Coinbase API endpoint
COINBASE_URLS = [
    "https://api.coinbase.com/v2/prices/CAD-BTC/spot ",
    "https://api.coinbase.com/v2/prices/CAD-LTC/spot ",
    "https://api.coinbase.com/v2/prices/CAD-BRL/spot ",
    "https://api.coinbase.com/v2/prices/USDT-BRL/spot ",
    "https://api.coinbase.com/v2/prices/USDT-BTC/spot ",
    "https://api.coinbase.com/v2/prices/USDT-LTC/spot ",
]

def fetch_all_prices():
    all_data = []
    for url in COINBASE_URLS:
        try:
            response = requests.get(url.strip())
            response.raise_for_status()
            data = response.json()
            all_data.append((
                data["data"]["base"],
                data["data"]["currency"],
                float(data["data"]["amount"])
            ))
        except (requests.RequestException, KeyError) as e:
            print(f"Error fetching data from {url}: {str(e)}")
    return all_data

def bulk_insert_into_neon(data_list):
    if not data_list:
        print("No data to insert")
        return
    
    api_key = os.environ['CRON_COLLECTABLE_KEY']
    conn = psycopg2.connect(api_key)
    cur = conn.cursor()
    
    query = sql.SQL("""
    INSERT INTO exchange_prices (base, currency, price)
    VALUES %s
    """)
    
    try:
        extras.execute_values(
            cur,
            query,
            data_list,
            template="(%s, %s, %s)",
            page_size=len(data_list)
        )
        conn.commit()
        print(f"Successfully inserted {len(data_list)} records")
    except Exception as e:
        conn.rollback()
        print(f"Database error: {str(e)}")
    finally:
        cur.close()
        conn.close()

def get_next_scheduled_time():
    """Calculate next 6-hour interval (0, 6, 12, 18 UTC)"""
    now = datetime.utcnow()
    # Round to next 6-hour mark (0, 6, 12, 18)
    hours = ((now.hour // 6) + 1) * 6
    next_time = now.replace(hour=hours, minute=0, second=0, microsecond=0)
    return next_time

if __name__ == "__main__":
    # Calculate when the next GitHub Action trigger will happen
    next_run = get_next_scheduled_time()
    end_time = next_run.timestamp()
    
    print(f"Current time (UTC): {datetime.utcnow()}")
    print(f"Next scheduled run: {next_run} UTC")
    print(f"Total runtime allowed: {(end_time - time.time()) / 3600:.2f} hours")

    while time.time() < end_time:
        print("\n" + "-" * 40)
        print(f"[{datetime.utcnow()} UTC] Starting new iteration...")
        
        # Fetch and insert data
        all_prices = fetch_all_prices()
        bulk_insert_into_neon(all_prices)
        
        # Calculate remaining time and sleep accordingly
        remaining_seconds = end_time - time.time()
        if remaining_seconds <= 0:
            print("Reached scheduled end time. Exiting.")
            break
            
        sleep_duration = min(300, remaining_seconds)  # Max 5 minutes
        next_wake = datetime.utcfromtimestamp(time.time() + sleep_duration)
        print(f"[{datetime.utcnow()} UTC] Sleeping for {sleep_duration}s... Next wake: {next_wake} UTC")
        time.sleep(sleep_duration)