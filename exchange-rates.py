import os
import requests
import psycopg2
from psycopg2 import sql, extras

# Coinbase API endpoint
COINBASE_URLS = [
    "https://api.coinbase.com/v2/prices/CAD-BTC/spot",
    "https://api.coinbase.com/v2/prices/CAD-LTC/spot",
    "https://api.coinbase.com/v2/prices/CAD-BRL/spot",
    "https://api.coinbase.com/v2/prices/USDT-BRL/spot",
    "https://api.coinbase.com/v2/prices/USDT-BTC/spot",
    "https://api.coinbase.com/v2/prices/USDT-LTC/spot",
]

def fetch_all_prices():
    all_data = []
    for url in COINBASE_URLS:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise exception for HTTP errors
            data = response.json()
            all_data.append((
                data["data"]["base"],
                data["data"]["currency"],
                data["data"]["amount"]
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
        # Using execute_values for efficient bulk insert
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

if __name__ == "__main__":
    all_prices = fetch_all_prices()
    bulk_insert_into_neon(all_prices)