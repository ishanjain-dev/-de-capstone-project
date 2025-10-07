"""
ingest_data.py

Fetch posts and comments from JSONPlaceholder and load raw JSON
into Snowflake tables:
USER_ACTIVITY_DB.RAW_DATA.POSTS
USER_ACTIVITY_DB.RAW_DATA.COMMENTS
"""

import os
import json
import requests
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path="../.env")

# URLs to fetch data from
POSTS_URL = "https://jsonplaceholder.typicode.com/posts"
COMMENTS_URL = "https://jsonplaceholder.typicode.com/comments"

# Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_AUTHENTICATOR = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")


def fetch_json_data(url):
    """Fetch JSON data from a given URL."""
    print(f"🌐 Fetching data from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


import base64
import json

def insert_raw_json(conn, table_name, data):
    try:
        print(f"📥 Inserting {len(data)} records into {table_name}...")

        select_statements = []
        for row in data:
            json_str = json.dumps(row)
            encoded = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
            select_statements.append(f"SELECT PARSE_JSON(BASE64_DECODE_STRING('{encoded}'))")

        union_selects = " UNION ALL ".join(select_statements)
        insert_query = f"INSERT INTO {table_name} (raw_json) {union_selects}"

        cursor = conn.cursor()
        cursor.execute(insert_query)
        cursor.close()

        print(f"✅ Successfully inserted {len(data)} records into {table_name}")

    except Exception as e:
        print(f"❌ Error inserting into {table_name}: {e}")
        raise


def main():
    """Main ingestion function."""
    print("🔗 Connecting to Snowflake... (this may open a browser for authentication)")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        authenticator=SNOWFLAKE_AUTHENTICATOR,
        password=os.getenv("SNOWFLAKE_PASSWORD")  # ignored if externalbrowser is used
    )

    # Fetch data
    print("📡 Fetching data from JSONPlaceholder...")
    posts = fetch_json_data(POSTS_URL)
    comments = fetch_json_data(COMMENTS_URL)

    # Insert data into Snowflake
    print("🚀 Loading data into Snowflake tables...")
    insert_raw_json(conn, f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.POSTS", posts)
    insert_raw_json(conn, f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COMMENTS", comments)

    # Close connection
    conn.close()
    print("🎉 Data ingestion complete!")


if __name__ == "__main__":
    main()
