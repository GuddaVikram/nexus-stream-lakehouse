import duckdb

# 1. Connect to the local lakehouse database file
conn = duckdb.connect("analytics_warehouse/analytics_lakehouse.duckdb") # Change path to "analytics_warehouse/analytics_lakehouse.duckdb" if running from root

# 2. Configure the standalone session to route S3 requests to local MinIO
conn.execute("SET s3_endpoint='localhost:9000';")
conn.execute("SET s3_access_key_id='admin_key';")
conn.execute("SET s3_secret_access_key='secret_session_password';")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style='path';")

print("\n📊 STAGING VIEW SAMPLE (FIRST 3 ROWS):")
print(conn.execute("SELECT * FROM main.stg_market_metrics LIMIT 3;").fetchdf())

print("\n🏆 GOLD MART - HOURLY MARKET TRENDS:")
print(conn.execute("SELECT * FROM main.fct_hourly_market_trends LIMIT 5;").fetchdf())

conn.close()