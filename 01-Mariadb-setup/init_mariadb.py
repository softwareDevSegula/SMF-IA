# init_mariadb.py — NUCLEAR RECREATE (RUN THIS ONCE)
import mariadb
import yaml
from pathlib import Path

print("NUCLEAR RECREATE OF DATABASE — DESTROYING OLD DATA...")

with open("config/database.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

conn = mariadb.connect(
    host=cfg['host'],
    port=cfg['port'],
    user=cfg['user'],
    password=cfg['password']
)
cur = conn.cursor()

db_name = cfg['database']

# TOTAL DESTRUCTION
cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
cur.execute(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
cur.execute(f"USE {db_name}")
print(f"Database {db_name} DESTROYED AND RECREATED")

# Load the FINAL PERFECT schema
schema_sql = Path("schema.sql").read_text(encoding="utf-8")
for statement in schema_sql.split(';'):
    stmt = statement.strip()
    if stmt:
        cur.execute(stmt)

conn.commit()
conn.close()
print("SUCCESS! Database is 100% clean with multi-subsystem support")
print("NOW RUN: python parser_excel_to_mariadb.py")


### END OF FILE