# parser_excel_to_mariadb.py — FINAL INDUSTRIAL VERSION WITH FULL LOGGING (2025)
import pandas as pd
import mariadb
import yaml
from pathlib import Path
from collections import defaultdict

print("MBSE Excel → MariaDB Parser")

# Load config
with open("../01-mariadb-setup/config/database.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

conn = mariadb.connect(
    host=cfg['host'], port=cfg['port'],
    user=cfg['user'], password=cfg['password'],
    database=cfg['database']
)
cur = conn.cursor()

excel_folder = Path("../data/input_excel")
print(f"Looking in: {excel_folder.resolve()}")

# Logging counters
skipped_reasons = defaultdict(list)   # reason → list of (file, row, details)
total_processed = 0
total_skipped = 0

for excel_file in excel_folder.glob("*.xlsx"):
    if excel_file.name.startswith("~"):
        skipped_reasons["Temporary file (~)"].append((excel_file.name, "-", "Ignored temp file"))
        total_skipped += 1
        continue

    print(f"\nProcessing: {excel_file.name}")

    # EXTRACT SUBSYSTEM NAME — STOP AT _OID
    stem = excel_file.stem
    if "_OID" in stem:
        subsystem_name = stem.split("_OID")[0].strip().upper()
    else:
        subsystem_name = stem.split("_", 1)[0].strip().upper()
    print(f"  Subsystem: {subsystem_name}")

    # Get or create subsystem
    cur.execute("INSERT INTO Subsystems (name) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)", (subsystem_name,))
    cur.execute("SELECT id FROM Subsystems WHERE name=?", (subsystem_name,))
    subsystem_id = cur.fetchone()[0]

    # Load Excel
    xls = pd.ExcelFile(excel_file)
    df = None
    for sheet in xls.sheet_names:
        temp_df = pd.read_excel(excel_file, sheet_name=sheet, header=None)
        for idx, row in temp_df.iterrows():
            row_str = " | ".join([str(c) for c in row if pd.notna(c)])
            if all(x in row_str for x in ["Function", "Flow", "Direction"]):
                df = pd.read_excel(excel_file, sheet_name=sheet, header=idx)
                print(f"  Found table in sheet '{sheet}' at row {idx+1}")
                break
        if df is not None: break

    if df is None or df.empty:
        skipped_reasons["No valid table found"].append((excel_file.name, "-", "No Function/Flow/Direction header"))
        print("  No valid table found in this file")
        total_skipped += 1
        continue

    valid_rows = 0
    file_skipped = 0

    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row number

        # Skip completely empty rows
        if pd.isna(row.get("Function")) or pd.isna(row.get("Flow")):
            skipped_reasons["Empty Function or Flow"].append((excel_file.name, row_num, f"Function='{row.get('Function')}' | Flow='{row.get('Flow')}'"))
            file_skipped += 1
            continue

        fct_tag = str(row["Function"]).strip()
        flux_name = str(row["Flow"]).strip()
        direction_raw = row.get("Direction")
        direction = str(direction_raw).strip().lower() if pd.notna(direction_raw) else ""

        if direction not in ["emission", "consumption"]:
            skipped_reasons["Invalid Direction"].append((excel_file.name, row_num, f"Direction='{direction_raw}'"))
            file_skipped += 1
            continue

        # SUCCESS — process it
        cur.execute("""
            INSERT INTO Functions (fct_tag, subsystem_id, source_file, source_row)
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)
        """, (fct_tag, subsystem_id, excel_file.name, row_num))
        cur.execute("SELECT id FROM Functions WHERE fct_tag=? AND subsystem_id=?", (fct_tag, subsystem_id))
        func_id = cur.fetchone()[0]

        cur.execute("INSERT INTO Fluxes (name) VALUES (?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)", (flux_name,))
        cur.execute("SELECT id FROM Fluxes WHERE name=?", (flux_name,))
        flux_id = cur.fetchone()[0]

        if direction == "emission":
            cur.execute("""
                INSERT IGNORE INTO FluxEmissions (flux_id, emitter_func_id, source_file, source_row)
                VALUES (?, ?, ?, ?)
            """, (flux_id, func_id, excel_file.name, row_num))
            print(f"  EMISSION: {fct_tag} ({subsystem_name}) → {flux_name}")
        else:
            cur.execute("""
                INSERT IGNORE INTO FluxConsumptions (flux_id, consumer_func_id, source_file, source_row)
                VALUES (?, ?, ?, ?)
            """, (flux_id, func_id, excel_file.name, row_num))
            print(f"  CONSUMPTION: {fct_tag} ({subsystem_name}) ← {flux_name}")

        valid_rows += 1
        total_processed += 1

    total_skipped += file_skipped
    print(f"  Processed {valid_rows} rows | Skipped {file_skipped} in this file")
    conn.commit()

# FINAL LOGGING REPORT
print("\n" + "="*80)
print("PARSING COMPLETED — FINAL REPORT")
print("="*80)
print(f"Total rows successfully imported : {total_processed}")
print(f"Total rows skipped               : {total_skipped}\n")

if total_skipped > 0:
    print("SKIPPED ELEMENTS — DETAILED BREAKDOWN:")
    print("-" * 80)
    for reason, items in skipped_reasons.items():
        print(f"{reason} → {len(items)} rows")
        for file, row, detail in items[:10]:  # show max 10 examples
            print(f"   • {file} | Row {row} | {detail}")
        if len(items) > 10:
            print(f"   ... and {len(items)-10} more")
        print()
else:
    print("Perfect run! No rows skipped")

print("Ready for JavaFX app, diagram generation, or Rhapsody export")
print("="*80)