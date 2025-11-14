import os
import zipfile
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

ZIP_FOLDER = "Data-Files"     # Folder that contains all ZIP files
EXTRACT_FOLDER = "extracted_data"

# Connect to Supabase Postgres
conn = psycopg2.connect(
    host=os.environ["PGHOST"],
    port=os.environ["PGPORT"],
    dbname=os.environ["PGDATABASE"],
    user=os.environ["PGUSER"],
    password=os.environ["PGPASSWORD"]
)
cur = conn.cursor()

# Create extraction folder if it doesn’t exist
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

# Find all ZIP files in the folder
zip_files = [f for f in os.listdir(ZIP_FOLDER) if f.endswith(".zip")]

print(f"Found ZIP files: {zip_files}")

for zip_file in zip_files:
    zip_path = os.path.join(ZIP_FOLDER, zip_file)
    print(f"\nExtracting ZIP: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_FOLDER)

    # Load all CSV files extracted from this ZIP
    for file in os.listdir(EXTRACT_FOLDER):
        if file.endswith(".csv"):
            csv_path = os.path.join(EXTRACT_FOLDER, file)
            table_name = os.path.splitext(file)[0]

            print(f"Loading CSV → {csv_path} into table '{table_name}'...")

            df = pd.read_csv(csv_path)

            # Create table if not exists (TEXT columns for simplicity)
            columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns});')

            # Clear existing rows
            cur.execute(f'DELETE FROM "{table_name}";')

            # Prepare data rows
            rows = [tuple(x) for x in df.to_numpy()]
            insert_query = (
                f'INSERT INTO "{table_name}" ("' +
                '","'.join(df.columns) +
                '") VALUES %s;'
            )

            execute_values(cur, insert_query, rows)
            conn.commit()

            print(f"✔ Uploaded {len(rows)} rows to '{table_name}'")

    # Clean extraction folder before next ZIP
    for f in os.listdir(EXTRACT_FOLDER):
        os.remove(os.path.join(EXTRACT_FOLDER, f))

print("\n🎉 All ZIP files processed and uploaded successfully!")
