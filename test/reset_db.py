#!/usr/bin/env python3
"""
reset_db.py

This script resets your PostgreSQL database by dropping and recreating the public schema,
and then restores the schema from an external SQL file (e.g., schema.sql).
If a price_class.csv file is present, it will also import that into the priceclass_lookup table.

WARNING: This will permanently delete ALL data in the database.
To proceed, you must type "YES" exactly when prompted.

Usage:
    python reset_db.py
"""

import psycopg2
import sys
from pathlib import Path
import os
import json

# Database connection details; adjust as needed.
DB_CONFIG = {
    "dbname": "iis_logs",
    "user": "atodd",       # Replace with your username
    "password": "",        # Add your password if required
    "host": "localhost",
    "port": 5432
}

# Paths to external files
SCHEMA_FILE = Path(__file__).parent / "schema.sql"
PRICE_CLASS_JSON_FILE = Path(__file__).parent / "price_class.json"

def reset_database():
    """
    Resets the database by dropping and recreating the public schema, then
    restoring the schema from the external SQL file.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # Needed for DROP SCHEMA
        cursor = conn.cursor()

        # Drop and recreate the public schema
        cursor.execute("DROP SCHEMA public CASCADE;")
        cursor.execute("CREATE SCHEMA public;")
        print("Public schema dropped and recreated.")

        # Read the schema SQL from the external file
        with open(SCHEMA_FILE, "r") as f:
            schema_sql = f.read()

        # Execute the schema SQL to restore the tables
        cursor.execute(schema_sql)
        print("Schema restored successfully from", SCHEMA_FILE)

        # Check if price_class.json exists
        if PRICE_CLASS_JSON_FILE.is_file():
            print(f"Found {PRICE_CLASS_JSON_FILE.name}. Importing price classes from JSON...")
            with open(PRICE_CLASS_JSON_FILE, "r", encoding="utf-8") as f:
                price_classes = json.load(f)
            
            with conn.cursor() as insert_cursor:
                for entry in price_classes:
                    insert_cursor.execute(
                        "INSERT INTO tbl_priceclass_lookup (priceclass, description) VALUES (%s, %s)",
                        (entry["priceclass"], entry["description"])
                    )
            conn.commit()
            print("Price classes imported successfully from JSON.")
        else:
            print(f"Warning: {PRICE_CLASS_JSON_FILE.name} not found. Skipping price class import.")

    except Exception as e:
        print("Error resetting database:", e)
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    confirm = input("WARNING: This will permanently delete ALL data in the database. Type 'YES' to continue: ")
    if confirm != "YES":
        print("Aborting. No changes were made.")
        sys.exit(1)
    
    reset_database()