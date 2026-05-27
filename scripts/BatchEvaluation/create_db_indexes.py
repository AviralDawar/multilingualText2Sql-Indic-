import sys
from pathlib import Path

# Add scripts directory to path
project_root = Path("/Users/aviraldawar/Desktop/Text2SQLResearch/IndicDB")
sys.path.insert(0, str(project_root / "scripts"))

from db_utils import load_config, get_connection, get_default_config_path

def main():
    print("Loading PostgreSQL configuration...")
    try:
        pg_config = load_config(get_default_config_path())
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    schema = "india_economic_census_firms"
    print(f"Connecting to database 'mydb' to index schema '{schema}'...")
    
    try:
        conn = get_connection(pg_config, database="mydb")
        conn.autocommit = True
        cursor = conn.cursor()

        # Get all Primary Keys in the target schema
        print("Fetching primary keys...")
        cursor.execute(f"""
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = '{schema}';
        """)
        pks = cursor.fetchall()
        print(f"Found {len(pks)} primary keys.")

        # Get all Foreign Keys in the target schema
        print("Fetching foreign keys...")
        cursor.execute(f"""
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = '{schema}';
        """)
        fks = cursor.fetchall()
        print(f"Found {len(fks)} foreign keys.")

        # Combine and create indexes
        targets = sorted(list(set(pks + fks)))
        print(f"Creating indexes for {len(targets)} unique columns...")

        created_count = 0
        for table, col in targets:
            index_name = f"idx_{table}_{col}".lower()
            # PostgreSQL identifier length limit is 63 bytes, truncate if needed
            if len(index_name) > 60:
                index_name = index_name[:60]
            
            stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ({col});"
            try:
                cursor.execute(stmt)
                print(f"  ✅ Indexed {table}({col})")
                created_count += 1
            except Exception as e:
                print(f"  ❌ Failed to index {table}({col}): {e}")

        cursor.close()
        conn.close()
        print(f"\n🎉 Success! Created {created_count} indexes on schema '{schema}'.")

    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main()
