import os
import sqlite3
import pandas as pd
from credentials import mysqldbconnector_pooled  # Keep your existing one for MySQL


class DBManager:
    def __init__(self):
        # Default to SQLite if DB_TYPE is not set to 'mysql'
        self.db_type = os.getenv("DB_TYPE", "sqlite").lower()
        self.sqlite_path = os.getenv("SQLITE_DB_PATH", "bybit_tax_data.db")

    def get_connection(self):
        if self.db_type == "mysql":
            return mysqldbconnector_pooled()
        else:
            return sqlite3.connect(self.sqlite_path)

    def insert_dataframe(self, table_name, df, chunksize=1000):
        if df.empty:
            return

        if self.db_type == "mysql":
            # --- REAL MYSQL LOGIC ---
            import tempfile
            import uuid

            # 1. Create a temp CSV file
            # We use a temp file in the configured upload directory or system temp
            upload_dir = os.getenv("MYSQL_UPLOAD_DIR", tempfile.gettempdir())
            temp_filename = f"bulk_load_{uuid.uuid4()}.csv"
            temp_path = os.path.join(upload_dir, temp_filename)

            # 2. Write DataFrame to CSV (Headerless for MySQL load)
            df.to_csv(temp_path, index=False, header=False, sep=',', line_terminator='\n')

            # 3. Execute LOAD DATA INFILE
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                # Sanitize path for Windows/SQL
                sql_path = temp_path.replace('\\', '\\\\')

                # Dynamic column list (crucial for correct mapping)
                cols = ",".join([f"`{c}`" for c in df.columns])

                sql = f"""
                LOAD DATA LOCAL INFILE '{sql_path}' 
                INTO TABLE {table_name} 
                FIELDS TERMINATED BY ',' 
                LINES TERMINATED BY '\\n' 
                IGNORE 1 LINES 
                ({cols});
                """
                # Note: remove "IGNORE 1 LINES" if you wrote header=False above

                cursor.execute(sql)
                conn.commit()
                print(f"Loaded {len(df)} rows into MySQL table '{table_name}'")
            except Exception as e:
                print(f"MySQL Load Error: {e}")
                raise
            finally:
                cursor.close()
                conn.close()
                # 4. Cleanup
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        else:
            # --- SQLITE LOGIC ---
            with sqlite3.connect(self.sqlite_path) as conn:
                df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=chunksize)
                print(f"Loaded {len(df)} rows into SQLite table '{table_name}'")


# Singleton instance
db = DBManager()
