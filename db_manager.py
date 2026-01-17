import os
import sqlite3
import pandas as pd
from credentials import mysqldbconnector_pooled

class DBManager:
    def __init__(self):
        self.db_type = os.getenv("DB_TYPE", "sqlite").lower()
        self.sqlite_path = os.getenv("SQLITE_DB_PATH", "bybit_tax_data.db")

    def get_connection(self):
        if self.db_type == "mysql":
            return mysqldbconnector_pooled()
        else:
            return sqlite3.connect(self.sqlite_path)

    def _sqlite_insert_on_conflict_ignore(self, table, conn, keys, data_iter):
        """
        Custom PyPandas method to perform INSERT OR IGNORE for SQLite.
        This ensures we don't crash on duplicates or create double entries.
        """
        columns = ', '.join(f'"{k}"' for k in keys)
        placeholders = ', '.join('?' for _ in keys)
        sql = f'INSERT OR IGNORE INTO "{table}" ({columns}) VALUES ({placeholders})'
        conn.executemany(sql, data_iter)

    def insert_dataframe(self, table_name, df, chunksize=1000):
        if df.empty:
            return

        if self.db_type == "mysql":
            import tempfile
            import uuid

            upload_dir = os.getenv("MYSQL_UPLOAD_DIR", tempfile.gettempdir())
            temp_filename = f"bulk_load_{uuid.uuid4()}.csv"
            temp_path = os.path.join(upload_dir, temp_filename)

            # FIX 1: MySQL - Write headerless, but do NOT ignore lines in SQL
            df.to_csv(temp_path, index=False, header=False, sep=',', line_terminator='\n')

            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                sql_path = temp_path.replace('\\', '\\\\')
                cols = ",".join([f"`{c}`" for c in df.columns])

                # FIX 1 (Continued): Removed "IGNORE 1 LINES"
                # Added "IGNORE" keyword to handle duplicates gracefully in MySQL
                sql = f"""
                LOAD DATA LOCAL INFILE '{sql_path}' 
                IGNORE INTO TABLE {table_name} 
                FIELDS TERMINATED BY ',' 
                LINES TERMINATED BY '\\n' 
                ({cols});
                """

                cursor.execute(sql)
                conn.commit()
                # logger.info not available here unless imported, using print as in your snippet
                print(f"Loaded {len(df)} rows into MySQL table '{table_name}'")
            except Exception as e:
                print(f"MySQL Load Error: {e}")
                raise
            finally:
                cursor.close()
                conn.close()
                if os.path.exists(temp_path):
                    os.remove(temp_path)

        else:
            # FIX 2: SQLite - Use custom method to handle deduplication
            with sqlite3.connect(self.sqlite_path) as conn:
                # We use method=self._sqlite_insert_on_conflict_ignore
                # This requires pandas >= 0.24
                try:
                    df.to_sql(
                        table_name, 
                        conn, 
                        if_exists='append', 
                        index=False, 
                        chunksize=chunksize, 
                        method=self._sqlite_insert_on_conflict_ignore
                    )
                    print(f"Loaded {len(df)} rows into SQLite table '{table_name}' (deduplicated)")
                except Exception as e:
                    print(f"SQLite Load Error: {e}")
                    raise

# Singleton instance
db = DBManager()
