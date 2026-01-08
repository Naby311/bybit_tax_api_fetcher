import os

# Conditional Import for MySQL
try:
    import mysql.connector
    from mysql.connector.pooling import MySQLConnectionPool

    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    MySQLConnectionPool = object  # Dummy class to prevent NameError
    print("Warning: mysql-connector-python not installed. MySQL features unavailable.")
from datetime import datetime
from pybit.unified_trading import HTTP
import threading

# --- ADD THIS BLOCK ---
from dotenv import load_dotenv

# Load variables from .env file into os.environ
load_dotenv()
# ----------------------

# Global script start time (set once when the module is loaded)
SCRIPT_START_TIME = datetime.now()

print(f"MysqlConnAndCreds module loaded at: {SCRIPT_START_TIME}")


# Function to load MySQL database credentials from environment variables
def load_env_variable_dbconnector():
    # Now these will actually find values from your .env file
    MySQLhost = os.environ.get('MYSQLhost')
    MysqlUser = os.environ.get('MYSQLuser')
    MYSQLpassword = os.environ.get('MYSQLpassword')
    MYSQLdatabase = os.environ.get('MYSQLdatabase')

    # Validation (Optional but recommended)
    if not all([MySQLhost, MysqlUser, MYSQLpassword, MYSQLdatabase]):
        print("WARNING: One or more MySQL environment variables are missing!")

    return MySQLhost, MysqlUser, MYSQLpassword, MYSQLdatabase


# Load DB credentials once when the module is imported
MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE = load_env_variable_dbconnector()

# Construct SQLAlchemy URL using the loaded credentials
SQLALCHEMY_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"

# Create a connection pool at the module level
# Add an optional infile flag to be true
# Create a connection pool at the module level
pool = None
if MYSQL_AVAILABLE:
    try:
        pool = MySQLConnectionPool(
            pool_name="mypool",
            pool_size=20,  # Adjust size based on your needs
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            allow_local_infile=True
        )
    except Exception as e:
        print(f"Warning: Failed to create MySQL pool: {e}")


# Original MySQL connection function (unchanged)
def mysqldbconnector():
    """
    Returns a new mysql.connector connection using module-level credentials.
    This function remains available for other scripts that call it directly.
    """
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        allow_local_infile=True
    )
    return conn


# New function to get a connection from the pool
def mysqldbconnector_pooled():
    """
    Returns a connection from the MySQL connection pool.
    This ensures efficient reuse of connections across threads.
    """
    if not MYSQL_AVAILABLE or pool is None:
        raise ImportError("MySQL driver not installed or Pool creation failed.")
    return pool.get_connection()


# In MysqlConnAndCreds.py (append this to the existing code)

def get_bybit_api_keys():
    """Fetch Bybit API credentials from environment variables."""
    api_key = os.environ.get("BybitAPIKey")
    api_secret = os.environ.get("BybitAPIKeySecret")
    return api_key, api_secret


def get_bybit_tax_api_keys():
    """Fetch Bybit API credentials from environment variables."""
    api_key = os.getenv("BYBIT_TAX_API_KEY")
    api_secret = os.getenv("BYBIT_TAX_PRIVATE_KEY")
    return api_key, api_secret


# For hybrid thread-local reuse
_thread_local = threading.local()


def get_bybit_client() -> HTTP:
    """Return a thread-local Bybit HTTP client using mainnet."""
    if not hasattr(_thread_local, "client"):
        api_key, api_secret = get_bybit_api_keys()
        _thread_local.client = HTTP(
            api_key=api_key,
            api_secret=api_secret,
            testnet=False,
        )
    return _thread_local.client
