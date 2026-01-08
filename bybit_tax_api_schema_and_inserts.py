#!/usr/bin/env python3
"""
Schema definitions and MySQL insert helpers for Bybit Tax API data.
Organized by ReportType and ReportNumber according to Bybit API documentation.
"""
import logging
import os
import uuid
import csv
import time
import pandas as pd
from typing import List, Tuple, Dict, Any
from enum import Enum
import inspect


# ------------ BYBIT TAX API ENUMS ------------

class ReportType(Enum):
    TRADE = "TRADE"
    PNL = "P&L"
    EARN = "EARN"
    DEPOSIT_WITHDRAWAL = "DEPOSIT&WITHDRAWAL"
    BONUS = "BONUS"
    AIRDROP = "AIRDROP"


# ------------ SCHEMA MAPPING CONFIGURATION ------------

# Map ReportType + ReportNumber to database table and schema info
REPORT_SCHEMAS = {
    # DEPOSIT&WITHDRAWAL Reports
    (ReportType.DEPOSIT_WITHDRAWAL, 1): {
        'table': 'crypto_deposits',
        'description': 'Get Crypto Deposit History',
        'columns': ['id', 'txHash', 'coin', 'amount', 'status', 'createTime', 'source_exchange'],
        'primary_keys': ['txHash'],
        'transform_function': None  # To be implemented when needed
    },
    (ReportType.DEPOSIT_WITHDRAWAL, 2): {
        'table': 'p2p_deposits',
        'description': 'Get P2P Deposit History',
        'columns': ['id', 'orderNo', 'amount', 'coin', 'status', 'createTime', 'source_exchange'],
        'primary_keys': ['orderNo'],
        'transform_function': None
    },
    (ReportType.DEPOSIT_WITHDRAWAL, 3): {
        'table': 'fiat_deposits',
        'description': 'Get Fiat Deposit and Withdraw History',
        'columns': [
            'orderNo', 'fiatCurrency', 'indicatedAmount', 'amount',
            'totalFee', 'method', 'status', 'createTime', 'source_exchange'
        ],
        'primary_keys': ['orderNo'],
        'column_metadata': {
            'id': {'type': 'int', 'null': False, 'key': 'PRI', 'extra': 'auto_increment'},
            'orderNo': {'type': 'varchar(255)', 'null': False, 'key': 'UNI'},
            'fiatCurrency': {'type': 'varchar(10)', 'null': False},
            'indicatedAmount': {'type': 'decimal(20,8)', 'null': False},
            'amount': {'type': 'decimal(20,8)', 'null': False},
            'totalFee': {'type': 'decimal(20,8)', 'null': False},
            'method': {'type': 'varchar(255)', 'null': False},
            'status': {'type': 'varchar(50)', 'null': False},
            'createTime': {'type': 'datetime', 'null': False},
            'source_exchange': {'type': 'varchar(255)', 'null': False},
        },
        'transform_function': 'transform_fiat_data_to_mysql_format'
    },
    (ReportType.DEPOSIT_WITHDRAWAL, 4): {
        'table': 'express_deposits',
        'description': 'Get Express Order Deposit History',
        'columns': ['id', 'orderNo', 'coin', 'amount', 'status', 'createTime', 'source_exchange'],
        'primary_keys': ['orderNo'],
        'transform_function': None
    },
    (ReportType.DEPOSIT_WITHDRAWAL, 5): {
        'table': 'third_party_deposits',
        'description': 'Get Third Party Deposit History',
        'columns': ['id', 'orderNo', 'coin', 'amount', 'status', 'createTime', 'source_exchange'],
        'primary_keys': ['orderNo'],
        'transform_function': None
    },
    (ReportType.DEPOSIT_WITHDRAWAL, 6): {
        'table': 'crypto_withdrawals',
        'description': 'Get Crypto Withdraw History',
        'columns': ['id', 'txHash', 'coin', 'amount', 'fee', 'status', 'createTime', 'source_exchange'],
        'primary_keys': ['txHash'],
        'transform_function': None
    },
    (ReportType.DEPOSIT_WITHDRAWAL, 7): {
        'table': 'nft_transactions',
        'description': 'Get NFT Deposit and Withdrawal History',
        'columns': ['id', 'txHash', 'tokenId', 'amount', 'type', 'status', 'createTime', 'source_exchange'],
        'primary_keys': ['txHash'],
        'transform_function': None
    },

    # TRADE Reports (examples - can be expanded)
    (ReportType.TRADE, 1): {
        'table': 'spot_trades',
        'description': 'Get Spot Trade History',
        'columns': ['id', 'symbol', 'orderId', 'execPrice', 'execQty', 'execFee', 'side', 'execTime',
                    'source_exchange'],
        'primary_keys': ['orderId', 'execTime'],
        'transform_function': None
    },
    (ReportType.TRADE, 2): {
        'table': 'contract_trades',
        'description': 'Get Contract Trade History',
        'columns': ['id', 'symbol', 'orderId', 'execPrice', 'execQty', 'execFee', 'side', 'execTime',
                    'source_exchange'],
        'primary_keys': ['orderId', 'execTime'],
        'transform_function': None
    },
    (ReportType.EARN, 1): {
        'table': 'bybit_earn',
        'description': 'Get BybitSavings Yield History',
        'columns': [
            'AssetEarnedCoin', 'EffectiveStakingAmount', 'yield_amount', 'TradeTime',
            'earn_type', 'asset_staked_coin', 'staking_type'
        ],
        'primary_keys': ['AssetEarnedCoin', 'TradeTime', 'earn_type'],
        'transform_function': 'transform_earn_data'  # Use the new unified function
    },
    (ReportType.EARN, 7): {
        'table': 'bybit_earn',
        'description': 'Get Launchpool Yield History',
        'columns': [
            'AssetEarnedCoin', 'EffectiveStakingAmount', 'yield_amount', 'TradeTime',
            'earn_type', 'asset_staked_coin', 'staking_type'
        ],
        'primary_keys': ['AssetEarnedCoin', 'TradeTime', 'earn_type'],
        'transform_function': 'transform_earn_data'  # Use the new unified function
    },

    # Now add the AIRDROP[1] entry at the same level:
    (ReportType.AIRDROP, 1): {
        'table': 'airdrop_events',
        'description': 'Get Airdrop History',  # Add this line
        'columns': ['Coin', 'FinalAmount', 'TransferType', 'TransferDescription', 'CompletedTime', 'source_exchange'],
        'primary_keys': ['Coin', 'FinalAmount', 'TransferType', 'CompletedTime'],
        'transform_function': None
    },

}

# ------------ CONFIGURATION CONSTANTS ------------

# MySQL upload directory
MYSQL_UPLOAD_DIR = r"C:\ProgramData\MySQL\MySQL Server 8.0\Uploads"


# ------------ SCHEMA HELPER FUNCTIONS ------------

def get_schema_info(report_type: ReportType, report_number: int) -> Dict[str, Any]:
    """Get schema information for a specific report type and number."""
    key = (report_type, report_number)
    if key not in REPORT_SCHEMAS:
        raise ValueError(f"No schema defined for {report_type.value}[{report_number}]")
    return REPORT_SCHEMAS[key]


def get_table_name(report_type: ReportType, report_number: int) -> str:
    """Get table name for a specific report type and number."""
    return get_schema_info(report_type, report_number)['table']


def get_columns(report_type: ReportType, report_number: int) -> List[str]:
    """Get column names for a specific report type and number."""
    return get_schema_info(report_type, report_number)['columns']


def get_primary_keys(report_type: ReportType, report_number: int) -> List[str]:
    """Get primary key columns for a specific report type and number."""
    return get_schema_info(report_type, report_number)['primary_keys']


def list_available_reports() -> None:
    """Print all available report types and numbers."""
    print("Available Bybit Tax API Reports:")
    for (report_type, report_number), info in REPORT_SCHEMAS.items():
        print(f"  {report_type.value}[{report_number}]: {info['description']} -> {info['table']}")


# ------------ CURRENT DEFAULTS FOR FIAT DEPOSITS (DEPOSIT&WITHDRAWAL[3]) ------------

# For backward compatibility and current usage
CURRENT_REPORT_TYPE = ReportType.DEPOSIT_WITHDRAWAL
CURRENT_REPORT_NUMBER = 3

# Get current schema info
_current_schema = get_schema_info(CURRENT_REPORT_TYPE, CURRENT_REPORT_NUMBER)
TABLE_NAME = _current_schema['table']
FIAT_COLUMNS = _current_schema['columns']
PRIMARY_KEY_COLUMNS = _current_schema['primary_keys']
COLUMN_METADATA = _current_schema.get('column_metadata', {})


# ------------ DATA TRANSFORMATION FUNCTIONS ------------

def transform_fiat_data_to_mysql_format(df: pd.DataFrame) -> pd.DataFrame:
    """Transform raw fiat deposit/withdrawal DataFrame to MySQL format."""
    if df.empty:
        logging.warning("Empty DataFrame provided for fiat deposit MySQL export")
        return pd.DataFrame()

    required_cols = ['OrderID', 'Fiat', 'DepositAmount', 'FinalAmount', 'Fee', 'PaymentMethods', 'CompletedTime']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns for fiat deposits: {missing_cols}")

    mysql_df = pd.DataFrame({
        'orderNo': df['OrderID'].astype(str),
        'fiatCurrency': df['Fiat'].astype(str),
        'indicatedAmount': pd.to_numeric(df['DepositAmount'], errors='coerce'),
        'amount': pd.to_numeric(df['FinalAmount'], errors='coerce'),
        'totalFee': pd.to_numeric(df['Fee'], errors='coerce'),
        'method': df['PaymentMethods'].astype(str),
        'status': 'Completed',
        'createTime': pd.to_datetime(
            pd.to_numeric(df['CompletedTime'], errors='coerce'),
            unit='ms', errors='coerce'
        ).dt.strftime('%Y-%m-%d %H:%M:%S'),
        'source_exchange': 'bybit'
    })

    # Remove rows with invalid critical data
    initial_count = len(mysql_df)
    mysql_df = mysql_df.dropna(subset=['orderNo', 'createTime'])
    if len(mysql_df) < initial_count:
        logging.warning(f"Dropped {initial_count - len(mysql_df)} fiat deposit rows due to invalid data")

    return mysql_df


def transform_earn_launchpool_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Launchpool data to MySQL format."""
    if df.empty:
        logging.warning("Empty DataFrame provided for Launchpool export")
        return pd.DataFrame()

    required_cols = ['AssetEarnedCoin', 'EffectiveStakingAmount', 'Yield', 'TradeTime']
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns for Launchpool: {missing_cols}")

    mysql_df = pd.DataFrame({
        'AssetEarnedCoin': df['AssetEarnedCoin'].astype(str),
        'EffectiveStakingAmount': pd.to_numeric(df['EffectiveStakingAmount'], errors='coerce'),
        'Yield': pd.to_numeric(df['Yield'], errors='coerce'),
        'TradeTime': pd.to_datetime(
            pd.to_numeric(df['TradeTime'], errors='coerce'),
            unit='ms', errors='coerce'
        ).dt.strftime('%Y-%m-%d %H:%M:%S')
    })

    # Remove rows with invalid critical data
    initial_count = len(mysql_df)
    mysql_df = mysql_df.dropna(subset=['AssetEarnedCoin', 'TradeTime'])

    if len(mysql_df) < initial_count:
        logging.warning(f"Dropped {initial_count - len(mysql_df)} Launchpool rows due to invalid data")

    return mysql_df


# In bybit_tax_api_schema_and_inserts.py

def transform_earn_data(df: pd.DataFrame, report_number: int) -> pd.DataFrame:
    """
    Transforms both Savings (1) and Launchpool (7) data to fit the bybit_earn table.
    This version includes robust type casting and cleaning to prevent errors.
    """
    if df.empty:
        return pd.DataFrame()

    # 1. Safely clean all 'object' type columns by first casting them to string.
    # This is the key fix for the '.str accessor' error.
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.replace('\\r', '', regex=False)

    # 2. Rename columns to match the database schema
    if 'Yield' in df.columns:
        df = df.rename(columns={'Yield': 'yield_amount'})
    if report_number == 1:  # BybitSavings
        df = df.rename(columns={
            'AssetStakedCoin': 'asset_staked_coin',
            'StakingType': 'staking_type'
        })

    # 3. Add report-specific static columns
    if report_number == 1:
        df['earn_type'] = 'savings'
    elif report_number == 7:
        df['earn_type'] = 'launchpool'
        df['asset_staked_coin'] = 'None'  # Use string 'None' for consistency
        df['staking_type'] = 'None'

    # 4. Safely convert numeric types, handling potential string 'None' values
    numeric_cols = ['yield_amount', 'EffectiveStakingAmount', 'TradeTime']
    for col in numeric_cols:
        if col in df.columns:
            # Replace the string 'None' (from the .astype(str) step) with a value that to_numeric can handle
            df.loc[df[col] == 'None', col] = None
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0

    # Cast TradeTime to a proper integer timestamp
    df['TradeTime'] = df['TradeTime'].astype('int64')

    # 5. Enforce the final column schema to prevent data shifts
    final_columns = [
        'AssetEarnedCoin', 'EffectiveStakingAmount', 'yield_amount', 'TradeTime',
        'earn_type', 'asset_staked_coin', 'staking_type'
    ]

    # Add any missing columns and fill with appropriate defaults
    for col in final_columns:
        if col not in df.columns:
            df[col] = None  # Default to None if completely missing

    return df[final_columns]


def transform_data_to_mysql_format(df: pd.DataFrame, report_type: ReportType, report_number: int) -> pd.DataFrame:
    """Generic transform function that dispatches to specific transformers."""
    schema_info = get_schema_info(report_type, report_number)
    transform_func_name = schema_info.get('transform_function')

    if not transform_func_name:
        # This warning is now expected for reports that need no transformation
        logging.debug(f"No transform function defined for {report_type.value}[{report_number}]")
        return df

    transform_func = globals().get(transform_func_name)
    if not transform_func:
        raise ValueError(f"Transform function '{transform_func_name}' not found")

    # Pass report_number to the transformer if it accepts it
    sig = inspect.signature(transform_func)
    if 'report_number' in sig.parameters:
        return transform_func(df, report_number=report_number)
    else:
        return transform_func(df)


def save_csv_for_mysql(df: pd.DataFrame, mysql_upload_dir: str, base_filename: str) -> str:
    """Save DataFrame as header-less CSV for MySQL loading."""
    os.makedirs(mysql_upload_dir, exist_ok=True)

    # Clean string columns before writing
    df_clean = df.copy()
    for col in df_clean.select_dtypes(include=['object']).columns:
        df_clean[col] = df_clean[col].astype(str).str.replace('\r', '').str.replace('\n', ' ').str.strip()

    name, ext = os.path.splitext(base_filename)
    mysql_path = os.path.join(mysql_upload_dir, f"{name}_mysql{ext}")

    # Force Unix line endings
    df_clean.to_csv(mysql_path, index=False, header=False, encoding='utf-8', lineterminator='\n')

    logging.info(f"Created MySQL CSV: {mysql_path} with {len(df_clean)} rows")
    return mysql_path


# ------------ SQL HELPER FUNCTIONS ------------

def build_insert_ignore_sql(table: str, columns: List[str]) -> str:
    """Generate an INSERT IGNORE SQL string for the given table and columns."""
    col_list = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT IGNORE INTO `{table}` ({col_list}) VALUES ({placeholders})"


# ------------ DATABASE INSERT FUNCTIONS ------------
def process_report_data_generic(conn, df, report_type: ReportType, report_number: int, output_dir=None):
    """Generic processor that dispatches to specific transformers and handles database insertion."""

    # Get schema info
    schema_info = get_schema_info(report_type, report_number)
    table_name = schema_info['table']
    columns = schema_info['columns']

    # Transform using the generic dispatcher (which you already have!)
    mysql_df = transform_data_to_mysql_format(df, report_type, report_number)

    # Save CSV if needed
    csv_path = None
    if output_dir and not mysql_df.empty:
        csv_path = save_csv_for_mysql(
            mysql_df, output_dir,
            f"{table_name}_{int(time.time())}.csv"
        )

    # Load to database
    if not mysql_df.empty:
        rows = [tuple(row) for row in mysql_df.itertuples(index=False, name=None)]
        insert_with_load_data_infile(
            conn=conn,
            table=table_name,
            columns=columns,
            rows=rows,
            report_type=report_type,
            report_number=report_number
        )

    return len(mysql_df), csv_path


def insert_with_load_data_infile(conn, table: str, columns: List[str], rows: List[Tuple],
                                 commit: bool = True,
                                 report_type: ReportType = CURRENT_REPORT_TYPE,
                                 report_number: int = CURRENT_REPORT_NUMBER) -> None:
    """Enhanced LOAD DATA INFILE with schema awareness."""
    if not rows:
        logging.debug(f"No records to insert into {table}.")
        return

    # Get primary keys for this report type
    primary_keys = get_primary_keys(report_type, report_number)

    # Write CSV
    upload_dir = os.getenv('MYSQL_UPLOAD_DIR', MYSQL_UPLOAD_DIR)
    os.makedirs(upload_dir, exist_ok=True)
    filename = f"{table}_{uuid.uuid4().hex}.csv"
    path = os.path.join(upload_dir, filename)

    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            # Handle None values and pandas NaN
            sanitized_row = []
            for v in row:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    sanitized_row.append('\\N')
                elif isinstance(v, bool):
                    sanitized_row.append(int(v))
                else:
                    sanitized_row.append(v)
            writer.writerow(sanitized_row)

    # Load into temp table
    cursor = conn.cursor()
    temp = f"tmp_{table}_{os.getpid()}"

    try:
        cursor.execute(f"CREATE TEMPORARY TABLE `{temp}` LIKE `{table}`")
        cols = ", ".join(f"`{c}`" for c in columns)
        infile = path.replace('\\', '/')

        load_sql = (
            f"LOAD DATA INFILE '{infile}' IGNORE INTO TABLE `{temp}` "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
            f"LINES TERMINATED BY '\\n' ({cols})"
        )
        cursor.execute(load_sql)

        # Delete duplicates against the main table using schema-defined primary keys
        join_cond = ' AND '.join(f"t.`{k}`=m.`{k}`" for k in primary_keys)
        cursor.execute(
            f"DELETE t FROM `{temp}` t INNER JOIN `{table}` m ON {join_cond}"
        )

        # Insert remaining rows
        cursor.execute(
            f"INSERT INTO `{table}` ({cols}) SELECT {cols} FROM `{temp}`"
        )

        if commit:
            conn.commit()
        logging.info(f"Loaded and inserted rows into {table}, filtered duplicates.")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"LOAD DATA INFILE error for {table}: {e}")
        raise
    finally:
        cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS `{temp}`")
        cursor.close()
        if os.path.exists(path):
            os.remove(path)


# Add to bybit_tax_api_schema_and_inserts.py


# ------------ BACKWARD COMPATIBILITY FUNCTIONS ------------

# For existing code that expects the old function names
def transform_raw_data_to_mysql_format(df: pd.DataFrame) -> pd.DataFrame:
    """Backward compatibility wrapper for fiat deposit transformation."""
    return transform_fiat_data_to_mysql_format(df)
