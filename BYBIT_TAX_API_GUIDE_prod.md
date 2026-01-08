# Bybit Tax Sync – Schema-Driven, Extensible Pipeline

## Overview
This script implements a **schema-driven, extensible data pipeline** for Bybit Tax API reports.
It is designed to be **"Zero Config"** for casual users (using SQLite) while supporting **Power Users** (using MySQL) with the same codebase.

Instead of hard-coding logic for each report type, it uses a central `REPORT_SCHEMAS` configuration to dispatch raw ORC data through the appropriate transformation and loading steps.
You simply define new report types in the schema, and the generic processor handles the rest—no changes required in the main script.

## Key Features
- **Zero-Config Database**: Defaults to a local SQLite file (`bybit_tax_data.db`). No server setup required.
- **MySQL Support**: Optional full integration for power users via `.env` configuration.
- **Schema-Driven Processing**: All report types are defined in `bybit_tax_api_schema_and_inserts.py`. Add new reports without touching the main script.
- **Resumable Sync**: Progress is tracked in `bybit_tax_sync_markers.log`. If interrupted, the script resumes exactly where it left off.
- **Adaptive Rate Limiting**: Smart handling of Bybit's complex weight-based rate limits (600 points/5 sec).
- **Multi-Output Modes**: CSV-only, SQL-only, or both modes depending on your needs.

## Supported Report Types
| Report Type          | Number | Description                         | Destination Table     | Transformer                          |
|----------------------|:------:|-------------------------------------|-----------------------|--------------------------------------|
| DEPOSIT_WITHDRAWAL   |   1    | Get Crypto Deposit History          | crypto_deposits       | None                                 |
| DEPOSIT_WITHDRAWAL   |   2    | Get P2P Deposit History             | p2p_deposits          | None                                 |
| DEPOSIT_WITHDRAWAL   |   3    | Get Fiat Deposit and Withdraw History | fiat_deposits       | transform_fiat_data_to_mysql_format  |
| DEPOSIT_WITHDRAWAL   |   4    | Get Express Order Deposit History   | express_deposits      | None                                 |
| DEPOSIT_WITHDRAWAL   |   5    | Get Third Party Deposit History     | third_party_deposits  | None                                 |
| DEPOSIT_WITHDRAWAL   |   6    | Get Crypto Withdraw History         | crypto_withdrawals    | None                                 |
| DEPOSIT_WITHDRAWAL   |   7    | Get NFT Deposit and Withdrawal History | nft_transactions    | None                                |
| TRADE                |   1    | Get Spot Trade History              | spot_trades           | None                                 |
| TRADE                |   2    | Get Contract Trade History          | contract_trades       | None                                 |
| EARN                 |   1    | Get BybitSavings Yield History      | bybit_earn            | transform_earn_data                  |
| EARN                 |   7    | Get Launchpool Yield History        | bybit_earn            | transform_earn_data                  |
| AIRDROP              |   1    | Airdrop Events                      | airdrop_events        | None                                 |

> **Multi-Number Support**: The CLI accepts multiple report numbers for a single report type via `--report-numbers "1,7"`. Internally, the job manager creates one job per number for each time window, enabling efficient parallel processing.

## Installation

### Step 1: Clone the Repository
```bash
git clone https://github.com/yourusername/bybit-tax-sync.git
cd bybit-tax-sync
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

This installs:
- `pandas` – Data manipulation
- `requests` – HTTP API calls
- `python-dotenv` – Environment variable loading
- `pyarrow` – ORC file parsing
- `mysql-connector-python` – Optional MySQL support
- `tzdata` – Timezone data for pyarrow

### Step 3: Configure Environment
Copy the example environment file and add your Bybit API credentials:
```bash
cp .env.example .env
```

Edit `.env` and add your Bybit Tax API keys:
```ini
BYBIT_TAX_API_KEY=your_api_key_here
BYBIT_TAX_PRIVATE_KEY=your_private_key_here
```

## Configuration (.env)

All configuration happens via the `.env` file. You can customize your database backend.

### Default (SQLite – Zero Config)
```ini
DB_TYPE=sqlite
SQLITE_DB_PATH=bybit_tax_data.db
BYBIT_TAX_API_KEY=your_api_key
BYBIT_TAX_PRIVATE_KEY=your_private_key
```

### Power User (MySQL)
```ini
DB_TYPE=mysql
MYSQLhost=localhost
MYSQLuser=root
MYSQLpassword=your_password
MYSQLdatabase=crypto_db
BYBIT_TAX_API_KEY=your_api_key
BYBIT_TAX_PRIVATE_KEY=your_private_key
```

## Getting Bybit Tax API Keys

1. Log in to your [Bybit Account](https://www.bybit.com).
2. Navigate to **Account** → **API Management**.
3. Create a new API key with **Tax Reporting** read permissions.
4. Copy the API Key and Private Key into your `.env` file.

## Usage

### Basic Usage (Interactive Mode)
Runs with defaults (EARN reports, last 7 days):
```bash
python bybit_tax_sync.py
```
The script will prompt you for date ranges if not provided.

### Command Line Options
```bash
# Run for Fiat Deposits (DEPOSIT_WITHDRAWAL[3])
python bybit_tax_sync.py --report-type DEPOSIT_WITHDRAWAL --report-numbers 3

# Run for multiple subtypes (Savings and Launchpool)
python bybit_tax_sync.py --report-type EARN --report-numbers 1,7

# Specific date range (UNIX timestamps)
python bybit_tax_sync.py --start 1704067200 --end 1735689600

# Last 7 days
python bybit_tax_sync.py --week

# CSV-only mode (no database)
python bybit_tax_sync.py --mode csv

# SQL-only mode (no CSV files)
python bybit_tax_sync.py --mode sql

# Both CSV and SQL
python bybit_tax_sync.py --mode both
```

### Output Modes

**CSV Only (`--mode csv`)**
- Produces standalone CSV files per time window.
- Ideal for portable, database-free workflows.
- Perfect for development-time debugging of transformations.
- Files: `report_EARN_1_1704067200_1735689600.csv`

**SQL Only (`--mode sql`)**
- Loads data directly into your configured database (SQLite or MySQL).
- Automatically deduplicates rows based on primary keys.
- No CSV files retained.

**Both (`--mode both`)**
- Creates both CSV files and database records.
- Useful for backup and audit trails.

## Data Transformation Pipeline

The script follows this sequence:

1. **Create Report Job**: Submits a request to Bybit's Tax API.
2. **Poll for Completion**: Checks job status with adaptive polling intervals.
3. **Download ORC Files**: Fetches raw ORC-format data from Bybit's storage.
4. **Merge ORC Parts**: Combines multi-part ORC files into a single DataFrame.
5. **Schema Lookup**: Identifies the destination table and transformer via `REPORT_SCHEMAS`.
6. **Transform Data**: Calls the appropriate `transform_*_data()` function for cleanup.
7. **CSV Export** (if enabled): Saves clean data to a timestamped CSV.
8. **Database Load** (if enabled): Inserts into SQLite or MySQL, deduplicating based on primary keys.

## Adding New Report Types

A key feature is the **extensibility without modifying the main script**. To support a new report simply edit the schema file as follows:

### Step 1: Define the Schema
Edit `bybit_tax_api_schema_and_inserts.py` and add an entry to `REPORT_SCHEMAS`:

```python
REPORT_SCHEMAS[(ReportType.OPTIONS, 1)] = {
    'table': 'options_trades',
    'description': 'Get Options Trade History',
    'columns': ['orderId', 'symbol', 'execPrice', 'execQty', 'execFee', 'side', 'execTime', 'source_exchange'],
    'primary_keys': ['orderId', 'execTime'],
    'transform_function': 'transform_options_data'
}
```

### Step 2: Implement the Transformer
Add a transformation function in the same file:

```python
def transform_options_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform raw Options API data to database format."""
    if df.empty:
        return pd.DataFrame()
    
    # Rename columns to match schema
    df = df.rename(columns={'tradeId': 'orderId'})
    
    # Convert timestamps
    df['execTime'] = pd.to_datetime(df['execTime'], unit='ms')
    
    # Ensure source
    df['source_exchange'] = 'bybit'
    
    return df[['orderId', 'symbol', 'execPrice', 'execQty', 'execFee', 'side', 'execTime', 'source_exchange']]
```

### Step 3: Run the Script
```bash
python bybit_tax_sync.py --report-type OPTIONS --report-numbers 1
```

The main script will **automatically** find your schema, transformer, and table definition.

## Error Handling and Recovery

### Automatic Retries
- **Rate Limits (HTTP 429)**: Automatically retries with exponential backoff (up to 3 attempts).
- **Temporary Errors (HTTP 403)**: Same retry logic.
- **Network Timeouts**: Graceful handling with detailed logging.

### Resume Capability
All completed time windows are logged to `bybit_tax_sync_markers.log`. If the script crashes or is stopped:
```bash
# Simply run it again—it will pick up where it left off
python bybit_tax_sync.py
```

### Database Fallback
If your database is unavailable (e.g., MySQL server down), the script:
1. Logs a warning.
2. Falls back to CSV-only mode.
3. Saves data to CSV so you don't lose it.
4. Allows you to retry with database later.

## Performance Optimizations

### Adaptive Rate Limiting
- Weights different request types (API calls cost more than status checks).
- Tracks 60-second rolling window of requests.
- Pauses automatically when approaching the 600 points/5-sec limit.

### Intelligent Polling
- Polling intervals scale based on job age and system load.
- Older jobs are checked less frequently to conserve API quota.

### Concurrent Processing
- Submits up to 4 jobs in parallel.
- Downloads and processes while new jobs are submitted.
- Maximizes throughput without hitting rate limits.

## Logging and Monitoring

The script logs at multiple levels:

- **INFO**: Job lifecycle events (submitted, completed, failed).
- **DEBUG**: Detailed API responses and rate-limiter waits.
- **WARNING**: Retries, missing data, database fallback.
- **ERROR**: Fatal failures (bad credentials, schema errors).

Example log output:
```
2025-12-30T14:22:33Z | INFO   | bybit_tax_export | 🚀 === Bybit Tax API starting ===
2025-12-30T14:22:34Z | INFO   | bybit_tax_export | Output mode: csv
2025-12-30T14:22:35Z | INFO   | bybit_tax_export | Window 1/5: 2025-01-01 to 2025-03-01
📊 Progress: 3/5 complete, 0 failed, 2 active | Rate: 2/5 (recent: 1) | {'completed': 3}
2025-12-30T14:23:10Z | INFO   | bybit_tax_export | ✅ Finished: 5 completed | 0 failed
```

## File Structure

```
./
├── bybit_tax_sync.py                 # Main script (Entry Point)
├── bybit_tax_api_schema_and_inserts.py  # Schema definitions & Transformers
├── db_manager.py                     # Database abstraction (SQLite/MySQL)
├── credentials.py                    # Internal credential loader
├── .env                              # Your API Keys & Config (DO NOT COMMIT)
├── .env.example                      # Template for users
├── requirements.txt                  # Python dependencies
├── README.md                         # This file
└── bybit_tax_sync_markers.log        # Resume tracking file (auto-generated)
```

## Troubleshooting

### "Database connection failed"
**Check your `.env`:**
```bash
# Verify DB_TYPE is set correctly
grep DB_TYPE .env

# If using SQLite, ensure write permissions in current folder
ls -la bybit_tax_data.db

# If using MySQL, test connectivity
mysql -h localhost -u root -p crypto_db
```

### "Rate Limit Exceeded" or "403 Forbidden"
The script handles this automatically:
1. Waits and retries up to 3 times.
2. Uses exponential backoff (wait 2s, then 4s, then 8s).
3. Logs the wait time in DEBUG mode.

Just let the script run; it will recover gracefully.

### "No Data Found" / Empty CSV
This is normal/expected if you didn't have transactions in that specific time window. The script will:
1. Log a warning (no rows returned).
2. Move to the next time window.
3. Continue processing.

### "ModuleNotFoundError: No module named 'mysql.connector'"
**For SQLite users (default):** You don't need MySQL. The error is harmless if using `DB_TYPE=sqlite`.

**For MySQL users:** Install the connector:
```bash
pip install mysql-connector-python
```

## Advanced: Database Schemas

### SQLite (Default)
When using SQLite, tables are auto-created on first insert with default types.

### MySQL (Power User)
If using MySQL, consider creating tables with proper types for better performance:

```sql
CREATE TABLE fiat_deposits (
  id INT AUTO_INCREMENT PRIMARY KEY,
  orderNo VARCHAR(255) UNIQUE NOT NULL,
  fiatCurrency VARCHAR(10) NOT NULL,
  indicatedAmount DECIMAL(20,8) NOT NULL,
  amount DECIMAL(20,8) NOT NULL,
  totalFee DECIMAL(20,8) NOT NULL,
  method VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL,
  createTime DATETIME NOT NULL,
  source_exchange VARCHAR(50) NOT NULL,
  INDEX idx_createTime (createTime)
);

CREATE TABLE bybit_earn (
  AssetEarnedCoin VARCHAR(50) NOT NULL,
  EffectiveStakingAmount DECIMAL(20,8),
  yield_amount DECIMAL(20,8),
  TradeTime DATETIME NOT NULL,
  earn_type VARCHAR(50) NOT NULL,
  asset_staked_coin VARCHAR(50),
  staking_type VARCHAR(50),
  PRIMARY KEY (AssetEarnedCoin, TradeTime, earn_type),
  INDEX idx_TradeTime (TradeTime)
);

CREATE TABLE airdrop_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  Coin VARCHAR(50),
  FinalAmount DECIMAL(30,18),
  TransferType VARCHAR(50),
  TransferDescription VARCHAR(100),
  CompletedTime DATETIME,
  source_exchange VARCHAR(50),
  INDEX idx_CompletedTime (CompletedTime)
);
```

## Rate Limit Information

- **API Cost**: 50 points per tax report request.
- **Limit**: 600 points per 5-second window per IP.
- **This Pipeline**: Uses adaptive bursting (~12 req/min) to stay well below limits.
- **Behavior**: If approaching the limit, the script automatically pauses and waits.


## API Key Requirements

Your Bybit Tax API key must have:
- ✅ **Read** permissions (not write).
- ✅ **Tax Reporting** scope.

Create a dedicated API key for this tool (never use your trading keys).

## License

This tool is provided as-is for personal use and tax compliance. **Not financial advice.** Verify all reports with a tax professional before filing.

## Contributing

Found a bug? Want to add support for a new report type? Submit an issue or pull request!

---

**Last Updated**: December 2025