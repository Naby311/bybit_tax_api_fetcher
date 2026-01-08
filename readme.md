# Bybit Tax API – Schema-Driven, Extensible Pipeline

## Overview  
This script suite implements a **schema-driven, extensible data pipeline** for Bybit Tax API reports. Instead of hard-coding logic for each report type, it uses a central `REPORT_SCHEMAS` configuration to dispatch raw ORC data through the correct transformation and loading steps. You simply define new report types in the schema, and the generic processor handles the rest without requiring changes in the main script.

## Architecture & Key Improvements  
- **Schema-Driven Processing**: All report types and their database/CSV mappings are defined in `bybit_tax_api_schema_and_inserts.py` under `REPORT_SCHEMAS`.  
- **Generic Processor**: A single `process_report_data_generic` function looks up the schema, transforms data via the specified transformer, writes CSV if needed, and loads into the correct MySQL table.  
- **Extensibility**: To support a new report, add its definition to `REPORT_SCHEMAS` (table name, columns, primary keys, transform function) and implement its `transform_*_data` function.  
- Suggested Replacement:

Multi-Report Support: Comes with pre-configured schemas for over 10 different report types, including deposits, withdrawals, trades, earnings, and airdrops, each mapping to a dedicated database table.
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

> Multi-number support
> The CLI accepts multiple report numbers for a single report type via --report-numbers "a,b,c".
> Internally, the job manager creates one job per number for each time window, enabling EARN[1,7] to run in one execution.


## Installation  
```bash
pip install pandas pyarrow requests tzdata
# Optional: MySQL connector for SQL modes
pip install mysql-connector-python
```

## Configuration  

### Schema Configuration  
Edit `bybit_tax_api_schema_and_inserts.py`’s `REPORT_SCHEMAS` to add or modify report types:
```python
REPORT_SCHEMAS[(ReportType.NEW_TYPE, N)] = {
    'table': 'new_table_name',
    'description': 'Get New Report',
    'columns': [...],
    'primary_keys': [...],
    'transform_function': 'transform_new_data'
}
```

## Usage
Describes the usage below. If run without any options directly in the script will use the below defaults  
### Defaults
- report-type: EARN
- report-numbers: 1
- mode: sql

### Command Line Options  
```bash
# CSV only (default)
python bybit_tax_api_combined_best.py --mode csv

# SQL only
python bybit_tax_api_combined_best.py --mode sql

# Both CSV and SQL
python bybit_tax_api_combined_best.py --mode both

# Date range as UNIX timestamps
python bybit_tax_api_combined_best.py --start 1704067200 --end 1735689600

# Last 7 days
python bybit_tax_api_combined_best.py --week

# Interactive date prompts
python bybit_tax_api_combined_best.py

# Run for EARN[1] reports (default) in SQL mode
python bybit_tax_api_combined_best.py --mode sql

# Fetch Fiat Deposits (DEPOSIT&WITHDRAWAL[3]) for a specific date range
python bybit_tax_api_combined_best.py --report-type DEPOSIT_WITHDRAWAL --report-number 3 --start 1704067200 --end 1735689600

# Fetch Airdrops (AIRDROP[1]) for the last week into a CSV file
python bybit_tax_api_combined_best.py --report-type AIRDROP --report-number 1 --week --mode csv

# Interactive date prompts for a specific report
python bybit_tax_api_combined_best.py --report-type TRADE --report-number 1
# Note: use --report-numbers (plural) and supply comma-separated numbers as needed
python bybit_tax_api_combined_best.py --report-type EARN --report-numbers 1,7
python bybit_tax_api_combined_best.py --report-type DEPOSIT_WITHDRAWAL --report-numbers 3 --start 1704067200 --end 1735689600
python bybit_tax_api_combined_best.py --report-type AIRDROP --report-numbers 1 --week --mode csv
python bybit_tax_api_combined_best.py --report-type TRADE --report-numbers 1

```

> **Why CSV Only?**  
> Choosing `--mode csv` generates standalone CSVs without a database. If you simply want an easy test drive of the scripts functionality, use this, or to debug transformations when adding new report types or in environments without MySQL.

### Environment Variables  
```bash
export BYBIT_TAX_API_KEY="your_api_key"
export BYBIT_TAX_PRIVATE_KEY="your_private_key"
```

## Data Transformation Pipeline  

1. **Download ORC Parts**: Merge ORC files into a Pandas DataFrame.  
2. **Schema Lookup**: Identify destination table, columns, and transformer via `REPORT_SCHEMAS`.  
3. **Transform**: Call `transform_data_to_mysql_format`, which dispatches to the specific `transform_*_data` function.  
4. **CSV Output**: If in CSV mode, save a clean CSV named `{table}_{timestamp}.csv`.  
5. **Database Load**: If in SQL mode, write headerless CSV, then `LOAD DATA INFILE` into the correct table with deduplication.

## Output Modes  

### CSV Only (`--mode csv`)  
- Produces one CSV per time window (e.g., `bybit_earn_1707823562.csv`).  
- Ideal for **portable**, **database-free** workflows and for development-time debugging.

### SQL Only (`--mode sql`)  
- Transforms data and bulk loads into the MySQL table defined in the schema (e.g., `bybit_earn`).  
- Automatically deduplicates rows based on primary keys.  
- No CSVs retained.

### Both (`--mode both`)  
- Combines the above: creates CSVs and loads into the database.

## MySQL Database Schema  

```sql
CREATE TABLE fiat_deposits (
  orderNo VARCHAR(255) PRIMARY KEY,
  fiatCurrency VARCHAR(10),
  indicatedAmount DECIMAL(20,8),
  amount DECIMAL(20,8),
  totalFee DECIMAL(20,8),
  method VARCHAR(255),
  status VARCHAR(50),
  createTime DATETIME,
  source_exchange VARCHAR(50)
);

CREATE TABLE bybit_earn (
  AssetEarnedCoin VARCHAR(50) NOT NULL,
  EffectiveStakingAmount DECIMAL(20,8),
  yield_amount DECIMAL(20,8),
  TradeTime DATETIME NOT NULL,
  earn_type VARCHAR(50) NOT NULL,
  asset_staked_coin VARCHAR(50),
  staking_type VARCHAR(50),
  PRIMARY KEY (AssetEarnedCoin, TradeTime, earn_type)
);

CREATE TABLE airdrop_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  Coin VARCHAR(50),
  FinalAmount DECIMAL(30,18),
  TransferType VARCHAR(50),
  TransferDescription VARCHAR(100),
  CompletedTime DATETIME,
  source_exchange VARCHAR(50)
);


```

## Adding New Report Types  
1. **Define Schema**: Add entry to `REPORT_SCHEMAS` with table, columns, primary keys, and `transform_function` name.  
2. **Implement Transformer**: Write `transform_new_report_data(df)` to normalize raw DataFrame.  
3. **Run**: Execute the script using the --report-type and --report-number flags for your new report."

## Logging and Monitoring  
- **INFO**: Job lifecycle events  
- **DEBUG**: API responses, rate-limiter waits  
- **WARNING**: Retries, missing data  
- **ERROR**: Fatal failures, database errors  

Progress snapshot example:  
```
📊 Progress: 10/12 complete, 0 failed, 2 active | Rate: 5/12 (recent: 1) | {'completed': 10}
```

## Error Handling and Recovery  
- **Retries**: Up to 3 for rate limits and 403 errors with exponential backoff.  
- **Resume**: Each completed window logged to `bybit_tax_sync_markers.log`; resumes from last success on restart.  
- **Fallback**: Database failures fall back to CSV-only backup.

## Performance Optimizations  
- **Adaptive Rate Limiter**: Weighted bursts within 60-second windows.  
- **Intelligent Polling**: Dynamic intervals based on job age and concurrency.  
- **Concurrent Jobs**: 4 parallel API jobs with separate threads for submission, polling, downloading, and progress.

## Troubleshooting  

1. **MySQL Unavailable**: Falls back to CSV-only. Install `mysql-connector-python`.  
2. **Rate Limit Errors**: Auto-retries handled by `AdaptiveRateLimiter`.  
3. **No Data**: Time window may have no activity—normal behavior.

## API Key Requirements  
Your Tax API key must have read permissions and tax-reporting access. Create on the Bybit Tax API page.

## Rate Limit Information  
- **Cost**: 50 points per tax API request  
- **Limit**: 600 points per 5 seconds per IP  
- This pipeline uses up to 12 points per 60-second burst, tracking history to avoid throttling.

## File Structure  
```
./
├── bybit_tax_api_combined_best.py    # Main script
├── bybit_tax_api_schema_and_inserts.py  # Central schema/config & processors
├── bybit_tax_sync_markers.log        # Resume tracking
├── report_<table>_<start>_<end>.csv  # CSV outputs per window
└── credentials.py                    # MySQL connector (optional)
```

---

This guide covers the new **schema-driven architecture**, the **continued utility of CSV-only mode**, and **extension steps** for additional report types. Ensure you update the schema file and transformer functions as needed for future expansions.
------------------

DISCLAIMER
The authors and contributors of this software do not provide tax, legal, accounting, or financial advice. This software and its content are provided for informational purposes only, and as such should not be relied upon for tax, legal, accounting, or financial advice.

You should obtain specific professional advice from a professional accountant, tax, or legal/financial advisor before you take any action.

This software is provided 'as is'. The authors do not give any warranties of any kind, express or implied, as to the suitability or usability of this software, or any of its content.
