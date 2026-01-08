"""
Bybit Tax API - Best of Both Worlds Combined Version

Features:
- Adaptive rate limiting with weighted request types (from adaptive_tax_export)
- Multi-mode output: CSV, SQL, or both (from scratch_30/dual_CSV_SQL)
- Proven API calls and concurrent job management (from working_csv)
- Custom start/end ranges specified at the start of runtime
- Class-based architecture with proper error handling
- MySQL database integration with bulk loading
- ORC file processing and data transformation
- Resume capability and progress reporting
"""

# ------------ IMPORTS ------------
from collections import OrderedDict, deque
import os
import time
import json
import hmac
import hashlib
import requests
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging
import threading
import queue
import math
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional
import argparse
import tzdata
import pyarrow as pa
import pyarrow.orc as orc

# Schema and credential imports
from bybit_tax_api_schema_and_inserts import (
     MYSQL_UPLOAD_DIR, ReportType, REPORT_SCHEMAS,
     process_report_data_generic, transform_data_to_mysql_format
)
from credentials import mysqldbconnector, get_bybit_tax_api_keys
from db_manager import db  # Import our new wrapper



# ------------ LOGGING CONFIGURATION ------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("bybit_tax_export")

# Handle timezone data for pyarrow
tzpkg_dir = os.path.dirname(tzdata.__file__)
tzdir = os.path.join(tzpkg_dir, "zoneinfo")
os.environ.setdefault("TZDIR", tzdir)

# ------------ CONFIGURATION CONSTANTS ------------

# API Configuration


REPORT_TYPE = "EARN"
REPORT_PART_NUMBER = 1
#
# Enter start date (YYYY-MM-DD)
# or UNIX timestamp: 1730067200, 1681903562610
# Enter end date (YYYY-MM-DD)
# or UNIX timestamp: 1752919800, 1755676800000
# 1733743562_1738927562
RECV_WINDOW = 5000

# Rate Limiting Configuration (based on research)
MAX_BURST_REQUESTS = 12  # Based on the 600/5sec limit and tax endpoint cost of 50
RATE_LIMIT_WINDOW = 60.0  # 60-second rolling window
MAX_CONCURRENT_JOBS = 4  # Conservative limit for stability

# Polling Configuration
POLL_INTERVAL_BASE = 5.0
POLL_INTERVAL_MAX = 30.0
MIN_JOB_AGE_FOR_SLOW_POLL = 30

# Windowing Configuration
MAX_WINDOW_DAYS = 60
TAX_SYNC_MARKER_FILE = "bybit_tax_sync_markers.log"


# ------------ ENUMS & DATA STRUCTURES ------------
# ------------ OUTPUT MODES ------------

class OutputMode(Enum):
    CSV_ONLY = "csv"
    SQL_ONLY = "sql"
    CSV_AND_SQL = "both"


# ------------ RATE LIMITING CLASSES ------------

class RequestType(Enum):
    CREATE_REPORT = "create"
    STATUS_CHECK = "status"
    GET_URLS = "url"


@dataclass
class RequestWeights:
    """Define the cost/weight of different request types for rate limiting"""
    CREATE_REPORT: int = 4  # Highest weight - most important to succeed
    STATUS_CHECK: int = 1  # Lowest weight - can afford to throttle
    GET_URLS: int = 2  # Medium weight


class AdaptiveRateLimiter:
    """
    Thread-safe adaptive rate limiter prioritizing different request types.
    """

    def __init__(self, max_burst: int = MAX_BURST_REQUESTS, window: float = RATE_LIMIT_WINDOW):
        self.max_burst = max_burst
        self.window = window
        self.request_history = deque()
        self.lock = threading.Lock()
        self.weights = RequestWeights()

    def wait_for_request(self, request_type: RequestType) -> float:
        """Wait if necessary to respect rate limits. Returns actual wait time."""
        now = time.monotonic()
        weight = getattr(self.weights, request_type.name)

        with self.lock:
            # Remove expired requests
            while self.request_history and now - self.request_history[0][0] > self.window:
                self.request_history.popleft()

            # Calculate current weight usage
            current_weight = sum(req[2] for req in self.request_history)

            # Determine if we need to wait
            if current_weight + weight > self.max_burst:
                if self.request_history:
                    oldest_time = self.request_history[0][0]
                    wait_time = max(0.1, (oldest_time + self.window) - now)
                else:
                    wait_time = 1.0
            else:
                wait_time = 0.0

        # Apply wait time
        if wait_time > 0:
            logger.debug("Rate limiting %s: waiting %.2fs", request_type.name, wait_time)
            time.sleep(wait_time)

        # Record this request
        with self.lock:
            self.request_history.append((time.monotonic(), request_type, weight))

        return wait_time


class AdaptivePollingStrategy:
    """Calculates optimal polling intervals based on job characteristics."""

    def __init__(self):
        self.base_interval = POLL_INTERVAL_BASE
        self.max_interval = POLL_INTERVAL_MAX

    def get_polling_interval(self, job_age_seconds: float, active_job_count: int) -> float:
        """Calculate the polling interval based on job age and system load."""
        # Age factor: longer intervals for older jobs
        if job_age_seconds <= MIN_JOB_AGE_FOR_SLOW_POLL:
            age_factor = 1.0
        else:
            age_factor = 1 + math.log10(job_age_seconds / MIN_JOB_AGE_FOR_SLOW_POLL)

        # Load factor: more active jobs = longer intervals
        load_factor = 1 + (active_job_count / 8.0)

        # Calculate final interval
        interval = self.base_interval * age_factor * load_factor
        return min(max(interval, self.base_interval), self.max_interval)


# ------------ API FUNCTIONS HTTP LOGIC ------------

def build_bybit_headers(
        api_key: str,
        private_key: str,
        payload_json: str,
        recv_window: int = RECV_WINDOW
) -> Dict[str, str]:
    timestamp_ms = int(time.time() * 1000)
    signature = generate_signature(timestamp_ms, api_key, recv_window, payload_json, private_key)
    return {
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-SIGN": signature,
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": str(timestamp_ms),
        "X-BAPI-RECV-WINDOW": str(recv_window),
        "Content-Type": "application/json"
    }


def generate_signature(timestamp_ms: int, api_key: str, recv_window: int, data_json_str: str, private_key: str) -> str:
    """Generate HMAC signature """
    plain = f"{timestamp_ms}{api_key}{recv_window}{data_json_str}"
    return hmac.new(private_key.encode("utf-8"), plain.encode("utf-8"), hashlib.sha256).hexdigest()


def create_report(api_key: str, private_key: str, report_type: str, number: int, start_s: int, end_s: int) -> str:
    """Create a new tax report job """
    payload = OrderedDict([
        ("startTime", str(start_s)),
        ("endTime", str(end_s)),
        ("type", report_type),
        ("number", str(number))
    ])
    json_body = json.dumps(payload, separators=(",", ":"))

    # Use the header factory
    headers = build_bybit_headers(api_key, private_key, json_body)

    url = "https://api.bybit.com/fht/compliance/tax/v3/private/create"
    resp = requests.post(url, headers=headers, data=json_body, timeout=(10, 60))
    resp.raise_for_status()
    data = resp.json()

    logger.debug("create_report response: retCode=%s", data.get("retCode"))
    if data.get("retCode") != 0:
        logger.error("create_report payload: %s", json_body)
        logger.error("create_report response: %s", data)
        raise RuntimeError(f"Bybit API create_report error {data.get('retCode')}: {data.get('retMsg')}")
    return str(data["result"]["queryId"])


def status_check(api_key: str, private_key: str, query_id: str) -> dict:
    """Check status"""
    payload = {"queryId": str(query_id)}
    json_body = json.dumps(payload, separators=(",", ":"))

    # Use the header factory
    headers = build_bybit_headers(api_key, private_key, json_body)

    url = "https://api.bybit.com/fht/compliance/tax/v3/private/status"
    resp = requests.post(url, headers=headers, data=json_body, timeout=(10, 30))

    logger.debug("Status check HTTP: %d for queryId %s", resp.status_code, query_id[:12])
    if resp.status_code == 403:
        logger.warning("403 Forbidden: Report status not accessible for %s", query_id[:12])
        # Return a dictionary that mimics the API response for this specific case
        return {"result": {"status": "403"}, "retCode": 0, "retMsg": "Locally-handled 403"}

    resp.raise_for_status()
    body = resp.json()
    if body.get("retCode") != 0:
        logger.error("status_check returned retCode=%s: %s", body.get("retCode"), body.get("retMsg"))
        raise RuntimeError("status_check returned error: " + str(body.get("retMsg")))
    return body


def get_report_urls(api_key: str, private_key: str, query_id: str) -> dict:
    """Get download URLs """
    payload = {"queryId": str(query_id)}
    json_body = json.dumps(payload, separators=(",", ":"))

    # Use the header factory
    headers = build_bybit_headers(api_key, private_key, json_body)

    url = "https://api.bybit.com/fht/compliance/tax/v3/private/url"
    resp = requests.post(url, headers=headers, data=json_body, timeout=(10, 30))
    resp.raise_for_status()
    data = resp.json()

    if data.get("retCode") != 0:
        raise RuntimeError("get_report_urls returned error: " + str(data.get("retMsg")))
    return data["result"]


def download_and_process_files(basepath: str, files: List[str], timeout: tuple = (10, 60)) -> pd.DataFrame:
    """Download and process ORC files """
    dfs: List[pd.DataFrame] = []
    basepath = basepath.rstrip("/") + "/"

    for part in files:
        if part.endswith("_SUCCESS"):
            continue

        url = basepath + part
        logger.info("Downloading ORC part: %s", url)
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.content

        if not data:
            logger.warning("Zero-length ORC for part: %s", part)
            continue

        buf = pa.BufferReader(data)
        reader = orc.ORCFile(buf)
        table = reader.read()
        df = table.to_pandas()
        logger.info("Read %d rows from part: %s", len(df), part)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# ------------ DATABASE OPERATIONS  ------------

# ------------ HELPER FUNCTIONS ------------

def generate_report_filename(report_type: str, report_number: int, start_s: int, end_s: int) -> str:
    """Generate a descriptive filename based on report parameters."""
    clean_type = report_type.replace("&", "AND")
    return f"report_{clean_type}_{report_number}_{start_s}_{end_s}.csv"


def create_tax_windows(start_dt: datetime, end_dt: datetime, window_days: int = MAX_WINDOW_DAYS) -> List[tuple]:
    """Generate non-overlapping time windows for Tax API batching."""
    windows = []
    current = start_dt
    window_delta = timedelta(days=window_days)

    while current < end_dt:
        window_end = min(current + window_delta, end_dt)
        windows.append((current, window_end))
        current = window_end

    logger.info("Created %d tax windows of %d days each", len(windows), window_days)
    return windows


def log_tax_window_complete(report_type: str, report_number: int, start_s: int, end_s: int, rows: int):
    """Log successful completion of a tax window for resume capability."""
    with open(TAX_SYNC_MARKER_FILE, "a") as f:
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"[TAX_COMPLETE] {report_type}_{report_number} {start_s} {end_s} {rows} {timestamp}\n")


# ------------ JOB STATE MANAGEMENT ------------

class JobState(Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PROCESSING = "processing"
    READY = "ready"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TaxJob:
    window_id: str
    report_type: str
    report_number: int
    start_s: int
    end_s: int
    output_mode: OutputMode = OutputMode.CSV_ONLY
    state: JobState = JobState.PENDING
    query_id: Optional[str] = None
    submitted_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    retries: int = 0
    rows_downloaded: int = 0
    csv_filename: Optional[str] = None
    last_status_check: Optional[float] = None


# ------------ ENHANCED JOB MANAGER ------------

class EnhancedTaxJobManager:
    def __init__(self, api_key: str, private_key: str, output_mode: OutputMode, report_type: str,
                 report_numbers: List[int]):
        self.api_key = api_key
        self.private_key = private_key
        self.output_mode = output_mode
        self.rate_limiter = AdaptiveRateLimiter()
        self.polling_strategy = AdaptivePollingStrategy()
        self.report_type = report_type  # NEW: Passed in (str, e.g., "EARN")
        self.report_numbers = report_numbers  # NEW: List[int], e.g., [1, 7]

        # Job management (unchanged)
        self.jobs: Dict[str, TaxJob] = {}
        self.submission_queue = queue.Queue()
        self.active_jobs: Dict[str, TaxJob] = {}
        self.download_queue = queue.Queue()

        # Threading controls (unchanged)
        self.running = False
        self.threads = []

        # Statistics (unchanged)
        self.total_submitted = 0
        self.total_completed = 0
        self.total_failed = 0
        # Load completed windows
        self.completed_windows = self._load_completed_windows()

    def _load_data_to_db(self, df: pd.DataFrame, table_name: str):
        """
        Replaces the complex CSV-export-load-infile logic with a simple call.
        """
        try:
            db.insert_dataframe(table_name, df)
        except Exception as e:
            logger.error(f"Failed to insert into DB: {e}")

    def _load_completed_windows(self) -> set:
        """Load completed tax windows from marker file for resume capability."""
        completed = set()
        if os.path.exists(TAX_SYNC_MARKER_FILE):
            with open(TAX_SYNC_MARKER_FILE, "r") as f:
                for line in f:
                    if line.startswith("[TAX_COMPLETE]"):
                        parts = line.strip().split()[1:]  # Skip [TAX_COMPLETE]
                        if len(parts) >= 4:
                            type_num = parts[0].split('_')  # e.g., EARN_1 -> ['EARN', '1']
                            report_type = type_num[0]
                            report_number = int(type_num[1])
                            start_s = int(parts[1])
                            end_s = int(parts[2])
                            completed.add((report_type, report_number, start_s, end_s))
        logger.info("Loaded %d completed windows from markers", len(completed))
        return completed

    def add_windows(self, windows: List[tuple]):
        for idx, (start_dt, end_dt) in enumerate(windows, 1):
            start_s = int(start_dt.timestamp())
            end_s = int(end_dt.timestamp())

            for report_number in self.report_numbers:
                # Skip if this specific type+number+window is already completed
                if (self.report_type, report_number, start_s, end_s) in self.completed_windows:
                    logger.info("Skipping completed window for %s[%d]: %d → %d", self.report_type, report_number,
                                start_s, end_s)
                    continue

                window_id = f"win{idx}_{report_number}"  # Unique per number
                filename = generate_report_filename(self.report_type, report_number, start_s, end_s)

                job = TaxJob(
                    window_id=window_id,
                    report_type=self.report_type,
                    report_number=report_number,
                    start_s=start_s,
                    end_s=end_s,
                    output_mode=self.output_mode,
                    csv_filename=filename
                )

                self.jobs[window_id] = job
                self.submission_queue.put(job)

        logger.info("Added %d jobs across %d report numbers", len(self.jobs), len(self.report_numbers))

    def start(self):
        """Start all worker threads."""
        self.running = True
        self.threads = [
            threading.Thread(target=self._adaptive_submission_worker, daemon=True),
            threading.Thread(target=self._adaptive_polling_worker, daemon=True),
            threading.Thread(target=self._download_worker, daemon=True),
            threading.Thread(target=self._progress_worker, daemon=True)
        ]
        for thread in self.threads:
            thread.start()
        logger.info("Started enhanced job manager with %d worker threads", len(self.threads))

    def stop(self):
        """Stop all worker threads."""
        self.running = False
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=5)
        logger.info("Stopped enhanced job manager")

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all jobs to complete or timeout."""
        start_time = time.time()
        while self.running:
            pending = sum(1 for job in self.jobs.values()
                          if job.state in [JobState.PENDING, JobState.SUBMITTED,
                                           JobState.PROCESSING, JobState.READY, JobState.DOWNLOADING])
            if pending == 0:
                logger.info("All jobs completed")
                return True

            if timeout and (time.time() - start_time) > timeout:
                logger.error("Timeout reached with %d jobs still pending", pending)
                return False

            time.sleep(5)
        return True

    def _adaptive_submission_worker(self):
        """Submit jobs with adaptive rate limiting."""
        while self.running:
            try:
                # Respect concurrent job limit
                if len(self.active_jobs) >= MAX_CONCURRENT_JOBS:
                    time.sleep(0.5)
                    continue

                # Get next job
                try:
                    job = self.submission_queue.get_nowait()
                except queue.Empty:
                    time.sleep(1)
                    continue

                # Apply adaptive rate limiting
                self.rate_limiter.wait_for_request(RequestType.CREATE_REPORT)

                # Submit using proven API call
                try:
                    query_id = create_report(
                        self.api_key, self.private_key,
                        job.report_type, job.report_number,
                        job.start_s, job.end_s
                    )

                    # Update job state
                    job.query_id = query_id
                    job.state = JobState.SUBMITTED
                    job.submitted_at = time.time()
                    self.active_jobs[query_id] = job
                    self.total_submitted += 1

                    logger.info("🚀 Submitted %s: queryId=%s (%d/%d active)",
                                job.window_id, query_id[:12],
                                len(self.active_jobs), MAX_CONCURRENT_JOBS)

                except Exception as e:
                    msg = str(e)
                    # Handle retry cases
                    if any(phrase in msg.lower() for phrase in ["limit", "403", "rate", "forbidden"]):
                        job.retries += 1
                        if job.retries <= 3:
                            logger.warning("⚠️ Retry %d for %s: %s",
                                           job.retries, job.window_id, msg)
                            time.sleep(5 * job.retries)
                            self.submission_queue.put(job)
                            continue

                    # Give up on this job
                    job.state = JobState.FAILED
                    job.error = msg
                    self.total_failed += 1
                    logger.error("❌ Failed to submit %s: %s", job.window_id, msg)

            except Exception as e:
                logger.error("Unexpected error in submission worker: %s", e)
                time.sleep(5)

    def _adaptive_polling_worker(self):
        """Poll active jobs with adaptive intervals."""
        while self.running:
            try:
                if not self.active_jobs:
                    time.sleep(2)
                    continue

                now = time.time()
                jobs_to_check = []

                # Determine which jobs need status checks
                for query_id, job in list(self.active_jobs.items()):
                    if job.state != JobState.SUBMITTED:
                        continue

                    job_age = now - job.submitted_at
                    active_count = len(self.active_jobs)

                    # Calculate optimal polling interval
                    optimal_interval = self.polling_strategy.get_polling_interval(job_age, active_count)

                    # Check if it's time to poll this job
                    last_check = job.last_status_check or job.submitted_at
                    time_since_check = now - last_check

                    if time_since_check >= optimal_interval:
                        jobs_to_check.append((job, optimal_interval))

                # Process status checks with rate limiting
                for job, interval in jobs_to_check:
                    try:
                        # Apply rate limiting
                        self.rate_limiter.wait_for_request(RequestType.STATUS_CHECK)
                        # Use proven API call
                        status_resp = status_check(self.api_key, self.private_key, job.query_id)
                        logger.debug("Status payload: %s", status_resp)
                        status = str(status_resp["result"]["status"])

                        if status == "2":  # Ready
                            job.state = JobState.READY
                            self.download_queue.put(job)
                            logger.info("✅ Job ready: %s (%.1fs processing)",
                                        job.window_id, time.time() - job.submitted_at)

                        elif status == "3":  # Failed
                            job.state = JobState.FAILED
                            job.error = "Server-side processing failed"
                            self.active_jobs.pop(job.query_id, None)
                            self.total_failed += 1
                            logger.error("❌ Job failed server-side: %s", job.window_id)
                        else:
                            # Still processing
                            logger.debug("⏳ Job %s still processing (next check in %.1fs)",
                                         job.window_id, interval)

                    except Exception as e:
                        logger.error("Polling error for %s: %s", job.window_id, e)
                        job.last_status_check = time.time()

                # Brief pause before next polling cycle
                time.sleep(1)

            except Exception as e:
                logger.error("Unexpected error in polling worker: %s", e)
                time.sleep(5)

    def _download_worker(self):
        """Download completed jobs."""
        while self.running:
            try:
                try:
                    job = self.download_queue.get(timeout=2)
                except queue.Empty:
                    continue

                if job.state != JobState.READY:
                    continue

                try:
                    job.state = JobState.DOWNLOADING
                    logger.info("⬇️ Starting download for %s", job.window_id)

                    # Apply rate limiting
                    self.rate_limiter.wait_for_request(RequestType.GET_URLS)
                    # Get download URLs
                    report_info = get_report_urls(self.api_key, self.private_key, job.query_id)
                    # Process the download
                    rows_processed = self._process_job_download(job, report_info)
                    # Mark complete
                    job.state = JobState.COMPLETED
                    job.completed_at = time.time()
                    job.rows_downloaded = rows_processed
                    self.total_completed += 1
                    # Remove from active tracking
                    self.active_jobs.pop(job.query_id, None)
                    # Log completion
                    log_tax_window_complete(
                        job.report_type, job.report_number,
                        job.start_s, job.end_s, rows_processed
                    )

                    total_time = job.completed_at - job.submitted_at
                    logger.info("✅ Completed %s: %d rows, %s (%.1fs total)",
                                job.window_id, rows_processed, job.csv_filename, total_time)

                except Exception as e:
                    job.state = JobState.FAILED
                    job.error = f"Download error: {e}"
                    self.active_jobs.pop(job.query_id, None)
                    self.total_failed += 1
                    logger.error("❌ Download failed for %s: %s", job.window_id, e)

            except Exception as e:
                logger.error("Unexpected error in download worker: %s", e)
                time.sleep(5)

    def _process_job_download(self, job: TaxJob, report_info: dict) -> int:
        """Process the download of a single job."""
        # Initialize rows_downloaded to ensure we always return an integer
        rows_downloaded = 0

        try:
            raw_url_field = report_info.get("url")
            if isinstance(raw_url_field, str):
                url_data = json.loads(raw_url_field)
            elif isinstance(raw_url_field, dict):
                url_data = raw_url_field
            else:
                raise RuntimeError("Unexpected url field type: " + str(type(raw_url_field)))

            basepath = (url_data.get("Basepath") or
                        url_data.get("basePath") or
                        url_data.get("basepath"))
            files = url_data.get("Files") or url_data.get("files") or []

            if not basepath or not files:
                logger.warning("No files to download for %s", job.window_id)
                return 0  # Explicitly return 0 for empty windows

            # Download and process files
            combined_df = download_and_process_files(basepath, files)
            rows_downloaded = len(combined_df)  # This ensures rows_downloaded is always an integer

            if not combined_df.empty:
                try:
                    # Process for the database if needed
                    if job.output_mode in (OutputMode.SQL_ONLY, OutputMode.CSV_AND_SQL):
                        conn = mysqldbconnector()
                        try:
                            # This generic function will now correctly look up and apply the right transformer
                            rows_loaded, csv_path = process_report_data_generic(
                                conn=conn,
                                df=combined_df,
                                report_type=ReportType(job.report_type.replace("&", "AND")),
                                report_number=job.report_number,
                                output_dir=MYSQL_UPLOAD_DIR if job.output_mode == OutputMode.CSV_AND_SQL else None
                            )

                            if rows_loaded > 0:
                                logger.info(
                                    f"Successfully processed {rows_loaded} rows"
                                    f" for {job.report_type}[{job.report_number}]")
                        finally:
                            if conn and conn.is_connected():
                                conn.close()

                    # Save to a clean CSV if the mode is CSV_ONLY
                    elif job.output_mode == OutputMode.CSV_ONLY:
                        # We must transform the data first to get the correct columns and format
                        transformed_df = transform_data_to_mysql_format(combined_df,
                                                                        ReportType(job.report_type.replace("&", "AND")),
                                                                        job.report_number)
                        transformed_df.to_csv(job.csv_filename, index=False, encoding="utf-8")
                        logger.info("Saved transformed CSV: %s", job.csv_filename)

                except Exception as e:
                    logger.error("Failed to process data for %s: %s", job.window_id, e)
                    # Fallback: save with timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_filename = f"report_RAW_{job.report_type}_{job.report_number}_{timestamp}.csv"
                    combined_df.to_csv(backup_filename, index=False, encoding="utf-8")
                    job.csv_filename = backup_filename
                    logger.warning("Used backup raw data filename: %s", backup_filename)
                    # Even in error cases, we still return the original row count
            else:
                logger.info("Empty dataset for %s, no processing needed", job.window_id)

            return rows_downloaded

        except Exception as e:
            logger.error("Download error for %s: %s", job.window_id, e)
            return 0  # Return 0 instead of letting an exception propagate with undefined rows_downloaded

    def _progress_worker(self):
        """Report progress periodically with rate limiting insights."""
        while self.running:
            time.sleep(30)

            # Job state summary
            states = {}
            for job in self.jobs.values():
                state = job.state.value
                states[state] = states.get(state, 0) + 1

            total_jobs = len(self.jobs)
            completed = states.get('completed', 0)
            failed = states.get('failed', 0)
            active = len(self.active_jobs)

            # Rate limiting stats
            with self.rate_limiter.lock:
                current_usage = len(self.rate_limiter.request_history)
                now = time.monotonic()
                recent_waits = sum(1 for req_tuple in self.rate_limiter.request_history
                                   if now - req_tuple[0] < 10)

            logger.info("📊 Progress: %d/%d complete, %d failed, %d active | Rate: %d/%d (recent: %d) | %s",
                        completed, total_jobs, failed, active,
                        current_usage, MAX_BURST_REQUESTS, recent_waits,
                        {k: v for k, v in states.items() if v > 0})


# ------------ ARGUMENT PARSING ------------

def parse_arguments():
    parser = argparse.ArgumentParser(description="Bybit Tax API CLI")

    # Existing args…
    parser.add_argument("--start", type=int, help="Start UNIX timestamp (seconds)")
    parser.add_argument("--end", type=int, help="End UNIX timestamp (seconds)")
    parser.add_argument("--week", action="store_true", help="Fetch last 7 days")
    parser.add_argument(
        "--mode", choices=["csv", "sql", "both"],
        default="sql",
        help="Output mode"
    )
    parser.add_argument(
        "--reports",
        type=str, default="",
        help="Semi-colon separated list of report specs. "
             "Format example: 'EARN:1,7;DEPOSIT_WITHDRAWAL:3'. "
             "Report type must be enum NAME (e.g. EARN, DEPOSIT_WITHDRAWAL)."
    )

    # Updated: Report type (unchanged)
    parser.add_argument(
        "--report-type",
        type=str,
        choices=[rt.name for rt in ReportType],
        default=ReportType.EARN.name,
        help="Report type (e.g. EARN, TRADE, PNL, AIRDROP, etc.)"
    )

    # NEW: Plural report-numbers, comma-separated
    parser.add_argument(
        "--report-numbers",
        type=str,
        default="1",
        help="Comma-separated report numbers for the given type, e.g., '1,7'"
    )

    return parser.parse_args()


# --- helper: parse --reports argument ---
def parse_reports_arg(reports_arg: str):
    """
    Parse a reports string into a list of tuples: [(ReportType, [int,...]), ...]
    Format expected: "EARN:1,7;DEPOSIT_WITHDRAWAL:3"
    Report types must be enum NAMES (e.g., EARN, DEPOSIT_WITHDRAWAL, TRADE).
    """
    specs = []
    if not reports_arg:
        return specs

    for block in reports_arg.split(';'):
        block = block.strip()
        if not block:
            continue
        try:
            rtype_str, nums_str = block.split(':', 1)
        except ValueError:
            raise ValueError(f"Invalid reports fragment: '{block}'. Expected 'TYPE:1,2'")

        rtype_str = rtype_str.strip()
        try:
            rtype = ReportType[rtype_str]
        except KeyError:
            raise ValueError(f"Unknown report type name: '{rtype_str}'. Use one of {[r.name for r in ReportType]}")

        nums = [int(x.strip()) for x in nums_str.split(',') if x.strip()]
        if not nums:
            raise ValueError(f"No report numbers specified for {rtype_str}")
        # Validate pairs exist in REPORT_SCHEMAS
        for n in nums:
            if (rtype, n) not in REPORT_SCHEMAS:
                raise ValueError(f"No schema defined for {rtype.name}[{n}]")
        specs.append((rtype, nums))
    return specs


def determine_date_range(args) -> tuple:
    """Determine start and end timestamps from arguments."""
    if args.start is not None and args.end is not None:
        start_s = args.start
        end_s = args.end
    elif args.week:
        end_s = int(time.time())
        start_s = end_s - 7 * 24 * 3600
    else:
        # Interactive input
        start_input = input("Enter start date (YYYY-MM-DD) or UNIX timestamp: ").strip()
        end_input = input("Enter end date (YYYY-MM-DD) or UNIX timestamp: ").strip()

        def parse_ts(value):
            # Try as integer
            ts = int(value)
            # If it’s in milliseconds (>=10^12), convert to seconds
            if ts > 1e12:
                return ts / 1000
            return ts

        try:
            start_s = parse_ts(start_input)
            end_s = parse_ts(end_input)
        except ValueError:
            # Fallback to date parsing
            start_s = int(datetime.strptime(start_input, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            end_s = int(datetime.strptime(end_input, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    if end_s <= start_s:
        logger.error("Invalid range: end must be > start")
        sys.exit(1)

    logger.info("Using interactive dates: %d → %d", int(start_s), int(end_s))
    return int(start_s), int(end_s)


# ------------ MAIN FUNCTION ------------

def main():
    logger.info("🚀 === Bybit Tax API starting ===")

    try:
        # Get credentials
        api_key, private_key = get_bybit_tax_api_keys()
        # Parse arguments
        args = parse_arguments()
        report_numbers = [int(n.strip()) for n in args.report_numbers.split(',')]

        # Determine output mode
        output_mode = OutputMode(args.mode)
        logger.info("Output mode: %s", output_mode.value)

        # Check MySQL availability for SQL modes
        # Optional: Verify DB connection on startup if SQL mode is requested
        if output_mode in (OutputMode.SQL_ONLY, OutputMode.CSV_AND_SQL):
            try:
                # A simple connectivity check
                conn = db.get_connection()
                conn.close()
                logger.info(f"Database connection verified ({db.db_type})")
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                sys.exit(1)

        # Determine date range
        start_s, end_s = determine_date_range(args)
        if end_s <= start_s:
            logger.error("Invalid range: end must be > start")
            sys.exit(1)

            # --- NEW: multi-report minimal loop (sequential) ---
            # Build report specs (use --reports if provided; otherwise fallback to legacy flags)
        # NEW: Override the report type "&" number from CLI arguments
        try:
            report_type = ReportType[args.report_type]  # e.g. ReportType.EARN
            report_type_str = report_type.value  # e.g., "EARN"
            job_manager = EnhancedTaxJobManager(
                api_key, private_key, output_mode,
                report_type=report_type_str,
                report_numbers=report_numbers
            )
            for num in report_numbers:
                if (report_type, num) not in REPORT_SCHEMAS:
                    logger.error(f"Invalid report number {num} for type {report_type.value}")
                    sys.exit(1)
        except KeyError:
            logger.error("Invalid report type: %s", args.report_type)
            sys.exit(1)

        logger.info("Selected report: %s[%s]", report_type_str, ','.join(map(str, report_numbers)))

        # Create windows and run
        start_dt = datetime.fromtimestamp(start_s, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_s, tz=timezone.utc)
        windows = create_tax_windows(start_dt, end_dt, MAX_WINDOW_DAYS)
        if not windows:
            logger.info("No windows to process")
            return

        logger.info("🔄 Processing %d windows covering %.1f days",
                    len(windows), (end_dt - start_dt).days)

        # Multi-report mode (runs before legacy single-report)
        specs = parse_reports_arg(getattr(args, "reports", ""))
        if specs:
            for rtype, nums in specs:
                logger.info("Selected report: %s[%s]", rtype.value, ','.join(map(str, nums)))
                job_manager = EnhancedTaxJobManager(
                    api_key, private_key, output_mode,
                    report_type=rtype.value,
                    report_numbers=nums
                )
                job_manager.add_windows(windows)
                job_manager.start()
                job_manager.wait_for_completion(timeout=1440)
                job_manager.stop()

        logger.info("✅ Finished: %d completed | %d failed",
                    job_manager.total_completed,
                    job_manager.total_failed)

    except (ValueError, RuntimeError) as e:
        logger.error("❌ Application error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
