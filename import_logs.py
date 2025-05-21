#!/usr/bin/env python3

import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import unquote_plus
import psycopg2
import psycopg2.extras
from user_agents import parse

# Configurable constants
LOCAL_LOG_DIR = Path("./data/raw-logs/")
BATCH_SIZE = 5000

DB_CONFIG = {
    "dbname": "iis_logs",
    "user": "atodd",
    "password": "",
    "host": "localhost",
    "port": 5432
}

KNOWN_BOT_KEYWORDS = [
    "bot", "spider", "crawler", "slurp", "baidu", "yandex", "ahrefs", "semrush",
    "mj12bot", "bingpreview", "facebookexternalhit", "archive.org_bot", "petalbot",
    "sogou", "duckduckbot", "qwantbot", "pinterest", "linkedinbot", "embedly",
    "whatsapp", "telegrambot"
]

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def already_imported(filename, conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM tbl_imported_files WHERE filename = %s", (filename,))
        return cursor.fetchone() is not None

def mark_as_imported(filename, conn):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO tbl_imported_files (filename) VALUES (%s)", (filename,))
    conn.commit()

def safe_text(value):
    if not isinstance(value, str):
        return value
    if "\x00" in value:
        log_import_error(f"NUL byte found and removed: {repr(value[:100])}")
    value = value.replace("\x00", "").strip()
    if value == "-":
        return None
    return value

def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_log_file(filepath):
    entries = []
    with open(filepath, encoding="utf-8", errors="replace") as f:
        fields = []
        for line in f:
            if line.startswith("#Fields:"):
                fields = line.strip().split()[1:]
            elif not line.startswith("#") and fields:
                values = line.strip().split()
                if len(values) == len(fields):
                    entry = dict(zip(fields, values))
                    entries.append(entry)
    return entries

def is_probable_bot(user_agent):
    if not user_agent or user_agent.strip() == "-":
        return True
    return any(keyword in user_agent.lower() for keyword in KNOWN_BOT_KEYWORDS)

def interpret_user_agent(ua_string):
    if not ua_string or ua_string.strip() == "-":
        return None, None, None, None
    try:
        ua = parse(ua_string)
        browser = ua.browser.family
        if ua.browser.version_string:
            browser += f" {ua.browser.version_string}"
        browser = browser.strip()
        os_name = ua.os.family
        os_version = ua.os.version_string if ua.os.version_string else None
        platform = ua.device.family
        if platform == "Other":
            platform = "Computer"
        return browser, os_name, os_version, platform
    except Exception as e:
        log_import_error(f"Failed to parse user agent: {ua_string} — {e}")
        return None, None, None, None

def log_import_error(message):
    with open("import_errors.log", "a") as log_file:
        log_file.write(f"{datetime.now().isoformat()} - {message}\n")

def transform(entry, skip_bots, internal_ips, bot_skip_counter=None, internal_skip_counter=None):
    try:
        log_timestamp = datetime.strptime(entry["date"] + " " + entry["time"], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

    ua_string = entry.get("cs(User-Agent)", "")
    if skip_bots and is_probable_bot(ua_string):
        if bot_skip_counter is not None:
            bot_skip_counter[0] += 1
        return None

    ip_address = entry.get("c-ip")
    if internal_ips and ip_address in internal_ips:
        if internal_skip_counter is not None:
            internal_skip_counter[0] += 1
        return None

    browser, os_name, os_version, platform = interpret_user_agent(ua_string)
    cookies_raw = safe_text(entry.get("cs(Cookie)"))

    return (
        log_timestamp,
        safe_text(entry.get("s-ip")),
        safe_text(entry.get("cs-method")),
        safe_text(entry.get("cs-uri-stem")),
        safe_text(entry.get("cs-uri-query")),
        safe_int(entry.get("s-port")),
        safe_text(entry.get("cs-username")),
        safe_text(ip_address),
        safe_text(ua_string),
        safe_text(cookies_raw),
        safe_text(entry.get("cs(Referer)")),
        safe_text(entry.get("cs-host")),
        safe_int(entry.get("sc-status")),
        safe_int(entry.get("sc-substatus")),
        safe_int(entry.get("sc-win32-status")),
        safe_int(entry.get("sc-bytes")),
        safe_int(entry.get("time-taken")),
        safe_text(browser),
        safe_text(os_name),
        safe_text(os_version),
        safe_text(platform),
        None, None, None, None, None, None  # Placeholder for cookie fields
    )

def print_status_bar(current, total, start_time=None):
    bar_length = 50
    progress = current / total
    filled_length = int(round(bar_length * progress))
    bar = '#' * filled_length + ' ' * (bar_length - filled_length)
    percent = progress * 100

    eta = ""
    if start_time:
        elapsed = time.time() - start_time
        if progress > 0:
            estimated_total_time = elapsed / progress
            remaining_time = estimated_total_time - elapsed
            eta = f" ETA: {int(remaining_time)}s"

    print(f'\r[{bar}] {percent:3.0f}%{eta}', end='', flush=True)

def import_file(filepath, skip_bots, internal_ips, verbose):
    entries = parse_log_file(filepath)
    if not entries:
        print(f"No entries found in {filepath.name}.")
        return

    transformed_data = []
    seen = set()
    bot_skipped = 0
    internal_skipped = 0
    errors_skipped = 0
    start_time = time.time()

    for entry in entries:
        try:
            data = transform(entry, skip_bots, internal_ips)
            if data is None:
                continue
            if data not in seen:
                seen.add(data)
                transformed_data.append(data)
        except Exception as e:
            log_import_error(f"Failed to transform entry in {filepath.name}: {e}")
            errors_skipped += 1
            continue

    total_records = len(transformed_data)
    if total_records == 0:
        print(f"After filtering, no records remain in {filepath.name}.")
        return

    if verbose:
        print(f"Importing {total_records} distinct records from {filepath.name}...")

    conn = connect_db()
    with conn.cursor() as cursor:
        sql = '''
            INSERT INTO tbl_iis_logs (
                log_timestamp, s_ip, cs_method, cs_uri_stem, cs_uri_query, s_port,
                cs_username, c_ip, cs_user_agent, cs_cookie, cs_referer, cs_host,
                sc_status, sc_substatus, sc_win32_status, sc_bytes, time_taken,
                browser, os_name, os_version, platform,
                cookie_region, cookie_lang, cookie_username, cookie_priceclass, cookie_pricelist, cookie_session_id
            )
            VALUES %s
            ON CONFLICT DO NOTHING
        '''
        inserted_so_far = 0
        for i in range(0, len(transformed_data), BATCH_SIZE):
            batch = transformed_data[i:i + BATCH_SIZE]
            psycopg2.extras.execute_values(cursor, sql, batch, page_size=100)
            conn.commit()
            if verbose:
                inserted_so_far += len(batch)
                print_status_bar(inserted_so_far, len(transformed_data), start_time=start_time)

    conn.close()
    if verbose:
        print_status_bar(len(transformed_data), len(transformed_data), start_time=start_time)
        print()

def main():
    parser = argparse.ArgumentParser(description="Import IIS log files into PostgreSQL database.")
    parser.add_argument('-b', '--skip-bots', action='store_true', help="Skip bot traffic")
    parser.add_argument('-i', '--skip-internal', action='store_true', help="Skip internal IPs")
    parser.add_argument('-v', '--verbose', action='store_true', help="Verbose mode")
    parser.add_argument('-l', '--log-file', help="Force import of specific log file (even if already imported)")
    args = parser.parse_args()

    internal_ips = {"192.168.1.1", "10.0.0.1"} if args.skip_internal else set()

    if args.log_file:
        target_file = LOCAL_LOG_DIR / args.log_file
        if not target_file.exists():
            print(f"Specified log file {args.log_file} not found in {LOCAL_LOG_DIR}.")
            return
        log_files = [target_file]
        skip_import_check = True
        skip_today_check = True
    else:
        log_files = sorted(LOCAL_LOG_DIR.glob("*.log"))
        if not log_files:
            print("No uncompressed log files found in local directory.")
            return
        skip_import_check = False
        skip_today_check = False

    today_str = datetime.utcnow().strftime("%y%m%d")

    for file in log_files:
        file_date_part = file.stem[-6:]
        if not skip_today_check and file_date_part == today_str:
            print(f"Skipping {file.name} (log for today — likely incomplete)")
            continue

        if not skip_import_check:
            conn = connect_db()
            if already_imported(file.name, conn):
                print(f"{file.name} has already been imported. Skipping.")
                conn.close()
                continue
            conn.close()

        import_file(file, args.skip_bots, internal_ips, args.verbose)

        conn = connect_db()
        mark_as_imported(file.name, conn)
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nImport interrupted by user.")
        sys.exit(0)