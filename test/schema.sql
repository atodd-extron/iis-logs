-- Enable pg_trgm extension for substring search acceleration
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Drop views first
DROP VIEW IF EXISTS vw_page_views CASCADE;
DROP VIEW IF EXISTS vw_sessions CASCADE;

-- Drop tables
DROP TABLE IF EXISTS tbl_iis_logs;
DROP TABLE IF EXISTS tbl_priceclass_lookup;
DROP TABLE IF EXISTS tbl_imported_files;

-- Create price class lookup table
CREATE TABLE tbl_priceclass_lookup (
    priceclass TEXT PRIMARY KEY,
    description TEXT
);

-- Create imported log file tracking table
CREATE TABLE tbl_imported_files (
    filename TEXT PRIMARY KEY,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create main IIS logs table
CREATE TABLE tbl_iis_logs (
    id SERIAL PRIMARY KEY,
    log_timestamp TIMESTAMP NOT NULL,
    s_ip TEXT,
    cs_method TEXT,
    cs_uri_stem TEXT,
    cs_uri_query TEXT,
    s_port INTEGER,
    cs_username TEXT,
    c_ip TEXT,
    cs_user_agent TEXT,
    cs_cookie TEXT,
    cs_referer TEXT,
    cs_host TEXT,
    sc_status INTEGER,
    sc_substatus INTEGER,
    sc_win32_status BIGINT,
    sc_bytes BIGINT,
    time_taken BIGINT,
    browser TEXT,
    os_name TEXT,
    os_version TEXT,
    platform TEXT,
    cookie_region TEXT,
    cookie_lang TEXT,
    cookie_username TEXT,
    cookie_priceclass TEXT,
    cookie_pricelist TEXT,
    cookie_session_id TEXT
);

-- Create view for recent page views (last 30 days)
CREATE VIEW vw_page_views AS
SELECT *
FROM tbl_iis_logs
WHERE cs_uri_stem IS NOT NULL
  AND log_timestamp >= NOW() - INTERVAL '30 days';

-- Create view for inferred user sessions (grouped by IP and session cookie)
CREATE VIEW vw_sessions AS
SELECT
    cookie_session_id,
    c_ip,
    MIN(log_timestamp) AS session_start,
    MAX(log_timestamp) AS session_end,
    COUNT(*) AS total_requests,
    MAX(log_timestamp) - MIN(log_timestamp) AS duration,
    STRING_AGG(DISTINCT cs_uri_stem, ', ' ORDER BY cs_uri_stem) AS visited_pages
FROM tbl_iis_logs
WHERE cookie_session_id IS NOT NULL
  AND log_timestamp >= NOW() - INTERVAL '30 days'
GROUP BY cookie_session_id, c_ip;

-- Index for substring searches
CREATE INDEX idx_iis_logs_cs_uri_stem_trgm
    ON tbl_iis_logs USING gin (cs_uri_stem gin_trgm_ops);

-- Index for time-based filtering
CREATE INDEX idx_iis_logs_log_timestamp
    ON tbl_iis_logs (log_timestamp);