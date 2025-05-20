# iis-logs
A Python-based analytics pipeline that ingests raw IIS logs, normalizes and stores them in a PostgreSQL database; includes filtered views, bot exclusion, user/session tracking, and customizable reporting

## Features

- Fast, batch-based log ingestion
- Normalization and deduplication
- User-agent parsing and client identification
- Session tracking via cookies
- Exclusion of known bots and crawlers
- Custom SQL views for analysis and reporting

## Getting Started

See [`docs/setup.md`](docs/setup.md) for installation instructions.
