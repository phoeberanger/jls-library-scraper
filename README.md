# JLS Parish Library Scraper

![Update dataset](https://github.com/phoeberanger/jls-library-scraper/actions/workflows/update-data.yml/badge.svg)

## About

This repository contains an automated data pipeline that collects and maintains a structured dataset of parish library branches operated by the Jamaica Library Service (JLS).

A Python scraper extracts publicly available information from JLS parish library pages and transforms it into structured data. The pipeline runs automatically via GitHub Actions, regenerating the dataset and committing updates whenever changes are detected.

The dataset includes:

Parish library networks

Branch locations

Addresses

Contact details

Opening hours

Operational status indicators

Because the JLS website presents branch information using card-based layouts that vary across parish pages, the scraper reconstructs structured records from page text and applies validation logic to produce consistent output.

## Data Pipeline Architecture
```mermaid
flowchart TD
    A[JLS Website] --> B[Python Scraper]
    B --> C[Structured Dataset<br>(CSV + JSON)]
    C --> D[GitHub Actions Workflow]
    D --> E[Automated Commit to Repository]

The workflow runs weekly and commits updates only when the dataset changes.

Example page:
https://www.jls.gov.jm/clarendon-parish-library/

## Data Collected

The scraper extracts:

- Parish library page title
- Parish URL
- Overview/history text
- Services offered
- Branch library name
- Address
- Phone numbers
- Email
- Status (Closed / Part-time / etc)
- Opening hours

Schema: 
Field | Description
------|-------------
parish_name | Parish library network
branch_name | Branch library name
address | Branch address
phones | Contact phone numbers
email | Branch email
hours | Opening hours
status | Operational status
  
- no hardcoded branch-name allowlists
- conservative branch-boundary detection
- parser validation that fails on obviously bad output
- page-level extraction summary output
  
## Files:
- `scrape_jls_parish_libraries.py` — scraper script
- outputs:
  - `jls_parish_libraries.json`
  - `jls_parish_libraries.csv`

## Install
```bash
pip install requests beautifulsoup4
```

## Run
```bash
python scrape_jls_parish_libraries.py --outdir data
```

## Notes
- The JLS pages are structurally inconsistent, so the scraper is built as a best-effort parser.
- It captures the visible branch/contact/hour blocks and preserves a `raw_text` field for anything messy.
- If the site changes, you will most likely only need to adjust:
  - `collect_services()`
  - `collect_branch_records()`
  - `STOP_HEADINGS`

## Installation

```bash
pip install -r requirements.txt
```

## Use Cases

This dataset can support:

open data initiatives

mapping of Jamaica’s public library network

service accessibility analysis

civic tech projects

data engineering and web-scraping examples

## Data Updates

The dataset is refreshed automatically using GitHub Actions on a scheduled basis.

The pipeline:

installs dependencies

runs the scraper

regenerates the dataset

commits updates if the data changes

### License

Data is derived from publicly available information published by the Jamaica Library Service.

