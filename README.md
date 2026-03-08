<<<<<<< HEAD
# JLS Parish Library Scraper

Files:
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
# JLS Parish Library Scraper

A Python scraper that extracts data from the Jamaica Library Service parish library pages.

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

## Installation

```bash
pip install -r requirements.txt

