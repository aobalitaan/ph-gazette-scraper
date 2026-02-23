# Philippine Official Gazette Scraper

A Python scraper for collecting presidential documents from the [Philippine Official Gazette](https://www.officialgazette.gov.ph/). Built for academic research on Philippine presidential communication.

This tool collects two types of documents:

- **State of the Nation Addresses (SONAs)** — scraped from the [past SONA speeches page](https://www.officialgazette.gov.ph/past-sona-speeches/) (85 speeches, 1935-2025)
- **Presidential issuances and speeches** — scraped from the [masterlist generator](https://www.officialgazette.gov.ph/masterlist-generator/) covering executive orders, administrative orders, proclamations, memoranda, speeches, and more across 15 presidents

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

```bash
git clone https://github.com/aobalitaan/ph-gazette-scraper.git
cd ph-gazette-scraper

# install dependencies
uv sync
```

## Quick Start

```bash
# scrape all 85 SONAs
uv run talasuri-scrape-sona

# scrape all masterlist documents (this takes a while — thousands of documents)
uv run talasuri-scrape-masterlist
```

## How the Scraper Works

### SONA Scraper

The SONA scraper is straightforward — it fetches the SONA index page, parses the table to get URLs for each speech, then visits each page to extract the full text.

```bash
# just fetch the index to see what's available
uv run talasuri-scrape-sona --index-only

# full scrape
uv run talasuri-scrape-sona

# re-scrape everything from scratch (ignores previously saved results)
uv run talasuri-scrape-sona --force
```

### Masterlist Scraper

The masterlist scraper works in two phases:

**Phase A — Index Collection** uses the Official Gazette's masterlist generator form to paginate through results tables and collect document metadata. For each category x president combination, it saves a JSON index of all entries found. This phase uses a regular HTTP client (`httpx`) since the masterlist generator pages aren't blocked by Cloudflare.

**Phase B — Content Fetching** takes those index entries and visits each individual document page to extract the full text. This phase uses `curl_cffi` which impersonates a real browser's TLS fingerprint (JA3/JA4), bypassing Cloudflare's bot detection without needing an actual browser.

```bash
# full scrape (both phases)
uv run talasuri-scrape-masterlist

# phase A only — just collect indexes, no content fetching
uv run talasuri-scrape-masterlist --index-only

# phase B only — fetch content from existing indexes
uv run talasuri-scrape-masterlist --content-only

# filter by category and/or president
uv run talasuri-scrape-masterlist --categories executive-orders proclamations
uv run talasuri-scrape-masterlist --presidents rodrigo-roa-duterte ferdinand-r-marcos

# multiple concurrent workers (each gets its own session)
uv run talasuri-scrape-masterlist -c 3

# verbose logging
uv run talasuri-scrape-masterlist -v
```

### Rate Limiting

The default delay between requests is **5 seconds** with ±50% random jitter (so 2.5–7.5 seconds in practice). This is intentionally conservative to avoid triggering the Gazette's WAF (Web Application Firewall). You can lower it with `--delay`, but going below 2 seconds risks 429 rate limits. A delay of 3–4 seconds works well in practice.

> **Note:** VPN/proxy IPs are often flagged by Cloudflare and may trigger aggressive bot challenges (HTTP 403). If you're getting blocked, try disabling your VPN first.

```bash
# default: 5 second delay
uv run talasuri-scrape-masterlist

# faster (not recommended unless you know what you're doing)
uv run talasuri-scrape-masterlist --delay 3
```

### Proxy Support

If you need to distribute requests across multiple IPs, pass a proxy file. Format is one proxy per line in `host:port:user:pass` format:

```bash
uv run talasuri-scrape-masterlist --proxy-file proxies.txt
```

When a proxy file is provided, concurrency is automatically set to match the number of proxies (one worker per IP).

### Resumability

Both scrapers support resuming from where they left off. If a run is interrupted (Ctrl+C triggers graceful shutdown), just run the same command again — already-scraped documents will be skipped. Use `--force` to re-scrape everything.

## Output Data

### Directory Structure

**SONA data** (`data/documents-raw/sona/`):

```
sona/
  raw_html/{president-slug}/{doc_id}.html    # original HTML pages
  text/{president-slug}/{doc_id}.txt         # extracted plain text
  metadata/{president-slug}/{doc_id}.json    # per-document metadata
  manifest.json                              # all documents in one file
```

**Masterlist data** (`data/documents-raw/masterlist/`):

```
masterlist/
  index/{category-slug}/{president-slug}.json   # Phase A index files
  raw_html/{category-slug}/{president-slug}/{doc_id}.html
  text/{category-slug}/{president-slug}/{doc_id}.txt
  metadata/{category-slug}/{president-slug}/{doc_id}.json
  manifest.json
```

### What are Index Files?

Index files (`index/*.json`) are the Phase A output — they store the metadata from the masterlist results table (title, URL, president, date, PDF link) without the actual document content. They're cached so you don't have to re-paginate through results tables on every run. Think of them as a "table of contents" for each category x president combination.

### Manifest

The `manifest.json` in each scraper's output directory is the master record of all documents with their scrape status. It tracks what was scraped successfully, what failed, what's still pending, and when each document was last scraped. This is how resumability works.

### Metadata Format

Each document's metadata JSON looks like this:

```json
{
  "doc_id": "executive-order-no-01-s-2016",
  "category": "executive_order",
  "category_slug": "executive-orders",
  "president_slug": "rodrigo-roa-duterte",
  "date": "2016-07-26",
  "title": "Executive Order No. 01, s. 2016",
  "content_url": "https://www.officialgazette.gov.ph/2016/07/26/executive-order-no-01-s-2016/",
  "pdf_url": "https://www.officialgazette.gov.ph/.../20160726-EO-1-RRD.pdf",
  "text": "...",
  "word_count": 542,
  "has_html_content": true,
  "is_pdf_only": false,
  "scrape_status": "success",
  "scraped_at": "2026-02-23T08:15:00Z"
}
```

### PDF-Only Documents

Some gazette documents don't have inline HTML text — they only link to a PDF file. The scraper marks these as `is_pdf_only: true` with no extracted text. You'll need OCR to get text from these.

## Document Categories

The masterlist generator covers 15 categories:

| Category | Slug |
|----------|------|
| Executive Orders | `executive-orders` |
| Administrative Orders | `administrative-orders` |
| Proclamations | `proclamations` |
| Memorandum Orders | `memorandum-orders` |
| Memorandum Circulars | `memorandum-circulars` |
| Presidential Decrees | `presidential-decrees-executive-issuances` |
| Speeches | `speeches` |
| Republic Acts | `republic-acts` |
| General Orders | `general-orders` |
| Letters of Instruction | `letters-of-instruction` |
| Letters of Implementation | `letters-of-implementation` |
| Other Issuances | `other-issuances` |
| Special Orders | `special-orders` |
| IRR (Executive Orders) | `implementing-rules-and-regulations-executive-orders` |
| IRR (Republic Acts) | `implementing-rules-and-regulations` |

## Presidents Covered

The scraper covers all presidents available in the Gazette's masterlist generator:

Manuel L. Quezon, Sergio Osmena, Manuel Roxas, Elpidio Quirino, Ramon Magsaysay, Carlos P. Garcia, Diosdado Macapagal, Ferdinand E. Marcos, Corazon C. Aquino, Fidel V. Ramos, Joseph Ejercito Estrada, Gloria Macapagal-Arroyo, Benigno S. Aquino III, Rodrigo Roa Duterte, Ferdinand R. Marcos Jr.

## Running Tests

```bash
uv run pytest
uv run ruff check backend/ tests/
```

## Project Structure

```
backend/
  common/
    logging.py                  # shared logging config
  pipeline/
    scraper/
      browser_client.py         # curl_cffi client (bypasses cloudflare TLS fingerprinting)
      http_client.py            # httpx-based client (for pages that aren't blocked)
      cli.py                    # SONA scraper CLI
      masterlist_cli.py         # masterlist scraper CLI
      sona_scraper.py           # SONA scraper orchestration
      masterlist_scraper.py     # masterlist scraper orchestration (phase A + B)
      parsers.py                # HTML parsing for SONA pages
      masterlist_parsers.py     # HTML parsing for masterlist pages
      storage.py                # file I/O for SONA data
      masterlist_storage.py     # file I/O for masterlist data
      models.py                 # pydantic data models
tests/
  fixtures/                     # sample HTML for testing
  pipeline/scraper/             # unit tests
```

## Acknowledgment

Data is sourced from the [Official Gazette of the Republic of the Philippines](https://www.officialgazette.gov.ph/). This scraper is intended for academic and research purposes.

## License

MIT
