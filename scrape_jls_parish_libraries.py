#!/usr/bin/env python3
"""
Scrape Jamaica Library Service (JLS) parish library pages into structured data.

Design goals
------------
- no hardcoded branch-name allowlists
- no dependence on brittle sibling traversal
- explicit record-boundary detection with lookahead
- conservative parsing: prefer fewer, cleaner records over noisy junk
- validation that fails loudly when extraction quality collapses

Outputs
-------
- data/jls_parish_libraries.csv
- data/jls_parish_libraries.json
- data/jls_page_summaries.json

Usage
-----
python3 scrape_jls_parish_libraries.py --outdir data

Dependencies
------------
pip install requests beautifulsoup4
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.jls.gov.jm/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; JLSParishScraper/2.0; "
        "+https://www.jls.gov.jm/)"
    )
}

FALLBACK_PARISH_URLS = [
    "https://www.jls.gov.jm/clarendon-parish-library/",
    "https://www.jls.gov.jm/hanover-parish-library/",
    "https://www.jls.gov.jm/kingston-st-andrew-parish-library/",
    "https://www.jls.gov.jm/manchester-parish-library/",
    "https://www.jls.gov.jm/portland-parish-library/",
    "https://www.jls.gov.jm/st-ann-parish-library/",
    "https://www.jls.gov.jm/st-catherine-parish-library/",
    "https://www.jls.gov.jm/st-elizabeth-parish-library/",
    "https://www.jls.gov.jm/st-james-parish-library/",
    "https://www.jls.gov.jm/st-mary-parish-library/",
    "https://www.jls.gov.jm/st-thomas-parish-library/",
    "https://www.jls.gov.jm/trelawny-parish-library/",
    "https://www.jls.gov.jm/westmoreland-parish-library/",
]

STOP_SECTION_MARKERS = {
    "page title",
    "catalogue search",
    "information partners",
    "publications",
    "our policies",
    "general information",
    "events",
    "news",
    "related links",
}

NOISE_LINES = {
    "view more",
    "close",
    "overview",
    "branch libraries opening hours",
    "back to top",
}

PHONE_RE = re.compile(
    r"""
    (?:
        \(?\d{3}\)?[\s\-]*
    )?
    \d{3}[\s\-]?\d{4}
    (?:\s*/\s*\d{3,4}[\s\-]?\d{0,4})*
    """,
    re.X,
)
EMAIL_RE = re.compile(r"[\w.\-+%]+@[\w.\-]+\.[A-Za-z]{2,}")
DAY_RE = re.compile(
    r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b",
    re.I,
)
TIME_RE = re.compile(
    r"\b\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)\b|\bnoon\b",
    re.I,
)
STATUS_RE = re.compile(
    r"\b(temporarily closed|closed on saturdays|closed|part-time|part time)\b",
    re.I,
)
ADDRESS_HINT_RE = re.compile(
    r"""
    \b(
        P\.?\s*O\.?|
        Box|
        Street|
        St\.|
        Road|
        Rd\.|
        Drive|
        Dr\.|
        Lane|
        Ave\.?|
        Avenue|
        District|
        Square|
        Crescent|
        Boulevard|
        Blvd\.|
        Park|
        Plaza|
        Community\s+Centre?r?|
        Community\s+Center|
        Kingston|
        Clarendon|
        Hanover|
        Manchester|
        Portland|
        Trelawny|
        Westmoreland|
        St\.\s*(Ann|Catherine|Elizabeth|James|Mary|Thomas|Andrew)
    )\b
    """,
    re.I | re.X,
)
TITLE_CASE_TOKEN_RE = re.compile(r"^[A-Z][A-Za-z'’.-]*$")


@dataclass
class BranchRecord:
    parish_name: str
    parish_page_title: str
    parish_page_url: str
    overview: str
    services: List[str] = field(default_factory=list)
    branch_name: str = ""
    address: str = ""
    phones: List[str] = field(default_factory=list)
    email: str = ""
    status: str = ""
    hours: str = ""
    raw_text: str = ""


@dataclass
class PageSummary:
    parish_name: str
    parish_page_title: str
    parish_page_url: str
    services: List[str]
    overview: str
    branch_count: int
    extraction_notes: List[str] = field(default_factory=list)


def get(url: str, session: requests.Session, timeout: int = 30) -> requests.Response:
    response = session.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ").replace("\u200b", "")
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def slug_to_title(url: str) -> str:
    slug = urlparse(url).path.strip("/").split("/")[-1]
    slug = slug.replace("-parish-library", "")
    words = slug.replace("-", " ").split()
    out = []
    for w in words:
        if w.lower() == "st":
            out.append("St.")
        elif w.lower() == "and":
            out.append("and")
        else:
            out.append(w.capitalize())
    return " ".join(out)


def normalize_line(line: str) -> str:
    line = clean_text(line)
    line = line.strip("•*- ")
    return line


def extract_visible_lines(soup: BeautifulSoup) -> List[str]:
    text = soup.get_text("\n")
    lines = [normalize_line(x) for x in text.splitlines()]
    lines = [x for x in lines if x]
    return lines


def discover_parish_urls(session: requests.Session) -> List[str]:
    urls: List[str] = []
    try:
        soup = BeautifulSoup(get(BASE_URL, session).text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            full = urljoin(BASE_URL, href)
            if "parish-library" not in full:
                continue
            if not full.startswith(BASE_URL):
                continue
            if not full.endswith("/"):
                full += "/"
            if re.search(r"/[^/]*parish-library/$", full) and full not in urls:
                urls.append(full)
    except Exception as exc:
        print(f"[warn] Could not auto-discover URLs: {exc}", file=sys.stderr)

    if not urls:
        urls = FALLBACK_PARISH_URLS.copy()

    return sorted(urls)


def extract_page_title(soup: BeautifulSoup, url: str) -> str:
    candidates = []
    for tag in soup.find_all(["h1", "title"]):
        txt = clean_text(tag.get_text(" ", strip=True))
        if not txt:
            continue
        if txt.lower() == "jamaica library service":
            continue
        if "parish library" in txt.lower():
            candidates.append(txt)

    if candidates:
        return sorted(candidates, key=len)[0]

    return f"{slug_to_title(url)} Parish Library"


def extract_parish_name(url: str, page_title: str) -> str:
    lowered = page_title.lower()
    if "parish library" in lowered:
        return clean_text(re.sub(r"(?i)\bparish library\b", "", page_title)).strip(" -")
    return slug_to_title(url)


def collect_overview(lines: List[str]) -> str:
    start = None
    for i, line in enumerate(lines):
        if line.lower() == "overview":
            start = i + 1
            break
    if start is None:
        return ""

    collected = []
    for line in lines[start:]:
        low = line.lower()
        if low == "branch libraries opening hours":
            break
        if low in STOP_SECTION_MARKERS:
            break
        if low in NOISE_LINES:
            continue
        if low == "read more":
            break
        collected.append(line)

    text = clean_text(" ".join(collected))
    text = re.split(r"\bREAD MORE\b", text, maxsplit=1, flags=re.I)[0]
    return clean_text(text)


def collect_services(lines: List[str], page_title: str) -> List[str]:
    start = None
    page_title_key = clean_text(page_title).lower()
    for i, line in enumerate(lines):
        if clean_text(line).lower() == page_title_key:
            start = i + 1
            break

    if start is None:
        return []

    service_like = []
    SERVICE_PATTERNS = [
        r"mobile library",
        r"ict access",
        r"wifi",
        r"printing",
        r"photocopy",
        r"scanning",
        r"facsimile",
        r"spiral binding",
        r"reference",
        r"research",
        r"lending",
        r"rental space",
    ]

    for line in lines[start:]:
        low = line.lower()
        if low == "overview":
            break
        if low in NOISE_LINES:
            continue
        if any(re.search(p, low) for p in SERVICE_PATTERNS):
            service_like.append(line)

    deduped = []
    seen = set()
    for item in service_like:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def get_branch_section(lines: List[str]) -> List[str]:
    start = None
    for i, line in enumerate(lines):
        if line.lower() == "branch libraries opening hours":
            start = i + 1
            break
    if start is None:
        return []

    section: List[str] = []
    for line in lines[start:]:
        low = line.lower()
        if low in STOP_SECTION_MARKERS:
            break
        if low in NOISE_LINES:
            continue
        section.append(line)
    return section


def is_phone_line(line: str) -> bool:
    return bool(PHONE_RE.search(line))


def is_email_line(line: str) -> bool:
    return bool(EMAIL_RE.search(line))


def is_hours_line(line: str) -> bool:
    if DAY_RE.search(line):
        return True
    if TIME_RE.search(line):
        return True
    if re.search(r"\bclosed\b", line, re.I):
        return True
    if re.search(r"\b\(sat\)\b", line, re.I):
        return True
    return False


def is_address_line(line: str) -> bool:
    if ADDRESS_HINT_RE.search(line):
        return True
    if "," in line and not is_phone_line(line) and not is_email_line(line):
        return True
    if re.match(r"^\d+\s+\w+", line):
        return True
    return False


def is_detail_line(line: str) -> bool:
    return (
        is_phone_line(line)
        or is_email_line(line)
        or is_hours_line(line)
        or is_address_line(line)
    )


def looks_like_branch_name(line: str) -> bool:
    line = line.strip()
    if not line:
        return False
    if line.lower() in NOISE_LINES:
        return False
    if line.lower() in STOP_SECTION_MARKERS:
        return False
    if len(line) > 40:
        return False
    if is_detail_line(line):
        return False
    if re.search(r"[.!?]", line):
        return False
    if re.match(r"^\d", line):
        return False
    if re.search(r"\b(jamaica library service|government of jamaica)\b", line, re.I):
        return False

    bare = re.sub(r"\([^)]*\)", "", line).strip()
    if not bare:
        return False

    words = bare.replace("&", " ").replace("/", " ").split()
    if not (1 <= len(words) <= 4):
        return False

    alpha_words = [w for w in words if re.search(r"[A-Za-z]", w)]
    if not alpha_words:
        return False

    good = 0
    for word in alpha_words:
        if TITLE_CASE_TOKEN_RE.match(word):
            good += 1
    if good < max(1, len(alpha_words) - 1):
        return False

    return True


def has_detail_lookahead(section: List[str], idx: int, window: int = 6) -> bool:
    for nxt in section[idx + 1 : idx + 1 + window]:
        if is_detail_line(nxt):
            return True
        if looks_like_branch_name(nxt):
            return False
    return False


def segment_branch_blocks(section: List[str]) -> List[tuple[str, List[str]]]:
    blocks: List[tuple[str, List[str]]] = []
    current_name: Optional[str] = None
    current_lines: List[str] = []

    def flush() -> None:
        nonlocal current_name, current_lines, blocks
        if current_name:
            cleaned = [x for x in current_lines if x and x.lower() not in NOISE_LINES]
            blocks.append((current_name, cleaned))
        current_name = None
        current_lines = []

    for idx, line in enumerate(section):
        if looks_like_branch_name(line) and has_detail_lookahead(section, idx):
            if current_name is not None:
                flush()
            current_name = line
            current_lines = []
            continue

        if current_name is not None:
            current_lines.append(line)

    flush()
    return blocks


def extract_phones(text: str) -> List[str]:
    phones = []
    for match in PHONE_RE.finditer(text):
        value = clean_text(match.group(0))
        if value and value not in phones:
            phones.append(value)
    return phones


def extract_status(branch_name: str, text: str) -> str:
    found = []
    for src in [branch_name, text]:
        for match in STATUS_RE.finditer(src):
            value = match.group(1).strip().lower()
            if value in {"part time", "part-time"}:
                value = "Part-Time"
            elif value == "temporarily closed":
                value = "Temporarily Closed"
            elif value == "closed on saturdays":
                value = "Closed on Saturdays"
            else:
                value = value.capitalize()
            if value not in found:
                found.append(value)
    return "; ".join(found)


def split_detail_lines(lines: List[str]) -> dict:
    phones: List[str] = []
    email = ""
    hours_lines: List[str] = []
    address_lines: List[str] = []
    other_lines: List[str] = []

    for line in lines:
        if not line:
            continue

        line_phones = extract_phones(line)
        if line_phones:
            for p in line_phones:
                if p not in phones:
                    phones.append(p)
            line_wo_phone = PHONE_RE.sub("", line).strip(" /,-")
        else:
            line_wo_phone = line

        email_match = EMAIL_RE.search(line)
        if email_match and not email:
            email = email_match.group(0)

        if is_hours_line(line):
            hours_lines.append(line)
        elif is_address_line(line_wo_phone) and not EMAIL_RE.search(line_wo_phone):
            address_lines.append(line_wo_phone)
        elif line_wo_phone and not EMAIL_RE.search(line_wo_phone):
            other_lines.append(line_wo_phone)

    if not address_lines:
        for line in other_lines:
            if not is_hours_line(line) and not is_phone_line(line) and not is_email_line(line):
                address_lines.append(line)

    def dedupe(seq: Iterable[str]) -> List[str]:
        out = []
        seen = set()
        for item in seq:
            item = clean_text(item).strip(" ,;-")
            if item and item not in seen:
                seen.add(item)
                out.append(item)
        return out

    address_lines = dedupe(address_lines)
    hours_lines = dedupe(hours_lines)

    return {
        "phones": phones,
        "email": email,
        "address": ", ".join(address_lines),
        "hours": " | ".join(hours_lines),
    }


def build_branch_records(
    page_title: str,
    parish_name: str,
    page_url: str,
    overview: str,
    services: List[str],
    blocks: List[tuple[str, List[str]]],
) -> List[BranchRecord]:
    records: List[BranchRecord] = []

    for branch_name, detail_lines in blocks:
        raw_text = clean_text("\n".join(detail_lines))
        parsed = split_detail_lines(detail_lines)
        status = extract_status(branch_name, raw_text)

        records.append(
            BranchRecord(
                parish_name=parish_name,
                parish_page_title=page_title,
                parish_page_url=page_url,
                overview=overview,
                services=services,
                branch_name=branch_name,
                address=parsed["address"],
                phones=parsed["phones"],
                email=parsed["email"],
                status=status,
                hours=parsed["hours"],
                raw_text=raw_text,
            )
        )

    return records


def validate_records(records: List[BranchRecord]) -> List[str]:
    notes: List[str] = []
    if not records:
        notes.append("No branch records extracted.")
        return notes

    bad_heading_count = sum(1 for rec in records if is_detail_line(rec.branch_name))
    if bad_heading_count:
        notes.append(f"{bad_heading_count} branch names look like detail lines.")

    names = [r.branch_name.lower() for r in records]
    dupes = len(names) - len(set(names))
    if dupes > 2:
        notes.append(f"{dupes} duplicate branch names detected.")

    with_details = sum(
        1
        for r in records
        if r.address or r.phones or r.email or r.hours or r.status
    )
    if with_details < max(1, len(records) // 2):
        notes.append("Too few records have usable detail fields.")

    return notes


def scrape_page(url: str, session: requests.Session) -> tuple[List[BranchRecord], PageSummary]:
    response = get(url, session)
    soup = BeautifulSoup(response.text, "html.parser")

    lines = extract_visible_lines(soup)
    page_title = extract_page_title(soup, url)
    parish_name = extract_parish_name(url, page_title)
    overview = collect_overview(lines)
    services = collect_services(lines, page_title)
    branch_section = get_branch_section(lines)
    blocks = segment_branch_blocks(branch_section)
    records = build_branch_records(
        page_title=page_title,
        parish_name=parish_name,
        page_url=url,
        overview=overview,
        services=services,
        blocks=blocks,
    )
    notes = validate_records(records)

    summary = PageSummary(
        parish_name=parish_name,
        parish_page_title=page_title,
        parish_page_url=url,
        services=services,
        overview=overview,
        branch_count=len(records),
        extraction_notes=notes,
    )
    return records, summary


def write_json(records: List[BranchRecord], path: Path) -> None:
    payload = [asdict(r) for r in records]
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_csv(records: List[BranchRecord], path: Path) -> None:
    fieldnames = [
        "parish_name",
        "parish_page_title",
        "parish_page_url",
        "overview",
        "services",
        "branch_name",
        "address",
        "phones",
        "email",
        "status",
        "hours",
        "raw_text",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            row = asdict(record)
            row["services"] = "; ".join(record.services)
            row["phones"] = "; ".join(record.phones)
            writer.writerow(row)


def write_page_summaries(summaries: List[PageSummary], path: Path) -> None:
    payload = [asdict(s) for s in summaries]
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def fail_if_quality_is_bad(all_records: List[BranchRecord], summaries: List[PageSummary]) -> None:
    if not all_records:
        raise RuntimeError("No records extracted. Parser failed completely.")

    bad_pages = [s for s in summaries if s.branch_count == 0]
    if bad_pages:
        urls = ", ".join(s.parish_page_url for s in bad_pages)
        raise RuntimeError(f"At least one page produced zero branch records: {urls}")

    detail_like_names = [r for r in all_records if is_detail_line(r.branch_name)]
    if detail_like_names:
        sample = ", ".join(r.branch_name for r in detail_like_names[:5])
        raise RuntimeError(
            f"Parser produced branch names that look like detail lines. Sample: {sample}"
        )

    if len(all_records) < 25:
        raise RuntimeError(
            f"Too few total records extracted ({len(all_records)}). "
            "The site structure may have changed."
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", default="data", help="Directory for output files")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests in seconds")
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    urls = discover_parish_urls(session)
    print(f"[info] Found {len(urls)} parish page URLs")

    all_records: List[BranchRecord] = []
    summaries: List[PageSummary] = []

    for idx, url in enumerate(urls, start=1):
        try:
            print(f"[info] Scraping ({idx}/{len(urls)}): {url}")
            records, summary = scrape_page(url, session)
            print(f"[info]   -> extracted {len(records)} branch records")
            if summary.extraction_notes:
                for note in summary.extraction_notes:
                    print(f"[warn]   {note}", file=sys.stderr)
            all_records.extend(records)
            summaries.append(summary)
        except Exception as exc:
            print(f"[warn] Failed to scrape {url}: {exc}", file=sys.stderr)
        time.sleep(max(args.delay, 0))

    fail_if_quality_is_bad(all_records, summaries)

    json_path = outdir / "jls_parish_libraries.json"
    csv_path = outdir / "jls_parish_libraries.csv"
    summary_path = outdir / "jls_page_summaries.json"

    write_json(all_records, json_path)
    write_csv(all_records, csv_path)
    write_page_summaries(summaries, summary_path)

    print(f"[done] Wrote {len(all_records)} records")
    print(f"[done] CSV:  {csv_path}")
    print(f"[done] JSON: {json_path}")
    print(f"[done] META: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
