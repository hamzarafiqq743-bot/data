"""
Charity Commission Contact Data Collector
Data published under Open Government Licence v3.0
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
"""

import csv
import io
import logging
import sqlite3
import tempfile
import time
import zipfile
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

BULK_URL = (
    "https://ccewuksprdoneregsadata1.blob.core.windows.net"
    "/data/txt/publicextract.charity.zip"
)
CONTACT_URL = (
    "https://register-of-charities.charitycommission.gov.uk"
    "/en/charity-search/-/charity-details/{}/contact-information"
)
DB_PATH = "charity_contacts.db"
LOG_PATH = "charity_scraper.log"
DELAY_SECONDS = 1
BATCH_SIZE = 100
LOG_EVERY = 500
USER_AGENT = (
    "CharityContactCollector/1.0 (Open Government Licence v3.0 data; "
    "research use; contact: see repository)"
)
# Extra sleep after rate-limit / server-error responses before one retry
RATE_LIMIT_SLEEP = 30


def setup_logging():
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
        ],
    )


def setup_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS charities (
            charity_number TEXT PRIMARY KEY,
            name           TEXT
        );

        CREATE TABLE IF NOT EXISTS contacts (
            charity_number TEXT PRIMARY KEY,
            name           TEXT,
            email          TEXT,
            phone          TEXT,
            website        TEXT,
            address        TEXT,
            fetched_at     TEXT,
            error          TEXT
        );
    """)
    conn.commit()


def download_and_load_bulk(conn: sqlite3.Connection):
    row_count = conn.execute("SELECT COUNT(*) FROM charities").fetchone()[0]
    if row_count > 0:
        logging.info("Bulk data already loaded (%d charities). Skipping download.", row_count)
        return

    logging.info("Downloading bulk charity extract from Charity Commission...")
    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        response = session.get(BULK_URL, stream=True, timeout=120)
        response.raise_for_status()

        with tempfile.TemporaryFile() as tmp:
            for chunk in response.iter_content(chunk_size=65536):
                tmp.write(chunk)
            tmp.seek(0)

            with zipfile.ZipFile(tmp) as zf:
                txt_names = [n for n in zf.namelist() if n.endswith(".txt")]
                if not txt_names:
                    raise RuntimeError("No .txt file found inside ZIP")
                filename = txt_names[0]
                logging.info("Parsing %s from ZIP...", filename)

                with zf.open(filename) as raw:
                    # Try UTF-8 first, fall back to latin-1
                    try:
                        content = raw.read().decode("utf-8")
                    except UnicodeDecodeError:
                        raw.seek(0)
                        content = raw.read().decode("latin-1")

                reader = csv.DictReader(
                    io.StringIO(content),
                    delimiter="|",
                    quoting=csv.QUOTE_MINIMAL,
                )

                # Normalise header names (strip whitespace, lowercase)
                rows = []
                for record in reader:
                    norm = {k.strip().lower(): v.strip() for k, v in record.items()}
                    number = norm.get("registered_charity_number") or norm.get("charity_number", "")
                    name = norm.get("charity_name") or norm.get("name", "")
                    if number:
                        rows.append((number, name))

                conn.executemany(
                    "INSERT OR IGNORE INTO charities (charity_number, name) VALUES (?, ?)",
                    rows,
                )
                conn.commit()
                logging.info("Loaded %d charities into database.", len(rows))


def already_fetched(conn: sqlite3.Connection, number: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM contacts WHERE charity_number=?", (number,)
    ).fetchone() is not None


def fetch_contact_page(session: requests.Session, number: str) -> str:
    url = CONTACT_URL.format(number)
    response = session.get(url, timeout=15)
    if response.status_code in (429, 503):
        logging.warning(
            "Rate limited / server error (%d) for %s. Sleeping %ds then retrying.",
            response.status_code, number, RATE_LIMIT_SLEEP,
        )
        time.sleep(RATE_LIMIT_SLEEP)
        response = session.get(url, timeout=15)
    response.raise_for_status()
    return response.text


def _clean(text: str) -> str:
    return " ".join(text.split()) if text else ""


def parse_contact_page(html: str, number: str, name: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # Email
    email_tag = soup.find("a", href=lambda h: h and h.startswith("mailto:"))
    email = email_tag["href"][7:].strip() if email_tag else None

    # Phone
    phone_tag = soup.find("a", href=lambda h: h and h.startswith("tel:"))
    if phone_tag:
        phone = phone_tag["href"][4:].strip()
    else:
        phone = None

    # Website — any external link that is not the CC domain itself
    cc_domain = "charitycommission.gov.uk"
    website = None
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("http") and cc_domain not in href:
            website = href
            break

    # Address — look for an address block; Liferay portal wraps it in
    # a <div> or <section> that contains the word "address" nearby,
    # or we can grab the first multi-line text block that looks postal.
    address = None
    address_candidates = []

    # Strategy 1: explicit <address> tag
    addr_tag = soup.find("address")
    if addr_tag:
        address = _clean(addr_tag.get_text(separator=", "))

    # Strategy 2: look for headings/labels containing "address" then grab sibling text
    if not address:
        for tag in soup.find_all(string=lambda t: t and "address" in t.lower()):
            parent = tag.parent
            # Walk up to find a container with useful sibling content
            for _ in range(3):
                if parent is None:
                    break
                sibling = parent.find_next_sibling()
                if sibling:
                    candidate = _clean(sibling.get_text(separator=", "))
                    if len(candidate) > 10:
                        address_candidates.append(candidate)
                        break
                parent = parent.parent
        if address_candidates:
            address = address_candidates[0]

    # Strategy 3: largest <p> block that looks like a postal address (contains digits)
    if not address:
        for p in soup.find_all("p"):
            text = _clean(p.get_text(separator=", "))
            if any(ch.isdigit() for ch in text) and len(text) > 15:
                address = text
                break

    return {
        "charity_number": number,
        "name": name,
        "email": email,
        "phone": phone,
        "website": website,
        "address": address,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }


def save_contact(conn: sqlite3.Connection, row: dict):
    conn.execute(
        """
        INSERT OR REPLACE INTO contacts
            (charity_number, name, email, phone, website, address, fetched_at, error)
        VALUES
            (:charity_number, :name, :email, :phone, :website, :address, :fetched_at, :error)
        """,
        row,
    )


def main():
    setup_logging()
    logging.info("=== Charity Contact Collector starting ===")

    conn = sqlite3.connect(DB_PATH)
    setup_db(conn)
    download_and_load_bulk(conn)

    charities = conn.execute(
        "SELECT charity_number, name FROM charities ORDER BY charity_number"
    ).fetchall()
    total = len(charities)
    logging.info("Starting contact scrape for %d charities.", total)

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    done = 0
    errors = 0

    for i, (number, name) in enumerate(charities):
        if already_fetched(conn, number):
            continue

        try:
            html = fetch_contact_page(session, number)
            row = parse_contact_page(html, number, name)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            msg = f"HTTP {status}"
            logging.warning("Skipping %s (%s): %s", number, name, msg)
            save_contact(conn, {
                "charity_number": number, "name": name,
                "email": None, "phone": None, "website": None, "address": None,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "error": msg,
            })
            errors += 1
            time.sleep(DELAY_SECONDS)
            if (i + 1) % BATCH_SIZE == 0:
                conn.commit()
            continue
        except Exception as exc:
            msg = type(exc).__name__ + ": " + str(exc)
            logging.warning("Skipping %s (%s): %s", number, name, msg)
            save_contact(conn, {
                "charity_number": number, "name": name,
                "email": None, "phone": None, "website": None, "address": None,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "error": msg,
            })
            errors += 1
            time.sleep(DELAY_SECONDS)
            if (i + 1) % BATCH_SIZE == 0:
                conn.commit()
            continue

        save_contact(conn, row)
        done += 1
        time.sleep(DELAY_SECONDS)

        if (i + 1) % BATCH_SIZE == 0:
            conn.commit()

        if (i + 1) % LOG_EVERY == 0:
            logging.info(
                "Progress: %d/%d processed, %d successful, %d errors.",
                i + 1, total, done, errors,
            )

    conn.commit()
    conn.close()
    logging.info(
        "=== Done. %d/%d processed, %d successful, %d errors. ===",
        done + errors, total, done, errors,
    )


if __name__ == "__main__":
    main()
