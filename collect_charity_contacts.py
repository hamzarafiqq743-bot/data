"""
Charity Commission Contact Data Collector
Data published under Open Government Licence v3.0
https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

Reads contact details directly from the bulk extract ZIP — no web scraping.
Typical runtime: 3-5 minutes for the full ~170k charity dataset.
"""

import csv
import io
import logging
import sqlite3
import sys
import tempfile
import zipfile

csv.field_size_limit(sys.maxsize)

import requests

BULK_URL = (
    "https://ccewuksprdoneregsadata1.blob.core.windows.net"
    "/data/txt/publicextract.charity.zip"
)
DB_PATH = "charity_contacts.db"
LOG_PATH = "charity_collector.log"
USER_AGENT = (
    "CharityContactCollector/1.0 (Open Government Licence v3.0; research use)"
)

# Known column name aliases in the CC bulk extract (checked case-insensitively)
COL_ALIASES = {
    "charity_number": [
        "registered_charity_number", "charity_number", "regno",
    ],
    "name": [
        "charity_name", "name", "charity_name_registered",
    ],
    "email": [
        "charity_contact_email", "email",
    ],
    "phone": [
        "charity_contact_phone", "phone", "telephone",
    ],
    "website": [
        "charity_contact_web", "website", "web",
    ],
    "address": [
        # Some extracts give separate address lines; others give a combined field.
        # We handle both below.
        "charity_contact_address1",
    ],
}

ADDRESS_LINE_ALIASES = [
    "charity_contact_address1",
    "charity_contact_address2",
    "charity_contact_address3",
    "charity_contact_address4",
    "charity_contact_address5",
    "charity_contact_postcode",
    "add1", "add2", "add3", "add4", "add5", "postcode",
]


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
        ],
    )


def setup_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS contacts (
            charity_number TEXT PRIMARY KEY,
            name           TEXT,
            email          TEXT,
            phone          TEXT,
            website        TEXT,
            address        TEXT
        );
    """)
    conn.commit()


def detect_column_map(header: list[str]) -> dict:
    """Map logical field names to actual column indices in the header."""
    lower = [h.strip().lower() for h in header]
    col_map = {}

    for field, aliases in COL_ALIASES.items():
        for alias in aliases:
            if alias in lower:
                col_map[field] = lower.index(alias)
                break

    # Address lines — collect all present
    addr_indices = [lower.index(a) for a in ADDRESS_LINE_ALIASES if a in lower]
    col_map["address_lines"] = addr_indices

    if "charity_number" not in col_map:
        raise ValueError(
            f"Could not find charity number column in header.\nHeader was: {header}"
        )

    logging.info("Column map: %s", col_map)
    return col_map


def parse_row(row: list[str], col_map: dict) -> dict | None:
    def get(field):
        idx = col_map.get(field)
        if idx is None:
            return None
        val = row[idx].strip() if idx < len(row) else ""
        return val or None

    number = get("charity_number")
    if not number:
        return None

    # Build address from separate lines if present
    addr_parts = [
        row[i].strip()
        for i in col_map.get("address_lines", [])
        if i < len(row) and row[i].strip()
    ]
    address = ", ".join(addr_parts) if addr_parts else None

    return {
        "charity_number": number,
        "name": get("name"),
        "email": get("email"),
        "phone": get("phone"),
        "website": get("website"),
        "address": address,
    }


def download_and_parse(conn: sqlite3.Connection):
    existing = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    if existing > 0:
        logging.info(
            "Database already has %d records. Delete %s to re-import.", existing, DB_PATH
        )
        return

    logging.info("Downloading bulk charity extract (~30-80 MB)...")
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    response = session.get(BULK_URL, stream=True, timeout=120)
    response.raise_for_status()

    downloaded = 0
    with tempfile.TemporaryFile() as tmp:
        for chunk in response.iter_content(chunk_size=65536):
            tmp.write(chunk)
            downloaded += len(chunk)
            if downloaded % (10 * 1024 * 1024) == 0:
                logging.info("  Downloaded %d MB...", downloaded // 1024 // 1024)
        tmp.seek(0)

        logging.info("Download complete. Parsing ZIP...")
        with zipfile.ZipFile(tmp) as zf:
            txt_files = [n for n in zf.namelist() if n.lower().endswith(".txt")]
            if not txt_files:
                raise RuntimeError(f"No .txt file found in ZIP. Contents: {zf.namelist()}")

            filename = txt_files[0]
            logging.info("Reading %s...", filename)

            with zf.open(filename) as raw:
                try:
                    content = raw.read().decode("utf-8-sig")
                except UnicodeDecodeError:
                    raw.seek(0)
                    content = raw.read().decode("latin-1")

    first_line = content.split("\n", 1)[0]
    delimiter = "\t" if "\t" in first_line else "|"
    logging.info("Delimiter detected: %r", delimiter)

    reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    header = next(reader)
    col_map = detect_column_map(header)

    rows = []
    skipped = 0
    for raw_row in reader:
        parsed = parse_row(raw_row, col_map)
        if parsed:
            rows.append(parsed)
        else:
            skipped += 1

        if len(rows) % 10000 == 0 and rows:
            logging.info("  Parsed %d rows so far...", len(rows))

    logging.info("Parsed %d charities (%d skipped). Saving to database...", len(rows), skipped)

    conn.executemany(
        """
        INSERT OR REPLACE INTO contacts
            (charity_number, name, email, phone, website, address)
        VALUES
            (:charity_number, :name, :email, :phone, :website, :address)
        """,
        rows,
    )
    conn.commit()
    logging.info("Done. %d records saved to %s", len(rows), DB_PATH)
    return rows


def export_csv(conn: sqlite3.Connection, csv_path: str):
    rows = conn.execute(
        "SELECT charity_number, name, email, phone, website, address FROM contacts ORDER BY name"
    ).fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["Charity Number", "Name", "Email", "Phone", "Website", "Address"])
        writer.writerows(rows)

    logging.info("Exported %d rows to %s (open with Excel)", len(rows), csv_path)


def print_summary(conn: sqlite3.Connection):
    total = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    with_email = conn.execute("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL").fetchone()[0]
    with_phone = conn.execute("SELECT COUNT(*) FROM contacts WHERE phone IS NOT NULL").fetchone()[0]
    with_web = conn.execute("SELECT COUNT(*) FROM contacts WHERE website IS NOT NULL").fetchone()[0]
    with_address = conn.execute("SELECT COUNT(*) FROM contacts WHERE address IS NOT NULL").fetchone()[0]

    logging.info("=== Summary ===")
    logging.info("Total charities : %d", total)
    logging.info("With email      : %d", with_email)
    logging.info("With phone      : %d", with_phone)
    logging.info("With website    : %d", with_web)
    logging.info("With address    : %d", with_address)


def main():
    setup_logging()
    logging.info("=== Charity Contact Collector ===")

    conn = sqlite3.connect(DB_PATH)
    setup_db(conn)
    download_and_parse(conn)
    print_summary(conn)
    export_csv(conn, "charity_contacts.csv")
    conn.close()

    logging.info("Open charity_contacts.csv in Excel to view the data.")


if __name__ == "__main__":
    main()
