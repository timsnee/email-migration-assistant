import imaplib
import email
import time
import sqlite3
import re
import json
import os
from email.header import decode_header
from bs4 import BeautifulSoup
from tqdm import tqdm   # progress bar (pip install tqdm)

# ------------------------- CONFIG ------------------------- #
# Load configuration from config.json
CONFIG_FILE = "config.json"

if not os.path.exists(CONFIG_FILE):
    print(f"Error: {CONFIG_FILE} not found!")
    print(f"Please copy config.json.template to {CONFIG_FILE} and fill in your settings.")
    exit(1)

with open(CONFIG_FILE, 'r') as f:
    config = json.load(f)

IMAP_SERVER = config.get("imap_server")
EMAIL_USER = config.get("email_user")
EMAIL_PASS = config.get("email_pass")
MAILBOX = config.get("mailbox", "INBOX")
BATCH_SIZE = config.get("batch_size", 500)
THROTTLE_PER_EMAIL = config.get("throttle_per_email", 0.05)
DB_FILE = config.get("db_file", "email_archive.db")
# ---------------------------------------------------------- #

# ---------------------------------------------------------- #
# DATABASE SETUP + RESUME LOGIC
# ---------------------------------------------------------- #
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT UNIQUE,
    sender TEXT,
    recipient TEXT,
    subject TEXT,
    date TEXT,
    body TEXT,
    domains_found TEXT
);
""")
conn.commit()

# Fetch already-archived message IDs for skipping:
cursor.execute("SELECT message_id FROM emails")
archive_cache = set(row[0] for row in cursor.fetchall())

print(f"Loaded {len(archive_cache)} existing messages → resume enabled.")

# ---------------------------------------------------------- #
# IMAP CONNECT FUNCTION (re-used every batch)
# ---------------------------------------------------------- #
def connect_imap():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_USER, EMAIL_PASS)
    mail.select(MAILBOX)
    return mail

mail = connect_imap()

# ---------------------------------------------------------- #
# FETCH MAILBOX INDEX
# ---------------------------------------------------------- #
result, data = mail.search(None, "ALL")
all_ids = data[0].split()
total = len(all_ids)

print(f"Total emails on server: {total}")

# Filter unarchived messages:
pending_ids = [mid for mid in all_ids if mid not in archive_cache]
print(f"Remaining emails to archive: {len(pending_ids)}")

# ---------------------------------------------------------- #
# EMAIL BODY & DOMAIN EXTRACTION
# ---------------------------------------------------------- #
def extract_domains(text):
    urls = re.findall(r'https?://[\w\.-]+', text)
    domains = [u.split("//")[1].split("/")[0] for u in urls]
    return ",".join(set(domains)) if domains else None

# ---------------------------------------------------------- #
# PROCESS EMAILS IN BATCHES
# ---------------------------------------------------------- #
processed = 0
for email_id in tqdm(pending_ids[:BATCH_SIZE], desc="Archiving"):

    # Periodically refresh IMAP connection to avoid timeouts
    if processed > 0 and processed % 200 == 0:
        mail.logout()
        mail = connect_imap()

    result, msg_data = mail.fetch(email_id, "(RFC822)")

    raw_email = msg_data[0][1]
    msg = email.message_from_bytes(raw_email)

    message_id = msg.get("Message-ID")
    if message_id in archive_cache:
        continue

    # Decode subject
    raw_subject = decode_header(msg.get("Subject"))[0]
    subject = raw_subject[0]
    if isinstance(subject, bytes):
        subject = subject.decode(raw_subject[1] or "utf-8", errors="ignore")

    # Basic details
    sender = msg.get("From")
    recipient = msg.get("To")
    date = msg.get("Date")

    # BODY PARSE
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                body += part.get_payload(decode=True).decode(errors="ignore")
            elif ctype == "text/html":
                html = part.get_payload(decode=True).decode(errors="ignore")
                body += BeautifulSoup(html, "html.parser").get_text()
    else:
        body = msg.get_payload(decode=True).decode(errors="ignore")

    domains = extract_domains(body)

    cursor.execute("""
        INSERT OR IGNORE INTO emails 
        (message_id, sender, recipient, subject, date, body, domains_found)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (message_id, sender, recipient, subject, date, body, domains))

    conn.commit()
    processed += 1

    time.sleep(THROTTLE_PER_EMAIL)  # avoid IMAP throttling

mail.logout()
conn.close()

print(f"\n\n✔ Batch complete — processed {processed} new emails.")
print("Re-run the script to process the next batch automatically.")
