#!/usr/bin/env python3
"""
Email Archive Query Script
Query the email_archive.db database with various filters and options.
"""

import sqlite3
import argparse
import sys
from datetime import datetime
from typing import Optional, List, Tuple
import json

DB_FILE = "email_archive.db"


def connect_db(db_file: str = DB_FILE) -> sqlite3.Connection:
    """Connect to the email archive database."""
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get statistics about the email archive."""
    cursor = conn.cursor()
    stats = {}
    
    # Total emails
    cursor.execute("SELECT COUNT(*) FROM emails")
    stats['total_emails'] = cursor.fetchone()[0]
    
    # Date range
    cursor.execute("SELECT MIN(date), MAX(date) FROM emails WHERE date IS NOT NULL")
    date_range = cursor.fetchone()
    stats['oldest_email'] = date_range[0]
    stats['newest_email'] = date_range[1]
    
    # Top senders
    cursor.execute("""
        SELECT sender, COUNT(*) as count 
        FROM emails 
        WHERE sender IS NOT NULL 
        GROUP BY sender 
        ORDER BY count DESC 
        LIMIT 10
    """)
    stats['top_senders'] = [dict(row) for row in cursor.fetchall()]
    
    # Emails with domains
    cursor.execute("SELECT COUNT(*) FROM emails WHERE domains_found IS NOT NULL")
    stats['emails_with_domains'] = cursor.fetchone()[0]
    
    return stats


def format_email(row: sqlite3.Row, show_body: bool = False, max_body_length: int = 200) -> str:
    """Format an email row for display."""
    output = []
    output.append(f"ID: {row['id']}")
    output.append(f"Message-ID: {row['message_id']}")
    output.append(f"From: {row['sender']}")
    output.append(f"To: {row['recipient']}")
    output.append(f"Date: {row['date']}")
    output.append(f"Subject: {row['subject']}")
    
    if row['domains_found']:
        output.append(f"Domains: {row['domains_found']}")
    
    if show_body and row['body']:
        body = row['body']
        if len(body) > max_body_length:
            body = body[:max_body_length] + "..."
        output.append(f"\nBody:\n{body}")
    
    return "\n".join(output)


def query_emails(
    conn: sqlite3.Connection,
    sender: Optional[str] = None,
    recipient: Optional[str] = None,
    subject: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    domain: Optional[str] = None,
    search_body: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    show_body: bool = False,
    max_body_length: int = 200
) -> List[sqlite3.Row]:
    """Query emails with various filters."""
    cursor = conn.cursor()
    
    query = "SELECT * FROM emails WHERE 1=1"
    params = []
    
    if sender:
        query += " AND sender LIKE ?"
        params.append(f"%{sender}%")
    
    if recipient:
        query += " AND recipient LIKE ?"
        params.append(f"%{recipient}%")
    
    if subject:
        query += " AND subject LIKE ?"
        params.append(f"%{subject}%")
    
    if date_from:
        query += " AND date >= ?"
        params.append(date_from)
    
    if date_to:
        query += " AND date <= ?"
        params.append(date_to)
    
    if domain:
        query += " AND domains_found LIKE ?"
        params.append(f"%{domain}%")
    
    if search_body:
        query += " AND body LIKE ?"
        params.append(f"%{search_body}%")
    
    query += " ORDER BY date DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    return cursor.fetchall()


def export_to_json(rows: List[sqlite3.Row], output_file: str):
    """Export query results to JSON file."""
    data = [dict(row) for row in rows]
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nExported {len(data)} emails to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Query the email archive database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show statistics
  python query_email_archive.py --stats

  # Search by sender
  python query_email_archive.py --sender "example.com"

  # Search by subject
  python query_email_archive.py --subject "meeting"

  # Search by date range
  python query_email_archive.py --date-from "2024-01-01" --date-to "2024-12-31"

  # Search for emails containing a domain
  python query_email_archive.py --domain "github.com"

  # Search in email body
  python query_email_archive.py --search-body "password"

  # Show full email body
  python query_email_archive.py --sender "example.com" --show-body

  # Export results to JSON
  python query_email_archive.py --sender "example.com" --export results.json
        """
    )
    
    parser.add_argument("--db", default=DB_FILE, help=f"Database file path (default: {DB_FILE})")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--sender", help="Filter by sender (partial match)")
    parser.add_argument("--recipient", help="Filter by recipient (partial match)")
    parser.add_argument("--subject", help="Filter by subject (partial match)")
    parser.add_argument("--date-from", help="Filter emails from this date (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="Filter emails to this date (YYYY-MM-DD)")
    parser.add_argument("--domain", help="Filter by domain found in email body")
    parser.add_argument("--search-body", help="Search for text in email body")
    parser.add_argument("--limit", type=int, default=50, help="Maximum number of results (default: 50)")
    parser.add_argument("--offset", type=int, default=0, help="Offset for pagination (default: 0)")
    parser.add_argument("--show-body", action="store_true", help="Show email body in results")
    parser.add_argument("--max-body-length", type=int, default=200, help="Max body length when showing body (default: 200)")
    parser.add_argument("--export", help="Export results to JSON file")
    
    args = parser.parse_args()
    
    conn = connect_db(args.db)
    
    # Show statistics
    if args.stats:
        stats = get_stats(conn)
        print("\n" + "="*60)
        print("EMAIL ARCHIVE STATISTICS")
        print("="*60)
        print(f"Total emails: {stats['total_emails']}")
        print(f"Oldest email: {stats['oldest_email']}")
        print(f"Newest email: {stats['newest_email']}")
        print(f"Emails with domains found: {stats['emails_with_domains']}")
        print("\nTop 10 Senders:")
        for sender_info in stats['top_senders']:
            print(f"  {sender_info['sender']}: {sender_info['count']} emails")
        print("="*60 + "\n")
        conn.close()
        return
    
    # Check if any filters are provided
    has_filters = any([
        args.sender, args.recipient, args.subject,
        args.date_from, args.date_to, args.domain, args.search_body
    ])
    
    if not has_filters:
        print("No filters specified. Use --stats to see database statistics,")
        print("or provide at least one filter (--sender, --subject, etc.)")
        print("Use --help for examples.")
        conn.close()
        return
    
    # Query emails
    rows = query_emails(
        conn,
        sender=args.sender,
        recipient=args.recipient,
        subject=args.subject,
        date_from=args.date_from,
        date_to=args.date_to,
        domain=args.domain,
        search_body=args.search_body,
        limit=args.limit,
        offset=args.offset,
        show_body=args.show_body,
        max_body_length=args.max_body_length
    )
    
    if not rows:
        print("No emails found matching the criteria.")
        conn.close()
        return
    
    # Display results
    print(f"\nFound {len(rows)} email(s):\n")
    print("="*80)
    
    for i, row in enumerate(rows, 1):
        print(f"\n[{i}/{len(rows)}]")
        print("-"*80)
        print(format_email(row, show_body=args.show_body, max_body_length=args.max_body_length))
        print()
    
    print("="*80)
    print(f"\nTotal: {len(rows)} email(s)")
    
    # Export if requested
    if args.export:
        export_to_json(rows, args.export)
    
    conn.close()


if __name__ == "__main__":
    main()

