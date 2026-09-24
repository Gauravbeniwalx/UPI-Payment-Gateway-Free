import imaplib
import email
import re
import os
import sqlite3
import threading
import time
import urllib.parse
from io import BytesIO
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from email.header import decode_header

import qrcode
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response, HTMLResponse, RedirectResponse
from pydantic import BaseModel


# ============================================================
# HDFC BANK UPI / UTR VERIFIER
# ------------------------------------------------------------
# Flow:
#   HDFC transaction email -> Gmail IMAP collector -> SQLite
#   -> /verify-utr API
#
# QR:
#   Dynamic UPI QR with NO fixed amount.
#   Only UPI ID/payee are encoded.
#
# History:
#   Last 10 days are scanned on startup and permanently kept
#   in SQLite. Older records are not automatically deleted.
# ============================================================


# ============================================================
# CONFIG
# ============================================================

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

UPI_ID = os.getenv("UPI_ID")
PAYEE_NAME = os.getenv("PAYEE_NAME")
BUSINESS_EMAIL = os.getenv("BUSINESS_EMAIL", GMAIL_USER or "")

DB_PATH = os.getenv("DB_PATH", "./hdfc_payments.db")

# Required history window.
SEARCH_DAYS = int(os.getenv("SEARCH_DAYS", "10"))

# Gmail polling interval.
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "3"))

# Optional exact sender filter.
# Example: alerts@hdfcbank.net
# Leave empty to accept HDFC-domain senders and validate the email content.
HDFC_EMAIL_FROM = os.getenv("HDFC_EMAIL_FROM", "").strip().lower()

# If true, only credit/incoming transaction emails are stored.
CREDIT_ONLY = os.getenv("CREDIT_ONLY", "true").lower() in {
    "1", "true", "yes", "on"
}

# Never automatically delete transaction history.
KEEP_HISTORY_FOREVER = True


if not GMAIL_USER or not GMAIL_APP_PASSWORD:
    raise RuntimeError(
        "Missing GMAIL_USER or GMAIL_APP_PASSWORD environment variables"
    )

if not UPI_ID or "@" not in UPI_ID:
    raise RuntimeError("UPI_ID must contain a valid UPI ID such as name@bank")

if not PAYEE_NAME.strip():
    raise RuntimeError("PAYEE_NAME cannot be empty")


# ============================================================
# GLOBAL STATE
# ============================================================

collector_running = False
collector_started_at = None
collector_last_check = None
collector_last_success = None
collector_last_error = None
collector_last_uid = 0
collector_total_scanned = 0
collector_total_saved = 0

collector_thread = None
collector_stop_event = threading.Event()
collector_lock = threading.Lock()


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat()


def parse_email_date(value):
    if not value:
        return iso_now()

    try:
        dt = email.utils.parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return iso_now()


# ============================================================
# DATABASE
# ============================================================

def init_db():
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_uid INTEGER UNIQUE,
                message_id TEXT,
                utr TEXT,
                amount REAL,
                sender_name TEXT,
                note TEXT,
                subject TEXT,
                sender_email TEXT,
                received_at TEXT,
                cached_at TEXT NOT NULL,
                raw_source TEXT DEFAULT 'HDFC_BANK_EMAIL'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS collector_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_utr
            ON payments(utr)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_received
            ON payments(received_at)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_message_id
            ON payments(message_id)
        """)

        conn.commit()


init_db()


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def get_state(key, default=None):
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM collector_state WHERE key = ?",
            (key,)
        ).fetchone()
        return row["value"] if row else default


def set_state(key, value):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO collector_state(key, value)
            VALUES (?, ?)
            ON CONFLICT(key)
            DO UPDATE SET value = excluded.value
        """, (key, str(value)))
        conn.commit()


def save_payment(
    email_uid,
    message_id,
    utr,
    amount,
    sender_name,
    note,
    subject,
    sender_email,
    received_at
):
    with get_db() as conn:
        # UID is the strongest duplicate key for one Gmail mailbox.
        if email_uid is not None:
            existing = conn.execute(
                "SELECT id FROM payments WHERE email_uid = ? LIMIT 1",
                (email_uid,)
            ).fetchone()
            if existing:
                return False

        # Message-ID protects against a repeated scan.
        if message_id:
            existing = conn.execute(
                "SELECT id FROM payments WHERE message_id = ? LIMIT 1",
                (message_id,)
            ).fetchone()
            if existing:
                return False

        conn.execute("""
            INSERT INTO payments (
                email_uid,
                message_id,
                utr,
                amount,
                sender_name,
                note,
                subject,
                sender_email,
                received_at,
                cached_at,
                raw_source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'HDFC_BANK_EMAIL')
        """, (
            email_uid,
            message_id,
            utr,
            amount,
            sender_name,
            note,
            subject,
            sender_email,
            received_at,
            iso_now()
        ))
        conn.commit()
        return True


# ============================================================
# EMAIL COLLECTOR
# ============================================================

class HDFCBankEmailCollector:

    def __init__(self, gmail_user, app_password):
        self.gmail_user = gmail_user
        self.app_password = app_password
        self.mail = None

    def connect(self):
        self.disconnect()

        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(self.gmail_user, self.app_password)

        status, _ = mail.select("INBOX")
        if status != "OK":
            try:
                mail.logout()
            except Exception:
                pass
            raise RuntimeError("Could not select Gmail INBOX")

        self.mail = mail
        return mail

    def disconnect(self):
        if self.mail:
            try:
                self.mail.close()
            except Exception:
                pass

            try:
                self.mail.logout()
            except Exception:
                pass

        self.mail = None

    def ensure_connection(self):
        if self.mail is None:
            self.connect()
            return

        try:
            status, _ = self.mail.noop()
            if status != "OK":
                self.connect()
        except Exception:
            self.connect()

    @staticmethod
    def decode_header_value(value):
        if not value:
            return ""

        try:
            parts = decode_header(value)
            result = []

            for part, encoding in parts:
                if isinstance(part, bytes):
                    result.append(
                        part.decode(
                            encoding or "utf-8",
                            errors="ignore"
                        )
                    )
                else:
                    result.append(str(part))

            return "".join(result)
        except Exception:
            return str(value)

    @staticmethod
    def extract_body(msg):
        text_parts = []
        html_parts = []

        if msg.is_multipart():
            for part in msg.walk():
                content_type = (
                    part.get_content_type() or ""
                ).lower()

                disposition = str(
                    part.get("Content-Disposition") or ""
                ).lower()

                if "attachment" in disposition:
                    continue

                if content_type not in {"text/plain", "text/html"}:
                    continue

                try:
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue

                    charset = (
                        part.get_content_charset() or "utf-8"
                    )

                    decoded = payload.decode(
                        charset,
                        errors="ignore"
                    )

                    if content_type == "text/plain":
                        text_parts.append(decoded)
                    else:
                        html_parts.append(decoded)
                except Exception:
                    continue
        else:
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    charset = (
                        msg.get_content_charset() or "utf-8"
                    )
                    decoded = payload.decode(
                        charset,
                        errors="ignore"
                    )

                    if msg.get_content_type() == "text/html":
                        html_parts.append(decoded)
                    else:
                        text_parts.append(decoded)
            except Exception:
                pass

        if text_parts:
            return "\n".join(text_parts)

        if html_parts:
            html = "\n".join(html_parts)
            html = re.sub(
                r"<br\s*/?>",
                "\n",
                html,
                flags=re.IGNORECASE
            )
            html = re.sub(
                r"</p\s*>",
                "\n",
                html,
                flags=re.IGNORECASE
            )
            html = re.sub(r"<[^>]+>", " ", html)
            html = re.sub(r"\s+", " ", html)
            return html.strip()

        return ""

    @staticmethod
    def clean_text(text):
        text = text or ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\r\n?", "\n", text)
        return text.strip()

    @staticmethod
    def sender_is_hdfc(sender):
        sender = (sender or "").lower()

        if HDFC_EMAIL_FROM:
            return HDFC_EMAIL_FROM in sender

        # HDFC commonly uses hdfcbank.net / hdfc.bank.in domains.
        # We deliberately require a HDFC domain rather than trusting
        # a subject line containing the word HDFC.
        return (
            "@hdfcbank.net" in sender
            or "@hdfc.bank.in" in sender
            or "@hdfcbank.com" in sender
        )

    @staticmethod
    def looks_like_credit(body, subject):
        combined = f"{subject} {body}".lower()

        positive = (
            "credited",
            "credit",
            "received",
            "upi payment received",
            "amount credited",
            "a/c credited",
            "ac credited",
            "account credited",
        )

        negative = (
            "debited",
            "debit",
            "withdrawn",
            "payment made",
            "paid from",
            "a/c debited",
            "ac debited",
        )

        if any(x in combined for x in positive):
            return True

        if any(x in combined for x in negative):
            return False

        # If CREDIT_ONLY is enabled and the email does not explicitly
        # identify credit/debit, do not trust it as a credit.
        return not CREDIT_ONLY

    @classmethod
    def is_transaction_email(cls, sender, subject, body):
        if not cls.sender_is_hdfc(sender):
            return False

        combined = f"{sender} {subject} {body}".lower()

        transaction_words = (
            "upi",
            "transaction",
            "credited",
            "credit",
            "utr",
            "reference",
            "ref no",
            "payment",
            "amount",
        )

        return any(word in combined for word in transaction_words)

    @staticmethod
    def parse_amount(text):
        patterns = [
            # INR 1,234.56 / Rs. 1234 / ₹1234
            r"(?:INR|Rs\.?|₹)\s*([\d,]+(?:\.\d{1,2})?)",
            r"([\d,]+(?:\.\d{1,2})?)\s*(?:INR|Rs\.?|₹)",
            # Amount: 1,234.56 / Amount INR 1234
            r"(?:amount|amount credited|credit amount|transaction amount)"
            r"[^0-9]{0,30}([\d,]+(?:\.\d{1,2})?)",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    value = float(m.group(1).replace(",", ""))
                    if 0 < value < 100000000:
                        return value
                except Exception:
                    pass

        return None

    @staticmethod
    def parse_utr(text):
        # Most useful: label + reference value.
        labelled_patterns = [
            r"(?:UPI\s*(?:REF|REFERENCE)|UTR|"
            r"TRANSACTION\s*(?:ID|REFERENCE|REF)|"
            r"REFERENCE\s*(?:NO|NUMBER|ID)?|REF\s*(?:NO|NUMBER|ID))"
            r"\s*[:#=\-]?\s*([A-Z0-9]{8,30})",

            r"(?:UPI\s*ID|TRANSACTION\s*ID)"
            r"\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
        ]

        for pattern in labelled_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for candidate in matches:
                candidate = candidate.strip().upper()
                # Avoid obvious non-reference values.
                if candidate in {
                    "NUMBER", "REFERENCE", "TRANSACTION", "PAYMENT"
                }:
                    continue
                if len(candidate) >= 8:
                    return candidate

        # UPI bank references are often 12-digit numeric strings.
        if re.search(r"\bUPI\b", text, re.IGNORECASE):
            numeric = re.findall(r"\b\d{12,18}\b", text)
            if numeric:
                return numeric[0]

        return None

    @staticmethod
    def parse_sender_name(text):
        patterns = [
            r"(?:from|by|sender|payer|remitter|received\s+from)"
            r"\s*[:\-]\s*([A-Za-z][A-Za-z .&'_-]{1,80})",

            r"(?:paid\s+by|received\s+from)"
            r"\s+([A-Za-z][A-Za-z .&'_-]{1,80})",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                candidate = m.group(1).strip()
                candidate = re.split(
                    r"\s+(?:via|on|using|through|for|ref|utr)\b",
                    candidate,
                    maxsplit=1,
                    flags=re.IGNORECASE
                )[0].strip()

                if 2 <= len(candidate) <= 80:
                    return candidate

        return None

    @staticmethod
    def parse_note(text):
        patterns = [
            r"(?:note|remarks?|remark|description|info)"
            r"\s*[:=\-]\s*([A-Za-z0-9_.#:/ @&(),_-]{1,120})",
            r"(?:reference|ref)"
            r"\s*[:=\-]\s*([A-Za-z0-9_.#:/ @&(),_-]{1,120})",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                return m.group(1).strip()[:120]

        return None

    @classmethod
    def parse_payment(cls, body, subject=""):
        normalized = cls.clean_text(body)

        return {
            "amount": cls.parse_amount(normalized),
            "utr": cls.parse_utr(normalized),
            "sender_name": cls.parse_sender_name(normalized),
            "note": cls.parse_note(normalized),
        }

    def fetch_message(self, uid):
        status, msg_data = self.mail.uid(
            "fetch",
            str(uid),
            "(RFC822)"
        )

        if status != "OK":
            return None

        raw = None
        for item in msg_data:
            if isinstance(item, tuple):
                raw = item[1]
                break

        if not raw:
            return None

        return email.message_from_bytes(raw)

    def process_uid(self, uid):
        try:
            msg = self.fetch_message(uid)
            if not msg:
                return False

            sender = self.decode_header_value(
                msg.get("From") or ""
            )
            subject = self.decode_header_value(
                msg.get("Subject") or ""
            )
            body = self.extract_body(msg)

            if not self.is_transaction_email(
                sender,
                subject,
                body
            ):
                return False

            if not self.looks_like_credit(body, subject):
                return False

            parsed = self.parse_payment(body, subject)

            # UTR is the critical field for this service.
            if not parsed["utr"]:
                return False

            message_id = (
                msg.get("Message-ID") or ""
            ).strip()

            received_at = parse_email_date(
                msg.get("Date")
            )

            return save_payment(
                email_uid=uid,
                message_id=message_id,
                utr=parsed["utr"],
                amount=parsed["amount"],
                sender_name=parsed["sender_name"],
                note=parsed["note"],
                subject=subject,
                sender_email=sender,
                received_at=received_at
            )

        except Exception as exc:
            print(
                f"[Collector] UID {uid} error: {exc}"
            )
            return False

    def initial_backfill(self):
        global collector_last_uid
        global collector_total_scanned
        global collector_total_saved

        self.ensure_connection()

        # Gmail SINCE is date based. Use one extra day to avoid timezone
        # boundary surprises; the API itself returns the stored records.
        since = (
            datetime.now() -
            timedelta(days=SEARCH_DAYS)
        ).strftime("%d-%b-%Y")

        if HDFC_EMAIL_FROM:
            search_query = (
                f'(FROM "{HDFC_EMAIL_FROM}" '
                f'SINCE "{since}")'
            )
        else:
            search_query = f'(SINCE "{since}")'

        status, data = self.mail.search(
            None,
            search_query
        )

        if status != "OK":
            raise RuntimeError(
                "Gmail history search failed"
            )

        email_ids = data[0].split()
        highest_uid = 0

        print(
            f"[Collector] 10-day scan: {len(email_ids)} emails found"
        )

        for raw_uid in email_ids:
            try:
                uid = int(raw_uid)
            except Exception:
                continue

            highest_uid = max(highest_uid, uid)
            collector_total_scanned += 1

            if self.process_uid(uid):
                collector_total_saved += 1

        if highest_uid:
            collector_last_uid = highest_uid
            set_state("last_uid", highest_uid)

        print(
            f"[Collector] Backfill complete. "
            f"Stored payments: {self.get_payment_count()}"
        )

    def fetch_new_emails(self):
        global collector_last_uid
        global collector_total_scanned
        global collector_total_saved

        self.ensure_connection()

        try:
            last_uid = int(
                get_state("last_uid", "0")
            )
        except Exception:
            last_uid = 0

        status, data = self.mail.uid(
            "search",
            None,
            f"UID {last_uid + 1}:*"
        )

        if status != "OK":
            raise RuntimeError(
                "Gmail incremental search failed"
            )

        uid_list = data[0].split()
        if not uid_list:
            return 0

        highest_uid = last_uid
        saved_count = 0

        for raw_uid in uid_list:
            try:
                uid = int(raw_uid)
            except Exception:
                continue

            highest_uid = max(highest_uid, uid)
            collector_total_scanned += 1

            if self.process_uid(uid):
                collector_total_saved += 1
                saved_count += 1

        if highest_uid > last_uid:
            collector_last_uid = highest_uid
            set_state("last_uid", highest_uid)

        return saved_count

    def run_cycle(self):
        return self.fetch_new_emails()

    def get_payment_count(self):
        with get_db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM payments"
            ).fetchone()
            return int(row["count"])


# ============================================================
# COLLECTOR WORKER
# ============================================================

collector = HDFCBankEmailCollector(
    GMAIL_USER,
    GMAIL_APP_PASSWORD
)


def collector_worker():
    global collector_running
    global collector_started_at
    global collector_last_check
    global collector_last_success
    global collector_last_error

    collector_running = True
    collector_started_at = iso_now()

    print("=" * 64)
    print("HDFC BANK UPI / UTR VERIFIER")
    print("=" * 64)

    try:
        # First boot/restart: recover the last 10 days.
        collector.initial_backfill()

        print(
            f"[Collector] Live monitoring every "
            f"{POLL_INTERVAL} seconds"
        )

        while not collector_stop_event.is_set():
            started = time.time()

            try:
                collector.run_cycle()
                collector_last_success = iso_now()
                collector_last_error = None
                collector_last_check = iso_now()
            except Exception as exc:
                collector_last_error = str(exc)
                collector_last_check = iso_now()

                print(
                    "[Collector] Cycle error:",
                    str(exc)
                )

                try:
                    collector.disconnect()
                except Exception:
                    pass

            elapsed = time.time() - started
            sleep_for = max(
                0,
                POLL_INTERVAL - elapsed
            )

            collector_stop_event.wait(sleep_for)

    except Exception as exc:
        collector_last_error = str(exc)
        print("[Collector] Fatal error:", str(exc))

    finally:
        collector_running = False

        try:
            collector.disconnect()
        except Exception:
            pass

        print("[Collector] Worker stopped")


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="HDFC Bank UPI UTR Verification API",
    version="1.0.0",
    description=(
        "UPI QR + HDFC Bank email based UTR verification. "
        "No fixed amount is embedded in the QR."
    )
)


@app.on_event("startup")
async def startup_event():
    global collector_thread

    collector_stop_event.clear()

    collector_thread = threading.Thread(
        target=collector_worker,
        daemon=True,
        name="HDFCBankPaymentCollector"
    )

    collector_thread.start()

    print("[System] HDFC collector started")


@app.on_event("shutdown")
async def shutdown_event():
    collector_stop_event.set()

    try:
        collector.disconnect()
    except Exception:
        pass


# ============================================================
# QR HELPERS
# ============================================================

def make_upi_url():
    # IMPORTANT:
    # No 'am=' parameter is added.
    # Therefore the payer enters/selects the amount in the UPI app.
    params = {
        "pa": UPI_ID,
        "pn": PAYEE_NAME,
        "cu": "INR",
    }

    return "upi://pay?" + urllib.parse.urlencode(
        params,
        quote_via=urllib.parse.quote
    )


def make_qr_png():
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )

    qr.add_data(make_upi_url())
    qr.make(fit=True)

    image = qr.make_image(
        fill_color="black",
        back_color="white"
    )

    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


# ============================================================
# PROFESSIONAL QR PAGE
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse("/qr", status_code=302)


@app.get("/qr.png")
async def qr_png():
    return Response(
        content=make_qr_png(),
        media_type="image/png",
        headers={
            "Cache-Control": "no-store, max-age=0"
        }
    )


@app.get("/qr", response_class=HTMLResponse)
async def qr_page():
    # Keep the public page intentionally minimal:
    # business name + QR + email only.
    safe_name = (
        PAYEE_NAME
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

    safe_email = (
        BUSINESS_EMAIL
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport"
          content="width=device-width,initial-scale=1">

    <meta name="robots" content="noindex,nofollow">
    <meta name="theme-color" content="#0b1220">

    <title>{safe_name} • UPI Payment</title>

    <style>
        * {{
            box-sizing: border-box;
        }}

        html, body {{
            min-height: 100%;
            margin: 0;
        }}

        body {{
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 24px;

            font-family:
                Inter,
                ui-sans-serif,
                system-ui,
                -apple-system,
                BlinkMacSystemFont,
                "Segoe UI",
                sans-serif;

            background:
                radial-gradient(
                    circle at top,
                    #eef4ff 0,
                    #f7f9fc 38%,
                    #eef1f6 100%
                );

            color: #111827;
        }}

        .card {{
            width: min(430px, 100%);
            background: rgba(255,255,255,.96);
            border: 1px solid rgba(15,23,42,.08);
            border-radius: 28px;
            padding: 30px 24px 26px;
            text-align: center;

            box-shadow:
                0 22px 70px rgba(15,23,42,.12);
        }}

        .brand {{
            font-size: 22px;
            font-weight: 750;
            letter-spacing: -.02em;
            margin-bottom: 5px;
        }}

        .label {{
            font-size: 13px;
            color: #667085;
            margin-bottom: 22px;
        }}

        .qr-wrap {{
            display: inline-flex;
            align-items: center;
            justify-content: center;

            padding: 15px;
            background: #fff;
            border-radius: 20px;
            border: 1px solid #e5e7eb;

            box-shadow:
                0 10px 30px rgba(15,23,42,.08);
        }}

        .qr {{
            display: block;
            width: min(285px, 70vw);
            height: auto;
        }}

        .email {{
            margin-top: 22px;
            font-size: 14px;
            color: #475467;
            word-break: break-word;
        }}

        .email strong {{
            color: #111827;
            font-weight: 650;
        }}

        .secure {{
            margin-top: 14px;
            font-size: 11px;
            color: #98a2b3;
        }}
    </style>
</head>

<body>
    <main class="card">
        <div class="brand">{safe_name}</div>
        <div class="label">Scan to pay via UPI</div>

        <div class="qr-wrap">
            <img
                class="qr"
                src="/qr.png"
                alt="UPI payment QR code"
                width="285"
                height="285"
            >
        </div>

        <div class="email">
            <strong>{safe_email}</strong>
        </div>

        <div class="secure">
            UPI payment • Amount entered by payer
        </div>
    </main>
</body>
</html>"""


# ============================================================
# UTR VERIFICATION API
# ============================================================

class VerifyUTRRequest(BaseModel):
    utr: str


def normalize_utr(value):
    value = (value or "").strip().upper()

    # Remove spaces/hyphens users sometimes paste around a reference.
    value = re.sub(r"[\s\-]+", "", value)

    if not re.fullmatch(r"[A-Z0-9]{8,30}", value):
        raise HTTPException(
            status_code=400,
            detail="Invalid UTR format"
        )

    return value


def payment_to_response(row):
    return {
        "found": True,
        "verified": True,
        "utr": row["utr"],
        "amount": row["amount"],
        "sender_name": row["sender_name"],
        "note": row["note"],
        "received_at": row["received_at"],
        "bank": "HDFC Bank",
        "source": "HDFC_BANK_EMAIL",
    }


@app.get("/verify-utr")
async def verify_utr(utr: str):
    """
    Main verification API.

    Example:
      GET /verify-utr?utr=123456789012

    Returns found=true only when the UTR exists in the
    locally cached HDFC transaction emails.
    """
    utr = normalize_utr(utr)

    with get_db() as conn:
        row = conn.execute("""
            SELECT
                utr,
                amount,
                sender_name,
                note,
                received_at
            FROM payments
            WHERE utr = ?
            ORDER BY received_at DESC
            LIMIT 1
        """, (utr,)).fetchone()

    if not row:
        return {
            "found": False,
            "verified": False,
            "utr": utr,
            "bank": "HDFC Bank",
            "source": "HDFC_BANK_EMAIL",
            "message": "UTR not found in cached HDFC transaction emails",
        }

    return payment_to_response(row)


@app.post("/verify-utr")
async def verify_utr_post(req: VerifyUTRRequest):
    return await verify_utr(req.utr)


# ============================================================
# HEALTH / INTERNAL STATUS
# ------------------------------------------------------------
# These are operational endpoints, not payment APIs.
# ============================================================

@app.get("/health")
async def health():
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM payments"
        ).fetchone()

    return {
        "status": "ok",
        "service": "hdfc-bank-utr-verifier",
        "collector_running": collector_running,
        "stored_payments": row["count"],
        "history_scan_days": SEARCH_DAYS,
        "poll_interval_seconds": POLL_INTERVAL,
        "upi_id": UPI_ID,
    }


@app.get("/collector/status")
async def collector_status():
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM payments"
        ).fetchone()

    return {
        "running": collector_running,
        "started_at": collector_started_at,
        "last_check": collector_last_check,
        "last_success": collector_last_success,
        "last_error": collector_last_error,
        "last_uid": collector_last_uid,
        "total_scanned": collector_total_scanned,
        "total_saved": collector_total_saved,
        "stored_payments": row["count"],
        "history_scan_days": SEARCH_DAYS,
        "poll_interval_seconds": POLL_INTERVAL,
        "history_retention": "forever",
    }


@app.post("/collector/refresh")
async def collector_refresh():
    try:
        saved = collector.run_cycle()
        return {
            "success": True,
            "new_payments_saved": saved,
            "stored_payments": collector.get_payment_count(),
        }
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
        }


# ============================================================
# RUN DIRECTLY
# ============================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
    )
