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


GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")

UPI_ID = os.getenv("UPI_ID", "")
PAYEE_NAME = os.getenv("PAYEE_NAME", "")
BUSINESS_EMAIL = os.getenv("BUSINESS_EMAIL", GMAIL_USER)

DB_PATH = os.getenv("DB_PATH", "./hdfc_payments.db")
SEARCH_DAYS = max(1, int(os.getenv("SEARCH_DAYS", "10")))
POLL_INTERVAL = max(3, int(os.getenv("POLL_INTERVAL", "5")))
HDFC_EMAIL_FROM = os.getenv("HDFC_EMAIL_FROM", "").strip().lower()
VERIFY_INCOMING_ONLY = os.getenv("VERIFY_INCOMING_ONLY", "true").lower() in {
    "1", "true", "yes", "on"
}

if not GMAIL_USER or not GMAIL_APP_PASSWORD:
    raise RuntimeError("Missing GMAIL_USER or GMAIL_APP_PASSWORD")
if not UPI_ID or "@" not in UPI_ID:
    raise RuntimeError("UPI_ID must contain a valid UPI ID")
if not PAYEE_NAME.strip():
    raise RuntimeError("PAYEE_NAME cannot be empty")

collector_running = False
collector_started_at = None
collector_last_check = None
collector_last_success = None
collector_last_error = None
collector_last_uid = 0
collector_total_scanned = 0
collector_total_saved = 0
collector_total_rejected = 0
collector_mailbox = ""

collector_thread = None
collector_stop_event = threading.Event()
collector_lock = threading.Lock()


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


def table_columns(conn, table):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def init_db():
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_uid INTEGER,
                message_id TEXT,
                utr TEXT,
                amount REAL,
                sender_name TEXT,
                note TEXT,
                subject TEXT,
                sender_email TEXT,
                received_at TEXT,
                cached_at TEXT NOT NULL,
                raw_source TEXT DEFAULT 'HDFC_BANK_EMAIL',
                direction TEXT DEFAULT 'UNKNOWN',
                is_verified INTEGER DEFAULT 0
            )
        """)

        cols = table_columns(conn, "payments")
        migrations = {
            "direction": "ALTER TABLE payments ADD COLUMN direction TEXT DEFAULT 'UNKNOWN'",
            "is_verified": "ALTER TABLE payments ADD COLUMN is_verified INTEGER DEFAULT 0",
        }
        for col, sql in migrations.items():
            if col not in cols:
                conn.execute(sql)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS collector_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_utr ON payments(utr)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_received ON payments(received_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_message_id ON payments(message_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_payments_direction ON payments(direction)")
        conn.commit()


init_db()


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
    finally:
        conn.close()


def get_state(key, default=None):
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM collector_state WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default


def set_state(key, value):
    with get_db() as conn:
        conn.execute("""
            INSERT INTO collector_state(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, str(value)))
        conn.commit()


def save_payment(
    email_uid, message_id, utr, amount, sender_name, note,
    subject, sender_email, received_at, direction
):
    with get_db() as conn:
        if message_id:
            existing = conn.execute(
                "SELECT id FROM payments WHERE message_id = ? LIMIT 1",
                (message_id,)
            ).fetchone()
            if existing:
                return False

        if email_uid is not None:
            existing = conn.execute(
                "SELECT id FROM payments WHERE email_uid = ? LIMIT 1",
                (email_uid,)
            ).fetchone()
            if existing:
                return False

        conn.execute("""
            INSERT INTO payments (
                email_uid, message_id, utr, amount, sender_name, note,
                subject, sender_email, received_at, cached_at, raw_source,
                direction, is_verified
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'HDFC_BANK_EMAIL', ?, 0)
        """, (
            email_uid, message_id or None, utr, amount, sender_name, note,
            subject, sender_email, received_at, iso_now(), direction
        ))
        conn.commit()
        return True


class HDFCBankEmailCollector:
    def __init__(self, gmail_user, app_password):
        self.gmail_user = gmail_user
        self.app_password = app_password
        self.mail = None
        self.mailbox = None

    def connect(self):
        self.disconnect()
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(self.gmail_user, self.app_password)

        selected = None
        for mailbox in ("[Gmail]/All Mail", "All Mail", "INBOX"):
            try:
                status, _ = mail.select(mailbox)
                if status == "OK":
                    selected = mailbox
                    break
            except Exception:
                continue

        if not selected:
            try:
                mail.logout()
            except Exception:
                pass
            raise RuntimeError("Could not select Gmail All Mail/INBOX")

        self.mail = mail
        self.mailbox = selected

        global collector_mailbox
        collector_mailbox = selected
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
        self.mailbox = None

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
            result = []
            for part, encoding in decode_header(value):
                if isinstance(part, bytes):
                    result.append(part.decode(encoding or "utf-8", errors="ignore"))
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
                content_type = (part.get_content_type() or "").lower()
                disposition = str(part.get("Content-Disposition") or "").lower()

                if "attachment" in disposition:
                    continue
                if content_type not in {"text/plain", "text/html"}:
                    continue

                try:
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue
                    charset = part.get_content_charset() or "utf-8"
                    decoded = payload.decode(charset, errors="ignore")
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
                    charset = msg.get_content_charset() or "utf-8"
                    decoded = payload.decode(charset, errors="ignore")
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
            html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
            html = re.sub(r"</p\s*>", "\n", html, flags=re.I)
            html = re.sub(r"<[^>]+>", " ", html)
            html = re.sub(r"\s+", " ", html)
            return html.strip()

        return ""

    @staticmethod
    def clean_text(text):
        text = text or ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"\r\n?", "\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    @staticmethod
    def sender_is_hdfc(sender):
        sender = (sender or "").lower()
        if HDFC_EMAIL_FROM:
            return HDFC_EMAIL_FROM in sender
        return (
            "@hdfcbank.net" in sender
            or "@hdfc.bank.in" in sender
            or "@hdfcbank.com" in sender
        )

    @classmethod
    def is_transaction_email(cls, sender, subject, body):
        if not cls.sender_is_hdfc(sender):
            return False

        combined = f"{sender} {subject} {body}".lower()
        transaction_words = (
            "upi", "transaction", "credited", "credit", "debited", "debit",
            "utr", "reference", "ref no", "payment", "amount", "vpa",
        )
        return any(word in combined for word in transaction_words)

    @staticmethod
    def parse_amount(text):
        patterns = [
            r"(?:INR|Rs\.?|₹)\s*([\d,]+(?:\.\d{1,2})?)",
            r"([\d,]+(?:\.\d{1,2})?)\s*(?:INR|Rs\.?|₹)",
            r"(?:amount|amount credited|amount debited|credit amount|debit amount|transaction amount)"
            r"[^0-9]{0,40}([\d,]+(?:\.\d{1,2})?)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.I)
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
        labelled_patterns = [
            r"UPI\s+TRANSACTION\s+REFERENCE\s+(?:NO|NUMBER)\.?\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
            r"UPI\s+TRANSACTION\s+REF(?:ERENCE)?\.?\s*(?:NO|NUMBER)?\.?\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
            r"UPI\s*(?:REF|REFERENCE)\s*(?:NO|NUMBER|ID)?\.?\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
            r"TRANSACTION\s*(?:ID|REFERENCE|REF)\s*(?:NO|NUMBER|ID)?\.?\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
            r"\bUTR\b\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
            r"\bREFERENCE\s*(?:NO|NUMBER|ID)?\.?\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
            r"\bREF\s*(?:NO|NUMBER|ID)?\.?\s*[:#=\-]?\s*([A-Z0-9]{8,30})",
        ]
        for pattern in labelled_patterns:
            for candidate in re.findall(pattern, text, re.I):
                candidate = candidate.strip().upper().rstrip(".")
                if candidate in {"NUMBER", "REFERENCE", "TRANSACTION", "PAYMENT", "NO", "ID"}:
                    continue
                if 8 <= len(candidate) <= 30:
                    return candidate

        if re.search(r"\bUPI\b", text, re.I):
            numeric = re.findall(r"\b\d{10,18}\b", text)
            if numeric:
                return numeric[0]
        return None

    @staticmethod
    def parse_sender_name(text):
        patterns = [
            r"\bVPA\s+[^\s()]+?\s*\(([^)]+)\)",
            r"(?:paid\s+by|received\s+from)\s*[:\-]?\s*([A-Za-z][A-Za-z .&'_-]{1,80})",
            r"(?:from|by|sender|payer|remitter|received\s+from)\s*[:\-]\s*([A-Za-z][A-Za-z .&'_-]{1,80})",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.I)
            if m:
                candidate = m.group(1).strip()
                candidate = re.split(
                    r"\s+(?:via|on|using|through|for|ref|utr)\b",
                    candidate, maxsplit=1, flags=re.I
                )[0].strip()
                if 2 <= len(candidate) <= 80:
                    return candidate
        return None

    @staticmethod
    def parse_note(text):
        patterns = [
            r"(?:note|remarks?|remark|description|info)\s*[:=\-]\s*([A-Za-z0-9_.#:/ @&(),_-]{1,120})",
            r"(?:reference|ref)\s*[:=\-]\s*([A-Za-z0-9_.#:/ @&(),_-]{1,120})",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.I)
            if m:
                return m.group(1).strip()[:120]
        return None

    @staticmethod
    def parse_direction(text):
        t = text.lower()
        if re.search(r"\b(?:is|has been|was)\s+credited\b", t):
            return "CREDIT"
        if re.search(r"\b(?:is|has been|was)\s+debited\b", t):
            return "DEBIT"
        if any(x in t for x in (
            "amount credited", "account credited", "a/c credited",
            "ac credited", "upi payment received", "payment received",
        )):
            return "CREDIT"
        if any(x in t for x in (
            "amount debited", "account debited", "a/c debited", "ac debited",
        )):
            return "DEBIT"
        return "UNKNOWN"

    @classmethod
    def parse_payment(cls, body, subject=""):
        normalized = cls.clean_text(body)
        return {
            "amount": cls.parse_amount(normalized),
            "utr": cls.parse_utr(normalized),
            "sender_name": cls.parse_sender_name(normalized),
            "note": cls.parse_note(normalized),
            "direction": cls.parse_direction(normalized),
        }

    def fetch_message(self, uid):
        status, msg_data = self.mail.uid("fetch", str(uid), "(RFC822)")
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
        global collector_total_rejected
        try:
            msg = self.fetch_message(uid)
            if not msg:
                collector_total_rejected += 1
                return False

            sender = self.decode_header_value(msg.get("From") or "")
            subject = self.decode_header_value(msg.get("Subject") or "")
            body = self.extract_body(msg)

            if not self.is_transaction_email(sender, subject, body):
                collector_total_rejected += 1
                return False

            parsed = self.parse_payment(body, subject)
            message_id = (msg.get("Message-ID") or "").strip()
            received_at = parse_email_date(msg.get("Date"))

            return save_payment(
                email_uid=uid,
                message_id=message_id,
                utr=parsed["utr"],
                amount=parsed["amount"],
                sender_name=parsed["sender_name"],
                note=parsed["note"],
                subject=subject,
                sender_email=sender,
                received_at=received_at,
                direction=parsed["direction"],
            )
        except Exception as exc:
            print(f"[Collector] UID {uid} error: {exc}")
            collector_total_rejected += 1
            return False

    def search_since(self):
        since_date = (
            datetime.now() - timedelta(days=SEARCH_DAYS + 1)
        ).strftime("%d-%b-%Y")

        if HDFC_EMAIL_FROM:
            query = f'(FROM "{HDFC_EMAIL_FROM}" SINCE "{since_date}")'
        else:
            query = f'(SINCE "{since_date}")'

        status, data = self.mail.search(None, query)
        if status != "OK":
            raise RuntimeError("Gmail history search failed")
        return data[0].split()

    def initial_backfill(self):
        global collector_last_uid, collector_total_scanned, collector_total_saved
        self.ensure_connection()
        email_ids = self.search_since()

        print(f"[Collector] Backfill: {len(email_ids)} emails found in {self.mailbox}")
        highest_uid = 0

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

        print(f"[Collector] Backfill complete. DB payments: {self.get_payment_count()}")

    def fetch_new_emails(self):
        global collector_last_uid, collector_total_scanned, collector_total_saved
        self.ensure_connection()

        try:
            last_uid = int(get_state("last_uid", "0"))
        except Exception:
            last_uid = 0

        status, data = self.mail.uid("search", None, f"UID {last_uid + 1}:*")
        if status != "OK":
            raise RuntimeError("Gmail incremental search failed")

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
            row = conn.execute("SELECT COUNT(*) AS count FROM payments").fetchone()
            return int(row["count"])


collector = HDFCBankEmailCollector(GMAIL_USER, GMAIL_APP_PASSWORD)


def collector_worker():
    global collector_running, collector_started_at
    global collector_last_check, collector_last_success, collector_last_error

    collector_running = True
    collector_started_at = iso_now()

    print("=" * 70)
    print("HDFC BANK UPI / UTR VERIFIER")
    print("=" * 70)

    try:
        collector.initial_backfill()
        print(f"[Collector] Live monitoring every {POLL_INTERVAL} seconds")

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
                print("[Collector] Cycle error:", str(exc))
                try:
                    collector.disconnect()
                except Exception:
                    pass

            elapsed = time.time() - started
            sleep_for = max(0, POLL_INTERVAL - elapsed)
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


app = FastAPI(
    title="HDFC Bank UPI UTR Verification API",
    version="2.0.0",
    description="HDFC email based UPI transaction collector and UTR verifier.",
)


@app.on_event("startup")
async def startup_event():
    global collector_thread
    collector_stop_event.clear()
    collector_thread = threading.Thread(
        target=collector_worker,
        daemon=True,
        name="HDFCBankPaymentCollector",
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


def make_upi_url():
    params = {"pa": UPI_ID, "pn": PAYEE_NAME, "cu": "INR"}
    return "upi://pay?" + urllib.parse.urlencode(
        params, quote_via=urllib.parse.quote
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
    image = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


@app.get("/", response_class=HTMLResponse)
async def root():
    return RedirectResponse("/qr", status_code=302)


@app.get("/qr.png")
async def qr_png():
    return Response(
        content=make_qr_png(),
        media_type="image/png",
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.get("/qr", response_class=HTMLResponse)
async def qr_page():
    safe_name = (
        PAYEE_NAME.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
    safe_email = (
        BUSINESS_EMAIL.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>__NAME__ • UPI Payment</title>
<style>
*{box-sizing:border-box}
body{
margin:0;min-height:100vh;display:flex;align-items:center;
justify-content:center;padding:24px;font-family:Inter,system-ui,sans-serif;
background:radial-gradient(circle at top,#eef4ff,#f7f9fc 42%,#eef1f6);
color:#111827
}
.card{
width:min(430px,100%);background:rgba(255,255,255,.96);
border:1px solid rgba(15,23,42,.08);border-radius:28px;
padding:30px 24px 26px;text-align:center;
box-shadow:0 22px 70px rgba(15,23,42,.12)
}
.brand{font-size:22px;font-weight:750;margin-bottom:5px}
.label{font-size:13px;color:#667085;margin-bottom:22px}
.qr-wrap{display:inline-flex;padding:15px;background:#fff;border-radius:20px;
border:1px solid #e5e7eb;box-shadow:0 10px 30px rgba(15,23,42,.08)}
.qr{display:block;width:min(285px,70vw);height:auto}
.email{margin-top:22px;font-size:14px;color:#475467;word-break:break-word}
.email strong{color:#111827}
.secure{margin-top:14px;font-size:11px;color:#98a2b3}
.links{margin-top:18px}
.links a{text-decoration:none;color:#2563eb;font-size:13px}
</style>
</head>
<body>
<main class="card">
<div class="brand">__NAME__</div>
<div class="label">Scan to pay via UPI</div>
<div class="qr-wrap">
<img class="qr" src="/qr.png" alt="UPI payment QR code" width="285" height="285">
</div>
<div class="email"><strong>__EMAIL__</strong></div>
<div class="links"><a href="/payment">Payment History</a></div>
<div class="secure">UPI payment • Amount entered by payer</div>
</main>
</body>
</html>"""
    return html.replace("__NAME__", safe_name).replace("__EMAIL__", safe_email)


class VerifyUTRRequest(BaseModel):
    utr: str


def normalize_utr(value):
    value = (value or "").strip().upper()
    value = re.sub(r"[\s\-]+", "", value)
    if not re.fullmatch(r"[A-Z0-9]{8,30}", value):
        raise HTTPException(status_code=400, detail="Invalid UTR format")
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
        "direction": row["direction"],
        "bank": "HDFC Bank",
        "source": "HDFC_BANK_EMAIL",
    }


@app.get("/verify-utr")
async def verify_utr(utr: str):
    utr = normalize_utr(utr)

    with get_db() as conn:
        if VERIFY_INCOMING_ONLY:
            row = conn.execute("""
                SELECT utr, amount, sender_name, note, received_at, direction
                FROM payments
                WHERE utr = ? AND direction = 'CREDIT'
                ORDER BY received_at DESC
                LIMIT 1
            """, (utr,)).fetchone()
        else:
            row = conn.execute("""
                SELECT utr, amount, sender_name, note, received_at, direction
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
            "message": (
                "UTR not found as an incoming HDFC payment"
                if VERIFY_INCOMING_ONLY
                else "UTR not found in cached HDFC transaction emails"
            ),
        }

    return payment_to_response(row)


@app.post("/verify-utr")
async def verify_utr_post(req: VerifyUTRRequest):
    return await verify_utr(req.utr)


def esc(value):
    value = "" if value is None else str(value)
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


@app.get("/payment", response_class=HTMLResponse)
async def payment_page():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, utr, amount, sender_name, note, subject,
                   sender_email, received_at, direction, is_verified
            FROM payments
            ORDER BY datetime(received_at) DESC, id DESC
        """).fetchall()

        stats = conn.execute("""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0) AS credits,
                COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END), 0) AS debits,
                COALESCE(SUM(CASE WHEN utr IS NOT NULL AND utr != '' THEN 1 ELSE 0 END), 0) AS utr_count
            FROM payments
        """).fetchone()

    cards = []
    for row in rows:
        direction = row["direction"] or "UNKNOWN"
        direction_class = direction.lower()
        utr = esc(row["utr"] or "UTR not parsed")
        amount = (
            f"₹{float(row['amount']):,.2f}"
            if row["amount"] is not None else "Amount unavailable"
        )
        sender = esc(row["sender_name"] or "Unknown")
        note = esc(row["note"] or "")
        received = esc(row["received_at"] or "")
        subject = esc(row["subject"] or "")
        sender_email = esc(row["sender_email"] or "")

        cards.append(f"""
        <article class="payment-card"
                 data-search="{utr} {sender} {note} {subject} {sender_email}">
            <div class="top">
                <div>
                    <div class="utr">{utr}</div>
                    <div class="date">{received}</div>
                </div>
                <span class="badge {direction_class}">{esc(direction)}</span>
            </div>
            <div class="amount">{amount}</div>
            <div class="meta">
                <div><span>Sender</span><b>{sender}</b></div>
                <div><span>Email</span><b>{sender_email}</b></div>
                <div><span>Note</span><b>{note or "—"}</b></div>
            </div>
        </article>
        """)

    cards_html = "".join(cards)
    safe_name = esc(PAYEE_NAME)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__NAME__ • Payment History</title>
<style>
*{box-sizing:border-box}
body{
margin:0;min-height:100vh;font-family:Inter,system-ui,-apple-system,sans-serif;
background:
radial-gradient(circle at 10% 0%,rgba(59,130,246,.16),transparent 28%),
radial-gradient(circle at 90% 10%,rgba(14,165,233,.12),transparent 25%),
#f5f7fb;color:#111827
}
.wrap{width:min(1100px,94%);margin:30px auto 60px}
.header{display:flex;justify-content:space-between;align-items:center;gap:15px;
margin-bottom:20px;flex-wrap:wrap}
h1{margin:0;font-size:28px;letter-spacing:-.03em}
.sub{color:#667085;font-size:13px;margin-top:5px}
.back{padding:11px 16px;border-radius:14px;text-decoration:none;color:#111827;
background:rgba(255,255,255,.7);border:1px solid rgba(15,23,42,.08);
backdrop-filter:blur(16px)}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px}
.stat{padding:18px;border-radius:22px;background:rgba(255,255,255,.72);
border:1px solid rgba(255,255,255,.8);box-shadow:0 12px 35px rgba(15,23,42,.07);
backdrop-filter:blur(18px)}
.stat span{display:block;color:#667085;font-size:12px;margin-bottom:7px}
.stat b{font-size:22px}
.search{width:100%;padding:15px 18px;border:1px solid rgba(15,23,42,.08);
border-radius:17px;background:rgba(255,255,255,.8);outline:none;font-size:15px;
margin-bottom:18px}
.list{display:grid;gap:12px}
.payment-card{padding:20px;border-radius:24px;background:rgba(255,255,255,.76);
border:1px solid rgba(255,255,255,.9);box-shadow:0 12px 35px rgba(15,23,42,.07);
backdrop-filter:blur(18px)}
.top{display:flex;justify-content:space-between;gap:15px;align-items:flex-start}
.utr{font-weight:750;font-size:17px;word-break:break-all}
.date{color:#667085;font-size:12px;margin-top:5px}
.badge{padding:6px 10px;border-radius:999px;font-size:11px;font-weight:750}
.credit{background:#dcfce7;color:#166534}
.debit{background:#fee2e2;color:#991b1b}
.unknown{background:#e5e7eb;color:#374151}
.amount{font-size:28px;font-weight:800;margin:16px 0}
.meta{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
.meta div{padding:12px;border-radius:15px;background:rgba(248,250,252,.8)}
.meta span{display:block;color:#98a2b3;font-size:11px;margin-bottom:4px}
.meta b{font-size:13px;word-break:break-word}
.empty{padding:50px;text-align:center;color:#667085}
@media(max-width:700px){
.stats{grid-template-columns:repeat(2,1fr)}
.meta{grid-template-columns:1fr}
h1{font-size:23px}
}
</style>
</head>
<body>
<div class="wrap">
<div class="header">
<div>
<h1>Payment History</h1>
<div class="sub">__NAME__ • All transactions saved in SQLite</div>
</div>
<a class="back" href="/qr">← QR</a>
</div>

<div class="stats">
<div class="stat"><span>Total Records</span><b>__TOTAL__</b></div>
<div class="stat"><span>Credits</span><b>₹__CREDITS__</b></div>
<div class="stat"><span>Debits</span><b>₹__DEBITS__</b></div>
<div class="stat"><span>UTRs Saved</span><b>__UTRS__</b></div>
</div>

<input id="search" class="search"
       placeholder="Search UTR, sender, email, note..."
       autocomplete="off">

<div id="list" class="list">
__CARDS__
</div>
</div>

<script>
const input = document.getElementById('search');
const cards = [...document.querySelectorAll('.payment-card')];

input.addEventListener('input', () => {
    const q = input.value.toLowerCase().trim();

    cards.forEach(card => {
        card.style.display =
            card.dataset.search.toLowerCase().includes(q) ? '' : 'none';
    });
});
</script>
</body>
</html>"""

    html = (
        html.replace("__NAME__", safe_name)
        .replace("__TOTAL__", str(int(stats["total"] or 0)))
        .replace("__CREDITS__", f"{float(stats['credits'] or 0):,.2f}")
        .replace("__DEBITS__", f"{float(stats['debits'] or 0):,.2f}")
        .replace("__UTRS__", str(int(stats["utr_count"] or 0)))
        .replace("__CARDS__", cards_html if cards_html else '<div class="empty">No payments saved yet.</div>')
    )
    return html


@app.get("/health")
async def health():
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM payments").fetchone()

    return {
        "status": "ok",
        "service": "hdfc-bank-utr-verifier",
        "collector_running": collector_running,
        "stored_payments": row["count"],
        "history_scan_days": SEARCH_DAYS,
        "poll_interval_seconds": POLL_INTERVAL,
        "mailbox": collector_mailbox,
        "db_path": DB_PATH,
    }


@app.get("/collector/status")
async def collector_status():
    with get_db() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) AS count,
                SUM(CASE WHEN direction='CREDIT' THEN 1 ELSE 0 END) AS credits,
                SUM(CASE WHEN direction='DEBIT' THEN 1 ELSE 0 END) AS debits
            FROM payments
        """).fetchone()

    return {
        "running": collector_running,
        "started_at": collector_started_at,
        "last_check": collector_last_check,
        "last_success": collector_last_success,
        "last_error": collector_last_error,
        "last_uid": collector_last_uid,
        "total_scanned": collector_total_scanned,
        "total_saved": collector_total_saved,
        "total_rejected": collector_total_rejected,
        "stored_payments": row["count"],
        "credit_records": row["credits"] or 0,
        "debit_records": row["debits"] or 0,
        "history_scan_days": SEARCH_DAYS,
        "poll_interval_seconds": POLL_INTERVAL,
        "mailbox": collector_mailbox,
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
        return {"success": False, "error": str(exc)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
