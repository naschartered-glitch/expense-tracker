"""
WhatsApp Expense Tracker — v4 (Final pre-production)
Features: Receipt-first (POS/email/text alert/SMS) · Budget vs Actuals ·
          Legacy Balances · Multi-item splits · Quick queries · Debt ageing ·
          Debt settlement · Savings goals · Foreign currency text entry ·
          Sheet aggregation · Weekly email backup · Net worth with CBN FX ·
          Month-end prompts
"""

import os, json, requests, base64, re, csv, io, smtplib, traceback
from datetime import datetime, timedelta
from calendar import month_name
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from anthropic import Anthropic
import gspread
from google.oauth2.service_account import Credentials

app    = Flask(__name__)
claude = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

WA_TOKEN      = os.environ["WHATSAPP_TOKEN"]
WA_PHONE_ID   = os.environ["WHATSAPP_PHONE_ID"]
VERIFY_TOKEN  = os.environ["VERIFY_TOKEN"]
SHEET_ID      = os.environ["GOOGLE_SHEET_ID"]
SCOPES        = ["https://www.googleapis.com/auth/spreadsheets"]
EMAIL_ADDRESS = os.environ.get("EMAIL_ADDRESS","")
EMAIL_PASSWORD= os.environ.get("EMAIL_APP_PASSWORD","")
BACKUP_EMAIL  = "naschartered@gmail.com"
OWNER_PHONE   = os.environ.get("OWNER_PHONE","")
CRON_KEY      = os.environ.get("CRON_KEY","")

# ── Tab names & headers ───────────────────────────────────────────────────────
TAB_TX      = "Transactions"
TAB_BUDGET  = "Budgets"
TAB_LEGACY  = "Legacy"
TAB_SUMMARY = "Monthly Summary"
TAB_GOALS   = "Goals"

TX_HEADERS  = ["Date","Month","Year","Type","Category","Description","Merchant",
               "Amount (N)","Original Amount","Currency","FX Rate","Confidence",
               "Budget Month","Split Parent"]
BUDGET_HEADERS = ["Month","Year","Type","Budgeted (N)","Set On"]
LEGACY_HEADERS = ["Type","Category","Description","Amount (N)","As Of","Set On"]
SUMMARY_HEADERS= ["Month","Year","Income","Expense","Savings","Investment","Debt",
                  "Net","Updated On"]
GOAL_HEADERS   = ["Name","Type","Target (N)","Deadline","Current (N)","Status","Created On"]

TYPES = ["income","expense","savings","investment","debt"]
MONTH_NAMES = {m.lower(): i for i, m in enumerate(month_name) if m}
CURRENCY_SYMBOLS = {"NGN":"N","USD":"$","GBP":"£","EUR":"€","GHS":"₵"}

# Debt sub-types — defines direction
DEBT_OWED_TO_ME = ["lent","owed to me","receivable","loan out"]
DEBT_I_OWE      = ["borrowed","i owe","loan repayment","credit card","payable"]


# ════════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ════════════════════════════════════════════════════════════════════════════════

def get_workbook():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDS_JSON"]), scopes=SCOPES)
    return gspread.authorize(creds).open_by_key(SHEET_ID)

def get_or_create_tab(wb, title, headers):
    try:
        ws = wb.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = wb.add_worksheet(title=title, rows=5000, cols=len(headers))
        ws.append_row(headers)
    return ws

def tabs(wb):
    return (get_or_create_tab(wb, TAB_TX,      TX_HEADERS),
            get_or_create_tab(wb, TAB_BUDGET,   BUDGET_HEADERS),
            get_or_create_tab(wb, TAB_LEGACY,   LEGACY_HEADERS),
            get_or_create_tab(wb, TAB_SUMMARY,  SUMMARY_HEADERS),
            get_or_create_tab(wb, TAB_GOALS,    GOAL_HEADERS))


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def parse_month(text: str):
    """Parse a month reference. Returns (label, year). Safe on empty input."""
    if not text or not text.strip():
        return datetime.now().strftime("%B %Y"), datetime.now().year
    now   = datetime.now()
    text  = text.strip().lower()
    parts = text.split()
    year  = now.year
    if parts and parts[0] in MONTH_NAMES:
        mn = MONTH_NAMES[parts[0]]
        if len(parts) > 1 and parts[1].isdigit() and len(parts[1]) == 4:
            year = int(parts[1])
        return f"{month_name[mn]} {year}", year
    m = re.match(r"(\d{1,2})[/-](\d{4})", text)
    if m:
        return f"{month_name[int(m.group(1))]} {int(m.group(2))}", int(m.group(2))
    return now.strftime("%B %Y"), now.year

def current_month():
    return datetime.now().strftime("%B %Y")

def expand_amount(s: str) -> str:
    """Convert '100k' → '100000', '1.5m' → '1500000'."""
    s = s.strip().lower().replace(",","")
    s = re.sub(r"(\d+\.?\d*)k$", lambda m: str(int(float(m.group(1)) * 1000)), s)
    s = re.sub(r"(\d+\.?\d*)m$", lambda m: str(int(float(m.group(1)) * 1_000_000)), s)
    return s

def fmt_ngn(n: float) -> str:
    return f"N{abs(float(n)):,.0f}"

def days_left_in_month() -> int:
    """Days remaining in current calendar month. Safe for December."""
    now      = datetime.now()
    if now.month == 12:
        next_month = now.replace(year=now.year+1, month=1, day=1)
    else:
        next_month = now.replace(month=now.month+1, day=1)
    last_day = (next_month - timedelta(days=1)).day
    return last_day - now.day


# ════════════════════════════════════════════════════════════════════════════════
# CBN EXCHANGE RATE
# ════════════════════════════════════════════════════════════════════════════════

def get_cbn_usd_rate() -> float:
    """CBN official USD/NGN — previous business day. Three-tier fallback."""
    # Tier 1: CBN .asp endpoint (HTML table)
    try:
        url  = ("https://www.cbn.gov.ng/rates/ExchRateByCurrency.asp"
                "?beginrec=1&endrec=10&currencytype=$USD")
        resp = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/120 Safari/537.36",
            "Accept":   "text/html,application/xhtml+xml",
            "Referer":  "https://www.cbn.gov.ng/rates/ExchRateByCurrency.html"
        })
        soup = BeautifulSoup(resp.text, "html.parser")
        for row in soup.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cells) >= 2:
                try:
                    rate = float(cells[1].replace(",",""))
                    if rate > 100:
                        return rate
                except (ValueError, IndexError):
                    continue
    except Exception as e:
        print(f"CBN tier-1 failed: {e}")

    # Tier 2: exchangerate.host free API
    try:
        r    = requests.get("https://api.exchangerate.host/latest?base=USD&symbols=NGN",
                            timeout=5)
        rate = r.json().get("rates",{}).get("NGN", 0)
        if rate > 100:
            return float(rate)
    except Exception:
        pass

    # Tier 3: hardcoded fallback — update monthly
    return 1570.0


def convert_to_ngn(amount: float, currency: str, fx_rate: float = None) -> tuple:
    """
    Convert foreign amount to NGN.
    Returns (amount_ngn, fx_rate_used).
    If fx_rate provided (from receipt), use it. Otherwise fetch CBN rate.
    """
    if currency == "NGN":
        return amount, None
    if fx_rate and fx_rate > 0:
        return round(amount * fx_rate, 2), fx_rate
    rate = get_cbn_usd_rate()   # works for USD; GBP/EUR get proximate via same rate
    # Rough cross rates from USD base
    CROSS = {"GBP": 1.27, "EUR": 1.09, "GHS": 0.067}
    if currency != "USD":
        rate = rate * CROSS.get(currency, 1.0)
    return round(amount * rate, 2), round(rate, 2)


# ════════════════════════════════════════════════════════════════════════════════
# CLAUDE: PARSE TRANSACTION (receipt-first — handles all alert types)
# ════════════════════════════════════════════════════════════════════════════════

def parse_transaction(text: str = None, image_b64: str = None, image_mime: str = None,
                      fx_hint: float = None) -> dict:
    today     = datetime.now().strftime("%Y-%m-%d")
    cur_month = datetime.now().strftime("%B %Y")
    has_image = bool(image_b64)

    system = f"""You are a financial transaction parser for a Nigerian user.
Today: {today}. Current budget month: {cur_month}. Primary currency: NGN.

Return ONLY valid JSON — no markdown, no explanation.

{{
  "type": "expense"|"income"|"savings"|"investment"|"debt_owed_to_me"|"debt_i_owe"|"unknown",
  "amount_ngn": <number — NGN equivalent>,
  "amount_original": <number or null — original foreign amount if not NGN>,
  "currency": "NGN"|"USD"|"GBP"|"EUR"|"GHS",
  "category": <string>,
  "description": <string max 60 chars>,
  "merchant": <string or null — business name, bank, or recipient name>,
  "beneficiary": <string or null — person/entity receiving money>,
  "fx_rate": <number or null>,
  "date": <YYYY-MM-DD>,
  "budget_month": <"Month YYYY">,
  "date_source": "receipt"|"user"|"today",
  "date_confidence": "certain"|"uncertain",
  "confidence": "high"|"medium"|"low"
}}

{'IMAGE IS PRIMARY SOURCE:' if has_image else 'TEXT ONLY MODE:'}

{'IGNORE available balance / account balance shown on bank alerts — extract only: amount debited/credited, date, beneficiary/sender, narration.' if has_image else ''}

{'''
ALERT TYPE HANDLING:
  SMS/text alert:  "Acct ***1234 debited NGN 5,000.00 on 03-Feb-2025. Ref:..."
    → extract debit amount, date, narration/ref. Ignore "Avail Bal".
  Email screenshot: similar format — ignore balance lines, extract transaction amount only.
  POS receipt: extract total amount paid, merchant name, date.
  Bank transfer receipt: extract amount, beneficiary name, date, narration.

TRANSFER DIRECTION (critical):
  Debit / DR / "debited" / "You sent" / "Transfer out" → money left your account
    → expense (if personal spend), debt_i_owe (if lending), savings/investment (if moving to your own account)
  Credit / CR / "credited" / "You received" / "inflow" → money came in
    → income (salary/business/gift IN), debt_owed_to_me (if someone repaying you)

  Narration words on a DEBIT:
    "gift","upkeep","feeding","allowance","school fees","pocket money" → expense/Other Expense
    "rent","house rent" → expense/Rent/Housing
    "fuel","transport" → expense/Transport
    "savings","save","emergency fund" → savings
    "investment","stocks","shares","NSE","mutual fund","treasury" → investment

  Narration words that indicate the PERSON OWES YOU (money you lent):
    "loan to [name]","lent [name]","[name] loan" → debt_owed_to_me
  Narration that you owe someone:
    "loan from [name]","borrowed from" → debt_i_owe

NARRATION/MEMO FIELD:
  If present on receipt, use it for description and infer category + budget_month.
  e.g. "Narration: March rent" → category=Rent/Housing, budget_month=March 2025
  e.g. "Ref: school fees april" → category=Education, budget_month=April 2025
  e.g. "Memo: NSE stocks" → type=investment, category=Stocks/NSE

BENEFICIARY / MERCHANT:
  For transfers: extract recipient name from "To: [Name]" or "Beneficiary: [Name]"
  For POS/card: extract merchant/shop name
  Store in both merchant and beneficiary fields where applicable.
''' if has_image else ''}

BUDGET MONTH PRIORITY:
1. Narration/memo on receipt mentions a month → use that month
2. Text hint contains a month name ("march","for april") → use that month
3. Otherwise → derive from transaction date

DATE PRIORITY:
1. Receipt/image: scan for transaction date, value date, posting date
   Nigerian formats: "03-Feb-2025","03/02/2025","2025-02-03","3rd Feb 2025"
   → date_source="receipt"
2. Text: "yesterday","jan 3","last monday","3rd feb" → date_source="user"
3. Fallback: {today} → date_source="today"

FOREIGN CURRENCY (text entry):
  If text says "usd [amount]", "USD [amount] stocks", "$[amount]":
    → currency=USD, amount_original=[amount], amount_ngn=[amount × {fx_hint or 'CBN rate ~1570'}]
  If user provides explicit rate "at [rate]": use that rate, set fx_rate=[rate]
  Otherwise use fallback rate {fx_hint or 1570.0}

Categories:
income:     Salary, Freelance, Business, Investment Return, Rental, Gift, Other Income
expense:    Food & Dining, Transport, Utilities, Rent/Housing, Healthcare, Education,
            Entertainment, Clothing, FX/Currency, Bank Charges, Other Expense
savings:    Emergency Fund, Target Savings, Fixed Deposit, Dollar Savings, Other Savings
investment: Stocks/NSE, Crypto, Real Estate, Treasury Bills, Mutual Fund, Other Investment
debt_owed_to_me: money someone owes you (you lent it out)
debt_i_owe:      money you owe (you borrowed it)

Text-only shortcuts (no image):
  water bill 4500      → expense/Utilities / N4,500
  salary 350k          → income/Salary / N350,000  (k=×1000, m=×1,000,000)
  savings 50k          → savings/Target Savings
  USD 200 at 1580      → expense/FX, amount_ngn=316000
  usd 500 stocks       → investment/Stocks, amount_ngn=500×CBN rate
  Emeka owes me 50000  → debt_owed_to_me / Lent
  borrowed 100k Abebe  → debt_i_owe / Borrowed

Unknown if type/amount cannot be determined.
"""

    content = []
    if image_b64 and image_mime:
        content.append({"type":"image","source":{"type":"base64",
                         "media_type":image_mime,"data":image_b64}})
    if text:
        content.append({"type":"text","text":text})
    if not content:
        return {"type":"unknown"}

    resp = claude.messages.create(
        model="claude-opus-4-6", max_tokens=700, system=system,
        messages=[{"role":"user","content":content}])
    raw = re.sub(r"^```json|^```|```$","",resp.content[0].text.strip(),
                 flags=re.MULTILINE).strip()
    tx = json.loads(raw)

    # Normalise debt types → storage type
    if tx.get("type") == "debt_owed_to_me":
        tx["type"]     = "investment"   # money out, expected back → treated as investment asset
        tx["category"] = tx.get("category") or "Lent Out"
        tx["is_receivable"] = True
    elif tx.get("type") == "debt_i_owe":
        tx["type"]     = "debt"
        tx["category"] = tx.get("category") or "Borrowed"
        tx["is_receivable"] = False

    # Handle foreign currency conversion for text entries without fx_rate
    if tx.get("currency","NGN") != "NGN" and not tx.get("fx_rate"):
        orig = tx.get("amount_original") or tx.get("amount_ngn",0)
        ngn, rate = convert_to_ngn(orig, tx["currency"], None)
        tx["amount_ngn"]      = ngn
        tx["amount_original"] = orig
        tx["fx_rate"]         = rate

    return tx


# ════════════════════════════════════════════════════════════════════════════════
# PARSE MULTI-ITEM SPLIT
# ════════════════════════════════════════════════════════════════════════════════

def parse_split(text: str, parent_amount: float) -> list:
    system = """Parse a receipt split instruction into individual line items.
Return ONLY a JSON array — no markdown:
[{"category": <string>, "description": <string>, "amount_ngn": <number>}, ...]

Categories: Food & Dining, Cleaning/Household, Personal Care,
Clothing, Healthcare, Entertainment, Transport, Other Expense.
k=×1000. Strip commas from numbers."""

    resp  = claude.messages.create(
        model="claude-opus-4-6", max_tokens=400, system=system,
        messages=[{"role":"user","content":text}])
    raw   = re.sub(r"^```json|^```|```$","",resp.content[0].text.strip(),
                   flags=re.MULTILINE).strip()
    items = json.loads(raw)

    total = sum(i.get("amount_ngn",0) for i in items)
    if parent_amount and total > 0 and abs(total - parent_amount) > parent_amount * 0.05:
        factor = parent_amount / total
        for i in items:
            i["amount_ngn"] = round(i["amount_ngn"] * factor)
    return items


# ════════════════════════════════════════════════════════════════════════════════
# SAVE TRANSACTION(S)
# ════════════════════════════════════════════════════════════════════════════════

def save_transaction(tx: dict, wb, split_parent_id: str = ""):
    ws,_,_,_,_ = tabs(wb)
    dt           = datetime.strptime(tx["date"], "%Y-%m-%d")
    budget_month = tx.get("budget_month") or dt.strftime("%B %Y")
    ws.append_row([
        tx["date"], dt.strftime("%B %Y"), dt.year,
        tx["type"].title(), tx.get("category",""),
        tx.get("description",""), tx.get("merchant") or tx.get("beneficiary") or "",
        round(float(tx.get("amount_ngn",0)), 2),
        round(float(tx.get("amount_original") or tx.get("amount_ngn",0)), 2),
        tx.get("currency","NGN"), tx.get("fx_rate") or "",
        tx.get("confidence",""), budget_month, split_parent_id
    ])
    refresh_summary(wb, budget_month)
    check_goal_progress_on_save(tx, wb)


def save_split_transactions(base_tx: dict, items: list, wb) -> str:
    parent_id = f"split_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    lines     = []
    for item in items:
        row = {**base_tx,
               "type":"expense","category":item["category"],
               "description":item["description"],
               "amount_ngn":item["amount_ngn"],"amount_original":item["amount_ngn"],
               "confidence":"high"}
        save_transaction(row, wb, split_parent_id=parent_id)
        lines.append(f"  {item['category']}: {fmt_ngn(item['amount_ngn'])}")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════════
# MONTHLY SUMMARY TAB
# ════════════════════════════════════════════════════════════════════════════════

def refresh_summary(wb, month_label: str):
    tx_ws,_,_,sum_ws,_ = tabs(wb)
    rows   = tx_ws.get_all_records()
    totals = {t: 0.0 for t in TYPES}
    for r in rows:
        bm  = r.get("Budget Month") or r.get("Month","")
        if bm != month_label: continue
        t   = r.get("Type","").lower()
        amt = float(r.get("Amount (N)",0) or 0)
        if t in totals:
            totals[t] += amt

    net     = totals["income"] - totals["expense"] - totals["savings"] - totals["investment"]
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        year = datetime.strptime(month_label, "%B %Y").year
    except ValueError:
        year = datetime.now().year

    new_row = [month_label, year,
               totals["income"], totals["expense"], totals["savings"],
               totals["investment"], totals["debt"], net, now_str]

    existing = sum_ws.get_all_records()
    for i, r in enumerate(existing, start=2):
        if r.get("Month") == month_label:
            sum_ws.update(f"A{i}:I{i}", [new_row])
            return
    sum_ws.append_row(new_row)


# ════════════════════════════════════════════════════════════════════════════════
# SAVINGS GOALS  (#10)
# ════════════════════════════════════════════════════════════════════════════════

def set_goal(wb, name: str, goal_type: str, target: float, deadline: str) -> str:
    """Create or update a savings/investment goal."""
    _,_,_,_,ws = tabs(wb)
    now_str     = datetime.now().strftime("%Y-%m-%d %H:%M")
    rows        = ws.get_all_records()
    for i, r in enumerate(rows, start=2):
        if r.get("Name","").lower() == name.lower():
            ws.update(f"A{i}:G{i}", [[name, goal_type, target, deadline,
                                       r.get("Current (N)",0), "active", now_str]])
            return "updated"
    ws.append_row([name, goal_type, target, deadline, 0, "active", now_str])
    return "created"

def get_goals(wb) -> list:
    _,_,_,_,ws = tabs(wb)
    return [r for r in ws.get_all_records() if r.get("Status","") == "active"]

def update_goal_current(wb, goal_name: str, amount_added: float):
    """Add amount_added to a goal's current balance."""
    _,_,_,_,ws = tabs(wb)
    rows        = ws.get_all_records()
    for i, r in enumerate(rows, start=2):
        if r.get("Name","").lower() == goal_name.lower():
            old = float(r.get("Current (N)",0) or 0)
            new = old + amount_added
            ws.update(f"E{i}", [[new]])
            target = float(r.get("Target (N)",0) or 0)
            if target and new >= target:
                ws.update(f"F{i}", [["completed"]])
                return "completed", new, target
            return "updated", new, target
    return None, 0, 0

def check_goal_progress_on_save(tx: dict, wb):
    """After every savings/investment transaction, nudge relevant goals."""
    if tx.get("type") not in ("savings","investment"):
        return
    goals = get_goals(wb)
    if not goals: return
    amt   = float(tx.get("amount_ngn",0))
    # Match by keyword in goal name vs transaction description/category
    desc  = (tx.get("description","") + " " + tx.get("category","")).lower()
    for g in goals:
        gname = g.get("Name","").lower()
        if any(word in desc for word in gname.split()):
            update_goal_current(wb, g["Name"], amt)
            break

def goals_report(wb) -> str:
    goals = get_goals(wb)
    if not goals:
        return ("No active goals.\n\n"
                "Set one: goal house deposit 5000000 by 2026-12-31")
    usd_rate = get_cbn_usd_rate()
    lines    = ["SAVINGS GOALS", "="*28]
    for g in goals:
        name    = g.get("Name","")
        target  = float(g.get("Target (N)",0) or 0)
        current = float(g.get("Current (N)",0) or 0)
        dl      = g.get("Deadline","")
        pct     = (current/target*100) if target else 0
        remain  = target - current
        bar_fill= round(min(pct/100,1.0)*10)
        bar     = f"[{'#'*bar_fill}{'.'*(10-bar_fill)}] {round(pct)}%"

        # Months to deadline
        months_left = ""
        if dl:
            try:
                days = (datetime.strptime(dl[:10],"%Y-%m-%d").date() - datetime.now().date()).days
                months_left = f" ({max(0,days//30)} mths left)"
            except ValueError:
                pass

        monthly_needed = ""
        if remain > 0 and dl:
            try:
                days = (datetime.strptime(dl[:10],"%Y-%m-%d").date() - datetime.now().date()).days
                months = max(1, days // 30)
                monthly_needed = f"\n  Save {fmt_ngn(remain/months)}/month to hit target"
            except ValueError:
                pass

        lines += [
            f"\n{name} ({g.get('Type','')}){months_left}",
            f"  Target:  {fmt_ngn(target)}  (${target/usd_rate:,.0f})",
            f"  Saved:   {fmt_ngn(current)}",
            f"  Remain:  {fmt_ngn(remain)}",
            f"  {bar}",
            f"  Deadline: {dl}" if dl else "",
        ]
        if monthly_needed: lines.append(monthly_needed)

    return "\n".join(l for l in lines if l is not None)

def cmd_goal(text: str, wb) -> str:
    """
    goal <name> <target> by <deadline>
    goal house deposit 5000000 by 2026-12-31
    goal nse portfolio 2000000 by 2025-12-31 investment
    goals                          → view all
    """
    lower = text.strip().lower()
    if lower in ("goals","goal"):
        return goals_report(wb)

    # Parse: goal <name...> <amount> by <date> [type]
    m = re.search(r"(\d[\d,kmKM]*)\s+by\s+(\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{4}|\w+ \d{4})",
                  text, re.IGNORECASE)
    if not m:
        return ("Format: goal <name> <amount> by <deadline>\n"
                "  e.g. goal house deposit 5000000 by 2026-12-31\n"
                "  e.g. goal dollar savings 3000000 by 2025-12-01")

    before = text[:m.start()].strip()
    name   = re.sub(r"^goal\s+","",before, flags=re.IGNORECASE).strip()
    if not name: name = "Goal"
    target_str = expand_amount(m.group(1))
    try:
        target = float(target_str)
    except ValueError:
        return f"Invalid amount: {m.group(1)}"

    # Parse deadline
    dl_raw = m.group(2)
    try:
        dl = datetime.strptime(dl_raw[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        try:
            dl = parse_month(dl_raw)[0]  # "december 2026" → "December 2026"
            dl = datetime.strptime("01 "+dl, "%d %B %Y").strftime("%Y-%m-%d")
        except Exception:
            dl = dl_raw

    # Goal type — default savings
    goal_type = "savings"
    after = text[m.end():].strip().lower()
    if "invest" in after or "invest" in name.lower():
        goal_type = "investment"

    action = set_goal(wb, name, goal_type, target, dl)
    usd    = get_cbn_usd_rate()
    return (f"Goal {action}!\n"
            f"  {name}\n"
            f"  Target: {fmt_ngn(target)}  (${target/usd:,.0f})\n"
            f"  By:     {dl}\n\n"
            f"_Type 'goals' to see all progress_")


# ════════════════════════════════════════════════════════════════════════════════
# DEBT SETTLEMENT  (#10 logic continuation)
# ════════════════════════════════════════════════════════════════════════════════

def settle_debt(text: str, wb) -> str:
    """
    settle Emeka 30000         → Emeka owes you, payment reduces investment/Lent Out
                                  + logs income/Debt Repayment
    settle car loan 50000      → you owe car loan, payment reduces debt/Borrowed
                                  + logs expense/Loan Repayment
    Full settle: settle Emeka  → settles full remaining balance
    """
    parts = text.strip().split()
    # Parse: settle <description> [amount]
    # Description can be multi-word
    amount   = None
    desc_parts = []
    for p in parts[1:]:
        cleaned = expand_amount(p)
        try:
            amount = float(cleaned)
        except ValueError:
            desc_parts.append(p)
    desc = " ".join(desc_parts).strip()
    if not desc:
        return "Format: settle <name/description> [amount]\n  settle Emeka 30000\n  settle car loan 50000"

    # Find matching legacy entry
    _,_,leg_ws,_,_ = tabs(wb)
    rows = leg_ws.get_all_records()
    match_row  = None
    match_idx  = None
    for i, r in enumerate(rows, start=2):
        if desc.lower() in r.get("Description","").lower():
            match_row = r
            match_idx = i
            break

    if not match_row:
        return (f"No debt found matching '{desc}'.\n"
                f"Type 'legacy' to see your debt list.")

    current_amt = float(match_row.get("Amount (N)",0) or 0)
    settle_amt  = amount if amount else current_amt   # full settle if no amount
    settle_amt  = min(settle_amt, current_amt)        # can't settle more than owed

    # Determine direction: owed_to_me (investment/Lent Out) or i_owe (debt/Borrowed)
    cat = match_row.get("Category","").lower()
    type_stored = match_row.get("Type","").lower()
    is_receivable = (type_stored == "investment" and
                     any(k in cat for k in ["lent","receivable","owed to me"]))

    new_balance = current_amt - settle_amt
    now_str     = datetime.now().strftime("%Y-%m-%d %H:%M")
    today_str   = datetime.now().strftime("%Y-%m-%d")

    # Update legacy row
    if new_balance <= 0:
        leg_ws.update(f"D{match_idx}:F{match_idx}",
                      [[0, today_str, now_str]])
    else:
        leg_ws.update(f"D{match_idx}:F{match_idx}",
                      [[new_balance, match_row.get("As Of",""), now_str]])

    # Log the settlement transaction
    if is_receivable:
        # They paid you back → income
        settle_tx = {
            "type": "income", "category": "Debt Repayment",
            "description": f"Repayment: {match_row['Description']}"[:60],
            "merchant": match_row.get("Description",""),
            "amount_ngn": settle_amt, "amount_original": settle_amt,
            "currency": "NGN", "fx_rate": None,
            "date": today_str,
            "budget_month": current_month(),
            "date_source": "today", "date_confidence": "certain",
            "confidence": "high"
        }
        direction_msg = "income logged (they paid you)"
    else:
        # You paid someone → expense
        settle_tx = {
            "type": "expense", "category": "Loan Repayment",
            "description": f"Repayment: {match_row['Description']}"[:60],
            "merchant": match_row.get("Description",""),
            "amount_ngn": settle_amt, "amount_original": settle_amt,
            "currency": "NGN", "fx_rate": None,
            "date": today_str,
            "budget_month": current_month(),
            "date_source": "today", "date_confidence": "certain",
            "confidence": "high"
        }
        direction_msg = "expense logged (you paid)"

    save_transaction(settle_tx, wb)

    status = "FULLY SETTLED" if new_balance <= 0 else f"Remaining: {fmt_ngn(new_balance)}"
    return (f"Settlement recorded!\n"
            f"  {match_row['Description']}\n"
            f"  Paid:   {fmt_ngn(settle_amt)}\n"
            f"  Status: {status}\n"
            f"  {direction_msg}\n\n"
            f"_Type 'legacy' to see updated balances_")


# ════════════════════════════════════════════════════════════════════════════════
# BUDGET HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def set_budget(wb, month_label, year, tx_type, amount):
    _, ws,_,_,_ = tabs(wb)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    for i, r in enumerate(ws.get_all_records(), start=2):
        if r["Month"] == month_label and r["Type"].lower() == tx_type:
            ws.update(f"D{i}:E{i}", [[amount, now_str]])
            return "updated"
    ws.append_row([month_label, year, tx_type.title(), amount, now_str])
    return "created"

def get_budgets(wb, month_label):
    _, ws,_,_,_ = tabs(wb)
    return {r["Type"].lower(): float(r["Budgeted (N)"] or 0)
            for r in ws.get_all_records() if r["Month"] == month_label}


# ════════════════════════════════════════════════════════════════════════════════
# LEGACY HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def set_legacy(wb, tx_type, amount, description, as_of, category=""):
    _,_,ws,_,_ = tabs(wb)
    now_str    = datetime.now().strftime("%Y-%m-%d %H:%M")
    for i, r in enumerate(ws.get_all_records(), start=2):
        if r["Type"].lower() == tx_type and r["Description"].lower() == description.lower():
            ws.update(f"D{i}:F{i}", [[amount, as_of, now_str]])
            return "updated"
    ws.append_row([tx_type.title(), category, description, amount, as_of, now_str])
    return "created"

def get_legacy(wb):
    _,_,ws,_,_ = tabs(wb)
    result     = {}
    for r in ws.get_all_records():
        t = r["Type"].lower()
        result.setdefault(t,{})[r["Description"] or r["Type"]] = float(r["Amount (N)"] or 0)
    return result

def get_legacy_with_dates(wb):
    _,_,ws,_,_ = tabs(wb)
    rows = []
    for r in ws.get_all_records():
        rows.append({
            "type":        r.get("Type","").lower(),
            "description": r.get("Description",""),
            "amount":      float(r.get("Amount (N)",0) or 0),
            "as_of":       r.get("As Of",""),
            "category":    r.get("Category",""),
        })
    return rows


# ════════════════════════════════════════════════════════════════════════════════
# AGGREGATE
# ════════════════════════════════════════════════════════════════════════════════

def aggregate(rows, month_filter=None):
    by_cat  = {}
    by_type = {}
    for r in rows:
        bm  = r.get("Budget Month") or r.get("Month","")
        if month_filter and bm != month_filter:
            continue
        t   = r.get("Type","").lower()
        cat = r.get("Category","Other")
        amt = float(r.get("Amount (N)",0) or 0)
        by_cat.setdefault(t,{})[cat] = by_cat.get(t,{}).get(cat,0) + amt
        by_type[t] = by_type.get(t,0) + amt
    return by_cat, by_type


# ════════════════════════════════════════════════════════════════════════════════
# BUDGET ALERT
# ════════════════════════════════════════════════════════════════════════════════

def check_budget_alert(wb, tx: dict):
    tx_type = tx.get("type","").lower()
    if tx_type not in ("expense","savings","investment"):
        return None
    month_label = tx.get("budget_month") or \
                  datetime.strptime(tx["date"],"%Y-%m-%d").strftime("%B %Y")
    budgets = get_budgets(wb, month_label)
    budget  = budgets.get(tx_type, 0)
    if not budget: return None

    tx_ws,_,_,_,_ = tabs(wb)
    rows = tx_ws.get_all_records()
    month_rows = [r for r in rows
                  if (r.get("Budget Month") or r.get("Month","")) == month_label
                  and r.get("Type","").lower() == tx_type]
    total = sum(float(r.get("Amount (N)",0) or 0) for r in month_rows)
    pct   = (total / budget) * 100

    e = {"expense":"OUT","savings":"SAV","investment":"INV"}.get(tx_type,"$")
    if total > budget:
        over = total - budget
        return (f"BUDGET ALERT [{e}] {tx_type.title()} OVER BUDGET\n"
                f"  Budget: {fmt_ngn(budget)} | Spent: {fmt_ngn(total)}\n"
                f"  Over by: {fmt_ngn(over)} ({round(pct)}%)\n"
                f"_Type 'report' for full breakdown_")
    elif pct >= 80:
        return (f"BUDGET WARNING [{e}] {tx_type.title()} at {round(pct)}%\n"
                f"  Budget: {fmt_ngn(budget)} | Spent: {fmt_ngn(total)}\n"
                f"  Remaining: {fmt_ngn(budget-total)}\n"
                f"_Type 'report' for breakdown_")
    return None


# ════════════════════════════════════════════════════════════════════════════════
# QUICK QUERIES
# ════════════════════════════════════════════════════════════════════════════════

def handle_query(text: str, wb) -> str:
    lower    = text.lower()
    now      = datetime.now()
    tx_ws,_,_,_,_ = tabs(wb)
    all_rows = tx_ws.get_all_records()

    # Last N transactions
    m = re.search(r"last\s+(\d+)", lower)
    if m:
        n    = int(m.group(1))
        rows = [r for r in all_rows if r.get("Type","").lower() in TYPES][-n:]
        if not rows:
            return "No transactions found."
        lines = [f"Last {n} transactions:"]
        for r in reversed(rows):
            lines.append(f"  {r.get('Date','--')} [{r.get('Type','--')}] "
                         f"{r.get('Description','--')} — "
                         f"{fmt_ngn(float(r.get('Amount (N)',0) or 0))}")
        return "\n".join(lines)

    # Today
    if "today" in lower and "report" not in lower:
        today_str = now.strftime("%Y-%m-%d")
        rows  = [r for r in all_rows
                 if r.get("Date") == today_str and r.get("Type","").lower() == "expense"]
        total = sum(float(r.get("Amount (N)",0) or 0) for r in rows)
        return f"Today's expenses: {fmt_ngn(total)} ({len(rows)} entries)"

    # This week
    if "this week" in lower or ("week" in lower and "last" not in lower):
        week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
        rows  = [r for r in all_rows
                 if r.get("Date","") >= week_start
                 and r.get("Type","").lower() == "expense"]
        total = sum(float(r.get("Amount (N)",0) or 0) for r in rows)
        return f"Expenses this week: {fmt_ngn(total)} ({len(rows)} entries)"

    # Category map
    CAT_MAP = {
        "food":"Food","groceries":"Food","grocery":"Food","eating":"Food",
        "transport":"Transport","uber":"Transport","bolt":"Transport",
        "fuel":"Transport","petrol":"Transport","diesel":"Transport",
        "utilities":"Utilities","electricity":"Utilities","nepa":"Utilities",
        "water":"Utilities","internet":"Utilities","data":"Utilities",
        "rent":"Rent","housing":"Rent",
        "health":"Health","medical":"Health","hospital":"Health","pharmacy":"Health",
        "school":"Education","education":"Education","fees":"Education","tuition":"Education",
        "entertainment":"Entertainment","movies":"Entertainment","cinema":"Entertainment",
        "clothing":"Clothing","clothes":"Clothing","shoes":"Clothing",
        "bank":"Bank Charges","charges":"Bank Charges",
        "savings":"savings","investment":"investment","income":"income",
    }
    matched_cat  = None
    matched_type = None
    for keyword, cat in CAT_MAP.items():
        if keyword in lower:
            if cat in TYPES:
                matched_type = cat
            else:
                matched_cat  = cat
            break

    # Time scope
    month_label = current_month()
    scope_label = "this month"
    if "last month" in lower:
        prev        = now.replace(day=1) - timedelta(days=1)
        month_label = prev.strftime("%B %Y")
        scope_label = "last month"
    else:
        for mn, mi in MONTH_NAMES.items():
            if mn[:3] in lower and mn[:3] not in ("tod","wee"):
                month_label = f"{month_name[mi]} {now.year}"
                scope_label = month_label
                break

    filtered = [r for r in all_rows
                if (r.get("Budget Month") or r.get("Month","")) == month_label]

    if matched_type:
        rows  = [r for r in filtered if r.get("Type","").lower() == matched_type]
        total = sum(float(r.get("Amount (N)",0) or 0) for r in rows)
        return f"{matched_type.title()} {scope_label}: {fmt_ngn(total)} ({len(rows)} entries)"

    if matched_cat:
        rows  = [r for r in filtered
                 if matched_cat.lower() in r.get("Category","").lower()
                 and r.get("Type","").lower() == "expense"]
        total = sum(float(r.get("Amount (N)",0) or 0) for r in rows)
        return (f"{matched_cat} {scope_label}:\n"
                f"  Total: {fmt_ngn(total)} ({len(rows)} entries)")

    # Fallback
    total = sum(float(r.get("Amount (N)",0) or 0)
                for r in filtered if r.get("Type","").lower() == "expense")
    return f"Expenses {scope_label}: {fmt_ngn(total)}"


# ════════════════════════════════════════════════════════════════════════════════
# DEBT AGEING
# ════════════════════════════════════════════════════════════════════════════════

def check_debt_ageing(wb) -> str:
    rows   = get_legacy_with_dates(wb)
    today  = datetime.now().date()
    alerts = []
    for r in rows:
        if r["type"] not in ("debt","investment"): continue
        if not r["as_of"]: continue
        cat = r.get("category","").lower()
        is_receivable = (r["type"] == "investment" and
                         any(k in cat for k in ["lent","receivable","owed to me"]))
        is_payable    = (r["type"] == "debt")
        if not (is_receivable or is_payable): continue
        try:
            as_of    = datetime.strptime(r["as_of"][:10], "%Y-%m-%d").date()
            age_days = (today - as_of).days
        except ValueError:
            continue
        desc      = r["description"]
        amt       = fmt_ngn(r["amount"])
        direction = "owed to you" if is_receivable else "you owe"
        if age_days >= 90:
            alerts.append(f"OVERDUE ({age_days}d): {desc} — {amt} ({direction})")
        elif age_days >= 30:
            alerts.append(f"Ageing ({age_days}d): {desc} — {amt} ({direction})")
    if not alerts:
        return "No ageing debts."
    return "Debt Ageing Report:\n" + "\n".join(f"  {a}" for a in alerts)

def proactive_debt_alerts(wb) -> list:
    rows   = get_legacy_with_dates(wb)
    today  = datetime.now().date()
    alerts = []
    for r in rows:
        if not r["as_of"]: continue
        cat = r.get("category","").lower()
        is_receivable = (r["type"]=="investment" and
                         any(k in cat for k in ["lent","receivable","owed to me"]))
        is_payable    = (r["type"]=="debt")
        if not (is_receivable or is_payable): continue
        try:
            as_of    = datetime.strptime(r["as_of"][:10],"%Y-%m-%d").date()
            age_days = (today - as_of).days
        except ValueError:
            continue
        if age_days >= 30:
            tag = "OVERDUE" if age_days >= 90 else "REMINDER"
            alerts.append(f"[DEBT {tag}] {r['description']}: {fmt_ngn(r['amount'])} "
                          f"— {age_days}d old")
    return alerts


# ════════════════════════════════════════════════════════════════════════════════
# FULL REPORT
# ════════════════════════════════════════════════════════════════════════════════

def generate_report(wb, month_label=None):
    month_label      = month_label or current_month()
    tx_ws,_,_,sum_ws,_ = tabs(wb)

    summaries = sum_ws.get_all_records()
    summary   = next((r for r in summaries if r.get("Month") == month_label), None)

    all_rows  = tx_ws.get_all_records()
    by_cat, by_type = aggregate(all_rows, month_filter=month_label)

    totals  = {t: float(summary.get(t.title(), 0) or 0) for t in TYPES} \
              if summary else by_type

    budgets    = get_budgets(wb, month_label)
    legacy     = get_legacy(wb)
    leg_totals = {t: sum(v.values()) for t, v in legacy.items()}

    req_dt = datetime.strptime(month_label, "%B %Y")
    ytd    = {t: 0.0 for t in TYPES}
    for r in summaries:
        try:
            if datetime.strptime(r.get("Month","Jan 1900"),"%B %Y") <= req_dt:
                for t in TYPES:
                    ytd[t] += float(r.get(t.title(),0) or 0)
        except ValueError:
            pass

    tx_count = sum(
        1 for r in all_rows
        if (r.get("Budget Month") or r.get("Month","")) == month_label
    )

    def bar(actual, budget, width=10):
        if not budget: return ""
        pct  = actual / budget
        fill = round(min(pct,1.0)*width)
        icon = "OK" if pct <= 0.80 else ("!!" if pct <= 1.0 else "OVER")
        return f"[{'#'*fill}{'.'*(width-fill)}] {icon} {round(pct*100)}%"

    def section(tx_type, label, emoji):
        actual  = totals.get(tx_type, 0)
        budget  = budgets.get(tx_type, 0)
        leg     = leg_totals.get(tx_type, 0)
        ytd_tot = leg + ytd.get(tx_type, 0)
        lines   = [f"\n{emoji} {label}"]
        for cat, amt in sorted(by_cat.get(tx_type,{}).items(), key=lambda x:-x[1]):
            lines.append(f"  {cat}: {fmt_ngn(amt)}")
        lines.append(f"  {'─'*20}")
        lines.append(f"  Actual: {fmt_ngn(actual)}")
        if budget:
            var    = actual - budget
            is_bad = (var>0 and tx_type!="income") or (var<0 and tx_type=="income")
            lines.append(f"  Budget: {fmt_ngn(budget)}")
            lines.append(f"  Var:    {'+' if var>=0 else ''}{fmt_ngn(var)} "
                         f"[{'OVER' if is_bad else 'OK'}]")
            lines.append(f"  {bar(actual,budget)}")
        if leg:    lines.append(f"  BalB/F: {fmt_ngn(leg)}")
        if ytd_tot:lines.append(f"  YTD:    {fmt_ngn(ytd_tot)}")
        return "\n".join(lines)

    net     = totals.get("income",0) - totals.get("expense",0) \
            - totals.get("savings",0) - totals.get("investment",0)
    bud_net = (budgets.get("income",0) - budgets.get("expense",0)
             - budgets.get("savings",0) - budgets.get("investment",0))
    ytd_net = ((leg_totals.get("income",0)+ytd.get("income",0))
             - (leg_totals.get("expense",0)+ytd.get("expense",0))
             - (leg_totals.get("savings",0)+ytd.get("savings",0))
             - (leg_totals.get("investment",0)+ytd.get("investment",0)))

    flag    = "OK" if net >= 0 else "!!"
    bud_ln  = f"  Budget: {fmt_ngn(bud_net)}\n" if bud_net else ""

    return (
        f"Report — {month_label}\n{'='*28}"
        f"{section('income',     'INCOME',     '[IN]')}"
        f"{section('expense',    'EXPENSES',   '[OUT]')}"
        f"{section('savings',    'SAVINGS',    '[SAV]')}"
        f"{section('investment', 'INVESTMENTS','[INV]')}"
        f"{section('debt',       'DEBT/LOANS', '[DEBT]')}\n"
        f"\n{'='*28}\n"
        f"NET [{flag}]\n"
        f"  Month: {fmt_ngn(net)}\n"
        f"{bud_ln}"
        f"  YTD:   {fmt_ngn(ytd_net)}\n"
        f"  Entries: {tx_count}\n\n"
        f"_networth | goals | debts | budget | legacy_"
    )


# ════════════════════════════════════════════════════════════════════════════════
# NET WORTH
# ════════════════════════════════════════════════════════════════════════════════

def generate_networth(wb) -> str:
    legacy         = get_legacy(wb)
    leg_with_dates = get_legacy_with_dates(wb)
    _,_,_,sum_ws,_ = tabs(wb)
    summaries      = sum_ws.get_all_records()

    ytd = {t: 0.0 for t in TYPES}
    for r in summaries:
        for t in TYPES:
            ytd[t] += float(r.get(t.title(),0) or 0)

    leg_totals = {t: sum(v.values()) for t, v in legacy.items()}

    savings_total = leg_totals.get("savings",0)    + ytd.get("savings",0)
    invest_total  = leg_totals.get("investment",0)  + ytd.get("investment",0)
    debt_total    = leg_totals.get("debt",0)         + ytd.get("debt",0)
    total_assets  = savings_total + invest_total
    net_worth_ngn = total_assets - debt_total

    usd_rate = get_cbn_usd_rate()
    def usd(n): return f"${n/usd_rate:>10,.0f}" if usd_rate else ""

    debt_items = [r for r in leg_with_dates if r["type"] == "debt"]
    lent_items = [r for r in leg_with_dates
                  if r["type"]=="investment"
                  and any(k in r.get("category","").lower()
                          for k in ["lent","receivable","owed to me"])]

    lines = [
        f"NET WORTH SNAPSHOT",
        f"CBN Rate: $1 = {fmt_ngn(usd_rate)} (prev. business day)",
        f"{'='*30}",
        f"",
        f"ASSETS",
        f"  Savings:      {fmt_ngn(savings_total):>14}  {usd(savings_total)}",
        f"  Investments:  {fmt_ngn(invest_total):>14}  {usd(invest_total)}",
    ]
    if lent_items:
        lines.append(f"  (incl. lent out:)")
        for r in lent_items:
            lines.append(f"    {r['description']}: {fmt_ngn(r['amount'])}")
    lines += [
        f"  {'─'*20}",
        f"  Total Assets: {fmt_ngn(total_assets):>14}  {usd(total_assets)}",
        f"",
        f"LIABILITIES",
        f"  Total Debt:   {fmt_ngn(debt_total):>14}  {usd(debt_total)}",
    ]
    for d in debt_items:
        lines.append(f"    {d['description']}: {fmt_ngn(d['amount'])}")
    lines += [
        f"",
        f"{'='*30}",
        f"NET WORTH:    {fmt_ngn(net_worth_ngn):>14}",
        f"            = {usd(net_worth_ngn)}",
        f"",
        f"_CBN rate as of {datetime.now().strftime('%d %b %Y')}_",
    ]
    return "\n".join(l for l in lines if l is not None)


# ════════════════════════════════════════════════════════════════════════════════
# EMAIL BACKUP
# ════════════════════════════════════════════════════════════════════════════════

def send_backup_email(wb) -> bool:
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        print("Email not configured")
        return False

    tx_ws,_,_,_,_ = tabs(wb)
    all_rows       = tx_ws.get_all_values()

    buf      = io.StringIO()
    csv.writer(buf).writerows(all_rows)
    csv_data = buf.getvalue().encode("utf-8")

    month_str = datetime.now().strftime("%B %Y")
    filename  = f"expense_tracker_{datetime.now().strftime('%Y%m%d')}.csv"

    msg            = MIMEMultipart()
    msg["From"]    = EMAIL_ADDRESS
    msg["To"]      = BACKUP_EMAIL
    msg["Subject"] = f"Expense Tracker Backup — {month_str}"
    msg.attach(MIMEText(
        f"Weekly backup — {month_str}\n"
        f"Transactions: {len(all_rows)-1} rows\n"
        f"Generated: {datetime.now().strftime('%d %b %Y %H:%M')}\n",
        "plain"
    ))
    part = MIMEBase("application","octet-stream")
    part.set_payload(csv_data)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            s.sendmail(EMAIL_ADDRESS, BACKUP_EMAIL, msg.as_string())
        return True
    except Exception as e:
        print(f"Email failed: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ════════════════════════════════════════════════════════════════════════════════

def cmd_budget(text, wb):
    parts = text.strip().split()
    idx   = 1
    now   = datetime.now()
    month_label = current_month()
    year  = now.year

    if idx < len(parts) and parts[idx].lower() in MONTH_NAMES:
        mn = MONTH_NAMES[parts[idx].lower()]
        idx += 1
        if idx < len(parts) and parts[idx].isdigit() and len(parts[idx]) == 4:
            year = int(parts[idx]); idx += 1
        month_label = f"{month_name[mn]} {year}"

    if idx + 1 >= len(parts):
        return ("Format: budget [month] <type> <amount>\n"
                f"Types: {', '.join(TYPES)}\n"
                "e.g. budget march income 2000000")

    tx_type = parts[idx].lower()
    if tx_type not in TYPES:
        return f"Unknown type. Use: {', '.join(TYPES)}"

    amt_str = expand_amount(parts[idx+1])
    try:    amount = float(amt_str)
    except: return f"Invalid amount: {parts[idx+1]}"

    action = set_budget(wb, month_label, year, tx_type, amount)
    return (f"Budget {action}!\n  {tx_type.title()} → {fmt_ngn(amount)}\n"
            f"  Month: {month_label}")

def cmd_legacy(text, wb):
    parts = text.strip().split()
    if len(parts) < 3:
        return ("Format: legacy <type> <amount> [description]\n"
                "  legacy savings 500000\n"
                "  legacy investment 1200000 NSE portfolio\n"
                "  legacy debt 300000 car loan as of 2024-12-31")

    tx_type = parts[1].lower()
    # Map "lent" → investment so receivables go to assets
    if tx_type == "lent":
        tx_type  = "investment"
        category = "Lent Out"
    elif tx_type in TYPES:
        category = ""
    else:
        return f"Unknown type '{tx_type}'. Use: {', '.join(TYPES)} or lent"

    amt_str = expand_amount(parts[2])
    try:    amount = float(amt_str)
    except: return f"Invalid amount: {parts[2]}"

    desc    = " ".join(parts[3:]) if len(parts) > 3 else tx_type.title()
    as_of_m = re.search(r"as of (\d{4}-\d{2}-\d{2})", desc, re.IGNORECASE)
    as_of   = as_of_m.group(1) if as_of_m else datetime.now().strftime("%Y-%m-%d")
    if as_of_m: desc = desc[:as_of_m.start()].strip()
    if not desc: desc = tx_type.title()

    action = set_legacy(wb, tx_type, amount, desc, as_of, category)
    return (f"Legacy {action}!\n  {tx_type.title()} — {desc}\n"
            f"  {fmt_ngn(amount)}  (as of {as_of})")

def view_budgets(wb, month_label):
    budgets = get_budgets(wb, month_label)
    if not budgets:
        return f"No budgets for {month_label}. Try: budget march income 2000000"
    total_in  = budgets.get("income",0)
    total_out = sum(budgets.get(t,0) for t in ["expense","savings","investment"])
    lines = [f"Budget — {month_label}", "="*28]
    for t, amt in sorted(budgets.items()):
        lines.append(f"  {t.title():12} {fmt_ngn(amt)}")
    lines += ["="*28, f"  Planned Net: {fmt_ngn(total_in-total_out)}"]
    return "\n".join(lines)

def view_legacy(wb):
    legacy = get_legacy(wb)
    if not legacy:
        return ("No opening balances set.\n\n"
                "  legacy savings 500000\n"
                "  legacy investment 1200000 NSE portfolio\n"
                "  legacy debt 300000 car loan\n"
                "  legacy lent 50000 Emeka loan")
    lines = ["Opening Balances (B/F)", "="*28]
    net   = 0
    for t, items in legacy.items():
        lines.append(f"\n{t.title()}")
        for desc, amt in items.items():
            lines.append(f"  {desc}: {fmt_ngn(amt)}")
            net += amt if t != "debt" else -amt
    lines += ["","="*28, f"Net Legacy: {fmt_ngn(net)}"]
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════════
# WHATSAPP HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def download_wa_media(media_id: str) -> tuple:
    meta = requests.get(f"https://graph.facebook.com/v19.0/{media_id}",
                        headers={"Authorization":f"Bearer {WA_TOKEN}"}).json()
    mime = meta.get("mime_type","image/jpeg")
    img  = requests.get(meta["url"], headers={"Authorization":f"Bearer {WA_TOKEN}"})
    return img.content, mime

def send_message(to: str, text: str):
    # WhatsApp has 4096 char limit per message
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        requests.post(
            f"https://graph.facebook.com/v19.0/{WA_PHONE_ID}/messages",
            headers={"Authorization":f"Bearer {WA_TOKEN}","Content-Type":"application/json"},
            json={"messaging_product":"whatsapp","to":to,
                  "type":"text","text":{"body":chunk}})

def fmt_confirmation(tx: dict, had_image: bool = False) -> str:
    sym   = CURRENCY_SYMBOLS.get(tx.get("currency","NGN"),"N")
    amt   = float(tx.get("amount_ngn",0))
    orig  = tx.get("amount_original")
    cur   = tx.get("currency","NGN")
    a_str = fmt_ngn(amt)
    if cur != "NGN" and orig and float(orig) != amt:
        a_str += f" ({sym}{float(orig):,.0f} @ {tx.get('fx_rate','')})"

    e = {"income":"IN","expense":"OUT","savings":"SAV",
         "investment":"INV","debt":"DEBT"}.get(tx.get("type",""),"$")

    src_label = {"receipt":"from receipt","user":"you specified",
                 "today":"today (default)"}.get(tx.get("date_source","today"),"today")

    date_warn = ""
    if tx.get("date_source") == "today" and had_image:
        date_warn = "\n  No date found — used today. Fix: date 3-feb"
    elif tx.get("date_confidence") == "uncertain":
        date_warn = "\n  Date uncertain — fix: date 3-feb"

    merchant     = tx.get("merchant") or tx.get("beneficiary")
    merch_ln     = f"\n  Merchant:  {merchant}" if merchant else ""

    today_str    = datetime.now().strftime("%Y-%m-%d")
    cal_month    = datetime.strptime(tx.get("date", today_str), "%Y-%m-%d").strftime("%B %Y")
    budget_month = tx.get("budget_month") or cal_month
    bm_ln        = (f"\n  Budget:    {budget_month} [counted here]"
                    if budget_month != cal_month else "")

    return (f"[{e}] Logged!\n"
            f"  Amount:    {a_str}\n"
            f"  Category:  {tx.get('category','--')}\n"
            f"  Desc:      {tx.get('description','--')}"
            f"{merch_ln}\n"
            f"  Date:      {tx.get('date', today_str)} ({src_label})"
            f"{date_warn}{bm_ln}\n\n"
            f"_undo | bm [month] | date [date] | report_")


# ════════════════════════════════════════════════════════════════════════════════
# WEBHOOK
# ════════════════════════════════════════════════════════════════════════════════

@app.route("/webhook", methods=["GET"])
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge",""), 200
    return "Forbidden", 403

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    try:
        entry    = data["entry"][0]["changes"][0]["value"]
        messages = entry.get("messages",[])
        if not messages:
            return jsonify({"status":"ok"}), 200

        msg        = messages[0]
        user_phone = msg["from"]
        msg_type   = msg["type"]
        wb         = get_workbook()

        text       = None
        image_b64  = None
        image_mime = None

        # ── TEXT MESSAGE ──────────────────────────────────────────────────────
        if msg_type == "text":
            text  = msg["text"]["body"].strip()
            lower = text.lower()

            # HELP
            if lower == "help":
                send_message(user_phone, HELP_TEXT)
                return jsonify({"status":"ok"}), 200

            # REPORT / BALANCE
            if lower.startswith("report") or lower.startswith("balance"):
                parts       = text.split(" ", 1)
                month_label = parse_month(parts[1])[0] if len(parts) > 1 \
                              else current_month()
                send_message(user_phone, generate_report(wb, month_label))
                return jsonify({"status":"ok"}), 200

            # NET WORTH
            if lower in ("networth","net worth","nw","wealth"):
                send_message(user_phone, generate_networth(wb))
                return jsonify({"status":"ok"}), 200

            # GOALS
            if lower.startswith("goal"):
                send_message(user_phone, cmd_goal(text, wb))
                return jsonify({"status":"ok"}), 200

            # DEBT SETTLEMENT
            if lower.startswith("settle"):
                send_message(user_phone, settle_debt(text, wb))
                return jsonify({"status":"ok"}), 200

            # DEBTS
            if lower in ("debts","debt ageing","debt aging","overdue"):
                send_message(user_phone, check_debt_ageing(wb))
                return jsonify({"status":"ok"}), 200

            # BUDGET
            if lower.startswith("budget"):
                parts = lower.split()
                reply = view_budgets(wb, current_month()) if len(parts) == 1 \
                        else cmd_budget(text, wb)
                send_message(user_phone, reply)
                return jsonify({"status":"ok"}), 200

            # LEGACY
            if lower.startswith("legacy"):
                parts = lower.split()
                reply = view_legacy(wb) if len(parts) == 1 else cmd_legacy(text, wb)
                send_message(user_phone, reply)
                return jsonify({"status":"ok"}), 200

            # BACKUP
            if lower in ("backup","export","send backup"):
                ok = send_backup_email(wb)
                send_message(user_phone,
                    f"Backup sent to {BACKUP_EMAIL}" if ok
                    else "Backup failed — check EMAIL env vars.")
                return jsonify({"status":"ok"}), 200

            # UNDO
            if lower == "undo":
                tx_ws,_,_,_,_ = tabs(wb)
                last = tx_ws.row_count
                if last > 1:
                    tx_ws.delete_rows(last)
                    send_message(user_phone, "Last entry removed.")
                else:
                    send_message(user_phone, "Nothing to undo.")
                return jsonify({"status":"ok"}), 200

            # BUDGET MONTH CORRECTION
            if lower.startswith("bm "):
                bm_label,_ = parse_month(text[3:].strip())
                tx_ws,_,_,_,_ = tabs(wb)
                last = tx_ws.row_count
                if last > 1:
                    tx_ws.update(f"M{last}", [[bm_label]])
                    send_message(user_phone, f"Budget month → {bm_label}")
                else:
                    send_message(user_phone, "No entry to update.")
                return jsonify({"status":"ok"}), 200

            # DATE CORRECTION
            if lower.startswith("date "):
                date_part   = text[5:].strip()
                parsed_date = None
                try:
                    parsed_date = datetime.strptime(date_part,"%Y-%m-%d").strftime("%Y-%m-%d")
                except ValueError:
                    pass
                if not parsed_date:
                    for fmt in ["%d %b","%b %d","%d %B","%B %d",
                                "%d-%b","%d/%m/%Y","%d %b %Y","%b %d %Y"]:
                        try:
                            dt = datetime.strptime(date_part, fmt)
                            if dt.year == 1900:
                                dt = dt.replace(year=datetime.now().year)
                            parsed_date = dt.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                if not parsed_date:
                    send_message(user_phone, "Try: date 2025-02-03  or  date 3 feb")
                    return jsonify({"status":"ok"}), 200
                tx_ws,_,_,_,_ = tabs(wb)
                last = tx_ws.row_count
                if last > 1:
                    nm = datetime.strptime(parsed_date,"%Y-%m-%d").strftime("%B %Y")
                    ny = datetime.strptime(parsed_date,"%Y-%m-%d").year
                    tx_ws.update(f"A{last}:C{last}", [[parsed_date, nm, ny]])
                    send_message(user_phone, f"Date → {parsed_date}")
                return jsonify({"status":"ok"}), 200

            # QUICK QUERIES
            query_triggers = ["spent","last ","this week","today","food","groceries",
                              "transport","utilities","rent","health","school",
                              "fuel","uber","bolt","entertainment","clothing",
                              "savings this","investment this","income this"]
            if any(t in lower for t in query_triggers):
                send_message(user_phone, handle_query(text, wb))
                return jsonify({"status":"ok"}), 200

            # SPLIT
            if lower.startswith("split"):
                tx_ws,_,_,_,_ = tabs(wb)
                last_rows      = tx_ws.get_all_records()
                if not last_rows:
                    send_message(user_phone, "No transaction to split.")
                    return jsonify({"status":"ok"}), 200
                parent     = last_rows[-1]
                parent_amt = float(parent.get("Amount (N)",0) or 0)
                items      = parse_split(text, parent_amt)
                if not items:
                    send_message(user_phone,
                        "Could not parse split. Try:\n"
                        "split: food 8000, cleaning 3000, personal 1500")
                    return jsonify({"status":"ok"}), 200
                base_tx = {
                    "date":           parent.get("Date", datetime.now().strftime("%Y-%m-%d")),
                    "budget_month":   parent.get("Budget Month") or parent.get("Month",""),
                    "merchant":       parent.get("Merchant",""),
                    "confidence":     "high",
                    "date_source":    "receipt",
                    "date_confidence":"certain",
                    "fx_rate":        None, "amount_original": None, "currency":"NGN",
                }
                tx_ws.delete_rows(tx_ws.row_count)
                breakdown = save_split_transactions(base_tx, items, wb)
                send_message(user_phone,
                    f"Split into {len(items)} entries:\n{breakdown}")
                return jsonify({"status":"ok"}), 200

            # ── Falls through to parse_transaction (text-only logging) ────────
            # This handles: "water bill 4500", "salary 350k",
            #               "usd 500 stocks", "expense food 15000", etc.

        # ── IMAGE MESSAGE ─────────────────────────────────────────────────────
        elif msg_type == "image":
            media_id   = msg["image"]["id"]
            text       = msg["image"].get("caption") or None
            img_bytes, image_mime = download_wa_media(media_id)
            image_b64  = base64.standard_b64encode(img_bytes).decode()

        else:
            send_message(user_phone,
                "Send text or a receipt/alert image.\nType 'help' for commands.")
            return jsonify({"status":"ok"}), 200

        # ── PARSE + SAVE ──────────────────────────────────────────────────────
        tx = parse_transaction(text=text, image_b64=image_b64, image_mime=image_mime)

        if tx.get("type") == "unknown":
            send_message(user_phone,
                "Could not understand that. Examples:\n"
                "  water bill 4500\n"
                "  salary 350k\n"
                "  usd 500 stocks\n"
                "  expense food 15000\n"
                "  [receipt/alert photo]\n"
                "  Type 'help' for all commands")
            return jsonify({"status":"ok"}), 200

        save_transaction(tx, wb)
        send_message(user_phone, fmt_confirmation(tx, had_image=bool(image_b64)))

        # Budget alert
        alert = check_budget_alert(wb, tx)
        if alert:
            send_message(user_phone, alert)

        # Proactive debt reminders (max 2)
        for da in proactive_debt_alerts(wb)[:2]:
            send_message(user_phone, da)

        # Month-end prompt (#12)
        left = days_left_in_month()
        if left <= 3:
            send_message(user_phone,
                f"Month ends in {left} day(s). "
                f"Type 'report' for summary or 'goals' for progress.")

    except Exception as e:
        print(f"Webhook error: {e}\n{traceback.format_exc()}")

    return jsonify({"status":"ok"}), 200


# ════════════════════════════════════════════════════════════════════════════════
# SCHEDULED ENDPOINT
# ════════════════════════════════════════════════════════════════════════════════

@app.route("/cron/weekly", methods=["POST","GET"])
def cron_weekly():
    secret = request.args.get("key","")
    if secret != CRON_KEY:
        return "Unauthorized", 401
    wb = get_workbook()

    ok = send_backup_email(wb)

    if OWNER_PHONE:
        alerts = proactive_debt_alerts(wb)
        if alerts:
            send_message(OWNER_PHONE,
                "Weekly Debt Reminder:\n" + "\n".join(f"  {a}" for a in alerts))

        left = days_left_in_month()
        if left <= 3:
            send_message(OWNER_PHONE,
                f"Month ends in {left} day(s). "
                f"Type 'report' or 'goals' for progress.")

    return jsonify({"backup": ok, "status": "done"})


# ════════════════════════════════════════════════════════════════════════════════
# HELP
# ════════════════════════════════════════════════════════════════════════════════

HELP_TEXT = """Expense Tracker v4

LOGGING (receipt / alert / text):
  [photo only]         full auto — reads amount, date, recipient
  [photo] + groceries  category hint
  [photo] + march      budget month hint
  water bill 4500      text only (uses today's date)
  expense food 15000   explicit text entry
  salary 350k          income shortcut (k=×1000, m=×million)
  usd 500 stocks       foreign currency — auto converts via CBN rate
  usd 200 at 1580      with explicit rate

SPLIT RECEIPT (after logging):
  split: food 8000, cleaning 3000, personal 1500

QUICK QUERIES:
  spent on food
  transport this month
  last 5 transactions
  this week total
  today total

DEBTS:
  legacy lent 50000 Emeka as of 2025-02-01   (owed TO you → investment)
  legacy debt 300000 car loan as of 2025-01-01  (you OWE → debt)
  settle Emeka 30000       partial settlement
  settle car loan          full settlement
  debts                    ageing report

SAVINGS GOALS:
  goal house deposit 5000000 by 2026-12-31
  goal nse portfolio 2000000 by 2025-12-31
  goals                    view all progress

BUDGET:
  budget march income 2000000
  budget march expense 1000000
  budget               view targets

LEGACY (opening balances):
  legacy savings 500000
  legacy investment 1200000 NSE portfolio
  legacy               view all

REPORTS:
  report               this month
  report march 2025    specific month
  networth             NGN + USD (CBN rate)
  goals                goal progress
  debts                debt ageing
  backup               email CSV

CORRECTIONS:
  undo                 remove last entry
  date 3 feb           fix date on last entry
  bm march             reassign last entry to March budget"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
