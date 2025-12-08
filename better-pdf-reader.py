import re
import pandas as pd
from datetime import datetime

from pdf2image import convert_from_path
import pytesseract
from PIL import Image

PDF_PATH = "/Users/cmcnally/Dropbox/python/textfiles/"
PDF_FILES = ["012024.pdf","022024.pdf",	"032024.pdf",	"042024.pdf",	"052025.pdf",	"062024.pdf",	"072024.pdf",	"082024.pdf",	"092024.pdf",	"102024.pdf",	"112024.pdf",	"122024.pdf"]
OUTPUT_CSV = "/Users/cmcnally/Dropbox/python/textfiles/converted_statements.csv"


# -------- Utility functions -------- #

def fix_date(d,default_year=2024):
    # mmdd = "1.02" or "12.31"
    if not d:
        return ""
    mm, dd = d.split(".")
    try:
        return datetime(default_year, int(mm), int(dd)).strftime("%Y-%m-%d")
    except:
        return ""

def clean_amount(s, force_neg=False):
    if not s:
        return None
    s = s.strip()

    neg = force_neg # there is no way to tell the difference make them all negative and fix manually
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]

    s = s.replace(" ", "").replace(",", "")  # remove thousand separators

    try:
        val = float(s)
    except:
        return None

    return -abs(val) if neg else val



def looks_like_date(s):
    return bool(re.match(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$", s))


# -------- OCR + header scanning -------- #

def extract_lines_from_pdf(pdf_path):
    pages = convert_from_path(pdf_path, dpi=300)
    all_lines = []

    for img in pages:
        gray = img.convert("L")
        bw = gray.point(lambda x: 0 if x < 180 else 255, '1')
        text = pytesseract.image_to_string(bw, lang="por+eng")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        all_lines.extend(lines)

    return all_lines


def parse_transactions(lines):
    HEADER_PATTERN = r"lanc|valor|descritivo|debito|credito|saldo"
    header_found = False

    rows = []

    for line in lines:
        low = line.lower()
        print(low)
        # Detect header row ANYWHERE in document
        if not header_found and re.search(HEADER_PATTERN, low):
            header_found = True
            continue

        # After header found: parse only lines starting with date
        if header_found:
#            m = re.match(r"^(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(.*)$", line)
            m = re.match(r"^(\d{1,2}\.\d{1,2})\s+(.*)$", line)

            if not m:
                continue

            date_lanc = m.group(1)
            rest = m.group(2).strip()

            # Next segment should begin with Data Valor
            m2 = re.match(r"^(\d{1,2}\.\d{1,2})\s+(.*)$", rest)
            if not m2:
                continue

            date_valor = m2.group(1)
            rest2 = m2.group(2).strip()

            # Amount-looking tokens at end
#            amounts = re.findall(r"\(?\d[\d\.\,]*\d\)?", rest2)
#            amounts = re.findall(r"\(?-?\d[\d\s,]*\.\d{2}\)?", rest2)
            AMOUNT_RE = r"\(?-?(?:\d+\.\d{2}|\d \d{3}\.\d{2})\)?"
            amounts = re.findall(AMOUNT_RE, rest2)
            if len(amounts) < 2:
                # must have debit/credit + balance at minimum
                continue

            balance_raw = amounts[-1]
            dc_raw = amounts[-2]
            desc = rest2
            desc = desc
            for a in amounts:
                desc = desc.replace(a, "").strip()

            # Determine debit/credit via sign or position
            debit_val = None
            credit_val = None

            # If negative or parentheses → debit
            if "-" in dc_raw or "(" in dc_raw or ")" in dc_raw:
                debit_val = clean_amount(dc_raw)
            else:
                credit_val = clean_amount(dc_raw,True)

            # Build structured row
            #"lance","dv","desc","amount","type","balance"
            rows.append({
                "lance": fix_date(date_lanc),
                "dv": fix_date(date_valor),
                "desc": desc,
                "amount": credit_val,
                "type":"D",
                "balance": clean_amount(balance_raw)
            })

    return rows


# -------- Main pipeline -------- #


all_rows = []
for file in PDF_FILES:
    lines = extract_lines_from_pdf(PDF_PATH + file)
    rows = parse_transactions(lines)
    all_rows.extend(rows)

if not all_rows:
    print("No rows parsed. You may need to provide 1–2 sample extracted text lines.")
    

df = pd.DataFrame(all_rows)
header = ["lance","dv","desc","amount","type","balance"]
df.to_csv(OUTPUT_CSV,index=False, columns=header)