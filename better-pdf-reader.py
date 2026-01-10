import re
import pandas as pd
from datetime import datetime

from pdf2image import convert_from_path
import pytesseract
from PIL import Image

PDF_PATH = "/Users/cmcnally/Dropbox/python/textfiles/"
PDF_FILES = ["FIXED-milen-2024.in","012024.pdf","022024.pdf",	"032024.pdf",	"042024.pdf",	"052025.pdf",	"062024.pdf",	"072024.pdf",	"082024.pdf",	"092024.pdf",	"102024.pdf",	"112024.pdf",	"122024.pdf"]
OUTPUT_CSV = "/Users/cmcnally/Dropbox/python/textfiles/converted_statements.csv"


# ---- with help from ChatGPT, this code OCRs the PDFs from Millennium Bank and creates rows ---#
# bugs include if description ends in a number, and the amount is 3 digists, xxx.xx, then it grabs
# the last digit of the description, n and makes it the thousands so amount becomes nxxx.xx
# there is no way to tell debit from credit so assume Debit and manually fix up

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
#    HEADER_PATTERN = r"lanc|valor|descritivo|debito|credito|saldo"
    HEADER_PATTERN = r"lanc|descritivo|debito|credito"
    header_found = False

    rows = []

    for line in lines:
        low = line.lower()
        # Detect header row ANYWHERE in document
        if not header_found and re.search(HEADER_PATTERN, low):
            header_found = True
            continue

        # After header found: parse only lines starting with date
        if header_found:
#            m = re.match(r"^(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(.*)$", line)
            print(line)
            m = re.match(r"^(\d{1,2}\.\d{1,2})\s+(.*)$", line)
            # sometimes this does not match lines so they are skipped
            if not m:
                print("line has no date")
                continue

            date_lanc = m.group(1)
            rest = m.group(2).strip()

            # Next segment should begin with Data Valor
            m2 = re.match(r"^(\d{1,2}\.\d{1,2})\s+(.*)$", rest)
            if not m2:
                print("line does not have second date")
                continue

            date_valor = m2.group(1)
            rest2 = m2.group(2).strip()

            # Amount-looking tokens at end
            #AMOUNT_RE = r"\(?-?(?:\d+\.\d{2}|\d \d{3}\.\d{2})\)?"
            #AMOUNT_RE = r"(?:\d{1,5}\.\d{2}|\d{1,2} \d{3}\.\d{2})"
            AMOUNT = r"(?:\d{1,5}\.\d{2}|\d{1,2} \d{3}\.\d{2})"
            TWO_AMOUNTS_AT_END = rf"({AMOUNT})\s+({AMOUNT})$"
            m = re.search(TWO_AMOUNTS_AT_END, rest2)
            if m:
                amount1 = m.group(1)
                amount2 = m.group(2)
            #amounts = re.findall(AMOUNT_RE, rest2)
            if not m:
                print("line does not end in 2 anounts")
                # must have debit/credit + balance at minimum
                continue

            balance_raw = amount2
            dc_raw = amount1
            desc = rest2
            desc = desc
            desc = desc.replace(amount1, "").strip()
            desc = desc.replace(amount2, "").strip()

            # Determine debit/credit via sign or position
            debit_val = None
            credit_val = None
            ctype = "D"

            # If negative or parentheses → debit
            if  "wise" in desc.lower():
                credit_val = clean_amount(dc_raw)
                ctype = "T"
            else:
                credit_val = clean_amount(dc_raw,True)

            # Build structured row
            #"lance","dv","desc","amount","type","balance"
            rows.append({
                "lance": fix_date(date_lanc),
                "dv": fix_date(date_valor),
                "desc": desc,
                "amount": credit_val,
                "type": ctype,
                "balance": clean_amount(balance_raw)
            })

    return rows


# -------- Main pipeline -------- #

def init_other_fields(rows):
    dataf = pd.DataFrame(rows)
    dataf["usd"] = 0.0
    dataf["erate"] = 0.0
    dataf["memo"] = ""
   # dataf['amount'] = dataf.amount.apply(lambda x :round( Decimal(x),2)) # make amount a decimal instead of float
    dataf["category"] = ""
    dataf["subcat"] = ""
    dataf["fragment"] = ""
    dataf["newt"] = dataf["type"]
    dataf["who"] = ""
    # transfers are not credits, change toT for transfer
    dataf.loc[dataf["desc"].str.contains("transferwise", case=False, na=False),"newt",] = "T"
    dataf.loc[dataf["desc"].str.contains("EMILY HELLA TSACONAS", case=True, na=False),"newt",] = "T"
    # this one too "Wise" and "Ord.Pgt.do Estrg"

    return dataf


# due to one difficult to read PDF (april 2024), I have modified this to take as input a cleaned up basic lines file which
# i created here by reading the pdf below (commented out) and writing a plain text file with the lines, manually then deleting the crap that is not
# the transactions and fixing mostly the dates which caused the issues. Then I ran the code to parse on that file. 

all_rows = []
for file in PDF_FILES:
#    lines = extract_lines_from_pdf(PDF_PATH + file)
    if (file == "FIXED-milen-2024.in"):
        text_file = open(PDF_PATH + file, "r")
        lines = text_file.readlines()
    #    outfile = file.replace(".pdf",".in")
    #    with open(PDF_PATH + outfile, 'w') as file_handler:
        # Join all items with a newline separator and write the single resulting string
    #        file_handler.write('\n'.join(lines))
        rows = parse_transactions(lines)
        dataframe = init_other_fields(rows)
        header = ["lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
        dataframe.to_csv(PDF_PATH + "converted_" + file.replace(".in",".csv"),index=False, columns=header,date_format='%Y-%m-%d',float_format='%.2f')        
        all_rows.extend(rows)

if not all_rows:
    print("No rows parsed. You may need to provide 1–2 sample extracted text lines.")
    

df = pd.DataFrame(all_rows)
#header = ["lance","dv","desc","amount","type","balance"]
#df.to_csv(OUTPUT_CSV,index=False, columns=header)
