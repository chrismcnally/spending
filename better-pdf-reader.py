import re
import pandas as pd
from datetime import datetime

from pdf2image import convert_from_path
import pytesseract
from PIL import Image

PDF_PATH = "/Users/cmcnally/Dropbox/python/textfiles/"
PDF_FILES = ["FIXED-milen-2024.in","012024.pdf","022024.pdf",	"032024.pdf",	"042024.pdf",	"052025.pdf",	"062024.pdf",	"072024.pdf",	"082024.pdf",	"092024.pdf",	"102024.pdf",	"112024.pdf",	"122024.pdf"]
PDF_FILES = ["013121 WellsFargo.pdf",
"013122 WellsFargo.pdf",
"013123 WellsFargo.pdf",
"013124 WellsFargo.pdf",
"022821 WellsFargo.pdf",
"022822 WellsFargo.pdf",
"022823 WellsFargo.pdf",
"022924 WellsFargo.pdf",
"033121 WellsFargo.pdf",
"033122 WellsFargo.pdf",
"033123 WellsFargo.pdf",
"033124 WellsFargo.pdf",
"043021 WellsFargo.pdf",
"043022 WellsFargo.pdf",
"043023 WellsFargo.pdf",
"043024 WellsFargo.pdf",
"053121 WellsFargo.pdf",
"053122 WellsFargo.pdf",
"053123 WellsFargo.pdf",
"063021 WellsFargo.pdf",
"063022 WellsFargo.pdf",
"063023 WellsFargo.pdf",
"063024 WellsFargo.pdf",
"073121 WellsFargo.pdf",
"073122 WellsFargo.pdf",
"073123 WellsFargo.pdf",
"073124 WellsFargo.pdf",
"083121 WellsFargo.pdf",
"083122 WellsFargo.pdf",
"083123 WellsFargo.pdf",
"083124 WellsFargo.pdf",
"093021 WellsFargo.pdf",
"093022 WellsFargo.pdf",
"093023 WellsFargo.pdf",
"103121 WellsFargo.pdf",
"103122 WellsFargo.pdf",
"103123 WellsFargo.pdf",
"113021 WellsFargo.pdf",
"113022 WellsFargo.pdf",
"113023 WellsFargo.pdf",
"123121 WellsFargo.pdf",
"123122 WellsFargo.pdf",
"123123 WellsFargo.pdf"]
OUTPUT_CSV = "/Users/cmcnally/Dropbox/python/textfiles/wellsFargo_converted_statements.csv"
#LANGS = "por+eng"
LANGS = "eng"
IS_MILEN = False
IS_WELLS = True
# ---- with help from ChatGPT, this code OCRs the PDFs from Millennium Bank and creates rows ---#
# bugs include if description ends in a number, and the amount is 3 digists, xxx.xx, then it grabs
# the last digit of the description, n and makes it the thousands so amount becomes nxxx.xx
# there is no way to tell debit from credit so assume Debit and manually fix up

# -------- Utility functions -------- #

def fix_date(d,default_year=2024):
    # mmdd = "1.02" or "12.31"
    if (IS_MILEN):
        split_char = "."
    else:
        split_char = "."
#        split_char = "/"
    if not d:
        return ""
    mm, dd = d.split(split_char)
    try:
        return datetime(int(default_year), int(mm), int(dd)).strftime("%Y-%m-%d")
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
        text = pytesseract.image_to_string(bw, lang=LANGS)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        all_lines.extend(lines)

    return all_lines

def process_lines(lines, default_year="2024"):
    rows = []
    FILE_PATTERN = r"^(\d{6})" # this is for Wells Fargo

    for line in lines:
        m = re.match(FILE_PATTERN,line)
        if (m):
            default_year = "20" + line[4:6]
            continue
        print(line)
        m = re.match(r"^(\d{1,2}\.\d{1,2})\s+(.*)$", line)
        if not m:
            print(f"line has no date {line}")
            continue
        date_lanc = m.group(1)
        rest = m.group(2).strip()
        date_valor = date_lanc
        rest2 = rest

        AMOUNT = r"\d{1,3}(?:,\d{3})*\.\d{2}"
        TWO_AMOUNTS_AT_END = rf"({AMOUNT})\s+({AMOUNT})$"
        m = re.search(TWO_AMOUNTS_AT_END, rest2)
        if m:
            amount1 = m.group(1)
            amount2 = m.group(2)
        if not m:
            print("line does not end in 2 anounts")
            continue
        balance_raw = amount2
        dc_raw = amount1
        desc = rest2
        desc = desc.replace(amount1, "").strip()
        desc = desc.replace(amount2, "").strip()

        credit_val = None
        ctype = "D"

        credit_val = clean_amount(dc_raw,True)
        if  "wise" in desc.lower():
            credit_val = clean_amount(dc_raw)
            ctype = "T"
        if ("hilary obrien" in desc.lower()):
            credit_val = clean_amount(dc_raw)
            ctype = "I"
        if ("caitlin l heinz" in desc.lower()):
            credit_val = clean_amount(dc_raw)
            ctype = "I"

        # Build structured row
        #"lance","dv","desc","amount","type","balance"
        rows.append({
            "lance": fix_date(date_lanc,default_year),
            "dv": fix_date(date_valor,default_year),
            "desc": desc,
            "amount": credit_val,
            "type": ctype,
            "balance": clean_amount(balance_raw)
        })

    return rows

           
def parse_transactions(lines,default_year="2024"):
#    HEADER_PATTERN = r"lanc|valor|descritivo|debito|credito|saldo"
    if IS_MILEN:
       HEADER_PATTERN = r"lanc|descritivo|debito|credito" # this is the old milenium,
    else:
        HEADER_PATTERN = r"Description" # this is for Wells Fargo
    EOF_PATTERN = "Ending balance on"
    header_found = False
    eof = False
    bad_date = False

    rows = []

    for line in lines:
        low = line.lower()
        #print(low)
        # Detect header row ANYWHERE in document
        if not header_found and re.search(HEADER_PATTERN, line):
            header_found = True
            continue
        if (header_found) and re.search(EOF_PATTERN, line):
            eof = True
            continue
        # After header found: parse only lines starting with date
        if header_found and not eof:
#            m = re.match(r"^(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(.*)$", line)
            print(line)
            if (IS_MILEN):
                m = re.match(r"^(\d{1,2}\.\d{1,2})\s+(.*)$", line)
            else:
                m = re.match(r"^(\d{1,2}/\d{1,2})\s+(.*)$", line)
            # sometimes this does not match lines so they are skipped
            if not m:
                print(f"line has no date {line}")
                bad_date = True
                # this is an op to manually fix the line in the debugger
                if (not bad_date):
                    m = re.match(r"^(\d{1,2}/\d{1,2})\s+(.*)$", line)
                else:
                    continue

            date_lanc = m.group(1)
            rest = m.group(2).strip()

            # Next segment should begin with Data Valor, however wells fargo does not have this
            if (IS_MILEN):
                m2 = re.match(r"^(\d{1,2}\.\d{1,2})\s+(.*)$", rest)
                if not m2:
                    print("line does not have second date")
                    continue
                date_valor = m2.group(1)
                rest2 = m2.group(2).strip()
            else:
                date_valor = date_lanc
                rest2 = rest
            # Amount-looking tokens at end
            #AMOUNT_RE = r"\(?-?(?:\d+\.\d{2}|\d \d{3}\.\d{2})\)?"
            #AMOUNT_RE = r"(?:\d{1,5}\.\d{2}|\d{1,2} \d{3}\.\d{2})"
            if (IS_MILEN):
                AMOUNT = r"(?:\d{1,5}\.\d{2}|\d{1,2} \d{3}\.\d{2})"
                TWO_AMOUNTS_AT_END = rf"({AMOUNT})\s+({AMOUNT})$"
            else:
                AMOUNT = r"\d{1,3}(?:,\d{3})*\.\d{2}"
                TWO_AMOUNTS_AT_END = rf"({AMOUNT})\s+({AMOUNT})$"

            m = re.search(TWO_AMOUNTS_AT_END, rest2)
            if m:
                amount1 = m.group(1)
                amount2 = m.group(2)
            #amounts = re.findall(AMOUNT_RE, rest2)
            if not m:
                print("line does not end in 2 anounts")
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


            credit_val = clean_amount(dc_raw,True)
            # If negative or parentheses → debit
            if  "wise" in desc.lower():
                credit_val = clean_amount(dc_raw)
                ctype = "T"
            if ("hilary obrien" in desc.lower()):
                credit_val = clean_amount(dc_raw)
                ctype = "I"
            if ("caitlin l heinz" in desc.lower()):
                credit_val = clean_amount(dc_raw)
                ctype = "I"

            # Build structured row
            #"lance","dv","desc","amount","type","balance"
            rows.append({
                "lance": fix_date(date_lanc,default_year),
                "dv": fix_date(date_valor,default_year),
                "desc": desc,
                "amount": credit_val,
                "type": ctype,
                "balance": clean_amount(balance_raw)
            })

    return rows


# -------- Main pipeline -------- #

def init_other_fields(rows):
    dataf = pd.DataFrame(rows)
    dataf["usd"] = dataf["amount"]
    if (IS_MILEN):
        dataf["usd"] = 0.0
    else:
        dataf["amount"] = 0.0
    dataf["erate"] = 0.0
    dataf["lance"] =  pd.to_datetime(dataf['lance'])
    dataf["dv"] =  pd.to_datetime(dataf['dv'])
    dataf["memo"] = ""
   # dataf['amount'] = dataf.amount.apply(lambda x :round( Decimal(x),2)) # make amount a decimal instead of float
    dataf["category"] = ""
    dataf["subcat"] = ""
    dataf["fragment"] = ""
    dataf["newt"] = dataf["type"]
    dataf["who"] = ""
    # transfers are not credits, change toT for transfer
    if (IS_MILEN):
        dataf.loc[dataf["desc"].str.contains("transferwise", case=False, na=False),"newt",] = "T"
        dataf.loc[dataf["desc"].str.contains("EMILY HELLA TSACONAS", case=True, na=False),"newt",] = "T"
    # this one too "Wise" and "Ord.Pgt.do Estrg"

    return dataf


# due to one difficult to read PDF (april 2024), I have modified this to take as input a cleaned up basic lines file which
# i created here by reading the pdf below (commented out) and writing a plain text file with the lines, manually then deleting the crap that is not
# the transactions and fixing mostly the dates which caused the issues. Then I ran the code to parse on that file. 

all_rows = []
all_rows_df = None
# the OCR is so bad that we can hardly use the code on the wells fargo. Instead I spit out the lines into a file and 
# will process those, the file starts with filename on a line ie mmddyy WellsFargo.pdf, then the lines. Need the file name
# to figure out the year (as the lines simply have short dates of mm.dd)
if (IS_WELLS):
    text_file = open(PDF_PATH + "scraped_wells_fargo.txt", "r")
    lines = text_file.readlines()
    rows = process_lines(lines,"2024")
    dataframe = init_other_fields(rows)
    file = "Wells_Fargo_PDFS_pass_2"
    header = ["lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
    dataframe.to_csv(PDF_PATH + "converted_" + file + ".csv",index=False, columns=header,date_format='%Y-%m-%d',float_format='%.2f')        
    quit()


for file in PDF_FILES:
#    lines = extract_lines_from_pdf(PDF_PATH + file)
#    if (file == "FIXED-milen-2024.in"):
#        text_file = open(PDF_PATH + file, "r")
#        lines = text_file.readlines()
    if (IS_WELLS):
        default_year = "20" + file[4:6]
    else:
        default_year="2024"
    lines = extract_lines_from_pdf(PDF_PATH + file)
    print(file)
    rows = parse_transactions(lines,default_year)
    if (len(rows) > 0):
        dataframe = init_other_fields(rows)
        if (all_rows_df is None):
            all_rows_df = dataframe
        else:
            all_rows_df = pd.concat([dataframe, all_rows_df], ignore_index=True)
        all_rows.extend(rows)

if not all_rows:
    print("No rows parsed. You may need to provide 1–2 sample extracted text lines.")
    

#df = pd.DataFrame(all_rows)
header = ["lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
if (IS_WELLS):
    file = "Wells_Fargo_PDFS"
dataframe.to_csv(PDF_PATH + "converted_" + file + ".csv",index=False, columns=header,date_format='%Y-%m-%d',float_format='%.2f')        

#header = ["lance","dv","desc","amount","type","balance"]
#df.to_csv(OUTPUT_CSV,index=False, columns=header)
