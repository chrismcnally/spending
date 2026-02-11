

import read_milen as mil
import read_chase as chase
import update_csv_trans as cat
import gspread 
import json
import os
import pandas as pd
import itertools

PATH_F = "/Users/cmcnally/Dropbox/python/textfiles/"
S_KEY = "1ai9nZYCNw5g5-fv0-siPryHWYwl7hSfTunq5wswYfDo"
HEADER = ["account","lance","dv","desc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who","PK"]


def easy_assign_accounts():
    millenium = ["COMPRA 2860",
                 "COMPRA 6253",
                 "COMPRA 6519",
                 "COMPRA 7455",
                 "COMPRA 7482",
                 "COMPRA 9496",
                 "COMPRA 0885",
                 "LEV ATM 0885",
                 "LEV ATM 2860",
                 "LEV ATM 6253",
                 "LEV ATM 6519",
                 "LEV ATM 7455",
                 "LEV ATM 7482",
                 "LEV ATM 9496",
                 "TRF MB WAY",
                 "COMISSAO TRF",
                 "Cleaning Clara",
                 "IMPOSTO DO SELO"]
    worksheet = open_sheet()
    origDb = worksheet.get_values()
    origDb = pd.DataFrame(origDb[ 1 :], columns = HEADER) # skip the first row

    before = len(origDb.loc[origDb["account"] is None or origDb["account"] == ""])
    origDb.loc[origDb['desc'].str.contains(r'^(?:' + '|'.join(millenium) + ')', na=False), 'account'] = "Millenium"
    after = len(origDb.loc[origDb["account"] is None or origDb["account"] == ""])
    rstr = r'^(?:' + '|'.join(millenium) + ')'
   
    print(f"updates {before - after} rows")
    rowcount = worksheet.row_count
    trans = origDb.values.tolist()
    range = f"A2:O{rowcount}"
    worksheet.update(range_name=range,values=trans,value_input_option='USER_ENTERED')

def open_sheet():
    credentials =  json.loads(os.environ["SERVICE_JSON"])
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(S_KEY)
    worksheet =  sh.get_worksheet(0)
    return worksheet

def clean_dupes(origDb, newRows):
    # does not work, probably related to datatypes
    # this amazing bit of code stacks newRows on the bottom of origDb, removes duplicates in the bottom,
    # then lops off the original top rows to leave bottom de-duped
    df_new_cleaned = pd.concat([origDb, newRows]).drop_duplicates(
        subset=['lance', 'amount', 'balance'], 
        keep='first'
    ).iloc[len(origDb):] # the colon here means all the rows AFTER the top dataset, origDb
    return df_new_cleaned


def old_dedupe(dataf,useUSD=False):
    # reworked this to go to the worksheet once, load to a dataframe and do all checking client side,
    # the reason is multiple calls to the sheet will eventually cause too many read errors from google, plus S.L.O.W.
    drops = []  
    worksheet = open_sheet()
    origDb = worksheet.get_values()
    origDb = pd.DataFrame(origDb[ 1 :], columns = HEADER) # skip the first row
    count = len(dataf)
    origDb['lance'] = origDb['lance'].astype(str)
    origDb['amount'] = origDb['amount'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    origDb['balance'] = origDb['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    dataf['amount'] = dataf['amount'].apply(lambda x: '' if x == '' else  str('%.2f' %  float(x) ))
    dataf['balance'] = dataf['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    if (useUSD):
        origDb['usd'] = origDb['usd'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
        dataf['usd'] = dataf['usd'].apply(lambda x: '' if x == '' else  str('%.2f' %  float(x) ))

    # dataf.drop(drops) where drops is rows [0,1,2] drop rows in dataf (new data) that's already in origDb
    for row in  dataf.itertuples(index=True):
        index = row.Index
        date = row.lance.strftime('%Y-%m-%d')
        #this can be done with ands instead of subsetting like this, original code only let you search the sheet by one column
        matches = origDb.loc[origDb.lance == date]
        if (len(matches) > 0):
            if (useUSD):
                subset = matches.loc[matches.usd == row.usd]
            else:    
                subset = matches.loc[matches.amount == row.amount]
            if (len(subset) > 0):
                anymore = subset.loc[subset.balance == row.balance]
                if (len(anymore) > 0):
                    print(f"dropping duplicate row {row.lance} {row.desc} for {row.amount} balance {row.balance}")
                    drops.append(index)
    # drop the drops
    dataf = dataf.drop(drops)
    newCount = len(dataf)
    print(f"Eliminated {count - newCount} duplicate rows out of {count} rows")
    return dataf

def write_to_sheet(dataf,worksheet=None):
    if (worksheet is None):
        worksheet = open_sheet()
    # convert dates to string for proper load into sheet as user values
    dataf["lance"] = dataf["lance"].apply( lambda x : x.strftime('%Y-%m-%d'))
    dataf["dv"] = dataf["dv"].apply( lambda x : x.strftime('%Y-%m-%d'))
    # add PK
    next_pk = worksheet.row_count + 1
    dataf['PK'] = list(range(next_pk + 1,next_pk + len(dataf)+1) )
    #header = ["account","lance","dv","de   sc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who","PK"]
    dataf = dataf[HEADER] # puts them in the right order
    dataf = dataf.fillna('') # MAYBE THIS SHOULD BE 0 OR 0.0 i was getting Nan in the amount column for usd transactions
    trans =  dataf.values.tolist()
    worksheet.insert_rows(trans,row=2,value_input_option='USER_ENTERED')
    #worksheet.sort((1, 'des'), (0, 'asc'))

def handle_millenium(file):
    dataf = mil.readMil(file)
    dataf = mil.initOtherColumns(dataf)
    dataf = mil.fix_credits(dataf)
    dataf = cat.add_categories_df(dataf)
    dataf = old_dedupe(dataf)
    write_to_sheet(dataf)

def handle_chase(file):
    dataf = chase.read_chase(file)
    dataf = chase.add_euro_other_fields_chase(dataf)
    dataf = cat.add_categories_df(dataf)
    dataf = old_dedupe(dataf,useUSD=True)
    write_to_sheet(dataf)

def handle_schwab(file,account):
    dataf = chase.read_schwab(file)
    dataf = chase.add_schwab_fields(dataf,account)
    dataf = cat.add_categories_df(dataf)
    dataf = old_dedupe(dataf,useUSD=True)
    write_to_sheet(dataf)

def handle_schwab_from_ynab(file,account):
#    dataf = chase.read_schwab(file)
#    dataf = chase.add_schwab_fields(dataf,account)

    pathf = "/Users/cmcnally/Dropbox/python/textfiles/" + file
    dataf = pd.read_csv(pathf, delimiter=",", engine='python',header=0,
          parse_dates=['lance','dv'], dayfirst=False) 

    dataf = cat.add_categories_df(dataf)
    dataf = old_dedupe(dataf,useUSD=True)
    write_to_sheet(dataf)

def handle_ally(file,account):
    dataf = chase.read_ally(file)
    dataf = chase.add_euro_other_fields_ally(dataf,account)
    dataf = cat.add_categories_df(dataf)
    dataf = old_dedupe(dataf,useUSD=True)
    write_to_sheet(dataf)


file = "Portuguese-bank-feb-2026-partial.csv"
#handle_millenium(file)
file = "Chase5869_Activity_20251231.csv"
#handle_chase(file)
file = "Schwab_751_Checking_31012026.csv"
#handle_schwab(file,"Sch-Checking-751")
#handle_schwab(file,"Sch-192-Rosemary")
file = "ally_1051307708_2025.csv"
#handle_ally(file,"ally-1051307708")
file = "ally_sav-mc-combo-2025.csv"
file = "ally-sav-mcnally-2024.csv"
handle_ally(file,"ally-2151307713-2190559050")
# fix accounts 
#easy_assign_accounts()
file = "test_schwab_2022.csv"
#handle_schwab_from_ynab(file,"Sch-Checking-751")
