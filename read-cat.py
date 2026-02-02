
import read_milen as mil
import read_chase as chase
import update_csv_trans as cat
import gspread 
import json
import os
import csv
import pandas as pd

PATH_F = "/Users/cmcnally/Dropbox/python/textfiles/"
S_KEY = "1ai9nZYCNw5g5-fv0-siPryHWYwl7hSfTunq5wswYfDo"

def open_sheet():
    credentials =  json.loads(os.environ["SERVICE_JSON"])
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(S_KEY)
    worksheet =  sh.get_worksheet(0)
    return worksheet

def convert_to_dict(dataf):
    file = "/Users/cmcnally/Dropbox/python/textfiles/temp.csv"
    header = ["account","lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
    dataf.to_csv(file, index=False, columns=header,date_format='%Y-%m-%d')
    with open(file,"r",encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        trans = list(reader)
    return trans

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
    origDb = pd.DataFrame(origDb[ 1 :], columns = header) # skip the first row
    count = len(dataf)


    origDb = pd.DataFrame(origDb[ 1 :], columns = header) # skip the first row
    origDb['lance'] = origDb['lance'].astype(str)
    origDb['amount'] = origDb['amount'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    origDb['balance'] = origDb['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    dataf['amount'] = dataf['amount'].apply(lambda x: '' if x == '' else  str('%.2f' %  float(x) ))
    dataf['balance'] = dataf['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    if (useUSD):
        origDb['usd'] = origDb['usd'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
        dataf['usd'] = dataf['usd'].apply(lambda x: '' if x == '' else  str('%.2f' %  float(x) ))

    # dataf.drop(drops) where drops is rows [0,1,2]
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
    dataf = dataf[header] # puts them in the right order
    trans =  dataf.values.tolist()
    worksheet.insert_rows(trans,row=2,value_input_option='USER_ENTERED')
    #worksheet.sort((1, 'des'), (0, 'asc'))

def handle_millenium(file):
    dataf = mil.readMil(file)
    dataf = mil.initOtherColumns(dataf)
    dataf = mil.fix_credits(dataf)
    dataf = cat.add_categories_df(dataf)
    # write to the spreadsheet appending new rows. maybe check for duplicates too yay!
    #worksheet = open_sheet()
    #origDb = worksheet.get_values()
    #origDb = pd.DataFrame(origDb[ 1 :], columns = header) # skip the first row

    #print(f"the spreadsheet currently has {worksheet.row_count} rows")
    #count = len(dataf)
    dataf = old_dedupe(dataf)
    #newCount = len(dataf)
    #print(f"Eliminated {count - newCount} duplicate rows out of {count} rows")

    write_to_sheet(dataf)

header = ["account","lance","dv","desc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who","PK"]
file = "Portuguese_bank-13-Jan-31-Jan.csv"
#handle_millenium(file)
file = "Chase5869_Activity-20260131.csv"
dataf = chase.read_chase(file)
dataf = chase.add_euro_other_fields_chase(dataf)
dataf = cat.add_categories_df(dataf)
dataf = old_dedupe(dataf,useUSD=True)
write_to_sheet(dataf)
