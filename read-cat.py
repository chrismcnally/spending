
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
    #trans = pd.DataFrame(worksheet.get_all_records())
    print(f"the spreadsheet currently has {worksheet.row_count} rows")
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
    # this amazing bit of code stacks newRows on the bottom of origDb, (origDb on the bottom) removed duplicates in the bottom,
    # then lops off the original top rows to leave bottom de-duped
    df_new_cleaned = pd.concat([origDb, newRows]).drop_duplicates(
        subset=['lance', 'amount', 'balance'], 
        keep='first'
    ).iloc[len(origDb):] # the colon here means all the rows AFTER the top dataset, origDb
    return df_new_cleaned

def old_dedupe(dataf, worksheet):
    # rework this to go to the worksheet once, load to a dataframe or something and do all checking client side,
    # the reason is multiple calls to the sheet will eventually cause too many read errors from google
    drops = []  
    origDb = worksheet.get_values()
    origDb = pd.DataFrame(origDb[ 1 :], columns = header) # skip the first row
    origDb['lance']=origDb['lance'].astype(str)
    origDb['amount'] = origDb['amount'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    origDb['balance'] = origDb['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    dataf['amount'] = dataf['amount'].apply(lambda x: '' if x == '' else  str('%.2f' %  float(x) ))
    dataf['balance'] = dataf['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))

    # dataf.drop(drops) where drops is rows [0,1,2]
    for row in  dataf.itertuples(index=True):
        index = row.Index
        date = row.lance.strftime('%Y-%m-%d')
        matches = origDb.loc[origDb.lance == date]
#        key = date + str('$%.2f' %  float(row.amount)) + row.newt + str('$%.2f' %  float(row.balance))  # include balance, duplicates already
#        matches = worksheet.findall(date,in_column=2)
        #optimization, we have a list of matches for a date, the next iteration could be the same date, save the matches and don't requery if so
        print(f"found {len(matches)} matches for {row.lance} in sheet")
#        for match in matches:
        if (len(matches) > 0):
            # must get the row  number of this cell then fetch the row to compare values
            #range = gspread.utils.rowcol_to_a1(match.row,1) + ":" + gspread.utils.rowcol_to_a1(match.row,9)
            #match1 = worksheet.get_values(range)
            subset = matches.loc[matches.amount == row.amount]
            if (len(subset) > 0):
                anymore = subset.loc[subset.balance == row.balance]
                if (len(anymore) > 0):
                    print(f"dropping duplicate row {row.lance} {row.desc} for {row.amount} balance {row.balance}")
                    drops.append(index)
    # drop the drops
    dataf = dataf.drop(drops)
    return dataf

file = "Portuguese_bank-13-Jan-31-Jan.csv"
header = ["account","lance","dv","desc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who","PK"]
dataf = mil.readMil(file)
dataf = mil.initOtherColumns(dataf)
dataf = mil.fix_credits(dataf)
#trans = convert_to_dict(dataf)
dataf = cat.add_categories_df(dataf)
# write to the spreadsheet appending new rows. maybe check for duplicates too yay!
worksheet = open_sheet()
origDb = worksheet.get_values()
origDb = pd.DataFrame(origDb[ 1 :], columns = header) # skip the first row

print(f"the spreadsheet currently has {worksheet.row_count} rows")
next_pk = worksheet.row_count + 1
count = len(dataf)
# ensure columns in the right order
#dataf = clean_dupes(origDb, dataf)
dataf = old_dedupe(dataf,worksheet)
newCount = len(dataf)
print(f"Eliminated {count - newCount} duplicate rows out of {count} rows")

#dataf["lance"] = dataf["lance"].apply( lambda x : x.date())
dataf["lance"] = dataf["lance"].apply( lambda x : x.strftime('%Y-%m-%d'))
#dataf["dv"] = dataf["dv"].apply( lambda x : x.date())
dataf["dv"] = dataf["dv"].apply( lambda x : x.strftime('%Y-%m-%d'))
# add PK
dataf['PK'] = list(range(next_pk + 1,next_pk + len(dataf)+1) )
#header = ["account","lance","dv","de   sc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who","PK"]
dataf = dataf[header]
# make into a list of lists
trans =  dataf.values.tolist()
# better to use append rows all at once than loop, get error about too many updates in short time from google
worksheet.insert_rows(trans,row=2,value_input_option='USER_ENTERED')
# using the new sequential index values + 1 (to start from 1 instead of 0)
# or append_row
worksheet.sort((1, 'des'), (0, 'asc'))
