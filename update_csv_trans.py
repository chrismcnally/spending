
import json
from datetime import datetime
import time
import csv
import datetime
from decimal import Decimal
from operator import itemgetter
import gspread 
import os
import pandas as pd

# this is the transaction updater, adds categories, uses the file categories_updated.json 
# which is a custom categories files, no longer YNAB, it has the field payee_includes with a 
# list of payees by partial name to fil in the categories for transactions. The results are written to a csv file

chris_cash_trans_payee = "6d265ca4-c546-42aa-8f0a-5aa7f4f5dda4"
hella_cash_trans_payee = "842c01e5-6259-45ea-bf69-c623adb48132"
S_KEY = "1ai9nZYCNw5g5-fv0-siPryHWYwl7hSfTunq5wswYfDo"
HEADER = ["account","lance","dv","desc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who","PK"]
PK_COL = 15


amazon_off = True  # means we are not loading each amazon transaction, just going off YNAB already categorized, only for 2021 - 2023
# for 2024 and beyond, using amazon files, we set this to false
monthly_ex_rates = {   
    # 2022
    "202201": (1.130000, 0.884956),
    "202202": (1.120000, 0.892857),
    "202203": (1.110000, 0.900901),
    "202204": (1.090000, 0.917431),
    "202205": (1.070000, 0.934579),
    "202206": (1.060000, 0.943396),
    "202207": (1.020000, 0.980392),
    "202208": (1.010000, 0.990099),
    "202209": (0.990000, 1.010101),
    "202210": (0.980000, 1.020408),
    "202211": (1.030000, 0.970874),
    "202212": (1.060000, 0.943396),

        # 2023
    "202301": (1.080000, 0.925926),
    "202302": (1.070000, 0.934579),
    "202303": (1.090000, 0.917431),
    "202304": (1.100000, 0.909091),
    "202305": (1.080000, 0.925926),
    "202306": (1.090000, 0.917431),
    "202307": (1.110000, 0.900901),
    "202308": (1.090000, 0.917431),
    "202309": (1.060000, 0.943396),
    "202310": (1.060000, 0.943396),
    "202311": (1.090000, 0.917431),
    "202312": (1.100000, 0.909091),
"202401" :	(1.091126, 0.916507 ),
"202402" :	(1.079318, 0.926522 ),
"202403" : 	(1.087005, 0.919976 ),
"202404" :	(1.072285, 0.932636 ),
"202405" : 	(1.081664, 0.924524 ),
"202406" : 	(1.076247, 0.92919 ),
"202407" :	(1.085428, 0.921315 ),
"202408" : 	(1.102099, 0.907444 ),
"202409" :	(1.110919, 0.900173 ), 
"202410" : 	(1.089561, 0.91785 ),
"202411" : 	(1.062408, 0.94144 ),
"202412" : (1.047744, 0.954469 ),
"202501" :	(1.035875, 0.965434 ),
"202502" :	(1.041557, 0.960138 ),
"202503" : 	(1.08043, 0.925734 ),
"202504" :	(1.123274, 0.890543 ),
"202505" : 	(1.127774, 0.886745 ),
"202506" : 	(1.15293, 0.867441 ),
"202507" :	(1.168078, 0.856157 ),
"202508" : 	(1.165494, 0.858019 ),
"202509" :	(1.173437, 0.852212 ),
"202510" : 	(1.164413, 0.858819 ),
"202511" : 	(1.156707, 0.864535 ),
"202512" :  (1.168906, 0.85552),
"202601" :  (1.178419, 0.848595), # 1.169317119  0.8552 our last transfer on, switch to mid transfer
"202602" :  (1.191185, 0.8395) # also from our last transfer on 2/2
}

def add_amount_if_none():
    worksheet = open_sheet()
    valarray = worksheet.get_values()
    dataf = pd.DataFrame(valarray[ 1 :], columns = HEADER) # skip the first row
    dataf['usd'] = dataf['usd'].astype(float,errors="ignore")
    dataf = dataf.loc[(dataf["amount"] == '0' ) &    (dataf["usd"] < 0) & (dataf["newt"].isin(['D','C']))].copy()
    dataf['amount'] = dataf['amount'].astype(float,errors="ignore")
    #dataf['amount'] = dataf['amount'].astype(float,errors="ignore")
    dataf["year_month"] = dataf.dv.apply(lambda x : x[0:4] + x[5:7])
    dataf["erate"] = dataf["year_month"].apply(lambda key :  monthly_ex_rates[key][0])
    dataf['erate'] = dataf['erate'].astype(float,errors="ignore")
#    dataf['usd'] = dataf.usd.apply(lambda x :round( Decimal(x),2)) # make amount a decimal instead of float and 
    dataf["amount"] = dataf["erate"].astype(float) *  dataf["usd"].astype(float)
    # we have to updat the amount fields for these 21 rows. must sleep a little to keep google from getting angry
    dataf = dataf[HEADER] # puts them in the right order
    for row in  dataf.itertuples(index=True):
        pk = row.PK
        cell = worksheet.find(pk,in_column=PK_COL)
        row_num =  cell.row
        celln = f"G{row_num}"
        print(f"will update row G{row_num}  with PK {pk} old amount 0 new amount {row.amount}")
        worksheet.update(range_name=celln, values=[[row.amount]], raw=False)
#        worksheet.update(f"A{row_num}:O{row_num}",row.values(),value_input_option='USER_ENTERED')
        time.sleep(2)

def find_unique_payees(transactions):
    payees = {}
    for trans in transactions.data.transactions:
        key = trans.payee_id 
        if not key in payees:
            payee = {"payee_id": trans.payee_id, "payee_name": trans.payee_name, 
            "category_name" : None, "Category_id" : None}
            payees[key] = payee
    return payees

def deal_with_atm(transactions):
    # first for all the weeks of these ATMs, make a 60 euro Clara transaction under categorty House Cleaning
    # reduce Hellas ATM cash by that 65, if no nearby ATM transaction, hit chris cash
    # take the remaining ATM transactions and make transactions with categories Chris Cash spending and Hellas too
    chris = "LEV ATM 6519"
    hella = "LEV ATM 0885"
    h_atms = []
    c_atms = []
    claras = []
    min_date = "2050-12-31"
    max_date = "2009-01-01"
    for trans in transactions:
        if chris in trans["desc"]:
            c_atms.append(trans)
        if hella in trans["desc"]:
            h_atms.append(trans)
        if (trans["lance"] < min_date ):
              min_date = trans["lance"]
        if (trans["lance"] > max_date ):
            max_date = trans["lance"]

    start_date = datetime.datetime.strptime(min_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(max_date, '%Y-%m-%d').date()
    weeks = (end_date - start_date).days // 7
    date_list = [start_date + datetime.timedelta(weeks=x)  for x in range(0, weeks)]
    for week in date_list:
        if not take_clara_from_cash(week,trans,h_atms):
             if not take_clara_from_cash(week,trans,c_atms):
                 print("No ATM money to play Clara on {}".format(week))

def take_clara_from_cash(week,trans,atms):
    # this is not perfect, we end up with no money for Clara 10 times out of a year. but then again she may have skipped
    # 10 weeks out of the year too, so OK for now. But an idea is to change the date and call recursively, to borrow money 
    # from an earlier withdrawal. even maybe it should be written that way from the start
    t_week = week.isoformat()
    closest_date = "2099-01-01"
    saved_idx = -1
    for index, atm in enumerate(atms):
        test_date  = atm["lance"]
        if (test_date <= t_week):
            saved_idx = index
            closest_date = test_date
        else:
            break
    if (closest_date > t_week):
        return False
    former_amount = Decimal(atms[saved_idx]["amount"])
    if former_amount > -60.0:
        return False
    trans = {"lance":t_week,"dv":t_week,"desc":"Cleaning Clara","amount":"-60.00","newt":"D","balance":"0.0",
                 "usd":"0.0","erate":"0.0","memo":"","category":"House Cleaning","subcat":"","fragment":"Cleaning Clara","who":""}
    transactions.append(trans)
    atms[saved_idx]["amount"] = str(former_amount + Decimal(60.0))
    return True

def apply_special_dates(tran):
    # we cannot do this, though It would catch gas on the road, it would clobber other known categories
    # if anything we can do it after categorization, but this can also be manual as it has been and 
    # to me it works better
    ranges = [{"what":"Sevilla","category":"Family Vacations","from":"2025-02-05","to":"2025-02-10"}]
    found = False
    for range in ranges:
        if (tran["lance"] >= range["from"] and tran["lance"] <= range["to"]):
            found = True
            tran["category"] = range["category"]
            tran["fragment"] = range["what"]
            if ("who" in range):
                tran["who"] = range["who"]
    return FileNotFoundError
            

def add_categories_df(trans, fragments = None, amazon_off=True):
    if (not fragments):
        fragments = load_fragments()

    for frag in fragments:
        
        trans.loc[trans["desc"].str.contains(frag["fragment"], case=False, na=False,regex=False),"category"] = frag["category"]
        trans.loc[trans["desc"].str.contains(frag["fragment"], case=False, na=False,regex=False),"subcat"] = frag["subcat"]
        trans.loc[trans["desc"].str.contains(frag["fragment"], case=False, na=False,regex=False),"fragment"] = frag["fragment"]

    amazon = "Amazon -- already categorized individulally"
    if (not amazon_off):
        trans.loc[trans["category"] == amazon,"amount"] = Decimal(0.00)
        trans.loc[trans["category"] == amazon,"usd"] = Decimal(0.00)
    mcount =  len(  trans[ trans["category"] == '']  )
    count =   len(trans)
    print(f"found categories for {count - mcount} rows, {mcount} rows missing categories")
    print( trans[ trans["category"] == ''][["lance","desc", "amount","usd"]])
    # report transactions that have no category?
    return trans

# very old code to add categories, use above instead now. no longer running this file as a script everything now called from read-cat.py
def add_categories(transactions, categories = None):
    if (not categories):
        categories = load_updated_categories()
    # lance,dv,desc,amount,type,balance,usd,erate,memo,category,subcat,fragment,who
    amazon = "Amazon -- already categorized individulally"
    for trans in transactions: #payees.values:
        #print("looking for payee {} ; on {} ; {} ; by {} ".format( trans["desc"],trans["lance"],trans["amount"], trans["memo"]))
        #if not apply_special_dates(trans):
        foundCat = False
        for cat in categories:
            for name_fragment in cat["payee_contains"]:
                if name_fragment in trans["desc"]:
                    if (amazon_off and cat["name"] == amazon and trans["category"] is not None and len( trans["category"] ) > 1 and cat["name"] != trans["category"]):
                        # don't override amazon we already categorized it in YNAB
                        #  trans["category"] = cat["name"]
                        trans["fragment"] = name_fragment
                    else:
                        if ( trans["category"] is not None and len( trans["category"] ) > 1 and cat["name"] != trans["category"]):
                            print("overriding category " + trans["category"] + " with " + cat["name"] + " for " + trans["desc"]) 
                        trans["category"] = cat["name"]
                        trans["fragment"] = name_fragment
                    foundCat = True
                    if "sub-category" in cat:
                        sub = cat["sub-category"]
                        if name_fragment in sub:
#                            trans["memo"] = sub[name_fragment]
                            trans["subcat"] = sub[name_fragment]
                    if ("who" in cat):
                        trans["who"] = cat["who"]
                    if (cat["name"] == amazon and not amazon_off):
                        trans["amount"] = Decimal(0.00)
                        trans["usd"] = Decimal(0.00)
                    update_category(name_fragment,cat,trans["amount"])
                    #print("applied category ",trans["category"])
                    break
            if foundCat:
                break
        # ynab files already have categories, so don't check len of category, not enough    
        if len(trans["category"]) < 1 or not foundCat:
            print("no category for desc {} ; on {} ; {} ; orig category {} ".format( trans["desc"],trans["lance"],trans["amount"], trans["category"]))
     
    return transactions

def update_category(fragment, category, amount):
        # found by fragment, give weight to this fragment so it can be searched earlier, make category more popular too
        # amount was to make a hashmap of amount to a category to test, or categories to test. a shortcut. not sure....
        if (not ("fragment_weights" in category)):
            fragment_weights = {}
            category["fragment_weights"] = fragment_weights
        fragment_weights = category["fragment_weights"]
        if (not (fragment in fragment_weights)):
            fragment_weights[fragment] =  0
        weight = fragment_weights[fragment]
        fragment_weights[fragment] = weight + 1

        if (not ( "weight" in category )):
            weight = 0
            category["weight"] = weight
        category["weight"]  = category["weight"] + 1
        

def load_fragments():
    return   load_csv_trans("frags-cats.csv")

def load_csv_trans(infile):
    with open(infile,"r",encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        trans = list(reader)
    return trans

def write_updated_transactions(trans,outfile, account=None):
    trans = sorted(trans, key=lambda x: (x['lance'], x['balance']))
    if (account == None):
        header = ["lance","dv","desc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who"]
    else:
        header = ["account","lance","dv","desc","category","memo","amount","newt","balance","usd","erate","subcat","fragment","who"]
        for t in trans:
            t["account"] = account

    with open(outfile, 'w', newline='') as csvfile:
       writer = csv.DictWriter(csvfile, fieldnames=header)
       writer.writeheader()
       writer.writerows(trans)


def update_categories_file(catlist):
    #Write the updated categories (and weights) back to the categories_udated.json file
    catFile = "categories_updated.json"
    with open(catFile, "w") as jFile:
            json.dump(catlist, jFile, indent=2)

def reformat_categories():
    fragments = []
    categories = load_updated_categories(False)
    for cat in categories:
        for name_fragment in cat["payee_contains"]:
            frag = {'fragment' : name_fragment, 'category' : cat["name"], 'weight':0}
            if ("fragment_weights" in cat and name_fragment in cat["fragment_weights"]):
                frag.update({'weight' : cat["fragment_weights"][name_fragment]})   
            if ("sub-category" in cat and name_fragment in cat["sub-category"]):
                frag.update({'subcat' : cat["sub-category"][name_fragment]})   
            fragments.append(frag)
            #{ "EVDCR" : "category" : "Hella's Hobbies and Sports", "weight":22, "subcat":"Escola Dance"}
    newlist = sorted(fragments, key=itemgetter('weight'), reverse=True)
    header = ["fragment","category","weight","subcat"]
    with open("new_fragments", 'w', newline='') as csvfile:
       writer = csv.DictWriter(csvfile, fieldnames=header)
       writer.writeheader()
       writer.writerows(newlist)

    return newlist

def load_updated_categories(reset=False):
    cats = []
    with open('categories_updated.json', 'r') as file:
        cats =  json.load(file)
    if reset:
        for cat in cats:
            cat["fragment_weights"] = {}
            cat["weight"] = 0
    return cats

def open_sheet():
    credentials =  json.loads(os.environ["SERVICE_JSON"])
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(S_KEY)
    worksheet =  sh.get_worksheet(0)
    return worksheet

def update_account(ts, account, useUSD=False,testRun=False):
    # find in the sheet any matching rows from subset. if row found in sheet and does not have an account add in the account, 
    # in the end write the whole sheet with just the acccount updated 
    source = pd.DataFrame(ts)
    worksheet = open_sheet()
    origDb = worksheet.get_values()
    origDb = pd.DataFrame(origDb[ 1 :], columns = HEADER) # skip the first row
    count = len(source)
    origDb['lance'] = origDb['lance'].astype(str)
    origDb['amount'] = origDb['amount'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    origDb['balance'] = origDb['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    source['amount'] = source['amount'].apply(lambda x: '' if x == '' else  str('%.2f' %  float(x) ))
    source['balance'] = source['balance'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
    if (useUSD):
        origDb['usd'] = origDb['usd'].apply(lambda x: '' if x == '' else str('%.2f' %  float(x) ))
        source['usd'] = source['usd'].apply(lambda x: '' if x == '' else  str('%.2f' %  float(x) ))

    # for rows in (source data) find a row int the sheet and update the account
    for row in  source.itertuples(index=True):
        index = row.Index
        date = row.lance #.strftime('%Y-%m-%d')
        #this can be done with ands instead of subsetting like this, original code only let you search the sheet by one column
        matches = origDb.loc[origDb.lance == date]
        if ("MBWAY WOO" in row.desc ):
            print("found woo, what is happening")
        if (len(matches) > 0):
            if (useUSD):
                subset = matches.loc[matches.usd == row.usd]
            else:    
                subset = matches.loc[matches.amount == row.amount]
            if (len(subset) > 0):
                anymore = subset.loc[subset.balance == row.balance]
                if (len(anymore) == 1):
                    # we have a single match
                    pk = anymore["PK"].to_numpy()[0]
                    old_acc = anymore["account"].to_numpy()[0]
                    if (testRun):
                        if (old_acc != account):
                            print(f"Found {len(anymore)} matches had account {old_acc} will change to {account} PK {pk} lance: {row.lance} {row.desc} for {row.amount} balance {row.balance}")
                    if (old_acc is None or old_acc == ""):
                        print(f"adding account {account} to PK {pk} lance: {row.lance} {row.desc} for {row.amount} balance {row.balance}")
                        origDb.loc[origDb['PK'] == pk, 'account'] = account
                else:
                    if (len(anymore) > 1):
                        print(f"too many matches for lance: {row.lance} {row.desc} for E{row.amount} ${row.usd} balance {row.balance}")
                    if (testRun):
                        pk = anymore["PK"].to_numpy()[0]
                        old_acc = anymore["account"].to_numpy()[0]
                        if (old_acc != account):
                            print(f"Found {len(anymore)} matches had account {old_acc} could change to {account}  PK {pk} lance: {row.lance} {row.desc} for {row.amount} balance {row.balance}")

    # write the new set

    rowcount = worksheet.row_count
    trans = origDb.values.tolist()
    range = f"A2:O{rowcount}"
    if (not testRun):
        worksheet.update(range_name=range,values=trans,value_input_option='USER_ENTERED')




work = [
  {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-mil-2025.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-mil-2025.csv",
    "do_atm": True,
    "do_cats": True,
    "process": False,
    "writeFile": True,
    "update_acc" : True,
    "useUSD" : False,
    "account" : "Millenium"
  },
  {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-mil-2024-fixed.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-mil-2024.csv",
    "do_atm": False,
    "do_cats": False,
    "process": False,
    "writeFile": False,
    "update_acc" : True,
    "useUSD" : False,
    "account" : "Millenium"
  },
  {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-chase-2024-2025.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-chase-2024-2025.csv",
    "do_atm": False,
    "do_cats": False,
    "process": False,
    "writeFile": False,
    "update_acc" : True,
    "useUSD" : True,
    "account" : "Chase Sapphire"
  },
  {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-amazon-2024-2025.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/alread-done.csv",
    "do_cats": False,
    "do_atm": False,
    "process": False,
    "writeFile": False
  },
    {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/test_milen-2023.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/test-milen-2023-categorized.csv",
    "do_cats": False,
    "do_atm": False,
    "process": False,
    "writeFile": False,
    "update_acc" : True,
    "useUSD" : False,
    "account" : "Millenium"
  },
   {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/test_chase-2023-2021.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/test_chase-2023-2021-categorized.csv",
    "do_cats": False,
    "do_atm": False,
    "process": False,
    "writeFile": False,
    "update_acc" : True,
    "useUSD" : True,
    "account" : "Chase Sapphire"
  },
     {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/test_ally-bank-2023-2021.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/test_ally-bank-2023-2021-categorized.csv",
    "do_cats": False,
    "do_atm": False,
    "process": False,
    "writeFile": False,
    "account": "ally-1051307708",
    "update_acc" : True,
    "useUSD" : True
  },
  # ancillary were  the end of 12/2025 and 11 and 12 from 2023 which I found, all from Millenium
    {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-ancillary-mixed.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-ancillary-mixed.csv",
    "do_atm": False,
    "do_cats": False,
    "process": False,
    "writeFile": False,
    "update_acc" : True,
    "useUSD" : False,
    "account" : "Millenium"
  },
   { # these were transactions from 2024 milenium pdf reads that were skipped by the pdf reader
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/converted_FIXED-milen-2024.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-converted_FIXED-milen-2024.csv",
    "do_atm": False,
    "do_cats": False,
    "process": False,
    "writeFile": False,
    "update_acc" : True,
    "useUSD" : False,
    "account" : "Millenium"
  },
   { 
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-mil-Portuguese-banks-2026-01-15.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized--mil-Portuguese-banks-2026-01-15.csv",
    "do_atm": False,
    "do_cats": True,
    "process": False,
    "writeFile": True,
    "account" : "Millenium"
  },
   { 
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized_Chase_Activity-20260115.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized_Chase_Activity-20260115.csv",
    "do_atm": False,
    "do_cats": True,
    "process": False,
    "writeFile": True,
    "account" : "Chase Sapphire"
  }
]


if __name__ == "__main__":
    transactions = []
    amount_to_category = {}
    reformat_categories()
    category_list = load_updated_categories(False)  
    # where is the amazon?
#    add_amount_if_none()
    all_trans =[]        
    for w in work:
        if (w["process"]):
            transactions = load_csv_trans(w["infile"])
            if w["update_acc"]:
                update_account(transactions,w["account"],w["useUSD"])
            if w["do_cats"]:
                transactions = add_categories(transactions, category_list)
            if w["do_atm"]:
                deal_with_atm(transactions) #this adjusts hellas atms for clara
            if w["writeFile"]:
                write_updated_transactions(transactions,w["outfile"],w["account"])
            all_trans.extend(transactions)
    #update_categories_file(category_list)

    #do all of them
    # write_updated_transactions(all_trans,"/Users/cmcnally/Dropbox/python/textfiles/categorized-all-2024-2025.csv")