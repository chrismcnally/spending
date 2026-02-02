
import json
import os
from datetime import datetime
import time
import csv
import datetime
from decimal import Decimal
from operator import itemgetter

# this is the transaction updater, adds categories, uses the file categories_updated.json 
# which is a custom categories files, no longer YNAB, it has the field payee_includes with a 
# list of payees by partial name to fil in the categories for transactions. The results are written to a csv file

chris_cash_trans_payee = "6d265ca4-c546-42aa-8f0a-5aa7f4f5dda4"
hella_cash_trans_payee = "842c01e5-6259-45ea-bf69-c623adb48132"

amazon_off = True  # means we are not loading each amazon transaction, just going off YNAB already categorized, only for 2021 - 2023
# for 2024 and beyond, using amazon files, we set this to false
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

# very old code to add categories, no longer running these as scripts everything called from read-cat.py
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



work = [
  {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-mil-2025.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-mil-2025.csv",
    "do_atm": True,
    "do_cats": True,
    "process": False,
    "writeFile": True
  },
  {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-mil-2024-fixed.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-mil-2024.csv",
    "do_atm": True,
    "do_cats": True,
    "process": False,
    "writeFile": True
  },
  {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-chase-2024-2025.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-chase-2024-2025.csv",
    "do_atm": False,
    "do_cats": True,
    "process": False,
    "writeFile": True
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
    "do_cats": True,
    "do_atm": False,
    "process": False,
    "writeFile": True
  },
   {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/test_chase-2023-2021.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/test_chase-2023-2021-categorized.csv",
    "do_cats": True,
    "do_atm": False,
    "process": False,
    "writeFile": True
  },
     {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/test_ally-bank-2023-2021.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/test_ally-bank-2023-2021-categorized.csv",
    "do_cats": True,
    "do_atm": False,
    "process": False,
    "writeFile": True
  },
  # ancillary were  the end of 12/2025 and 11 and 12 from 2023 which I found, all from Millenium
    {
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-ancillary-mixed.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-ancillary-mixed.csv",
    "do_atm": False,
    "do_cats": True,
    "process": False,
    "writeFile": True
  },
   { # these were transactions from 2024 milenium pdf reads that were skipped by the pdf reader
    "infile": "/Users/cmcnally/Dropbox/python/textfiles/converted_FIXED-milen-2024.csv",
    "outfile": "/Users/cmcnally/Dropbox/python/textfiles/categorized-converted_FIXED-milen-2024.csv",
    "do_atm": False,
    "do_cats": True,
    "process": False,
    "writeFile": True,
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
    "process": True,
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

    all_trans =[]        
    for w in work:
        if (w["process"]):
            transactions = load_csv_trans(w["infile"])
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