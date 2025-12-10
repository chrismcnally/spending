
import json
import os
from datetime import datetime
import time
import csv
import datetime
from decimal import Decimal

# this is the transaction updater, adds categories, uses the file categories_updated.json 
# which is a custom categories files, no longer YNAB, it has the field payee_includes with a 
# list of payees by partial name to fil in the categories for transactions. The results are written to a csv file

chris_cash_trans_payee = "6d265ca4-c546-42aa-8f0a-5aa7f4f5dda4"
hella_cash_trans_payee = "842c01e5-6259-45ea-bf69-c623adb48132"


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
            

def add_categories(transactions, categories):
    # lance,dv,desc,amount,type,balance,usd,erate,memo,category,subcat,fragment,who
    amazon = "Amazon -- already categorized individulally"
    for trans in transactions: #payees.values:
        #print("looking for payee {} ; on {} ; {} ; by {} ".format( trans["desc"],trans["lance"],trans["amount"], trans["memo"]))
        #if not apply_special_dates(trans):
        for cat in categories:
            for name_fragment in cat["payee_contains"]:
                if name_fragment in trans["desc"]:
                    trans["category"] = cat["name"]
                    trans["fragment"] = name_fragment
                    if "sub-category" in cat:
                        sub = cat["sub-category"]
                        if name_fragment in sub:
                            trans["memo"] = sub[name_fragment]
                            trans["subcat"] = sub[name_fragment]
                    if ("who" in cat):
                        trans["who"] = cat["who"]
                    if (cat["name"]) == amazon:
                        trans["amount"] = Decimal(0.00)
                        trans["usd"] = Decimal(0.00)
                    update_category(name_fragment,cat,trans["amount"])
                    #print("applied category ",trans["category"])
                    break
            if trans["category"] is not None and len(trans["category"]) > 1:
                break
        if len(trans["category"]) < 1:
            print("no category for payee {} ; on {} ; {} ; by {} ".format( trans["desc"],trans["lance"],trans["amount"], trans["memo"]))
     
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
        

def load_csv_trans(infile):
    with open(infile,"r",encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        trans = list(reader)
    return trans

def write_updated_transactions(trans,outfile):
    trabs = sorted(trans, key=lambda x: (x['lance'], x['balance']))
#    trans = trans.sort_values(by=["lance","balance","category"])
    header = ["lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
    with open(outfile, 'w', newline='') as csvfile:
       writer = csv.DictWriter(csvfile, fieldnames=header)
       writer.writeheader()
       writer.writerows(trans)


def update_categories_file(catlist):
    #Write the updated categories (and weights) back to the categories_udated.json file
    catFile = "categories_updated.json"
    with open(catFile, "w") as jFile:
            json.dump(catlist, jFile)

def load_updated_categories(reset=False):
    cats = []
    with open('categories_updated.json', 'r') as file:
        cats =  json.load(file)
    if reset:
        for cat in cats:
            cat["fragment_weights"] = {}
            cat["weight"] = 0
    return cats


# this parsed the old YNAB file, categories.json, still have it but not used, categories-updated.json is the new source
def parse_categories_with_contains(catJson):
    categories = []
    groups = catJson["data"]["category_groups"]
    for cg in groups:
        #print (cg["id"],cg["name"])
        for cats in cg["categories"]:
            # skip some categories used by ynab but not me
            if  "payee_contains" in cats  and  cats["payee_contains"] is not None:
                weight = 1
                frags = {}
                if  ("weight" in cats):
                    weight = cats["weight"]
                if  ("fragment_weights" in cats):
                    frags = cats["fragment_weights"]
                categories.append({"id":cats["id"], 
                                    "name": cats["name"],
                                    "group_id": cats["category_group_id"], 
                                    "group_name":cg["name"],
                                    "payee_contains" : cats["payee_contains"],
                                    "weight" : weight,
                                    "fragment_weights" : frags
                                    })
    return categories

def make_search_structures():
    # sort the categories by category weight so the top categories are first. all this optimization
    # is not needed because it runs fast already
    category_list.sort(reverse=True,key=lambda cat: cat["weight"])
    # when we have some data, find the most popular keys, make a list of the top 10 or 25 and check those first
    # could also do a hashmap by value, ie all 80 eros are Nuno, all 45 are iris, 

transactions = []
amount_to_category = {}
category_list = load_updated_categories(True)  
work = [{"infile" : "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-2025.csv",
         "outfile" : "/Users/cmcnally/Dropbox/python/textfiles/categorized-mil-2025.csv",
         "do_atm" : True, "writeFile": True},
         {"infile" : "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-chase-2024-2025.csv",
         "outfile" : "/Users/cmcnally/Dropbox/python/textfiles/categorized-chase-2024-2025.csv",
         "do_atm" : False, "writeFile": True},
          {"infile" : "/Users/cmcnally/Dropbox/python/textfiles/uncategorized-mil-2024.csv",
         "outfile" : "/Users/cmcnally/Dropbox/python/textfiles/categorized-mil-2024.csv",
         "do_atm" : True, "writeFile":True}
         ]
all_trans =[]        
for w in work:
    transactions = load_csv_trans(w["infile"])
    transactions = add_categories(transactions, category_list)
    if w["do_atm"]:
        deal_with_atm(transactions) #this adjusts hellas atms for clara
    if w["writeFile"]:
        write_updated_transactions(transactions,w["outfile"])
    all_trans.extend(transactions)
update_categories_file(category_list)

#do all of them
write_updated_transactions(all_trans,"/Users/cmcnally/Dropbox/python/textfiles/categorized-all-2024-2025.csv")