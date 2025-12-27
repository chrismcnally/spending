
import datetime
import pandas as pd
from decimal import Decimal
import numpy as np

def read_chase(filename):
    pathf = "/Users/cmcnally/Dropbox/python/textfiles/" + filename
    return pd.read_csv(pathf, delimiter=",", engine='python',header=None, skiprows=1,
        names=["lance","dv","desc","junk","type","usd","junk2"],   parse_dates=['dv','lance'], dayfirst=False) 

def read_amazon_euros(filename):
    header = ["junk1","junk2","lance","junk3","junk4","junk5","junk6","junk7","junk8","amount","junk99","junk9","junk10","junk11","junk12","junk13","junk14","junk15","dv","junk17","junk18","junk19","category","desc","Gift_Message","Gift_Sender","Gift_Recipien","Item_Serial"]
    pathf = "/Users/cmcnally/Dropbox/python/textfiles/" + filename
    return pd.read_csv( pathf, delimiter=",", engine='python',header=None, skiprows=1, names=header,   parse_dates=['lance'], dayfirst=False) 
 
def read_amazon_usd(filename):
    header = ["junk1","junk2","lance","junk3","junk4","junk5","junk6","junk7","junk8","amount","junk88","junk9","junk10","junk11","junk12","junk13","junk14","junk15","dv","junk17","junk18","junk19","category","desc","Gift_Message","Gift_Sender","Gift_Recipien","Item_Serial"]
    pathf = "/Users/cmcnally/Dropbox/python/textfiles/" + filename
    return pd.read_csv( pathf, delimiter=",", engine='python',header=None, skiprows=1, names=header,   parse_dates=['lance'], dayfirst=False) 

def add_usd_other_fields(dataf):
    dataf['amount'] = dataf.amount.apply(lambda x :round( Decimal(x),2)*Decimal(-1.0)) # make amount a decimal instead of float and negative
    dataf["balance"] = 0
    dataf["erate"] = Decimal(0)
    dataf["memo"] = ""
    dataf["usd"] = Decimal(0)
    # fix this to convert to euros
    dataf["subcat"] = ""
    dataf["fragment"] = ""
    dataf["newt"] = "D"
    dataf["who"] = ""
    dataf["dv"] = dataf.dv.apply(lambda x : x[0:10] ) 
    dataf["year_month"] = dataf.lance.apply(lambda x : x.date().strftime("%Y%m"))
    dataf["erate"] = dataf.year_month.apply(lambda key :  monthly_ex_rates[key][0]  )
    dataf['usd'] = dataf.amount.apply(lambda usdol : round( Decimal(usdol),2)) # copy euro into usd  column

    # for all the conversion factors we have, apply against USD to get Euros, store in the Amount field
    for key in monthly_ex_rates:
        dataf.loc[dataf.year_month == key, 'usd'] *= Decimal(monthly_ex_rates[key][0])
  

    return dataf

def add_euro_other_fields(dataf):
    dataf['usd'] = dataf.amount.apply(lambda x :round( Decimal(x),2)*Decimal(-1.0)) # make amount a decimal instead of float and negative
    dataf["balance"] = 0
    dataf["erate"] = Decimal(0)
    dataf["memo"] = ""
    dataf["amount"] = Decimal(0)
    # fix this to convert to euros
    dataf["subcat"] = ""
    dataf["fragment"] = ""
    dataf["newt"] = "D"
    dataf["who"] = ""

    dataf["dv"] = dataf.dv.apply(lambda x : x[0:10])
    dataf["year_month"] = dataf.lance.apply(lambda x : x.date().strftime("%Y%m"))
    dataf["erate"] = dataf.year_month.apply(lambda key :  monthly_ex_rates[key][0]  )
    dataf['amount'] = dataf.usd.apply(lambda usdol : round( Decimal(usdol),2)) # copy usd into euro  column

    # for all the conversion factors we have, apply against USD to get Euros, store in the Amount field
    for key in monthly_ex_rates:
        dataf.loc[dataf.year_month == key, 'amount'] *= Decimal(monthly_ex_rates[key][1])
  

    return dataf

monthly_ex_rates = {   
#1 euro is (e to u) 1 dollar is (u to e)
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
"202512" :  (1.168906, 0.85552)
}
# convert ctype to newt (D or C)
# convert amount to Euros (from usd)

files = ["Amazon-2024-2025-eruos.csv","Amazon-euro-dec-2025.csv"]
euros = pd.concat(map(read_amazon_euros,files), axis = 0, ignore_index=True)
euros = euros[['lance', 'dv', 'desc','amount','category']].copy()
euros = add_usd_other_fields(euros)

file = "Amazon-2024-2025-usd.csv"
usds = read_amazon_usd(file)
usds = usds[['lance', 'dv', 'desc','amount','category']].copy()
usds = add_euro_other_fields(usds)

trans = pd.concat([euros,usds])
header = ["lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
trans.to_csv("/Users/cmcnally/Dropbox/python/textfiles/categorized-amazon-2024-2025.csv", index=False, columns=header,date_format='%Y-%m-%d')

files = ("Chase_Activity-2024.csv","Chase_Activity-2025.csv","Chase-12-2025.csv","atms-from-schwab-2024-2025.csv")
dataf = pd.concat(map(read_chase,files),axis = 0, ignore_index=True)
dataf = dataf.sort_values(by=['lance', 'dv'], ascending=[True,True])

amounts =  [Decimal(0)] * dataf["usd"].count() #make array of count() zeros
dataf["amount"] = amounts
balances =  [Decimal(0)] * dataf["usd"].count() #make array of count() zeros
dataf["balance"] = balances
rates =  [Decimal(0)] * dataf["usd"].count()
dataf["erate"] = rates
memos = [""] * dataf["usd"].count() 
dataf["memo"] = memos
# fix this to convert to euros
dataf['usd'] = dataf.usd.apply(lambda x :round( Decimal(x),2)) # make usd a decimal instead of float
category = [""] * dataf["usd"].count()
dataf["category"] = category
subcat = [""] * dataf["usd"].count()
dataf["subcat"] = subcat
frags = [""] * dataf["usd"].count()
dataf["fragment"] = frags
newt = ["x"] * dataf["usd"].count()
dataf["newt"] = newt
who = [""] * dataf["usd"].count()
dataf["who"] = who
# convert Debito to D (in a new column newt)
dataf.loc[dataf.type == "Sale",'newt'] = "D"
dataf.loc[dataf.type == "Fee",'newt'] = "D"
dataf.loc[dataf.type == "Adjustment",'newt'] = "C"
dataf.loc[dataf.type == "Payment",'newt'] = "P"
dataf.loc[dataf.type == "Return",'newt'] = "C"

all_dates = dataf["lance"] 
year_months = [x.date().strftime("%Y%m") for x in all_dates]  
dataf["year_month"] = year_months
erates = [ monthly_ex_rates[key][0] for key in year_months]  # store conversion from euro to usd in erate
dataf["erate"] = erates
# for all the conversion factors we have, apply against USD to get Euros, store in the Amount field
dataf['amount'] = dataf.usd.apply(lambda usdol : round( Decimal(usdol),2)) # copy usd into amount (euro) column
for key in monthly_ex_rates:
    dataf.loc[dataf.year_month == key, 'amount'] *= Decimal(monthly_ex_rates[key][1])
#    dataf.loc[dataf['year_month'] == key, 'amount'] = Decimal(dataf['usd'],2) * Decimal(monthly_ex_rates[key][0],4)
#    dataf['amount'] = np.where(np.equal(dataf['year_month'],key), Decimal(dataf['usd']) * Decimal(monthly_ex_rates[key][0]), dataf['amount'])

header = ["lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
dataf.to_csv("/Users/cmcnally/Dropbox/python/textfiles/uncategorized-chase-2024-2025.csv", index=False,encoding="ascii", columns=header)
