import datetime
import pandas as pd
from decimal import Decimal
import re


# this creates a file to import into YNAB converting all transactions to USD based on the ex rate at the last deposit 
def readMil(filename):
    # when your UI is in English, and you downloaad a file, it's UTF-8 and Debit, Credit
    # when your UI is Portuguese, you get UTF-16-be and Débito and Crédito with accents
    pathf = "/Users/cmcnally/Dropbox/python/textfiles/" + filename
    if filename == "AUG-2021-2.csv" or filename == "SEPT-2021-2.csv" or filename == "JULY-2021-2.csv":
        return pd.read_csv(pathf, delimiter=";", engine='python',header=None,
        names=["lance","dv","desc","amount","type","balance"],   parse_dates=['dv','lance'], dayfirst=True) 
    else:
        return pd.read_csv(pathf, delimiter=";", engine='python', skiprows=13, skipfooter=1, encoding="utf_16_le",header=None,
        names=["lance","dv","desc","amount","type","balance"], parse_dates=['dv','lance'], dayfirst=True)

def getUSDForCredit(item = None, defaultRate=Decimal("1.2")):
    euros = item["amount"]
    #print(item)
    for exch in exchanges:
        if exch["teuro"] == euros or abs(exch["teuro"] - euros) < 0.009: 
            usd = euros * exch["erate"] 
            if abs(usd - exch["tusd"]) > 2: # check my rates for mistakes
                print("error on exchange rate on euros " , euros , " rate should be " , exch["tusd"] / exch["teuro"] )
            return exch["tusd"]
    print("no exchange rate found for credit ", item["desc"], " for ", euros , " rate default " , defaultRate )
    return Decimal(defaultRate) * euros,

def append_some_credits(dataframe1):
    data_start = datetime.datetime(2021, 8, 3)
    balance = Decimal(0.0)
    prepend = pd.DataFrame(columns = ["lance","dv","desc","amount","type","balance"])
    i = 0
    for credits in exchanges:
        if credits["tdate"] < data_start:
            balance = balance + credits["teuro"] 
            prepend.loc[i] = [credits["tdate"], credits["tdate"], "transferwise",credits["teuro"], "Credit", float(balance)   ]
            i = i + 1
    data = [prepend, dataframe1]
    df2 = pd.concat(data)        
    return df2, balance    

def getRateApplyDebit( amount = None, item = None):
    #amounts are negative for debits
    rate = Decimal(0.0)
    if item is not None:
        amount = item["amount"]
    for credit in exchanges:
        if credit["teuro"] > 0 and  credit["teuro"] >= abs(amount):
            credit["teuro"] = credit["teuro"] + amount
            if rate == Decimal(0.0):
                rate = credit["erate"]
            return rate
        elif credit["teuro"] > 0:
            if credit["teuro"] >= abs(amount / 2):
                rate = credit["erate"]
            amount = amount + credit["teuro"] 
            credit["teuro"] = 0
    if rate == Decimal(0.0):
        # this is an error, we've run out of money some credits are missing
        print("OUT OF MONEY ERROR on ", item)

def createMemo(item):
    memo = "{:.2f} euros at {:.4f}".format(item["amount"],item["erate"])
    if item["desc"].startswith("COMPRA 7455 "):
        item["desc"] = item["desc"][12:]
        memo = memo + " CM"
    elif item["desc"].startswith("COMPRA 0885 "):
        item["desc"] = item["desc"][12:]
        memo = memo + " EHT"
    elif item["desc"].startswith("LEV ATM 0885"):
        memo = memo + " EHT"
    elif item["desc"].startswith("LEV ATM 7455"):
        memo = memo + " CM"
    return memo
# COMPRA 0885 IS HELLA
# COMPRA 7455 IS CHRIS
# can ignore Credito in type column df["type"].unique() for values 37,700
exchanges = tuple(
({"tdate":datetime.datetime(2021,4,26), "tusd":Decimal(305.48), "teuro":Decimal(250),"erate":Decimal(1.2219)}, # usd / euro is the rate
{"tdate":datetime.datetime(2021,5,10), "tusd":Decimal(15000), "teuro":Decimal("12341.33"), "erate":Decimal(1.2154)},
{"tdate":datetime.datetime(2021,5,11), "tusd":Decimal(7000), "teuro":Decimal("5721.07"), "erate":Decimal(1.2235)}, 
{"tdate":datetime.datetime(2021,5,12), "tusd":Decimal(5000), "teuro":Decimal("4090.16"), "erate":Decimal(1.2224)}, 
{"tdate":datetime.datetime(2021,9,22), "tusd":Decimal(8500), "teuro":Decimal("7202.83"), "erate":Decimal(1.1800)}, 
{"tdate":datetime.datetime(2021,9,30), "tusd":Decimal(10000), "teuro":Decimal("8585.33"), "erate":Decimal(1.1647)}, 
{"tdate":datetime.datetime(2021,12,9), "tusd":Decimal(8250), "teuro":Decimal("7074.83"), "erate":Decimal(1.1662)}, 
{"tdate":datetime.datetime(2022,1,6), "tusd":Decimal(600), "teuro":Decimal("500.00"), "erate":Decimal(1.200)},
{"tdate":datetime.datetime(2022,1,31), "tusd":Decimal(360), "teuro":Decimal("300"), "erate":Decimal(1.200)},
{"tdate":datetime.datetime(2022,3,7), "tusd":Decimal(5000), "teuro":Decimal("4560.08"), "erate":Decimal(1.0964)},
{"tdate":datetime.datetime(2022,5,2), "tusd":Decimal(5000), "teuro":Decimal("4719.86"), "erate":Decimal(1.0593)},
{"tdate":datetime.datetime(2022,5,10), "tusd":Decimal(5000), "teuro":Decimal("4719.12"), "erate":Decimal(1.0595)},
{"tdate":datetime.datetime(2022,6,8), "tusd":Decimal(5000), "teuro":Decimal("4641.83"), "erate":Decimal(1.0772)},
{"tdate":datetime.datetime(2022,7,6), "tusd":Decimal(5000), "teuro":Decimal("4840.89"), "erate":Decimal(1.0328)},
{"tdate":datetime.datetime(2022,7,12), "tusd":Decimal(5000), "teuro":Decimal("4953.21"), "erate":Decimal(1.0094)},
{"tdate":datetime.datetime(2022,7,20), "tusd":Decimal(6000), "teuro":Decimal("5821.69"), "erate":Decimal(1.0306)},
{"tdate":datetime.datetime(2022,8,25), "tusd":Decimal(6000), "teuro":Decimal("5972.79"), "erate":Decimal(1.0045)},
{"tdate":datetime.datetime(2022,11,2), "tusd":Decimal(5000), "teuro":Decimal("5021.67"), "erate":Decimal(0.9956)},
{"tdate":datetime.datetime(2023,1,1), "tusd":Decimal(3000), "teuro":Decimal("2783.63"), "erate":Decimal(1.0777)},
{"tdate":datetime.datetime(2023,1,13), "tusd":Decimal(5000), "teuro":Decimal("4589.38"), "erate":Decimal(1.0894)},
{"tdate":datetime.datetime(2023,2,24), "tusd":Decimal(2000), "teuro":Decimal("1875.72"), "erate":Decimal(1.0662)},
{"tdate":datetime.datetime(2023,3,22), "tusd":Decimal(5000), "teuro":Decimal("4602.54"), "erate":Decimal(1.0863)},
{"tdate":datetime.datetime(2023,4,18), "tusd":Decimal(5000), "teuro":Decimal("4526.06"), "erate":Decimal(1.1047)},
{"tdate":datetime.datetime(2023,5,11), "tusd":Decimal(5000), "teuro":Decimal("4546.44"), "erate":Decimal(1.0998)},
{"tdate":datetime.datetime(2023,6,5), "tusd":Decimal(5000), "teuro":Decimal("4647.79"), "erate":Decimal(1.0758)},
{"tdate":datetime.datetime(2023,7,4), "tusd":Decimal(4000), "teuro":Decimal("3654.12"), "erate":Decimal(1.0946)},
{"tdate":datetime.datetime(2023,7,31), "tusd":Decimal(2000), "teuro":Decimal("1810.46"), "erate":Decimal(1.10469162)},
{"tdate":datetime.datetime(2023,8,8), "tusd":Decimal(5000), "teuro":Decimal("4523.08"), "erate":Decimal(1.1054)},
{"tdate":datetime.datetime(2023,8,21), "tusd":Decimal(7000), "teuro":Decimal("6376.03"), "erate":Decimal(1.09786)},
{"tdate":datetime.datetime(2023,10,9), "tusd":Decimal(3000), "teuro":Decimal("2831.06"), "erate":Decimal(1.1059)},
)
)
files = ("JULY-2021-2.csv",
        "AUG-2021-2.csv",
        "SEPT-2021-2.csv",
        "OCT-2021.csv",
        "NOV-2021.csv",
        "DEC-2021.csv",
        "JAN-2022.csv",
        "FEB-2022.csv",
        "MARCH-2022.csv",
        "APRIL-2022.csv",
        "MAY-2022.csv",
        "JUNE-2022.csv",
        "JULY-2022.csv",
        "AUG-2022.csv",
        "SEPT-2022.csv",
        "OCT-2022.csv",
        "NOV-2022.csv",
        "DEC-2022.csv","JAN-2023.csv","FEB-2023.csv","MARCH-2023.csv","APRIL-2023.csv","MAY-2023.csv",
        "JUNE-2023.csv","JULY-2023.csv","AUG-2023.csv","SEPT-2023.csv","OCT-2023.csv")
files = ("Portugues-bank-2024-12.csv",
"Portugues-bank-2025-01.csv","Portugues-bank-2025-02.csv",
"Portugues-bank-2025-03.csv","Portugues-bank-2025-04.csv",
"Portugues-bank-2025-05.csv","Portugues-bank-2025-06.csv",
"Portugues-bank-2025-07.csv","Portugues-bank-2025-08.csv",
"Portugues-bank-2025-09.csv","Portugues-bank-2025-10.csv",
"Portugues-bank-2025-11.csv")
# make a panda dataframe from all the files
dataf = pd.concat(map(readMil,files),axis = 0, ignore_index=True)
#balance = Decimal(0.0)

#dataf, balance  = append_some_credits(dataf)

dataf = dataf.sort_values(by=['lance', 'balance'], ascending=[True,False])
#print(dataf.sort_values(by=["lance","dv"]).head(10))
# add extra columns for the rate and usd
usds =  [Decimal(0)] * dataf["amount"].count() #make array of count() zeros
dataf["usd"] = usds
rates =  [Decimal(0)] * dataf["amount"].count()
dataf["erate"] = rates
memos = [""] * dataf["amount"].count() #was conversion rate
dataf["memo"] = memos
dataf['amount'] = dataf.amount.apply(lambda x :round( Decimal(x),2)) # make amount a decimal instead of float
category = [""] * dataf["amount"].count()
dataf["category"] = category
subcat = [""] * dataf["amount"].count()
dataf["subcat"] = subcat
who = [""] * dataf["amount"].count()
frags = [""] * dataf["amount"].count()
dataf["fragment"] = frags
newt = ["x"] * dataf["amount"].count()
dataf["newt"] = newt

dataf["who"] = who
# convert Debito to D (in a new column newt)
dataf.loc[dataf.type == "Débito",'newt'] = "D"
dataf.loc[dataf.type == "Crédito",'newt'] = "C"
#for index, item in dataf.iterrows(): # somehow this does not work, try iterrows()
#    if (item["type"].startswith("D")):
#        item["newt"]= "D" 
#    elif (item["type"].startswith("C")):
#        item["newt"]= "C" 
#    else:
#        item["newt"] = " "

header = ["lance","dv","desc","amount","newt","balance","usd","erate","memo","category","subcat","fragment","who"]
dataf.to_csv("/Users/cmcnally/Dropbox/python/textfiles/uncategorized-mil-2025.csv", index=False,encoding="ascii", columns=header)


#the following was before we had all months, now we have data from the begingin
startingBalance = Decimal(38190.71) #sum of all transfers in prior to 10/1/2021=
balance10_1 = Decimal(25004.90) # based on the downloaded transactions, balance was on 10/1
alreadyUsed = Decimal(-13185.80) # this needs to be removed first from the tuple
# don't have to do this when we have all months and we do rate = getRateApplyDebit(amount=alreadyUsed)
droppings = []
rate = Decimal("1.2")
dataf.sort_values(by=["lance","dv","balance"], ascending=[True, True, False], inplace=True)
for index, item in dataf.iterrows():
    if item["type"].startswith("D"):
        if item["amount"] > 1.0:
            item["amount"] = Decimal(0.0) - item["amount"]
        rate = getRateApplyDebit(None, item)
        item["erate"] = rate
        item["usd"] = item["amount"] * rate
        item["memo"] = createMemo(item)
        dataf.loc[index] = item
    elif item["type"].startswith("Cr"):
        usd = getUSDForCredit(item,rate) # default to the last rate if we don't have it
        item["usd"] = usd
        item["erate"] = usd / item["amount"]
        item["memo"] = createMemo(item)
        dataf.loc[index] = item
    else:
        print(item)
        droppings.append(index) #these are being removed, what are they 
dataf['usd'] = dataf.usd.apply(lambda x : float(x)) #why? oh for to_csv which allows a float format
dataf = dataf.drop(droppings)
dataf.sort_values(by=["lance","dv","balance"], ascending=[True, True, False]).to_csv('/Users/cmcnally/Downloads/all-months-with-usd.csv')
dataf.to_csv(float_format="%.2f",columns=["dv","desc","memo","usd"],header=["Date","Payee","Memo","Amount"],
index_label=False,index=False,path_or_buf="/Users/cmcnally/Downloads/all-months-for-ynab.csv")

#usds = list(range(0,dataf["amount"].count()))
#dataf.info()
#dataf.head(10)
#Date,Payee,Memo,Outflow,Inflow
#def applyRateFixMemo(dfr)
