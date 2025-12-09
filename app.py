from decimal import Decimal,ROUND_HALF_UP
from shiny import reactive
import json
from shiny.express import input
from shiny.express import render, ui
from shiny import App, reactive
import csv
import pandas as pd
import matplotlib.pyplot as plt


def load_categorized_trans():
#    trans =  pd.read_csv("textfiles/categorized-all-2024-2025.csv")
    trans = pd.read_csv("https://raw.githubusercontent.com/chrismcnally/spending/refs/heads/master/textfiles/categorized-all-2024-2025.csv")
    trans = trans.loc[trans.newt == "D"] # we currently have D, C, P, and F
    trans['category'] = trans['category'].fillna("Unknown") # mark the unknown category
    trans.sort_values(by=["lance","dv","balance"], ascending=[True, True, False], inplace=True)
    trans.drop(columns=["balance","erate","fragment","who"], inplace=True)
    all_dates = trans["lance"] #gather all dates. this is a list of the values in that column
    year_months = [x[0:4] + x[5:7] for x in all_dates]  
    years =  [x[0:4]  for x in all_dates]
    testdates = [x[0:4] + "-" +  x[5:7] + "-01" for x in all_dates]  
    trans["year"] = years
    trans["year_month"] = year_months
    trans["testdate"] = testdates
    trans['amount'] = trans['amount'].apply(lambda x: Decimal(x).quantize(Decimal("0.01"),rounding=ROUND_HALF_UP) )

   # trans['amount'] = trans['amount'].astype(float)
 
    return  trans.sort_values(by=['year', 'year_month','category'])   
    

def make_summary_table(trans):
#    return trans.groupby([pd.Grouper(key='Category', freq='ME')])['amount'].sum()
    summary = trans.groupby(['category','year', 'year_month','testdate'])['amount'].sum()
    return summary.reset_index()


trans = load_categorized_trans()
summary = make_summary_table(trans)
ui.h2("All Transactions")
with ui.sidebar():
    ui.input_selectize("input_year", "Years", choices=("All Years","Date Range","2025","2024","2023"), selected="All Years"),
    ui.input_date_range("inDateRange", "Input date", start="2024-11-01", end="2025-11-30")
    ui.input_radio_buttons("months_or_years","Summarize by:",["Year","Month"],selected = ["Month"])
    ui.input_radio_buttons("sort_by","Sort by:",["amount","Date","category"],selected = ["category"])

# looks like I am using shiny express here, refering to input.inDateRange() without specifying a server section
@reactive.calc
def get_summary():
    dates = input.inDateRange()
    year = input.input_year()
    sum_by = input.months_or_years()
    sort = input.sort_by()
    asc = True
    if (sum_by == "Month"):
        if (sort =="Date"):
            sort = "year_month"
        summary = trans.groupby(['category','year', 'year_month','testdate'])['amount'].sum().reset_index()
    else:   
        if (sort =="Date"):
            sort = "year"
        summary = trans.groupby(['category','year'])['amount'].sum().reset_index()
    summary = summary.sort_values(by=[sort,"category"],ascending=asc)
    if (year == "All Years"):
        return summary.round({'amount': 2, 'usd': 2})
    elif (year == "Date Range"):
        if (sum_by == "Month"):
            qstr = "testdate >= '" + dates[0].isoformat() + "' and testdate <= '" + dates[1].isoformat() + "'"
            summary = summary.query(qstr)
            return summary.round({'amount': 2, 'usd': 2})
        else:
            qstr = "testdate >= '" + dates[0].isoformat() + "' and testdate <= '" + dates[1].isoformat() + "'"
            summary = trans.query(qstr)     
            summary = summary.groupby(['category','year'])['amount'].sum().reset_index()   
            return summary.round({'amount': 2, 'usd': 2})
    else: #user chose a single year to filter by
        qstr = "year == '" + year  + "'"
        summary = summary.query(qstr)
#        output_file(summary,year)
        return summary.round({'amount': 2, 'usd': 2})


#def output_file(sumt,year):
   #  sumt.to_csv("/Users/cmcnally/Dropbox/python/textfiles/sorted" + year + ".csv", index=False)


@reactive.calc
def get_trans():
   # dates = input.inDateRange()
   # qstr = "lance >= '" + dates[0].isoformat() + "' and lance <= '" + dates[1].isoformat() + "'"
   # return trans.query(qstr).round({'amount': 2, 'usd': 2})
     return trans.round({'amount': 2, 'usd': 2})


@render.data_frame
def summary_df():
    #round(2) would work better if the column was already a decimal. 
    return render.DataGrid(get_summary(), filters=True, selection_mode="rows")

@render.data_frame
def transactions_df():
    return render.DataGrid(filtered_df(), filters=True)  

@reactive.calc
def filtered_df():
    # When a summary rows is selected use it as a filter, otherwise, render the detail rows normally 
    data_selected = summary_df.data_view(selected=True)
    if ( data_selected.empty):
        return get_trans()
    else:
        category = data_selected["category"].to_numpy()[0]
        category = category.replace("'","\\'",1)
        by = input.months_or_years()
        qstr1 = ""
        if (by == "Month"): 
            year_month = data_selected["year_month"].to_numpy()[0]
            qstr1 = f"category ==  '{category}' and year_month == '{year_month}'"
        else:
            year_month = data_selected["year"].to_numpy()[0]
            qstr1 = f"category ==  '{category}' and year == '{year_month}'"
        # Filter data for selected category and dates
        return get_trans().query(qstr1)
    
@render.plot
def my_scatter():
    qstr = "year == '2025'"
    summary = get_trans().query(qstr)
#    amounts = summary.amount
#    negs = [abs(Decimal(x)) for x in amounts]
#    summary["negs"] = negs
    summary.amount = summary.amount.apply(lambda negamt : abs(round( Decimal(negamt),2))) # pie positive numbers only
    summary = summary.groupby(['category','year'])['amount'].sum().reset_index()
    # reshape the data with pivot table and aggregate the mean this produces one column for every category, ugg
    #dfp = summary.pivot_table(index='year', columns='category', values='amount', aggfunc='sum')
    top10 = summary.sort_values(by=["year","amount","category"],ascending=[True,False,True]).iloc[:15]
    return plt.pie(x = top10.amount, labels = top10.category)
