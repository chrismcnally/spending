from decimal import Decimal,ROUND_HALF_UP
from shiny import reactive
from shiny.express import input, render, ui
import pandas as pd
from faicons import icon_svg as icon
import matplotlib.pyplot as plt
import gspread 
import os
import json

S_KEY = "1ai9nZYCNw5g5-fv0-siPryHWYwl7hSfTunq5wswYfDo"


# old code to load from the file, using a spreadsheet now
def load_categorized_trans():
    #trans =  pd.read_csv("textfiles/categorized-all-2024-2025.csv")
    trans = pd.read_csv("https://raw.githubusercontent.com/chrismcnally/spending/refs/heads/master/textfiles/categorized-all-2024-2025.csv")
    #trans = trans.loc[trans["newt"].isin(["D", "C"])] # we currently have D, C, P, I and F? 
    trans['category'] = trans['category'].fillna("Unknown") # mark the unknown category
    trans.sort_values(by=["lance","dv","balance"], ascending=[True, True, False], inplace=True)
    trans.drop(columns=["balance","dv","erate","fragment","who"], inplace=True)
    all_dates = trans["lance"] #gather all dates. this is a list of the values in that column
    year_months = [x[0:4] + x[5:7] for x in all_dates]  
    years =  [x[0:4]  for x in all_dates]
#    testdates = [x[0:4] + "-" +  x[5:7] + "-01" for x in all_dates]  
    trans["year"] = years
    trans["year_month"] = year_months
    trans["amount"]  = pd.to_numeric(trans["amount"] , errors='coerce')
    trans["amount"] = trans["amount"].astype(float)
    trans['amount'] = trans['amount'].apply(lambda x: Decimal(x).quantize(Decimal("0.01"),rounding=ROUND_HALF_UP) )

   # trans['amount'] = trans['amount'].astype(float)
 
    return  trans.sort_values(by=['year', 'year_month','category'])   

def load_trans_from_gsheet():
    # json.loads(os.environ["GSPREAD_CREDS_JSON"])
    credentials =  json.loads(os.environ["SERVICE_JSON"])
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(S_KEY)
    worksheet =  sh.get_worksheet(0)
    trans = pd.DataFrame(worksheet.get_all_records())
#    trans = trans.loc[trans["newt"].isin(["D", "C"])] # we currently have D, C, P, T and F? MAYBE I for income

    # this won't work anymore, unless we replace empty string with None
    trans["category"] = trans["category"].apply(lambda x: None if x == "" else x)
    trans['category'] = trans['category'].fillna("Unknown") # mark the unknown category

    trans.sort_values(by=["lance","dv","balance"], ascending=[True, True, False], inplace=True)
    trans.drop(columns=["balance","dv","erate","fragment","who"], inplace=True)
    all_dates = trans["lance"] #gather all dates. this is a list of the values in that column
    year_months = [x[0:4] + x[5:7] for x in all_dates]  
    years =  [x[0:4]  for x in all_dates]
#    testdates = [x[0:4] + "-" +  x[5:7] + "-01" for x in all_dates]  
    trans["year"] = years
    trans["year_month"] = year_months
    trans["amount"]  = pd.to_numeric(trans["amount"] , errors='coerce')
    trans["amount"] = trans["amount"].astype(float)
    trans['amount'] = trans['amount'].apply(lambda x: Decimal(x).quantize(Decimal("0.01"),rounding=ROUND_HALF_UP) )
    return  trans.sort_values(by=['year', 'year_month','category'])

def make_summary_table(trans):
#    return trans.groupby([pd.Grouper(key='Category', freq='ME')])['amount'].sum()
    summary = trans.groupby(['category','year', 'year_month'])['amount'].sum()
    return summary.reset_index()


#trans = load_categorized_trans()

trans = load_trans_from_gsheet()
summary = make_summary_table(trans)
with ui.sidebar():
    ui.input_selectize("input_year", "Years", choices=("All Years","Date Range","2025","2024","2023","2022","2021"), selected="All Years")
    ttypes = {"'D'":"Debit","'C'":"Credit","'T'":"Transfers","'P'":"Credit Card Payments","'I'":"Income"}
    ui.input_checkbox_group("types","Transaction Types",choices=ttypes,selected=(["'D'","'C'"]))
#    ui.input_date_range("inDateRange", "Input date", start="2024-11-01", end="2025-11-30")
    ui.input_radio_buttons("months_or_years","Summarize by:",["Year","Month"],selected = ["Year"])
    ui.input_radio_buttons("sort_by","Sort by:",["amount","Date","category"],selected = ["amount"])

with ui.value_box(showcase=icon("piggy-bank")):
    "Total Euros"
    @render.ui
    def show_total():
        return '{:20,.2f} '.format(get_summary()["amount"].sum())


with ui.layout_columns(col_widths=[5,7,12]):

    with ui.card(full_screen=True):
        ui.card_header("Summary Data")
        @render.data_frame
        def summary_df():
            #round(2) would work better if the column was already a decimal. 
            return render.DataGrid(get_summary(), filters=True, selection_mode="rows")

    with ui.card(full_screen=True):
        @render.plot
        def my_scatter():
            top10, title = get_pie_data()
            toal = top10.amount.sum()
            fig, ax = plt.subplots()
            ax.set_title(title)
            ax.pie(x = top10.amount, labels = top10.category, autopct = lambda x : '€{:,.0f}'.format(Decimal(x.item()) * Decimal(toal)/Decimal(100.0) ) )
            return fig
#            return plt.pie(x = top10.amount, labels = top10.category, autopct = lambda x : '€{:,.0f}'.format(Decimal(x.item()) * Decimal(toal)/Decimal(100.0) ) )


    with ui.card(full_screen=True):
        ui.card_header("Detail Data")
        @render.data_frame
        def transactions_df():
#            load_trans_from_gsheet()
            return render.DataGrid(filtered_df(), filters=True)  
    

@reactive.calc
def filtered_df():
    # When a summary rows is selected use it as a filter, otherwise, render the detail rows normally 
    # this should respect the filters in the sidebar too (eh not sure)
    data_selected = summary_df.data_view(selected=True)
    if ( data_selected.empty):
        trans = get_trans()
        sort = input.sort_by()
        if (sort == "Date"):
            sort = "lance"
        return  trans.sort_values(by=[sort,'lance','category'])   
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
    
@reactive.calc
def buildFilter():
    qstr = None
    types = input.types()
    # ["D","C"] is default
    if types:
        llist = ",".join(types)
        qstr = f"newt in [ {llist} ]"
    else:
        qstr = "newt in ['D']"
    return qstr

@reactive.calc
def get_pie_data():
    data_selected = summary_df.data_view(selected=True)
    if ( data_selected.empty):
        qstr = ""
        if input.input_year() in ["All Years","Date Range"]:
            title = "All Years"
            qstr = ""
        else:
            qstr = "year == '" + input.input_year() + "'"
            title = "Year " + input.input_year() 
        types = buildFilter()    
        if qstr == "":
            if (types is not None):
                qstr = types    
                summary = get_trans().query(qstr).copy()
            else:
                summary = get_trans().copy()
        else:
            qstr += " and " + types
            summary = get_trans().query(qstr).copy()

        summary = summary.groupby(['category'])['amount'].sum().reset_index()
        total = summary.amount.sum()
        summary.amount = summary.amount.apply(lambda negamt : abs(round( Decimal(negamt),2))) # pie positive numbers only
        top10 = summary.sort_values(by=["amount","category"],ascending=[False,True]).iloc[:12]
        return top10, f"{title} €{total: ,.0f}"
#        top10_rows = len(summary.index)
#        if (top10_rows <= 12):
#            return top10
#        rest_amt = summary.sort_values(by=["amount","category"],ascending=[False,True]).tail(top10_rows - 12)['amount'].sum()
        # Creating a new row as a DataFrame
#        new_row = pd.DataFrame({'category': ['The Rest'], 'amount': [rest_amt]})
#        top10 = pd.concat([top10, new_row],ignore_index=True)
#        return top10.sort_values(by=["amount","category"],ascending=[False,True])
    else:
        category = data_selected["category"].to_numpy()[0]
        category = category.replace("'","\\'",1)
        title = f"Subtotal for {category}"
        by = input.months_or_years()
        qstr1 = ""
        if (by == "Month"): 
            year_month = data_selected["year_month"].to_numpy()[0]
            qstr1 = f"category ==  '{category}' and year_month == '{year_month}'"
            title = f"{title} and {year_month}"
        else:
            year = data_selected["year"].to_numpy()[0]
            qstr1 = f"category ==  '{category}' and year == '{year}'"
            title = f"{title} and {year}"
        types = buildFilter()    
        qstr1 += " and " + types
        # Filter data for selected category and dates
        data = get_trans().query(qstr1).copy()
        total = data.amount.sum()
        title = f"{title} €{total:,.0f}" #{:,.0f}
        # gsheet has empty string instead of null or None, fix to None
        data["subcat"] = data["subcat"].apply(lambda x: None if x == "" else x)
        data['subcat'] = data.subcat.combine_first(data.desc)# when subcat is null, replace with desc
        data = data.groupby('subcat')['amount'].sum().reset_index()
        data.amount = data.amount.apply(lambda negamt : abs(round( Decimal(negamt),2))) # pie positive numbers only
        data = data.rename(columns={'subcat': 'category'})
        top10 = data.sort_values(by=["amount","category"],ascending=[False,True]).iloc[:10]
        top10_rows = len(data.index)
        if (top10_rows <= 10):
            return top10, title
        rest_amt = data.sort_values(by=["amount","category"],ascending=[False,True]).tail(top10_rows - 10)['amount'].sum()
        # Creating a new row as a DataFrame
        new_row = pd.DataFrame({'category': ['The Rest'], 'amount': [rest_amt]})
        top10 = pd.concat([top10, new_row],ignore_index=True)
        return top10.sort_values(by=["amount","category"],ascending=[False,True]), title


@reactive.calc
def get_summary():
#    dates = input.inDateRange()
    year = input.input_year()
    sum_by = input.months_or_years()
    sort = input.sort_by()
    asc = True
    qstr  = buildFilter()
    if (year != "All Years"):
        qstr += "and year == '" + year  + "'"

    if (sum_by == "Month"):
        if (sort =="Date"):
            sort = "year_month"
        summary = trans.query(qstr).groupby(['category','year', 'year_month'])['amount'].sum().reset_index()
    else:   
        if (sort =="Date"):
            sort = "year"
        summary = trans.query(qstr).groupby(['category','year'])['amount'].sum().reset_index()
    summary = summary.sort_values(by=[sort,"category"],ascending=asc)
    return summary.round({'amount': 2, 'usd': 2})
    
#def output_file(sumt,year):
   #  sumt.to_csv("/Users/cmcnally/Dropbox/python/textfiles/sorted" + year + ".csv", index=False)



@reactive.calc
def get_trans():
   # dates = input.inDateRange()
   # qstr = "lance >= '" + dates[0].isoformat() + "' and lance <= '" + dates[1].isoformat() + "'"
   # return trans.query(qstr).round({'amount': 2, 'usd': 2})
     return trans.round({'amount': 2, 'usd': 2})
