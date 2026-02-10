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
PK_COL = 15
ACC_UPDATE = 1
CAT_UPDATE = 5
MEMO_UPDATE = 6
SUB_UPDATE = 12
PK_UPDATE = 15

# could read this from the spreadsheet, it's in the second sheet
categories = ["Amazon -- already categorized individulally",
"Bicycle Maintenance",
"Birthdays, Christmas, Gifts, Parties",
"Books, mostly hella",
"Caldas home wares etc",
"Car/Gas/Tolls/Parking/repair",
"Charity/Activist Contributions",
"Children's Enrichment Classes",
"Chris Cash Spending",
"Chris Clothing, Shoes, & Accessories",
"Chris Hobbies",
"Chris Travel",
"Chris's Lunches/Fast Food",
"CORREOS_EXPRESS(63964410554091701999998)",
"Dental",
"Electric",
"Family Entertainment",
"Family Vacations",
"Fitness & Health Hacking",
"Fraud",
"GADGETS",
"Google Fi + Cell phone",
"Groceries",
"Health Insurance Premiums",
"Hella Cash Spending",
"Hella Clothing, Shoes, Accessories",
"Hella Travel",
"Hella's Hobbies and Sports",
"Hella's Lunches/Fast Food",
"Hella's yarn and crafts",
"Hellas Podcast",
"House Cleaning",
"Income Taxes",
"Kids Clothing and Baby Gear",
"Legal & Administrative Fees",
"Lessons & Learning",
"Media Subscriptions",
"Medical + Birth",
"Pets",
"Rent",
"Restaurants/Dinner Take Out",
"School Tuition and Fees",
"transit",
"Uber, Lyft, Cabs, Car2Go, Public Transit, Bikeshare ",
"Water Bill",
"Unknown",
"Internet",
"Hella's Real Estate Expenses",
"Occasional Babysitting",
"Rosemary Funeral",
"Stuff I Forgot to Budget For",
"Portugal Relocation Project",
"Mortgage"]

# old code to load from the file, using a spreadsheet now
def load_categorized_trans():
    #trans =  pd.read_csv("textfiles/categorized-all-2024-2025.csv")
    trans = pd.read_csv("https://raw.githubusercontent.com/chrismcnally/spending/refs/heads/master/textfiles/categorized-all-2024-2025.csv")
    trans['category'] = trans['category'].fillna("Unknown") # mark the unknown category
    trans.sort_values(by=["lance","dv","balance"], ascending=[True, True, False], inplace=True)
    trans.drop(columns=["balance","dv","erate","fragment","who"], inplace=True)
    all_dates = trans["lance"] #gather all dates. this is a list of the values in that column
    year_months = [x[0:4] + x[5:7] for x in all_dates]  
    years =  [x[0:4]  for x in all_dates]
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
    print(f"the spreadsheet currently has {worksheet.row_count} rows")
    # this won't work anymore, unless we replace empty string with None
    trans["category"] = trans["category"].apply(lambda x: None if x == "" else x)
    trans['category'] = trans['category'].fillna("Unknown") # mark the unknown category

    trans.sort_values(by=["lance","dv","balance"], ascending=[True, True, False], inplace=True)
    trans.drop(columns=["balance","dv","erate","fragment","who"], inplace=True)
    all_dates = trans["lance"] #gather all dates. this is a list of the values in that column
    year_months = [x[0:4] + x[5:7] for x in all_dates]  
    years =  [x[0:4]  for x in all_dates]
    trans["year"] = years
    trans["year_month"] = year_months
    trans["amount"]  = pd.to_numeric(trans["amount"] , errors='coerce')
    trans["amount"] = trans["amount"].astype(float)
    trans['amount'] = trans['amount'].apply(lambda x: Decimal(x).quantize(Decimal("0.01"),rounding=ROUND_HALF_UP) )
    return  trans.sort_values(by=['year', 'year_month','category'])

def make_summary_table(detail):
#    return trans.groupby([pd.Grouper(key='Category', freq='ME')])['amount'].sum()
    summary = detail.groupby(['category','year', 'year_month'])['amount'].sum()
    return summary.reset_index()


#trans = load_categorized_trans()

details = load_trans_from_gsheet()
trans = reactive.Value(details)
summary = make_summary_table(details)

ui.tags.style("""
    .modal-dialog { 
        margin-top: 5vh !important; 
    }
""")
with ui.sidebar():
    ui.input_selectize("input_year", "Years", choices=("All Years","2026","2025","2024","2023","2022","2021"), selected="All Years", multiple=True)
    ui.input_selectize("input_category","Filter Categories", choices = categories,selected=None,multiple=True)
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
            df = filtered_df()
            return render.DataGrid(df, filters=True, selection_mode="row")  
        
    with ui.card():
        ui.card_header("Filtered Totals")
        @render.text
        def do_totals():
            # this might be a problem
            totals = calc_filtered_sum()
            return f'{totals["count"]} filtered rows. Total Euros {totals["euros"]:,.2f} Total dollars {totals["usd"]:,.2f}'

@transactions_df.set_patch_fn
def _(*, patch: render.CellPatch):
        update_data_with_patch(patch)
        return patch["value"]    

def update_data_with_patch():
    # we have to update trans, the source dataset, and the spreadsheet, we maybe could also update the data behind the grid as displayed
    pk_ = input.edit_pk()
    print(f"Updating row with pk {pk_}")
    ed_cat = input.edit_cat()
    ed_sub = input.edit_sub()
    ed_memo = input.edit_memo()
    ed_desc = input.edit_desc()
    ed_acc = input.edit_acc()
   # df_copy = transactions_df.data_view()
    new_trans = get_trans().copy()
    orig_acc = new_trans.loc[new_trans["PK"] == int(pk_)]["account"].to_numpy()[0]
    orig_sub = new_trans.loc[new_trans["PK"] == int(pk_)]["subcat"].to_numpy()[0]
    orig_memo = new_trans.loc[new_trans["PK"] == int(pk_),"memo"].to_numpy()[0] 
    orig_cat = new_trans.loc[new_trans["PK"] == int(pk_)]["category"].to_numpy()[0]
    orig_desc = new_trans.loc[new_trans["PK"] == int(pk_)]["desc"].to_numpy()[0]
    print(f"Original row: PK {pk_} account:{orig_acc} Desc: {orig_desc} Memo:{orig_memo} cat:{orig_cat} subcat:{orig_sub}")
 #   df_copy.loc[df_copy["PK"] == int(pk_),"subcat"] = ed_sub
 #   df_copy.loc[df_copy["PK"] == int(pk_),"memo"] = ed_memo
 #   df_copy.loc[df_copy["PK"] == int(pk_),"category"] = ed_cat
    new_trans.loc[new_trans["PK"] == int(pk_),"subcat"] = ed_sub
    new_trans.loc[new_trans["PK"] == int(pk_),"memo"] = ed_memo
    new_trans.loc[new_trans["PK"] == int(pk_),"category"] = ed_cat
    new_trans.loc[new_trans["PK"] == int(pk_),"account"] = ed_acc
    trans.set(new_trans) # trans is a reactive variable, called with trans.get() and trans.set() (use get_trans() to get the value not trans.get())
    credentials =  json.loads(os.environ["SERVICE_JSON"])
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(S_KEY)
    # omg what a stupid assumption. Find the PK first, then get the row number, merde. 

    worksheet =  sh.get_worksheet(0)
    cell = worksheet.find(str(pk_),in_column=PK_COL)
    if (cell):
        row_num = cell.row
        print(f"Going to update row {row_num} with PK {pk_} other info {cell}")
        if (orig_sub != ed_sub ):
            worksheet.update_cell(row_num, SUB_UPDATE,  ed_sub)
        if (orig_memo != ed_memo ):
            worksheet.update_cell(row_num, MEMO_UPDATE, ed_memo)
        if (orig_cat != ed_cat ):
            worksheet.update_cell(row_num, CAT_UPDATE,  ed_cat)
        if (orig_acc != ed_acc ):
            worksheet.update_cell(row_num, ACC_UPDATE,  ed_acc)

@reactive.calc
def calc_filtered_sum():
    # this is the problem
    view = None
    try:
        view = transactions_df.data_view()
    except:
        print("simpluy calling transactions_df.data_view() throws an exception, I don't know why: transactions_df is not null")
    if view is None or view.empty:
        return {
            "count": 0,
            "euros": 0.0,
            "usd" : 0.0
        }

    return {
        "count": len(view),
        "euros": view["amount"].sum(),
        "usd" : view["usd"].sum()
    }
# this is called 2x on startup no errors. when select 2026
@reactive.calc
def filtered_df():
    # When a summary rows is selected use it as a filter, otherwise, render the detail rows normally 
    # this should respect the filters in the sidebar, years, transaction types and sort
    data_selected = summary_df.data_view(selected=True)
    alltrans = get_trans()
    # filter by transaction type
    qstr = buildFilter()
    # filter by years
    years = input.input_year()
    if (years and len(years) > 0 and "All Years" not in years):
        qstr += f" and year in {years} " 
    alltrans = alltrans.query(qstr)
    if ( data_selected.empty):
        sort = input.sort_by()
        if (sort == "Date"):
            sort = "lance"
        return  alltrans.sort_values(by=[sort,'lance','category'])   
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
        return alltrans.query(qstr1)
    
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
        qstr = buildFilter()    
        years = input.input_year()
        if "All Years" in years:
            title = "All Years"
        else:
            title = f"Year {years}" 
            qstr = f"{qstr} and year in {years}" 
        summary = get_trans().query(qstr).copy() #shouldn't we just call get_summary()?
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
        data["memo"] = data["memo"].apply(lambda x: None if x == "" else x)
        data['subcat'] = data.subcat.combine_first(data.memo)# when subcat is null, replace with memo first then desc
        data['subcat'] = data.subcat.combine_first(data.desc)# if subcat is still null, replace with desc
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
    years = input.input_year()
    sum_by = input.months_or_years()
    sort = input.sort_by()
    cats = input.input_category()
    asc = True
    qstr  = buildFilter()     
    if (years and len(years) > 0) and "All Years" not in years:
        qstr += f" and year in {years}"
#    if (year != "All Years"):
#        qstr += "and year == '" + year  + "'"
    if (cats and len(cats) > 0):
        qstr += f" and category in {cats}"
    if (sum_by == "Month"):
        if (sort =="Date"):
            sort = "year_month"
        print(f"query string is {qstr}")
        summary = get_trans().query(qstr).groupby(['category','year', 'year_month'])['amount'].sum().reset_index()
    else:   
        if (sort =="Date"):
            sort = "year"
        print(f"query string is {qstr}")
        summary = get_trans().query(qstr).groupby(['category','year'])['amount'].sum().reset_index()
    summary = summary.sort_values(by=[sort,"year","category"],ascending=asc)
    return summary.round({'amount': 2, 'usd': 2})
    
#def output_file(sumt,year):
   #  sumt.to_csv("/Users/cmcnally/Dropbox/python/textfiles/sorted" + year + ".csv", index=False)

@reactive.effect
@reactive.event(input.transactions_df_selected_rows)
def on_row_selected():
    data_selected = input.transactions_df_selected_rows()
    if not data_selected:
        return
    data_selected = transactions_df.data_view(selected=True)
    if  data_selected.empty:
        return
    acc_ = data_selected["account"].to_numpy()[0]
    pk_ = data_selected["PK"].to_numpy()[0]
    desc_ = data_selected["desc"].to_numpy()[0]
    cat_ =  data_selected["category"].to_numpy()[0]
    sub_ = data_selected["subcat"].to_numpy()[0]
    memo_ = data_selected["memo"].to_numpy()[0]
    amount_ = data_selected["amount"].to_numpy()[0]
    usd_ = data_selected["usd"].to_numpy()[0]
    # Create a read-only input by adding the 'readonly' attribute
    #pk_input = ui.input_text("pk", "Primary Key:", value="TXN-90210")
    #pk_input.children[1].attrs.update({"readonly": "readonly"})
    acc_input = ui.input_text("edit_acc", "Account", value=str(acc_))
    acc_input.children[1].attrs.update({"readonly": "readonly"})
    pk_input = ui.input_text("edit_pk", "PK", value=str(pk_))
    pk_input.children[1].attrs.update({"readonly": "readonly"})
    amount_input =    ui.input_text("edit_amount", "Euros", value=str(amount_))
    amount_input.children[1].attrs.update({"readonly": "readonly"})
    usd_input =    ui.input_text("edit_usd", "USD", value=str(usd_))
    usd_input .children[1].attrs.update({"readonly": "readonly"})
    m = ui.modal(
        pk_input,
        acc_input,
        ui.input_text("edit_desc", "Desc", value=desc_),
        ui.input_text("edit_memo", "Memo", value=memo_),
        ui.input_selectize("edit_cat", "Category", categories, selected = cat_),
        ui.input_text("edit_sub", "Subcategory", value=sub_),
        amount_input,
        usd_input, 
        title = "Edit Transaction",
        footer=ui.div(
            ui.modal_button("Cancel"), # Closes modal without action
            ui.input_action_button("save", "Save Changes", class_="btn-primary")
        ),easy_close=True
    )
    ui.modal_show(m)

# Logic to close the modal after clicking 'Save'
@reactive.effect
@reactive.event(input.save)
def _():
    update_data_with_patch()
    ui.modal_remove()

@reactive.calc
def get_trans():
   # dates = input.inDateRange()
   # qstr = "lance >= '" + dates[0].isoformat() + "' and lance <= '" + dates[1].isoformat() + "'"
   # return trans.query(qstr).round({'amount': 2, 'usd': 2})
     return trans.get().round({'amount': 2, 'usd': 2})
