from tableauDL import tableauDL

"""
pyinstaller --onefile --add-data "note-439603-7d886709325b.json;." --add-data "view_ids.pkl" --add-data "view_names.pkl" --add-data "tableauDL.py" md_wscale.py
"""

SPREADSHEET_ID = '19nNPY2NjsPKNxeY6M7Yje81Hw5ZyA7kZlwFj-frWKqc'
FORMAT_FLAG_LIST= ['form', 'const', 'form', 'const']
CREDENTIAL_FILE = 'note-439603-7d886709325b.json'

tableau = tableauDL(api_ver="3.20")
token, site_id = tableau.signin()
view_ids, view_names = tableau.load_view_information("view_ids.pkl", "view_names.pkl")
dl_head = {"X-Tableau-Auth": token}
gc = tableau.gspread_authorization(CREDENTIAL_FILE)

functions_list=[
    tableau.md_brand_POS, 
    tableau.md_maker_share, 
    tableau.md_color_rank, 
    tableau.md_product_and_shop
]
for v_name, v_id, app_function, format_flag in zip(view_names, view_ids, functions_list, FORMAT_FLAG_LIST):

    csv_url= f"{tableau.server_url}/api/{tableau.api_ver}/sites/{site_id}/views/{v_id}/data"
    df = tableau.download_row_csv(csv_url, dl_head)

    df_form = app_function(df)

    tableau.transfer_to_gspread(
        df_form, gc, spreadsheet_id=SPREADSHEET_ID, 
        sheet_name=v_name, flag=format_flag
    )

    print("Finish: ", v_name)
