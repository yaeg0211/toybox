import os
import sys
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

class tableauDL:
    def __init__(self, api_ver):
        self.api_ver = api_ver
        self.server_url = "https://prod-apnortheast-a.online.tableau.com"
        self.signin_url = f"{self.server_url}/api/{self.api_ver}/auth/signin"
        self.test_csv="test_csv.csv"

    def signin(self):

        authpayload = {
            "credentials": {
                "personalAccessTokenName": "your_token_name",
                "personalAccessTokenSecret": "your_access_token",
                "site": {
                    "contentUrl": "riseupws"
                }
            }
        }
        headers = {"Content-Type": "application/json",  "Accept": "application/json"}

        response = requests.post(self.signin_url, json=authpayload, headers=headers)
        if response.status_code == 200:
            self.token = response.json()['credentials']['token']
            self.site_id = response.json()['credentials']['site']['id']
            print("Sign-in successful")

            self.dl_head = {"X-Tableau-Auth": self.token}

            return self.token, self.site_id
        else:
            print(f"Sign-in failed: {response.status_code} - {response.text}")
            raise Exception("Failed to sign in to Tableau")

    def get_base_path(self):
        if getattr(sys, 'frozen', False):
            # PyInstallerでの実行時
            return os.path.dirname(sys.executable)
        else:
            # 通常のPythonスクリプト実行時
            return os.path.dirname(os.path.abspath(__file__))


    def load_view_information(self, ids_pkl, names_pkl):

        import pickle

        path_to_id = os.path.join(self.get_base_path(), ids_pkl)
        path_to_name = os.path.join(self.get_base_path(), names_pkl)

        with open(path_to_id, 'rb') as f:
            view_ids = pickle.load(f)
        
        with open(path_to_name, 'rb') as f:
            view_names = pickle.load(f)

        return view_ids, view_names


    def download_row_csv(self, csv_url, dl_header):

        import io

        csv_resp = requests.get(csv_url, headers=dl_header)
        csv_resp.raise_for_status()

        csv_text = csv_resp.content.decode('utf-8')

        csv_buffer = io.StringIO(csv_resp.content.decode('utf-8'))
        df = pd.read_csv(csv_buffer)

        return df


    def read_testcsv(self, csv_path):
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        return df


    def save_csv(self, df, name):
        df.to_csv("save/"+name+".csv", encoding="utf-8-sig", index=True)


    def tempo_00_comfirm_format_csv(self, df):

        df = df[df["Month of 日付"] != 'All'].fillna(0)
        df["売上金額"] = df["売上金額"].replace(",", "", regex=True).astype(int)
        df_pivot = df.pivot_table(index=["店舗コード (ゼロ落ち)", "店舗名称"],
                                columns="Month of 日付", 
                                values="売上金額", 
                                aggfunc='sum')
        df_pivot["総計"] = df_pivot.sum(axis=0)
        df_pivot = df_pivot.astype('Int64')

        return df_pivot

    def total_01_format_csv(self, df):

        df["売上金額"] = df["売上金額"].replace(",", "", regex=True).astype(int)
        df_pivot = df.pivot_table(index=['タイプ'], 
                                columns="Month, Year of 日付", 
                                values="売上金額", 
                                aggfunc='sum')
        df_pivot = df_pivot.astype('Int64')

        return df_pivot

    def total_02_prev_rate_format_csv(df):

        s_columns=['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月','9月', '10月', '11月', '12月']

        df_pivot = df.pivot_table(index=['Year of 日付', "Measure Names"], 
                          columns=["Month of 日付"], 
                          values="Measure Values", 
                          aggfunc='sum')

        df_pivot=df_pivot[s_columns]
        df_pivot.index = df_pivot.index.set_levels(
            df_pivot.index.levels[1].map({'前年売上金額 along Year of 日付': '日付の年に沿った前年売上金額', 
                                        '前年比 along Year of 日付': '日付の年に沿った前年比',
                                        '売上金額':'売上金額'}), level=1
        )

        df_pivot = df_pivot.reset_index()
        df_pivot['Measure Names'] = pd.Categorical(df_pivot['Measure Names'], categories=new_level, ordered=True)
        df_pivot = df_pivot.sort_values(['Year of 日付', 'Measure Names']).set_index(['Year of 日付', 'Measure Names'])

        mask = df_pivot.index.get_level_values('Measure Names') == '日付の年に沿った前年比'
        df_pivot.loc[mask] = df_pivot.loc[mask].applymap(lambda x: f"{float(x) * 100:.2f}%")

        return df_pivot

    def total_03_tempo_prev_rate_format_csv(df):

        df["売上金額"] = df["売上金額"].replace(",", "", regex=True).astype(float)
        df['店舗コード']=df['店舗コード'].astype(str).str.zfill(5)

        df_pivot = df.pivot_table(index=['店舗コード'], 
                          columns=["Month, Year of 日付"], 
                          values="売上金額", 
                          aggfunc='sum')

        df_pivot = df_pivot.astype('Int64')

        return df_pivot

    def total_04_tempo_and_amount(df):

        df_t = df.T

        df_t.columns = df_t.iloc[0]  # Set the first row as the header
        df_t = df_t[1:]

        df_t=df_t.reindex(['売上金額', '店舗数', '売上数量']) # sort index

        return df_t

    def tempo_05_best50(df):

        df_pivot = df.pivot_table(index=['店舗コード (ゼロ落ち)', '店舗名称'], 
                                columns=["Month, Year of 日付", "Measure Names"], 
                                values=["Measure Values"], 
                                aggfunc='sum')

        cols=df_pivot.columns
        new_level_2 = cols.get_level_values(2).map(lambda x: '表(下)に沿った売上金額のランク' if x == 'Rank of 売上金額 along Table (Down)' else x) # rename
        df_pivot.columns = pd.MultiIndex.from_arrays([
            cols.get_level_values(0),
            cols.get_level_values(1),
            new_level_2
        ], names=cols.names)

        rank_col =df_pivot.columns[0]
        df_pivot[rank_col] = df_pivot[rank_col].astype(int)
        df_t = df_pivot.sort_values(by=rank_col) # sort by rank column

        return pd.concat([df_t.iloc[1:], df_t.iloc[0:1]]) # return after moving rank_row to the bottom

    def gspread_authorization(self, credentials_file):

        import gspread
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        path_to_credentials = os.path.join(self.get_base_path(), credentials_file)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credentials, scope)
        gc = gspread.authorize(credentials)

        return gc

    def fix_money_format(self, df):

        df.fillna(0, inplace=True)
        df=df.replace("All", "合計")
        df["売上金額"] = df["売上金額"].replace(",", "", regex=True).astype("int64")

        return df

    def fix_month_format(self, df):

        df['Month of 日付']=pd.to_datetime(df['Month of 日付'], format='%B %Y')
        df['Month of 日付']=df['Month of 日付'].dt.strftime('%Y年%m月')

        return df

    def sort_along_lastmonth(self, df):

        df.columns = df.columns.get_level_values(1)
        tmp = df.iloc[:, :-1]
        
        return tmp.sort_values(by=tmp.columns[-1],ascending=False)


    def format_cell(self, x):
        try:
            if pd.isnull(x) or x == '':
                return ""
            num = float(x)
            return "¥0" if num == 0 else f"¥{int(num):,}"
        except:
            return str(x)

    def md_brand_POS(self, df):

        df = self.fix_money_format(df)
        df = self.fix_month_format(df)

        df_pivot = df.pivot_table(index=['ブランド名_中分類_半', 'パートナー', 'メーカー'], 
                                columns=["Month of 日付"], 
                                values=["売上金額"], 
                                aggfunc='sum')
        
        df_fin = self.sort_along_lastmonth(df_pivot)
        df_fin.reset_index(inplace=True)

        return df_fin

    def md_maker_share(self, df):

        df.fillna('0%', inplace=True)
        df["% of Total 売上金額"] = (
            df["% of Total 売上金額"]
            .str.replace("%", "", regex=False)   # remove "%"
            .astype(float)                       # fix it to float
            .round(2)                            # round to 2 decimal places
        )
        df = self.fix_month_format(df)

        df_pivot = df.pivot_table(index=['メーカー'], 
                                columns=["Month of 日付"], 
                                values=["% of Total 売上金額"], 
                                aggfunc='sum')
        df_fin = self.sort_along_lastmonth(df_pivot)
        df_fin = df_fin.applymap(lambda x: f"{x}%" if pd.notnull(x) else "") # give "%"

        df_fin.reset_index(inplace=True)

        return df_fin

    def md_color_rank(self, df):

        df = self.fix_money_format(df)
        df=df[df['Month of 日付'] != '合計']
        df = self.fix_month_format(df)

        df_pivot = df.pivot_table(index=['カラーランキング', 'ブランド名_中分類_半', 'パートナー', 'メーカー'], 
                                columns=["Month of 日付"], 
                                values=["売上金額"], 
                                aggfunc='sum')

        df_fin = self.sort_along_lastmonth(df_pivot)
        df_fin.reset_index(inplace=True)

        # insert a new column for total
        total=df_fin.iloc[:, 4:].sum(axis=1)
        df_fin.insert(4, '総計', total)

        return df_fin

    def md_product_and_shop(self, df):

        df['Month, Year of 日付']=pd.to_datetime(df['Month, Year of 日付'], format='%B %Y')
        df['Month, Year of 日付']=df['Month, Year of 日付'].dt.strftime('%Y年%m月')

        df_pivot = df.pivot_table(index=['ブランド名_中分類_半', '店舗コード', '店舗名称'], 
                                columns=["Month, Year of 日付" ,'Measure Names'], 
                                values=["Measure Values"], 
                                aggfunc='sum')

        # rename the rank column
        new_columns = []
        for col in df_pivot.columns:
            if col[2] == "Rank of 店舗合計 along Table (Down)":
                new_col = (col[0], col[1], "店舗合計のランク")
            else:
                new_col = col
            new_columns.append(new_col)

        # apply change
        df_pivot.columns = pd.MultiIndex.from_tuples(new_columns)

        # sort columns by custom order
        sorted_cols = []
        custom_order = ["売上金額", "売上数量", "店舗合計", "店舗合計のランク", "店舗内シェア"]
        months = sorted(set(col[1] for col in df_pivot.columns))
        for month in months:
            # extract only the columns for the target months and sort them based on custom_order.
            month_cols = [col for col in df_pivot.columns if col[1] == month]
            month_cols_sorted = sorted(
                month_cols,
                key=lambda x: custom_order.index(x[2]) if x[2] in custom_order else 999
            )
            sorted_cols.extend(month_cols_sorted)

        # apply change
        df_fin = df_pivot.reindex(columns=sorted_cols)

        # make the column's value to parcent format
        for col in df_fin.columns:
            if col[2] == "店舗内シェア":
                df_fin[col].fillna("0", inplace=True)
                df_fin[col] = df_fin[col].apply(
                    lambda x: f"{round(float(str(x).replace('%', '')) * 100, 2)}%" if pd.notnull(x) else ""
                )

        # reset index to make it flat        
        df_fin.reset_index(inplace=True)

        # sort by brand name and last month value
        tmp=df_fin.iloc[:, :-5]
        df_fin=tmp.sort_values(
            by=["ブランド名_中分類_半", tmp.columns[-3]],
            key=lambda col: (
                col if col.name == "ブランド名_中分類_半"
                else col.str.replace(",", "").astype(float)
            ),
            ascending=[True, False]
        )

        return df_fin


    def transfer_to_gspread(self, df, gc, spreadsheet_id, sheet_name, flag):

        # Open the spreadsheet
        sh = gc.open_by_key(spreadsheet_id)

        try:
            worksheet = sh.worksheet(sheet_name)
            worksheet.clear() # clear all
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=sheet_name, rows="1000", cols="20")

        df.fillna(0, inplace=True)
        if flag == 'const':
            data = df.applymap(lambda x: str(int(x)) if isinstance(x, (int, float)) and x == int(x) else str(x)).values.tolist()
        elif flag == 'form': # if you need to give yen-mark
            data = df.applymap(self.format_cell).values.tolist()

        if sheet_name == '商品×店舗': # for multi-index
            header_level_0 = [col[0] for col in df.columns]
            header_level_1 = [col[1] for col in df.columns]
            header_level_2 = [col[2] for col in df.columns]

            all_data = [header_level_0, header_level_1, header_level_2] + data
        else:
            header = df.columns.tolist() # make header
            all_data = [header] + data

        worksheet.update('A1', all_data)
