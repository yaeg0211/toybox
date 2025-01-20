import numpy as np
import pandas as pd
import pdfplumber

# read pdf file
with pdfplumber.open('path_to_pdf') as pdf:
    # extract text&tables
    num_page = 0
    print(f'{pdf.pages[num_page].extract_text()}')

    tables = pdf.pages[num_page].find_tables()
    print(tables[0].extract())

# transform the table to .csv
ext_list=tables[0].extract()
c_name=ext_list[0]

rows=[]
for i in range(1, len(ext_list)-1):
    row=[]
    tmp=ext_list[i]
    if i%2!=0:
        buf=tmp[0].split(' ')
        row.append(buf[0])
        row.append(buf[1].split('\n')[0])
    elif i%2==0:
        row.append(np.nan)
        row.append(tmp[1])
    row.extend(tmp[-1].split(' '))
    rows.append(row)
row=ext_list[-1][:2]
row.extend(ext_list[-1][2].split(' '))
rows.append(row)

# columns name
c_name=ext_list[0][:2]
c_name.extend(ext_list[0][2].split(' '))

df=pd.DataFrame(rows)
df.set_axis(c_name, axis=1).to_csv('.csv', encoding='utf-8_sig')
