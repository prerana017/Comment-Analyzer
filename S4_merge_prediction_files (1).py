# -*- coding: utf-8 -*-
"""
Created on Thu May  7 10:00:15 2020

@author: monika.joshi
"""

import sys
import csv
import glob
import pandas as pd
import re
import calendar
year = 2015


# get data file names['HANDELSBANKEN','SEB','NORDEA','SWEDBANK']
path =r'C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Companies\Astrazeneca\Text'
filenames = glob.glob(path + "/*_prediction.xlsx")

dfs =pd.DataFrame()

for f in filenames: 
    #print (f)
    df = pd.read_excel(f)
    import os
    print()
    name=os.path.basename(f)
    print(os.path.basename(f))
    print()
    match = re.match(r'.*([1-3][0-9]{3})', os.path.basename(f))
    year=0
    if match is not None:
        # Then it found a match!
        year=match.group(1)
        month = 12
        import datetime

        x = datetime.datetime(int(year), 12, 31)
        values={'Date of Review':x}
        #df=df.fillna(value=values)
    df['filename']=name
    if 'Job_Title' is  df.columns:
        dfs=dfs.append(df, ignore_index=True,sort=False)
        
    else:
        df["Job_Title"]=""
        dfs=dfs.append(df, ignore_index=True,sort=False)
        
dfs.to_excel(path + '\\combined.xlsx', index=False)