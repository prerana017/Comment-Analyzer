# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 17:25:39 2020

@author: taru.b.agarwal
"""
from pathlib import Path
import os

def get_pdf_content(pdf_file_path):
    from PyPDF2 import PdfFileReader
    with open(pdf_file_path,'rb') as f:
        pdf_reader = PdfFileReader(f,strict=False)
        #if pdf_reader.isEncrypted:
        #    pdf_reader.decrypt("")
            
        content = "\n".join(page.extractText().strip() for page in pdf_reader.pages)
        content = ' '.join(content.split())
        print ("text:::"+content)
        return content
    
def call_main(path, fname):
    import datetime
    import pandas as pd
    import numpy as np
    import re
    path=str(path)
    
    ext=os.path.splitext(fname)[1]
    if ext=='.pdf':
        content=get_pdf_content(path+"\\Data\\"+fname)
        filename = path + '\\Text\\' + os.path.splitext(fname)[0]
        file1 = open(filename+".txt", "w", encoding='utf-8')
        file1.write(content)
        file1.close()
        return
    elif ext=='.csv':
        df=pd.read_csv(path+"\\Data\\"+fname)
    elif ext=='.xlsx':
        df=pd.read_excel(path+"\\Data\\"+fname)
    else:
        try:
            txtfile = open(path+"\\Data\\"+fname,"r",encoding="utf8")
            datafile_Full = txtfile.readlines()
            txtfile.close()
        except:
            with open(path+"\\Data\\"+fname, 'rb') as f:
                datafile_Full = [l.decode('utf8', 'ignore') for l in f.readlines()]
        #------Split the .txt file at end of sentence ------
        content=' '.join(datafile_Full)
        filename = path + '\\Text\\' + os.path.splitext(fname)[0]
        file1 = open(filename+".txt", "w", encoding='utf-8')
        file1.write(content)
        file1.close()
        return
    
    df=df.replace('NA',"")
    df.fillna("",inplace=True)
    try:
        print("name conversion for thinknum data")       
        df['Job_Location']=df['Author Location']
        df['Review_Title']=df['Summary']
        df[	'Review_Text']=df['Description']	
        df['Pro_Review']=df['PROs']	
        df['Con_Review']=df['CONs']
        df['Date_of_Review']=df['As Of Date']
        df['Review_Rating']=df['Rating: Overall']
        df['Job_Title']=[x.split(' (',1)[0] if x.find(' (')>-1  else x for x in df['Author Title']]
        if "Is current job?" in df.columns:
            df['Former_employee']=df["Is current job?"]
            df['Former_employee']=df['Former_employee'].apply({True:'Current Employee', False:'Former Employee'}.get)
            
            
        else:
            df['Former_employee']=[x.split(' (',1)[1][0:-1] if x.find(' (')>-1  else '' for x in df['Author Title']]

    except:
        print("HArvested data")
    for colname in ['Review_Text','Pro_Review','Con_Review', 'Review_Title']:
        try:
            df[colname] = [x.rstrip('.') for x in df[colname]]
        except:
            pass
    try:
        
        if 'Former_employee' in df.columns:
            print("already a  columns with former employee information")
        else:
            df['Former_employee']=[x.split(' - ',1)[0] if x.find(' - ')>-1  else '' for x in df['Job_Title']]
        df['Job_Title']=[x.split(' - ',1)[1] if x.find(' - ')>-1  else x for x in df['Job_Title']]
           
        
        df['Combined_R_P_C'] = df[['Review_Text','Pro_Review','Con_Review','Review_Title']].apply(lambda x: '. '.join(map(str, x)), axis=1)#Glasdoor
        df['Date_of_Review'] = np.where(df['Date_of_Review'] == '','1 January 2020', df['Date_of_Review'])
        try:
            df['Date'] = [datetime.datetime.strptime(x, '%d %B %Y') for x in df['Date_of_Review']]
        except:
                df['Date'] = [datetime.datetime.strptime(x[0:8], '%Y%m%d') for x in df['Date_of_Review']]
        df = df[['Combined_R_P_C', 'Date','Job_Title','Job_Location','Former_employee','Review_Rating']].drop_duplicates()
        df.to_excel(path+"\\Text\\"+os.path.splitext(fname)[0]+"_date.xlsx",index=False)
    except:
        try:
            if 'Former_employee' in df.columns:
                print("already a  columns with former employee information")
            else:
                df['Former_employee']=[x.split(' - ',1)[0] if x.find(' - ')>-1  else '' for x in df['Job_Title']]
                df['Job_Title']=[x.split(' - ',1)[1] if x.find(' - ')>-1  else x for x in df['Job_Title']]

            df['Combined_R_P_C'] = df[['Review_Text','Pro_Review','Con_Review','Review_Title']].apply(lambda x: '. '.join(map(str, x)), axis=1)#Indeed
            df['Date_of_Review'] = np.where(df['Date_of_Review'] == '','January 1, 2020',df['Date_of_Review'])
            try:
                df['Date'] = [datetime.datetime.strptime(x, '%B %d, %Y') for x in df['Date_of_Review']]
            except:
                df['Date'] = [datetime.datetime.strptime(x[0:8], '%Y%m%d') for x in df['Date_of_Review']]
            
            df = df[['Combined_R_P_C', 'Date','Job_Title','Job_Location','Former_employee','Review_Rating']].drop_duplicates()

            df.to_excel(path+"\\Text\\"+os.path.splitext(fname)[0]+"_date.xlsx",index=False)
        except:
            
            if 'Date' in df.columns:
                print("Check*")
            else:
                print("^^^^^")
                df['Date_of_Review'] = np.where(df['Date_of_Review'] == '','2020',df['Date_of_Review'])
                try:
                    df['Date'] = [datetime.datetime.strptime(re.findall('\d\d\d\d',x)[0], '%Y') for x in df['Date_of_Review']] #for vault
                except:df['Date']=""

            
            df['Combined_R_P_C'] = df[['Review_Text','Pro_Review','Con_Review','Review_Title']].apply(lambda x: '. '.join(map(str, x)), axis=1)#Vault
            #df['Job_Title']=""
            #df['Job_Location']=""
            #df['Former_employee']=""
            #df['Review_Rating']=""
            expected_columns={'Combined_R_P_C', 'Date','Job_Title','Job_Location','Former_employee','Review_Rating'}
            common_columns=expected_columns.intersection(list(df.columns))
            df = df[common_columns].drop_duplicates()

            df.to_excel(path+"\\Text\\"+os.path.splitext(fname)[0]+"_date.xlsx",index=False)
            
            
    
 # Set the directory you want to start from
## create the list of company folder names
#comp_list = ['Southwest','Starbucks','Tesla','Tetra Tech','The Walt Disney','Turner Sports - Time Warner','United Health Group','Volvo','Walgreens','WalMart','WSP']
comp_list=['Astrazeneca']
for comp in comp_list:
    #comp ='Oracle'
    print(comp)
    #rootDir="C:\\Users\\taru.b.agarwal\\OneDrive - Accenture\\Culture_Outside_In\\Companies_Data\\" + comp
    rootDir=r"C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Companies\\"+comp
    for dirName, subdirList, fileList in os.walk(rootDir):
        if dirName.endswith('Data'):
            for fname in fileList:
                print('\t%s' % fname)
                path=Path(dirName).parent
                print (path)
                try:
                    call_main(str(path), fname)
                except Exception as e:
                    print (e)
                 

                 
                 
                 