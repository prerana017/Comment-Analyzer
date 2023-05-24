
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os
import spacy

#pip install spacy==2.2.3


comp = 'Walmart_USA'
#txtfile = open(os.getcwd() + '\\Data\\Consolidated\\entire_available_corpus.txt',"r",errors='ignore')
#datafile_Full = txtfile.readlines()
#txtfile.close()
Sheet='Sheet1'
txtfile = pd.read_excel(r'C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Companies\Astrazeneca\Text\combined.xlsx',sheet_name=Sheet)
#txtfile['ID'] = [str(x) for x in range(1,len(txtfile)+1)]
#textfile=
nlp = spacy.load('en_core_web_sm')
#raw_corpus = [word for word in datafile_Full if word != '']

raw_list = txtfile['Sentence'].tolist()
#c=1
df_cnoun_role=[]
#for k in range(0,len(raw_corpus),1000):
#    df_cnoun_role=[]
#    try:
#        raw_list = raw_corpus[k:k+1000]
#    except:
#        raw_list = raw_corpus[k:]
for each in range(0,len(raw_list)):
    print(each)
    data_token=raw_list[each]
    data_token=nlp(data_token)
    
    #collocation nouns
    c_noun=[]
    for tok in [tok for tok in data_token if tok.dep_ == 'compound']: # Get list of compounds in doc
        noun = data_token[tok.i: tok.head.i + 1]
        c_noun.append(noun.text)
       
    c_noun= pd.DataFrame({'keywords':list(set(c_noun))})
    if len(c_noun):
        c_noun['keywords'] = c_noun['keywords'].str.replace('[^\w\s]',' ')
        c_noun['keywords'] = [x.strip() for x in c_noun['keywords']]
        c_noun['process']="Collocation"
        #c_noun = c_noun.drop_duplicates(subset='keywords')
    else:
        c_noun = pd.DataFrame({'keywords':['NA'], 'process':['Collocation']})
    
    #adj-nouns
    comp1 = pd.DataFrame(columns=['Word', 'POS','Is_Alpha','Is_StopWord'])    
    for token in data_token:
        data = [token.text, token.pos_,token.is_alpha, token.is_stop]
        comp1.loc[len(comp1)] = data
    
    word_stopwords = comp1[(comp1['Is_StopWord']==False)]
    word_stopwords = word_stopwords[(word_stopwords['Is_Alpha']==True)]
    word_stopwords.reset_index(drop= True ,inplace=True)

    all_data=[]
    for rowid in range(0, len(word_stopwords)-1):
        data=[]
        if (word_stopwords.POS.loc[rowid]=="ADJ") and (word_stopwords.POS.loc[rowid+1]=="NOUN"):
            word_1= word_stopwords.Word.loc[rowid]
            word_2 =word_stopwords.Word.loc[rowid+1]
            word=word_1+' '+word_2
            data.append(word)
        all_data.append(data)
    all_data = [x for x in all_data if x != []]
    
    if len(all_data):
        all_data = [item.strip() for sublist in all_data for item in sublist] 
        all_data = [x for x in all_data if x not in c_noun['keywords'].unique().tolist()]
        all_data= pd.DataFrame({'keywords':list(set(all_data)),'process':'adjective+noun'})
    else:
        all_data = pd.DataFrame({'keywords':['NA'], 'process':['adjective+noun']})
    #appending collocation & adj+noun keywords
    df_r= pd.DataFrame()
    df_r = c_noun.append(all_data)    
    
    df_r = df_r.loc[df_r['keywords'] != 'NA']
    if len(df_r):
        df_r_txt = ';'.join(df_r['keywords'].tolist())
    else:
        df_r_txt = 'No keywords found'
    df_cnoun_role.append(df_r_txt)

#df_cnoun_role = pd.concat(df_cnoun_role,axis=0)
#df_cnoun_role = df_cnoun_role.loc[df_cnoun_role['keywords'] != 'NA'].drop_duplicates(['keywords'])
#df_cnoun_role.to_excel(os.getcwd() + '\\Data\\Collocation\\collocation_' + str(c) + '.xlsx', index=False)
#print(c)
#c=c+1

txtfile['Collocation_phrases'] = df_cnoun_role
txtfile['Collocation_unique_phrases'] = [';'.join(list(set(x.split(';')))) for x in txtfile['Collocation_phrases']]

theme_word = pd.DataFrame(columns=['Theme', 'Phrase', 'Count'])
for theme in txtfile['Final_Theme'].unique().tolist():
    print(theme)
    temp = txtfile.loc[txtfile['Final_Theme'] == theme][['Collocation_unique_phrases']]
    phrase_text = pd.DataFrame({'phrases': ';'.join(temp['Collocation_unique_phrases'].tolist()).split(';')})
    df = pd.DataFrame(phrase_text['phrases'].value_counts()).reset_index()
    df.columns=['Phrase', 'Count']
    df['Theme'] = theme
    theme_word = theme_word.append(df)

#txtfile.drop(['Collocation_phrases'], axis=1, inplace=True)
theme_word.to_excel(r'C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Companies\Astrazeneca\Text\\'+'d_collocation_count.xlsx', index=False)
txtfile.to_excel(r'C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Companies\Astrazeneca\Text\\'+'d_collocation.xlsx', index=False)
