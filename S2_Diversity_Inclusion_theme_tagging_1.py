'''
open folder data/doesn't exist just leave

inside data folder
  loop through each pdf file
  create another folder textfile papraller to text
  put all text there
  
  run the code on text file
'''

###import the ELMO function
#execfile('DCA_theme_tagging_1.py')
#from DCA_theme_tagging_1 import *
from tqdm import tqdm
from itertools import islice, count
from joblib import Parallel, delayed, parallel_backend
# %%
# ------- Data Preprocessing -------
def Data_Preparation(raw_corpus):
    import pandas as pd
    from nltk import word_tokenize, pos_tag
    from nltk.stem import WordNetLemmatizer
    #from nltk.corpus import stopwords

    def convert_pos_tag(pos_tuple):
        if (pos_tuple[1][0].lower()) == "n":
            return "n"
        elif (pos_tuple[1][0].lower()) == "v":
            return "v"
        if (pos_tuple[1][0].lower()) == "r":
            return "r"
        if (pos_tuple[1][0].lower()) == "j":
            return "a"
        else:
            return "n"

    raw_corpus = pd.Series(raw_corpus)
    pre_clean_raw = list(raw_corpus[0:len(raw_corpus)].dropna(axis=0))

    '''stop = stopwords.words('english')
    newStopWords = [] # List the additional stopwords if any i.e ['apple']
    stop.extend(newStopWords)'''
    stop = []

    clean_numeric = [''.join(word for word in str(sublist) if not word.isnumeric()) for sublist in pre_clean_raw]
    clean_corpus = [' '.join([word for word in document.lower().split() if word not in stop]) for document in
                    clean_numeric]

    lemmatizer = WordNetLemmatizer()
    clean_corpus = pd.Series(clean_corpus)
    clean_corpus = list(clean_corpus.apply(lambda comment:
                                           " ".join([lemmatizer.lemmatize(x[0], convert_pos_tag(x)) for x in
                                                     pos_tag(word_tokenize(comment))])))
    clean_corpus = pd.DataFrame(clean_corpus)
    clean_corpus.columns = ["Sentence"]
    clean_corpus = clean_corpus[clean_corpus["Sentence"] != '']
    
    #counter=counter+1
    return clean_corpus

def clean_text(unlistdata):
    import re
    regFloat=r'\d+\.\d+'
    regInt=r'\d+'
    regAllNumber = regFloat+'|'+regInt
    unlistdata_clean= re.sub(regAllNumber,'',unlistdata)
    #   Remove wired symbol, only keep alphabet and typical symbol
    unlistdata_clean=re.sub('[^A-Za-z+\.\;\?\:\!\，\、\；\!]', ' ', unlistdata_clean)
    # Remove single alphabet
    unlistdata_clean = re.sub("\b[a-zA-Z]{1,1}\b", " ", unlistdata_clean)
    # Remove additional space
    unlistdata_clean = re.sub(" +", " ", unlistdata_clean)
    # fullstop adding spac e
    unlistdata_clean=re.sub(' +', ' ', re.sub('\.','. ',unlistdata_clean)).strip()
    return unlistdata_clean

def get_pdf_content(pdf_file_path):
    from PyPDF2 import PdfFileReader
    with open(pdf_file_path,'rb') as f:
        pdf_reader = PdfFileReader(f)
        #if pdf_reader.isEncrypted:
        #    pdf_reader.decrypt("")
            
        content = "\n".join(page.extractText().strip() for page in pdf_reader.pages)
        content = ' '.join(content.split())
        print ("text:::"+content)
        return content
# %%
##Import .txt file , split into sentences
def call_main(pdf_file_path,filename,ONTOLOGY_PATH):
 
    from nltk.tokenize import sent_tokenize
    import pandas as pd
    import numpy as np
    import re
    import os

    ext=os.path.splitext(filename)[1]
    print (ext)

    filename=pdf_file_path+"\\Text\\"+os.path.splitext(filename)[0]
    
    ##Import .txt file , split into sentences    
    
    # ----- read the output single file ----
    if ext == '.txt':
        try:
            with open(filename + '.txt',"r",encoding="utf8") as f:
                datafile_Full = f.readlines()
        except:
            with open(filename + '.txt', 'rb') as f:
               datafile_Full = [l.decode('utf8', 'ignore') for l in f.readlines()]
        
        unlistdata = ' '.join(datafile_Full)
        
        # ------ Clean up the text---------------
        unlistdata_clean = clean_text(unlistdata)
        
        # ------Split the .txt file at end of sentence ------
        unlistdata = unlistdata_clean
        raw_data = sent_tokenize(unlistdata)
        raw_corpus = pd.Series(raw_data)
    else:
        datafile_Full = pd.read_excel(filename + '.xlsx')
        col=list(datafile_Full.columns)
        col.insert(len(datafile_Full.columns)+1,"splitted_sentence")
        col = ['Combined_R_P_C','Date_of_Review','splitted_sentence','Job_Title','Job_Location','Former_employee','Review_Rating']
        new_df=pd.DataFrame(columns=col)
        datafile_Full['unlistdata'] = datafile_Full['Combined_R_P_C'].apply(clean_text)
        for index, row in datafile_Full.iterrows():
            #print(index)
            raw_data = sent_tokenize(row['unlistdata'])
            for i in raw_data:
                new_df=new_df.append(pd.DataFrame(list(zip([row['Combined_R_P_C']],[row['Date']],[i],[row['Job_Title']],[row['Job_Location']],[row['Former_employee']],[row['Review_Rating']])),columns=col),ignore_index=True)
        raw_corpus = pd.Series(new_df['splitted_sentence'])
    
    # %%
    
    
    # Reading Input file ----------------
    # input_file =  Data_Preparation(pd.read_csv("C:/Subramanya/Cultural Assessment/Short Reports/OneDrive_1_11-25-2018/LDA Data - Culture Assesment/Amazon.csv").Review)
    input_file = Data_Preparation(raw_corpus)
    # input_file = pd.Series(raw_corpus)
    # input_file = pd.Series(raw_corpus)
    
    # Reading Ontology file -----------
    theme_ontology = pd.read_excel(ONTOLOGY_PATH, sheet_name="Ontology_Required_Cutoff_Cosdis")
    # Sorting to keep keywords first, by using groupby and sort
    theme_ontology = theme_ontology.groupby(["Theme"], sort=False).apply(
        lambda x: x.sort_values(["Cosine_dist"], ascending=False)).reset_index(drop=True)
    # Removing duplicate synonyms within a theme and keeping hight value of cosine_dist
    theme_syn = theme_ontology.drop_duplicates(["Theme", "Synonyns"], keep="first")
    # Listing unique themes in the ontology
    theme_nm = list(theme_syn["Theme"].unique())
    # ts= theme_syn.groupby(["Theme"]).count()
    
    final_list = input_file#beginning to add input columns t output final_list
    print ("Testing-Monika")
    print(theme_nm)
    print(final_list.head(5))
    
    final_list.columns = ["Sentence"]
    final_list["Original_Sentence"]=raw_corpus#keeping original sentences
    if ext == '.txt':
        final_list['Date of Review'] = ''
    else:
        final_list['Date of Review'] = new_df['Date_of_Review']
        final_list['Job_Title']=new_df['Job_Title']
        final_list['Job_Location']=new_df['Job_Location']
        final_list['Former_employee']=new_df['Former_employee']
        final_list['Review_Rating']=new_df['Review_Rating']
    #final_list.to_csv(str(filename)+"_ALL2.csv")#uncomment this if you want to get all sentences of the document, because after tagging the untagged sentences wont be part of final list
    for i in range(0, len(theme_nm)):
        print(i)
        ontology = theme_syn[theme_syn.Theme == theme_nm[i]]
        # ontology= theme_ontology.loc[theme_ontology["Theme"]==theme_nm[x],["Theme","Cosine_dist"]]
        synonym = ontology['Synonyns'].unique().tolist()
        synonym = ["\\b" + y + "\\b" for y in synonym]
        pattern = re.compile("|".join(synonym), re.IGNORECASE)
        sel_theme = theme_nm[i]
        print (sel_theme)
        # final_list[sel_theme] = np.nan
        final_list = final_list.assign(sel_theme=np.nan)
        print (final_list.columns)
        for j in range(0, len(final_list)):  # len(final_list) #len(input_file)
            match_synonym = re.findall(pattern, input_file.iloc[j, 0])
            match_synonym2 = np.unique(match_synonym).tolist()
            if len(match_synonym2) > 0:
                match_synonym2 = [k.lower() for k in match_synonym2]
                get_cosdis = pd.DataFrame(match_synonym2)
                get_cosdis.columns = ['Synonyns']
                get_cosdis = pd.merge(get_cosdis, ontology[['Synonyns', 'Cosine_dist']], on='Synonyns', how='left')
                similarity_score = get_cosdis[['Cosine_dist']].sum(axis=0)[0]
                if similarity_score != 0:
                    #print ("a"+final_list.columns)
                    final_list.loc[j, theme_nm[i]] = similarity_score
                    final_list.at[j, theme_nm[i] + '_Words'] = ",".join(match_synonym2)
                    #print ("b"+final_list.columns)
    
    print(final_list.columns)
    final_list.drop(["sel_theme"], axis=1, inplace=True)
    print("testing")
    temp_list=list(set(theme_nm)-set(final_list.columns))
    print (theme_nm)
    theme_nm=list(set(theme_nm)-set(temp_list))
    final_list["Temp_Theme"] = final_list[theme_nm].idxmax(axis=1, skipna=True)
    final_list["Temp_Theme_score"] = final_list[theme_nm].max(axis=1, skipna=True)
    
    final_list2 = final_list[
        final_list['Temp_Theme'].notnull()]  # for removing the sentences which are not classified from Temp theme
    final_list2.reset_index(inplace=True, drop=True)
    
    #final_list2["Final_Theme"] = ''  # Creating empty column for Final Theme
    final_theme = []
    for m in range(0, len(final_list2)):
        Temp_Theme_words = final_list2.loc[m, "Temp_Theme"] + '_Words'  # Column Name of the Temp_Theme
        Num_of_words = len(
            re.split(',| ', final_list2.loc[m, Temp_Theme_words]))  # Number of words in Temp_Theme for given sentence
        theme_score = final_list2.loc[m, "Temp_Theme_score"]  # Score of Temp_Theme for given sentence
        if (Num_of_words >= 2 or theme_score > 1):#condition to check word count and scores
            #final_list2.loc[m, "Final_Theme"] = final_list2.loc[m, "Temp_Theme"]
            final_theme.append(final_list2.loc[m, "Temp_Theme"])
        else:
            final_theme.append('NA')
    chk = pd.DataFrame({'Ontology_Theme': final_theme})
    final_list2 = pd.concat([final_list2,chk], axis=1)
    final_list2['Ontology_Theme'] = np.where(final_list2['Ontology_Theme'] == 'NA', np.nan, final_list2['Ontology_Theme'])
    final_list2 = final_list2[
        final_list2['Ontology_Theme'].notnull()]  # for removing the sentences which are not classified from final theme
    final_list2.reset_index(inplace=True, drop=True)
    final_list2.drop(["Temp_Theme"], axis=1, inplace=True)  # Droping Temp Theme
    
    # ---- Calling sentiment Analysis function -----
    #culture_senti_result = sentiment_s(final_list2)
    culture_senti_result=final_list2
    # save_obj(final_list,filename + "_Sentence_Theme_Similarity")
    #writer = pd.ExcelWriter((filename + '_Sentence_Theme_Similarity.xlsx'), engine='xlsxwriter')
    writer=filename + '_Sentence_Theme_Similarity.xlsx'
    theme_tagging(culture_senti_result,filename,Path(filename).parent)#ELMO based second tagging model
    #writer=filename + '_tagged_ontology.xlsx'
    #culture_senti_result.to_excel(writer, index=False)
    #writer.save()
    
    # ------ End of Code -------------
    
import os
from pathlib import Path
 # Set the directory you want to start from
#comp_list=['HANDELSBANKEN','SEB','NORDEA','SWEDBANK']
comp_list=['Astrazeneca']

onto_path=r'C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Codes_files\D&I code\Ontology_Diversity_V3.xlsx'


for k in range(len(comp_list)):
    comp = comp_list[k]
       
    #comp ='Oracle'
    #print(comp)
    #rootDir="C:\\Users\\taru.b.agarwal\\OneDrive - Accenture\\Culture_Outside_In\\Companies_Data\\" + comp
    rootDir=r"C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Companies\\"+comp
        
    for dirName, subdirList, fileList in os.walk(rootDir):
        if dirName.endswith('Text'):
            for fname in fileList:
                print('\t%s' % fname)
                path=Path(dirName).parent
                print (path)
                try:
                    call_main(str(path), fname,onto_path)
                except Exception as e:
                    print (e)
                       
