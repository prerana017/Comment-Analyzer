
import os, glob
import pandas as pd
#from predict import bertPredict
#pip install bert-tensorflow==1.0.1

def sentiment_tagging(path,fname):
    file_full_path=fname
    print("full_path="+ file_full_path)
    df = pd.read_excel(file_full_path)
    df['Bert_Sentiment']=bertPredict(df['Sentence'].values) #Replaces 'text' with the column name of your text data
    df.to_excel(os.path.splitext(fname)[0]+'_prediction.xlsx',index=False) #Save the Predictions

def convert_lines(example, max_seq_length,tokenizer):
    max_seq_length -=2
    all_tokens = []
    longer = 0
    for i in range(example.shape[0]):
      tokens_a = tokenizer.tokenize(example[i])
      if len(tokens_a)>max_seq_length:
        tokens_a = tokens_a[:max_seq_length]
        longer += 1
      one_token = tokenizer.convert_tokens_to_ids(["[CLS]"]+tokens_a+["[SEP]"])+[0] * (max_seq_length - len(tokens_a))
      all_tokens.append(one_token)
    print(longer)
    return np.array(all_tokens)
    
maxlen=128
def bertPredict(sentences):
  token_input2 = convert_lines(np.array(sentences),maxlen,tokenizer)
  seg_input2 = np.zeros((token_input2.shape[0],maxlen))
  pred = model.predict([token_input2, seg_input2])
  pred=pred.argmax(axis=1)
  pred=np.where(pred==0,'Negative',np.where(pred==1,'Neutral','Positive'))
  return pred

from pathlib import Path

#Enter the directory where the model and tokenizer file are 
#os.chdir(r'C:\Users\monika.joshi\Documents\Project-culture out-in\Sentiment_Bert_model')

from keras.models import load_model
from keras_bert import get_custom_objects


# =============================================================================
# import tokenizer
# 
#import tensorflow as tf
# 
#import tensorflow_hub as hub
# 
#from tensorflow.keras import layers
# 
#import tensorflow.compat.v1 as tf
# 
# 
# 
# import bert

# from bert import run_classifier
# from bert import optimization
#import bert_tokenizer as tokenizer
#from bert import tokenization
# from bert import modeling
# =============================================================================

from bert.tokenization import FullTokenizer
import numpy as np
import pickle

import pickle

# saving
# =============================================================================
# with open('tokenizer.pickle', 'wb') as handle:
#     pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)
# 
# =============================================================================
# loading
# =============================================================================
# with open('tokenizer.pickle', 'rb') as handle:
#     tokenizer = pickle.load(handle)
with open(r'C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Codes_files\D&I code\tokenizer.pickle', 'rb') as handle:
    tokenizer = pickle.load(handle)
    #tokenizer = FullTokenizer(tokenizer)
    
model=load_model(r'model.h5',custom_objects=get_custom_objects())


    # Set the directory you want to start from

comp_list = ['Astrazeneca']
for comp in comp_list:
    print(comp)
    rootDir=r"C:\Users\prerana.a.singh\OneDrive - Accenture\Project-Diversity\Companies\\" + comp
    
    for dirName, subdirList, fileList in os.walk(rootDir):
        if dirName.endswith('Text'):
            for fname in glob.glob(str(Path(dirName) )+ '\\*Sentence_Theme_Similarity.xlsx'):
                print('\t%s' % fname)
                path=Path(dirName).parent
                print (path)
                try:
                    sentiment_tagging(str(path), fname)
                except Exception as e:
                    print (e)
                
                        