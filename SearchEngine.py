import pandas as pd
from time import time
from datetime import datetime
import nltk
from nltk import re 
from elasticsearch import Elasticsearch
from nltk.tokenize import RegexpTokenizer
import csv
from nltk import word_tokenize
from elasticsearch import helpers
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import num2words as num2
es = Elasticsearch([{'host':'localhost', 'port':9200}])

if(es.ping()):
    print("connected")


##connecting to elasticsearch via port

stop_words = (stopwords.words("english")) ##creating stopwords 

lemma = WordNetLemmatizer()
tokenizer = RegexpTokenizer(r'[^.?!•]+') ##regex function to identify punct and bullet points 


def read_csv():
    dataframe = pd.read_csv("archive\\wiki_movie_plots_deduped.csv", nrows=1000)
    return dataframe
##reading csv and selecting first 100 docs using nrows

        
        
def remove_stop_w(ws):
    return[word.lower() for word in ws if re.search("\w",word)]

def remove_punct(ws):
    return[word.lower() for word in ws if word not in stop_words]

def pre_process(dataframe):
    
    for i in range(0, len(dataframe)):
        ent = dataframe.iloc[i]
        Plot = nltk.word_tokenize(ent["Plot"])
        Plot = remove_punct(Plot)   ##removing punct from plot column
        Plot = remove_stop_w(Plot)  ##removing stop words from plot column

        for x, value in enumerate (Plot):
            Plot[x] = lemma.lemmatize(value) ##applying lemmatization rather than stemming
        Title = nltk.word_tokenize(ent["Title"])
        Title = remove_punct(Title)
        Title = remove_stop_w(Title)

        

        Realease_Year = num2.num2words(ent["Release Year"], to = "year") ##creating new columns in dataframe for pre processed data
        dataframe.at[i, "Mod_Realease_Year"] = Realease_Year
        dataframe.at[i, "ModTitle"] = Title
        dataframe.at[i, "ModPlot"] = Plot
        
    return dataframe
        
    
def upload_index(dataframe): ##indexing data to elasticsearch
    bulk_ent = []
    for i in range(len(dataframe)):
        ent = dataframe.iloc[i]
        stat = {}
        for key, value, in ent.iteritems():
            value = str(value)
            stat[key] = value
        bulk_ent.append(stat)
    helpers.bulk(es,bulk_ent, index="reviews")
    

def run():
    dataframe = read_csv()
    if es.indices.exists ("reviews"): ##if index already exists, delete and rebuild 
        es.indices.delete("reviews")
    es.indices.create(index="reviews")

    print("index created")
    
    dataframe.insert(0, "Mod_Realease_Year","None") ##creating new columns in dataframe for pre processed data
    dataframe.insert(0, "ModTitle","None")
    dataframe.insert(0, "ModCast","None")
    dataframe.insert(0, "ModPlot","None")

    pre_process(dataframe)
    upload_index(dataframe)

run()
