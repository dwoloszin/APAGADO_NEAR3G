import os
import sys
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from os.path import getmtime

import unicodedata

def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.name},inplace=True)
    return df

def normalize_string(name):
    normalized_name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8').upper()
    return normalized_name

def ImportDF(pathImport,normalizeColumns):
    pathImportSI = os.getcwd() + pathImport
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []
    lastData = datetime.fromtimestamp(getmtime(all_filesSI[0])).strftime('%Y%m%d')
    for filename in all_filesSI:
        fileData = datetime.fromtimestamp(getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, on_bad_lines='skip',dtype=str, sep = ';',iterator=True, chunksize=10000)
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS
        #df2["dataArchive_Import"] = fileData   
        li.append(df)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()
    if len(normalizeColumns)>0:
      for i in normalizeColumns:
        frameSI[i] = frameSI[i].apply(normalize_string)
        

    return frameSI



#MUNICIPIO['MUNICIPIO'] = MUNICIPIO['MUNICIPIO'].apply(normalize_string)

