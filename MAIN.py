import ImportDF
import os
import sys
import pandas as pd

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')

#Run Once
import ReturnDist


pathImport = '\export\Distancia'
normalizeColumns = ['CIDADE']
frameSI = ImportDF.ImportDF(pathImport,normalizeColumns)
frameSI = frameSI.loc[~frameSI['dist'].isna()]
splitValue = 'dist'
frameSI = (frameSI.set_index(frameSI.columns.drop(splitValue,1).tolist())[splitValue].str.split('|', expand=True).stack().reset_index().rename(columns={0:splitValue}).loc[:, frameSI.columns] )



pathImport1 = '\import\DadosDosSite'
normalizeColumns1 = []
DadosDosSite = ImportDF.ImportDF(pathImport1,normalizeColumns1)



frameSI = pd.merge(frameSI,DadosDosSite, how='left',left_on=['dist'],right_on=['LOCATION2'])
frameSI = frameSI.drop(['LOCATION2'], axis=1)












csv_path = os.path.join(script_dir, 'export/Consolidado/'+'Consolidado'+'.csv')
frameSI.to_csv(csv_path,index=True,header=True,sep=';')




