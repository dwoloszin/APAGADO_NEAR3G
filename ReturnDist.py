import os
import sys
import ImportDF
import CalcDistAzim

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')

pathImport = '\import\LIST'
normalizeColumns = ['CIDADE']
Dados = ImportDF.ImportDF(pathImport,normalizeColumns)
Dados.sort_values(by=['LAT', 'LONG'], ascending=[True, True], inplace=True)


pathImport2 = '\import\BASE'
Dados2 = ImportDF.ImportDF(pathImport2,normalizeColumns)
Dados2.sort_values(by=['LAT', 'LONG'], ascending=[True, True], inplace=True)

col_one_list = Dados['CIDADE'].unique()

for i in col_one_list:
  df2 = Dados2.loc[Dados2['CIDADE'] == i]
  convert1 = ['LAT','LONG']
  for c in convert1:
    df2[c] = df2[c].str.replace(',', '.')  # Remove commas from the strings
    df2[c] = df2[c].astype(float)  
  df3 = ImportDF.ImportDF(pathImport,normalizeColumns)
  df3.sort_values(by=['LAT', 'LONG'], ascending=[True, True], inplace=True)
  df2.sort_values(by=['LAT', 'LONG'], ascending=[True, True], inplace=True)
  df3 = Dados.loc[Dados['CIDADE'] == i]
  df3.insert(len(df3.columns),'dist','')
  df3.insert(len(df3.columns),'dist_Value','')
  for index1,row1 in df3.iterrows():
    print('Processing ',row1['CIDADE'])
    maxValue = 2000.0
    maxDistanceSquare = 100 # 20Km
    #One degree of latitude is approximately 111 kilometers
    distance_degrees = maxDistanceSquare / 111
    ref3 = []
    ref4 = []
    KeepGoing = True
   
    '''
    df2 = df2.loc[
                    (df2['LAT'] >= float(row1['LAT'].replace(',','.')) - distance_degrees) &
                    (df2['LAT'] <= float(row1['LAT'].replace(',','.')) + distance_degrees) &
                    (df2['LONG'] >= float(row1['LONG'].replace(',','.')) - distance_degrees) &
                    (df2['LONG'] <= float(row1['LONG'].replace(',','.')) + distance_degrees)
                  ]
    '''

    #print(df2,df2.shape[0] )
    while len(ref3) < 6 and not df2.empty:
      for index2, row2 in df2.iterrows():
        if row1['CIDADE'] == row2['CIDADE']:
          distancia = CalcDistAzim.CalcDist(row1['LAT'],row1['LONG'],row2['LAT'],row2['LONG'])
          if distancia < maxValue:
            if (row2['LOCATION'] not in ref3):
              ref3.append(row2['LOCATION'])
              ref4.append(round(distancia,0))
      s = '|'.join(str(x) for x in ref3)
      d = '|'.join(str(x) for x in ref4)        
      df3.at[index1,'dist'] = s
      df3.at[index1,'dist_Value'] = d
      maxValue +=50
      maxDistanceSquare +=5
  print(row1['CIDADE'], ' OK')  
  csv_path = os.path.join(script_dir, 'export/Distancia/'+ i +'_Distancia'+'.csv')
  df3.to_csv(csv_path,index=True,header=True,sep=';')
    




