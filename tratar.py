import ImportDF
import os
import sys
import pandas as pd

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')

pathImport = '/import/tratar'
normalizeColumns = ['MUNICÍPIO']

FrameSI = ImportDF.ImportDF(pathImport,normalizeColumns)

grouped = FrameSI.groupby('CELL GSM Detentora')
counts = grouped.size()
threshold = 16  # Set your desired threshold here
filtered_groups = counts[counts <= threshold]

indices_to_drop = FrameSI[FrameSI['CELL GSM Detentora'].isin(filtered_groups.index)].index
df_dropped = FrameSI.drop(indices_to_drop)
print(df_dropped)



csv_path = os.path.join(script_dir, 'export/Consolidado/'+'Consolidado2'+'.csv')
df_dropped.to_csv(csv_path,index=True,header=True,sep=';')






# Step 1: Read the data into a dataframe
FrameSI = ImportDF.ImportDF(pathImport,normalizeColumns)

# Step 2: Group the dataframe by the reference column
grouped = FrameSI.groupby('CELL GSM Detentora')

# Step 3: Count the number of rows in each group
counts = grouped.size()

# Step 4: Identify the rows to keep based on the count threshold
threshold = 16  # Set your desired threshold here

# Create an empty dataframe to store the rows to keep
df_keep = pd.DataFrame()

# Iterate over each group
for name, group in grouped:
    # Check if the group has rows above the threshold
    if len(group) > threshold:
        # Append the first threshold rows to the dataframe to keep
        df_keep = df_keep.append(group[:threshold])
    else:
        # Append all rows if the group has rows below or equal to the threshold
        df_keep = df_keep.append(group)

# Print the resulting dataframe with the desired rows kept
print(df_keep)
csv_path = os.path.join(script_dir, 'export/Consolidado/'+'Consolidado3'+'.csv')
df_keep.to_csv(csv_path,index=True,header=True,sep=';')

