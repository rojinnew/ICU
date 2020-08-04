import pandas as pd
import re

# Reading content from the CSV files
old = pd.read_csv('NOTEEVENTS.csv', dtype={'ROW_ID': str, 'SUBJECT_ID':str,'HADM_ID':str,
							 'CHARTDATE': str,'CHARTTIME':str,
							'STORETIME':str,'CATEGORY':str, 'DESCRIPTION':str,
							'CGID':str,'ISERROR':str,
							 'TEXT':str })  

# Replacing newlines and white-spaces in the Name column with "  " separating the category and name
old['TEXT'] = old['TEXT'].str.replace('\n', ' ')
#old['TEXT'] = old['TEXT'].str.replace(' +', ' ')

#print(old['TEXT'])
# Writing the structured data to new CSV files
old.to_csv('new2.csv', index=False)
new2 = pd.read_csv('new2.csv')  

