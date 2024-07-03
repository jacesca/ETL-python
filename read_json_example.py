import json
import pandas as pd
from pprint import pprint


########################################################
# Using pandas
########################################################
print('-'*15)
print('Reading into pandas dataframe:')
print('-'*15)
file_path = 'data-sources/nested_scores.json'
df = pd.read_json(file_path, orient="index")
print('Last 2 rows:', df.tail(2), sep='\n')
print('Shape:', df.shape)

print('-'*15)
print('Extracting the nested json from scores column')
print('-'*15)
new_df = pd.concat(
    [df.drop('scores', axis='columns'), df.scores.apply(pd.Series)],
    axis='columns'
)
print('Last 2 rows:', new_df.tail(2), sep='\n')
print('Shape:', new_df.shape)
print('-'*15)
print()
print('-'*15)


########################################################
# Using json
########################################################
print('Reading with json pkg')
print('-'*15)
with open(file_path, 'r') as f:
    data = json.load(f)
pprint(list(data.items())[:2])
print('Size:', len(list(data.items())))
print('-'*15)

print('Finally transformation into DataFrame')
print('-'*15)
normalized_data = [
    [
        school_id,
        school_data.get('city'),
        school_data.get('street_address'),
        school_data.get('scores').get('math'),
        school_data.get('scores').get('reading'),
        school_data.get('scores').get('writing')
    ] if school_data.get('scores') else [
        school_id,
        school_data.get('city'),
        school_data.get('street_address'),
        None,
        None,
        None
    ]
    for school_id, school_data in data.items()
]
final_df = pd.DataFrame(normalized_data)
final_df.columns = ['', 'city', 'address', 'math', 'reading', 'writing']
final_df.set_index('', inplace=True)
print('Last 2 rows:', final_df.tail(2), sep='\n')
print('Shape:', final_df.shape)
print('-'*15)
