from read_json_example import new_df


##############################################################
# Filling missing values
##############################################################
# Data
print('\nBefore fillna:', new_df.head(), sep='\n')
scores_df = new_df.copy()

# Fill NaN values with the average from that column
scores_df["math"] = \
    scores_df["math"].fillna(scores_df["math"].mean())

# Many columns at the same time:
scores_df.fillna(
    value={
        'reading': scores_df.reading.mean(),
        'writing': scores_df.writing.mean(),
    },
    inplace=True
)
print('\nAfter fillna:', scores_df.head(), sep='\n')


##############################################################
# Grouping data
##############################################################
# Use .loc[] to only return the needed columns
raw_data = scores_df.loc[:, ["city", "math", "reading", "writing"]]

# Group the data by city, return the grouped DataFrame
grouped_scores = raw_data.groupby(by=["city"]).mean()

# Print the head of the DataFrame
print('\nAfter grouping:', grouped_scores.head(), sep='\n')
