import pandas as pd


def find_street_name(row):
    # Split the street_address by spaces
    split_street_address = row["Street Address"].split(" ")

    # Remove the number
    street_number = split_street_address[0]
    try:
        int(street_number)
    except ValueError:
        return row["Street Address"]

    return " ".join(split_street_address[1:])


def transform(raw_data):
    # Use the apply function to extract the street_name from the street_address
    raw_data["Street Name"] = raw_data.apply(
        find_street_name,  # Pass the correct function to the apply method
        axis=1
    )
    return raw_data


if __name__ == '__main__':
    raw_testing_scores = pd.read_csv('data-sources/scores.csv')

    # Transform the raw_testing_scores DataFrame
    cleaned_testing_scores = transform(raw_testing_scores)

    # Print the head of the cleaned_testing_scores DataFrame
    print(cleaned_testing_scores.head())
