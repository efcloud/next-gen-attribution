import pandas as pd

data = pd.read_csv("file_for_hans_2.csv")

data.rename(columns = {list(data)[0]:'user_id'}, inplace=True)

# put all of adobe into email as per Vivek's 2022-11-17-1757 message "source=email and source=adobe is one and the same"
data["utm_source_email"] = data["utm_source_email"] + data["utm_source_adobe"]
data = data.drop(columns=["utm_source_adobe"])

# create journey vector
non_journey_columns = ["user_id", "_uid", "is_converted"]
utm_source_columns = list(data.filter(regex="utm_source_"))
utm_campaign_columns = list(data.filter(regex="utm_campaign_"))
non_journey_columns.extend(utm_campaign_columns)
data["jvector"] = data.drop(columns=non_journey_columns).apply(list, axis=1)
print(f"There are {len(data['jvector'].value_counts())} unique user journeys in this dataset")
