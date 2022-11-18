

# for each journey, compute the sum of conversions generated
conversion_table = data.groupby("jvector").agg({"is_converted": "sum"})
print(f"{len(conversion_table.loc[conversion_table['is_converted']>0])} out of {len(conversion_table)} user journeys have generated some conversions")


