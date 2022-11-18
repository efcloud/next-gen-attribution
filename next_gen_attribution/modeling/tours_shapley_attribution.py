import itertools

def power_set(input_list):
    '''
    computes the power set of an input set (cast to a list)
    '''
    pset = [",".join(list(j)) for i in range(len(input_list)) for j in itertools.combinations(input_list, i+1)]
    return pset

def value_function(coalition, coalition_value_dict):
    '''
    computes the value of each coalition.
    '''
    subsets_of_coalition = subsets(coalition)
    value_of_coalition=0
    for subset in subsets_of_coalition:
        if subset in coalition_value_dict:
            value_of_coalition += coalition_value_dict[subset]
    return value_of_coalition


# define column subsets (TODO: refactor)
non_touchpoints = ["user_id", "_uid", "is_converted"]
utm_source_columns = list(data.filter(regex="utm_source_"))
utm_campaign_columns = list(data.filter(regex="utm_campaign_"))
non_touchpoints.extend(utm_campaign_columns)
touchpoints = utm_source_columns

# for each journey vector, compute the observed sum of conversions generated by each coalition
jvector_conversion_df = data.groupby("jvector", as_index=False).agg({"is_converted": "sum"})
jvector_conversion_df = jvector_conversion_df.rename(columns={"is_converted": "conversions"})
print(f"{len(jvector_conversion_df.loc[jvector_conversion_df['conversions']>0])} out of {len(jvector_conversion_df)} user journeys have generated some conversions")

# define a coalition as the element-wise multiplication of a journey vector and the vector of journey column names
coalition_conversion_df = pd.DataFrame()
coalition_conversion_df["coalition"] = [tuple(map(lambda x,y: x*y, jvector, tuple(touchpoints))) for jvector in jvector_conversion_df["jvector"]]
coalition_conversion_df["coalition"] = coalition_conversion_df["coalition"].apply(lambda x: ",".join([element for element in x if element]))
coalition_conversion_df["conversions"] = jvector_conversion_df["conversions"]

# construct coalition-value dictionary of the form {coalition: coalition_value}
coalition_value_dict = dict((coalition, conversions) for coalition, conversions in zip(coalition_conversion_df["coalition"], coalition_conversion_df["conversions"]))

# for the set of all journeys, compute the power set
cpset = power_set(touchpoints)

# construct subset-value dictionary of the form {subset: subset_value} 
subset_value_dict = dict()
for subset in cpset:
    subset_value_dict[subset] = coalition_value_dict.get(subset, 0)
print(f"There are {sum(subset_value_dict.values())} out of 2^{len(touchpoints)} coalitions with non-zero value")

# compute shapley values
shapley_values = defaultdict(int)
for touchpoint in touchpoints:
    for coalition in subset_value_dict.keys():
        if touchpoint not in coalition.split(","):
            cardinal_coalition=len(coalition.split(","))
            coalition_with_touchpoint = coalition.split(",")
            coalition_with_touchpoint.append(touchpoint)            
            coalition_with_touchpoint=",".join(sorted(coalition_with_touchpoint))
            weight = (factorial(cardinal_coalition)*factorial(n-cardinal_coalition-1)/factorial(n)) # weight = |S|!(n-|S|-1)!/n!
            contrib = (v_values[coalition_with_touchpoint]-v_values[coalition]) # marginal contribution = v(S U {i})-v(S)
            shapley_values[touchpoint] += weight * contrib
    shapley_values[touchpoint] += v_values[touchpoint]/n # add the term corresponding to the empty set
