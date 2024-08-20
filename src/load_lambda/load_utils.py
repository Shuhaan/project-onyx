import pandas as pd




def read_parquet_to_df():
    df = pd.read_parquet("dim_currency.parquet")
    # print(df)
    # print(df.values)
    values_lst = df.values.tolist()
    # print(values)
    # print(values[0].split(' '))
    
    # lst = list(df.columns)
    values2 = [f"({','.join(val)}), " for val in values_lst]
    print(values2)
    # print(', '.join(lst))
    
    # for column in df.columns:
    #     print(column)
    return df

read_parquet_to_df()