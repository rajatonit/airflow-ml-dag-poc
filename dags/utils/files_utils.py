import pandas as pd
import os.path

def save_files(df_list):
    '''
    accepts dataframe list as input
    saves each dataframe in the tmp folder as csv
    the file name corresponds to the dataframe "name" attribute
    '''
    [ df.to_csv('gs://spark-poc-ca/data/' + df.name + '.csv' , sep=',', index=False) for df in df_list ]


def load_files(names_list):
    '''
    accepts a list of names (str) as input
    load each csv file from the tmp folder with the input names
    returns a list of loaded dataframes
    '''
    df_list = []
    [df_list.append(pd.read_csv("gs://spark-poc-ca/data/" + name + ".csv")) for name in names_list]
    return df_list