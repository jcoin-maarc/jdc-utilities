"""Data transformations"""

import pandas as pd
import openpyxl
import yaml
import re
from collections import OrderedDict
import pandas_flavor as pf

def read_df(file_path):
    '''
    read in a data file based on
    the type of file

    '''
    file_type = re.split("\.",file_path)[-1]

    if file_type=='csv':
        df = pd.read_csv(file_path)
    elif file_type=='tsv':
        df = pd.read_csv(file_path,sep='\t')
    elif file_type=='xlsx':
        df = pd.read_excel(file_path)
    #TO ADD:
    #redcap 
    #xls
    else:
        sys.exit("Data type not supported")
    return df

def read_transformfile(transformfile):
    with open(transformfile) as file:
        return yaml.safe_load(file)

def run_transformfile(df,transformfile):
    ''' 
    loop through transformations and mappings as specified 
    in the yaml file

    then runs a given function name with a set of paramaters.
    Intended to transform the dataframe inplace.
    To provide compatability with native pandas fxns, 
    the inplace argument assumed to be a parameter. 

    If kwargs, then need to register a function 
    calling the dictionary as keyword args in TransformDf class.
    '''
    transform_mappings = OrderedDict(read_transformfile(transformfile))


    for fxn_name,params in transform_mappings.items():
            print(fxn_name)
            print(params)
            getattr(df,fxn_name)(**params)

#Note: alternative to using pandas-flavor is making a child class of pd.DataFrame
#all registered functions should transforms df inplace or have capability of inplace
#if making an inplace option to registered functions, make inplace=True as default.
@pf.register_dataframe_method
def to_quarter(df,from_date_name_to_quarter_name):
    ''' 
    adds a quarter column by converting a date-like column
    into a quarter
    '''
    for from_date_name,to_quarter_name in from_date_name_to_quarter_name.items():
        var = pd.to_datetime(df[from_date_name])
        quarters = pd.PeriodIndex(var, freq='Q')
        df[to_quarter_name] = quarters
    #return quarters

@pf.register_dataframe_method
def collapse_checkall(df, resulting_column_name,columns=None, columns_to_labels=None,checked='Checked',
                    multi_checked='Multiple checked',
                    none_checked='None checked', fillna=False, labels=None,inplace=True):
    """Collapse multiple check-all-that-apply fields into one"""
    
    #added option to make a columns to labels dict to make easier to see mappings
    if columns_to_labels and not columns and not labels:
        columns = columns_to_labels.keys()
        labels = columns_to_labels.values()

    bcols = df[columns]==checked
    var = bcols.idxmax(axis=1).where(bcols.sum(axis=1)==1, multi_checked).\
                            where(bcols.sum(axis=1)>0, none_checked)
    
    var = var.where((df[columns].notnull().sum(axis=1)==len(columns)) |
                    (var==multi_checked), None)
    
    if labels:
        var.replace(dict(zip(columns,labels)), inplace=True)
    
    if fillna:
        var.fillna(fillna, inplace=True)
    
    if inplace:
        df[resulting_column_name] =  var
    else:
        return var


@pf.register_dataframe_method
def replace_all_values(df,from_value_to_value,inplace=True):
    df.replace(from_value_to_value,inplace=inplace)

@pf.register_dataframe_method
def add_columns_of_constants(df,column_name_and_value):
    for name,value in column_name_and_value.items():
        df[name] = value

@pf.register_dataframe_method
def rename_columns(df,from_name_to_name,inplace=True):
    df.rename(columns=from_name_to_name,inplace=inplace)

@pf.register_dataframe_method
def replace_column_values(df,within_column_from_value_to_value,inplace=True):
    df.replace(within_column_from_value_to_value,inplace=inplace)