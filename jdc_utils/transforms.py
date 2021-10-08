"""Data transformations"""

import pandas as pd
import yaml
from collections import OrderedDict

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
        try: #see if function is a pandas method
            print(fxn_name)
            print(params)
            getattr(df,fxn_name)(**params,inplace=True)
        except AttributeError: #if not, run the global function
            print(fxn_name)
            print(params)
            eval(fxn_name)(df,**params,inplace=True)

def to_quarter(df,from_date_name,to_quarter_name,inplace=None):
    ''' 
    adds a quarter column by converting a date-like column
    into a quarter
    '''
    var = pd.to_datetime(df[from_date_name])
    quarters = pd.PeriodIndex(var, freq='Q')
    if inplace:
        df[to_quarter_name] = quarters
    else:
        return quarters

def collapse_checkall(df, collapsed_name,columns, checked='Checked',
                    multi_checked='Multiple checked',
                    none_checked='None checked', fillna=False, labels=None,inplace=None):
    """Collapse multiple check-all-that-apply fields into one"""
    
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
        df[collapsed_name] =  var
    else:
        return var



